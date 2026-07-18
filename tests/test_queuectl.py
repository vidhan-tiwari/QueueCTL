import unittest
import os
import sqlite3
import time
from datetime import datetime, timezone, timedelta

# Import queuectl modules
from queuectl.db import init_db, get_db_connection, get_utc_now, DB_PATH, DB_DIR
from queuectl.config import Config
from queuectl.worker import Worker
from queuectl import manager

class TestQueueCTL(unittest.TestCase):
    def setUp(self):
        """Prepares a clean test environment by resetting the database."""
        # Clean up database file if it exists to start fresh
        if os.path.exists(DB_PATH):
            try:
                # Close any open connections before removing
                conn = sqlite3.connect(DB_PATH)
                conn.close()
                os.remove(DB_PATH)
            except Exception:
                pass
        init_db()

    def tearDown(self):
        """Cleans up the database file after test completion."""
        # Stop any active workers recorded in the DB during testing
        conn = get_db_connection()
        try:
            with conn:
                conn.execute("DELETE FROM workers;")
        except Exception:
            pass
        finally:
            conn.close()

    def test_database_and_config(self):
        """Tests database initialization and configuration get/set operations."""
        # Verify defaults
        self.assertEqual(Config.get_max_retries(), 3)
        self.assertEqual(Config.get_backoff_base(), 2.0)
        self.assertEqual(Config.get_default_timeout(), 300)
        
        # Test updating configuration
        Config.set_max_retries(5)
        Config.set_backoff_base(1.5)
        Config.set_default_timeout(60)
        
        self.assertEqual(Config.get_max_retries(), 5)
        self.assertEqual(Config.get_backoff_base(), 1.5)
        self.assertEqual(Config.get_default_timeout(), 60)

    def test_job_enqueueing(self):
        """Tests that jobs are enqueued with correct fields, defaults, and timestamps."""
        conn = get_db_connection()
        now_str = get_utc_now()
        
        # Enqueue a job
        with conn:
            conn.execute(
                """
                INSERT INTO jobs (
                    id, command, state, priority, attempts, max_retries, 
                    backoff_base, timeout, created_at, updated_at, next_run_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                """,
                ("test-job-1", "echo 'Hello World'", "pending", 2, 0, 3, 2.0, 30, now_str, now_str, now_str)
            )
            
        # Verify it exists and has correct values
        job = conn.execute("SELECT * FROM jobs WHERE id = 'test-job-1';").fetchone()
        self.assertIsNotNone(job)
        self.assertEqual(job["command"], "echo 'Hello World'")
        self.assertEqual(job["state"], "pending")
        self.assertEqual(job["priority"], 2)
        self.assertEqual(job["max_retries"], 3)
        self.assertEqual(job["backoff_base"], 2.0)
        self.assertEqual(job["timeout"], 30)
        conn.close()

    def test_atomic_job_fetching(self):
        """Tests that jobs are fetched atomically and other workers cannot retrieve them."""
        # Enqueue two jobs with different priorities
        conn = get_db_connection()
        now_str = get_utc_now()
        with conn:
            conn.execute(
                """
                INSERT INTO jobs (id, command, state, priority, attempts, max_retries, backoff_base, timeout, created_at, updated_at, next_run_at)
                VALUES 
                ('low-priority', 'sleep 1', 'pending', 1, 0, 3, 2.0, 30, ?, ?, ?),
                ('high-priority', 'sleep 1', 'pending', 3, 0, 3, 2.0, 30, ?, ?, ?);
                """,
                (now_str, now_str, now_str, now_str, now_str, now_str)
            )
        conn.close()

        # Create two workers
        worker1 = Worker("worker-1")
        worker2 = Worker("worker-2")
        worker1.register()
        worker2.register()
        
        # Worker 1 fetches next job. It should get the high priority job first.
        job1 = worker1.fetch_next_job()
        self.assertIsNotNone(job1)
        self.assertEqual(job1["id"], "high-priority")
        self.assertEqual(job1["state"], "pending")  # Fetched row contains snapshot before state change
        
        # Worker 2 fetches next job. It should get the remaining low priority job.
        job2 = worker2.fetch_next_job()
        self.assertIsNotNone(job2)
        self.assertEqual(job2["id"], "low-priority")
        
        # Worker 1 tries to fetch again. No jobs left.
        job3 = worker1.fetch_next_job()
        self.assertIsNone(job3)

    def test_worker_execution_success(self):
        """Tests that a successful job run updates state to completed."""
        # Enqueue a quick successful job
        conn = get_db_connection()
        now_str = get_utc_now()
        with conn:
            conn.execute(
                """
                INSERT INTO jobs (id, command, state, priority, attempts, max_retries, backoff_base, timeout, created_at, updated_at, next_run_at)
                VALUES ('success-job', 'echo \"Success\"', 'pending', 1, 0, 3, 2.0, 30, ?, ?, ?);
                """,
                (now_str, now_str, now_str)
            )
        conn.close()
        
        worker = Worker("test-worker")
        worker.register()
        job = worker.fetch_next_job()
        self.assertIsNotNone(job)
        
        # Execute job
        worker.execute_job(job)
        
        # Verify job is completed
        conn = get_db_connection()
        db_job = conn.execute("SELECT * FROM jobs WHERE id = 'success-job';").fetchone()
        self.assertEqual(db_job["state"], "completed")
        self.assertEqual(db_job["attempts"], 1)
        self.assertIsNone(db_job["worker_id"])
        conn.close()

    def test_worker_execution_retry_and_dlq(self):
        """Tests automatic job retry with exponential backoff and transition to DLQ on failure exhaustion."""
        # Enqueue a failing job (exit code 1)
        conn = get_db_connection()
        now_str = get_utc_now()
        with conn:
            conn.execute(
                """
                INSERT INTO jobs (id, command, state, priority, attempts, max_retries, backoff_base, timeout, created_at, updated_at, next_run_at)
                VALUES ('failing-job', 'python -c \"import sys; sys.exit(1)\"', 'pending', 1, 0, 2, 2.0, 30, ?, ?, ?);
                """,
                (now_str, now_str, now_str)
            )
        conn.close()
        
        worker = Worker("test-worker")
        worker.register()
        
        # First execution (Attempt 1)
        job = worker.fetch_next_job()
        worker.execute_job(job)
        
        conn = get_db_connection()
        db_job = conn.execute("SELECT * FROM jobs WHERE id = 'failing-job';").fetchone()
        self.assertEqual(db_job["state"], "pending")
        self.assertEqual(db_job["attempts"], 1)
        # Verify next_run_at is scheduled in the future (backoff: 2.0^1 = 2 seconds)
        next_run_dt = datetime.fromisoformat(db_job["next_run_at"])
        now_dt = datetime.now(timezone.utc)
        self.assertTrue(next_run_dt > now_dt)
        self.assertTrue((next_run_dt - now_dt).total_seconds() <= 2.1)
        
        # Force set next_run_at to now for test processing speed
        with conn:
            conn.execute("UPDATE jobs SET next_run_at = ? WHERE id = 'failing-job';", (get_utc_now(),))
        conn.close()
        
        # Second execution (Attempt 2)
        job = worker.fetch_next_job()
        worker.execute_job(job)
        
        conn = get_db_connection()
        db_job = conn.execute("SELECT * FROM jobs WHERE id = 'failing-job';").fetchone()
        self.assertEqual(db_job["state"], "pending")
        self.assertEqual(db_job["attempts"], 2)
        # Force set next_run_at to now again
        with conn:
            conn.execute("UPDATE jobs SET next_run_at = ? WHERE id = 'failing-job';", (get_utc_now(),))
        conn.close()
        
        # Third execution (Attempt 3, exceeds max_retries = 2)
        job = worker.fetch_next_job()
        worker.execute_job(job)
        
        conn = get_db_connection()
        db_job = conn.execute("SELECT * FROM jobs WHERE id = 'failing-job';").fetchone()
        self.assertEqual(db_job["state"], "dead")
        self.assertEqual(db_job["attempts"], 3)
        self.assertIsNotNone(db_job["error_log"])
        conn.close()

    def test_worker_execution_timeout(self):
        """Tests that a job exceeding its timeout is terminated and marked failed."""
        # Enqueue a job with timeout = 1 second, but takes 3 seconds to run
        conn = get_db_connection()
        now_str = get_utc_now()
        with conn:
            conn.execute(
                """
                INSERT INTO jobs (id, command, state, priority, attempts, max_retries, backoff_base, timeout, created_at, updated_at, next_run_at)
                VALUES ('timeout-job', 'python -c \"import time; time.sleep(3)\"', 'pending', 1, 0, 1, 2.0, 1, ?, ?, ?);
                """,
                (now_str, now_str, now_str)
            )
        conn.close()
        
        worker = Worker("test-worker")
        worker.register()
        job = worker.fetch_next_job()
        
        start = time.time()
        worker.execute_job(job)
        duration = time.time() - start
        
        # Ensure job execution finished in under 2 seconds (not waiting the full 3 seconds sleep)
        self.assertTrue(duration < 2.0)
        
        conn = get_db_connection()
        db_job = conn.execute("SELECT * FROM jobs WHERE id = 'timeout-job';").fetchone()
        # Should be marked pending (retryable) with attempt 1
        self.assertEqual(db_job["state"], "pending")
        self.assertEqual(db_job["attempts"], 1)
        self.assertIn("timed out", db_job["error_log"].lower())
        conn.close()

    def test_background_worker_spawning(self):
        """Tests that manager.start_workers spawns workers that successfully register in the database."""
        # Clear workers first
        conn = get_db_connection()
        with conn:
            conn.execute("DELETE FROM workers;")
        conn.close()
        
        # Start 1 worker
        spawned = manager.start_workers(1)
        self.assertEqual(spawned, 1)
        
        # Wait up to 3 seconds for it to start and register
        registered = False
        for _ in range(30):
            time.sleep(0.1)
            conn = get_db_connection()
            workers = conn.execute("SELECT * FROM workers;").fetchall()
            conn.close()
            if len(workers) == 1:
                registered = True
                break
                
        # Stop workers gracefully
        manager.stop_workers()
        
        self.assertTrue(registered, "Worker process failed to start and register in the database.")

if __name__ == "__main__":
    unittest.main()
