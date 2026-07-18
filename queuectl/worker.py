import os
import sys
import uuid
import time
import sqlite3
import subprocess
import signal
from datetime import datetime, timezone, timedelta
from queuectl.db import get_db_connection, get_utc_now, LOGS_DIR

class Worker:
    def __init__(self, worker_id=None):
        self.id = worker_id or str(uuid.uuid4())
        self.pid = os.getpid()
        self.should_stop = False
        
        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self.handle_signal)
        signal.signal(signal.SIGTERM, self.handle_signal)
        # Windows SIGBREAK
        if hasattr(signal, "SIGBREAK"):
            signal.signal(signal.SIGBREAK, self.handle_signal)

    def handle_signal(self, signum, frame):
        """Sets the stop flag to initiate graceful shutdown on signal receipt."""
        self.should_stop = True

    def register(self):
        """Registers this worker in the database."""
        conn = get_db_connection()
        try:
            with conn:
                conn.execute(
                    """
                    INSERT OR REPLACE INTO workers (id, pid, state, heartbeat)
                    VALUES (?, ?, ?, ?);
                    """,
                    (self.id, self.pid, "running", get_utc_now())
                )
        finally:
            conn.close()

    def update_heartbeat(self):
        """Updates the worker's heartbeat in the database and checks for external shutdown requests."""
        conn = get_db_connection()
        try:
            # Check if requested to stop via database
            row = conn.execute("SELECT state FROM workers WHERE id = ?;", (self.id,)).fetchone()
            if row and row["state"] == "shutting_down":
                self.should_stop = True
            
            with conn:
                conn.execute(
                    "UPDATE workers SET heartbeat = ? WHERE id = ?;",
                    (get_utc_now(), self.id)
                )
        except Exception:
            pass  # Don't crash worker loop on heartbeat failures
        finally:
            conn.close()

    def unregister(self):
        """Removes the worker from the database on exit."""
        conn = get_db_connection()
        try:
            with conn:
                conn.execute("DELETE FROM workers WHERE id = ?;", (self.id,))
        except Exception:
            pass
        finally:
            conn.close()

    def fetch_next_job(self) -> dict:
        """Atomically locks and returns the next pending job using SQLite immediate transactions."""
        conn = get_db_connection()
        conn.isolation_level = None  # Manual transaction management
        try:
            conn.execute("BEGIN IMMEDIATE;")
            now_str = get_utc_now()
            
            # Select the highest priority and oldest eligible pending job
            row = conn.execute(
                """
                SELECT * FROM jobs 
                WHERE state = 'pending' AND next_run_at <= ? 
                ORDER BY priority DESC, created_at ASC 
                LIMIT 1;
                """,
                (now_str,)
            ).fetchone()
            
            if row:
                job_id = row["id"]
                conn.execute(
                    """
                    UPDATE jobs 
                    SET state = 'processing', worker_id = ?, updated_at = ? 
                    WHERE id = ?;
                    """,
                    (self.id, now_str, job_id)
                )
                conn.execute("COMMIT;")
                return dict(row)
            
            conn.execute("COMMIT;")
            return None
        except sqlite3.OperationalError:
            # Locking failure, rollback and wait for next poll
            try:
                conn.execute("ROLLBACK;")
            except Exception:
                pass
            return None
        except Exception:
            try:
                conn.execute("ROLLBACK;")
            except Exception:
                pass
            raise
        finally:
            conn.close()

    def execute_job(self, job: dict):
        """Runs the job command and handles success, failure, timeout, logging, and retry logic."""
        job_id = job["id"]
        command = job["command"]
        timeout = job["timeout"]
        
        log_path = os.path.join(LOGS_DIR, f"{job_id}.log")
        start_time = get_utc_now()
        
        # Write start header to job log
        with open(log_path, "a", encoding="utf-8") as lf:
            lf.write(f"\n--- Job {job_id} Run started at {start_time} (Attempt {job['attempts'] + 1}) ---\n")
            lf.write(f"Command: {command}\n\n")
        
        exit_code = -1
        output = ""
        error_msg = ""
        timed_out = False
        
        try:
            # Run command using subprocess
            res = subprocess.run(
                command,
                shell=True,
                capture_output=True,
                text=True,
                timeout=timeout
            )
            exit_code = res.returncode
            output = f"STDOUT:\n{res.stdout}\nSTDERR:\n{res.stderr}\n"
        except subprocess.TimeoutExpired as te:
            timed_out = True
            exit_code = -9
            output = f"STDOUT (partial):\n{te.stdout.decode('utf-8', errors='ignore') if te.stdout else ''}\n" \
                     f"STDERR (partial):\n{te.stderr.decode('utf-8', errors='ignore') if te.stderr else ''}\n"
            error_msg = f"Job timed out after {timeout} seconds."
        except Exception as e:
            exit_code = -2
            error_msg = f"Execution failed: {str(e)}"
        
        end_time = get_utc_now()
        
        # Write execution results to job log
        with open(log_path, "a", encoding="utf-8") as lf:
            lf.write(output)
            if error_msg:
                lf.write(f"Error: {error_msg}\n")
            lf.write(f"--- Job finished at {end_time} with exit code {exit_code} ---\n")

        # Update Job Status & Retry/DLQ Logic
        conn = get_db_connection()
        try:
            with conn:
                if exit_code == 0:
                    # Successful completion
                    conn.execute(
                        """
                        UPDATE jobs 
                        SET state = 'completed', attempts = attempts + 1, worker_id = NULL, updated_at = ? 
                        WHERE id = ?;
                        """,
                        (end_time, job_id)
                    )
                else:
                    # Failure case
                    new_attempts = job["attempts"] + 1
                    err_info = error_msg or f"Command failed with exit code {exit_code}."
                    
                    if new_attempts > job["max_retries"]:
                        # Move to Dead Letter Queue (DLQ)
                        conn.execute(
                            """
                            UPDATE jobs 
                            SET state = 'dead', attempts = ?, worker_id = NULL, updated_at = ?, error_log = ? 
                            WHERE id = ?;
                            """,
                            (new_attempts, end_time, err_info, job_id)
                        )
                    else:
                        # Retry with exponential backoff
                        backoff_delay = job["backoff_base"] ** new_attempts
                        next_run = (datetime.now(timezone.utc) + timedelta(seconds=backoff_delay)).isoformat()
                        
                        conn.execute(
                            """
                            UPDATE jobs 
                            SET state = 'pending', attempts = ?, worker_id = NULL, updated_at = ?, next_run_at = ?, error_log = ? 
                            WHERE id = ?;
                            """,
                            (new_attempts, end_time, next_run, err_info, job_id)
                        )
        finally:
            conn.close()

    def run(self):
        """Main worker execution loop."""
        self.register()
        try:
            while not self.should_stop:
                self.update_heartbeat()
                if self.should_stop:
                    break
                
                job = self.fetch_next_job()
                if job:
                    self.execute_job(job)
                else:
                    # No job found, sleep briefly to prevent high CPU utilization
                    time.sleep(1.0)
        finally:
            self.unregister()
