import argparse
import sys
import json
import sqlite3
from datetime import datetime, timezone, timedelta
from queuectl.db import init_db, get_db_connection, get_utc_now
from queuectl.config import Config
from queuectl import manager
from queuectl.worker import Worker

def format_relative_time(iso_str: str) -> str:
    """Formats an ISO timestamp to a human-readable relative time."""
    try:
        dt = datetime.fromisoformat(iso_str)
        now = datetime.now(timezone.utc)
        diff = now - dt
        
        # Heartbeat can be slightly in the future due to clock sync
        if diff.total_seconds() < 0:
            return "Just now"
            
        seconds = int(diff.total_seconds())
        if seconds < 5:
            return "Just now"
        elif seconds < 60:
            return f"{seconds}s ago"
        elif seconds < 3600:
            return f"{seconds // 60}m ago"
        else:
            return f"{seconds // 3600}h ago"
    except Exception:
        return "Unknown"

def handle_enqueue(args):
    """Handles 'enqueue' command."""
    try:
        payload = json.loads(args.payload)
    except json.JSONDecodeError as e:
        print(f"Error: Invalid JSON payload. {e}", file=sys.stderr)
        sys.exit(1)
        
    if "id" not in payload:
        print("Error: Job payload must contain an 'id' field.", file=sys.stderr)
        sys.exit(1)
    if "command" not in payload:
        print("Error: Job payload must contain a 'command' field.", file=sys.stderr)
        sys.exit(1)
        
    job_id = payload["id"]
    command = payload["command"]
    
    # Load optional fields or fall back to defaults
    priority = int(payload.get("priority", 1))
    max_retries = int(payload.get("max_retries", Config.get_max_retries()))
    backoff_base = float(payload.get("backoff_base", Config.get_backoff_base()))
    timeout = payload.get("timeout")
    if timeout is not None:
        timeout = int(timeout)
    else:
        timeout = Config.get_default_timeout()
        
    # Calculate next_run_at
    now_str = get_utc_now()
    next_run_at = now_str
    
    if "run_at" in payload:
        next_run_at = payload["run_at"]
    elif "delay" in payload:
        try:
            delay_sec = float(payload["delay"])
            next_run_at = (datetime.now(timezone.utc) + timedelta(seconds=delay_sec)).isoformat()
        except ValueError:
            print("Error: 'delay' must be a number.", file=sys.stderr)
            sys.exit(1)

    # Insert into database
    conn = get_db_connection()
    try:
        with conn:
            conn.execute(
                """
                INSERT INTO jobs (
                    id, command, state, priority, attempts, max_retries, 
                    backoff_base, timeout, created_at, updated_at, next_run_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                """,
                (
                    job_id, command, "pending", priority, 0, max_retries,
                    backoff_base, timeout, now_str, now_str, next_run_at
                )
            )
        print(f"Successfully enqueued job '{job_id}'.")
    except sqlite3.IntegrityError:
        print(f"Error: A job with ID '{job_id}' already exists in the queue.", file=sys.stderr)
        sys.exit(1)
    finally:
        conn.close()

def handle_worker(args):
    """Handles 'worker' subcommands."""
    if args.worker_cmd == "start":
        count = args.count
        if count < 1:
            print("Error: Worker count must be at least 1.", file=sys.stderr)
            sys.exit(1)
        spawned = manager.start_workers(count)
        print(f"Successfully started {spawned} background worker(s).")
        
    elif args.worker_cmd == "stop":
        stopped = manager.stop_workers()
        print(f"Successfully stopped workers (gracefully shut down {stopped} processes).")
        
    elif args.worker_cmd == "run":
        # Internal worker process loop
        worker = Worker()
        worker.run()
        
    else:
        print("Error: Specify a worker command ('start', 'stop').", file=sys.stderr)
        sys.exit(1)

def handle_status(args):
    """Handles 'status' command."""
    conn = get_db_connection()
    try:
        # Fetch counts of jobs by state
        job_counts = {
            "pending": 0,
            "processing": 0,
            "completed": 0,
            "failed": 0,
            "dead": 0
        }
        rows = conn.execute("SELECT state, COUNT(*) as c FROM jobs GROUP BY state;").fetchall()
        for row in rows:
            state = row["state"]
            if state in job_counts:
                job_counts[state] = row["c"]
                
        # Fetch active workers
        workers = conn.execute("SELECT * FROM workers ORDER BY heartbeat DESC;").fetchall()
        
        # Display Status
        print("=" * 20 + " QueueCTL Status " + "=" * 20)
        from queuectl.db import DB_PATH
        print(f"Database Path:  {DB_PATH}")
        print("\nJob Status Summary:")
        print(f"  Pending:    {job_counts['pending']}")
        print(f"  Processing: {job_counts['processing']}")
        print(f"  Completed:  {job_counts['completed']}")
        print(f"  Failed:     {job_counts['failed']} (retryable)")
        print(f"  Dead (DLQ): {job_counts['dead']}")
        
        print(f"\nActive Workers ({len(workers)}):")
        if workers:
            for w in workers:
                rel_time = format_relative_time(w["heartbeat"])
                print(f"  - Worker ID: {w['id'][:8]}... | PID: {w['pid']} | State: {w['state']} | Heartbeat: {rel_time}")
        else:
            print("  No active workers running.")
        print("=" * 57)
    finally:
        conn.close()

def handle_list(args):
    """Handles 'list' command."""
    conn = get_db_connection()
    try:
        query = "SELECT * FROM jobs"
        params = []
        if args.state:
            query += " WHERE state = ?"
            params.append(args.state.lower())
        query += " ORDER BY created_at DESC"
        
        jobs = conn.execute(query, params).fetchall()
        if not jobs:
            print("No jobs found.")
            return
            
        # Format printing
        print(f"{'ID':<15} {'State':<12} {'Priority':<8} {'Attempts':<10} {'Max Retries':<12} {'Next Run At':<25} {'Command'}")
        print("-" * 100)
        for job in jobs:
            # Shorten command if too long
            cmd = job["command"]
            if len(cmd) > 35:
                cmd = cmd[:32] + "..."
            print(f"{job['id']:<15} {job['state']:<12} {job['priority']:<8} {job['attempts']:<10} {job['max_retries']:<12} {job['next_run_at']:<25} {cmd}")
    finally:
        conn.close()

def handle_dlq(args):
    """Handles 'dlq' subcommands."""
    if args.dlq_cmd == "list":
        conn = get_db_connection()
        try:
            jobs = conn.execute("SELECT * FROM jobs WHERE state = 'dead' ORDER BY updated_at DESC;").fetchall()
            if not jobs:
                print("DLQ is empty.")
                return
                
            print(f"{'ID':<15} {'Priority':<8} {'Attempts':<10} {'Failed At':<25} {'Error Log'}")
            print("-" * 100)
            for job in jobs:
                err_log = job["error_log"] or "N/A"
                if len(err_log) > 40:
                    err_log = err_log[:37] + "..."
                print(f"{job['id']:<15} {job['priority']:<8} {job['attempts']:<10} {job['updated_at']:<25} {err_log}")
        finally:
            conn.close()
            
    elif args.dlq_cmd == "retry":
        job_id = args.job_id
        conn = get_db_connection()
        try:
            with conn:
                row = conn.execute("SELECT state FROM jobs WHERE id = ?;", (job_id,)).fetchone()
                if not row:
                    print(f"Error: Job '{job_id}' not found.", file=sys.stderr)
                    sys.exit(1)
                if row["state"] != "dead":
                    print(f"Error: Job '{job_id}' is not in DLQ (current state: '{row['state']}').", file=sys.stderr)
                    sys.exit(1)
                    
                # Reset job properties for retry
                conn.execute(
                    """
                    UPDATE jobs 
                    SET state = 'pending', attempts = 0, worker_id = NULL, 
                        updated_at = ?, next_run_at = ?, error_log = NULL 
                    WHERE id = ?;
                    """,
                    (get_utc_now(), get_utc_now(), job_id)
                )
            print(f"Successfully rescheduled job '{job_id}' from DLQ for immediate execution.")
        finally:
            conn.close()
    else:
        print("Error: Specify a DLQ command ('list', 'retry').", file=sys.stderr)
        sys.exit(1)

def handle_config(args):
    """Handles 'config' command."""
    if args.config_cmd == "set":
        key = args.key.lower().replace("-", "_")
        val = args.value
        
        # Validation
        if key == "max_retries":
            try:
                val = int(val)
                if val < 0:
                    raise ValueError
                Config.set_max_retries(val)
            except ValueError:
                print("Error: 'max-retries' must be a non-negative integer.", file=sys.stderr)
                sys.exit(1)
        elif key == "backoff_base":
            try:
                val = float(val)
                if val <= 0:
                    raise ValueError
                Config.set_backoff_base(val)
            except ValueError:
                print("Error: 'backoff-base' must be a positive float.", file=sys.stderr)
                sys.exit(1)
        elif key == "default_timeout":
            try:
                val = int(val)
                if val <= 0:
                    raise ValueError
                Config.set_default_timeout(val)
            except ValueError:
                print("Error: 'default-timeout' must be a positive integer.", file=sys.stderr)
                sys.exit(1)
        else:
            print(f"Error: Unknown configuration key '{args.key}'. Available: 'max-retries', 'backoff-base', 'default-timeout'.", file=sys.stderr)
            sys.exit(1)
            
        print(f"Successfully set config '{args.key}' to '{val}'.")
    else:
        # Display all configuration values
        print("QueueCTL Configuration:")
        print(f"  max-retries:     {Config.get_max_retries()}")
        print(f"  backoff-base:    {Config.get_backoff_base()}")
        print(f"  default-timeout: {Config.get_default_timeout()} seconds")

def main():
    """Main CLI parser entrypoint."""
    # Ensure database is initialized before any operation
    init_db()
    
    parser = argparse.ArgumentParser(
        description="QueueCTL: A CLI-based background job queue system.",
        prog="queuectl"
    )
    
    subparsers = parser.add_subparsers(dest="command", required=True)
    
    # Enqueue Command
    enqueue_parser = subparsers.add_parser("enqueue", help="Add a new job to the queue")
    enqueue_parser.add_argument("payload", help="JSON string of the job specification. E.g. '{\"id\":\"job1\",\"command\":\"sleep 2\"}'")
    
    # Worker Command
    worker_parser = subparsers.add_parser("worker", help="Manage worker processes")
    worker_subparsers = worker_parser.add_subparsers(dest="worker_cmd", required=True)
    
    worker_start = worker_subparsers.add_parser("start", help="Start one or more background workers")
    worker_start.add_argument("--count", type=int, default=1, help="Number of workers to start (default: 1)")
    
    worker_subparsers.add_parser("stop", help="Stop running workers gracefully")
    worker_subparsers.add_parser("run", help=argparse.SUPPRESS)  # Internal run loop command
    
    # Status Command
    subparsers.add_parser("status", help="Show summary of all job states & active workers")
    
    # List Command
    list_parser = subparsers.add_parser("list", help="List jobs by state")
    list_parser.add_argument("--state", choices=["pending", "processing", "completed", "failed", "dead"], help="Filter by state")
    
    # DLQ Command
    dlq_parser = subparsers.add_parser("dlq", help="View or retry DLQ jobs")
    dlq_subparsers = dlq_parser.add_subparsers(dest="dlq_cmd", required=True)
    dlq_subparsers.add_parser("list", help="List all jobs in Dead Letter Queue (DLQ)")
    
    dlq_retry = dlq_subparsers.add_parser("retry", help="Retry a specific dead job")
    dlq_retry.add_argument("job_id", help="The ID of the dead job to retry")
    
    # Config Command
    config_parser = subparsers.add_parser("config", help="Manage configuration parameters")
    config_subparsers = config_parser.add_subparsers(dest="config_cmd")
    
    config_set = config_subparsers.add_parser("set", help="Set a configuration parameter")
    config_set.add_argument("key", choices=["max-retries", "backoff-base", "default-timeout"], help="Config key to set")
    config_set.add_argument("value", help="Value to set")
    
    args = parser.parse_args()
    
    # Route execution
    if args.command == "enqueue":
        handle_enqueue(args)
    elif args.command == "worker":
        handle_worker(args)
    elif args.command == "status":
        handle_status(args)
    elif args.command == "list":
        handle_list(args)
    elif args.command == "dlq":
        handle_dlq(args)
    elif args.command == "config":
        handle_config(args)

if __name__ == "__main__":
    main()
