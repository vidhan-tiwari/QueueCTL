import os
import sys
import subprocess
import time
from queuectl.db import get_db_connection

def start_workers(count: int):
    """Spawns N background worker processes running 'queuectl worker run'."""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    workspace_dir = os.path.dirname(script_dir)
    
    cmd = [sys.executable, "-m", "queuectl.main", "worker", "run"]
    
    creation_flags = 0
    if sys.platform == "win32":
        creation_flags = subprocess.CREATE_NEW_PROCESS_GROUP | subprocess.DETACHED_PROCESS

    spawned = 0
    env = os.environ.copy()
    env["PYTHONPATH"] = workspace_dir
    for _ in range(count):
        try:
            # Start process in the background
            subprocess.Popen(
                cmd,
                creationflags=creation_flags,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                close_fds=True,
                cwd=workspace_dir,
                env=env
            )
            spawned += 1
        except Exception as e:
            print(f"Error spawning worker process: {e}", file=sys.stderr)
    return spawned

def stop_workers():
    """Signals all running workers to shutdown gracefully and waits for them to exit."""
    conn = get_db_connection()
    try:
        # Get count of active workers
        active_workers = conn.execute("SELECT id, pid FROM workers;").fetchall()
        if not active_workers:
            return 0
        
        # Set database state to shutting_down
        with conn:
            conn.execute("UPDATE workers SET state = 'shutting_down';")
            
    finally:
        conn.close()
        
    print(f"Signaled {len(active_workers)} worker(s) to shut down gracefully...")
    
    # Wait for workers to clean up and unregister
    timeout = 15.0  # 15 seconds max wait time
    start_time = time.time()
    
    while time.time() - start_time < timeout:
        conn = get_db_connection()
        try:
            remaining = conn.execute("SELECT id, pid, state FROM workers;").fetchall()
            if not remaining:
                print("All workers stopped successfully.")
                return len(active_workers)
            
            # Print status update
            print(f"Waiting for {len(remaining)} worker(s) to finish current jobs...", end="\r")
            time.sleep(0.5)
        finally:
            conn.close()
            
    # If timeout exceeded, list remaining workers
    conn = get_db_connection()
    try:
        remaining = conn.execute("SELECT id, pid FROM workers;").fetchall()
        if remaining:
            print(f"\nGraceful shutdown timed out. {len(remaining)} worker(s) are still active:")
            for r in remaining:
                print(f"  - Worker ID: {r['id']}, PID: {r['pid']}")
            print("Note: They will exit after finishing their current jobs.")
    finally:
        conn.close()
        
    return len(active_workers) - len(remaining)
