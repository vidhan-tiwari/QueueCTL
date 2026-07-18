import os
import sqlite3
import json
from datetime import datetime, timezone
import time

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
WORKSPACE_DIR = os.path.dirname(SCRIPT_DIR)
DB_DIR = os.path.join(WORKSPACE_DIR, ".queuectl")
DB_PATH = os.path.join(DB_DIR, "queuectl.db")
LOGS_DIR = os.path.join(DB_DIR, "logs")

def get_db_connection():
    """Returns a sqlite3 connection in WAL mode with a busy timeout."""
    os.makedirs(DB_DIR, exist_ok=True)
    os.makedirs(LOGS_DIR, exist_ok=True)
    conn = sqlite3.connect(DB_PATH, timeout=10.0)
    # Enable Write-Ahead Logging (WAL) for better concurrency
    conn.execute("PRAGMA journal_mode=WAL;")
    # Use foreign keys
    conn.execute("PRAGMA foreign_keys = ON;")
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    """Initializes the SQLite database tables."""
    conn = get_db_connection()
    try:
        with conn:
            # Jobs table
            conn.execute("""
            CREATE TABLE IF NOT EXISTS jobs (
                id TEXT PRIMARY KEY,
                command TEXT NOT NULL,
                state TEXT NOT NULL CHECK(state IN ('pending', 'processing', 'completed', 'failed', 'dead')),
                priority INTEGER NOT NULL DEFAULT 1,
                attempts INTEGER NOT NULL DEFAULT 0,
                max_retries INTEGER NOT NULL,
                backoff_base REAL NOT NULL,
                timeout INTEGER,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                next_run_at TEXT NOT NULL,
                worker_id TEXT,
                error_log TEXT,
                FOREIGN KEY(worker_id) REFERENCES workers(id) ON DELETE SET NULL
            );
            """)

            # Workers table
            conn.execute("""
            CREATE TABLE IF NOT EXISTS workers (
                id TEXT PRIMARY KEY,
                pid INTEGER NOT NULL,
                state TEXT NOT NULL CHECK(state IN ('running', 'shutting_down', 'stopped')),
                heartbeat TEXT NOT NULL
            );
            """)

            # Config table
            conn.execute("""
            CREATE TABLE IF NOT EXISTS config (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
            """)

            # Default configuration insertion
            defaults = {
                "max_retries": "3",
                "backoff_base": "2.0",
                "default_timeout": "300"  # 5 minutes
            }
            for k, v in defaults.items():
                conn.execute(
                    "INSERT OR IGNORE INTO config (key, value) VALUES (?, ?);", 
                    (k, v)
                )
    finally:
        conn.close()

def get_config(key, default=None):
    """Retrieves a configuration value from the config table."""
    conn = get_db_connection()
    try:
        row = conn.execute("SELECT value FROM config WHERE key = ?;", (key,)).fetchone()
        if row:
            return row["value"]
        return default
    finally:
        conn.close()

def set_config(key, value):
    """Sets a configuration value in the config table."""
    conn = get_db_connection()
    try:
        with conn:
            conn.execute(
                "INSERT OR REPLACE INTO config (key, value) VALUES (?, ?);", 
                (key, str(value))
            )
    finally:
        conn.close()

def get_utc_now():
    """Helper to return current UTC ISO 8601 string."""
    return datetime.now(timezone.utc).isoformat()
