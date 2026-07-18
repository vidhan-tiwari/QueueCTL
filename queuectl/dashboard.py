import http.server
import json
import sqlite3
import sys
import os
from queuectl.db import get_db_connection, DB_PATH

HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>QueueCTL Dashboard</title>
    <style>
        :root {{
            --bg-color: #0f172a;
            --card-bg: #1e293b;
            --text-primary: #f8fafc;
            --text-secondary: #94a3b8;
            --primary: #6366f1;
            --primary-hover: #4f46e5;
            --border: #334155;
            
            --state-pending: #e2e8f0;
            --state-processing: #38bdf8;
            --state-completed: #4ade80;
            --state-failed: #fb7185;
            --state-dead: #f43f5e;
        }}
        
        * {{
            box-sizing: border-box;
            margin: 0;
            padding: 0;
        }}
        
        body {{
            background-color: var(--bg-color);
            color: var(--text-primary);
            font-family: 'Inter', system-ui, -apple-system, sans-serif;
            padding: 2rem;
            line-height: 1.5;
        }}
        
        .container {{
            max-width: 1200px;
            margin: 0 auto;
        }}
        
        header {{
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 2rem;
            border-bottom: 1px solid var(--border);
            padding-bottom: 1rem;
        }}
        
        h1 {{
            font-size: 2rem;
            font-weight: 700;
            background: linear-gradient(to right, #818cf8, #c084fc);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
        }}
        
        .db-path {{
            font-size: 0.85rem;
            color: var(--text-secondary);
            font-family: monospace;
            background: #020617;
            padding: 0.5rem 1rem;
            border-radius: 6px;
            border: 1px solid var(--border);
        }}
        
        .stats-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
            gap: 1rem;
            margin-bottom: 2rem;
        }}
        
        .stat-card {{
            background-color: var(--card-bg);
            border: 1px solid var(--border);
            border-radius: 12px;
            padding: 1.5rem;
            text-align: center;
            transition: transform 0.2s, box-shadow 0.2s;
        }}
        
        .stat-card:hover {{
            transform: translateY(-2px);
            box-shadow: 0 10px 15px -3px rgba(0, 0, 0, 0.3);
        }}
        
        .stat-val {{
            font-size: 2.25rem;
            font-weight: 700;
            margin-bottom: 0.25rem;
        }}
        
        .stat-lbl {{
            font-size: 0.875rem;
            text-transform: uppercase;
            letter-spacing: 0.05em;
            color: var(--text-secondary);
            font-weight: 600;
        }}
        
        .main-grid {{
            display: grid;
            grid-template-columns: 1fr;
            gap: 2rem;
        }}
        
        @media(min-width: 1024px) {{
            .main-grid {{
                grid-template-columns: 2fr 1fr;
            }}
        }}
        
        .panel {{
            background-color: var(--card-bg);
            border: 1px solid var(--border);
            border-radius: 12px;
            padding: 1.5rem;
        }}
        
        .panel-title {{
            font-size: 1.25rem;
            font-weight: 600;
            margin-bottom: 1rem;
            border-bottom: 1px solid var(--border);
            padding-bottom: 0.5rem;
        }}
        
        table {{
            width: 100%;
            border-collapse: collapse;
            font-size: 0.9rem;
        }}
        
        th, td {{
            padding: 0.75rem 1rem;
            text-align: left;
            border-bottom: 1px solid var(--border);
        }}
        
        th {{
            color: var(--text-secondary);
            font-weight: 600;
            text-transform: uppercase;
            font-size: 0.75rem;
            letter-spacing: 0.05em;
        }}
        
        tr:hover td {{
            background-color: rgba(255, 255, 255, 0.02);
        }}
        
        .badge {{
            display: inline-block;
            padding: 0.25rem 0.5rem;
            border-radius: 4px;
            font-size: 0.75rem;
            font-weight: 600;
            text-transform: uppercase;
        }}
        
        .badge-pending {{ background: rgba(226, 232, 240, 0.1); color: var(--state-pending); }}
        .badge-processing {{ background: rgba(56, 189, 248, 0.1); color: var(--state-processing); }}
        .badge-completed {{ background: rgba(74, 222, 128, 0.1); color: var(--state-completed); }}
        .badge-failed {{ background: rgba(251, 113, 133, 0.1); color: var(--state-failed); }}
        .badge-dead {{ background: rgba(244, 63, 94, 0.1); color: var(--state-dead); }}
        
        .code-font {{
            font-family: monospace;
            background: #020617;
            padding: 0.2rem 0.4rem;
            border-radius: 4px;
            font-size: 0.85rem;
        }}
        
        .worker-item {{
            display: flex;
            justify-content: space-between;
            align-items: center;
            padding: 0.75rem 0;
            border-bottom: 1px solid var(--border);
        }}
        
        .worker-item:last-child {{
            border-bottom: none;
        }}
        
        .worker-info {{
            display: flex;
            flex-direction: column;
        }}
        
        .worker-id {{
            font-weight: 600;
            font-size: 0.95rem;
        }}
        
        .worker-meta {{
            font-size: 0.75rem;
            color: var(--text-secondary);
            margin-top: 0.25rem;
        }}
        
        .empty-state {{
            text-align: center;
            color: var(--text-secondary);
            padding: 2rem 0;
            font-size: 0.95rem;
        }}
        
        .auto-refresh {{
            font-size: 0.8rem;
            color: var(--text-secondary);
            display: flex;
            align-items: center;
            gap: 0.5rem;
        }}
        
        .pulse {{
            width: 8px;
            height: 8px;
            background-color: var(--state-completed);
            border-radius: 50%;
            display: inline-block;
            box-shadow: 0 0 0 0 rgba(74, 222, 128, 0.7);
            animation: pulse 1.6s infinite;
        }}
        
        @keyframes pulse {{
            0% {{
                transform: scale(0.95);
                box-shadow: 0 0 0 0 rgba(74, 222, 128, 0.7);
            }}
            70% {{
                transform: scale(1);
                box-shadow: 0 0 0 6px rgba(74, 222, 128, 0);
            }}
            100% {{
                transform: scale(0.95);
                box-shadow: 0 0 0 0 rgba(74, 222, 128, 0);
            }}
        }}
    </style>
    <script>
        // Auto-refresh the page every 3 seconds to keep monitoring statistics fresh
        setInterval(() => {{
            window.location.reload();
        }}, 3000);
    </script>
</head>
<body>
    <div class="container">
        <header>
            <div>
                <h1>QueueCTL</h1>
                <div class="auto-refresh" style="margin-top: 0.25rem;">
                    <span class="pulse"></span> Monitoring active (auto-refreshes every 3s)
                </div>
            </div>
            <div class="db-path">DB: {db_path}</div>
        </header>
        
        <div class="stats-grid">
            <div class="stat-card" style="border-top: 4px solid var(--state-pending)">
                <div class="stat-val" style="color: var(--state-pending)">{cnt_pending}</div>
                <div class="stat-lbl">Pending</div>
            </div>
            <div class="stat-card" style="border-top: 4px solid var(--state-processing)">
                <div class="stat-val" style="color: var(--state-processing)">{cnt_processing}</div>
                <div class="stat-lbl">Processing</div>
            </div>
            <div class="stat-card" style="border-top: 4px solid var(--state-completed)">
                <div class="stat-val" style="color: var(--state-completed)">{cnt_completed}</div>
                <div class="stat-lbl">Completed</div>
            </div>
            <div class="stat-card" style="border-top: 4px solid var(--state-failed)">
                <div class="stat-val" style="color: var(--state-failed)">{cnt_failed}</div>
                <div class="stat-lbl">Failed</div>
            </div>
            <div class="stat-card" style="border-top: 4px solid var(--state-dead)">
                <div class="stat-val" style="color: var(--state-dead)">{cnt_dead}</div>
                <div class="stat-lbl">Dead (DLQ)</div>
            </div>
        </div>
        
        <div class="main-grid">
            <!-- Jobs Panel -->
            <div class="panel">
                <div class="panel-title">Recent Jobs (Latest 50)</div>
                <div style="overflow-x: auto;">
                    {jobs_table}
                </div>
            </div>
            
            <!-- Workers Panel -->
            <div class="panel">
                <div class="panel-title">Active Workers ({cnt_workers})</div>
                <div>
                    {workers_list}
                </div>
            </div>
        </div>
    </div>
</body>
</html>
"""

class DashboardHandler(http.server.BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        # Silence server log output in CLI
        return

    def do_GET(self):
        if self.path == "/":
            self.send_response(200)
            self.send_header("Content-type", "text/html")
            self.end_headers()
            
            # Fetch data from DB
            conn = get_db_connection()
            try:
                # Job counts
                cnt_pending = conn.execute("SELECT COUNT(*) FROM jobs WHERE state = 'pending';").fetchone()[0]
                cnt_processing = conn.execute("SELECT COUNT(*) FROM jobs WHERE state = 'processing';").fetchone()[0]
                cnt_completed = conn.execute("SELECT COUNT(*) FROM jobs WHERE state = 'completed';").fetchone()[0]
                cnt_failed = conn.execute("SELECT COUNT(*) FROM jobs WHERE state = 'failed';").fetchone()[0]
                cnt_dead = conn.execute("SELECT COUNT(*) FROM jobs WHERE state = 'dead';").fetchone()[0]
                
                # Active workers
                workers = conn.execute("SELECT * FROM workers ORDER BY heartbeat DESC;").fetchall()
                cnt_workers = len(workers)
                
                # Latest jobs
                jobs = conn.execute("SELECT * FROM jobs ORDER BY created_at DESC LIMIT 50;").fetchall()
            finally:
                conn.close()
                
            # Build jobs table HTML
            if jobs:
                jobs_html = """<table>
                    <thead>
                        <tr>
                            <th>ID</th>
                            <th>Command</th>
                            <th>State</th>
                            <th>Priority</th>
                            <th>Attempts</th>
                        </tr>
                    </thead>
                    <tbody>"""
                for j in jobs:
                    badge_class = f"badge badge-{j['state']}"
                    cmd_safe = j['command']
                    if len(cmd_safe) > 50:
                        cmd_safe = cmd_safe[:47] + "..."
                    jobs_html += f"""
                        <tr>
                            <td>{j['id']}</td>
                            <td><span class="code-font">{cmd_safe}</span></td>
                            <td><span class="{badge_class}">{j['state']}</span></td>
                            <td>{j['priority']}</td>
                            <td>{j['attempts']}/{j['max_retries']}</td>
                        </tr>"""
                jobs_html += "</tbody></table>"
            else:
                jobs_html = '<div class="empty-state">No jobs in queue database.</div>'
                
            # Build workers list HTML
            if workers:
                workers_html = ""
                for w in workers:
                    workers_html += f"""
                        <div class="worker-item">
                            <div class="worker-info">
                                <span class="worker-id">Worker {w['id'][:8]}</span>
                                <span class="worker-meta">PID: {w['pid']} | State: {w['state']}</span>
                            </div>
                            <span class="badge badge-processing">active</span>
                        </div>"""
            else:
                workers_html = '<div class="empty-state">No active workers running.</div>'
                
            # Render page
            page = HTML_TEMPLATE.format(
                db_path=DB_PATH,
                cnt_pending=cnt_pending,
                cnt_processing=cnt_processing,
                cnt_completed=cnt_completed,
                cnt_failed=cnt_failed,
                cnt_dead=cnt_dead,
                cnt_workers=cnt_workers,
                jobs_table=jobs_html,
                workers_list=workers_html
            )
            self.wfile.write(page.encode("utf-8"))
        else:
            self.send_error(404, "File Not Found")

def start_dashboard(port=8000):
    """Starts the built-in HTTP server hosting the monitor dashboard."""
    server_address = ("", port)
    httpd = http.server.HTTPServer(server_address, DashboardHandler)
    print(f"=====================================================")
    print(f"QueueCTL Monitor Dashboard active!")
    print(f"URL: http://localhost:{port}/")
    print(f"Press Ctrl+C to stop the dashboard server.")
    print(f"=====================================================")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\nStopping dashboard server...")
        sys.exit(0)
