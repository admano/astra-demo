#!/usr/bin/env python3
"""
portal.py — Unified ASTRA Demo Portal
--------------------------------------
Starts all 12 pipeline phases as background processes and serves a
single web portal on http://localhost:8080 with sidebar navigation
and live phase-status indicators.

    python3 portal.py

Press Ctrl-C to stop everything.
"""

from __future__ import annotations

import http.server
import json
import os
import signal
import socket
import subprocess
import sys
import threading
import time
from pathlib import Path

# ─────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────

DEMO_DIR    = Path(__file__).parent
PORTAL_PORT = 8080

# Phases enabled for demo interaction (others shown but disabled)
DEMO_ACTIVE_PHASES: set[int] = {1, 2, 3, 4}

PHASES: list[tuple[int, str, int, str]] = [
    (1,  "Reception",         8000, "reception.py"),
    (2,  "Ingestion",         8001, "ingestion.py"),
    (3,  "Security",          8002, "security.py"),
    (4,  "Privacy",           8003, "privacy/privacy.py"),
    
   (5,  "Analysis",          8004, "analysis.py"),
   (6,  "Decomposition",     8005, "decomposition.py"),
   (7,  "Prompt Enrichment", 8006, "prompt_enrichment.py"),
   (8,  "Response",          8007, "response.py"),
   (9,  "Quality",           8008, "quality.py"),
   (10, "Recomposition",     8009, "recomposition.py"),
   (11, "Validation",        8010, "validation.py"),
   (12, "Dispatch",          8011, "dispatch.py"),
]

PHASE_GROUPS = [
    ("Input",      [1, 2, 3]),
    ("Processing", [4, 5, 6, 7]),
    ("AI",         [8, 9]),
    ("Output",     [10, 11, 12]),
]

# ─────────────────────────────────────────────────────────────
# GLOBAL STATE
# ─────────────────────────────────────────────────────────────

_processes:    list[subprocess.Popen] = []
_phase_status: dict[int, str]         = {p[0]: "starting" for p in PHASES}
_phase_by_num: dict[int, tuple]       = {p[0]: p for p in PHASES}

# ─────────────────────────────────────────────────────────────
# PHASE MANAGEMENT
# ─────────────────────────────────────────────────────────────

def _is_port_open(port: int) -> bool:
    try:
        with socket.create_connection(("127.0.0.1", port), timeout=0.5):
            return True
    except OSError:
        return False


def _kill_port_occupant(port: int) -> None:
    """Kill any process currently listening on the given port (macOS/Linux)."""
    try:
        result = subprocess.run(
            ["lsof", "-ti", f"tcp:{port}"],
            capture_output=True, text=True,
        )
        for pid_str in result.stdout.strip().splitlines():
            pid = int(pid_str.strip())
            try:
                os.kill(pid, signal.SIGTERM)
                time.sleep(0.3)
                # Force-kill if still alive
                try:
                    os.kill(pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass  # already gone
                print(f"    ↳ killed stale process PID {pid} on port {port}")
            except (ProcessLookupError, PermissionError):
                pass
    except FileNotFoundError:
        pass  # lsof not available


def start_phases() -> None:
    env = os.environ.copy()
    for num, name, port, script in PHASES:
        if num not in DEMO_ACTIVE_PHASES:
            _phase_status[num] = "inactive"
            print(f"  ○ Phase {num:02d} {name:<20} — skipped (demo inactive)")
            continue
        script_path = DEMO_DIR / script
        if not script_path.exists():
            print(f"  ✗ Phase {num:02d} {name:<20} — script not found: {script_path}")
            _phase_status[num] = "missing"
            continue
        # Evict any stale process holding this port before starting fresh
        if _is_port_open(port):
            _kill_port_occupant(port)
            time.sleep(0.5)
        proc = subprocess.Popen(
            [sys.executable, str(script_path)],
            env=env,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            cwd=str(DEMO_DIR),
        )
        _processes.append(proc)
        print(f"  ✓ Phase {num:02d} {name:<20} started (PID {proc.pid}, port {port})")


def _status_checker() -> None:
    """Background thread: polls each phase port every 3 s."""
    while True:
        for num, _name, port, _script in PHASES:
            if _phase_status.get(num) in ("missing", "inactive"):
                continue
            _phase_status[num] = "up" if _is_port_open(port) else "down"
        time.sleep(3)


def shutdown_all() -> None:
    print("\n  Stopping all phases…")
    for proc in _processes:
        try:
            proc.terminate()
        except Exception:
            pass
    # Give them a moment, then kill stragglers
    time.sleep(1)
    for proc in _processes:
        try:
            if proc.poll() is None:
                proc.kill()
        except Exception:
            pass
    print("  Done.")


# ─────────────────────────────────────────────────────────────
# HTML TEMPLATES  (loaded from templates/ folder)
# ─────────────────────────────────────────────────────────────

TEMPLATES_DIR = DEMO_DIR / "templates"


def _load_template(filename: str) -> str:
    return (TEMPLATES_DIR / filename).read_text(encoding="utf-8")


def _extract_step_tmpl(landing_raw: str) -> str:
    """Pull the pipeline_step_tmpl block out of the HTML comment in landing.html."""
    start = landing_raw.index("<!-- pipeline_step_tmpl") + len("<!-- pipeline_step_tmpl")
    end   = landing_raw.index("-->", start)
    return landing_raw[start:end].strip()


def _landing_body(landing_raw: str) -> str:
    """Return only the visible part of landing.html (before the comment)."""
    return landing_raw[:landing_raw.index("<!-- pipeline_step_tmpl")].rstrip()

def _dot_color(num: int) -> str:
    s = _phase_status.get(num, "starting")
    return {"up": "#22c55e", "down": "#ef4444", "starting": "#f59e0b", "missing": "#6b7280", "inactive": "#2d333b"}.get(s, "#f59e0b")


def _build_sidebar_html() -> str:
    parts: list[str] = []
    for group_name, nums in PHASE_GROUPS:
        parts.append(f'<div class="grp-hdr">{group_name}</div>')
        for num in nums:
            _, name, port, _ = _phase_by_num[num]
            status = _phase_status.get(num, "starting")
            disabled = "" if num in DEMO_ACTIVE_PHASES else " disabled"
            extra_cls = "" if num in DEMO_ACTIVE_PHASES else " inactive-phase"
            parts.append(
                f'<button class="phase-btn{disabled}{extra_cls}" data-port="{port}" '
                f'onclick="loadPhase({port},{num},\'{name}\')">'
                f'<span class="num">{num:02d}</span>'
                f'<span class="pname">{name}</span>'
                f'<span class="dot {status}" data-dot="{num}"></span>'
                f'</button>'
            )
    return "\n".join(parts)


def _build_landing_html() -> str:
    landing_raw  = _load_template("landing.html")
    step_tmpl    = _extract_step_tmpl(landing_raw)
    landing_body = _landing_body(landing_raw)
    steps = []
    for num, name, port, _ in PHASES:
        color = _dot_color(num)
        if num in DEMO_ACTIVE_PHASES:
            step = step_tmpl.format(num=num, name=name, port=port, dot_color=color)
        else:
            step = step_tmpl.format(num=num, name=name, port=port, dot_color=color).replace(
                'cursor:pointer;"',
                'cursor:not-allowed;opacity:0.35;pointer-events:none;"',
            )
        steps.append(step)
    return landing_body.format(pipeline_steps="\n".join(steps))


def _build_portal_html() -> str:
    from string import Template
    sidebar = _build_sidebar_html()
    landing = _build_landing_html()
    # Escape for srcdoc attribute (double-quotes only — single quotes are fine in HTML)
    landing_escaped = landing.replace("&", "&amp;").replace('"', "&quot;")
    return Template(_load_template("portal.html")).substitute(
        sidebar_html=sidebar,
        landing_srcdoc=landing_escaped,
    )


# ─────────────────────────────────────────────────────────────
# HTTP HANDLER
# ─────────────────────────────────────────────────────────────

class PortalHandler(http.server.BaseHTTPRequestHandler):

    def log_message(self, *args: object) -> None:
        pass  # silence access log

    def do_GET(self) -> None:
        if self.path == "/status":
            self._serve_status()
        elif self.path in ("/", "/favicon.ico"):
            if self.path == "/favicon.ico":
                self.send_response(204); self.end_headers(); return
            self._serve_portal()
        else:
            self.send_response(404)
            self.end_headers()

    def _serve_portal(self) -> None:
        html = _build_portal_html()
        data = html.encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _serve_status(self) -> None:
        payload = json.dumps(_phase_status).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)


# ─────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────

def main() -> None:
    print("\n" + "═" * 62)
    print("  ASTRA — Unified Demo Portal")
    print("═" * 62)

    # Launch all phase processes
    print("\n  Starting pipeline phases…\n")
    start_phases()

    # Background status checker
    checker = threading.Thread(target=_status_checker, daemon=True)
    checker.start()

    # Start portal HTTP server
    server = http.server.ThreadingHTTPServer(("", PORTAL_PORT), PortalHandler)
    srv_thread = threading.Thread(target=server.serve_forever, daemon=True)
    srv_thread.start()

    print(f"\n  ✓  Portal ready:  http://localhost:{PORTAL_PORT}")
    print(f"\n  Phase dashboards load in the sidebar — wait for green dots.")
    print(f"  Press Ctrl-C to stop all phases and the portal.\n")
    print("─" * 62)

    # Graceful shutdown on Ctrl-C
    def _sigint(sig, frame):  # type: ignore[override]
        server.shutdown()
        shutdown_all()
        sys.exit(0)

    signal.signal(signal.SIGINT, _sigint)
    threading.Event().wait()


if __name__ == "__main__":
    main()
