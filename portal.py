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

PHASES: list[tuple[int, str, int, str]] = [
    (1,  "Reception",         8000, "reception.py"),
    (2,  "Ingestion",         8001, "ingestion.py"),
    (3,  "Security",          8002, "security.py"),
    (4,  "Privacy",           8003, "privacy.py"),
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
            if _phase_status.get(num) == "missing":
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
# HTML TEMPLATES
# ─────────────────────────────────────────────────────────────

LANDING_HTML = """
<div style="display:flex;flex-direction:column;align-items:center;justify-content:center;
            height:100%;gap:32px;padding:40px;color:#c9d1d9;font-family:system-ui,sans-serif;">
  <div style="text-align:center;">
    <div style="font-size:3rem;font-weight:700;letter-spacing:0.1em;
                background:linear-gradient(135deg,#7c9ef0,#a78bfa);
                -webkit-background-clip:text;-webkit-text-fill-color:transparent;">
      ASTRA
    </div>
    <div style="font-size:1rem;color:#8b949e;margin-top:4px;">
      Automated Structured Treatment &amp; Response Architecture
    </div>
    <div style="font-size:0.82rem;color:#484f58;margin-top:2px;">Demo Pipeline — Unified Portal</div>
  </div>

  <!-- pipeline flow -->
  <div style="display:flex;flex-wrap:wrap;gap:8px;justify-content:center;max-width:780px;">
    {pipeline_steps}
  </div>

  <div style="font-size:0.82rem;color:#484f58;text-align:center;max-width:480px;">
    Select a phase in the sidebar to open its dashboard.<br>
    All phases start automatically — wait for the status dots to turn green.
  </div>
</div>
"""

PIPELINE_STEP_TMPL = """
<div style="display:flex;flex-direction:column;align-items:center;gap:4px;
            padding:12px 16px;background:#161b22;border:1px solid #30363d;
            border-radius:8px;min-width:120px;cursor:pointer;"
     onclick="top.loadPhase({port})">
  <div style="font-size:0.68rem;color:#484f58;font-weight:600;">PHASE {num:02d}</div>
  <div style="font-size:0.82rem;font-weight:500;color:#c9d1d9;">{name}</div>
  <div id="step-dot-{num}" style="width:8px;height:8px;border-radius:50%;
       background:{dot_color};margin-top:2px;"></div>
</div>
"""

PORTAL_HTML_TMPL = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>ASTRA — Demo Portal</title>
<style>
  *{{box-sizing:border-box;margin:0;padding:0}}
  html,body{{height:100%;overflow:hidden;background:#0d1117;font-family:system-ui,sans-serif}}

  /* ── Header ── */
  #hdr{{
    display:flex;align-items:center;gap:12px;
    padding:0 20px;height:48px;flex-shrink:0;
    background:#161b22;border-bottom:1px solid #21262d;
  }}
  #hdr .logo{{font-size:1.05rem;font-weight:700;letter-spacing:.08em;
              background:linear-gradient(135deg,#7c9ef0,#a78bfa);
              -webkit-background-clip:text;-webkit-text-fill-color:transparent}}
  #hdr .tagline{{font-size:0.75rem;color:#484f58}}
  #hdr .spacer{{flex:1}}
  #hdr .phase-label{{font-size:0.78rem;color:#8b949e;
    padding:3px 10px;background:#21262d;border-radius:4px}}

  /* ── Layout ── */
  #shell{{display:flex;height:calc(100vh - 48px)}}

  /* ── Sidebar ── */
  #sb{{
    width:210px;flex-shrink:0;
    background:#0d1117;border-right:1px solid #21262d;
    display:flex;flex-direction:column;overflow-y:auto;
  }}
  .grp-hdr{{
    padding:12px 14px 4px;font-size:0.65rem;
    font-weight:700;letter-spacing:.1em;
    color:#484f58;text-transform:uppercase;
  }}
  .phase-btn{{
    display:flex;align-items:center;gap:9px;
    padding:9px 14px;cursor:pointer;border:none;
    background:transparent;width:100%;text-align:left;
    color:#8b949e;transition:background .12s;
    border-left:3px solid transparent;
  }}
  .phase-btn:hover{{background:#161b22;color:#c9d1d9}}
  .phase-btn.active{{
    background:#1c2333;border-left-color:#7c9ef0;color:#e6edf3;
  }}
  .phase-btn .num{{
    font-size:0.65rem;color:#484f58;width:22px;flex-shrink:0;font-weight:600;
  }}
  .phase-btn.active .num{{color:#7c9ef0}}
  .phase-btn .pname{{font-size:0.82rem;flex:1;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}}
  .dot{{
    width:7px;height:7px;border-radius:50%;flex-shrink:0;
    background:#484f58;transition:background .3s;
  }}
  .dot.up{{background:#22c55e}}
  .dot.down{{background:#ef4444}}
  .dot.starting{{background:#f59e0b}}
  .dot.missing{{background:#6b7280}}

  /* ── Home button ── */
  #home-btn{{
    display:flex;align-items:center;gap:9px;
    padding:11px 14px;cursor:pointer;border:none;
    background:#161b22;width:100%;text-align:left;
    color:#7c9ef0;border-bottom:1px solid #21262d;
    border-left:3px solid #7c9ef0;font-size:0.82rem;font-weight:600;
    transition:background .12s;flex-shrink:0;
  }}
  #home-btn:hover{{background:#1c2333}}
  #home-btn svg{{flex-shrink:0}}

  /* ── Content ── */
  #content{{flex:1;display:flex;flex-direction:column}}
  #landing{{flex:1;background:#0d1117;display:block}}
  #phase-frame{{flex:1;border:none;width:100%;height:100%;display:none;background:#fff}}
</style>
</head>
<body>

<div id="hdr">
  <span class="logo">ASTRA</span>
  <span class="tagline">Demo Pipeline Portal</span>
  <span class="spacer"></span>
  <span class="phase-label" id="current-label">Overview</span>
</div>

<div id="shell">

  <!-- Sidebar -->
  <div id="sb">
    <button id="home-btn" onclick="showOverview()">
      <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor"
           stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round">
        <path d="M3 9.5L12 3l9 6.5V20a1 1 0 0 1-1 1H4a1 1 0 0 1-1-1Z"/>
        <polyline points="9 21 9 13 15 13 15 21"/>
      </svg>
      Dashboard
    </button>
    {sidebar_html}
  </div>

  <!-- Content -->
  <div id="content">
    <iframe id="landing" srcdoc="{landing_srcdoc}" scrolling="yes"></iframe>
    <iframe id="phase-frame" src="about:blank" allowfullscreen></iframe>
  </div>

</div>

<script>
var currentPort = null;

function loadPhase(port, num, name) {{
  currentPort = port;
  var frame = document.getElementById('phase-frame');
  var landing = document.getElementById('landing');
  frame.src = 'http://localhost:' + port + '/';
  frame.style.display = 'block';
  landing.style.display = 'none';
  document.getElementById('current-label').textContent = 'Phase ' + String(num).padStart(2,'0') + ' — ' + name;
  document.querySelectorAll('.phase-btn').forEach(function(b) {{ b.classList.remove('active'); }});
  document.getElementById('home-btn').style.borderLeftColor = 'transparent';
  document.getElementById('home-btn').style.background = '#161b22';
  var btn = document.querySelector('[data-port="' + port + '"]');
  if (btn) btn.classList.add('active');
}}

function showOverview() {{
  currentPort = null;
  var frame = document.getElementById('phase-frame');
  var landing = document.getElementById('landing');
  frame.style.display = 'none';
  landing.style.display = 'block';
  document.getElementById('current-label').textContent = 'Overview';
  document.querySelectorAll('.phase-btn').forEach(function(b) {{ b.classList.remove('active'); }});
  var hb = document.getElementById('home-btn');
  hb.style.borderLeftColor = '#7c9ef0';
  hb.style.background = '#1c2333';
}}

// Live status polling
function updateStatus() {{
  fetch('/status')
    .then(function(r) {{ return r.json(); }})
    .then(function(data) {{
      Object.keys(data).forEach(function(num) {{
        var dot = document.querySelector('[data-dot="' + num + '"]');
        if (dot) {{
          dot.className = 'dot ' + data[num];
        }}
        // also update step dot in landing if visible
        var sd = document.getElementById('step-dot-' + num);
        if (sd) {{
          var c = data[num] === 'up' ? '#22c55e' :
                  data[num] === 'down' ? '#ef4444' :
                  data[num] === 'starting' ? '#f59e0b' : '#6b7280';
          sd.style.background = c;
        }}
      }});
    }})
    .catch(function() {{}});
}}
setInterval(updateStatus, 3000);
updateStatus();
</script>
</body>
</html>
"""


def _dot_color(num: int) -> str:
    s = _phase_status.get(num, "starting")
    return {"up": "#22c55e", "down": "#ef4444", "starting": "#f59e0b", "missing": "#6b7280"}.get(s, "#f59e0b")


def _build_sidebar_html() -> str:
    parts: list[str] = []
    for group_name, nums in PHASE_GROUPS:
        parts.append(f'<div class="grp-hdr">{group_name}</div>')
        for num in nums:
            _, name, port, _ = _phase_by_num[num]
            status = _phase_status.get(num, "starting")
            parts.append(
                f'<button class="phase-btn" data-port="{port}" '
                f'onclick="loadPhase({port},{num},\'{name}\')">'
                f'<span class="num">{num:02d}</span>'
                f'<span class="pname">{name}</span>'
                f'<span class="dot {status}" data-dot="{num}"></span>'
                f'</button>'
            )
    return "\n".join(parts)


def _build_landing_html() -> str:
    steps = []
    for num, name, port, _ in PHASES:
        color = _dot_color(num)
        steps.append(PIPELINE_STEP_TMPL.format(num=num, name=name, port=port, dot_color=color))
    return LANDING_HTML.format(pipeline_steps="\n".join(steps))


def _build_portal_html() -> str:
    sidebar = _build_sidebar_html()
    landing = _build_landing_html()
    # Escape for srcdoc attribute (double-quotes only — single quotes are fine in HTML)
    landing_escaped = landing.replace("&", "&amp;").replace('"', "&quot;")
    return PORTAL_HTML_TMPL.format(
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
