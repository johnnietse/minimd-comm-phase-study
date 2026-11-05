#!/usr/bin/env python3
"""
miniMD Communication Phase Test Runner — Web GUI
=================================================
Zero external dependencies. Uses only Python's built-in http.server.
Run on the cluster, then open in your Windows browser via SSH tunnel.

Usage:
  python3 test_gui_web.py
  Then open http://localhost:8080 in your browser.
"""

import http.server
import json
import subprocess
import threading
import time
import os
import socketserver

PORT = 8080
CORE_FOR_CTRL = "30"
CONTROLLER_WAIT = 3

# ── Shared state ─────────────────────────────────────────────────────
log_lines = []
is_running = False
current_test = ""

def add_log(msg):
    ts = time.strftime("%H:%M:%S")
    log_lines.append(f"[{ts}] {msg}")
    if len(log_lines) > 500:
        log_lines.pop(0)

# ── Test execution logic ─────────────────────────────────────────────
def get_energy():
    try:
        e = int(open('/sys/class/powercap/intel-rapl:0/energy_uj').read().strip())
        m = int(open('/sys/class/powercap/intel-rapl:0/max_energy_range_uj').read().strip())
        return e, m
    except:
        return 0, 1

def run_test(test_type, workers, run_id, dry_run):
    global is_running, current_test
    is_running = True
    current_test = test_type

    try:
        add_log(f"{'='*50}")
        add_log(f"STARTING TEST {test_type} | Workers={workers} | Run={run_id}")
        add_log(f"{'='*50}")

        if not dry_run:
            # Kill old controllers + reset governors
            add_log("Killing old controllers...")
            subprocess.run("pkill -f freq_controller", shell=True, stderr=subprocess.DEVNULL)

            add_log(f"Resetting governors to 'performance' for {workers} cores...")
            reset_cmd = f"for c in $(seq 0 $(( {workers} - 1 ))); do echo 'performance' > /sys/devices/system/cpu/cpu$c/cpufreq/scaling_governor; done"
            subprocess.run(reset_cmd, shell=True)

            add_log("Cleaning phase_marker.txt...")
            subprocess.run("rm -f phase_marker.txt", shell=True)
        else:
            add_log("[DRY RUN] Skipping governor reset and cleanup.")

        # ── Energy BEFORE ──
        if dry_run:
            before_e, max_r = 1000, 100000000
        else:
            before_e, max_r = get_energy()
        add_log(f"Energy BEFORE: {before_e} uJ")

        # ── Launch controller (Test C / C2 only) ──
        ctrl_pid = None
        if test_type in ("C", "C2"):
            script = "comm_freq_controller.py" if test_type == "C" else "integrated_freq_controller.py"
            log_tag = "c" if test_type == "C" else "c2"
            ctrl_log = f"/tmp/ctrl_{log_tag}_{workers}_{run_id}.log"

            add_log(f"Starting controller: {script} ...")
            if not dry_run:
                ctrl_cmd = f"taskset -c {CORE_FOR_CTRL} python3 -u {script} --workers {workers} > {ctrl_log} 2>&1 &"
                proc = subprocess.Popen(ctrl_cmd, shell=True)
                ctrl_pid = proc.pid
                add_log(f"Controller launched (shell PID {ctrl_pid}). Waiting {CONTROLLER_WAIT}s...")
                time.sleep(CONTROLLER_WAIT)

                # Verify controller is running
                check = subprocess.run(f"head -5 {ctrl_log}", shell=True, capture_output=True, text=True)
                if check.stdout.strip():
                    add_log(f"Controller output: {check.stdout.strip()[:200]}")
                else:
                    add_log("WARNING: Controller log is empty — it may have crashed.")
            else:
                add_log("[DRY RUN] Skipping controller launch.")

        # ── Run miniMD ──
        app_tag = test_type.lower()
        app_log = f"/tmp/test_{app_tag}_{workers}_{run_id}.log"
        add_log(f"Executing miniMD → {app_log} ...")

        if dry_run:
            add_log("[DRY RUN] Simulating 3-second miniMD run...")
            time.sleep(3)
        else:
            app_cmd = f"mpirun -np {workers} --bind-to core ./miniMD_openmpi -i in.lj.miniMD 2>&1 | tee {app_log}"
            subprocess.run(app_cmd, shell=True)

        # ── Energy AFTER ──
        if dry_run:
            after_e = 1500
        else:
            after_e, _ = get_energy()
        add_log(f"Energy AFTER: {after_e} uJ")

        diff = after_e - before_e
        if diff < 0:
            diff += max_r
        energy_j = round(diff / 1000000.0, 3)

        add_log(f"★ ENERGY: {energy_j} J")

        # ── Stop controller + count transitions ──
        transitions = 0
        if test_type in ("C", "C2"):
            if not dry_run:
                time.sleep(1)
                subprocess.run("pkill -f freq_controller", shell=True, stderr=subprocess.DEVNULL)

                grep_pat = "Phase transition:" if test_type == "C" else "(Phase transition:|PHASE:)"
                log_tag = "c" if test_type == "C" else "c2"
                ctrl_log = f"/tmp/ctrl_{log_tag}_{workers}_{run_id}.log"
                try:
                    result = subprocess.run(f"grep -cE '{grep_pat}' {ctrl_log}", shell=True, capture_output=True, text=True)
                    transitions = int(result.stdout.strip()) if result.stdout.strip() else 0
                except:
                    transitions = 0
            else:
                transitions = 10
            add_log(f"★ TRANSITIONS: {transitions}")

        # ── Parse PERF_SUMMARY + write CSV ──
        perf_data = "0,0,0,0,0,0"
        if not dry_run:
            try:
                with open(app_log, 'r') as f:
                    for line in f:
                        if "PERF_SUMMARY" in line:
                            parts = line.split()
                            if len(parts) >= 11:
                                perf_data = ",".join(parts[4:10])
                            break
            except:
                pass

            add_log(f"PERF_SUMMARY: {perf_data}")

            # Write to CSV
            if test_type == "B":
                csv_name = "results_manual_test_b.csv"
                csv_line = f"{run_id},{workers},{energy_j},{perf_data}\n"
            elif test_type == "C":
                csv_name = "results_manual_test_c.csv"
                csv_line = f"{run_id},{workers},{energy_j},{perf_data},{transitions}\n"
            else:  # C2
                csv_name = "results_manual_test_c2.csv"
                csv_line = f"{run_id},{workers},{energy_j},{perf_data},{transitions}\n"

            with open(csv_name, "a") as f:
                f.write(csv_line)
            add_log(f"✓ Appended to {csv_name}")

        # ── Reset governors (Tests C/C2) ──
        if test_type in ("C", "C2") and not dry_run:
            reset_cmd = f"for c in $(seq 0 $(( {workers} - 1 ))); do echo 'performance' > /sys/devices/system/cpu/cpu$c/cpufreq/scaling_governor; done"
            subprocess.run(reset_cmd, shell=True)
            add_log("Governors reset to 'performance'.")

        add_log(f"{'='*50}")
        add_log(f"✅  TEST {test_type} COMPLETE  —  Energy: {energy_j} J")
        add_log(f"{'='*50}")

    except Exception as ex:
        add_log(f"❌ ERROR: {str(ex)}")
    finally:
        is_running = False
        current_test = ""

# ── HTML page (self-contained) ────────────────────────────────────────
HTML_PAGE = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>miniMD Test Runner — ELEC 498 Group 30</title>
<style>
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body {
    font-family: 'Segoe UI', Arial, sans-serif;
    background: linear-gradient(135deg, #0f0c29, #302b63, #24243e);
    color: #e0e0e0;
    min-height: 100vh;
    display: flex; flex-direction: column; align-items: center;
    padding: 30px;
  }
  h1 {
    font-size: 28px;
    background: linear-gradient(90deg, #00d2ff, #3a7bd5);
    -webkit-background-clip: text; -webkit-text-fill-color: transparent;
    margin-bottom: 5px;
  }
  .subtitle { color: #888; font-size: 13px; margin-bottom: 25px; }
  .card {
    background: rgba(255,255,255,0.06);
    border: 1px solid rgba(255,255,255,0.1);
    border-radius: 12px;
    padding: 25px;
    width: 100%; max-width: 850px;
    margin-bottom: 20px;
    backdrop-filter: blur(10px);
  }
  .card h2 { font-size: 16px; color: #aaa; margin-bottom: 15px; text-transform: uppercase; letter-spacing: 1px; }
  .config-row { display: flex; gap: 20px; align-items: center; flex-wrap: wrap; }
  .config-row label { font-size: 14px; color: #ccc; }
  .config-row input[type=number] {
    background: rgba(255,255,255,0.1); border: 1px solid rgba(255,255,255,0.2);
    border-radius: 8px; padding: 10px 14px; color: #fff; font-size: 16px; width: 100px;
    outline: none; transition: border 0.3s;
  }
  .config-row input:focus { border-color: #3a7bd5; }
  .checkbox-label { display: flex; align-items: center; gap: 8px; cursor: pointer; }
  .checkbox-label input { width: 18px; height: 18px; }
  .btn-row { display: flex; gap: 12px; flex-wrap: wrap; }
  .btn {
    flex: 1; min-width: 200px; padding: 14px 20px;
    border: none; border-radius: 10px; cursor: pointer;
    font-size: 15px; font-weight: 600; color: #fff;
    transition: transform 0.15s, box-shadow 0.3s;
  }
  .btn:hover { transform: translateY(-2px); box-shadow: 0 6px 20px rgba(0,0,0,0.4); }
  .btn:active { transform: translateY(0); }
  .btn:disabled { opacity: 0.4; cursor: not-allowed; transform: none; }
  .btn-b  { background: linear-gradient(135deg, #f093fb, #f5576c); }
  .btn-c  { background: linear-gradient(135deg, #4facfe, #00f2fe); }
  .btn-c2 { background: linear-gradient(135deg, #43e97b, #38f9d7); }
  .console {
    background: #0d0d0d; border-radius: 10px; padding: 15px;
    height: 320px; overflow-y: auto; font-family: 'Consolas', 'Courier New', monospace;
    font-size: 13px; line-height: 1.6; color: #33ff33;
    border: 1px solid rgba(255,255,255,0.08);
  }
  .status-bar {
    width: 100%; max-width: 850px;
    display: flex; justify-content: space-between;
    font-size: 12px; color: #666; padding: 5px 0;
  }
  .pulse { animation: pulse 1.5s infinite; }
  @keyframes pulse { 0%,100%{opacity:1} 50%{opacity:0.4} }
</style>
</head>
<body>
  <h1>⚡ miniMD Communication Phase Test Runner</h1>
  <p class="subtitle">ELEC 498 — Group 30 — Energy-Aware HPC Optimization</p>

  <div class="card">
    <h2>⚙ Configuration</h2>
    <div class="config-row">
      <div>
        <label>NUM_WORKERS</label><br>
        <input type="number" id="workers" value="16" min="1" max="32">
      </div>
      <div>
        <label>RUN ID</label><br>
        <input type="number" id="runid" value="1" min="1">
      </div>
      <div>
        <label class="checkbox-label">
          <input type="checkbox" id="dryrun"> Dry Run (Simulated)
        </label>
      </div>
    </div>
  </div>

  <div class="card">
    <h2>🚀 Run Tests</h2>
    <div class="btn-row">
      <button class="btn btn-b" id="btnB" onclick="startTest('B')">Test B<br><small>No Controller</small></button>
      <button class="btn btn-c" id="btnC" onclick="startTest('C')">Test C<br><small>comm_freq_controller</small></button>
      <button class="btn btn-c2" id="btnC2" onclick="startTest('C2')">Test C2<br><small>integrated_freq_controller</small></button>
    </div>
  </div>

  <div class="card">
    <h2>📟 Live Output</h2>
    <div class="console" id="console"></div>
  </div>

  <div class="status-bar">
    <span id="statusText">Ready</span>
    <span>Port: """ + str(PORT) + """</span>
  </div>

<script>
  let pollTimer = null;
  let lastLen = 0;

  function startTest(type) {
    const w = document.getElementById('workers').value;
    const r = document.getElementById('runid').value;
    const d = document.getElementById('dryrun').checked;
    setButtons(true);
    document.getElementById('statusText').innerHTML = '<span class="pulse">⏳ Running Test ' + type + '...</span>';
    fetch('/run', {
      method: 'POST',
      headers: {'Content-Type':'application/json'},
      body: JSON.stringify({test: type, workers: parseInt(w), run_id: parseInt(r), dry_run: d})
    });
    lastLen = 0;
    pollTimer = setInterval(pollLog, 800);
  }

  function pollLog() {
    fetch('/log?since=' + lastLen)
      .then(r => r.json())
      .then(data => {
        const c = document.getElementById('console');
        if (data.lines.length > 0) {
          data.lines.forEach(l => { c.innerHTML += l + '\n'; });
          lastLen += data.lines.length;
          c.scrollTop = c.scrollHeight;
        }
        if (!data.running) {
          clearInterval(pollTimer);
          setButtons(false);
          document.getElementById('statusText').textContent = '✅ Ready';
        }
      });
  }

  function setButtons(disabled) {
    document.getElementById('btnB').disabled = disabled;
    document.getElementById('btnC').disabled = disabled;
    document.getElementById('btnC2').disabled = disabled;
  }

  // Initial log poll
  setInterval(() => {
    fetch('/log?since=' + lastLen).then(r=>r.json()).then(data => {
      if (data.lines.length > 0) {
        const c = document.getElementById('console');
        data.lines.forEach(l => { c.innerHTML += l + '\n'; });
        lastLen += data.lines.length;
        c.scrollTop = c.scrollHeight;
      }
    }).catch(()=>{});
  }, 2000);
</script>
</body>
</html>
"""

# ── HTTP Handler ──────────────────────────────────────────────────────
class Handler(http.server.BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        pass  # suppress default logging

    def do_GET(self):
        if self.path == '/' or self.path == '/index.html':
            self.send_response(200)
            self.send_header('Content-Type', 'text/html')
            self.end_headers()
            self.wfile.write(HTML_PAGE.encode())

        elif self.path.startswith('/log'):
            since = 0
            if '?since=' in self.path:
                try: since = int(self.path.split('since=')[1])
                except: since = 0
            new_lines = log_lines[since:]
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps({"lines": new_lines, "running": is_running}).encode())
        else:
            self.send_response(404)
            self.end_headers()

    def do_POST(self):
        if self.path == '/run':
            length = int(self.headers.get('Content-Length', 0))
            body = json.loads(self.rfile.read(length))
            test_type = body.get('test', 'B')
            workers = body.get('workers', 16)
            run_id = body.get('run_id', 1)
            dry_run = body.get('dry_run', False)

            thread = threading.Thread(target=run_test, args=(test_type, str(workers), str(run_id), dry_run))
            thread.daemon = True
            thread.start()

            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps({"status": "started"}).encode())
        else:
            self.send_response(404)
            self.end_headers()

class ThreadedServer(socketserver.ThreadingMixIn, http.server.HTTPServer):
    allow_reuse_address = True

if __name__ == "__main__":
    add_log("miniMD Test Runner started. Waiting for commands...")
    server = ThreadedServer(("0.0.0.0", PORT), Handler)
    print(f"\n{'='*60}")
    print(f"  miniMD Test Runner — Web GUI")
    print(f"  Open in your browser: http://localhost:{PORT}")
    print(f"  Press Ctrl+C to stop.")
    print(f"{'='*60}\n")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down...")
        subprocess.run("pkill -f freq_controller", shell=True, stderr=subprocess.DEVNULL)
        server.shutdown()
