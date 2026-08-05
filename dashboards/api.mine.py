#!/usr/bin/env python3
"""
WHPG Real Streaming Demo — Unified Dashboard API
Serves metrics, live tickers, AND process/job control via HTTP.

Usage:
  python3 dashboards/api.py [options]

Options:
  --pg-host HOST      PostgreSQL host        (default: localhost)
  --pg-port PORT      PostgreSQL port        (default: 5432)
  --pg-user USER      PostgreSQL user        (default: gpadmin)
  --pg-password PASS  PostgreSQL password    (default: )
  --pg-dbname DB      PostgreSQL database    (default: streaming_demo)
  --kafka BROKER      Kafka broker           (default: localhost:9092)
  --app-port PORT     HTTP port for this API (default: 5055)
  --repo-dir DIR      Repo root directory    (default: auto-detect)
"""

import argparse
import os
import signal
import subprocess
import threading
import time
from datetime import datetime
from pathlib import Path

import psycopg2
import psycopg2.extras
from flask import Flask, jsonify, request, send_from_directory
from flask_cors import CORS

app = Flask(__name__, static_folder=".")
CORS(app)

# ── Config (filled in by CLI args / env) ──────────────────────────────────────
DB_CONFIG = {
    "host":     os.environ.get("PGHOST",     "localhost"),
    "port":     int(os.environ.get("PGPORT", 5432)),
    "user":     os.environ.get("PGUSER",     "gpadmin"),
    "password": os.environ.get("PGPASSWORD", ""),
    "database": os.environ.get("PGDATABASE", "streaming_demo"),
}
KAFKA_BROKER  = os.environ.get("KAFKA_BROKER", "localhost:9092")
REPO_DIR      = Path(__file__).resolve().parent.parent   # dashboards/../
PID_DIR       = Path("/tmp/flowserver-demo")
LOG_DIR       = PID_DIR / "logs"
FS_SERVER_URL = "http://localhost:6060"

# ── Generator runtime state ───────────────────────────────────────────────────
GEN_CONFIG = {
    "ecom_rate":  50,
    "iot_rate":   20,
    "ecom_total": 0,
    "iot_total":  0,
}


# ══════════════════════════════════════════════════════════════════════════════
#  DB HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def get_conn():
    return psycopg2.connect(**DB_CONFIG)


def q(sql, params=None):
    conn = get_conn()
    cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute(sql, params or ())
    rows = cur.fetchall()
    cur.close(); conn.close()
    return [dict(r) for r in rows]


def q1(sql, params=None):
    rows = q(sql, params)
    return rows[0] if rows else {}


# ══════════════════════════════════════════════════════════════════════════════
#  PROCESS HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def pid_file(name: str) -> Path:
    return PID_DIR / f"{name}.pid"


def log_file(name: str) -> Path:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    return LOG_DIR / f"{name}.log"


def read_pid(name: str):
    pf = pid_file(name)
    if pf.exists():
        try:
            pid = int(pf.read_text().strip())
            os.kill(pid, 0)   # raises if dead
            return pid
        except (ValueError, ProcessLookupError, OSError):
            pf.unlink(missing_ok=True)
    return None


def write_pid(name: str, pid: int):
    PID_DIR.mkdir(parents=True, exist_ok=True)
    pid_file(name).write_text(str(pid))


def kill_pid(name: str) -> bool:
    pid = read_pid(name)
    if pid is None:
        return False
    try:
        os.kill(pid, signal.SIGTERM)
        for _ in range(10):
            time.sleep(0.3)
            try: os.kill(pid, 0)
            except OSError: break
        else:
            os.kill(pid, signal.SIGKILL)
    except OSError:
        pass
    pid_file(name).unlink(missing_ok=True)
    return True


def run_bg(name: str, cmd: list, cwd=None) -> int:
    """Start a background process, save PID, return PID."""
    with open(log_file(name), "a") as lf:
        proc = subprocess.Popen(
            cmd, cwd=str(cwd or REPO_DIR),
            stdout=lf, stderr=lf,
            start_new_session=True,
        )
    write_pid(name, proc.pid)
    return proc.pid


def flowcli(*args) -> dict:
    """Run flowcli command, return {'ok', 'stdout', 'stderr'}.
    Correct syntax: flowcli --host HOST --port PORT COMMAND [JOB]
    Commands: submit, start, stop, list, monitor
    """
    host = FS_SERVER_URL.replace("http://", "").split(":")[0]
    port = FS_SERVER_URL.split(":")[-1]
    cmd = ["flowcli", "--host", host, "--port", port] + list(args)
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
        return {"ok": r.returncode == 0,
                "stdout": r.stdout.strip(),
                "stderr": r.stderr.strip()}
    except FileNotFoundError:
        return {"ok": False, "stdout": "", "stderr": "flowcli not found in PATH"}
    except subprocess.TimeoutExpired:
        return {"ok": False, "stdout": "", "stderr": "flowcli timed out"}


def tail_log(name: str, n: int = 30) -> str:
    lf = log_file(name)
    if not lf.exists():
        return ""
    lines = lf.read_text(errors="replace").splitlines()
    return "\n".join(lines[-n:])


# ══════════════════════════════════════════════════════════════════════════════
#  STATUS / HEALTH
# ══════════════════════════════════════════════════════════════════════════════

@app.route("/api/health")
def health():
    try:
        row = q1("SELECT version() AS ver, current_database() AS db, now() AS ts")
        return jsonify({"ok": True, "version": row.get("ver", ""),
                        "database": row.get("db", ""), "ts": str(row.get("ts", ""))})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/api/status")
def status():
    """Full system status — DB, processes, jobs."""
    # DB row counts
    db_ok = False
    ecom = iot = {}
    try:
        ecom = q1("""
            SELECT COUNT(*) AS total,
                   MAX(timestamp) AS latest,
                   COUNT(*) FILTER (WHERE timestamp > NOW()-INTERVAL '1 minute') AS last_min
            FROM public.ecommerce_orders""")
        iot = q1("""
            SELECT COUNT(*) AS total,
                   MAX(timestamp) AS latest,
                   COUNT(*) FILTER (WHERE timestamp > NOW()-INTERVAL '1 minute') AS last_min
            FROM public.iot_sensor_readings""")
        db_ok = True
    except Exception as e:
        ecom = iot = {"error": str(e)}

    # Process status
    procs = {}
    for name in ["flowserver", "order-generator", "iot-generator", "dashboard-api"]:
        pid = read_pid(name)
        procs[name] = {"running": pid is not None, "pid": pid}

    # FlowServer job status via flowcli list (no status subcommand)
    jobs = {}
    r = flowcli("list")
    list_out = r["stdout"] + r["stderr"]
    for job in ["ecommerce-orders", "iot-sensors-csv"]:
        running = job in list_out and "JOB_RUNNING" in list_out.split(job)[-1][:60] if job in list_out else False
        jobs[job] = {"raw": list_out, "running": running}

    return jsonify({
        "ok": True,
        "db_ok": db_ok,
        "ecommerce": {
            "total":    int(ecom.get("total", 0) or 0),
            "latest":   str(ecom.get("latest", "") or ""),
            "last_min": int(ecom.get("last_min", 0) or 0),
        },
        "iot": {
            "total":    int(iot.get("total", 0) or 0),
            "latest":   str(iot.get("latest", "") or ""),
            "last_min": int(iot.get("last_min", 0) or 0),
        },
        "processes": procs,
        "jobs": jobs,
        "gen_config": GEN_CONFIG,
    })


# ══════════════════════════════════════════════════════════════════════════════
#  FLOWSERVER CONTROL
# ══════════════════════════════════════════════════════════════════════════════

def find_flowserver_bin() -> str:
    """Find flowserver binary — check known locations in order."""
    import shutil
    candidates = [
        "/usr/local/greenplum-db-7.3.1-WHPG/bin/flowserver",  # known install path
        str(REPO_DIR / "flowserver"),                           # repo root symlink/copy
        "/usr/local/bin/flowserver",
        "/usr/bin/flowserver",
        shutil.which("flowserver") or "",                       # PATH fallback
    ]
    for c in candidates:
        if c and os.path.isfile(c) and os.access(c, os.X_OK):
            return c
    return ""


@app.route("/api/flowserver/start", methods=["POST"])
def flowserver_start():
    """Start the FlowServer process."""
    pid = read_pid("flowserver")
    if pid:
        return jsonify({"ok": True, "message": f"FlowServer already running (PID {pid})", "pid": pid})

    fs_bin = find_flowserver_bin()
    if not fs_bin:
        return jsonify({"ok": False,
                        "error": f"flowserver binary not found in {REPO_DIR} or PATH"}), 400

    fs_cfg = REPO_DIR / "configs" / "flow_server.json"
    pid = run_bg("flowserver", [fs_bin, "-c", str(fs_cfg)])
    time.sleep(2)
    alive = read_pid("flowserver") is not None
    return jsonify({"ok": alive,
                    "pid": pid,
                    "binary": fs_bin,
                    "message": "FlowServer started" if alive else "FlowServer exited — check logs",
                    "log": tail_log("flowserver", 15)})


@app.route("/api/flowserver/stop", methods=["POST"])
def flowserver_stop():
    """Stop the FlowServer process."""
    stopped = kill_pid("flowserver")
    return jsonify({"ok": True, "stopped": stopped,
                    "message": "FlowServer stopped" if stopped else "FlowServer was not running"})


@app.route("/api/flowserver/log")
def flowserver_log():
    n = int(request.args.get("n", 50))
    return jsonify({"log": tail_log("flowserver", n)})


# ══════════════════════════════════════════════════════════════════════════════
#  JOB CONTROL  (flowcli wrappers)
# ══════════════════════════════════════════════════════════════════════════════

@app.route("/api/jobs")
def jobs_list():
    """List all jobs and their status."""
    result = []
    r = flowcli("list")
    list_out = r["stdout"]
    for job in ["ecommerce-orders", "iot-sensors-csv"]:
        if job in list_out:
            job_line = [l for l in list_out.splitlines() if l.startswith(job)]
            raw = job_line[0] if job_line else list_out
            running = "JOB_RUNNING" in raw
        else:
            raw = "not submitted"
            running = False
        result.append({"name": job, "raw": raw, "running": running, "ok": r["ok"]})
    return jsonify(result)


@app.route("/api/jobs/<job_name>/submit", methods=["POST"])
def job_submit(job_name):
    """Submit a job from its YAML file."""
    yaml_map = {
        "ecommerce-orders": "jobs/ecommerce-orders.yaml",
        "iot-sensors-csv":  "jobs/iot-sensors-csv.yaml",
    }
    yaml_path = yaml_map.get(job_name)
    if not yaml_path:
        return jsonify({"ok": False, "error": f"Unknown job: {job_name}"}), 400
    r = flowcli("submit", str(REPO_DIR / yaml_path))
    return jsonify({"ok": r["ok"], "output": r["stdout"] or r["stderr"]})


@app.route("/api/jobs/<job_name>/start", methods=["POST"])
def job_start(job_name):
    r = flowcli("start", job_name)
    return jsonify({"ok": r["ok"], "output": r["stdout"] or r["stderr"]})


@app.route("/api/jobs/<job_name>/stop", methods=["POST"])
def job_stop(job_name):
    r = flowcli("stop", job_name)
    return jsonify({"ok": r["ok"], "output": r["stdout"] or r["stderr"]})


@app.route("/api/jobs/<job_name>/restart", methods=["POST"])
def job_restart(job_name):
    flowcli("stop", job_name)
    time.sleep(0.5)
    r = flowcli("start", job_name)
    return jsonify({"ok": r["ok"], "output": r["stdout"] or r["stderr"]})


# ══════════════════════════════════════════════════════════════════════════════
#  GENERATOR CONTROL
# ══════════════════════════════════════════════════════════════════════════════

@app.route("/api/generators/start", methods=["POST"])
def generators_start():
    """Start one or both generators."""
    body = request.get_json(silent=True) or {}

    # Update config if provided
    for k in ("ecom_rate", "iot_rate", "ecom_total", "iot_total"):
        if k in body:
            GEN_CONFIG[k] = int(body[k])

    started = []
    errors  = []

    gen_dir = REPO_DIR / "generators"
    ecom_bin = gen_dir / "order-generator"
    iot_bin  = gen_dir / "iot-generator"

    # Auto-build if missing
    if not ecom_bin.exists() or not iot_bin.exists():
        r = subprocess.run(["go", "build", "-o", "order-generator", "order-generator.go"],
                           capture_output=True, cwd=str(gen_dir))
        subprocess.run(["go", "build", "-o", "iot-generator", "iot-generator.go"],
                       capture_output=True, cwd=str(gen_dir))

    target = body.get("target", "both")   # both | ecom | iot

    if target in ("both", "ecom") and not read_pid("order-generator"):
        cmd = [str(ecom_bin),
               "-rate", str(GEN_CONFIG["ecom_rate"]),
               "-max-messages", str(GEN_CONFIG["ecom_total"])]
        pid = run_bg("order-generator", cmd, cwd=gen_dir)
        started.append(f"order-generator (PID {pid})")

    if target in ("both", "iot") and not read_pid("iot-generator"):
        cmd = [str(iot_bin),
               "-rate", str(GEN_CONFIG["iot_rate"]),
               "-max-messages", str(GEN_CONFIG["iot_total"])]
        pid = run_bg("iot-generator", cmd, cwd=gen_dir)
        started.append(f"iot-generator (PID {pid})")

    return jsonify({"ok": True, "started": started, "errors": errors,
                    "config": GEN_CONFIG})


@app.route("/api/generators/stop", methods=["POST"])
def generators_stop():
    body = request.get_json(silent=True) or {}
    target = body.get("target", "both")
    stopped = []
    if target in ("both", "ecom"):
        if kill_pid("order-generator"): stopped.append("order-generator")
    if target in ("both", "iot"):
        if kill_pid("iot-generator"):   stopped.append("iot-generator")
    return jsonify({"ok": True, "stopped": stopped})


@app.route("/api/generators/config", methods=["GET", "POST"])
def generators_config():
    if request.method == "POST":
        body = request.get_json(silent=True) or {}
        for k in ("ecom_rate", "iot_rate", "ecom_total", "iot_total"):
            if k in body:
                GEN_CONFIG[k] = int(body[k])
        return jsonify({"ok": True, "config": GEN_CONFIG})
    return jsonify(GEN_CONFIG)


@app.route("/api/generators/log/<name>")
def generator_log(name):
    allowed = {"order-generator", "iot-generator"}
    if name not in allowed:
        return jsonify({"error": "unknown generator"}), 400
    n = int(request.args.get("n", 30))
    return jsonify({"log": tail_log(name, n)})


# ══════════════════════════════════════════════════════════════════════════════
#  START ALL / STOP ALL
# ══════════════════════════════════════════════════════════════════════════════

FS_BIN_PATH = "/usr/local/greenplum-db-7.3.1-WHPG/bin/flowserver"

KAFKA_BIN    = "/opt/kafka/bin"
KAFKA_BROKER = "localhost:9092"


def start_flowserver() -> tuple:
    """Start FlowServer, wait for port 6060. Returns (ok, message)."""
    pid = read_pid("flowserver")
    if pid:
        return True, f"FlowServer already running (PID {pid})"

    fs_bin = find_flowserver_bin()
    if not fs_bin:
        return False, f"flowserver binary not found (checked {FS_BIN_PATH} and PATH)"

    fs_cfg = REPO_DIR / "configs" / "flow_server.json"
    new_pid = run_bg("flowserver", [fs_bin, "-c", str(fs_cfg)])

    # Wait up to 15s for port 6060
    import socket
    for i in range(15):
        time.sleep(1)
        try:
            with socket.create_connection(("localhost", 6060), timeout=1):
                return True, f"FlowServer started (PID {new_pid}, ready in {i+1}s)"
        except OSError:
            pass
        try: os.kill(new_pid, 0)
        except OSError:
            return False, f"FlowServer crashed — check {log_file('flowserver')}"

    return False, "FlowServer did not open port 6060 within 15s"


def stop_flowserver() -> str:
    """Stop FlowServer process."""
    stopped = kill_pid("flowserver")
    # Also kill any stray
    try:
        subprocess.run(["pkill", "-f", "flowserver -c"], capture_output=True)
    except Exception:
        pass
    return "FlowServer stopped" if stopped else "FlowServer was not running"


def delete_kafka_topics() -> list:
    """Delete Kafka topics as kafka user via sudo su."""
    inner = f"/tmp/kafka-reset-{os.getpid()}.sh"
    script = f"""#!/bin/bash
KAFKA_BIN="{KAFKA_BIN}"
KAFKA_BROKER="{KAFKA_BROKER}"
[[ -x "$KAFKA_BIN/kafka-topics" ]] && KT="$KAFKA_BIN/kafka-topics" || KT="$KAFKA_BIN/kafka-topics.sh"
RESULTS=""
for TOPIC in ecommerce-orders iot-sensors-csv; do
    if "$KT" --list --bootstrap-server "$KAFKA_BROKER" 2>/dev/null | grep -q "^${{TOPIC}}$"; then
        "$KT" --delete --topic "$TOPIC" --bootstrap-server "$KAFKA_BROKER" 2>/dev/null || true
        echo "deleted:$TOPIC"
    else
        echo "notfound:$TOPIC"
    fi
done
"""
    Path(inner).write_text(script)
    Path(inner).chmod(0o755)
    try:
        r = subprocess.run(["sudo", "su", "-", "kafka", "-c", f"bash {inner}"],
                           capture_output=True, text=True, timeout=30)
        lines = r.stdout.strip().splitlines()
        msgs = []
        for line in lines:
            if line.startswith("deleted:"): msgs.append(f"Topic '{line.split(':')[1]}' deleted")
            elif line.startswith("notfound:"): msgs.append(f"Topic '{line.split(':')[1]}' not found")
        return msgs or ["Topics processed"]
    except Exception as e:
        return [f"Topic deletion error: {e}"]
    finally:
        Path(inner).unlink(missing_ok=True)


def recreate_kafka_topics() -> list:
    """Recreate Kafka topics as kafka user."""
    inner = f"/tmp/kafka-create-{os.getpid()}.sh"
    script = f"""#!/bin/bash
KAFKA_BIN="{KAFKA_BIN}"
KAFKA_BROKER="{KAFKA_BROKER}"
[[ -x "$KAFKA_BIN/kafka-topics" ]] && KT="$KAFKA_BIN/kafka-topics" || KT="$KAFKA_BIN/kafka-topics.sh"
for TOPIC in ecommerce-orders iot-sensors-csv; do
    if ! "$KT" --list --bootstrap-server "$KAFKA_BROKER" 2>/dev/null | grep -q "^${{TOPIC}}$"; then
        "$KT" --create --topic "$TOPIC" --bootstrap-server "$KAFKA_BROKER" \
              --partitions 3 --replication-factor 1
        echo "created:$TOPIC"
    else
        echo "exists:$TOPIC"
    fi
done
"""
    Path(inner).write_text(script)
    Path(inner).chmod(0o755)
    try:
        r = subprocess.run(["sudo", "su", "-", "kafka", "-c", f"bash {inner}"],
                           capture_output=True, text=True, timeout=30)
        lines = r.stdout.strip().splitlines()
        msgs = []
        for line in lines:
            if line.startswith("created:"): msgs.append(f"Topic '{line.split(':')[1]}' created")
            elif line.startswith("exists:"): msgs.append(f"Topic '{line.split(':')[1]}' already exists")
        return msgs or ["Topics processed"]
    except Exception as e:
        return [f"Topic creation error: {e}"]
    finally:
        Path(inner).unlink(missing_ok=True)


@app.route("/api/start_all", methods=["POST"])
def start_all():
    """Start FlowServer → jobs → generators."""
    body   = request.get_json(silent=True) or {}
    steps  = []
    errors = []

    # 1. FlowServer
    ok, msg = start_flowserver()
    steps.append(msg)
    if not ok:
        errors.append(msg)

    # 2. Jobs (only if FlowServer is up)
    if ok or read_pid("flowserver"):
        for job, yaml in [("ecommerce-orders", "jobs/ecommerce-orders.yaml"),
                          ("iot-sensors-csv",  "jobs/iot-sensors-csv.yaml")]:
            flowcli("submit", str(REPO_DIR / yaml))
            r = flowcli("start", job)
            out = r["stdout"] or r["stderr"]
            if r["ok"] or "already" in out.lower():
                steps.append(f"Job '{job}' started")
            else:
                errors.append(f"Job '{job}': {out}")

    # 3. Generators
    for key in ("ecom_rate", "iot_rate", "ecom_total", "iot_total"):
        if key in body: GEN_CONFIG[key] = int(body[key])

    gen_dir  = REPO_DIR / "generators"
    ecom_bin = gen_dir / "order-generator"
    iot_bin  = gen_dir / "iot-generator"

    if not ecom_bin.exists() or not iot_bin.exists():
        errors.append("Generators not built — run setup.sh first")
    else:
        for name, binary, rate_key, total_key in [
            ("order-generator", ecom_bin, "ecom_rate", "ecom_total"),
            ("iot-generator",   iot_bin,  "iot_rate",  "iot_total"),
        ]:
            if not read_pid(name):
                pid = run_bg(name, [str(binary),
                             "-rate", str(GEN_CONFIG[rate_key]),
                             "-max-messages", str(GEN_CONFIG[total_key])],
                             cwd=gen_dir)
                steps.append(f"{name} started (PID {pid}, {GEN_CONFIG[rate_key]}/s)")
            else:
                steps.append(f"{name} already running")

    return jsonify({"ok": len(errors) == 0, "steps": steps, "errors": errors})


@app.route("/api/stop_all", methods=["POST"])
def stop_all():
    """Stop jobs → generators → FlowServer."""
    steps = []

    # 1. Stop jobs
    if read_pid("flowserver"):
        for job in ["ecommerce-orders", "iot-sensors-csv"]:
            r = flowcli("stop", job)
            steps.append(f"Job '{job}': {(r['stdout'] or r['stderr']).strip() or 'stop sent'}")
    else:
        steps.append("FlowServer not running — skipping job stop")

    # 2. Stop generators
    for name in ["order-generator", "iot-generator"]:
        steps.append(f"{name}: {'stopped' if kill_pid(name) else 'not running'}")

    # 3. Stop FlowServer
    steps.append(stop_flowserver())

    return jsonify({"ok": True, "steps": steps, "errors": []})


@app.route("/api/reset_all", methods=["POST"])
def reset_all():
    """
    Full reset + restart:
      stop jobs → stop generators → stop FlowServer
      → truncate tables → delete Kafka topics → recreate topics
      → start FlowServer → submit + start jobs → start generators
    """
    body   = request.get_json(silent=True) or {}
    steps  = []
    errors = []

    # ── STOP ─────────────────────────────────────────────────────────────────
    steps.append("── Stopping ──")

    if read_pid("flowserver"):
        for job in ["ecommerce-orders", "iot-sensors-csv"]:
            r = flowcli("stop", job)
            steps.append(f"Job '{job}': {(r['stdout'] or r['stderr']).strip() or 'stop sent'}")

    for name in ["order-generator", "iot-generator"]:
        steps.append(f"{name}: {'stopped' if kill_pid(name) else 'not running'}")

    steps.append(stop_flowserver())
    time.sleep(1)

    # ── TRUNCATE TABLES ───────────────────────────────────────────────────────
    steps.append("── Resetting Data ──")
    try:
        conn = get_conn()
        cur  = conn.cursor()
        cur.execute("TRUNCATE TABLE public.ecommerce_orders")
        cur.execute("TRUNCATE TABLE public.iot_sensor_readings")
        conn.commit()
        cur.close(); conn.close()
        steps.append("Tables truncated (ecommerce_orders, iot_sensor_readings)")
    except Exception as e:
        errors.append(f"Truncate failed: {e}")

    # ── KAFKA TOPICS ──────────────────────────────────────────────────────────
    steps.append("── Resetting Kafka Topics ──")
    steps.extend(delete_kafka_topics())
    time.sleep(2)   # give Kafka time to process deletion
    steps.extend(recreate_kafka_topics())

    # ── START ─────────────────────────────────────────────────────────────────
    steps.append("── Starting ──")

    ok, msg = start_flowserver()
    steps.append(msg)
    if not ok:
        errors.append(msg)

    if ok or read_pid("flowserver"):
        for job, yaml in [("ecommerce-orders", "jobs/ecommerce-orders.yaml"),
                          ("iot-sensors-csv",  "jobs/iot-sensors-csv.yaml")]:
            flowcli("submit", str(REPO_DIR / yaml))
            r = flowcli("start", job)
            out = r["stdout"] or r["stderr"]
            steps.append(f"Job '{job}': {'started' if r['ok'] or 'already' in out.lower() else out}")

    for key in ("ecom_rate", "iot_rate", "ecom_total", "iot_total"):
        if key in body: GEN_CONFIG[key] = int(body[key])

    gen_dir  = REPO_DIR / "generators"
    for name, binary, rate_key, total_key in [
        ("order-generator", gen_dir / "order-generator", "ecom_rate", "ecom_total"),
        ("iot-generator",   gen_dir / "iot-generator",   "iot_rate",  "iot_total"),
    ]:
        if binary.exists():
            pid = run_bg(name, [str(binary),
                         "-rate", str(GEN_CONFIG[rate_key]),
                         "-max-messages", str(GEN_CONFIG[total_key])],
                         cwd=gen_dir)
            steps.append(f"{name} started (PID {pid}, {GEN_CONFIG[rate_key]}/s)")
        else:
            errors.append(f"{name} binary not found — run setup.sh first")

    return jsonify({"ok": len(errors) == 0, "steps": steps, "errors": errors})


# ══════════════════════════════════════════════════════════════════════════════
#  E-COMMERCE METRICS
# ══════════════════════════════════════════════════════════════════════════════

@app.route("/api/ecom/metrics")
def ecom_metrics():
    try:
        totals = q1("""
            SELECT COUNT(*)                                              AS total_orders,
                   COALESCE(SUM(total_price),0)                         AS total_revenue,
                   COALESCE(AVG(total_price),0)                         AS avg_order_value,
                   COUNT(*) FILTER (WHERE timestamp > NOW()-INTERVAL '1 minute') AS orders_per_min,
                   COUNT(*) FILTER (WHERE is_bulk_order)                AS bulk_orders
            FROM public.ecommerce_orders""")
        buckets = q("""
            SELECT revenue_bucket, COUNT(*) AS order_count, SUM(total_price) AS revenue
            FROM public.ecommerce_orders
            GROUP BY revenue_bucket
            ORDER BY CASE revenue_bucket
                WHEN 'low' THEN 1 WHEN 'medium' THEN 2
                WHEN 'high' THEN 3 WHEN 'premium' THEN 4 END""")
        categories = q("""
            SELECT category, COUNT(*) AS order_count, SUM(total_price) AS revenue
            FROM public.ecommerce_orders
            GROUP BY category ORDER BY revenue DESC LIMIT 6""")
        countries = q("""
            SELECT country, COUNT(*) AS order_count, SUM(total_price) AS revenue
            FROM public.ecommerce_orders
            GROUP BY country ORDER BY revenue DESC LIMIT 10""")
        throughput = q("""
            SELECT DATE_TRUNC('minute', timestamp) AS bucket,
                   COUNT(*) AS cnt, SUM(total_price) AS revenue
            FROM public.ecommerce_orders
            WHERE timestamp > NOW()-INTERVAL '10 minutes'
            GROUP BY bucket ORDER BY bucket""")
        return jsonify({
            "ts": datetime.now().isoformat(),
            "totals":     {k: float(v) if isinstance(v, (int, float)) else v
                           for k, v in totals.items()},
            "buckets":    buckets,
            "categories": categories,
            "countries":  countries,
            "throughput": [{"bucket": str(r["bucket"]), "cnt": r["cnt"],
                            "revenue": float(r["revenue"] or 0)} for r in throughput],
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/ecom/recent")
def ecom_recent():
    try:
        rows = q("""
            SELECT order_id, timestamp, customer_name, product_name,
                   category, total_price, revenue_bucket, country, is_bulk_order
            FROM public.ecommerce_orders
            ORDER BY timestamp DESC LIMIT 20""")
        return jsonify(rows)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/ecom/flowserver_sql")
def ecom_flowserver_sql():
    try:
        yaml_path = REPO_DIR / "jobs" / "ecommerce-orders.yaml"
        content = yaml_path.read_text()
    except Exception:
        content = "# jobs/ecommerce-orders.yaml not found"
    return jsonify({"yaml": content})


# ══════════════════════════════════════════════════════════════════════════════
#  IoT METRICS
# ══════════════════════════════════════════════════════════════════════════════

@app.route("/api/iot/metrics")
def iot_metrics():
    try:
        totals = q1("""
            SELECT COUNT(*) AS total_readings,
                   COUNT(DISTINCT sensor_id) AS active_sensors,
                   COUNT(*) FILTER (WHERE timestamp > NOW()-INTERVAL '1 minute') AS readings_per_min,
                   COUNT(*) FILTER (WHERE alert_level >= 2) AS critical_alerts,
                   ROUND(AVG(temperature)::numeric, 2) AS avg_temp,
                   ROUND(AVG(humidity)::numeric, 2) AS avg_humidity
            FROM public.iot_sensor_readings""")
        alerts = q("""
            SELECT alert_level, COUNT(*) AS reading_count
            FROM public.iot_sensor_readings
            GROUP BY alert_level ORDER BY alert_level""")
        buildings = q("""
            SELECT building, COUNT(*) AS reading_count,
                   AVG(temperature) AS avg_temp,
                   AVG(humidity) AS avg_humidity,
                   AVG(battery_level) AS avg_battery
            FROM public.iot_sensor_readings
            WHERE building IS NOT NULL
            GROUP BY building ORDER BY building""")
        comfort = q("""
            SELECT comfort_index, COUNT(*) AS cnt
            FROM public.iot_sensor_readings
            WHERE comfort_index IS NOT NULL
            GROUP BY comfort_index""")
        throughput = q("""
            SELECT DATE_TRUNC('minute', timestamp) AS bucket, COUNT(*) AS cnt
            FROM public.iot_sensor_readings
            WHERE timestamp > NOW()-INTERVAL '10 minutes'
            GROUP BY bucket ORDER BY bucket""")
        return jsonify({
            "ts": datetime.now().isoformat(),
            "totals":    {k: float(v) if isinstance(v, (int, float)) else v
                          for k, v in totals.items()},
            "alerts":    alerts,
            "buildings": buildings,
            "comfort":   comfort,
            "throughput": [{"bucket": str(r["bucket"]), "cnt": r["cnt"]} for r in throughput],
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/iot/sensors")
def iot_sensors():
    try:
        rows = q("""
            SELECT sensor_id, building, floor,
                   COUNT(*) AS reading_count,
                   ROUND(AVG(temperature)::numeric, 2)   AS avg_temp,
                   ROUND(AVG(temperature_f)::numeric, 2) AS avg_temp_f,
                   ROUND(AVG(humidity)::numeric, 2)      AS avg_humidity,
                   ROUND(AVG(battery_level)::numeric, 1) AS avg_battery,
                   MAX(timestamp)  AS last_reading,
                   MAX(alert_level) AS max_alert
            FROM public.iot_sensor_readings
            WHERE building IS NOT NULL
            GROUP BY sensor_id, building, floor
            ORDER BY sensor_id""")
        return jsonify(rows)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/iot/recent")
def iot_recent():
    try:
        rows = q("""
            SELECT timestamp, sensor_id, building, floor,
                   temperature, temperature_f, humidity,
                   comfort_index, alert_level, status, battery_status
            FROM public.iot_sensor_readings
            ORDER BY timestamp DESC LIMIT 30""")
        return jsonify(rows)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/iot/flowserver_sql")
def iot_flowserver_sql():
    try:
        yaml_path = REPO_DIR / "jobs" / "iot-sensors-csv.yaml"
        content = yaml_path.read_text()
    except Exception:
        content = "# jobs/iot-sensors-csv.yaml not found"
    return jsonify({"yaml": content})


# ══════════════════════════════════════════════════════════════════════════════
#  SQL FILE SERVING  (for the DB Setup tab)
# ══════════════════════════════════════════════════════════════════════════════

SQL_FILES = {
    "01_create_database.sql": "sql/01_create_database.sql",
    "02_create_tables.sql":   "sql/02_create_tables.sql",
    "03_verify_setup.sql":    "sql/03_verify_setup.sql",
}

@app.route("/api/sql/<filename>")
def serve_sql(filename):
    """Serve raw SQL file content for display in the dashboard."""
    rel = SQL_FILES.get(filename)
    if not rel:
        return jsonify({"error": f"Unknown file: {filename}"}), 404
    try:
        content = (REPO_DIR / rel).read_text()
        return jsonify({"filename": filename, "content": content})
    except FileNotFoundError:
        return jsonify({"error": f"{rel} not found in repo at {REPO_DIR}"}), 404
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ══════════════════════════════════════════════════════════════════════════════
#  SERVE STATIC (dashboard.html)
# ══════════════════════════════════════════════════════════════════════════════

@app.route("/favicon.svg")
def favicon():
    return send_from_directory(str(Path(__file__).parent), "favicon.svg",
                               mimetype="image/svg+xml")

@app.route("/")
def root():
    return send_from_directory(str(Path(__file__).parent), "dashboard.html")


@app.route("/<path:filename>")
def static_files(filename):
    return send_from_directory(str(Path(__file__).parent), filename)


# ══════════════════════════════════════════════════════════════════════════════
#  MAIN
# ══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--pg-host",     default="localhost")
    parser.add_argument("--pg-port",     type=int, default=5432)
    parser.add_argument("--pg-user",     default="gpadmin")
    parser.add_argument("--pg-password", default="")
    parser.add_argument("--pg-dbname",   default="streaming_demo")
    parser.add_argument("--kafka",       default="localhost:9092")
    parser.add_argument("--app-port",    type=int, default=5055)
    parser.add_argument("--repo-dir",    default=None)
    args = parser.parse_args()

    DB_CONFIG.update({
        "host":     args.pg_host,
        "port":     args.pg_port,
        "user":     args.pg_user,
        "password": args.pg_password,
        "database": args.pg_dbname,
    })
    KAFKA_BROKER = args.kafka
    if args.repo_dir:
        REPO_DIR = Path(args.repo_dir).resolve()

    PID_DIR.mkdir(parents=True, exist_ok=True)
    LOG_DIR.mkdir(parents=True, exist_ok=True)

    # Register this process's own PID
    write_pid("dashboard-api", os.getpid())

    print(f"\n  WHPG Real Streaming Demo — Dashboard API")
    print(f"  DB      : {args.pg_user}@{args.pg_host}:{args.pg_port}/{args.pg_dbname}")
    print(f"  Kafka   : {KAFKA_BROKER}")
    print(f"  Repo    : {REPO_DIR}")
    print(f"  Open    : http://localhost:{args.app_port}/\n")

    app.run(host="0.0.0.0", port=args.app_port, debug=False, threaded=True)


