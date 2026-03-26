#!/usr/bin/env python3
"""
run.py
───────
Orchestrator — starts all three services in parallel sub-processes.

Usage:
    python run.py                # start everything
    python run.py --api          # API only
    python run.py --poller       # Poller only
    python run.py --processor    # Processor only
"""

import argparse
import os
import signal
import subprocess
import sys
import time
from pathlib import Path


# ─── ANSI colour helpers ──────────────────────────────────────────────────────
def _c(code: str, text: str) -> str:
    return f"\033[{code}m{text}\033[0m"

CYAN   = lambda t: _c("96",   t)
GREEN  = lambda t: _c("92",   t)
YELLOW = lambda t: _c("93",   t)
RED    = lambda t: _c("91",   t)
BOLD   = lambda t: _c("1",    t)
DIM    = lambda t: _c("2",    t)


BANNER = f"""
{CYAN('╔═══════════════════════════════════════════════════════════╗')}
{CYAN('║')}   {BOLD('🚀  GitHub Event Intelligence System')}                    {CYAN('║')}
{CYAN('║')}   {DIM('Real-time · NLP-enriched · WebSocket-powered')}             {CYAN('║')}
{CYAN('╚═══════════════════════════════════════════════════════════╝')}
"""


# ─── Service definitions ──────────────────────────────────────────────────────
SERVICES = {
    "api": {
        "label":   "API Server",
        "cmd":     [sys.executable, "-m", "uvicorn", "api.main:app",
                    "--host", "0.0.0.0", "--port", "8000",
                    "--reload", "--log-level", "info"],
        "color":   CYAN,
        "delay":   0,
    },
    "processor": {
        "label":   "NLP Processor",
        "cmd":     [sys.executable, "processor/event_processor.py"],
        "color":   GREEN,
        "delay":   2,      # wait for API/MySQL to be ready
    },
    "poller": {
        "label":   "GitHub Poller",
        "cmd":     [sys.executable, "ingestion/github_poller.py"],
        "color":   YELLOW,
        "delay":   3,      # start last — everything else must be ready
    },
}


# ─── Pre-flight checks ────────────────────────────────────────────────────────
def check_env() -> bool:
    ok = True
    env_path = Path(".env")
    if not env_path.exists():
        print(YELLOW("  ⚠  .env file not found — copying from .env.example"))
        example = Path(".env.example")
        if example.exists():
            import shutil
            shutil.copy(example, env_path)
            print(GREEN("  ✓  .env created — please fill in GITHUB_TOKEN & DB credentials\n"))
        else:
            print(RED("  ✗  .env.example not found. Run from the project root.\n"))
            ok = False
    return ok


def check_requirements() -> bool:
    try:
        import fastapi, redis, mysql.connector, textblob  # noqa: F401
        return True
    except ImportError as e:
        print(RED(f"  ✗  Missing dependency: {e}"))
        print(YELLOW("  →  Run:  pip install -r requirements.txt\n"))
        return False


# ─── Main ────────────────────────────────────────────────────────────────────
def run(services_to_start: list[str]) -> None:
    print(BANNER)

    if not check_requirements():
        sys.exit(1)
    check_env()

    print(BOLD("  Starting services…\n"))
    procs: list[tuple[str, subprocess.Popen]] = []

    try:
        for key in services_to_start:
            svc   = SERVICES[key]
            color = svc["color"]
            delay = svc["delay"]

            if delay:
                time.sleep(delay)

            print(f"  {color('▶')} Starting {color(svc['label'])}…")
            proc = subprocess.Popen(
                svc["cmd"],
                cwd=str(Path(__file__).parent),
            )
            procs.append((svc["label"], proc))
            print(f"    {DIM(f'PID {proc.pid}')}")

        print()
        print(BOLD("  ✅  All services running!\n"))
        print(f"  {CYAN('📊  Dashboard')}  →  http://localhost:8000")
        print(f"  {CYAN('📡  API')}        →  http://localhost:8000/api")
        print(f"  {CYAN('📚  API Docs')}   →  http://localhost:8000/docs")
        print(f"  {CYAN('❤️   Health')}     →  http://localhost:8000/health")
        print()
        print(DIM("  Press Ctrl+C to stop all services\n"))

        # Wait for all processes
        while True:
            for label, proc in procs:
                ret = proc.poll()
                if ret is not None:
                    print(RED(f"\n  ⚠  {label} exited (code {ret}) — restarting not implemented"))
            time.sleep(2)

    except KeyboardInterrupt:
        print(f"\n{YELLOW('  Shutting down all services…')}")
        for label, proc in procs:
            try:
                proc.terminate()
                print(f"  {DIM(f'✓ {label} stopped')}")
            except Exception:
                pass
        # Allow graceful shutdown
        time.sleep(1)
        for _, proc in procs:
            try:
                proc.kill()
            except Exception:
                pass
        print(GREEN("  Goodbye! 👋\n"))


# ─── CLI ──────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Event Intelligence System Orchestrator")
    parser.add_argument("--api",       action="store_true", help="Start API server only")
    parser.add_argument("--poller",    action="store_true", help="Start GitHub poller only")
    parser.add_argument("--processor", action="store_true", help="Start NLP processor only")
    args = parser.parse_args()

    selected = []
    if args.api:       selected.append("api")
    if args.processor: selected.append("processor")
    if args.poller:    selected.append("poller")
    if not selected:   selected = ["api", "processor", "poller"]   # default: all

    run(selected)