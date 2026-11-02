#!/usr/bin/env python3
"""Run the full benchmark matrix and write raw results to results/results.csv.

Usage:
    python run_benchmark.py --quick      # short smoke test (a few minutes)
    python run_benchmark.py              # full run (about 30-40 minutes)
    python run_benchmark.py --resume     # continue an unfinished run

Every repeat visits every server configuration once, in a shuffled order,
so slow drift on the machine (thermal throttling, background load) is
spread across configurations instead of landing on one of them.
"""
import argparse
import csv
import json
import os
import platform
import random
import signal
import socket
import subprocess
import sys
import time
import urllib.request
from datetime import datetime, timezone
from importlib import metadata
from pathlib import Path

import loadgen

HERE = Path(__file__).resolve().parent

# Group A: handler type x payload size x serializer x worker count.
GROUP_A = ["/small-sync", "/small-async", "/large-sync", "/large-async"]
# Group B: three ways of waiting 5 ms, run with 1 worker and the default serializer.
GROUP_B = ["/io-async", "/io-sync", "/io-blocking-async"]
SERIALIZERS = ["default", "orjson"]
WORKERS = [1, 2, 4]

FIELDS = [
    "repeat", "serializer", "workers", "workers_seen", "endpoint", "loop", "concurrency",
    "client_procs", "requests", "errors", "rps", "mean_ms", "p50_ms",
    "p95_ms", "p99_ms", "client_cpu_max", "client_bound",
]


def pkg_version(name):
    try:
        return metadata.version(name)
    except metadata.PackageNotFoundError:
        return None


def start_server(app, app_dir, port, workers, serializer, loop):
    env = dict(os.environ, SERIALIZER=serializer)
    cmd = [
        sys.executable, "-m", "uvicorn", app,
        "--app-dir", str(app_dir),
        "--host", "127.0.0.1", "--port", str(port),
        "--workers", str(workers),
        "--log-level", "warning", "--no-access-log",
        "--loop", loop,
    ]
    kwargs = {}
    if os.name == "nt":
        kwargs["creationflags"] = subprocess.CREATE_NEW_PROCESS_GROUP
    else:
        kwargs["start_new_session"] = True
    return subprocess.Popen(cmd, env=env, cwd=str(app_dir), **kwargs)


def wait_ready(port, proc, timeout=40.0):
    url = f"http://127.0.0.1:{port}/small-sync"
    deadline = time.time() + timeout
    while time.time() < deadline:
        if proc.poll() is not None:
            raise RuntimeError(
                "The server exited during startup. Check that the packages in "
                "requirements.txt are installed and that the port is free."
            )
        try:
            with urllib.request.urlopen(url, timeout=1) as resp:
                if resp.status == 200:
                    time.sleep(1.0)  # let every worker finish starting
                    return
        except Exception:
            time.sleep(0.25)
    raise RuntimeError("The server did not become ready in time.")


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def port_open(port):
    try:
        with socket.create_connection(("127.0.0.1", port), timeout=0.5):
            return True
    except OSError:
        return False


def stop_server(proc, port=None):
    """Stop the server and its workers, then wait until the port is closed."""
    if proc.poll() is None:
        try:
            if os.name == "nt":
                proc.send_signal(signal.CTRL_BREAK_EVENT)
            else:
                os.killpg(proc.pid, signal.SIGTERM)
            proc.wait(timeout=6)
        except (subprocess.TimeoutExpired, ProcessLookupError, OSError):
            pass
    if proc.poll() is None or os.name == "nt":
        # Make sure no worker survives (on Windows this also kills the children).
        if os.name == "nt":
            subprocess.run(["taskkill", "/F", "/T", "/PID", str(proc.pid)],
                           capture_output=True)
        else:
            try:
                os.killpg(proc.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            pass
    if port is not None:
        deadline = time.time() + 15
        while port_open(port) and time.time() < deadline:
            time.sleep(0.25)


def count_workers(port, expected, tries=400):
    """Ask /pid repeatedly (new connection each time) and count distinct worker PIDs."""
    seen = set()
    for _ in range(tries):
        try:
            with urllib.request.urlopen(f"http://127.0.0.1:{port}/pid", timeout=2) as resp:
                seen.add(resp.read().decode().strip())
        except Exception:
            pass
        if len(seen) >= expected:
            break
    return len(seen)


def launch(args, serializer, workers, port):
    """Start the server and confirm the requested number of workers is answering.

    A worker can fail to start (seen on Windows: WinError 10022), which would
    silently turn a "2 workers" run into a 1-worker run. If fewer workers answer
    than requested, restart the server, up to 3 attempts, and record the result.
    """
    for attempt in range(1, 4):
        proc = start_server(args.app, args.app_dir, port, workers, serializer, args.loop)
        try:
            wait_ready(port, proc)
            seen = count_workers(port, workers)
        except Exception:
            stop_server(proc, port)
            raise
        if seen >= workers:
            return proc, seen
        if attempt == 3:
            print(f"  WARNING: only {seen} of {workers} workers answered after 3 attempts",
                  flush=True)
            return proc, seen
        print(f"  only {seen} of {workers} workers answered; restarting "
              f"(attempt {attempt}/3)", flush=True)
        stop_server(proc, port)
        time.sleep(1.0)


def build_server_configs():
    configs = []
    for serializer in SERIALIZERS:
        for workers in WORKERS:
            endpoints = list(GROUP_A)
            if serializer == "default" and workers == 1:
                endpoints += GROUP_B
            configs.append((serializer, workers, endpoints))
    return configs


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--repeats", type=int, default=5)
    ap.add_argument("--duration", type=float, default=10.0, help="measured seconds per run")
    ap.add_argument("--warmup", type=float, default=3.0, help="discarded seconds per run")
    ap.add_argument("--concurrency", type=int, default=64, help="simultaneous connections")
    ap.add_argument("--procs", type=int, default=0, help="client processes (0 = automatic)")
    ap.add_argument("--port", type=int, default=0,
                    help="server port (0 = pick a free port for every server start)")
    ap.add_argument("--resume", action="store_true",
                    help="continue an unfinished run: keep results.csv and skip finished rows")
    ap.add_argument("--loop", default="auto", choices=["auto", "asyncio", "uvloop"],
                    help="event loop used by the Uvicorn server")
    ap.add_argument("--app", default="app:app")
    ap.add_argument("--app-dir", default=str(HERE))
    ap.add_argument("--out", default=str(HERE / "results"))
    ap.add_argument("--seed", type=int, default=12345)
    ap.add_argument("--quick", action="store_true",
                    help="2 repeats, 4 s per run, 1 s warm-up")
    args = ap.parse_args()

    if args.quick:
        args.repeats, args.duration, args.warmup = 2, 4.0, 1.0
    cpus = os.cpu_count() or 2
    procs = args.procs or max(1, min(4, cpus // 2))

    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)
    csv_path = out_dir / "results.csv"
    meta_path = out_dir / "meta.json"
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

    finished = set()
    resuming = args.resume and csv_path.exists() and meta_path.exists()
    if resuming:
        meta = json.loads(meta_path.read_text())
        old = meta["args"]
        for k in ("repeats", "duration", "warmup", "concurrency", "seed", "loop", "quick"):
            if old.get(k) != getattr(args, k):
                sys.exit(f"--resume needs the same settings as the unfinished run "
                         f"({k}: was {old.get(k)}, now {getattr(args, k)}).")
        procs = meta["client_procs"]
        with open(csv_path, newline="") as fh:
            for r in csv.DictReader(fh):
                finished.add((int(r["repeat"]), r["serializer"], int(r["workers"]), r["endpoint"]))
        meta.setdefault("resumed_utc", []).append(stamp)
        print(f"Resuming: {len(finished)} measurements already in {csv_path}")
    else:
        if csv_path.exists():
            csv_path.rename(out_dir / f"results_{stamp}_old.csv")
        meta = {
            "started_utc": stamp,
            "python": sys.version,
            "platform": platform.platform(),
            "processor": platform.processor(),
            "cpu_count_logical": cpus,
            "packages": {n: pkg_version(n) for n in
                         ["fastapi", "starlette", "uvicorn", "orjson", "uvloop", "httptools"]},
            "client_loop": loadgen.CLIENT_LOOP,
            "client_procs": procs,
            "args": vars(args),
        }
    meta_path.write_text(json.dumps(meta, indent=2))
    print(json.dumps({k: meta[k] for k in ("platform", "cpu_count_logical", "packages")}, indent=2))
    if cpus < 4:
        print("NOTE: fewer than 4 logical CPUs. The 4-worker results will be oversubscribed.")

    configs = build_server_configs()
    total = args.repeats * sum(len(c[2]) for c in configs)
    done = len(finished)
    failures = []
    t_start = time.time()

    new_file = not (resuming and csv_path.stat().st_size > 0)
    with open(csv_path, "w" if new_file else "a", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=FIELDS)
        if new_file:
            writer.writeheader()
        for rep in range(1, args.repeats + 1):
            rng = random.Random(args.seed + rep)
            order = configs[:]
            rng.shuffle(order)
            for serializer, workers, endpoints in order:
                # Shuffle first, so the order is identical whether or not rows are skipped.
                eps = endpoints[:]
                rng.shuffle(eps)
                todo = [ep for ep in eps if (rep, serializer, workers, ep) not in finished]
                if not todo:
                    continue
                port = args.port or free_port()
                print(f"-- rep {rep}: starting {serializer} server, {workers} worker(s), "
                      f"port {port} (takes up to ~20 s)", flush=True)
                try:
                    proc, seen = launch(args, serializer, workers, port)
                except Exception as exc:
                    print(f"  FAILED to start this server: {exc}", flush=True)
                    failures.append((rep, serializer, workers, "(server start)"))
                    continue
                try:
                    for ep in todo:
                        print(f"   running {ep} ...", flush=True)
                        try:
                            stats = loadgen.run_load("127.0.0.1", port, ep,
                                                     args.concurrency, args.warmup,
                                                     args.duration, procs)
                        except Exception as exc:
                            print(f"  FAILED {ep}: {exc!r}", flush=True)
                            failures.append((rep, serializer, workers, ep))
                            continue
                        row = dict(
                            repeat=rep, serializer=serializer, workers=workers, workers_seen=seen,
                            endpoint=ep, loop=args.loop, concurrency=args.concurrency,
                            client_procs=procs,
                            client_bound=int(stats["client_cpu_max"] > 0.90),
                            **stats,
                        )
                        writer.writerow({k: (round(v, 4) if isinstance(v, float) else v)
                                         for k, v in row.items()})
                        fh.flush()
                        done += 1
                        print(f"[{done}/{total}] rep {rep} {serializer:7} w={workers} "
                              f"{ep:19} rps={stats['rps']:8.0f} "
                              f"p50={stats['p50_ms']:7.2f}ms p99={stats['p99_ms']:8.2f}ms "
                              f"client_cpu={stats['client_cpu_max']:.2f} err={stats['errors']}",
                              flush=True)
                finally:
                    print("   stopping server ...", flush=True)
                    stop_server(proc, port)

    meta["elapsed_minutes_this_session"] = round((time.time() - t_start) / 60, 1)
    if not failures:
        meta["finished_utc"] = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    meta_path.write_text(json.dumps(meta, indent=2))
    if failures:
        print(f"\n{len(failures)} step(s) failed:")
        for f in failures:
            print("  ", f)
        print("Run the same command again with --resume added to fill the gaps.")
        sys.exit(1)
    print(f"\nDone. {done}/{total} measurements in {csv_path}")
    print("Next: python analyze.py")


if __name__ == "__main__":
    main()
