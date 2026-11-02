# FastAPI / Uvicorn performance benchmark

A small, reproducible benchmark of how common configuration choices affect
latency and throughput of a FastAPI service running under Uvicorn.

## What it measures

| Factor | Levels |
|---|---|
| Handler type | `def` (sync, runs in a thread pool) vs `async def` |
| Response size | small (about 70 bytes) vs large (about 90 KB, 1000 objects) |
| JSON serializer | standard-library `json` vs `orjson` |
| Uvicorn workers | 1, 2, 4 |
| Waiting 5 ms on I/O | `await asyncio.sleep` vs `time.sleep` in a sync handler vs `time.sleep` inside `async def` (blocks the event loop) |

Metrics per run: requests per second, mean / p50 / p95 / p99 latency, and error count.

## Setup

Python 3.10 or newer.

```
python -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

## Run

```
python run_benchmark.py --quick   # smoke test, about 5 minutes
python run_benchmark.py           # full run, about 30-40 minutes
python analyze.py                 # summary table and results/summary.csv
```

Output goes to `results/`:

- `results.csv` - one row per run (raw data)
- `meta.json` - Python version, package versions, CPU count, settings
- `summary.csv` - mean and 95% confidence interval per configuration

## For trustworthy numbers

- Plug in the power and disable sleep. Close browsers and other heavy apps.
- Don't use the machine while the benchmark runs.
- Prefer Linux, macOS or WSL2. Native Windows has coarser timers (which distorts
  the 5 ms wait tests), and uvloop is not available there.
- Run on a machine with at least 4 logical CPUs. With fewer, the 4-worker results
  are oversubscribed, because the server workers and the load generator share cores.
- Check the `client_bound` column in `results.csv`. If it is 1, the load generator
  used more than 90% of a CPU, and throughput in that run may be limited by the
  client rather than the server. Re-run with `--procs 2` or more.

## Troubleshooting

- **Output stops scrolling (Windows):** clicking or selecting text inside the
  terminal window pauses the program. Press Esc or Enter to resume. Don't click
  in the window while the benchmark runs.
- **To stop a run:** press Ctrl+C once. Then check Task Manager for leftover
  `python.exe` processes and end them before starting again.
- **A connection that stops responding** is cancelled 15 seconds after the run
  window closes and counted in the `errors` column, instead of hanging the run.
- **The run stopped or failed part-way:** run the same command again with `--resume`
  added (`python run_benchmark.py --resume`). It keeps `results/results.csv`, skips
  finished measurements and continues with the same planned order. Use the same
  settings as the first run.
- **Port:** each server start picks a free port automatically. To force one, add
  `--port 8800`.

## How it works

- `app.py` - the service under test. Handlers return Response objects directly,
  so FastAPI's `jsonable_encoder` step is skipped and the serializer is the only
  thing that differs between the two serializer settings.
- `loadgen.py` - closed-loop load generator (standard library only). 64 keep-alive
  connections by default; each sends its next request when the previous response
  completes. The first 3 seconds of each run are discarded as warm-up. Connections
  open 10 ms apart and retry with backoff, because Windows can reset a burst of
  simultaneous connects. A connection that fails is counted (inside the measured
  window only), its error type is printed, and it reconnects so the concurrency
  level stays constant.
- `run_benchmark.py` - starts Uvicorn for each configuration, runs every endpoint,
  stops it. Each repeat visits every configuration in a shuffled order, so slow
  drift on the machine is spread across configurations. After each server start
  it asks `/pid` repeatedly to confirm the requested number of workers is really
  answering, and restarts the server (up to 3 attempts) if not. The
  `workers_seen` column in `results.csv` records what it found.

## Limitations

- Results describe one machine, one load pattern and one set of package versions
  (see `meta.json`). They don't transfer exactly to other hardware.
- The load generator runs on the same machine as the server.
- The test service does no database or network I/O. The 5 ms wait is simulated.
- Closed-loop load at fixed concurrency measures behavior at saturation. It does
  not model real traffic arrival patterns.

## Testing note

The load generator, runner and analysis script were tested end to end against a
stand-in Starlette app with the same routes (27 runs, 0 errors). `app.py` itself
could not be executed in the environment where the code was written, because
FastAPI couldn't be installed there. If `python run_benchmark.py --quick` fails
at startup, the error message will point to the cause.
