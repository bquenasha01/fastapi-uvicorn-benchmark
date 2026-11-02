"""Small closed-loop HTTP load generator (standard library only).

Each simulated client holds one keep-alive connection and sends the next
request as soon as the previous response is complete. Latency is measured
per request with a monotonic clock. Requests started during the warm-up
window are discarded.

The generator also reports how busy its own CPU was during the measured
window. If that is close to 1.0 the client, not the server, was the
bottleneck, and the run should not be trusted for throughput.
"""
import asyncio
import collections
import math
import multiprocessing as mp
import time

try:  # optional: a faster event loop for the client
    import uvloop

    _LOOP_FACTORY = uvloop.new_event_loop
    CLIENT_LOOP = "uvloop"
except ImportError:
    _LOOP_FACTORY = None
    CLIENT_LOOP = "asyncio"

# Seconds a connection may stay busy after the measured window closes before it
# is cancelled and counted as an error. This stops one stalled connection from
# hanging an entire run.
GRACE_S = 15.0


CONNECT_STAGGER_S = 0.01   # spread connection opens so the server's accept queue is not hit all at once
CONNECT_RETRIES = 6


async def _open(host, port):
    """Open a connection, retrying with backoff (Windows can reset a burst of connects)."""
    for attempt in range(CONNECT_RETRIES):
        try:
            return await asyncio.open_connection(host, port)
        except OSError:
            if attempt == CONNECT_RETRIES - 1:
                raise
            await asyncio.sleep(0.05 * 2 ** attempt)


async def _connection(idx, host, port, path, record_from, stop_at, lat, err, kinds):
    request = (
        f"GET {path} HTTP/1.1\r\nHost: {host}:{port}\r\n"
        "Connection: keep-alive\r\n\r\n"
    ).encode()
    await asyncio.sleep(idx * CONNECT_STAGGER_S)
    writer = None
    while time.perf_counter_ns() < stop_at:
        try:
            if writer is None:
                reader, writer = await _open(host, port)
            t0 = time.perf_counter_ns()
            if t0 >= stop_at:
                break
            writer.write(request)
            head = await reader.readuntil(b"\r\n\r\n")
            ok = head.startswith(b"HTTP/1.1 200")
            length = -1
            for line in head.split(b"\r\n"):
                if line[:15].lower() == b"content-length:":
                    length = int(line[15:])
                    break
            if length < 0:
                raise RuntimeError("response without Content-Length")
            await reader.readexactly(length)
            t1 = time.perf_counter_ns()
            if t0 >= record_from:
                if ok:
                    lat.append(t1 - t0)
                else:
                    err[0] += 1
                    kinds[f"HTTP status {head[9:12].decode(errors='replace')}"] += 1
        except (asyncio.IncompleteReadError, ConnectionError, RuntimeError, OSError) as exc:
            # Count only errors inside the measured window, remember what kind
            # they were, then reconnect so the concurrency level stays constant.
            if time.perf_counter_ns() >= record_from:
                err[0] += 1
                kinds[f"{type(exc).__name__}: {exc}"[:80]] += 1
            if writer is not None:
                writer.close()
            writer = None
            await asyncio.sleep(0.05)
    if writer is not None:
        writer.close()


async def _cpu_probe(record_from, stop_at, out):
    await asyncio.sleep(max(0.0, (record_from - time.perf_counter_ns()) / 1e9))
    cpu0, wall0 = time.process_time(), time.perf_counter()
    await asyncio.sleep(max(0.0, (stop_at - time.perf_counter_ns()) / 1e9))
    wall = time.perf_counter() - wall0
    out["cpu"] = (time.process_time() - cpu0) / wall if wall > 0 else 0.0


async def _run(host, port, path, conc, warmup, duration):
    start = time.perf_counter_ns()
    record_from = start + int(warmup * 1e9)
    stop_at = record_from + int(duration * 1e9)
    lat, err, probe, kinds = [], [0], {}, collections.Counter()
    conns = [
        asyncio.create_task(_connection(i, host, port, path, record_from, stop_at, lat, err, kinds))
        for i in range(conc)
    ]
    cpu_task = asyncio.create_task(_cpu_probe(record_from, stop_at, probe))
    budget = max(0.0, (stop_at - time.perf_counter_ns()) / 1e9) + GRACE_S
    _, pending = await asyncio.wait(conns, timeout=budget)
    for task in pending:  # stalled connections: cancel and count as errors
        task.cancel()
    if pending:
        err[0] += len(pending)
        await asyncio.gather(*pending, return_exceptions=True)
    await cpu_task
    if kinds:
        print(f"  loadgen errors on {path}: {dict(kinds)}", flush=True)
    return lat, err[0], probe.get("cpu", 0.0)


def _client_proc(host, port, path, conc, warmup, duration):
    coro = _run(host, port, path, conc, warmup, duration)
    if _LOOP_FACTORY is None:
        return asyncio.run(coro)
    loop = _LOOP_FACTORY()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


def _percentile(sorted_ns, p):
    # Nearest-rank percentile on a sorted list, returned in milliseconds.
    idx = min(len(sorted_ns) - 1, max(0, math.ceil(p / 100 * len(sorted_ns)) - 1))
    return sorted_ns[idx] / 1e6


def run_load(host, port, path, concurrency, warmup, duration, procs=1):
    """Drive `path` and return a dict of throughput and latency numbers."""
    per_proc = max(1, -(-concurrency // procs))
    job = (host, port, path, per_proc, warmup, duration)
    if procs == 1:
        results = [_client_proc(*job)]
    else:
        with mp.get_context("spawn").Pool(procs) as pool:
            # Bounded wait: a crashed client process raises instead of hanging.
            results = pool.starmap_async(_client_proc, [job] * procs).get(
                timeout=warmup + duration + GRACE_S + 30)

    lat, errors, cpu = [], 0, 0.0
    for l, e, c in results:
        lat.extend(l)
        errors += e
        cpu = max(cpu, c)
    lat.sort()
    n = len(lat)
    if n == 0:
        return dict(requests=0, errors=errors, rps=0.0, mean_ms=0.0,
                    p50_ms=0.0, p95_ms=0.0, p99_ms=0.0, client_cpu_max=cpu)
    return dict(
        requests=n,
        errors=errors,
        rps=n / duration,
        mean_ms=sum(lat) / n / 1e6,
        p50_ms=_percentile(lat, 50),
        p95_ms=_percentile(lat, 95),
        p99_ms=_percentile(lat, 99),
        client_cpu_max=cpu,
    )
