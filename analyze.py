#!/usr/bin/env python3
"""Summarize results/results.csv: mean and 95% confidence interval across repeats.

Usage:  python analyze.py [path/to/results.csv]
Writes results/summary.csv and prints a Markdown table.
"""
import csv
import statistics
import sys
from collections import defaultdict
from pathlib import Path

HERE = Path(__file__).resolve().parent
# Two-sided 95% t critical values by degrees of freedom (n - 1).
T95 = {1: 12.706, 2: 4.303, 3: 3.182, 4: 2.776, 5: 2.571,
       6: 2.447, 7: 2.365, 8: 2.306, 9: 2.262}


def ci95(values):
    n = len(values)
    if n < 2:
        return 0.0
    t = T95.get(n - 1, 2.0)
    return t * statistics.stdev(values) / n ** 0.5


def main():
    path = Path(sys.argv[1]) if len(sys.argv) > 1 else HERE / "results" / "results.csv"
    groups = defaultdict(list)
    with open(path, newline="") as fh:
        for row in csv.DictReader(fh):
            groups[(row["endpoint"], row["serializer"], int(row["workers"]))].append(row)

    summary = []
    for (endpoint, serializer, workers), rows in sorted(groups.items()):
        rps = [float(r["rps"]) for r in rows]
        summary.append({
            "endpoint": endpoint, "serializer": serializer, "workers": workers,
            "n": len(rows),
            "rps_mean": round(statistics.mean(rps), 1),
            "rps_ci95": round(ci95(rps), 1),
            "p50_ms": round(statistics.mean(float(r["p50_ms"]) for r in rows), 3),
            "p95_ms": round(statistics.mean(float(r["p95_ms"]) for r in rows), 3),
            "p99_ms": round(statistics.mean(float(r["p99_ms"]) for r in rows), 3),
            "errors": sum(int(r["errors"]) for r in rows),
            "client_bound_runs": sum(int(r["client_bound"]) for r in rows),
            "short_worker_runs": sum(
                1 for r in rows if int(r.get("workers_seen") or workers) < workers),
        })

    out = path.parent / "summary.csv"
    with open(out, "w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(summary[0]))
        writer.writeheader()
        writer.writerows(summary)

    print("| endpoint | serializer | workers | n | req/s (mean ± 95% CI) | p50 ms | p95 ms | p99 ms | errors | client-bound runs |")
    print("|---|---|---|---|---|---|---|---|---|---|")
    for s in summary:
        print(f"| {s['endpoint']} | {s['serializer']} | {s['workers']} | {s['n']} | "
              f"{s['rps_mean']:.0f} ± {s['rps_ci95']:.0f} | {s['p50_ms']} | {s['p95_ms']} | "
              f"{s['p99_ms']} | {s['errors']} | {s['client_bound_runs']} |")
    print(f"\nWrote {out}")
    short = sum(s["short_worker_runs"] for s in summary)
    if short:
        print(f"WARNING: {short} run(s) had fewer server workers answering than requested. "
              "Treat worker-scaling results for those runs with caution.")
    flagged = sum(s["client_bound_runs"] for s in summary)
    if flagged:
        print(f"WARNING: {flagged} run(s) had the load generator above 90% CPU. "
              "Throughput in those runs may be limited by the client, not the server.")


if __name__ == "__main__":
    main()
