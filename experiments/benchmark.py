"""Run every query in several scan modes on each file variant and record the engine's measurements.

Modes: default (needed columns, pruning), no_prune, all_columns (every column, no pruning: the
row-store-like baseline), and the two experimental dictionary decoders. Each run is checked against
DuckDB, then timed as the median of --repeats runs after a warm-up. With --model, the engine's
metadata-only prediction is recorded next to the measurement.
"""

import argparse
import csv
import datetime as dt
import json
import re
import statistics
import subprocess
import time
from pathlib import Path

import duckdb

ALL_COLUMNS = (
    "l_orderkey,l_partkey,l_suppkey,l_linenumber,l_quantity,l_extendedprice_cents,"
    "l_discount_pct,l_tax_pct,l_shipdate,l_commitdate,l_receiptdate"
)
PROJECTION = "l_extendedprice_cents,l_discount_pct"
SHIP_FIRST, SHIP_LAST = dt.date(1992, 1, 2), dt.date(1998, 12, 1)
SHIP_FRACTIONS = (0.001, 0.01, 0.05, 0.1, 0.25, 0.5, 1.0)
ORDERKEY_FRACTIONS = (0.001, 0.01, 0.1, 0.5)
FILE_PATTERN = re.compile(r"lineitem_(?P<order>\w+?)_(?P<encoding>dict|plain)_rg(?P<row_group>\d+)\.parquet")
FIELDS = [
    "file", "order", "encoding", "row_group", "query", "family", "mode", "selectivity", "pruned_groups",
    "planned_bytes", "actual_bytes", "file_bytes", "execution_ms", "read_ms", "decode_ms", "filter_ms",
    "wall_ms", "duckdb_ms", "predicted_ms", "predicted_decode_ms",
]


def make_queries(max_orderkey):
    """(name, family, projection, engine predicate flags, SQL predicate, SQL parameters)."""
    queries = []
    span = (SHIP_LAST - SHIP_FIRST).days
    for fraction in SHIP_FRACTIONS:
        width = max(1, round(fraction * span))
        low = SHIP_FIRST + dt.timedelta(days=(span - width) // 2)
        high = low + dt.timedelta(days=width - 1)
        flags = ["--where", "l_shipdate", "--ge", low.isoformat(), "--le", high.isoformat()]
        queries.append((f"ship_{fraction:g}", "ship", PROJECTION, flags, "l_shipdate BETWEEN ? AND ?", [low, high]))
    for fraction in ORDERKEY_FRACTIONS:
        width = max(1, round(fraction * max_orderkey))
        low = 1 + (max_orderkey - width) // 2
        high = low + width - 1
        flags = ["--where", "l_orderkey", "--ge", str(low), "--le", str(high)]
        queries.append((f"okey_{fraction:g}", "orderkey", PROJECTION, flags, "l_orderkey BETWEEN ? AND ?", [low, high]))
    queries.append(("discount_eq", "discount", "l_extendedprice_cents", ["--where", "l_discount_pct", "--eq", "5"],
                    "l_discount_pct = ?", [5]))
    queries.append(("full_scan", "full", "l_quantity", [], None, []))
    return queries


def modes(encoding, family):
    """Scan modes for a query: (name, projection override, extra flags)."""
    selected = [("default", None, [])]
    if family != "full":
        selected.append(("no_prune", None, ["--no-prune"]))
    if family in ("ship", "orderkey"):
        selected.append(("all_columns", ALL_COLUMNS, ["--no-prune"]))
    if encoding == "dict" and family in ("full", "discount"):
        selected += [("naive_decoder", None, ["--naive-decoder"]), ("separate_ids", None, ["--separate-ids"])]
    return selected


def engine(binary, command, path, args):
    started = time.perf_counter()
    done = subprocess.run([binary, command, str(path), *args], capture_output=True, text=True)
    if done.returncode != 0:
        raise RuntimeError(f"{command} {path.name} {' '.join(args)}: {done.stderr.strip()}")
    return json.loads(done.stdout), (time.perf_counter() - started) * 1000


def duckdb_count(con, path, sql, params):
    started = time.perf_counter()
    where = f" WHERE {sql}" if sql else ""
    count = con.execute(f"SELECT count(*) FROM read_parquet(?){where}", [str(path), *params]).fetchone()[0]
    return count, (time.perf_counter() - started) * 1000


def measure(binary, path, args, repeats):
    """Median engine stage timings and process wall time, after one warm-up run."""
    runs = [engine(binary, "scan", path, [*args, "--timing"]) for _ in range(repeats + 1)][1:]
    timing = {f"{k}_ms": statistics.median(out["timing_ms"][k] for out, _ in runs)
              for k in ("execution", "read", "decode", "filter")}
    return {**timing, "wall_ms": statistics.median(wall for _, wall in runs)}


def rows(args):
    con = duckdb.connect()
    con.execute("SET threads = 1")
    files = sorted(p for p in args.data.glob("*.parquet") if FILE_PATTERN.fullmatch(p.name))
    if not files:
        raise SystemExit(f"no lineitem variants in {args.data}")
    max_orderkey, total_rows = con.execute(
        "SELECT max(l_orderkey), count(*) FROM read_parquet(?)", [str(files[0])]
    ).fetchone()
    for path in files:
        layout = FILE_PATTERN.fullmatch(path.name).groupdict()
        for name, family, projection, flags, sql, params in make_queries(max_orderkey):
            expected, _ = duckdb_count(con, path, sql, params)
            duckdb_ms = statistics.median(duckdb_count(con, path, sql, params)[1] for _ in range(args.repeats))
            for mode, override, extra in modes(layout["encoding"], family):
                query = ["--select", override or projection, *flags, *extra]
                result, _ = engine(args.bin, "scan", path, [*query, "--verify"])
                if result["matched_rows"] != expected:
                    raise AssertionError(f"{path.name} {name} {mode}: {result['matched_rows']} rows, DuckDB {expected}")
                row = {
                    "file": path.name, **layout, "query": name, "family": family, "mode": mode,
                    "selectivity": expected / total_rows, "pruned_groups": result["pruned_groups"],
                    "planned_bytes": result["io"]["planned_chunk_bytes"],
                    "actual_bytes": result["io"]["actual_chunk_bytes"], "file_bytes": path.stat().st_size,
                    **measure(args.bin, path, query, args.repeats), "duckdb_ms": duckdb_ms,
                }
                if args.model and mode in ("default", "no_prune", "all_columns"):
                    predicted, _ = engine(args.bin, "predict", path, [*query, "--model", str(args.model)])
                    row["predicted_ms"] = predicted["predicted_ms"]["total"]
                    row["predicted_decode_ms"] = predicted["predicted_ms"]["decode"]
                yield row
        print(f"done {path.name}", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bin", required=True, help="parquet_metadata executable")
    parser.add_argument("--data", type=Path, required=True, help="directory written by generate.py")
    parser.add_argument("--out", type=Path, required=True, help="output CSV")
    parser.add_argument("--model", type=Path, help="cost model from `parquet_metadata calibrate`")
    parser.add_argument("--repeats", type=int, default=5)
    args = parser.parse_args()
    with args.out.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=FIELDS)
        writer.writeheader()
        for row in rows(args):
            writer.writerow(row)


if __name__ == "__main__":
    main()
