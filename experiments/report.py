"""Summarize a benchmark.py CSV as Markdown tables."""

import argparse
import csv
import math
import statistics
from pathlib import Path

ORDERS = ("random", "orderkey", "partial365", "partial30", "shipdate")


def table(header, body):
    lines = ["| " + " | ".join(map(str, header)) + " |", "|" + "---|" * len(header)]
    return "\n".join(lines + ["| " + " | ".join(map(str, row)) + " |" for row in body]) + "\n"


def geomean(values):
    return math.exp(statistics.fmean(math.log(v) for v in values))


def load(path):
    with path.open(newline="") as handle:
        rows = list(csv.DictReader(handle))
    for row in rows:
        for key, value in row.items():
            if key == "row_group":
                row[key] = int(value)
            elif key not in ("file", "order", "encoding", "query", "family", "mode") and value != "":
                row[key] = float(value)
    return rows


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("csv", type=Path)
    rows = load(parser.parse_args().csv)
    at = {(r["order"], r["encoding"], r["row_group"], r["query"], r["mode"]): r for r in rows}
    orders = [o for o in ORDERS if any(r["order"] == o for r in rows)]
    groups = sorted({r["row_group"] for r in rows})
    print(f"{len(rows)} runs, all verified against DuckDB.\n")

    for family, column in (("ship", "l_shipdate"), ("orderkey", "l_orderkey")):
        queries = sorted({(r["selectivity"], r["query"]) for r in rows if r["family"] == family})
        print(f"## Pruning on {column} (plain): bytes read vs no pruning / speedup\n")
        body = []
        for order in orders:
            for group in groups:
                cells = []
                for _, query in queries:
                    pruned, full = (at[(order, "plain", group, query, m)] for m in ("default", "no_prune"))
                    bytes_read = pruned["actual_bytes"] / full["actual_bytes"]
                    cells.append(f"{bytes_read:.0%} / {full['execution_ms'] / pruned['execution_ms']:.1f}x")
                body.append([order, group, *cells])
        print(table(["order", "row group", *(f"{s:.2%}" for s, _ in queries)], body))

    print("## Columnar vs physical organization (l_shipdate ranges below 100%, geometric mean)\n")
    print("Columnar: all 11 columns -> needed 3, no pruning. Organization: pruning on the needed columns.\n")
    queries = {r["query"] for r in rows if r["family"] == "ship" and r["selectivity"] < 0.99}
    body = []
    for order in orders:
        for encoding in ("plain", "dict"):
            for group in groups:
                runs = [{m: at[(order, encoding, group, q, m)] for m in ("all_columns", "no_prune", "default")}
                        for q in queries]
                columnar = geomean([r["all_columns"]["execution_ms"] / r["no_prune"]["execution_ms"] for r in runs])
                organization = geomean([r["no_prune"]["execution_ms"] / r["default"]["execution_ms"] for r in runs])
                saved = geomean([r["all_columns"]["actual_bytes"] / r["default"]["actual_bytes"] for r in runs])
                share = math.log(columnar) / math.log(columnar * organization) if organization > 1.1 else None
                body.append([order, encoding, group, f"{columnar:.1f}x", f"{organization:.1f}x",
                             f"{columnar * organization:.1f}x", f"{saved:.1f}x",
                             "-" if share is None else f"{share:.0%} / {1 - share:.0%}"])
    print(table(["order", "encoding", "row group", "columnar", "organization", "total", "fewer bytes",
                 "share columnar / organization"], body))

    print("## Encoding and decoders (random order)\n")
    body = []
    for group in groups:
        plain, dictionary = (at[("random", e, group, "full_scan", "default")] for e in ("plain", "dict"))
        naive, separate = (at[("random", "dict", group, "full_scan", m)] for m in ("naive_decoder", "separate_ids"))
        body.append([group, f"{plain['file_bytes'] / dictionary['file_bytes']:.2f}x",
                     f"{plain['decode_ms']:.2f}", f"{dictionary['decode_ms']:.2f}",
                     f"{separate['decode_ms'] / dictionary['decode_ms']:.2f}x",
                     f"{naive['decode_ms'] / dictionary['decode_ms']:.2f}x"])
    print(table(["row group", "plain / dict size", "plain decode ms", "dict decode ms", "separate ids", "naive"],
                body))

    predicted = [r for r in rows if r["predicted_ms"] != ""]
    if predicted:
        print("## Cost model: absolute percentage error of predicted execution time\n")
        exact = sum(r["planned_bytes"] == r["actual_bytes"] for r in rows)
        print(f"Planned bytes equal bytes read in {exact}/{len(rows)} runs.\n")
        body = []
        subsets = [("all", predicted)] + [
            (f"{key} = {value}", [r for r in predicted if r[key] == value])
            for key in ("order", "encoding", "row_group", "mode")
            for value in sorted({r[key] for r in predicted})
        ]
        for label, subset in subsets:
            errors = sorted(abs(r["predicted_ms"] - r["execution_ms"]) / r["execution_ms"] for r in subset)
            decode = [abs(r["predicted_decode_ms"] - r["decode_ms"]) / r["decode_ms"] for r in subset]
            body.append([label, len(subset), f"{statistics.median(errors):.1%}",
                         f"{errors[int(0.9 * (len(errors) - 1))]:.1%}", f"{statistics.median(decode):.1%}"])
        print(table(["runs", "n", "median", "p90", "decode median"], body))

        feature_subset = [r for r in predicted if r.get("est_packed") != "" and r.get("act_packed") is not None]
        if feature_subset:
            print("## Feature accuracy: metadata estimates vs decoded values\n")
            print("Errors are a share of all dictionary-encoded values, so they stay meaningful when a count is 0.\n")
            f_body = []
            for order in orders:
                order_runs = [r for r in feature_subset if r["order"] == order and r["encoding"] == "dict"]
                if not order_runs:
                    continue
                totals = [max(1.0, r["act_packed"] + r["act_rle"]) for r in order_runs]
                p_err = [abs(r["est_packed"] - r["act_packed"]) / t for r, t in zip(order_runs, totals, strict=True)]
                r_err = [abs(r["est_rle"] - r["act_rle"]) / t for r, t in zip(order_runs, totals, strict=True)]
                f_body.append([order, len(order_runs), f"{statistics.median(p_err):.1%}",
                               f"{statistics.median(r_err):.1%}"])
            if f_body:
                print(table(["order", "n", "bit-packed error", "rle error"], f_body))


if __name__ == "__main__":
    main()
