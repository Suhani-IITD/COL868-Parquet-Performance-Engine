"""Write physical variants of TPC-H lineitem that the engine can scan.

All variants hold the same rows. The engine reads required INT32/INT64/DATE columns from
uncompressed V1 pages, so decimals are stored as scaled integers and every column is non-nullable.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

ROW_GROUP_SIZES = (4_096, 16_384, 131_072, 1_048_576)
ENCODINGS = {"dict": True, "plain": False}
_HASH = "hash(l_orderkey, l_linenumber)"
ORDERS = {
    "random": _HASH,
    "orderkey": "l_orderkey, l_linenumber",
    "shipdate": f"l_shipdate, {_HASH}",
    # Partially clustered: sorted on shipdate shifted by a deterministic offset in [-N, N] days.
    "partial30": f"l_shipdate + CAST({_HASH} % 61 AS INTEGER) - 30, {_HASH}",
    "partial365": f"l_shipdate + CAST({_HASH} % 731 AS INTEGER) - 365, {_HASH}",
}
COLUMNS = """
    l_orderkey::BIGINT AS l_orderkey,
    l_partkey::BIGINT AS l_partkey,
    l_suppkey::BIGINT AS l_suppkey,
    l_linenumber::INTEGER AS l_linenumber,
    l_quantity::INTEGER AS l_quantity,
    (l_extendedprice * 100)::BIGINT AS l_extendedprice_cents,
    (l_discount * 100)::INTEGER AS l_discount_pct,
    (l_tax * 100)::INTEGER AS l_tax_pct,
    l_shipdate, l_commitdate, l_receiptdate
"""


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sf", type=float, default=1.0, help="TPC-H scale factor")
    parser.add_argument("--out", type=Path, default=Path("data"))
    args = parser.parse_args()
    out = args.out / f"sf{args.sf:g}"
    out.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect()
    con.execute("INSTALL tpch")
    con.execute("LOAD tpch")
    con.execute(f"CALL dbgen(sf = {args.sf})")
    for order, key in ORDERS.items():
        table = con.sql(f"SELECT {COLUMNS} FROM lineitem ORDER BY {key}").to_arrow_table()
        table = table.cast(pa.schema([field.with_nullable(False) for field in table.schema]))
        for encoding, use_dictionary in ENCODINGS.items():
            for size in ROW_GROUP_SIZES:
                path = out / f"lineitem_{order}_{encoding}_rg{size}.parquet"
                pq.write_table(
                    table,
                    path,
                    row_group_size=size,
                    compression="none",
                    use_dictionary=use_dictionary,
                    data_page_version="1.0",
                    write_statistics=True,
                    store_schema=False,
                )
                print(f"{path.name}: {path.stat().st_size / 1e6:.1f} MB")


if __name__ == "__main__":
    main()
