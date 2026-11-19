# Experiments

TPC-H `lineitem` written in several physical layouts, scanned by the engine. DuckDB generates the
data and checks matching row counts. See the [project README](../README.md) for setup, CLI options and measurement limits.

```sh
sudo apt-get install libboost-dev   # needed by the Thrift headers
cmake -S .. -B ../build && cmake --build ../build -j
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
PYTHON=.venv/bin/python ./run_all.sh  # SF 0.1 and 1; SCALES, DATA, OUT, REPEATS override
```

Keep `DATA` on a local Linux filesystem; a Windows mount dominates the timings.

| Variant | Values |
|---|---|
| Order | `random`, `orderkey`, `shipdate`, `partial30`, `partial365` (shipdate ± N days) |
| Encoding | `dict`, `plain` |
| Row group | 4K, 16K, 128K, 1M rows |

Queries sweep selectivity on `l_shipdate` and `l_orderkey`. Each runs with pruning, without it, and
over all columns without it (an all-column Parquet baseline). The cost model is calibrated on the random and
shipdate-sorted files of the first scale factor and predicts every run. Times are single-threaded, warm-cache medians.
