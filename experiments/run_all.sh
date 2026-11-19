#!/usr/bin/env bash
# Generate data, calibrate the cost model on the first scale factor, benchmark every scale factor.
set -euo pipefail

cd "$(dirname "$0")"
BIN=${BIN:-../build/parquet_metadata}
DATA=${DATA:-data}
OUT=${OUT:-results}
SCALES=(${SCALES:-0.1 1})
REPEATS=${REPEATS:-5}
PYTHON=${PYTHON:-python3}

mkdir -p "$OUT"
for sf in "${SCALES[@]}"; do
    [[ -d "$DATA/sf$sf" ]] || "$PYTHON" generate.py --sf "$sf" --out "$DATA"
done
calibration="$DATA/sf${SCALES[0]}"
"$BIN" calibrate "$calibration"/lineitem_{random,shipdate}_*.parquet > "$OUT/model.json"
for sf in "${SCALES[@]}"; do
    "$PYTHON" benchmark.py --bin "$BIN" --data "$DATA/sf$sf" --model "$OUT/model.json" \
        --out "$OUT/sf$sf.csv" --repeats "$REPEATS"
    "$PYTHON" report.py "$OUT/sf$sf.csv" > "$OUT/sf$sf.md"
done
