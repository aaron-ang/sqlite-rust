#!/usr/bin/env bash
# Benchmark sqlite3 vs sqlite-rust (50 samples/query, mean). Usage: ./bench/run_benchmarks.sh [root]

set -e
NUM_SAMPLES=50
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT="${1:-$(cd "${SCRIPT_DIR}/.." && pwd)}"
SQL_FILE="${SCRIPT_DIR}/benchmark_queries.sql"
RUST_BIN="${ROOT}/target/release/sqlite-rust"
cd "$ROOT"

parse_real_time() {
  awk '/Run Time:/ { for(i=1;i<=NF;i++) if($i=="real") { print $(i+1); exit } }'
}

run_samples_and_mean() {
  local engine="$1"
  local db_path="$2"
  local query="$3"
  local n="${4:-$NUM_SAMPLES}"
  local times=""
  local i line t
  for ((i=0; i<n; i++)); do
    if [[ "$engine" == "sqlite3" ]]; then
      line=$(echo "$query" | sqlite3 -cmd ".timer on" "$db_path" 2>&1 | tail -1)
    else
      line=$("$engine" -cmd ".timer on" "$db_path" "$query" 2>&1 | tail -1)
    fi
    t=$(echo "$line" | parse_real_time)
    [[ -n "$t" ]] && times="$times $t"
  done
  if [[ -z "$times" ]]; then
    echo "N/A"
  else
    echo "$times" | awk '{ sum=0; n=0; for(i=1;i<=NF;i++) { sum+=$i; n++ } if(n>0) printf "%.6f", sum/n; else print "N/A" }'
  fi
}

if [[ ! -f "$SQL_FILE" ]]; then
  echo "Error: benchmark_queries.sql not found at $SQL_FILE" >&2
  exit 1
fi

echo "Building sqlite-rust (release)..."
cargo build --release --quiet
if [[ ! -x "$RUST_BIN" ]]; then
  echo "Error: binary not found at $RUST_BIN" >&2
  exit 1
fi

run_awk() {
  awk '
    /-- sample\.db \(/     { db="sample"; next }
    /-- companies\.db \(/  { db="companies"; next }
    /-- superheroes\.db \(/{ db="superheroes"; next }
    db && /^SELECT/        { print db "|" $0 }
  ' "$SQL_FILE"
}

echo ""
echo "Benchmark: sqlite3 vs sqlite-rust (release)"
echo "==========================================="

while IFS='|' read -r db query; do
  db_path="${ROOT}/${db}.db"
  if [[ ! -f "$db_path" ]]; then
    echo "[SKIP] ${db}.db not found" >&2
    continue
  fi
  echo ""
  echo "--- ${db}.db ---"
  echo "  $query"
  printf "  sqlite3:     "
  sqlite3_mean=$(run_samples_and_mean "sqlite3" "$db_path" "$query")
  echo "mean ${sqlite3_mean}s (${NUM_SAMPLES} runs)"
  printf "  sqlite-rust: "
  rust_mean=$(run_samples_and_mean "$RUST_BIN" "$db_path" "$query")
  echo "mean ${rust_mean}s (${NUM_SAMPLES} runs)"
done < <(run_awk)

echo ""
echo "Done."
