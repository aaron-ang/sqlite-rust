#!/usr/bin/env bash
# Benchmark sqlite3 vs sqlite-rust (50 samples/query, mean). Usage: ./bench/run_benchmarks.sh [root]

set -euo pipefail
NUM_SAMPLES=50
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT="${1:-$(cd "${SCRIPT_DIR}/.." && pwd)}"
SQL_FILE="${SCRIPT_DIR}/benchmark_queries.sql"
RUST_BIN="${ROOT}/target/release/sqlite-rust"
cd "$ROOT"

parse_real_times() {
  awk '/Run Time:/ { for (i = 1; i <= NF; i++) if ($i == "real") { print $(i + 1); break } }'
}

run_awk() {
  awk '
    /-- sample\.db \(/      { db="sample"; next }
    /-- companies\.db \(/   { db="companies"; next }
    /-- superheroes\.db \(/ { db="superheroes"; next }
    db && /^SELECT/         { print db "|" $0 }
  ' "$SQL_FILE"
}

repeat_query_batch() {
  local query="$1"
  local count="${2:-$NUM_SAMPLES}"
  local i

  for ((i = 0; i < count; i++)); do
    printf '%s\n' "$query"
  done
}

run_query_mean() {
  local engine="$1"
  local db_path="$2"
  local query="$3"
  local n="${4:-$NUM_SAMPLES}"
  local parsed_times
  local time_count

  if [[ "$engine" == "sqlite3" ]]; then
    parsed_times=$(repeat_query_batch "$query" "$n" | sqlite3 -cmd ".timer on" "$db_path" 2>&1 | parse_real_times)
  else
    parsed_times=$(repeat_query_batch "$query" "$n" | "$engine" -cmd ".timer on" "$db_path" 2>&1 | parse_real_times)
  fi

  time_count=$(printf '%s\n' "$parsed_times" | awk 'NF { count += 1 } END { print count + 0 }')
  if [[ "$time_count" -ne "$n" ]]; then
    echo "Error: expected ${n} timer lines for ${db_path}, got ${time_count}" >&2
    exit 1
  fi

  printf '%s\n' "$parsed_times" | awk 'NF { sum += $1; count += 1 } END { if (count > 0) printf "%.6f", sum / count; else print "N/A" }'
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

echo ""
echo "Benchmark: sqlite3 vs sqlite-rust (release)"
echo "==========================================="

current_db=""
while IFS='|' read -r db query; do
  db_path="${ROOT}/${db}.db"
  if [[ ! -f "$db_path" ]]; then
    echo "[SKIP] ${db}.db not found" >&2
    continue
  fi

  if [[ "$db" != "$current_db" ]]; then
    current_db="$db"
    echo ""
    echo "--- ${db}.db ---"
  fi

  sqlite3_mean=$(run_query_mean "sqlite3" "$db_path" "$query")
  rust_mean=$(run_query_mean "$RUST_BIN" "$db_path" "$query")

  echo "  $query"
  echo "    sqlite3:     mean ${sqlite3_mean}s (${NUM_SAMPLES} runs)"
  echo "    sqlite-rust: mean ${rust_mean}s (${NUM_SAMPLES} runs)"
done < <(run_awk)

echo ""
echo "Done."
