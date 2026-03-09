#!/usr/bin/env bash
# Benchmark sqlite3 vs sqlite-rust: run benchmark_queries.sql and report timings.
# Builds the Rust binary in release mode, then runs each query with both engines.
# Usage: ./bench/run_benchmarks.sh [path_to_repo_root]

set -e
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT="${1:-$(cd "${SCRIPT_DIR}/.." && pwd)}"
SQL_FILE="${SCRIPT_DIR}/benchmark_queries.sql"
RUST_BIN="${ROOT}/target/release/sqlite-rust"
cd "$ROOT"

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

# Extract "db|SELECT ..." lines (db = sample | companies | superheroes)
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
  # sqlite3
  sqlite3_time=$(echo "$query" | sqlite3 -cmd ".timer on" "$db_path" 2>&1 | tail -1)
  echo "  sqlite3:     $sqlite3_time"
  # sqlite-rust
  rust_time=$("$RUST_BIN" -cmd ".timer on" "$db_path" "$query" 2>&1 | tail -1)
  echo "  sqlite-rust: $rust_time"
done < <(run_awk)

echo ""
echo "Done."
