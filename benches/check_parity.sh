#!/usr/bin/env bash
# Compare sqlite3 vs sqlite-rust output for benchmark queries. Usage: ./benches/check_parity.sh [root]

set -euo pipefail

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

run_awk() {
  awk '
    /-- sample\.db \(/     { db="sample"; next }
    /-- companies\.db \(/  { db="companies"; next }
    /-- superheroes\.db \(/{ db="superheroes"; next }
    db && /^SELECT/        { print db "|" $0 }
  ' "$SQL_FILE"
}

failed=0
while IFS='|' read -r db query; do
  db_path="${ROOT}/${db}.db"
  if [[ ! -f "$db_path" ]]; then
    echo "[SKIP] ${db}.db not found"
    continue
  fi
  sqlite3_out=$(echo "$query" | sqlite3 "$db_path" 2>/dev/null | sort)
  rust_out=$("$RUST_BIN" "$db_path" "$query" 2>/dev/null | sort)
  if [[ "$sqlite3_out" != "$rust_out" ]]; then
    echo "FAIL: ${db}.db"
    echo "  Query: $query"
    echo "  sqlite3 lines: $(echo -n "$sqlite3_out" | wc -l | tr -d ' '), sqlite-rust lines: $(echo -n "$rust_out" | wc -l | tr -d ' ')"
    failed=1
  else
    echo "OK: ${db}.db — ${query:0:60}..."
  fi
done < <(run_awk)

if [[ $failed -eq 1 ]]; then
  echo ""
  echo "Result parity check failed."
  exit 1
fi
echo ""
echo "Result parity check passed: sqlite-rust matches sqlite3 on all queries."
