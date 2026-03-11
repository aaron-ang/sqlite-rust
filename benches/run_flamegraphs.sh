#!/usr/bin/env bash
# Generate aggregate flamegraphs for sqlite-rust and sqlite3 on Linux.
# Usage: ./benches/run_flamegraphs.sh [root]

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT="${1:-$(cd "${SCRIPT_DIR}/.." && pwd)}"
ROOT="$(cd "$ROOT" && pwd)"
SQL_FILE="${ROOT}/benches/benchmark_queries.sql"
RUST_BIN="${ROOT}/target/release/sqlite-rust"
SQLITE3_BIN="$(command -v sqlite3)"
OUT_DIR="${BENCH_FLAMEGRAPH_OUT_DIR:-${ROOT}/benches/artifacts/flamegraphs}"
NUM_SAMPLES="${BENCH_NUM_SAMPLES:-10}"
PROFILE_ROOT=""
RUN_ENGINE_SCRIPT=""

cleanup() {
  local status=$?
  trap - EXIT INT TERM

  if [[ -n "$PROFILE_ROOT" && -d "$PROFILE_ROOT" ]]; then
    rm -rf "$PROFILE_ROOT"
  fi

  exit "$status"
}

trap cleanup EXIT INT TERM

build_group_sql_files() {
  rm -f \
    "$PROFILE_ROOT/sample.sql" \
    "$PROFILE_ROOT/companies.sql" \
    "$PROFILE_ROOT/superheroes.sql"

  awk -v outdir="$PROFILE_ROOT" '
    function select_group(name) {
      current = name
      file = outdir "/" name ".sql"
    }

    /-- sample\.db \(/      { select_group("sample"); next }
    /-- companies\.db \(/   { select_group("companies"); next }
    /-- superheroes\.db \(/ { select_group("superheroes"); next }

    current && /^SELECT/ {
      print $0 >> file
      counts[current] += 1
    }

    END {
      required[1] = "sample"
      required[2] = "companies"
      required[3] = "superheroes"

      for (i = 1; i <= 3; i++) {
        name = required[i]
        if (counts[name] == 0) {
          printf "Error: %s.db section did not contain any benchmark queries\n", name > "/dev/stderr"
          exit 1
        }
      }
    }
  ' "$SQL_FILE"
}

build_aggregate_workload() {
  local workload="${PROFILE_ROOT}/aggregate.workload.sql"
  local i
  local db

  : > "$workload"

  for ((i = 0; i < NUM_SAMPLES; i++)); do
    for db in sample companies superheroes; do
      printf '.open %s/%s.db\n' "$PROFILE_ROOT" "$db" >> "$workload"
      cat "${PROFILE_ROOT}/${db}.sql" >> "$workload"
      printf '\n' >> "$workload"
    done
  done
}

copy_databases() {
  local db

  for db in sample companies superheroes; do
    cp "${ROOT}/${db}.db" "${PROFILE_ROOT}/${db}.db"
  done
}

prepare_runner_script() {
  RUN_ENGINE_SCRIPT="${PROFILE_ROOT}/run-engine.sh"

  cat > "$RUN_ENGINE_SCRIPT" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
exec "$1" "$2" < "$3" >/dev/null 2>/dev/null
EOF
  chmod +x "$RUN_ENGINE_SCRIPT"
}

run_engine_flamegraph() {
  local engine="$1"
  local executable="$2"
  local title="$3"
  local output="${OUT_DIR}/${engine}.svg"

  echo "Generating ${engine}.svg..."
  (
    cd "$PROFILE_ROOT"
    rm -rf "${PROFILE_ROOT}/cargo-flamegraph.trace"
    flamegraph \
      --deterministic \
      --title "$title" \
      -o "$output" \
      -- \
      "$RUN_ENGINE_SCRIPT" \
      "$executable" \
      "${PROFILE_ROOT}/sample.db" \
      "${PROFILE_ROOT}/aggregate.workload.sql"
  )
}

if [[ ! -f "$SQL_FILE" ]]; then
  echo "Error: benchmark_queries.sql not found at $SQL_FILE" >&2
  exit 1
fi

for db in sample companies superheroes; do
  if [[ ! -f "${ROOT}/${db}.db" ]]; then
    echo "Error: benchmark database not found at ${ROOT}/${db}.db" >&2
    exit 1
  fi
done

if ! command -v cargo >/dev/null 2>&1; then
  echo "Error: cargo is required" >&2
  exit 1
fi

if ! command -v sqlite3 >/dev/null 2>&1; then
  echo "Error: sqlite3 is required" >&2
  exit 1
fi

if ! command -v flamegraph >/dev/null 2>&1; then
  echo "Error: flamegraph is required. Install it with: cargo install flamegraph" >&2
  exit 1
fi

rm -rf "$OUT_DIR"
mkdir -p "$OUT_DIR"

PROFILE_ROOT="$(mktemp -d /tmp/sqlite-rust-flamegraphs.XXXXXX)"

build_group_sql_files
copy_databases
build_aggregate_workload

echo "Building sqlite-rust (release)..."
cargo build --release --quiet
if [[ ! -x "$RUST_BIN" ]]; then
  echo "Error: binary not found at $RUST_BIN" >&2
  exit 1
fi

prepare_runner_script

echo ""
echo "Profiling engines..."
run_engine_flamegraph "sqlite-rust" "$RUST_BIN" "sqlite-rust aggregate benchmark queries"
run_engine_flamegraph "sqlite3" "$SQLITE3_BIN" "sqlite3 aggregate benchmark queries"

echo ""
echo "Artifacts written to ${OUT_DIR}"
echo "  ${OUT_DIR}/sqlite-rust.svg"
echo "  ${OUT_DIR}/sqlite3.svg"
