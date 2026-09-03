#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"
export PYTHONPATH="${ROOT_DIR}${PYTHONPATH:+:${PYTHONPATH}}"

SEED="${SEED:-42}"
PYTHON="${PYTHON:-${ROOT_DIR}/.venv/bin/python}"
if [[ ! -x "${PYTHON}" ]]; then
  PYTHON="$(command -v python3)"
fi

if [[ -z "${JAVA_HOME:-}" ]]; then
  for candidate in \
    "/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home" \
    "/usr/lib/jvm/java-17-openjdk-amd64" \
    "/usr/lib/jvm/java-17-amazon-corretto"; do
    if [[ -d "${candidate}" ]]; then
      export JAVA_HOME="${candidate}"
      export PATH="${JAVA_HOME}/bin:${PATH}"
      break
    fi
  done
fi

if [[ -z "${JAVA_HOME:-}" ]]; then
  echo "JAVA_HOME is not set and no JDK 17 candidate was found." >&2
  exit 1
fi

echo "FinSignal Lakehouse — Full Pipeline Run"
echo "seed: ${SEED}"
echo "storage format: ${FINSIGNAL_STORAGE_FORMAT:-delta}"
echo "java home: ${JAVA_HOME}"
echo "python: ${PYTHON}"
echo "------------------------------------------------------------------------"

run_step() {
  echo
  echo "==> $1"
  shift
  "$@"
}

run_step "Generate synthetic ledger" "${PYTHON}" -m src.data_generation.generate_ledger --seed "${SEED}"
run_step "Bronze ingestion" "${PYTHON}" -m src.pipelines.bronze_ingestion --load-id "test_load_001"
run_step "Silver cleaning" "${PYTHON}" -m src.pipelines.silver_cleaning --run-id "silver_test_001"
run_step "Gold reconciliation" "${PYTHON}" -m src.pipelines.gold_reconciliation --run-id "gold_recon_001"
run_step "Gold event window" "${PYTHON}" -m src.pipelines.gold_event_window --run-id "gold_event_001"
run_step "Manifest validation" "${PYTHON}" -m src.validation.manifest_validation
run_step "Investigation corpus" "${PYTHON}" -m src.retrieval.build_investigation_corpus --run-id "corpus_001"
run_step "Vector index" "${PYTHON}" -m src.retrieval.build_vector_index --run-id "vector_001"

if command -v cmake >/dev/null 2>&1; then
  run_step "Build C++ reference engine" env PYTHON="${PYTHON}" bash scripts/build_cpp_engine.sh
else
  echo
  echo "==> Skipping C++ build (cmake not found)"
fi

run_step "Integration tests" "${PYTHON}" -m pytest tests/ -q

echo
echo "FinSignal Lakehouse — Full Pipeline Complete"
