#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUILD_DIR="${ROOT_DIR}/build/cpp"
PYTHON="${PYTHON:-${ROOT_DIR}/.venv/bin/python}"

if [[ ! -x "${PYTHON}" ]]; then
  PYTHON="$(command -v python3 || true)"
fi

if [[ -z "${PYTHON}" || ! -x "${PYTHON}" ]]; then
  echo "Python executable not found. Set PYTHON or create .venv." >&2
  exit 1
fi

cmake -S "${ROOT_DIR}/cpp" -B "${BUILD_DIR}" -DPYTHON_EXECUTABLE="${PYTHON}"
cmake --build "${BUILD_DIR}" --config Release

SITE_PACKAGES="$("${PYTHON}" -c 'import site; print(site.getsitepackages()[0])')"
cp "${BUILD_DIR}/finsignal_engine"*.so "${SITE_PACKAGES}/" 2>/dev/null || \
cp "${BUILD_DIR}/Release/finsignal_engine"*.pyd "${SITE_PACKAGES}/" 2>/dev/null || \
cp "${BUILD_DIR}/finsignal_engine"*.dylib "${SITE_PACKAGES}/" 2>/dev/null || true

echo "Built finsignal_engine extension into ${SITE_PACKAGES}"
