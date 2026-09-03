#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUILD_DIR="${ROOT_DIR}/build/cpp"
VENV_PYTHON="${ROOT_DIR}/.venv/bin/python"

if [[ ! -x "${VENV_PYTHON}" ]]; then
  echo "Missing virtualenv python at ${VENV_PYTHON}" >&2
  exit 1
fi

cmake -S "${ROOT_DIR}/cpp" -B "${BUILD_DIR}" -DPYTHON_EXECUTABLE="${VENV_PYTHON}"
cmake --build "${BUILD_DIR}" --config Release

SITE_PACKAGES="$("${VENV_PYTHON}" -c 'import site; print(site.getsitepackages()[0])')"
cp "${BUILD_DIR}/finsignal_engine"*.so "${SITE_PACKAGES}/" 2>/dev/null || \
cp "${BUILD_DIR}/Release/finsignal_engine"*.pyd "${SITE_PACKAGES}/" 2>/dev/null || \
cp "${BUILD_DIR}/finsignal_engine"*.dylib "${SITE_PACKAGES}/" 2>/dev/null || true

echo "Built finsignal_engine extension into ${SITE_PACKAGES}"
