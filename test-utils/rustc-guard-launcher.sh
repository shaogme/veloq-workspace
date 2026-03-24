#!/usr/bin/env sh
set -eu
SCRIPT_DIR="$(CDPATH= cd -- "$(dirname "$0")" && pwd)"
WORKSPACE_ROOT="$(CDPATH= cd -- "${SCRIPT_DIR}/.." && pwd)"
export CARGO_TARGET_DIR="${WORKSPACE_ROOT}/target/guard-host"
GUARD_EXE="${CARGO_TARGET_DIR}/debug/rustc-guard"
if [ ! -x "${GUARD_EXE}" ]; then
  export CARGO_BUILD_RUSTC_WORKSPACE_WRAPPER=""
  (
    cd "${WORKSPACE_ROOT}"
    cargo --config "build.rustc-workspace-wrapper=''" run -q -p rustc-guard -- --warmup
  )
fi
export VELOQ_GUARD_FROM_LAUNCHER=1
export CARGO_BUILD_RUSTC_WORKSPACE_WRAPPER=""
exec "${GUARD_EXE}" "$@"
