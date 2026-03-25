#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
RESULT_DIR="${ROOT_DIR}/dev/bench/urpc/results"
mkdir -p "${RESULT_DIR}"

URPC_HOST="${URPC_HOST:-127.0.0.1}"
URPC_PORT="${URPC_PORT:-19999}"
GRPC_HOST="${GRPC_HOST:-127.0.0.1}"
GRPC_PORT="${GRPC_PORT:-19997}"
WARMUP_SECS="${WARMUP_SECS:-30}"
MEASURE_SECS="${MEASURE_SECS:-120}"
REPEATS="${REPEATS:-5}"
TIMEOUT_MS="${TIMEOUT_MS:-3000}"

run_case() {
  local mode="$1"
  local payload="$2"
  local conc="$3"
  local output="${RESULT_DIR}/${mode}_p${payload}_c${conc}.json"

  "${ROOT_DIR}/target/release/urpc_bench_client" \
    --urpc-host "${URPC_HOST}" \
    --urpc-port "${URPC_PORT}" \
    --grpc-host "${GRPC_HOST}" \
    --grpc-port "${GRPC_PORT}" \
    --payload-bytes "${payload}" \
    --concurrency "${conc}" \
    --warmup-secs "${WARMUP_SECS}" \
    --measure-secs "${MEASURE_SECS}" \
    --repeats "${REPEATS}" \
    --timeout-ms "${TIMEOUT_MS}" \
    --mode-tag "${mode}" \
    --output "${output}"
}

echo "[INFO] build urpc_bench_client ..."
cd "${ROOT_DIR}"
cargo build --release -p riffle-server --bin urpc_bench_client

echo "[INFO] running epoll benchmark cases ..."
run_case epoll 1024 1
run_case epoll 1024 32
run_case epoll 16384 32
run_case epoll 262144 128

echo "[INFO] switch server to io-uring mode before continuing."
echo "[INFO] press Enter when uring server is ready."
read -r

echo "[INFO] running uring benchmark cases ..."
run_case uring 1024 1
run_case uring 1024 32
run_case uring 16384 32
run_case uring 262144 128

echo "[INFO] benchmark finished. results are in ${RESULT_DIR}"
