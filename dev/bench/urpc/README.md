# URPC Micro Bench

This benchmark keeps the client path unchanged and only switches server mode:

- `epoll urpc` (default)
- `io-uring urpc` (`io_uring_enable=true`)

## Build

```bash
cargo build --release -p riffle-server --bin urpc_bench_client
```

## Run on single host

1. Start server in epoll mode.
2. Run:

```bash
bash dev/bench/urpc/run_single_host.sh
```

3. When prompted, stop epoll server and start io-uring server.
4. Press Enter to continue uring benchmark.

## Manual run

```bash
./target/release/urpc_bench_client \
  --urpc-host 127.0.0.1 \
  --urpc-port 19999 \
  --grpc-host 127.0.0.1 \
  --grpc-port 19997 \
  --payload-bytes 16384 \
  --concurrency 32 \
  --warmup-secs 30 \
  --measure-secs 120 \
  --repeats 5 \
  --timeout-ms 3000 \
  --mode-tag epoll \
  --output dev/bench/urpc/results/epoll_p16384_c32.json
```

## Output schema

Each output file contains:

- case metadata (`mode_tag`, payload, concurrency, timeout, etc.)
- per-repeat metrics:
  - `qps`
  - `error_rate`
  - `p50_us`, `p95_us`, `p99_us`, `max_us`
  - `success`, `errors`
