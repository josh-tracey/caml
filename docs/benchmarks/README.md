# caml Benchmarks

This directory contains the benchmark results for `caml`.

## Running the benchmarks

To run the benchmark suite, execute:

```bash
cargo bench
```

## Available Benchmarks

- **Buffer Pool (`benches/buffer_pool.rs`)**: Measures the throughput of acquiring and releasing memory buffers from the `BufferPool`.
- **Mock Passthrough (`benches/mock_passthrough.rs`)**: Measures throughput of media packets (packets/second) through a mock passthrough pipeline.
