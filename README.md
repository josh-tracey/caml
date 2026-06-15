# caml

[![CI](https://github.com/josh-tracey/caml/actions/workflows/ci.yml/badge.svg)](https://github.com/josh-tracey/caml/actions)

`caml` is a Rust workspace for compiling declarative camera/media manifests into supervised runtime graphs. The repository implements a pure-native, zero-subprocess, hardware-aware architecture with specific optimizations for embedded media paths like Raspberry Pi 4 and 5.

## Current Status

We have established the core architecture, featuring full native user-space FFI loops, strict compile-time capability guardrails, and verified memory-reuse and execution runtimes.

- **Strict YAML Manifest Parsing**: Typed enums, unit parsing, and structural validation.
- **Hardware Capability Matrix**: Compile-time host capability probing that detects Pi model, V4L2/libcamera availability, Pi 4 encode nodes, and Pi 5 stateless decode topology. This dynamically configures pipelines and strictly rejects unsupported configurations before execution.
- **Pure-Native Execution**: Complete elimination of `std::process::Command`. All backends (FFmpeg, WebRTC, Libcamera) run inside the user-space process using FFI bindings.
- **Native FFmpeg-Next Ingestion**: Fully native ingest loops parsing RTSP streams and dispatching to hardware/software transcoders.
- **Native Libcamera Provider**: Production-ready native libcamera FFI session implementation using a background worker thread and configuration profiles in `caml-linux-media`.
- **Staged Async Runtime**: A `source -> transform(s) -> sink` architecture with bounded reusable buffer pooling and execution observability.
- **Resilient Recovery Policy**: Adapter-oriented recovery classification (`network`, `device`, `hardware`). The `RuntimeEngine` uses async cooperative watchdogs to detect hardware/network stalls.


## Workspace Layout

The repo is split into these crates:

- `caml`: public facade, re-exports, and `RuntimeBuilder`
- `caml-core`: manifest parsing, compilation, capability modeling, metrics, and staged runtime supervision
- `caml-ffmpeg`: feature-gated native FFmpeg ingest and transcode backend
- `caml-webrtc`: feature-gated WebRTC RTP sink backend
- `caml-linux-media`: Linux and Raspberry Pi libcamera source-factory and hardware execution testing

## Supported Build Tiers

### Default build

The default build is self-contained and compiles without FFmpeg or Linux-specific media dependencies:

```bash
cargo test
```

This tier is intended for parser/compiler/runtime development and for applications that want to plug in custom source/sink backends.

### Feature-gated media backends

The backend crates can be enabled together:

```bash
cargo check --features ffmpeg,webrtc,pi
```

Today, these crates cover concrete Linux media slices:

- H.264 passthrough through FFmpeg and RTP packetization into a WebRTC track sink.
- Software H.264 transcode for RTSP and V4L2 device video with optional 90/180/270-degree rotation.
- Pi 4 hardware-backed H.264 encode selection through FFmpeg's `h264_v4l2m2m` path.
- Pi 5 stateless hardware decode selection through FFmpeg's `*_v4l2request` decoders, paired with software H.264 encode.
- Physical device execution using native `libcamera`.

## Manifest Example

```yaml
system:
  hardware_target: "RASPBERRY_PI_5"
  cma_allocation_limit: "512MB"

pipelines:
  - id: "forward_primary"
    input: "rtsp://192.168.1.50:554/stream1"
    type: "rtsp"
    strategy: "passthrough"
    network:
      transport: "tcp"
      packet_size_limit: 1200
      stall_timeout: 10s
    outputs:
      - type: "webrtc_rtp"
        codec: "h264"
        payload_type: 102
        mtu: 1200
        ssrc: "stable"
        clock_rate: 90000

  - id: "belly_optical"
    input: "/dev/video0"
    type: "device"
    strategy: "transcode"
    backend: "v4l2"
    processing:
      codec: "h264"
      encoder: "software"
      preset: "ultrafast"
      tune: "zerolatency"
      frame_rate: 30
      bitrate: "512k"
      rotation: 90
```

## Claim Checklist

As part of our commitment to hardware honesty, this checklist maps `caml`'s bold architectural claims to the tests or benchmarks that prove them. **No claim is considered implemented unless linked here.**

- [x] [No Subprocess Orchestration](docs/claims.md#no-subprocess-orchestration): Enforced architecture. Backends exclusively use native bindings.
- [x] [Buffer Reuse](docs/claims.md#buffer-reuse): Pooled buffers are preallocated and reused in the hot loop.
- [x] [Bounded Hot-Path Allocation](docs/claims.md#bounded-hot-path-allocation): Verified via custom counting allocator test.
- [ ] [Zero-Copy Media Path](docs/claims.md#zero-copy-media-path) (Partial): Pipeline uses pooled or borrowed storage slices to bypass cloning, but WebRTC packetizer has slice copy constraints.
- [x] [RTSP to WebRTC RTP](docs/claims.md#rtsp-to-webrtc-rtp): Dynamic configuration is fully propagated to WebRTC output RTP packets.
- [ ] [Native Libcamera Provider](docs/claims.md#native-libcamera-provider) (Experimental): Native FFI session via safe background worker thread and channels.
- [x] [Pi Hardware Guardrails](docs/claims.md#pi-hardware-guardrails): Probe-backed compile-time checks validate target and capabilities.
- [x] [Pi 4 Hardware Encode Execution](docs/claims.md#pi-4-hardware-encode-execution): Verified execution with target-gated tests.
- [x] [Pi 5 Stateless Decode Execution](docs/claims.md#pi-5-stateless-decode-execution): Verified execution with target-gated tests.
- [x] [Recovery Classes and Metrics Hooks](docs/claims.md#recovery-classes-and-metrics-hooks): Error categorization and metrics telemetry.
- [x] [Class-Specific Recovery Behavior](docs/claims.md#class-specific-recovery-behavior): Differentiated backoff policies for network, device, and hardware classes.

*(Performance claims and zero-copy semantics link to benchmark artifacts as they are finalized. Benchmarks can be run using `cargo bench`.)*


