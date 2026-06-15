# caml

`caml` is a Rust workspace for compiling declarative camera/media manifests into supervised runtime graphs. The repository now has a real split between a stable core crate and feature-gated media backend crates, so the public `caml` facade can grow toward native FFmpeg, WebRTC, and Raspberry Pi media paths without burying those concerns inside one monolithic crate.

## Current Status

What is implemented today:

- Strict YAML manifest parsing with typed enums, unit parsing, and structural validation
- Graph compilation into a runtime-ready topology with backend resolution, execution mode selection, recovery policy, and capability requirements
- A staged async runtime that supports `source -> transform(s) -> sink`, bounded reusable buffer pooling, observable task status, and supervised stall recovery
- A public `RuntimeBuilder` facade in the top-level `caml` crate
- Feature-gated backend crates for FFmpeg, WebRTC, and Linux/Pi capability probing
- Concrete FFmpeg media paths: RTSP H.264 passthrough, RTSP/V4L2 software H.264 transcode, and Pi-aware codec selection for Pi 4 hardware encode plus Pi 5 stateless hardware decode
- Linux/Pi capability probing that detects Pi model, V4L2/libcamera availability, Pi 4 encode nodes, and Pi 5 stateless decode topology for probe-backed compile-time guardrails
- A libcamera source-factory foundation in `caml-linux-media` that accepts native frame providers for `backend: "libcamera"` pipelines without shelling out to libcamera command-line tools
- Adapter-oriented recovery classification (`network`, `device`, `hardware`) on compiled pipelines, ready for backend-specific telemetry and tuning

What is not complete yet:

- Production libcamera FFI/device-session implementation on top of the provider-backed source-factory foundation
- adapter-specific recovery tuning beyond the current compiled recovery classes
- host-backed media-flow execution coverage for Pi hardware paths

The repository is no longer pretending those pieces already exist in production form. The workspace and APIs are now shaped to support them cleanly.

## Workspace Layout

The repo is split into these crates:

- `caml`: public facade, re-exports, and `RuntimeBuilder`
- `caml-core`: manifest parsing, compilation, capability modeling, and staged runtime supervision
- `caml-ffmpeg`: feature-gated FFmpeg ingest backend
- `caml-webrtc`: feature-gated WebRTC RTP sink backend
- `caml-linux-media`: Linux and Raspberry Pi capability probing plus libcamera source-factory foundation

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

Today, these crates can cover concrete Linux media slices:

- H.264 passthrough through FFmpeg and RTP packetization into a WebRTC track sink
- software H.264 transcode for RTSP and V4L2 device video with optional 90/180/270-degree rotation
- Pi 4 hardware-backed H.264 encode selection through FFmpeg's `h264_v4l2m2m` path
- Pi 5 stateless hardware decode selection through FFmpeg's `*_v4l2request` decoders, paired with software H.264 encode

They also provide Linux/Pi host capability probing that can be merged with FFmpeg/WebRTC probes through `RuntimeBuilder::with_feature_capability_probe()`. Host capability guardrails are enforced only when compiling with an explicit probe path, such as `CamlCompiler::compile_with_probe()` or a `RuntimeBuilder` configured with a capability probe; plain `CamlCompiler::compile()` performs schema and hardware-target validation without inspecting the host.

Production libcamera FFI capture, broader adapter recovery tuning, and host-backed Pi media-flow validation are still follow-up work. See `docs/roadmap.md` and `docs/pi-testing.md` for acceptance criteria and host test prerequisites.

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
      bitrate: 512k
      rotation: 90
```

## Quick Start

### Parse and compile a graph

```rust
use caml::{CamlCompiler, CamlManifest};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let raw_manifest = std::fs::read_to_string("config/pipelines.caml")?;
    let manifest = CamlManifest::from_yaml_str(&raw_manifest)?;
    let compiled = CamlCompiler::validate_and_compile(&manifest)?;

    println!("compiled {} pipeline(s)", compiled.pipelines.len());
    Ok(())
}
```

### Start the runtime through the facade

```rust
use std::sync::Arc;

use caml::{
    runtime::mock::{MockSinkFactory, MockSinkRecorder, MockSourceAction, MockSourceFactory, MockSourcePlan},
    CamlCompiler, CamlManifest, RuntimeAdapters, RuntimeBuilder,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let manifest = CamlManifest::from_yaml_str(r#"
system:
  hardware_target: "GENERIC_LINUX"
  cma_allocation_limit: "128MB"
pipelines:
  - id: "camera_a"
    input: "rtsp://127.0.0.1:8554/live"
    type: "rtsp"
    strategy: "passthrough"
    network:
      transport: "tcp"
      packet_size_limit: 1200
      stall_timeout: 200ms
"#)?;

    let compiled = CamlCompiler::compile(&manifest)?;
    let source_factory = Arc::new(MockSourceFactory::new(std::collections::HashMap::from([(
        "camera_a".to_string(),
        MockSourcePlan::new(vec![MockSourceAction::EndOfStream]),
    )])));
    let sink_factory = Arc::new(MockSinkFactory::new(std::collections::HashMap::from([(
        "camera_a".to_string(),
        MockSinkRecorder::default(),
    )])));

    let runtime = RuntimeBuilder::new()
        .with_compiled_graph(compiled)
        .with_runtime_factory(RuntimeAdapters::new(source_factory, sink_factory))
        .start()
        .await?;

    runtime.shutdown().await?;
    Ok(())
}
```

## Runtime Model

The runtime is now a staged graph rather than a single byte pump:

- `MediaSource` produces `MediaPayload`
- zero or more `MediaTransform`s reshape the payload
- `MediaSink` consumes the final payload

Payloads can represent encoded packets or decoded frames, which gives the core runtime a path to support both passthrough and transcode pipelines without changing the public supervision model again.

## Recovery Model

Each compiled pipeline carries a recovery policy. On watchdog timeout the supervisor transitions through:

`Spawning -> Running -> Stalled -> Recovering -> Running/Failed`

The current tests cover:

- valid and invalid manifest parsing
- graph compilation guardrails
- capability probe merging, RTP packetization requirements, and Pi host/target mismatch rejection
- buffer reuse in the hot path
- transient stall detection and supervised recovery
- transient recoverable source errors that rebuild and restart a pipeline

## Roadmap

The next implementation milestone is:

1. Build the production libcamera FFI provider behind the new provider-backed `LibcameraSourceFactory` foundation
2. Expand adapter-specific recovery tuning beyond the compiled `network`/`device`/`hardware` recovery classes
3. Extend the host-gated Pi tests from probe-backed compile-time guardrails into full media-flow execution coverage for Pi 4 hardware encode and Pi 5 hardware decode

That path now has a concrete workspace structure and runtime contract to land into, instead of another round of aspirational prose.

## Claim Checklist

As part of our commitment to hardware honesty, this checklist maps `caml`'s bold architectural claims to the tests or benchmarks that prove them. **No claim is considered implemented unless linked here.**

- [ ] **No Subprocess Orchestration**: Enforced by static analysis in `tests/no_subprocess.rs`. Backends (FFmpeg, WebRTC, Libcamera) use native bindings.
- [ ] **Memory Model & Bounded Allocation**: Hot-path allocation ceilings are verified by `tests/hot_path_alloc.rs`.
- [ ] **RTSP Passthrough to WebRTC RTP**: Proven end-to-end.
- [ ] **Native Libcamera Provider**: Host-backed test in `tests/libcamera_host.rs` (or equivalent) using real V4L2/libcamera endpoints.
- [ ] **Pi 4 Hardware Encode Execution**: Tested via `CAML_PI_HOST_TESTS=1` against real hardware.
- [ ] **Pi 5 Stateless Decode Execution**: Tested via `CAML_PI_HOST_TESTS=1` against real hardware.
- [ ] **Class-Specific Recovery & Observability**: Tested via soak and chaos recovery tests.

*(Performance claims and zero-copy semantics will link to benchmark artifacts as they are finalized.)*
