# caml ── Camera Architecture Markup Language

`caml` is a hardware-aware, declarative pipeline compiler and native runtime written in Rust. It eliminates the orchestration boilerplate, context-switching overhead, and process-thrashing typical of embedded video streaming systems on single-board computers like the Raspberry Pi 4 and Pi 5.

Instead of spawning heavy `ffmpeg` or `libcamera` child processes, wrapping shell commands, and routing raw data over local loopback network interfaces, `caml` parses a simple, human-readable manifest file and compiles it directly into an optimized, user-space asynchronous media graph.

---

## Core Ethos & Design Principles

Production video streaming on edge hardware shouldn't rely on fragile string parsing or shell abstractions. `caml` is engineered around three baseline tenets:

* **Zero Process Forks (Pure FFI):** The runtime links directly to the underlying shared C multimedia libraries (`libavcodec`, `libavformat`, `libavutil`, `libswscale`) via safe Rust bindings. All networking, demuxing, and frame extraction loops run natively inside the application's user-space memory layout.
* **Hardware Honesty:** The compiler enforces physical silicon realities at compile time. If a chip lacks a hardware encoder block (like the Broadcom BCM2712 on the Raspberry Pi 5), the compiler rejects the layout configuration *before* it can deploy, preventing silent runtime failures.
* **Single-Allocation Pipelines:** To completely eliminate heap fragmentation and allocation thrashing, memory buffers are declared once at startup and recycled over non-blocking asynchronous execution channels.

---

## The Network Loopback Bottleneck vs. Native FFI

Traditional architectures utilize a process-wrapper pattern that introduces a massive internal performance penalty:

```
[ Traditional ] : Camera Feed ──► External FFmpeg Process ──► UDP Loopback (127.0.0.1) ──► Runtime Socket ──► WebRTC Out
[ caml Native ] : Camera Feed ──► Direct FFI Memory Context ───────────────────────────────────────────────► WebRTC Out

```

### Why the Native FFI Path Wins:

1. **Zero Pipe Bottlenecks:** Process-wrapper systems pipe raw video data across standard I/O channels or local UDP loopbacks, forcing the kernel to continuously copy video blocks across memory spaces. `caml` opens and reads input streams directly inside its own memory space.
2. **Granular Memory Control:** By talking directly to the underlying C structures, `caml` directly exposes the Raspberry Pi 5’s stateless V4L2 decoding pipelines. Frames are processed inside hardware-allocated memory regions (`DRM_PRIME` / `SAND` formats) rather than down-sampling blindly through system RAM.
3. **Synchronous Error Catching:** Instead of scraping text from an external process's standard error stream to diagnose dropouts, `caml` captures explicit error codes (`AVERROR`) natively returned across the FFI boundary, triggering immediate, clean recovery paths.

---

## The Manifest Specification (`pipelines.caml`)

You define your entire multi-camera topology in a declarative, scannable configuration file:

```yaml
system:
  hardware_target: "RASPBERRY_PI_5" # Valid options: [RASPBERRY_PI_4, RASPBERRY_PI_5, GENERIC_LINUX]
  cma_allocation_limit: 512MB      # Memory safety limit for stateless allocation pools

pipelines:
  - id: "forward_payload_optics"
    input: "rtsp://192.168.1.50:554/stream1"
    type: "rtsp"
    strategy: "passthrough"         # Zero-copy packet cloning straight into WebRTC tracks
    network:
      transport: "tcp"              # Enforces TCP delivery to eliminate UDP packet drops
      packet_size_limit: 1200       # Matches MTU targets to avoid packet fragmentation
      stall_timeout: 10s            # Watchdog reset loop limit for stalled network links

  - id: "belly_optical_sensor"
    input: "/dev/video0"
    type: "device"
    strategy: "transcode"
    processing:
      codec: "h264"
      encoder: "software"           # Auto-selects libx264 utilizing ARMv8 NEON assembly vectors on Pi 5
      preset: "ultrafast"
      tune: "zerolatency"
      frame_rate: 30
      bitrate: 512k
      rotation: 90                  # Applied inside user-space memory

```

---

## Compilation & Guardrail Matrix

When `caml` evaluates your file, it analyzes the pipeline node choices against the structural architecture of your targeted chip:

| Target Hardware | Chosen Strategy | Compiler Action | Native Data Pathway |
| --- | --- | --- | --- |
| **Raspberry Pi 5** | `passthrough` | Approved | Bypasses codec blocks; clones raw frames to WebRTC |
| **Raspberry Pi 5** | `transcode` (`hardware`) | **Rejected at Build** | Throws error: Pi 5 lacks hardware H.264/H.265 encoding blocks |
| **Raspberry Pi 5** | `transcode` (`software`) | Approved | Generates multi-threaded, NEON-optimized software loops |
| **Raspberry Pi 5** | `hardware_decode` | Approved | Hooks stateless V4L2 decoder via `DRM_PRIME` buffers |
| **Raspberry Pi 4** | `transcode` (`hardware`) | Approved | Allocates and boots the legacy stateful `h264_v4l2m2m` engine |

---

## Quick Start Guide

### 1. System Dependencies

Because `caml` uses direct FFI integration, you must ensure your build target has the necessary FFmpeg development libraries installed:

```bash
# Ubuntu/Debian/Raspberry Pi OS
sudo apt-get install libavcodec-dev libavformat-dev libavutil-dev libswscale-dev libavdevice-dev

```

### 2. Configuration Setup

Add `caml` to your project's `Cargo.toml` dependency block:

```toml
[dependencies]
caml = { git = "https://github.com/your-org/caml.git" }
tokio = { version = "1.38", features = ["full"] }

```

### 3. Application Implementation Example

```rust
use std::sync::Arc;
use caml::{CamlManifest, CamlCompiler, RuntimeEngine};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // 1. Read the declarative configuration file
    let raw_manifest = std::fs::read_to_string("config/pipelines.caml")?;
    let manifest: CamlManifest = serde_yaml::from_str(&raw_manifest)?;

    // 2. Pass layout rules through the hardware validation compiler
    CamlCompiler::validate_and_compile(&manifest)?;

    // 3. Allocate a native WebRTC output target
    let rtp_track = Arc::new(webrtc::track::track_local::track_local_static_rtp::TrackLocalStaticRTP::new(
        webrtc::rtp_transceiver::rtp_codec::RTPCodecCapability::default(),
        "video".to_string(),
        "caml-stream".to_string(),
    ));

    // 4. Build and boot the native async runtime engine
    let target_node = &manifest.pipelines[0];
    let net_profile = target_node.network.as_ref().unwrap();
    
    let engine = RuntimeEngine::new(
        target_node.id.clone(),
        target_node.input.clone(),
        5004,                            // Target binding port
        net_profile.packet_size_limit,    // Core block buffer allocation ceiling
        net_profile.stall_timeout,       // Active watchdog threshold
    );

    println!("[caml] Spawning async media graph...");
    engine.start_pipeline(rtp_track).await;

    // Block current execution context to maintain the streaming runtime
    tokio::signal::ctrl_c().await?;
    Ok(())
}

```

---

## Built-In Watchdog Mechanics

The runtime natively embeds your production-tested streaming resilience logic. If a network camera stops transmitting frames or drops packets, the asynchronous `tokio::time::timeout` monitor catches the gap based on your `stall_timeout` setting.

Instead of executing a chaotic, resource-heavy process kill (`kill -9`) and clean-spawning a child runtime, `caml` safely flushes the internal buffer, puts the specific pipeline loop into a `TaskStatus::WatchdogStall` state, and executes a lightweight, warm user-space reconnection sequence without disturbing adjacent camera tasks.
