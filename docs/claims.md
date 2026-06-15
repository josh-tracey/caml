# Claim Evidence Registry

Every claim in the README's **Claim Checklist** must have a corresponding entry here documenting its status, evidence, scope, and known limitations.

Valid statuses: `implemented`, `partial`, `experimental`, `not implemented`.

An `implemented` claim must name at least one automated or host-gated test.

---

## No Subprocess Orchestration

**Status:** implemented

**Evidence:**
- Static test: [`tests/no_subprocess.rs`](../tests/no_subprocess.rs) — scans all `.rs` files under `crates/` for `std::process::Command`, `Command::new`, `kill -9`, and `sh -c`.
- CI job: `.github/workflows/ci.yml` → `core` job runs this test as part of the workspace tests.
- Architecture: all backends (FFmpeg via `ffmpeg-next`, WebRTC via `webrtc-rs`, Libcamera via `libcamera` crate) use native Rust FFI bindings.

**Scope:** `crates/` directory only.

**Known limitations:** Does not detect dynamic subprocess invocation through transitive dependencies. Does not scan the top-level `caml` crate source (only `crates/`).

---

## Buffer Reuse

**Status:** implemented

**Evidence:**
- `BufferPool` in [`crates/caml-core/src/runtime.rs`](../crates/caml-core/src/runtime.rs) — `MediaBuffer` returns its `Vec<u8>` to the pool on `Drop`.
- Test: [`tests/runtime.rs`](../tests/runtime.rs) → `runtime_reuses_the_same_working_buffer` verifies same pointer across multiple packets.
- API: Added `BufferPool::preallocate(count)` and `BufferPool::stats()` to query availability and avoid on-demand allocation.

**Known limitations:** None.

---

## Bounded Hot-Path Allocation

**Status:** implemented

**Evidence:**
- Test: [`tests/hot_path_alloc.rs`](../tests/hot_path_alloc.rs) implements a custom counting allocator (`#[global_allocator]`) to verify that zero dynamic memory allocations are made during hot-loop frame ingestion and processing.

**Known limitations:** None.

---

## Zero-Copy Media Path

**Status:** partial

**Evidence:**
- `MediaStorage` enum in [`crates/caml-core/src/runtime.rs`](../crates/caml-core/src/runtime.rs) distinguishes between `Pooled` and `Borrowed` buffers to pass data slices down the processing pipeline without cloning/allocation.
- Bounded allocation tests prove that no copies/allocations are performed under standard ingestion workloads.

**Known limitations:** WebRTC packetization (`caml-webrtc`) performs a slice copy into the packetizer payload buffer due to third-party crate (`webrtc-rs`) API constraints.

---

## RTSP to WebRTC RTP

**Status:** implemented

**Evidence:**
- `caml-webrtc` crate implements `WebRtcSink` which handles H.264 packetization and writes to `TrackLocalStaticRTP`.
- Test: `test_custom_config_propagation` in [`crates/caml-webrtc/src/lib.rs`](../crates/caml-webrtc/src/lib.rs) verifies config parameters (`payload_type`, `mtu`, `ssrc`, `clock_rate`) propagate to generated RTP packets.
- Test: [`tests/readme_manifest.rs`](../tests/readme_manifest.rs) parses the manifest and builds the pipeline adapter with WebRTC output.

**Known limitations:** Actual WebRTC peer connection management (ICE, SDP) is handled outside of the media compiler scope.

---

## Native Libcamera Provider

**Status:** experimental

**Evidence:**
- Modularized camera code split into config, frame, error, and native wrapper files in [`crates/caml-linux-media/src/camera/`](../crates/caml-linux-media/src/camera/).
- Safe background worker thread model with standard channels implemented to resolve unsafe raw pointer casting.
- Configuration matching of `CaptureProfile` dimensions, pixel format, and frame rate.
- Gated tests check compilation and mock execution, skipping gracefully if target hardware is absent.

**Known limitations:**
- Requires physical camera and libcamera system configuration to run natively.

---

## Pi Hardware Guardrails

**Status:** implemented

**Evidence:**
- `CamlCompiler` in [`crates/caml-core/src/compiler.rs`](../crates/caml-core/src/compiler.rs) enforces validation of target architecture, hardware encode blocks, and stateless decoding pathways.
- Test: [`tests/compiler.rs`](../tests/compiler.rs) validates the compiler's strict guardrail checks, target model validation, and capability probe integration.

**Known limitations:** None.

---

## Pi 4 Hardware Encode Execution

**Status:** implemented

**Evidence:**
- Test: [`tests/pi4_hardware_encode_flow.rs`](../tests/pi4_hardware_encode_flow.rs) validates execution under the `RASPBERRY_PI_4` target.
- Automatically skipped when executed on non-Pi 4 hosts.

**Known limitations:** Requires physical Pi 4 with functional V4L2 H.264 encode blocks.

---

## Pi 5 Stateless Decode Execution

**Status:** implemented

**Evidence:**
- Test: [`tests/pi5_stateless_decode_flow.rs`](../tests/pi5_stateless_decode_flow.rs) validates stateless hardware decoding under the `RASPBERRY_PI_5` target.
- Automatically skipped when executed on non-Pi 5 hosts.

**Known limitations:** Requires physical Pi 5 with functional V4L2 request API decode blocks.

---

## Recovery Classes and Metrics Hooks

**Status:** implemented

**Evidence:**
- `RecoveryClass` enum splits errors into `Network`, `Device`, and `Hardware` categories.
- `MetricsExporter` trait in [`crates/caml-core/src/metrics.rs`](../crates/caml-core/src/metrics.rs) tracks restarts, backpressure drops, memory watermarks, throughput, and stream errors.
- Test: [`tests/soak_tests.rs`](../tests/soak_tests.rs) runs chaos recovery simulations.

**Known limitations:** None.

---

## Class-Specific Recovery Behavior

**Status:** implemented

**Evidence:**
- Supervisor implementation in [`crates/caml-core/src/runtime.rs`](../crates/caml-core/src/runtime.rs) adapts exponential backoff and timeout strategies based on the recovery class.
- Test: [`tests/recovery_policy.rs`](../tests/recovery_policy.rs) and [`tests/recovery_chaos.rs`](../tests/recovery_chaos.rs) verify recovery progression, backoff limits, and reset parameters.

**Known limitations:** None.
