# caml implementation roadmap

This document turns the README roadmap into explicit acceptance criteria. All work must preserve the repository constitution: native user-space media paths, no subprocess media execution, typed manifest/compiler structures, zero-copy where the backend allows it, and no per-frame hot-path allocation beyond bounded initialization pools.

## Milestone 1: Roadmap hygiene

Acceptance criteria:

- README status stays aligned with executable code and tests.
- Each roadmap item names the crate or module where the work should land.
- Incomplete hardware paths are documented as incomplete rather than implied to be production-ready.
- Every milestone has at least one automated check, host-gated integration test, or documented skip condition.

## Milestone 2: Libcamera-backed device capture

Target area: `crates/caml-linux-media` for Linux/Pi capture glue, with runtime integration through `caml_core::SourceFactory`.

Acceptance criteria:

- `backend: "libcamera"` remains a typed manifest/backend selection.
- Probe-backed capability validation rejects libcamera pipelines when the Linux probe cannot detect libcamera support.
- A libcamera source factory can build a `MediaSource` for `ResolvedInputBackend::LibcameraDevice` without using `std::process::Command` or shelling out to libcamera command-line tools.
- Provider implementations hand encoded packets or native frame buffers into `MediaPayload` without introducing avoidable frame-loop allocation or copies.
- Tests cover provider-backed source construction and end-of-stream behavior with a fake provider.

## Milestone 3: Adapter-specific recovery

Target area: `crates/caml-core/src/compiler.rs` and `crates/caml-core/src/runtime.rs`.

Acceptance criteria:

- Compiled pipelines carry an adapter-oriented recovery class (`network`, `device`, or `hardware`) in addition to restart limits and backoff.
- RTSP paths compile as network recovery.
- V4L2/libcamera device paths compile as device recovery.
- hardware-decode paths compile as hardware recovery.
- Runtime events keep exposing `Stalled` and `Recovering` transitions, while policy classification is available to adapters and future telemetry.

## Milestone 4: Pi host-backed execution coverage

Target area: host-gated integration tests and docs.

Acceptance criteria:

- Pi 4 hardware encode execution tests are available but skipped unless the host is a real Pi 4 with the expected V4L2 encoder topology.
- Pi 5 stateless hardware decode execution tests are available but skipped unless the host is a real Pi 5 with media topology exposing the stateless decoder.
- Skip messages explain the missing host prerequisite instead of failing generic CI.
- Tests validate probe-backed compile-time capability guardrails first, then can be extended to run real media once dedicated hardware runners exist.
