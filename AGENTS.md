# AGENTS.md ── System Prompt & Architecture Constitution

> **Attention AI Agents:** You are executing inside the repository context of `caml`. Before generating any code, refactoring logic, or modifying dependencies, you must ingest this document. Any pull request or code change that violates these directives will be rejected automatically.

---

## 1. System Role & Core Objective

Your objective when modifying this repository is to preserve an ultra-low-latency, hardware-aware, user-space media streaming runtime written in Rust. `caml` exists specifically to eliminate the overhead of OS process execution, IPC serialization, and memory bandwidth saturation on resource-constrained edge hardware (e.g., Raspberry Pi 4/5).

---

## 2. Unalterable Directives (The Constitution)

### Directive 1: Absolute Prohibition of Subprocesses

* **The Rule:** You must never under any circumstances emit code that calls `std::process::Command`, forks the current process, or runs shell scripts (`/bin/sh -c ...`).
* **The Reason:** `caml` is a pure-native runtime. Spawning external binaries (like the `ffmpeg` CLI) forces OS-level context switching, breaks native thread scheduling, and causes memory allocation thrashing across standard pipelines.
* **The Pattern:** All media operations (demuxing, parsing, decoding) must be performed within user-space memory via safe Foreign Function Interface (FFI) bindings (e.g., `ffmpeg-next`).

### Directive 2: Pure Native FFI and Zero-Copy Paths

* **The Rule:** Media packets must flow from the input network/device source directly into active WebRTC memory contexts (`webrtc-rs` tracks) without undergoing intermediary down-sampling or cloning unless explicitly flagged by a `transcode` strategy.
* **The Pattern:** Leverage `track.write_raw_rtp()` or direct pointer passing. Do not convert native C buffers into standard owned vectors (`Vec<u8>`) on the frame hot path.

### Directive 3: Hardware Honesty & Compile-Time Safeguards

* **The Rule:** Code changes must never abstract away physical hardware constraints. Software capabilities must strictly align with the target architecture defined in the manifest.
* **The Pattern:** If the hardware target is defined as `RASPBERRY_PI_5`, any configuration path specifying hardware encoding blocks must be short-circuited and rejected during the `validate_and_compile` phase. Do not implement software abstractions that trick the developer into thinking a physical block exists when it does not.

### Directive 4: Zero Allocations on Hot Paths

* **The Rule:** The frame ingestion loop must execute with an allocation complexity of $O(1)$ relative to runtime operations.
* **The Pattern:** Allocate a fixed-size buffer allocation pool during pipeline initialization. Reuse this heap space continuously across loops. Never declare `vec![0u8; size]` or perform dynamic allocations inside `loop {}` or asynchronous select branches.

### Directive 5: Non-Blocking Asynchronous Concurrency

* **The Rule:** Thread management must rely on cooperative async multitasking (`tokio`). Network stalls and watchdogs must be resolved inline using clean async state management, never via heavy thread blockages or OS process manipulation.
* **The Pattern:** Use `tokio::select!` combined with `tokio::time::timeout` for network watchdogs. When a stall occurs, transition the internal status enum (`TaskStatus`) smoothly to trigger a warm reconnection loop.

---

## 3. Anti-Drift Guardrails (Detecting LLM Hallucinations)

When an agent attempts to implement a complex feature, it can easily drift toward standard fallback solutions. Verify your proposed changes against this anti-pattern matrix:

| If you are trying to solve... | ❌ DO NOT DO THIS (Agent Drift) | libavcodec FFI API or native Rust primitives. |
| --- | --- | --- |
| **Stream Initialization** | Generate arguments to call a local `ffmpeg` CLI process. | Interface directly with `ffmpeg_next::format::input` to open network context. |
| **Network Loss Handling** | Kill the process and invoke a shell-level bash loop to restart it. | Catch `AVERROR` codes natively, transition state to `WatchdogStall`, and retry async. |
| **Frame Rotations / Processing** | Pass frames back into a heavy software-scaler or file write pipeline. | Perform direct user-space manipulations within the `SAND`/`NC12` memory layout. |
| **Buffer Management** | Unmarshal and map every packet into highly verbose intermediate structs. | Pass raw packet slices directly to the underlying `write` API to preserve memory bandwidth. |

---

## 4. Manifest Schema Enforcement

If you are modifying the parser frontend (`src/frontend.rs`) or the configuration schema, you must adhere strictly to the type layout specified below. Never introduce untyped or loose string parameters (`K: String, V: String`) for critical structural behaviors.

```
                  ┌──────────────────────────────┐
                  │      CamlManifest (Root)     │
                  └──────────────┬───────────────┘
                                 │
         ┌───────────────────────┴───────────────────────┐
         ▼                                               ▼
┌─────────────────┐                             ┌──────────────────┐
│  SystemConfig   │                             │  PipelineNode[]  │
└────────┬────────┘                             └────────┬─────────┘
         │                                               │
         ├─ hardware_target: HardwareTarget (Enum)       ├─ strategy: StreamStrategy (Enum)
         └─ cma_allocation_limit: ByteUnit               ├─ network: Option<NetworkProfile>
                                                         └─ processing: Option<ProcessingProfile>

```

---

## 5. Architectural Verification Checklist

Before finalizing any modifications, step through this validation script mentally or execute the integrated tests:

1. **Did I inject an allocation?** Check if any macro, `to_owned()`, `clone()`, or vector generation was added to `src/runtime.rs`.
2. **Did I handle FFI safety boundaries?** Ensure raw pointers retrieved from underlying C layers are wrapped cleanly, and memory is dropped safely when structures exit scope.
3. **Is my error handling structured?** Never swallow internal system discrepancies or convert everything into string formatting (`.map_err(|e| e.to_string())`). Map explicit error primitives back to `caml::compiler::CompileError` or standard `anyhow` contexts.
