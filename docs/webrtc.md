# WebRTC Boundary and Integration Guide

In `caml`, WebRTC streaming is split between the low-level media runtime and the host application layer.

```
┌──────────────────────────────────────┐     ┌───────────────────────────────────────┐
│         Host Application             │     │             caml-webrtc               │
│                                      │     │                                       │
│  - WebSocket / HTTP Signaling Server │     │  - Pooled media buffers               │
│  - ICE / STUN Configuration          │     │  - Annex-B (H.264) parsing            │
│  - SDP Offer/Answer Exchange         │────>│  - RTP packetization (MTU-aware)      │
│  - RTCPeerConnection Lifecycle       │     │  - Thread-safe TrackLocalStaticRTP    │
│  - Peer management and session keys  │     │    raw packet writes                  │
└──────────────────────────────────────┘     └───────────────────────────────────────┘
```

## Division of Responsibilities

### What `caml` handles (Native Media Path)
- **Buffer Management**: Reuses allocations via `BufferPool` to prevent dynamic heap fragmentation.
- **RTP Packetization**: Splits incoming H.264 Annex-B frames into MTU-compliant RTP packets.
- **Backpressure & Watchdog**: If a track write times out (e.g. peer disconnected or socket blocked), `caml` terminates or recovers the pipeline cleanly.
- **Zero-Copy writes**: RTP packet payloads are written directly using FFI bindings to avoid intermediate user-space vector copying where possible.

### What the Application must handle (Orchestration & Control Path)
- **Signaling**: Exchanging SDP offers, answers, and ICE candidates between browser clients and the server via WebSockets, gRPC, or HTTP.
- **Session Lifecycle**: Creating, closing, and tracking `RTCPeerConnection` instances for active users.
- **Track Registration**: Constructing `TrackLocalStaticRTP` objects, adding them to peer connections via `.add_track()`, and passing them into `caml`'s pipeline builder.

## Code Example

For a complete working example showing how to initialize an `RTCPeerConnection`, register a static RTP track, and pass it to `CamlPipelineBuilder`, see [webrtc_peer_connection.rs](file:///Users/adrift/projects/caml/examples/webrtc_peer_connection.rs).
