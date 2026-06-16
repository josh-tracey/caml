//! Integration tests for the `FanoutRouter` multi-sink distribution layer.
//!
//! Validates:
//! * payloads are delivered to all registered sinks,
//! * `DropOldest` and `DropNewest` policies drop frames rather than blocking,
//! * cloning a `MediaPayload` backed by a `PooledBuffer` does not heap-allocate
//!   on the payload path (pool stats are used as the allocation witness).

use std::{
    sync::Arc,
    time::Duration,
};

use async_trait::async_trait;
use caml::{
    runtime::{
        BufferPool, DropPolicy, EncodedPacket, FanoutRouter, MediaPayload, MediaSink,
        MediaStorage, PipelineContext, RecordedPacket, RecordingSink, SinkActorConfig,
    },
    CompiledPipeline, RuntimeError,
};

// ---------------------------------------------------------------------------
// Helper: build a minimal PipelineContext backed by its own pool.
// ---------------------------------------------------------------------------

fn make_context(pool_size: usize) -> PipelineContext {
    let pool = BufferPool::new(pool_size);
    pool.preallocate(8);
    PipelineContext {
        pipeline: CompiledPipeline::sentinel(),
        buffer_pool: pool,
        metrics: None,
    }
}

// ---------------------------------------------------------------------------
// Helper: build a `MediaPayload::EncodedPacket` from a pool buffer.
// ---------------------------------------------------------------------------

fn make_packet(ctx: &mut PipelineContext, bytes: &[u8]) -> MediaPayload {
    let mut buf = ctx.acquire_buffer();
    buf.extend_from_slice(bytes);
    MediaPayload::EncodedPacket(EncodedPacket {
        codec: "h264".to_string(),
        timestamp: None,
        duration: None,
        is_keyframe: false,
        data: MediaStorage::Pooled(buf.freeze()),
    })
}

// ---------------------------------------------------------------------------
// Helper: build a shared `RecordingSink`.
// ---------------------------------------------------------------------------

fn make_recording_sink() -> (
    Box<dyn MediaSink>,
    Arc<tokio::sync::Mutex<Vec<RecordedPacket>>>,
) {
    let packets: Arc<tokio::sync::Mutex<Vec<RecordedPacket>>> =
        Arc::new(tokio::sync::Mutex::new(Vec::new()));
    let sink = Box::new(RecordingSink {
        packets: packets.clone(),
    });
    (sink, packets)
}

// ---------------------------------------------------------------------------
// Test 1: two sinks each receive every payload (fanout semantics).
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_fanout_delivers_to_all_sinks() {
    let (sink_a, packets_a) = make_recording_sink();
    let (sink_b, packets_b) = make_recording_sink();

    let configs = vec![
        SinkActorConfig {
            sink: sink_a,
            queue_limit: 32,
            drop_policy: DropPolicy::Block,
        },
        SinkActorConfig {
            sink: sink_b,
            queue_limit: 32,
            drop_policy: DropPolicy::Block,
        },
    ];

    let mut router = FanoutRouter::new(configs);
    let mut ctx = make_context(512);

    for i in 0u8..5 {
        let payload = make_packet(&mut ctx, &[i, i + 1, i + 2]);
        router
            .consume(payload, &mut ctx)
            .await
            .expect("fanout consume should not fail");
    }

    // Give actor tasks time to drain their queues.
    tokio::time::sleep(Duration::from_millis(100)).await;

    let a = packets_a.lock().await;
    let b = packets_b.lock().await;

    assert_eq!(a.len(), 5, "sink A should have received all 5 packets");
    assert_eq!(b.len(), 5, "sink B should have received all 5 packets");

    // All delivered packets should be 3 bytes each.
    for pkt in a.iter().chain(b.iter()) {
        assert_eq!(pkt.bytes, 3);
    }
}

// ---------------------------------------------------------------------------
// Test 2: `DropNewest` policy — overflowed frames are silently discarded.
// ---------------------------------------------------------------------------

/// A sink that blocks forever so the channel fills up immediately.
struct BlockingSink;

#[async_trait]
impl MediaSink for BlockingSink {
    async fn consume(
        &mut self,
        _payload: MediaPayload,
        _context: &mut PipelineContext,
    ) -> Result<(), RuntimeError> {
        // Never drain — simulates a stalled downstream.
        std::future::pending::<()>().await;
        Ok(())
    }
}

#[tokio::test]
async fn test_drop_newest_does_not_block_ingestion() {
    let configs = vec![SinkActorConfig {
        sink: Box::new(BlockingSink),
        queue_limit: 2,
        drop_policy: DropPolicy::DropNewest,
    }];

    let mut router = FanoutRouter::new(configs);
    let mut ctx = make_context(512);

    // Sending more frames than queue_limit must not block.
    for i in 0u8..10 {
        let payload = make_packet(&mut ctx, &[i]);
        tokio::time::timeout(Duration::from_millis(200), router.consume(payload, &mut ctx))
            .await
            .expect("consume must not block when DropNewest policy is active")
            .expect("fanout consume should not return an error");
    }
}

// ---------------------------------------------------------------------------
// Test 3: `DropOldest` policy — ingestion is non-blocking even on full queue.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_drop_oldest_does_not_block_ingestion() {
    let configs = vec![SinkActorConfig {
        sink: Box::new(BlockingSink),
        queue_limit: 2,
        drop_policy: DropPolicy::DropOldest,
    }];

    let mut router = FanoutRouter::new(configs);
    let mut ctx = make_context(512);

    for i in 0u8..10 {
        let payload = make_packet(&mut ctx, &[i]);
        tokio::time::timeout(Duration::from_millis(200), router.consume(payload, &mut ctx))
            .await
            .expect("consume must not block when DropOldest policy is active")
            .expect("fanout consume should not return an error");
    }
}

// ---------------------------------------------------------------------------
// Test 4: `MediaPayload` clone does not expand the pool's allocation count —
// confirming no heap allocation occurs per clone on the hot path.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_pooled_buffer_clone_is_allocation_free() {
    let pool = BufferPool::new(256);
    pool.preallocate(4);
    let mut ctx = PipelineContext {
        pipeline: CompiledPipeline::sentinel(),
        buffer_pool: pool.clone(),
        metrics: None,
    };

    // Pre-snapshot: pool stats before payload creation.
    let stats_before = pool.stats();

    let payload = make_packet(&mut ctx, b"hello fanout");

    // After construction: exactly one slot is in use.
    let stats_after_create = pool.stats();
    // The buffer was acquired from the pre-allocated pool, so in_use should
    // not exceed what was available (i.e. no new allocation).
    // high_watermark stays the same because preallocate set it.
    assert_eq!(
        stats_after_create.high_watermark, stats_before.high_watermark,
        "creating a payload must not grow the high-watermark"
    );

    // Clone the payload (simulates FanoutRouter distributing to N sinks).
    let clone1 = payload.clone();
    let clone2 = payload.clone();
    let stats_after_clone = pool.stats();

    // Cloning must not increase the high-watermark — no new Vec is allocated.
    assert_eq!(
        stats_after_clone.high_watermark, stats_before.high_watermark,
        "cloning a PooledBuffer payload must not allocate a new buffer slot"
    );

    // Verify all clones see identical data.
    assert_eq!(payload.data(), clone1.data());
    assert_eq!(payload.data(), clone2.data());

    // After all handles are dropped, the slot is reclaimed.
    drop(payload);
    drop(clone1);
    drop(clone2);

    let stats_final = pool.stats();
    assert!(
        stats_final.available >= stats_before.available,
        "slot should be returned to the pool after all PooledBuffer handles are dropped"
    );
}

// ---------------------------------------------------------------------------
// Test 5: FanoutRouter with zero sinks is a no-op and does not panic.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_empty_fanout_is_noop() {
    let mut router = FanoutRouter::new(vec![]);
    let mut ctx = make_context(256);

    let payload = make_packet(&mut ctx, b"noop");
    router
        .consume(payload, &mut ctx)
        .await
        .expect("empty fanout should succeed without error");
}
