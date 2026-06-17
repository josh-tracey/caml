use std::alloc::{GlobalAlloc, Layout, System};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use caml::runtime::mock::{
    MockSinkFactory, MockSinkRecorder, MockSourceAction, MockSourceFactory, MockSourcePlan,
};
use caml::{CamlCompiler, CamlManifest, RuntimeAdapters, RuntimeEngine};

struct CountingAllocator;

static ALLOC_COUNT: AtomicUsize = AtomicUsize::new(0);
static TRACK_ALLOC: AtomicBool = AtomicBool::new(false);

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // Only count larger allocations (like media buffers) to avoid counting
        // tokio/future runtime internals. Media buffers are typically >= 1024 bytes.
        if TRACK_ALLOC.load(Ordering::Relaxed) && layout.size() >= 512 {
            ALLOC_COUNT.fetch_add(1, Ordering::Relaxed);
        }
        System.alloc(layout)
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        System.dealloc(ptr, layout)
    }
}

#[global_allocator]
static A: CountingAllocator = CountingAllocator;

#[tokio::test]
async fn test_hot_path_zero_allocations() {
    let manifest = CamlManifest::from_yaml_str(
        r#"
system:
  hardware_target: "GENERIC_LINUX"
  cma_allocation_limit: "128MB"
pipelines:
  - id: "alloc_test"
    input: "rtsp://127.0.0.1:8554/live"
    type: "rtsp"
    strategy: "passthrough"
    network:
      transport: "tcp"
      packet_size_limit: 1024
      stall_timeout: 10s
"#,
    )
    .unwrap();

    let mut compiled = CamlCompiler::compile(&manifest).unwrap();

    // Set buffer size to 1024
    compiled.pipelines[0].runtime.buffer_size = 1024;

    // We preallocate the buffer pool so it has enough buffers before starting
    // Our mock pipeline will run one packet at a time, so 5 buffers is plenty.
    let recorder = MockSinkRecorder::default();
    let recorders = HashMap::from([("alloc_test".to_string(), recorder.clone())]);

    // Let's run a warmup phase to let the runtime boot and initialize.
    let warmup_actions = vec![
        MockSourceAction::Packet(vec![0; 100]),
        MockSourceAction::Packet(vec![0; 100]),
        MockSourceAction::EndOfStream,
    ];
    let plans = HashMap::from([(
        "alloc_test".to_string(),
        MockSourcePlan::new(warmup_actions),
    )]);

    let source_factory = Arc::new(MockSourceFactory::new(plans));
    let adapters = RuntimeAdapters::new(
        source_factory,
        Arc::new(MockSinkFactory::new(recorders.clone())),
    );

    let handle = RuntimeEngine::start(compiled.clone(), adapters, None)
        .await
        .unwrap();

    // Let warmup finish
    tokio::time::sleep(Duration::from_millis(50)).await;
    handle.shutdown().await.unwrap();

    // Now start the tracked measurement phase.
    // We pre-fill the actions for the measurement run.
    let mut measurement_actions = Vec::new();
    for _ in 0..50 {
        measurement_actions.push(MockSourceAction::Packet(vec![0; 100]));
    }
    measurement_actions.push(MockSourceAction::EndOfStream);

    let plans = HashMap::from([(
        "alloc_test".to_string(),
        MockSourcePlan::new(measurement_actions),
    )]);
    let source_factory = Arc::new(MockSourceFactory::new(plans));
    let adapters = RuntimeAdapters::new(source_factory, Arc::new(MockSinkFactory::new(recorders)));

    let handle = RuntimeEngine::start(compiled, adapters, None)
        .await
        .unwrap();

    // Wait a brief moment for the runtime to spin up threads, then start tracking
    tokio::time::sleep(Duration::from_millis(20)).await;

    // Reset allocation count and enable tracking
    ALLOC_COUNT.store(0, Ordering::Relaxed);
    TRACK_ALLOC.store(true, Ordering::Relaxed);

    // Let the 50 packets get processed
    tokio::time::sleep(Duration::from_millis(150)).await;

    // Disable tracking
    TRACK_ALLOC.store(false, Ordering::Relaxed);

    let final_allocs = ALLOC_COUNT.load(Ordering::Relaxed);
    println!(
        "Total large allocations during measurement: {}",
        final_allocs
    );

    // Shutdown cleanly
    handle.shutdown().await.unwrap();

    // Assert that zero large allocations occurred during the hot path frame loop
    assert_eq!(
        final_allocs, 0,
        "Expected 0 buffer allocations on the hot path, found {}",
        final_allocs
    );
}
