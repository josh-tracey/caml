//! Example: Run a mock pipeline through the RuntimeBuilder.

use std::collections::HashMap;
use std::sync::Arc;

use caml::runtime::mock::{
    MockSinkFactory, MockSinkRecorder, MockSourceAction, MockSourceFactory, MockSourcePlan,
};
use caml::{CamlManifest, RuntimeAdapters, RuntimeBuilder};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let manifest = CamlManifest::from_yaml_str(
        r#"
system:
  hardware_target: "GENERIC_LINUX"
  media_memory_limit: "128MB"
pipelines:
  - id: "test_pipeline"
    input: "rtsp://127.0.0.1:8554/live"
    type: "rtsp"
    strategy: "passthrough"
    network:
      transport: "tcp"
      packet_size_limit: 1200
      stall_timeout: 200ms
"#,
    )?;

    let source_plan = MockSourcePlan::new(vec![
        MockSourceAction::Packet(vec![0x00, 0x00, 0x00, 0x01, 0x67]),
        MockSourceAction::Packet(vec![0x00, 0x00, 0x00, 0x01, 0x68]),
        MockSourceAction::Packet(vec![0x00, 0x00, 0x00, 0x01, 0x65]),
        MockSourceAction::EndOfStream,
    ]);
    
    let source_factory = Arc::new(MockSourceFactory::new(HashMap::from([(
        "test_pipeline".to_string(),
        source_plan,
    )])));

    let recorder = MockSinkRecorder::default();
    let sink_factory = Arc::new(MockSinkFactory::new(HashMap::from([(
        "test_pipeline".to_string(),
        recorder.clone(),
    )])));

    let adapters = RuntimeAdapters::new(source_factory, sink_factory);
    
    // Start the runtime using the builder pattern
    let handle = RuntimeBuilder::from_manifest(manifest)
        .with_runtime_factory(adapters)
        .start()
        .await?;

    // Wait for the pipeline to finish processing
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    let status = handle.status().await;
    println!("Pipeline status: {:?}", status);
    println!("Packets received: {}", recorder.payloads().await.len());

    handle.shutdown().await?;
    println!("Runtime shut down cleanly.");

    Ok(())
}
