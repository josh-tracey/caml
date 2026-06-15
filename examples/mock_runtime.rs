//! Example: Run a mock pipeline through the RuntimeEngine.

use std::collections::HashMap;
use std::sync::Arc;

use caml::runtime::mock::{
    MockSinkFactory, MockSinkRecorder, MockSourceAction, MockSourceFactory, MockSourcePlan,
};
use caml::{CamlCompiler, CamlManifest, RuntimeAdapters, RuntimeEngine};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let manifest = CamlManifest::from_yaml_str(
        r#"
system:
  hardware_target: "GENERIC_LINUX"
  cma_allocation_limit: "128MB"
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

    let compiled = CamlCompiler::compile(&manifest)?;

    let source_factory = Arc::new(MockSourceFactory::new(HashMap::from([(
        "test_pipeline".to_string(),
        MockSourcePlan::new(vec![
            MockSourceAction::Packet(vec![0x00, 0x00, 0x00, 0x01, 0x67]),
            MockSourceAction::Packet(vec![0x00, 0x00, 0x00, 0x01, 0x68]),
            MockSourceAction::Packet(vec![0x00, 0x00, 0x00, 0x01, 0x65]),
            MockSourceAction::EndOfStream,
        ]),
    )])));

    let recorder = MockSinkRecorder::default();
    let sink_factory = Arc::new(MockSinkFactory::new(HashMap::from([(
        "test_pipeline".to_string(),
        recorder.clone(),
    )])));

    let adapters = RuntimeAdapters::new(source_factory, sink_factory);
    let handle = RuntimeEngine::start(compiled, adapters, None).await?;

    // Wait for the pipeline to finish processing
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    let status = handle.status().await;
    println!("Pipeline status: {:?}", status);
    println!("Packets received: {}", recorder.payloads().await.len());

    handle.shutdown().await?;
    println!("Runtime shut down cleanly.");

    Ok(())
}
