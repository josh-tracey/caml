//! Example: Run a full pipeline using the convenience builder pattern.

use caml::{CamlError, CamlPipeline};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let manifest_str = r#"
system:
  hardware_target: "GENERIC_LINUX"
  media_memory_limit: "512MB"
pipelines:
  - id: "full_pipeline"
    input: "rtsp://mock-host/live"
    type: "rtsp"
    strategy: "passthrough"
    network:
      transport: "tcp"
      packet_size_limit: 1200
      stall_timeout: 10s
    outputs:
      - type: "recording"
"#;

    println!("Initializing CamlPipelineBuilder...");
    let builder = CamlPipeline::from_manifest_str(manifest_str)?
        .with_feature_capability_probe()
        .with_native_adapters();

    println!("Starting runtime...");
    let runtime = match builder.start().await {
        Ok(rt) => rt,
        Err(CamlError::MissingAdapter {
            pipeline_id,
            backend,
        }) => {
            println!(
                "Notice: missing adapter for pipeline '{}' (backend '{}'). Ensure the feature is enabled.",
                pipeline_id, backend
            );
            return Ok(());
        }
        Err(e) => return Err(e.into()),
    };

    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    println!("Shutting down runtime...");
    runtime.shutdown().await?;
    println!("Full runtime clean shutdown completed.");

    Ok(())
}
