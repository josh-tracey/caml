//! Example: Parse and compile a YAML manifest into a runtime-ready graph.

use caml::{CamlCompiler, CamlManifest};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let manifest = CamlManifest::from_yaml_str(r#"
system:
  hardware_target: "GENERIC_LINUX"
  cma_allocation_limit: "256MB"

pipelines:
  - id: "camera_feed"
    input: "rtsp://192.168.1.50:554/stream1"
    type: "rtsp"
    strategy: "passthrough"
    network:
      transport: "tcp"
      packet_size_limit: 1200
      stall_timeout: 5s
    outputs:
      - type: "webrtc_rtp"
        codec: "h264"
        payload_type: 102
        mtu: 1200
        clock_rate: 90000
"#)?;

    let compiled = CamlCompiler::compile(&manifest)?;

    println!("Compiled {} pipeline(s)", compiled.pipelines.len());
    for pipeline in &compiled.pipelines {
        println!("  Pipeline '{}': {:?} via {:?}",
            pipeline.id,
            pipeline.execution_mode,
            pipeline.resolved_backend,
        );
        println!("    Recovery class: {:?}", pipeline.recovery.class);
        println!("    Outputs: {}", pipeline.outputs.len());
    }

    Ok(())
}
