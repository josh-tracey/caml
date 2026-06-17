//! Example: Run a WebRTC pipeline using the convenience builder pattern.
//! Gated by the `webrtc` feature.

#[cfg(feature = "webrtc")]
use caml::{webrtc::TrackLocalStaticRTP, CamlPipeline};
#[cfg(feature = "webrtc")]
use std::sync::Arc;
#[cfg(feature = "webrtc")]
use webrtc::rtp_transceiver::rtp_codec::RTCRtpCodecCapability;

#[cfg(feature = "webrtc")]
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let manifest_str = r#"
system:
  hardware_target: "GENERIC_LINUX"
  media_memory_limit: "256MB"
pipelines:
  - id: "webrtc_pipeline"
    input: "rtsp://mock-host/live"
    type: "rtsp"
    strategy: "passthrough"
    network:
      transport: "tcp"
      packet_size_limit: 1200
      stall_timeout: 10s
    outputs:
      - type: "webrtc_rtp"
        codec: "h264"
        payload_type: 102
        mtu: 1200
        ssrc: "stable"
        clock_rate: 90000
"#;

    let track = Arc::new(TrackLocalStaticRTP::new(
        RTCRtpCodecCapability {
            mime_type: "video/h264".to_string(),
            ..Default::default()
        },
        "video".to_string(),
        "stream_id".to_string(),
    ));

    println!("Initializing CamlPipelineBuilder...");
    let builder = CamlPipeline::from_manifest_str(manifest_str)?
        .with_feature_capability_probe()
        .with_webrtc_track("webrtc_pipeline", track)
        .with_native_adapters();

    println!("Starting runtime...");
    let runtime = builder.start().await?;

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    println!("Shutting down runtime...");
    runtime.shutdown().await?;
    println!("Done!");

    Ok(())
}

#[cfg(not(feature = "webrtc"))]
fn main() {
    println!("This example requires the 'webrtc' feature to be enabled.");
}
