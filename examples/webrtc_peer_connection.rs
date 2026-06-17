//! Example: Shows how to integrate a WebRTC Peer Connection with a `caml` track.
//!
//! Under this architecture, `caml` is responsible for low-level media ingestion,
//! buffering, packetization, and track writing, while the host application is
//! responsible for signaling, ICE negotiation, and managing RTCPeerConnection sessions.

#[cfg(feature = "webrtc")]
use caml::{webrtc::TrackLocalStaticRTP, CamlPipeline};
#[cfg(feature = "webrtc")]
use std::sync::Arc;
#[cfg(feature = "webrtc")]
use webrtc::api::APIBuilder;
#[cfg(feature = "webrtc")]
use webrtc::peer_connection::configuration::RTCConfiguration;
#[cfg(feature = "webrtc")]
use webrtc::rtp_transceiver::rtp_codec::RTCRtpCodecCapability;

#[cfg(feature = "webrtc")]
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Setup the WebRTC API & Peer Connection (App Layer)
    // Usually, the application sets up a MediaEngine, API, and PeerConnection.
    let api = APIBuilder::new().build();
    let config = RTCConfiguration::default();
    let peer_connection = Arc::new(api.new_peer_connection(config).await?);

    // 2. Create the Track Local (shared between app layer and caml)
    let track = Arc::new(TrackLocalStaticRTP::new(
        RTCRtpCodecCapability {
            mime_type: "video/h264".to_string(),
            ..Default::default()
        },
        "video".to_string(),
        "stream_id".to_string(),
    ));

    // 3. Add the track to the PeerConnection (WebRTC sends this track to browser client)
    let rtp_sender = peer_connection
        .add_track(
            Arc::clone(&track) as Arc<dyn webrtc::track::track_local::TrackLocal + Send + Sync>
        )
        .await?;

    // Spawn a task to read incoming RTCP packets (e.g. PLI, feedback) from the peer
    tokio::spawn(async move {
        let mut rtcp_buf = vec![0u8; 1500];
        while let Ok((_, _)) = rtp_sender.read(&mut rtcp_buf).await {}
    });

    // 4. Configure the Caml Pipeline manifest
    let manifest_str = r#"
system:
  hardware_target: "GENERIC_LINUX"
  media_memory_limit: "128MB"
pipelines:
  - id: "live_webrtc"
    input: "rtsp://mock-host/live"
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
        ssrc: "stable"
        clock_rate: 90000
"#;

    println!("Initializing CamlPipeline...");
    let builder = CamlPipeline::from_manifest_str(manifest_str)?
        .with_feature_capability_probe()
        // Pass the track created above to the builder
        .with_webrtc_track("live_webrtc", track)
        .with_native_adapters();

    println!("Starting caml media ingestion and packetization...");
    let runtime = builder.start().await?;

    println!("Streaming media packets... (running for 1 second)");
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;

    println!("Shutting down...");
    runtime.shutdown().await?;
    peer_connection.close().await?;
    println!("Closed successfully.");

    Ok(())
}

#[cfg(not(feature = "webrtc"))]
fn main() {
    println!("This example requires the 'webrtc' feature to be enabled.");
}
