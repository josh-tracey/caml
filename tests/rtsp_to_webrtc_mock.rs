#[cfg(all(feature = "ffmpeg", feature = "webrtc"))]
mod tests {
    use std::sync::{Arc, Mutex};
    use std::time::Duration;
    use async_trait::async_trait;
    use caml::{
        CamlPipeline, metrics::{MetricsExporter, CopyEvent},
        webrtc::TrackLocalStaticRTP,
    };
    use webrtc::rtp_transceiver::rtp_codec::RTCRtpCodecCapability;

    #[derive(Default, Clone)]
    struct TestMetrics {
        copy_events: Arc<Mutex<Vec<(CopyEvent, usize)>>>,
    }

    #[async_trait]
    impl MetricsExporter for TestMetrics {
        async fn record_restart(&self, _pipeline_id: &str, _reason: &str) {}
        async fn record_backpressure_drop(&self, _pipeline_id: &str) {}
        async fn record_memory_watermark(&self, _pipeline_id: &str, _bytes: usize) {}
        async fn record_throughput(&self, _pipeline_id: &str, _bytes: usize) {}
        async fn record_stream_error(&self, _pipeline_id: &str, _error: &str) {}
        async fn record_copy_event(&self, _pipeline_id: &str, event: CopyEvent, bytes: usize) {
            self.copy_events.lock().unwrap().push((event, bytes));
        }
    }

    #[tokio::test]
    async fn test_rtsp_to_webrtc_mock_pipeline() {
        let manifest_str = r#"
system:
  hardware_target: "GENERIC_LINUX"
  media_memory_limit: "128MB"
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

        let metrics = TestMetrics::default();

        let builder = CamlPipeline::from_manifest_str(manifest_str)
            .expect("should parse manifest")
            .with_feature_capability_probe()
            .with_webrtc_track("webrtc_pipeline", track.clone())
            .with_native_adapters()
            .with_metrics_exporter(Arc::new(metrics.clone()));

        let runtime = builder.start().await.expect("pipeline should start");

        // The mock input produces 5 frames, sleeping 10ms between each.
        // Wait 150ms for execution to finish.
        tokio::time::sleep(Duration::from_millis(150)).await;

        runtime.shutdown().await.expect("shutdown should succeed");

        let events = metrics.copy_events.lock().unwrap().clone();
        assert!(!events.is_empty(), "expected at least some copy events");

        let has_ffmpeg_copy = events.iter().any(|(e, _)| *e == CopyEvent::FfmpegPacketToPooledBuffer);
        let has_webrtc_copy = events.iter().any(|(e, _)| *e == CopyEvent::WebRtcPacketizerCopy);

        assert!(has_ffmpeg_copy, "should have logged CopyEvent::FfmpegPacketToPooledBuffer");
        assert!(has_webrtc_copy, "should have logged CopyEvent::WebRtcPacketizerCopy");
    }
}
