#[cfg(all(feature = "ffmpeg", feature = "webrtc"))]
mod tests {
    use async_trait::async_trait;
    use caml::{
        adapters::BuiltinAdapters,
        ffmpeg::FfmpegSourceFactory,
        metrics::{CopyEvent, MetricsExporter},
        webrtc::{RtpPacketWriter, TrackLocalStaticRTP, WebRtcSinkFactory},
        CamlManifest, CamlPipeline, RuntimeBuilder, TaskStatus,
    };
    use rtp::packet::Packet;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;
    use tokio::sync::Notify;
    use webrtc::rtp_transceiver::rtp_codec::RTCRtpCodecCapability;

    #[derive(Default, Clone)]
    struct TestMetrics {
        copy_events: Arc<Mutex<Vec<(CopyEvent, usize)>>>,
        backpressure_drops: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl MetricsExporter for TestMetrics {
        async fn record_restart(&self, _pipeline_id: &str, _reason: &str) {}
        async fn record_backpressure_drop(&self, _pipeline_id: &str) {
            self.backpressure_drops.fetch_add(1, Ordering::SeqCst);
        }
        async fn record_memory_watermark(&self, _pipeline_id: &str, _bytes: usize) {}
        async fn record_throughput(&self, _pipeline_id: &str, _bytes: usize) {}
        async fn record_stream_error(&self, _pipeline_id: &str, _error: &str) {}
        async fn record_copy_event(&self, _pipeline_id: &str, event: CopyEvent, bytes: usize) {
            self.copy_events.lock().unwrap().push((event, bytes));
        }
    }

    struct BlockingWriter {
        started: Arc<AtomicUsize>,
        released: Arc<AtomicBool>,
        notify: Arc<Notify>,
    }

    #[async_trait]
    impl RtpPacketWriter for BlockingWriter {
        async fn write_packet(&self, _packet: &Packet) -> Result<(), caml::RuntimeError> {
            self.started.fetch_add(1, Ordering::SeqCst);
            while !self.released.load(Ordering::SeqCst) {
                self.notify.notified().await;
            }
            Ok(())
        }
    }

    fn mock_manifest(drop_policy: &str, queue_limit: usize) -> CamlManifest {
        CamlManifest::from_yaml_str(&format!(
            r#"
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
        queue_limit: {queue_limit}
        drop_policy: "{drop_policy}"
"#
        ))
        .expect("manifest should parse")
    }

    async fn runtime_with_blocking_writer(
        drop_policy: &str,
        queue_limit: usize,
    ) -> (
        caml::runtime::RuntimeHandle,
        TestMetrics,
        Arc<AtomicUsize>,
        Arc<AtomicBool>,
        Arc<Notify>,
    ) {
        let metrics = TestMetrics::default();
        let started = Arc::new(AtomicUsize::new(0));
        let released = Arc::new(AtomicBool::new(false));
        let notify = Arc::new(Notify::new());
        let writer = Arc::new(BlockingWriter {
            started: started.clone(),
            released: released.clone(),
            notify: notify.clone(),
        });

        let mut adapters = BuiltinAdapters::default();
        adapters.ffmpeg_source = Some(Arc::new(FfmpegSourceFactory::new()));
        adapters.webrtc_sinks.insert(
            "webrtc_pipeline".to_string(),
            Arc::new(WebRtcSinkFactory::from_writer(writer)),
        );

        let runtime = RuntimeBuilder::from_manifest(mock_manifest(drop_policy, queue_limit))
            .with_feature_capability_probe()
            .with_runtime_factory(adapters)
            .with_metrics_exporter(Arc::new(metrics.clone()))
            .start()
            .await
            .expect("runtime should start");

        (runtime, metrics, started, released, notify)
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

        let has_ffmpeg_copy = events
            .iter()
            .any(|(e, _)| *e == CopyEvent::FfmpegPacketToPooledBuffer);
        let has_webrtc_copy = events
            .iter()
            .any(|(e, _)| *e == CopyEvent::WebRtcPacketizerCopy);

        assert!(
            has_ffmpeg_copy,
            "should have logged CopyEvent::FfmpegPacketToPooledBuffer"
        );
        assert!(
            has_webrtc_copy,
            "should have logged CopyEvent::WebRtcPacketizerCopy"
        );
    }

    #[tokio::test]
    async fn single_output_drop_newest_drops_under_writer_stall() {
        let (runtime, metrics, started, released, notify) =
            runtime_with_blocking_writer("drop_newest", 1).await;

        while started.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }

        tokio::time::sleep(Duration::from_millis(120)).await;

        assert!(
            metrics.backpressure_drops.load(Ordering::SeqCst) > 0,
            "expected single-output WebRTC sink to drop under sustained backpressure"
        );

        released.store(true, Ordering::SeqCst);
        notify.notify_waiters();
        runtime.shutdown().await.expect("shutdown should succeed");
    }

    #[tokio::test]
    async fn single_output_block_policy_backpressures_runtime() {
        let (runtime, metrics, started, released, notify) =
            runtime_with_blocking_writer("block", 1).await;

        while started.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }

        tokio::time::sleep(Duration::from_millis(120)).await;

        assert_eq!(
            metrics.backpressure_drops.load(Ordering::SeqCst),
            0,
            "block policy should not drop frames"
        );

        let status = runtime.status().await;
        assert_eq!(
            status.pipelines.get("webrtc_pipeline"),
            Some(&TaskStatus::Running),
            "pipeline should still be running while blocked on the writer"
        );

        released.store(true, Ordering::SeqCst);
        notify.notify_waiters();
        runtime.shutdown().await.expect("shutdown should succeed");
    }
}
