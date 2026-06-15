#[cfg(feature = "pi")]
mod tests {
    use std::sync::Arc;
    use std::time::{Duration, Instant};
    use caml::{CamlManifest, RuntimeBuilder, CapabilityProbe, HardwareTarget};

    fn pi_host_tests_enabled() -> bool {
        std::env::var_os("CAML_PI_HOST_TESTS").is_some()
    }

    #[tokio::test]
    async fn test_libcamera_host_capture() {
        if !pi_host_tests_enabled() {
            eprintln!("skipping libcamera host test; set CAML_PI_HOST_TESTS=1 on Raspberry Pi hardware");
            return;
        }

        let probe = caml_linux_media::linux_capability_probe();
        let capabilities = probe
            .capabilities(HardwareTarget::RaspberryPi4)
            .expect("capability probe should run");

        if capabilities.pi_model.is_none() {
            eprintln!("skipping libcamera host test: not running on Raspberry Pi hardware");
            return;
        }

        let manifest = CamlManifest::from_yaml_str(
            r#"
system:
  hardware_target: "RASPBERRY_PI_4"
  media_memory_limit: "256MB"
pipelines:
  - id: "libcamera_pipeline"
    input: "camera:0"
    type: "device"
    backend: "libcamera"
    strategy: "passthrough"
    outputs:
      - type: "recording"
"#
        )
        .expect("manifest should parse");

        let recording_packets = Arc::new(tokio::sync::Mutex::new(Vec::new()));
        let mut adapters = caml::adapters::BuiltinAdapters::default();
        adapters.recording_packets = Some(recording_packets.clone());

        #[cfg(target_os = "linux")]
        {
            adapters.libcamera_source = Some(Arc::new(
                caml_linux_media::LibcameraSourceFactory::new(Arc::new(caml_linux_media::camera::NativeLibcameraFactory)),
            ));
        }

        let builder = RuntimeBuilder::new()
            .with_manifest(manifest)
            .with_capability_probe(Arc::new(probe))
            .with_runtime_factory(adapters);

        let runtime = builder.start().await.expect("failed to start libcamera runtime");
        let start_time = Instant::now();

        tokio::time::sleep(Duration::from_secs(2)).await;

        let status = runtime.status().await;
        let pipeline_status = status.pipeline("libcamera_pipeline");
        println!("Pipeline status: {:?}", pipeline_status);

        assert_ne!(pipeline_status, Some(caml::runtime::TaskStatus::Failed));

        runtime.shutdown().await.expect("shutdown failed");
        println!("Libcamera host capture test completed in {:?}", start_time.elapsed());
    }
}
