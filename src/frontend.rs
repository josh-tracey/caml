use std::{io::Read, path::Path};

use serde::Deserialize;
use url::Url;

use crate::{
    error::ManifestError,
    units::{deserialize_duration, Bitrate, ByteSize},
};

#[derive(Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
pub enum HardwareTarget {
    #[serde(rename = "RASPBERRY_PI_4")]
    RaspberryPi4,
    #[serde(rename = "RASPBERRY_PI_5")]
    RaspberryPi5,
    #[serde(rename = "GENERIC_LINUX")]
    GenericLinux,
}

#[derive(Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum InputType {
    Rtsp,
    Device,
}

#[derive(Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum StreamStrategy {
    Passthrough,
    Transcode,
    HardwareDecode,
}

#[derive(Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Transport {
    Tcp,
    Udp,
}

#[derive(Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct NetworkProfile {
    pub transport: Transport,
    pub packet_size_limit: usize,
    #[serde(deserialize_with = "deserialize_duration")]
    pub stall_timeout: std::time::Duration,
}

#[derive(Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ProcessingProfile {
    pub codec: String,
    pub encoder: String,
    pub preset: String,
    pub tune: String,
    pub frame_rate: u32,
    pub bitrate: Bitrate,
    pub rotation: Option<i32>,
}

#[derive(Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PipelineNode {
    pub id: String,
    pub input: String,
    #[serde(rename = "type")]
    pub input_type: InputType,
    pub strategy: StreamStrategy,
    pub network: Option<NetworkProfile>,
    pub processing: Option<ProcessingProfile>,
}

#[derive(Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SystemConfig {
    pub hardware_target: HardwareTarget,
    pub cma_allocation_limit: ByteSize,
}

#[derive(Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct CamlManifest {
    pub system: SystemConfig,
    pub pipelines: Vec<PipelineNode>,
}

impl CamlManifest {
    pub fn from_yaml_str(input: &str) -> Result<Self, ManifestError> {
        let manifest = serde_yaml::from_str::<Self>(input)?;
        manifest.validate()?;
        Ok(manifest)
    }

    pub fn from_reader<R: Read>(mut reader: R) -> Result<Self, ManifestError> {
        let mut raw = String::new();
        reader.read_to_string(&mut raw)?;
        Self::from_yaml_str(&raw)
    }

    pub fn validate(&self) -> Result<(), ManifestError> {
        if self.pipelines.is_empty() {
            return Err(ManifestError::Validation(
                "manifest must define at least one pipeline".to_string(),
            ));
        }

        for pipeline in &self.pipelines {
            match pipeline.strategy {
                StreamStrategy::Passthrough => {
                    if pipeline.network.is_none() {
                        return Err(ManifestError::Validation(format!(
                            "pipeline '{}' uses passthrough and requires a network profile",
                            pipeline.id
                        )));
                    }
                    if pipeline.processing.is_some() {
                        return Err(ManifestError::Validation(format!(
                            "pipeline '{}' defines processing but is marked as passthrough",
                            pipeline.id
                        )));
                    }
                }
                StreamStrategy::Transcode => {
                    if pipeline.processing.is_none() {
                        return Err(ManifestError::Validation(format!(
                            "pipeline '{}' uses transcode and requires a processing profile",
                            pipeline.id
                        )));
                    }
                }
                StreamStrategy::HardwareDecode => {}
            }

            match pipeline.input_type {
                InputType::Rtsp => validate_rtsp_input(&pipeline.id, &pipeline.input)?,
                InputType::Device => validate_device_input(&pipeline.id, &pipeline.input)?,
            }
        }

        Ok(())
    }
}

fn validate_rtsp_input(pipeline_id: &str, input: &str) -> Result<(), ManifestError> {
    let parsed = Url::parse(input).map_err(|error| {
        ManifestError::Validation(format!(
            "pipeline '{}' has an invalid rtsp input '{}': {}",
            pipeline_id, input, error
        ))
    })?;

    if parsed.scheme() != "rtsp" || parsed.host_str().is_none() {
        return Err(ManifestError::Validation(format!(
            "pipeline '{}' must use an rtsp:// URL with a host",
            pipeline_id
        )));
    }

    Ok(())
}

fn validate_device_input(pipeline_id: &str, input: &str) -> Result<(), ManifestError> {
    let path = Path::new(input);
    let looks_like_path = path.is_absolute() || input.starts_with("./") || input.starts_with("../");
    if !looks_like_path || input.contains("://") {
        return Err(ManifestError::Validation(format!(
            "pipeline '{}' must use a filesystem-like path for device input",
            pipeline_id
        )));
    }

    Ok(())
}
