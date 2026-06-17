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

#[derive(Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum InputBackend {
    Auto,
    Ffmpeg,
    V4l2,
    Libcamera,
}

#[derive(Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct NetworkProfile {
    pub transport: Transport,
    pub packet_size_limit: usize,
    #[serde(deserialize_with = "deserialize_duration")]
    pub stall_timeout: std::time::Duration,
}

/// Drop policy applied when a sink's queue is full.
#[derive(Deserialize, Debug, Clone, Copy, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum DropPolicy {
    /// Back-pressure the ingestion loop until the sink drains.
    #[default]
    Block,
    /// Discard the oldest buffered frame to make room for the incoming one.
    DropOldest,
    /// Discard the incoming frame; the sink keeps its current queue contents.
    DropNewest,
}

#[derive(Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum OutputProfile {
    WebrtcRtp {
        codec: Option<String>,
        payload_type: Option<u8>,
        mtu: Option<usize>,
        ssrc: Option<String>,
        clock_rate: Option<u32>,
        /// Maximum number of frames buffered in the sink's actor queue.
        /// Defaults to 10 when unset.
        #[serde(default)]
        queue_limit: Option<usize>,
        /// Action taken when `queue_limit` is reached.
        #[serde(default)]
        drop_policy: DropPolicy,
    },
    Recording {
        /// Maximum number of frames buffered in the recorder's actor queue.
        /// Defaults to 100 when unset.
        #[serde(default)]
        queue_limit: Option<usize>,
        /// Action taken when `queue_limit` is reached.
        #[serde(default)]
        drop_policy: DropPolicy,
    },
}

#[derive(Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct CaptureProfile {
    pub width: u32,
    pub height: u32,
    pub pixel_format: String,
    pub frame_rate: u32,
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

#[derive(Deserialize, Debug, Clone, Copy, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum OverlayPosition {
    #[default]
    TopLeft,
    TopRight,
    BottomLeft,
    BottomRight,
}

#[derive(Deserialize, Debug, Clone, Copy, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum TimestampSource {
    #[default]
    WallClock,
}

#[derive(Deserialize, Debug, Clone, Copy, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum OverlayTimezone {
    Local,
    #[default]
    Utc,
}

#[derive(Deserialize, Debug, Clone, PartialEq, Eq, Default)]
#[serde(deny_unknown_fields)]
pub struct TextOverlayStyle {
    #[serde(default)]
    pub font_size: Option<u32>,
    #[serde(default)]
    pub font_color: Option<String>,
    #[serde(default)]
    pub background_color: Option<String>,
    #[serde(default)]
    pub background_alpha: Option<u8>,
    #[serde(default)]
    pub padding: Option<u32>,
    #[serde(default)]
    pub margin: Option<u32>,
}

#[derive(Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum OverlayLayer {
    Timestamp {
        position: OverlayPosition,
        #[serde(default)]
        source: TimestampSource,
        #[serde(default)]
        timezone: OverlayTimezone,
        #[serde(default)]
        format: Option<String>,
        #[serde(flatten)]
        style: TextOverlayStyle,
    },
    Text {
        text: String,
        position: OverlayPosition,
        #[serde(flatten)]
        style: TextOverlayStyle,
    },
    Watermark {
        image_path: String,
        position: OverlayPosition,
        #[serde(default)]
        max_width_px: Option<u32>,
        #[serde(default)]
        opacity: Option<u8>,
        #[serde(default)]
        margin: Option<u32>,
    },
}

#[derive(Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct OverlayProfile {
    pub layers: Vec<OverlayLayer>,
}

#[derive(Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PipelineNode {
    pub id: String,
    pub input: String,
    #[serde(rename = "type")]
    pub input_type: InputType,
    pub strategy: StreamStrategy,
    pub backend: Option<InputBackend>,
    pub network: Option<NetworkProfile>,
    pub capture: Option<CaptureProfile>,
    pub processing: Option<ProcessingProfile>,
    pub overlay: Option<OverlayProfile>,
    #[serde(default)]
    pub outputs: Vec<OutputProfile>,
}

#[derive(Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SystemConfig {
    pub hardware_target: HardwareTarget,
    pub cma_allocation_limit: Option<ByteSize>,
    pub media_memory_limit: Option<ByteSize>,
}

#[derive(Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct CamlManifest {
    pub version: Option<u32>,
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
        if self.system.cma_allocation_limit.is_none() && self.system.media_memory_limit.is_none() {
            return Err(ManifestError::Validation(
                "Either cma_allocation_limit or media_memory_limit must be specified".to_string(),
            ));
        }

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
                    if pipeline.overlay.is_some() {
                        return Err(ManifestError::Validation(format!(
                            "pipeline '{}' defines overlay but overlays require transcode or hardware_decode",
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
                StreamStrategy::HardwareDecode => {
                    if pipeline.processing.is_none() {
                        return Err(ManifestError::Validation(format!(
                            "pipeline '{}' uses hardware_decode and requires a processing profile",
                            pipeline.id
                        )));
                    }
                }
            }

            if let Some(overlay) = pipeline.overlay.as_ref() {
                validate_overlay_profile(&pipeline.id, overlay)?;
            }

            match pipeline.input_type {
                InputType::Rtsp => {
                    if let Some(backend) = pipeline.backend {
                        if !matches!(backend, InputBackend::Auto | InputBackend::Ffmpeg) {
                            return Err(ManifestError::Validation(format!(
                                "pipeline '{}' uses an rtsp input and only supports 'auto' or 'ffmpeg' backends",
                                pipeline.id
                            )));
                        }
                    }
                    validate_rtsp_input(&pipeline.id, &pipeline.input)?;
                }
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

fn validate_overlay_profile(
    pipeline_id: &str,
    overlay: &OverlayProfile,
) -> Result<(), ManifestError> {
    if overlay.layers.is_empty() {
        return Err(ManifestError::Validation(format!(
            "pipeline '{}' defines overlay but provides no layers",
            pipeline_id
        )));
    }

    for layer in &overlay.layers {
        match layer {
            OverlayLayer::Timestamp { format, style, .. } => {
                validate_text_overlay_style(pipeline_id, style)?;
                if format
                    .as_deref()
                    .is_some_and(|value| value.trim().is_empty())
                {
                    return Err(ManifestError::Validation(format!(
                        "pipeline '{}' timestamp overlay format cannot be empty",
                        pipeline_id
                    )));
                }
            }
            OverlayLayer::Text { style, .. } => {
                validate_text_overlay_style(pipeline_id, style)?;
            }
            OverlayLayer::Watermark {
                image_path,
                max_width_px,
                opacity,
                ..
            } => {
                if image_path.trim().is_empty() {
                    return Err(ManifestError::Validation(format!(
                        "pipeline '{}' watermark overlay image_path cannot be empty",
                        pipeline_id
                    )));
                }
                if image_path.contains("://") {
                    return Err(ManifestError::Validation(format!(
                        "pipeline '{}' watermark overlay image_path must reference a local file",
                        pipeline_id
                    )));
                }
                if max_width_px.is_some_and(|value| value == 0) {
                    return Err(ManifestError::Validation(format!(
                        "pipeline '{}' watermark overlay max_width_px must be greater than 0",
                        pipeline_id
                    )));
                }
                if opacity.is_some_and(|value| value > 100) {
                    return Err(ManifestError::Validation(format!(
                        "pipeline '{}' watermark overlay opacity must be between 0 and 100",
                        pipeline_id
                    )));
                }
            }
        }
    }

    Ok(())
}

fn validate_text_overlay_style(
    pipeline_id: &str,
    style: &TextOverlayStyle,
) -> Result<(), ManifestError> {
    if style.font_size.is_some_and(|value| value == 0) {
        return Err(ManifestError::Validation(format!(
            "pipeline '{}' overlay font_size must be greater than 0",
            pipeline_id
        )));
    }
    if style.background_alpha.is_some_and(|value| value > 100) {
        return Err(ManifestError::Validation(format!(
            "pipeline '{}' overlay background_alpha must be between 0 and 100",
            pipeline_id
        )));
    }

    Ok(())
}
