use std::{collections::{HashMap, HashSet}, sync::Arc, time::Duration};

use crate::{
    error::CompileError,
    frontend::{
        CamlManifest, HardwareTarget, InputBackend, InputType, NetworkProfile, OutputProfile,
        OverlayLayer, OverlayPosition, OverlayProfile, OverlayTimezone, PipelineNode,
        ProcessingProfile, StreamStrategy, TextOverlayStyle, Transport,
    },
};

const DEFAULT_PACKET_BUFFER_SIZE: usize = 2_048;
const DEFAULT_STALL_TIMEOUT_SECS: u64 = 10;
const DEFAULT_OVERLAY_FONT_SIZE_PX: u32 = 18;
const DEFAULT_OVERLAY_FONT_COLOR: &str = "white";
const DEFAULT_OVERLAY_BACKGROUND_COLOR: &str = "black";
const DEFAULT_OVERLAY_BACKGROUND_ALPHA: u8 = 60;
const DEFAULT_OVERLAY_PADDING_PX: u32 = 6;
const DEFAULT_OVERLAY_MARGIN_PX: u32 = 12;
const DEFAULT_TIMESTAMP_FORMAT_UTC: &str = "%Y-%m-%d %H:%M:%S UTC";
const DEFAULT_TIMESTAMP_FORMAT_LOCAL: &str = "%Y-%m-%d %H:%M:%S";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResolvedInputBackend {
    FfmpegRtsp,
    V4l2Device,
    LibcameraDevice,
    AutoDevice,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutionMode {
    EncodedPackets,
    DecodedFrames,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CodecPath {
    Passthrough,
    SoftwareTranscode,
    HardwareTranscode,
    HardwareDecode,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PiModel {
    Pi4,
    Pi5,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CapabilityRequirement {
    Ffmpeg,
    V4l2,
    Libcamera,
    RtpPacketization,
    Pi4HardwareEncoder,
    Pi5StatelessDecoder,
    DrawTextFilter,
    OverlayFilter,
    ScaleFilter,
    ColorChannelMixerFilter,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct HostCapabilities {
    pub ffmpeg_available: bool,
    pub v4l2_available: bool,
    pub libcamera_available: bool,
    pub rtp_packetization_available: bool,
    pub pi_model: Option<PiModel>,
    pub has_pi4_h264_encoder: bool,
    pub has_pi5_stateless_decoder: bool,
    pub has_drawtext_filter: bool,
    pub has_overlay_filter: bool,
    pub has_scale_filter: bool,
    pub has_color_channel_mixer_filter: bool,
}

impl HostCapabilities {
    pub fn merge(&self, other: &Self) -> Result<Self, CompileError> {
        let pi_model = match (self.pi_model, other.pi_model) {
            (Some(left), Some(right)) if left != right => {
                return Err(CompileError::ProbeFailure(format!(
                    "capability probes disagreed on the detected Raspberry Pi model: {:?} vs {:?}",
                    left, right
                )));
            }
            (Some(model), _) | (_, Some(model)) => Some(model),
            (None, None) => None,
        };

        Ok(Self {
            ffmpeg_available: self.ffmpeg_available || other.ffmpeg_available,
            v4l2_available: self.v4l2_available || other.v4l2_available,
            libcamera_available: self.libcamera_available || other.libcamera_available,
            rtp_packetization_available: self.rtp_packetization_available
                || other.rtp_packetization_available,
            pi_model,
            has_pi4_h264_encoder: self.has_pi4_h264_encoder || other.has_pi4_h264_encoder,
            has_pi5_stateless_decoder: self.has_pi5_stateless_decoder
                || other.has_pi5_stateless_decoder,
            has_drawtext_filter: self.has_drawtext_filter || other.has_drawtext_filter,
            has_overlay_filter: self.has_overlay_filter || other.has_overlay_filter,
            has_scale_filter: self.has_scale_filter || other.has_scale_filter,
            has_color_channel_mixer_filter: self.has_color_channel_mixer_filter
                || other.has_color_channel_mixer_filter,
        })
    }
}

pub trait CapabilityProbe: Send + Sync {
    fn capabilities(
        &self,
        hardware_target: HardwareTarget,
    ) -> Result<HostCapabilities, CompileError>;
}

#[derive(Debug, Default)]
pub struct StaticCapabilityProbe {
    capabilities: HostCapabilities,
}

impl StaticCapabilityProbe {
    pub fn new(capabilities: HostCapabilities) -> Self {
        Self { capabilities }
    }
}

impl CapabilityProbe for StaticCapabilityProbe {
    fn capabilities(
        &self,
        _hardware_target: HardwareTarget,
    ) -> Result<HostCapabilities, CompileError> {
        Ok(self.capabilities.clone())
    }
}

#[derive(Default)]
pub struct CompositeCapabilityProbe {
    probes: Vec<Arc<dyn CapabilityProbe>>,
}

impl CompositeCapabilityProbe {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn from_probes(probes: Vec<Arc<dyn CapabilityProbe>>) -> Self {
        Self { probes }
    }

    pub fn push(&mut self, probe: Arc<dyn CapabilityProbe>) {
        self.probes.push(probe);
    }
}

impl CapabilityProbe for CompositeCapabilityProbe {
    fn capabilities(
        &self,
        hardware_target: HardwareTarget,
    ) -> Result<HostCapabilities, CompileError> {
        let mut merged = HostCapabilities::default();
        for probe in &self.probes {
            let capabilities = probe.capabilities(hardware_target)?;
            merged = merged.merge(&capabilities)?;
        }
        Ok(merged)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResourceWarning {
    HighMemoryUsage,
}

impl std::fmt::Display for ResourceWarning {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::HighMemoryUsage => write!(
                f,
                "Estimated total memory usage (buffers + backend) exceeds configured limit"
            ),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BufferPoolPlan {
    pub pipeline_id: String,
    pub buffer_size: usize,
    pub buffer_count: usize,
    pub estimated_bytes: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CompiledGraph {
    pub system: CompiledSystem,
    pub pipelines: Vec<CompiledPipeline>,
    pub resource_plan: ResourcePlan,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompiledSystem {
    pub hardware_target: HardwareTarget,
    pub cma_allocation_limit_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResourcePlan {
    pub configured_limit_bytes: u64,
    pub estimated_pool_bytes: u64,
    pub estimated_backend_bytes: Option<u64>,
    pub buffer_pools: Vec<BufferPoolPlan>,
    pub warnings: Vec<ResourceWarning>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CompiledPipeline {
    pub id: String,
    pub input: CompiledInput,
    pub strategy: StreamStrategy,
    pub network: Option<CompiledNetworkProfile>,
    pub processing: Option<CompiledProcessingProfile>,
    pub overlay: Option<CompiledOverlayProfile>,
    pub runtime: RuntimePolicy,
    pub resolved_backend: ResolvedInputBackend,
    pub execution_mode: ExecutionMode,
    pub codec_path: CodecPath,
    pub recovery: RecoveryPolicy,
    pub capability_requirements: Vec<CapabilityRequirement>,
    pub outputs: Vec<OutputProfile>,
}

impl CompiledPipeline {
    /// Return a minimal sentinel pipeline used only by actor task stubs that
    /// never exercise the runtime policy or buffer pool.
    pub fn sentinel() -> Self {
        Self {
            id: String::from("__sentinel__"),
            input: CompiledInput {
                kind: InputType::Rtsp,
                source: String::new(),
            },
            strategy: StreamStrategy::Passthrough,
            network: None,
            processing: None,
            overlay: None,
            runtime: RuntimePolicy {
                buffer_size: 1,
                watchdog_timeout: Duration::from_secs(0),
                buffer_count: 0,
            },
            resolved_backend: ResolvedInputBackend::FfmpegRtsp,
            execution_mode: ExecutionMode::EncodedPackets,
            codec_path: CodecPath::Passthrough,
            recovery: RecoveryPolicy {
                class: RecoveryClass::Network,
                max_restarts: 0,
                initial_backoff: Duration::from_millis(0),
                max_backoff: Duration::from_millis(0),
                backoff_multiplier: 1.0,
                reset_after: Duration::from_millis(0),
            },
            capability_requirements: Vec::new(),
            outputs: Vec::new(),
        }
    }
}


#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompiledInput {
    pub kind: InputType,
    pub source: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompiledNetworkProfile {
    pub transport: Transport,
    pub packet_size_limit: usize,
    pub stall_timeout: Duration,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompiledProcessingProfile {
    pub codec: String,
    pub encoder: String,
    pub preset: String,
    pub tune: String,
    pub frame_rate: u32,
    pub bitrate_bps: u64,
    pub rotation: Option<i32>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompiledOverlayProfile {
    pub layers: Vec<CompiledOverlayLayer>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CompiledOverlayLayer {
    Timestamp(CompiledTimestampOverlay),
    Text(CompiledTextOverlay),
    Watermark(CompiledWatermarkOverlay),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompiledTextOverlayStyle {
    pub position: OverlayPosition,
    pub font_size: u32,
    pub font_color: String,
    pub background_color: String,
    pub background_alpha: u8,
    pub padding: u32,
    pub margin: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompiledTimestampOverlay {
    pub format: String,
    pub timezone: OverlayTimezone,
    pub style: CompiledTextOverlayStyle,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompiledTextOverlay {
    pub text: String,
    pub style: CompiledTextOverlayStyle,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompiledWatermarkOverlay {
    pub image_path: String,
    pub position: OverlayPosition,
    pub max_width_px: Option<u32>,
    pub opacity: u8,
    pub margin: u32,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RecoveryPolicy {
    pub class: RecoveryClass,
    pub max_restarts: u32,
    pub initial_backoff: Duration,
    pub max_backoff: Duration,
    pub backoff_multiplier: f32,
    pub reset_after: Duration,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecoveryClass {
    Network,
    Device,
    Hardware,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimePolicy {
    pub buffer_size: usize,
    pub watchdog_timeout: Duration,
    pub buffer_count: usize,
}

pub struct CamlCompiler;

impl CamlCompiler {
    pub fn validate_and_compile(manifest: &CamlManifest) -> Result<CompiledGraph, CompileError> {
        let overlay_variables = HashMap::new();
        Self::compile_unchecked_with_overlay_variables(manifest, &overlay_variables)
    }

    pub fn compile(manifest: &CamlManifest) -> Result<CompiledGraph, CompileError> {
        let overlay_variables = HashMap::new();
        Self::compile_with_overlay_variables(manifest, &overlay_variables)
    }

    pub fn compile_with_overlay_variables(
        manifest: &CamlManifest,
        overlay_variables: &HashMap<String, String>,
    ) -> Result<CompiledGraph, CompileError> {
        if manifest.system.hardware_target != HardwareTarget::GenericLinux {
            return Err(CompileError::InvalidConfiguration(
                "hardware-target compilation requires a capability probe. \
                 Use `compile_with_probe` or `compile_unchecked` if you want to skip guardrails."
                    .to_string(),
            ));
        }
        Self::compile_internal(manifest, None, overlay_variables)
    }

    pub fn compile_unchecked(manifest: &CamlManifest) -> Result<CompiledGraph, CompileError> {
        let overlay_variables = HashMap::new();
        Self::compile_unchecked_with_overlay_variables(manifest, &overlay_variables)
    }

    pub fn compile_unchecked_with_overlay_variables(
        manifest: &CamlManifest,
        overlay_variables: &HashMap<String, String>,
    ) -> Result<CompiledGraph, CompileError> {
        Self::compile_internal(manifest, None, overlay_variables)
    }

    pub fn compile_with_probe(
        manifest: &CamlManifest,
        capability_probe: &dyn CapabilityProbe,
    ) -> Result<CompiledGraph, CompileError> {
        let overlay_variables = HashMap::new();
        Self::compile_with_probe_and_overlay_variables(
            manifest,
            capability_probe,
            &overlay_variables,
        )
    }

    pub fn compile_with_probe_and_overlay_variables(
        manifest: &CamlManifest,
        capability_probe: &dyn CapabilityProbe,
        overlay_variables: &HashMap<String, String>,
    ) -> Result<CompiledGraph, CompileError> {
        let capabilities = capability_probe.capabilities(manifest.system.hardware_target)?;
        Self::compile_internal(manifest, Some(&capabilities), overlay_variables)
    }

    fn compile_internal(
        manifest: &CamlManifest,
        capabilities: Option<&HostCapabilities>,
        overlay_variables: &HashMap<String, String>,
    ) -> Result<CompiledGraph, CompileError> {
        if let Some(capabilities) = capabilities {
            validate_host_target(manifest.system.hardware_target, capabilities)?;
        }

        let mut seen_ids = HashSet::new();
        let mut pipelines = Vec::with_capacity(manifest.pipelines.len());

        for pipeline in &manifest.pipelines {
            if !seen_ids.insert(pipeline.id.clone()) {
                return Err(CompileError::DuplicatePipelineId(pipeline.id.clone()));
            }

            validate_pipeline(manifest, pipeline, capabilities)?;
            pipelines.push(compile_pipeline(pipeline, overlay_variables)?);
        }

        let cma_allocation_limit_bytes = match (
            manifest.system.cma_allocation_limit.as_ref(),
            manifest.system.media_memory_limit.as_ref(),
        ) {
            (Some(limit), _) => limit.as_bytes(),
            (None, Some(limit)) => limit.as_bytes(),
            (None, None) => 0,
        };

        let mut buffer_pools = Vec::new();
        let mut estimated_pool_bytes = 0u64;
        for pipeline in &pipelines {
            let pool_bytes = (pipeline.runtime.buffer_size as u64)
                .saturating_mul(pipeline.runtime.buffer_count as u64);
            buffer_pools.push(BufferPoolPlan {
                pipeline_id: pipeline.id.clone(),
                buffer_size: pipeline.runtime.buffer_size,
                buffer_count: pipeline.runtime.buffer_count,
                estimated_bytes: pool_bytes,
            });
            estimated_pool_bytes = estimated_pool_bytes.saturating_add(pool_bytes);
        }

        if estimated_pool_bytes > cma_allocation_limit_bytes {
            return Err(CompileError::ResourceLimitExceeded {
                configured_limit_bytes: cma_allocation_limit_bytes,
                estimated_usage_bytes: estimated_pool_bytes,
            });
        }

        let mut estimated_backend_bytes = 0u64;
        for pipeline in &pipelines {
            let backend_est = match pipeline.resolved_backend {
                ResolvedInputBackend::FfmpegRtsp => {
                    let count = 128u64;
                    let size = pipeline.runtime.buffer_size as u64;
                    count.saturating_mul(size)
                }
                ResolvedInputBackend::AutoDevice | ResolvedInputBackend::V4l2Device => {
                    let input_pool = 128u64 * 1200;
                    let frame_size = 1920 * 1080 * 3 / 2;
                    let frame_pool = 8u64 * frame_size;
                    let output_pool = 32u64 * 256_000;
                    input_pool + frame_pool + output_pool
                }
                ResolvedInputBackend::LibcameraDevice => {
                    let frame_size = 1920 * 1080 * 3 / 2;
                    let frame_pool = 8u64 * frame_size;
                    let request_pool = 8u64 * frame_size;
                    frame_pool + request_pool
                }
            };
            estimated_backend_bytes = estimated_backend_bytes.saturating_add(backend_est);
        }

        let mut warnings = Vec::new();
        if estimated_pool_bytes.saturating_add(estimated_backend_bytes) > cma_allocation_limit_bytes {
            warnings.push(ResourceWarning::HighMemoryUsage);
        }

        Ok(CompiledGraph {
            system: CompiledSystem {
                hardware_target: manifest.system.hardware_target,
                cma_allocation_limit_bytes,
            },
            resource_plan: ResourcePlan {
                configured_limit_bytes: cma_allocation_limit_bytes,
                estimated_pool_bytes,
                estimated_backend_bytes: Some(estimated_backend_bytes),
                buffer_pools,
                warnings,
            },
            pipelines,
        })
    }
}

fn validate_host_target(
    hardware_target: HardwareTarget,
    capabilities: &HostCapabilities,
) -> Result<(), CompileError> {
    match (hardware_target, capabilities.pi_model) {
        (HardwareTarget::RaspberryPi4, Some(PiModel::Pi5)) => Err(
            CompileError::UnsupportedCapability(
                "manifest targets Raspberry Pi 4 but the capability probe detected a Raspberry Pi 5 host"
                    .to_string(),
            ),
        ),
        (HardwareTarget::RaspberryPi5, Some(PiModel::Pi4)) => Err(
            CompileError::UnsupportedCapability(
                "manifest targets Raspberry Pi 5 but the capability probe detected a Raspberry Pi 4 host"
                    .to_string(),
            ),
        ),
        _ => Ok(()),
    }
}

fn validate_pipeline(
    manifest: &CamlManifest,
    pipeline: &PipelineNode,
    capabilities: Option<&HostCapabilities>,
) -> Result<(), CompileError> {
    if matches!(pipeline.strategy, StreamStrategy::Passthrough) && pipeline.processing.is_some() {
        return Err(CompileError::InvalidConfiguration(format!(
            "pipeline '{}' defines processing but is marked as passthrough",
            pipeline.id
        )));
    }

    if matches!(pipeline.strategy, StreamStrategy::Passthrough) && pipeline.network.is_none() {
        return Err(CompileError::InvalidConfiguration(format!(
            "pipeline '{}' uses passthrough and requires a network profile",
            pipeline.id
        )));
    }

    if matches!(pipeline.strategy, StreamStrategy::Passthrough) && pipeline.overlay.is_some() {
        return Err(CompileError::InvalidConfiguration(format!(
            "pipeline '{}' defines overlay but overlays require transcode or hardware_decode",
            pipeline.id
        )));
    }

    if matches!(pipeline.strategy, StreamStrategy::Transcode) && pipeline.processing.is_none() {
        return Err(CompileError::InvalidConfiguration(format!(
            "pipeline '{}' uses transcode and requires a processing profile",
            pipeline.id
        )));
    }

    if matches!(pipeline.strategy, StreamStrategy::HardwareDecode) && pipeline.processing.is_none()
    {
        return Err(CompileError::InvalidConfiguration(format!(
            "pipeline '{}' uses hardware_decode and requires a processing profile",
            pipeline.id
        )));
    }

    if manifest.system.hardware_target == HardwareTarget::RaspberryPi5 {
        if let Some(processing) = pipeline.processing.as_ref() {
            let encoder = processing.encoder.trim().to_ascii_lowercase();
            if encoder == "hardware" || encoder == "v4l2m2m" {
                return Err(CompileError::HardwareMismatch(format!(
                    "pipeline '{}' cannot use '{}' on Raspberry Pi 5; use software encoding instead",
                    pipeline.id, processing.encoder
                )));
            }
        }
    }

    if manifest.system.hardware_target == HardwareTarget::RaspberryPi4 {
        if let Some(processing) = pipeline.processing.as_ref() {
            let encoder = processing.encoder.trim().to_ascii_lowercase();
            if (encoder == "hardware" || encoder == "v4l2m2m")
                && capabilities
                    .map(|caps| !caps.has_pi4_h264_encoder)
                    .unwrap_or(false)
            {
                return Err(CompileError::UnsupportedCapability(format!(
                    "pipeline '{}' requested Raspberry Pi 4 hardware encoding but no Pi 4 H.264 encoder was detected",
                    pipeline.id
                )));
            }
        }
    }

    if matches!(pipeline.strategy, StreamStrategy::HardwareDecode)
        && manifest.system.hardware_target == HardwareTarget::RaspberryPi5
        && capabilities
            .map(|caps| !caps.has_pi5_stateless_decoder)
            .unwrap_or(false)
    {
        return Err(CompileError::UnsupportedCapability(format!(
            "pipeline '{}' requested Raspberry Pi 5 hardware decode but no stateless decoder was detected",
            pipeline.id
        )));
    }

    if let Some(capabilities) = capabilities {
        for requirement in capability_requirements_for(pipeline) {
            requirement.validate_for(&pipeline.id, capabilities)?;
        }
    }

    Ok(())
}

fn compile_pipeline(
    pipeline: &PipelineNode,
    overlay_variables: &HashMap<String, String>,
) -> Result<CompiledPipeline, CompileError> {
    let network = pipeline.network.as_ref().map(compile_network_profile);
    let processing = pipeline.processing.as_ref().map(compile_processing_profile);
    let overlay = pipeline
        .overlay
        .as_ref()
        .map(|profile| compile_overlay_profile(&pipeline.id, profile, overlay_variables))
        .transpose()?;
    let buffer_count = match pipeline.strategy {
        StreamStrategy::Passthrough => 100,
        StreamStrategy::Transcode => 64,
        StreamStrategy::HardwareDecode => 32,
    };
    let runtime = RuntimePolicy {
        buffer_size: network
            .as_ref()
            .map(|profile| profile.packet_size_limit)
            .unwrap_or(DEFAULT_PACKET_BUFFER_SIZE),
        watchdog_timeout: network
            .as_ref()
            .map(|profile| profile.stall_timeout)
            .unwrap_or(Duration::from_secs(DEFAULT_STALL_TIMEOUT_SECS)),
        buffer_count,
    };

    Ok(CompiledPipeline {
        id: pipeline.id.clone(),
        input: CompiledInput {
            kind: pipeline.input_type,
            source: pipeline.input.clone(),
        },
        strategy: pipeline.strategy,
        network,
        processing,
        overlay,
        runtime,
        resolved_backend: resolve_backend(pipeline),
        execution_mode: resolve_execution_mode(pipeline.strategy),
        codec_path: resolve_codec_path(pipeline),
        recovery: recovery_policy_for(pipeline),
        capability_requirements: capability_requirements_for(pipeline),
        outputs: pipeline.outputs.clone(),
    })
}

fn recovery_policy_for(pipeline: &PipelineNode) -> RecoveryPolicy {
    let class = match (
        pipeline.input_type,
        pipeline.strategy,
        pipeline.backend.unwrap_or(InputBackend::Auto),
    ) {
        (_, StreamStrategy::HardwareDecode, _) => RecoveryClass::Hardware,
        (InputType::Rtsp, _, _) => RecoveryClass::Network,
        (
            InputType::Device,
            _,
            InputBackend::V4l2
            | InputBackend::Libcamera
            | InputBackend::Auto
            | InputBackend::Ffmpeg,
        ) => RecoveryClass::Device,
    };

    let (initial_backoff, max_backoff, backoff_multiplier, reset_after, max_restarts) = match class {
        RecoveryClass::Network => (
            Duration::from_millis(250),
            Duration::from_secs(5),
            2.0,
            Duration::from_secs(30),
            5,
        ),
        RecoveryClass::Device => (
            Duration::from_secs(1),
            Duration::from_secs(30),
            2.0,
            Duration::from_secs(60),
            3,
        ),
        RecoveryClass::Hardware => (
            Duration::from_secs(2),
            Duration::from_secs(60),
            2.0,
            Duration::from_secs(120),
            3,
        ),
    };

    RecoveryPolicy {
        class,
        max_restarts,
        initial_backoff,
        max_backoff,
        backoff_multiplier,
        reset_after,
    }
}

fn compile_network_profile(profile: &NetworkProfile) -> CompiledNetworkProfile {
    CompiledNetworkProfile {
        transport: profile.transport,
        packet_size_limit: profile.packet_size_limit,
        stall_timeout: profile.stall_timeout,
    }
}

fn compile_processing_profile(profile: &ProcessingProfile) -> CompiledProcessingProfile {
    CompiledProcessingProfile {
        codec: profile.codec.clone(),
        encoder: profile.encoder.clone(),
        preset: profile.preset.clone(),
        tune: profile.tune.clone(),
        frame_rate: profile.frame_rate,
        bitrate_bps: profile.bitrate.as_bits_per_second(),
        rotation: profile.rotation,
    }
}

fn compile_overlay_profile(
    pipeline_id: &str,
    profile: &OverlayProfile,
    overlay_variables: &HashMap<String, String>,
) -> Result<CompiledOverlayProfile, CompileError> {
    let pipeline_overlay_variables = overlay_variables_for_pipeline(pipeline_id, overlay_variables);
    let layers = profile
        .layers
        .iter()
        .map(|layer| compile_overlay_layer(pipeline_id, layer, &pipeline_overlay_variables))
        .collect::<Result<Vec<_>, _>>()?;

    Ok(CompiledOverlayProfile { layers })
}

fn compile_overlay_layer(
    pipeline_id: &str,
    layer: &OverlayLayer,
    overlay_variables: &HashMap<String, String>,
) -> Result<CompiledOverlayLayer, CompileError> {
    match layer {
        OverlayLayer::Timestamp {
            position,
            timezone,
            format,
            style,
            ..
        } => Ok(CompiledOverlayLayer::Timestamp(CompiledTimestampOverlay {
            format: format.clone().unwrap_or_else(|| default_timestamp_format(*timezone)),
            timezone: *timezone,
            style: compile_text_overlay_style(*position, style),
        })),
        OverlayLayer::Text {
            text,
            position,
            style,
        } => Ok(CompiledOverlayLayer::Text(CompiledTextOverlay {
            text: expand_overlay_template(pipeline_id, text, overlay_variables)?,
            style: compile_text_overlay_style(*position, style),
        })),
        OverlayLayer::Watermark {
            image_path,
            position,
            max_width_px,
            opacity,
            margin,
        } => Ok(CompiledOverlayLayer::Watermark(CompiledWatermarkOverlay {
            image_path: image_path.clone(),
            position: *position,
            max_width_px: *max_width_px,
            opacity: opacity.unwrap_or(100),
            margin: margin.unwrap_or(DEFAULT_OVERLAY_MARGIN_PX),
        })),
    }
}

fn compile_text_overlay_style(
    position: OverlayPosition,
    style: &TextOverlayStyle,
) -> CompiledTextOverlayStyle {
    CompiledTextOverlayStyle {
        position,
        font_size: style.font_size.unwrap_or(DEFAULT_OVERLAY_FONT_SIZE_PX),
        font_color: style
            .font_color
            .clone()
            .unwrap_or_else(|| DEFAULT_OVERLAY_FONT_COLOR.to_string()),
        background_color: style
            .background_color
            .clone()
            .unwrap_or_else(|| DEFAULT_OVERLAY_BACKGROUND_COLOR.to_string()),
        background_alpha: style
            .background_alpha
            .unwrap_or(DEFAULT_OVERLAY_BACKGROUND_ALPHA),
        padding: style.padding.unwrap_or(DEFAULT_OVERLAY_PADDING_PX),
        margin: style.margin.unwrap_or(DEFAULT_OVERLAY_MARGIN_PX),
    }
}

fn resolve_backend(pipeline: &PipelineNode) -> ResolvedInputBackend {
    match pipeline.input_type {
        InputType::Rtsp => ResolvedInputBackend::FfmpegRtsp,
        InputType::Device => match pipeline.backend.unwrap_or(InputBackend::Auto) {
            InputBackend::Auto | InputBackend::Ffmpeg => ResolvedInputBackend::AutoDevice,
            InputBackend::V4l2 => ResolvedInputBackend::V4l2Device,
            InputBackend::Libcamera => ResolvedInputBackend::LibcameraDevice,
        },
    }
}

fn resolve_execution_mode(strategy: StreamStrategy) -> ExecutionMode {
    match strategy {
        StreamStrategy::Passthrough => ExecutionMode::EncodedPackets,
        StreamStrategy::Transcode | StreamStrategy::HardwareDecode => ExecutionMode::DecodedFrames,
    }
}

fn resolve_codec_path(pipeline: &PipelineNode) -> CodecPath {
    match pipeline.strategy {
        StreamStrategy::Passthrough => CodecPath::Passthrough,
        StreamStrategy::HardwareDecode => CodecPath::HardwareDecode,
        StreamStrategy::Transcode => {
            let encoder = pipeline
                .processing
                .as_ref()
                .map(|processing| processing.encoder.trim().to_ascii_lowercase())
                .unwrap_or_else(|| "software".to_string());

            if encoder == "hardware" || encoder == "v4l2m2m" {
                CodecPath::HardwareTranscode
            } else {
                CodecPath::SoftwareTranscode
            }
        }
    }
}

fn capability_requirements_for(pipeline: &PipelineNode) -> Vec<CapabilityRequirement> {
    let mut requirements = Vec::new();

    match pipeline.input_type {
        InputType::Rtsp => requirements.push(CapabilityRequirement::Ffmpeg),
        InputType::Device => match pipeline.backend.unwrap_or(InputBackend::Auto) {
            InputBackend::Auto => {}
            InputBackend::Ffmpeg => requirements.push(CapabilityRequirement::Ffmpeg),
            InputBackend::V4l2 => requirements.push(CapabilityRequirement::V4l2),
            InputBackend::Libcamera => requirements.push(CapabilityRequirement::Libcamera),
        },
    }

    for output in &pipeline.outputs {
        if matches!(output, OutputProfile::WebrtcRtp { .. }) {
            requirements.push(CapabilityRequirement::RtpPacketization);
        }
    }

    if matches!(resolve_codec_path(pipeline), CodecPath::HardwareTranscode) {
        requirements.push(CapabilityRequirement::Pi4HardwareEncoder);
    }

    if matches!(pipeline.strategy, StreamStrategy::HardwareDecode) {
        requirements.push(CapabilityRequirement::Pi5StatelessDecoder);
    }

    if overlay_uses_text(pipeline.overlay.as_ref()) {
        requirements.push(CapabilityRequirement::DrawTextFilter);
    }

    if overlay_uses_watermark(pipeline.overlay.as_ref()) {
        requirements.push(CapabilityRequirement::OverlayFilter);
        requirements.push(CapabilityRequirement::ScaleFilter);
        requirements.push(CapabilityRequirement::ColorChannelMixerFilter);
    }

    requirements
}

fn overlay_uses_text(overlay: Option<&OverlayProfile>) -> bool {
    overlay.is_some_and(|profile| {
        profile.layers.iter().any(|layer| {
            matches!(layer, OverlayLayer::Timestamp { .. } | OverlayLayer::Text { .. })
        })
    })
}

fn overlay_uses_watermark(overlay: Option<&OverlayProfile>) -> bool {
    overlay.is_some_and(|profile| {
        profile
            .layers
            .iter()
            .any(|layer| matches!(layer, OverlayLayer::Watermark { .. }))
    })
}

fn overlay_variables_for_pipeline(
    pipeline_id: &str,
    overlay_variables: &HashMap<String, String>,
) -> HashMap<String, String> {
    let mut resolved = overlay_variables.clone();
    resolved.insert("camera_id".to_string(), pipeline_id.to_string());
    resolved.insert("pipeline_id".to_string(), pipeline_id.to_string());
    resolved
}

fn expand_overlay_template(
    pipeline_id: &str,
    template: &str,
    overlay_variables: &HashMap<String, String>,
) -> Result<String, CompileError> {
    let mut output = String::with_capacity(template.len());
    let mut chars = template.chars().peekable();

    while let Some(ch) = chars.next() {
        if ch == '{' {
            let mut key = String::new();
            let mut terminated = false;
            for next in chars.by_ref() {
                if next == '}' {
                    terminated = true;
                    break;
                }
                key.push(next);
            }

            if !terminated {
                return Err(CompileError::InvalidConfiguration(format!(
                    "pipeline '{}' overlay template contains an unterminated variable placeholder",
                    pipeline_id
                )));
            }

            let key = key.trim();
            let value = overlay_variables.get(key).ok_or_else(|| {
                CompileError::InvalidConfiguration(format!(
                    "pipeline '{}' overlay template references unknown variable '{}'",
                    pipeline_id, key
                ))
            })?;
            output.push_str(value);
        } else if ch == '}' {
            return Err(CompileError::InvalidConfiguration(format!(
                "pipeline '{}' overlay template contains an unmatched closing brace",
                pipeline_id
            )));
        } else {
            output.push(ch);
        }
    }

    Ok(output)
}

fn default_timestamp_format(timezone: OverlayTimezone) -> String {
    match timezone {
        OverlayTimezone::Utc => DEFAULT_TIMESTAMP_FORMAT_UTC.to_string(),
        OverlayTimezone::Local => DEFAULT_TIMESTAMP_FORMAT_LOCAL.to_string(),
    }
}

impl CapabilityRequirement {
    fn validate_for(
        &self,
        pipeline_id: &str,
        capabilities: &HostCapabilities,
    ) -> Result<(), CompileError> {
        match self {
            CapabilityRequirement::Ffmpeg if !capabilities.ffmpeg_available => Err(
                CompileError::UnsupportedCapability(format!(
                    "pipeline '{}' requires FFmpeg but no FFmpeg capability probe was available",
                    pipeline_id
                )),
            ),
            CapabilityRequirement::V4l2 if !capabilities.v4l2_available => Err(
                CompileError::UnsupportedCapability(format!(
                    "pipeline '{}' requires a V4L2 backend but no V4L2 capability was detected",
                    pipeline_id
                )),
            ),
            CapabilityRequirement::Libcamera if !capabilities.libcamera_available => Err(
                CompileError::UnsupportedCapability(format!(
                    "pipeline '{}' requires a libcamera backend but no libcamera capability was detected",
                    pipeline_id
                )),
            ),
            CapabilityRequirement::RtpPacketization
                if !capabilities.rtp_packetization_available =>
            {
                Err(CompileError::UnsupportedCapability(format!(
                    "pipeline '{}' requires RTP packetization support but none was detected",
                    pipeline_id
                )))
            }
            CapabilityRequirement::Pi4HardwareEncoder if !capabilities.has_pi4_h264_encoder => Err(
                CompileError::UnsupportedCapability(format!(
                    "pipeline '{}' requires a Raspberry Pi 4 hardware encoder that was not detected",
                    pipeline_id
                )),
            ),
            CapabilityRequirement::Pi5StatelessDecoder
                if !capabilities.has_pi5_stateless_decoder =>
            {
                Err(CompileError::UnsupportedCapability(format!(
                    "pipeline '{}' requires a Raspberry Pi 5 stateless decoder that was not detected",
                    pipeline_id
                )))
            }
            CapabilityRequirement::DrawTextFilter if !capabilities.has_drawtext_filter => Err(
                CompileError::UnsupportedCapability(format!(
                    "pipeline '{}' requires the FFmpeg drawtext filter but it was not detected",
                    pipeline_id
                )),
            ),
            CapabilityRequirement::OverlayFilter if !capabilities.has_overlay_filter => Err(
                CompileError::UnsupportedCapability(format!(
                    "pipeline '{}' requires the FFmpeg overlay filter but it was not detected",
                    pipeline_id
                )),
            ),
            CapabilityRequirement::ScaleFilter if !capabilities.has_scale_filter => Err(
                CompileError::UnsupportedCapability(format!(
                    "pipeline '{}' requires the FFmpeg scale filter but it was not detected",
                    pipeline_id
                )),
            ),
            CapabilityRequirement::ColorChannelMixerFilter
                if !capabilities.has_color_channel_mixer_filter =>
            {
                Err(CompileError::UnsupportedCapability(format!(
                    "pipeline '{}' requires the FFmpeg colorchannelmixer filter but it was not detected",
                    pipeline_id
                )))
            }
            _ => Ok(()),
        }
    }
}
