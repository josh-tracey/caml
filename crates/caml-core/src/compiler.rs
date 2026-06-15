use std::{collections::HashSet, sync::Arc, time::Duration};

use crate::{
    error::CompileError,
    frontend::{
        CamlManifest, HardwareTarget, InputBackend, InputType, NetworkProfile, PipelineNode,
        ProcessingProfile, StreamStrategy, Transport,
    },
};

const DEFAULT_PACKET_BUFFER_SIZE: usize = 2_048;
const DEFAULT_STALL_TIMEOUT_SECS: u64 = 10;
const DEFAULT_RECOVERY_BACKOFF_MS: u64 = 250;
const DEFAULT_MAX_RECOVERIES: u32 = 3;

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
    WebRtcPacketization,
    Pi4HardwareEncoder,
    Pi5StatelessDecoder,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HostCapabilities {
    pub ffmpeg_available: bool,
    pub v4l2_available: bool,
    pub libcamera_available: bool,
    pub webrtc_packetization_available: bool,
    pub pi_model: Option<PiModel>,
    pub has_pi4_h264_encoder: bool,
    pub has_pi5_stateless_decoder: bool,
}

impl Default for HostCapabilities {
    fn default() -> Self {
        Self {
            ffmpeg_available: false,
            v4l2_available: false,
            libcamera_available: false,
            webrtc_packetization_available: false,
            pi_model: None,
            has_pi4_h264_encoder: false,
            has_pi5_stateless_decoder: false,
        }
    }
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
            webrtc_packetization_available: self.webrtc_packetization_available
                || other.webrtc_packetization_available,
            pi_model,
            has_pi4_h264_encoder: self.has_pi4_h264_encoder || other.has_pi4_h264_encoder,
            has_pi5_stateless_decoder: self.has_pi5_stateless_decoder
                || other.has_pi5_stateless_decoder,
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

#[derive(Debug, Clone, PartialEq, Eq)]
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
    pub cma_allocation_limit_bytes: u64,
    pub estimated_cma_usage_bytes: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompiledPipeline {
    pub id: String,
    pub input: CompiledInput,
    pub strategy: StreamStrategy,
    pub network: Option<CompiledNetworkProfile>,
    pub processing: Option<CompiledProcessingProfile>,
    pub runtime: RuntimePolicy,
    pub resolved_backend: ResolvedInputBackend,
    pub execution_mode: ExecutionMode,
    pub codec_path: CodecPath,
    pub recovery: RecoveryPolicy,
    pub capability_requirements: Vec<CapabilityRequirement>,
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
pub struct RecoveryPolicy {
    pub max_restarts: u32,
    pub restart_backoff: Duration,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimePolicy {
    pub buffer_size: usize,
    pub watchdog_timeout: Duration,
}

pub struct CamlCompiler;

impl CamlCompiler {
    pub fn validate_and_compile(manifest: &CamlManifest) -> Result<CompiledGraph, CompileError> {
        Self::compile(manifest)
    }

    pub fn compile(manifest: &CamlManifest) -> Result<CompiledGraph, CompileError> {
        Self::compile_internal(manifest, None)
    }

    pub fn compile_with_probe(
        manifest: &CamlManifest,
        capability_probe: &dyn CapabilityProbe,
    ) -> Result<CompiledGraph, CompileError> {
        let capabilities = capability_probe.capabilities(manifest.system.hardware_target)?;
        Self::compile_internal(manifest, Some(&capabilities))
    }

    fn compile_internal(
        manifest: &CamlManifest,
        capabilities: Option<&HostCapabilities>,
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
            pipelines.push(compile_pipeline(pipeline));
        }

        let cma_allocation_limit_bytes = manifest.system.cma_allocation_limit.as_bytes();
        let estimated_cma_usage_bytes = pipelines
            .iter()
            .map(|pipeline| pipeline.runtime.buffer_size as u64)
            .reduce(|acc, current| acc.saturating_add(current));

        Ok(CompiledGraph {
            system: CompiledSystem {
                hardware_target: manifest.system.hardware_target,
                cma_allocation_limit_bytes,
            },
            resource_plan: ResourcePlan {
                cma_allocation_limit_bytes,
                estimated_cma_usage_bytes,
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

fn compile_pipeline(pipeline: &PipelineNode) -> CompiledPipeline {
    let network = pipeline.network.as_ref().map(compile_network_profile);
    let processing = pipeline.processing.as_ref().map(compile_processing_profile);
    let runtime = RuntimePolicy {
        buffer_size: network
            .as_ref()
            .map(|profile| profile.packet_size_limit)
            .unwrap_or(DEFAULT_PACKET_BUFFER_SIZE),
        watchdog_timeout: network
            .as_ref()
            .map(|profile| profile.stall_timeout)
            .unwrap_or(Duration::from_secs(DEFAULT_STALL_TIMEOUT_SECS)),
    };

    CompiledPipeline {
        id: pipeline.id.clone(),
        input: CompiledInput {
            kind: pipeline.input_type,
            source: pipeline.input.clone(),
        },
        strategy: pipeline.strategy,
        network,
        processing,
        runtime,
        resolved_backend: resolve_backend(pipeline),
        execution_mode: resolve_execution_mode(pipeline.strategy),
        codec_path: resolve_codec_path(pipeline),
        recovery: RecoveryPolicy {
            max_restarts: DEFAULT_MAX_RECOVERIES,
            restart_backoff: Duration::from_millis(DEFAULT_RECOVERY_BACKOFF_MS),
        },
        capability_requirements: capability_requirements_for(pipeline),
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

    if matches!(pipeline.strategy, StreamStrategy::Passthrough) {
        requirements.push(CapabilityRequirement::WebRtcPacketization);
    }

    if matches!(resolve_codec_path(pipeline), CodecPath::HardwareTranscode) {
        requirements.push(CapabilityRequirement::Pi4HardwareEncoder);
    }

    if matches!(pipeline.strategy, StreamStrategy::HardwareDecode) {
        requirements.push(CapabilityRequirement::Pi5StatelessDecoder);
    }

    requirements
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
            CapabilityRequirement::WebRtcPacketization
                if !capabilities.webrtc_packetization_available =>
            {
                Err(CompileError::UnsupportedCapability(format!(
                    "pipeline '{}' requires WebRTC packetization support but none was detected",
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
            _ => Ok(()),
        }
    }
}
