use std::{collections::HashSet, time::Duration};

use crate::{
    error::CompileError,
    frontend::{
        CamlManifest, HardwareTarget, InputType, NetworkProfile, PipelineNode, ProcessingProfile,
        StreamStrategy, Transport,
    },
};

const DEFAULT_PACKET_BUFFER_SIZE: usize = 2_048;
const DEFAULT_STALL_TIMEOUT_SECS: u64 = 10;

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
pub struct RuntimePolicy {
    pub buffer_size: usize,
    pub watchdog_timeout: Duration,
}

pub struct CamlCompiler;

impl CamlCompiler {
    pub fn compile(manifest: &CamlManifest) -> Result<CompiledGraph, CompileError> {
        let mut seen_ids = HashSet::new();
        let mut pipelines = Vec::with_capacity(manifest.pipelines.len());

        for pipeline in &manifest.pipelines {
            if !seen_ids.insert(pipeline.id.clone()) {
                return Err(CompileError::DuplicatePipelineId(pipeline.id.clone()));
            }

            validate_pipeline(manifest, pipeline)?;
            pipelines.push(compile_pipeline(pipeline));
        }

        let cma_allocation_limit_bytes = manifest.system.cma_allocation_limit.as_bytes();

        Ok(CompiledGraph {
            system: CompiledSystem {
                hardware_target: manifest.system.hardware_target,
                cma_allocation_limit_bytes,
            },
            resource_plan: ResourcePlan {
                cma_allocation_limit_bytes,
                estimated_cma_usage_bytes: None,
            },
            pipelines,
        })
    }
}

fn validate_pipeline(manifest: &CamlManifest, pipeline: &PipelineNode) -> Result<(), CompileError> {
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
