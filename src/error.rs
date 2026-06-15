use std::io;

use thiserror::Error;

#[derive(Debug, Error)]
pub enum ManifestError {
    #[error("failed to read manifest: {0}")]
    Io(#[from] io::Error),
    #[error("failed to parse manifest YAML: {0}")]
    Yaml(#[from] serde_yaml::Error),
    #[error("{0}")]
    Validation(String),
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum CompileError {
    #[error("hardware constraint violation: {0}")]
    HardwareMismatch(String),
    #[error("invalid configuration: {0}")]
    InvalidConfiguration(String),
    #[error("duplicate pipeline id: {0}")]
    DuplicatePipelineId(String),
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum RuntimeError {
    #[error("adapter error: {0}")]
    Adapter(String),
    #[error("source error: {0}")]
    Source(String),
    #[error("sink error: {0}")]
    Sink(String),
    #[error("pipeline error: {0}")]
    Pipeline(String),
    #[error("task join error: {0}")]
    Join(String),
}

impl RuntimeError {
    pub fn adapter(message: impl Into<String>) -> Self {
        Self::Adapter(message.into())
    }

    pub fn source(message: impl Into<String>) -> Self {
        Self::Source(message.into())
    }

    pub fn sink(message: impl Into<String>) -> Self {
        Self::Sink(message.into())
    }

    pub fn pipeline(message: impl Into<String>) -> Self {
        Self::Pipeline(message.into())
    }
}
