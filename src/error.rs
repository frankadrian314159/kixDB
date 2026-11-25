//! Error types modeled after kdb+/q signals (`'type`, `'length`, `'rank`, `'domain`, `'nyi`, ...).
use serde::{Deserialize, Serialize};
use std::fmt;

pub type KResult<T> = Result<T, KError>;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct KError {
    /// Short kdb+ signal name without leading quote (`type`, `length`, `rank`, `domain`, ...).
    pub signal: String,
    /// Optional human-readable explanation for diagnostics, CLI, and GUI.
    pub detail: String,
}

impl KError {
    pub fn new(signal: impl Into<String>, detail: impl Into<String>) -> Self {
        let signal = signal.into();
        let detail = detail.into();
        Self { signal, detail }
    }

    pub fn signal(sig: impl Into<String>) -> Self {
        let s = sig.into();
        Self { signal: s, detail: String::new() }
    }

    pub fn ty(detail: impl Into<String>) -> Self {
        Self::new("type", detail)
    }

    pub fn length(detail: impl Into<String>) -> Self {
        Self::new("length", detail)
    }

    pub fn rank(detail: impl Into<String>) -> Self {
        Self::new("rank", detail)
    }

    pub fn domain(detail: impl Into<String>) -> Self {
        Self::new("domain", detail)
    }

    pub fn value(name: impl Into<String>) -> Self {
        Self::new(name.into(), "undefined value")
    }

    pub fn syntax(detail: impl Into<String>) -> Self {
        Self::new("syntax", detail)
    }

    pub fn nyi(detail: impl Into<String>) -> Self {
        Self::new("nyi", detail)
    }

    pub fn io(detail: impl Into<String>) -> Self {
        Self::new("os", detail)
    }

    pub fn ipc(detail: impl Into<String>) -> Self {
        Self::new("ipc", detail)
    }

    pub fn message(&self) -> String {
        if self.detail.is_empty() || self.detail == self.signal {
            format!("'{}", self.signal)
        } else {
            format!("'{}: {}", self.signal, self.detail)
        }
    }
}

impl fmt::Display for KError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message())
    }
}

impl std::error::Error for KError {}

impl From<std::io::Error> for KError {
    fn from(e: std::io::Error) -> Self {
        KError::io(e.to_string())
    }
}
