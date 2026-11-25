//! Static cluster configuration (`cluster.json`).
use serde::{Deserialize, Serialize};
use std::path::Path;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShardInfo {
    pub id: u32,
    /// `host:port`, or comma-separated replica addresses `"10.0.0.1:50051,10.0.0.2:50051,10.0.0.3:50051"`.
    pub addr: String,
}

impl ShardInfo {
    pub fn endpoints(&self) -> Vec<String> {
        self.addr
            .split(',')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(String::from)
            .collect()
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClusterConfig {
    pub shards: Vec<ShardInfo>,
}

impl ClusterConfig {
    pub fn from_file(path: impl AsRef<Path>) -> Result<Self, String> {
        let text = std::fs::read_to_string(path).map_err(|e| e.to_string())?;
        serde_json::from_str(&text).map_err(|e| e.to_string())
    }
}
