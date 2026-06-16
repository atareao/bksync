use std::collections::HashMap;
use std::path::PathBuf;

use serde::Deserialize;

const DEFAULT_CONFIG_TEMPLATE: &str = r#"[profile.default]
endpoint = "https://rustfs.example.com"
region = "us-east-1"
bucket = "my-bucket"
access_key = "YOUR_ACCESS_KEY"
secret_key = "YOUR_SECRET_KEY"
local_dir = "/home/user/s3sync"
concurrency = 10
include = ["**/*"]
exclude = [".DS_Store", "*.tmp", "*.log", ".git/"]
# disable_path_normalization = true  # for S3-compatible stores that don't handle double encoding
"#;

#[derive(Deserialize)]
pub struct Config {
    pub profile: HashMap<String, Profile>,
}

#[derive(Deserialize, Clone)]
pub struct Profile {
    pub endpoint: String,
    pub region: String,
    pub bucket: String,
    pub access_key: String,
    pub secret_key: String,
    pub local_dir: PathBuf,
    #[serde(default = "default_concurrency")]
    pub concurrency: usize,
    #[serde(default = "default_include")]
    pub include: Vec<String>,
    #[serde(default)]
    pub exclude: Vec<String>,
    #[serde(default)]
    pub disable_path_normalization: bool,
}

fn default_concurrency() -> usize {
    10
}

fn default_include() -> Vec<String> {
    vec!["**/*".to_string()]
}

fn expand_path(path: &str) -> anyhow::Result<String> {
    if path.starts_with('~') {
        let home = std::env::var("HOME")
            .map_err(|_| anyhow::anyhow!("HOME environment variable not set"))?;
        Ok(path.replacen('~', &home, 1))
    } else {
        Ok(path.to_string())
    }
}

impl Config {
    pub fn load(path: &str) -> anyhow::Result<Self> {
        let expanded = expand_path(path)?;
        let config_path = std::path::Path::new(&expanded);

        if !config_path.exists() {
            if let Some(parent) = config_path.parent() {
                std::fs::create_dir_all(parent)
                    .map_err(|e| anyhow::anyhow!("Failed to create config directory '{}': {}", parent.display(), e))?;
            }
            std::fs::write(config_path, DEFAULT_CONFIG_TEMPLATE)
                .map_err(|e| anyhow::anyhow!("Failed to write default config to '{}': {}", expanded, e))?;
            anyhow::bail!(
                "Default config created at '{}'. Edit it with your credentials and run again.",
                expanded
            );
        }

        let content = std::fs::read_to_string(&expanded)
            .map_err(|e| anyhow::anyhow!("Failed to read config file '{}': {}", expanded, e))?;
        let config: Config = toml::from_str(&content)
            .map_err(|e| anyhow::anyhow!("Failed to parse config file '{}': {}", expanded, e))?;
        Ok(config)
    }
}