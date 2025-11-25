use anyhow::{Context, Result};
use serde::Deserialize;
use std::path::Path;

#[derive(Debug, Deserialize, Clone)]
pub struct AppConfig {
    pub server: ServerConfig,
    pub database: DatabaseConfig,
    #[serde(default)]
    pub logging: LogConfig,
}

#[derive(Debug, Deserialize, Clone)]
pub struct ServerConfig {
    pub host: String,
    pub port: u16,
    #[serde(default = "default_worker_threads")]
    pub worker_threads: usize,
}

#[derive(Debug, Deserialize, Clone)]
pub struct DatabaseConfig {
    pub host: String,
    pub port: u16,
    pub dbname: String,
    pub user: String,
    pub password: String,
    #[serde(default = "default_pool_size")]
    pub pool_size: usize,
}

#[derive(Debug, Deserialize, Clone)]
pub struct LogConfig {
    #[serde(default = "default_log_dir")]
    pub log_dir: String,
    #[serde(default = "default_log_level")]
    pub log_level: String,
    #[serde(default = "default_max_log_files")]
    pub max_log_files: usize,
}

impl Default for LogConfig {
    fn default() -> Self {
        Self {
            log_dir: default_log_dir(),
            log_level: default_log_level(),
            max_log_files: default_max_log_files(),
        }
    }
}

fn default_log_dir() -> String {
    "./logs".to_string()
}

fn default_log_level() -> String {
    "info".to_string()
}

fn default_max_log_files() -> usize {
    7
}

fn default_pool_size() -> usize {
    16
}

fn default_worker_threads() -> usize {
    4
}

impl AppConfig {
    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self> {
        let config_str = std::fs::read_to_string(path.as_ref())
            .with_context(|| format!("Failed to read config file: {:?}", path.as_ref()))?;

        let config: AppConfig =
            toml::from_str(&config_str).context("Failed to parse config file")?;

        Ok(config)
    }

    pub fn database_url(&self) -> String {
        format!(
            "host={} port={} dbname={} user={} password={}",
            self.database.host,
            self.database.port,
            self.database.dbname,
            self.database.user,
            self.database.password
        )
    }
}
