use anyhow::{Context, Result};
use serde::Deserialize;
use std::fs;

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    #[serde(default)]
    pub server: ServerConfig,
    #[serde(default)]
    pub cache: CacheConfig,
    #[serde(default)]
    pub aof: AofConfig,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct ServerConfig {
    #[serde(default = "default_host")]
    pub host: String,
    #[serde(default = "default_port")]
    pub port: u16,
    #[serde(default = "default_max_connections")]
    pub max_connections: usize,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct CacheConfig {
    #[serde(default = "default_max_memory")]
    pub max_memory: String,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct AofConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default = "default_aof_path")]
    pub path: String,
}

fn default_host() -> String {
    "127.0.0.1".to_string()
}

fn default_port() -> u16 {
    6379
}

fn default_max_connections() -> usize {
    10000
}

fn default_max_memory() -> String {
    "100MB".to_string()
}

fn default_true() -> bool {
    true
}

fn default_aof_path() -> String {
    "./data/aof.log".to_string()
}

impl Config {
    pub fn load(path: &str) -> Result<Self> {
        let content = fs::read_to_string(path)
            .with_context(|| format!("Failed to read config file: {}", path))?;

        let config: Config =
            toml::from_str(&content).with_context(|| "Failed to parse config file")?;

        config.validate()?;

        Ok(config)
    }

    pub fn from_file_or_default(path: &str) -> Self {
        Self::load(path).unwrap_or_else(|_| Self::default())
    }

    pub fn parse_max_memory(&self) -> Result<usize> {
        parse_memory_size(&self.cache.max_memory)
    }

    fn validate(&self) -> Result<()> {
        if self.server.port == 0 {
            anyhow::bail!("Port cannot be 0");
        }
        if self.server.max_connections == 0 {
            anyhow::bail!("max_connections cannot be 0");
        }
        if self.parse_max_memory()? == 0 {
            anyhow::bail!("max_memory cannot be 0");
        }
        Ok(())
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            server: ServerConfig {
                host: default_host(),
                port: default_port(),
                max_connections: default_max_connections(),
            },
            cache: CacheConfig {
                max_memory: default_max_memory(),
            },
            aof: AofConfig {
                enabled: default_true(),
                path: default_aof_path(),
            },
        }
    }
}

fn parse_memory_size(size: &str) -> Result<usize> {
    let size = size.trim();

    let (num, suffix) = size.split_at(size.len() - 2);
    let num: usize = num
        .trim()
        .parse()
        .with_context(|| format!("Invalid memory size number: {}", num))?;

    let multiplier = match suffix.to_uppercase().as_str() {
        "KB" => 1024,
        "MB" => 1024 * 1024,
        "GB" => 1024 * 1024 * 1024,
        _ => anyhow::bail!("Invalid memory size suffix: {}. Use KB, MB, or GB", suffix),
    };

    Ok(num * multiplier)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = Config::default();
        assert_eq!(config.server.host, "127.0.0.1");
        assert_eq!(config.server.port, 6379);
        assert_eq!(config.server.max_connections, 10000);
        assert_eq!(config.cache.max_memory, "100MB");
        assert!(config.aof.enabled);
        assert_eq!(config.aof.path, "./data/aof.log");
    }

    #[test]
    fn test_parse_memory_size() {
        assert_eq!(parse_memory_size("1KB").unwrap(), 1024);
        assert_eq!(parse_memory_size("1MB").unwrap(), 1024 * 1024);
        assert_eq!(parse_memory_size("1GB").unwrap(), 1024 * 1024 * 1024);
        assert_eq!(parse_memory_size("100MB").unwrap(), 100 * 1024 * 1024);
    }

    #[test]
    fn test_parse_memory_size_invalid() {
        assert!(parse_memory_size("invalid").is_err());
        assert!(parse_memory_size("100TB").is_err());
    }

    #[test]
    fn test_load_config_from_string() {
        let content = r#"
            [server]
            host = "0.0.0.0"
            port = 6380
            max_connections = 100

            [cache]
            max_memory = "50MB"

            [aof]
            enabled = false
            path = "/tmp/aof.log"
        "#;

        let config: Config = toml::from_str(content).unwrap();
        assert_eq!(config.server.host, "0.0.0.0");
        assert_eq!(config.server.port, 6380);
        assert_eq!(config.server.max_connections, 100);
        assert_eq!(config.cache.max_memory, "50MB");
        assert!(!config.aof.enabled);
        assert_eq!(config.aof.path, "/tmp/aof.log");
    }
}
