use serde::{Deserialize, Serialize};
use directories::BaseDirs;
use tokio::fs;
use tracing::{debug, error, info};

#[derive(Debug, Deserialize, Serialize)]
pub struct Config {
    pub db_path: String,
    pub watch_directory: String,
    pub watcher_poll_duration_secs: u64,
    pub queue_process_interval_secs: u64,
    pub queue_batch_size: usize,
    // Potentially add OpenAI model and dimensions here if they need to be configurable
    // pub openai_model: String,
    // pub openai_dimensions: usize,
}

impl Config {
    pub async fn load_or_init() -> Result<Self, Box<dyn std::error::Error>> {
        let base_dirs = BaseDirs::new().ok_or("Couldn't find the base directory")?;
        let bako_config_dir = base_dirs.config_dir().join("io.tonythetaiga.bako");

        if !bako_config_dir.exists() {
            info!("Creating config directory: {}", bako_config_dir.display());
            std::fs::create_dir_all(&bako_config_dir)?;
        }

        let config_path: std::path::PathBuf = bako_config_dir.join("config.toml");

        if config_path.exists() {
            info!("Reading existing config from {}", config_path.display());
            let data = fs::read_to_string(&config_path).await.map_err(|e| {
                error!("Failed to read config file {}: {}", config_path.display(), e);
                format!("Failed to read config file: {}", e)
            })?;

            let cfg: Config = toml::from_str(&data).map_err(|e| {
                error!("Failed to parse config file {}: {}", config_path.display(), e);
                format!("Failed to parse config toml: {}", e)
            })?;

            debug!("Config loaded successfully: {:?}", cfg);
            Ok(cfg)
        } else {
            Err("Config file does not exist. Creating default config.".into())
        }
    }
}