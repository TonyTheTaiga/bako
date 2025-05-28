use blake3::Hasher;
use directories::BaseDirs;
use std::path::Path;
use tokio::fs;
use tokio::io::{self, AsyncReadExt};
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

mod db;
use db::Database;
mod config;
use config::Config;
mod embeddings;
mod file;
mod logging;
mod watcher;

fn get_file_type(path_str: &str) -> io::Result<String> {
    let kind = infer::get_from_path(path_str)?;
    Ok(kind
        .map(|k| k.mime_type().to_string())
        .unwrap_or_else(|| "text/plain".into()))
}

async fn hash_file(path: &str) -> io::Result<String> {
    let mut file = fs::File::open(path).await?;
    let mut hasher = Hasher::new();
    let mut buf = [0u8; 8192];

    loop {
        let n = file.read(&mut buf).await?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }

    Ok(hasher.finalize().to_hex().to_string())
}

async fn handle_file_event(
    event: db::FileEvent,
    db: &Database,
) -> Result<(), Box<dyn std::error::Error>> {
    info!(
        "File event received: {} for {}",
        event.event_type, event.path
    );

    match event.event_type {
        db::FileEventType::Create => {
            info!("Processing create event for: {}", event.path);
            if let Err(e) = process_create_event(&event, db).await {
                error!("Failed to process create event for {}: {}", event.path, e);
                return Err(e);
            }
        }
        db::FileEventType::Modify => {
            info!("Processing modify event for: {}", event.path);
        }
        db::FileEventType::Delete => {
            info!("Processing delete event for: {}", event.path);
            if let Err(e) = process_delete_event(&event, db).await {
                error!("Failed to process delete event for {}: {}", event.path, e);
                return Err(e);
            }
        }
    }

    Ok(())
}

async fn process_create_event(
    event: &db::FileEvent,
    db: &Database,
) -> Result<(), Box<dyn std::error::Error>> {
    let file_type = get_file_type(&event.path)?;
    let hash = hash_file(&event.path).await?;

    match db.insert_file(&event.path, &file_type, &hash) {
        Ok(file) => {
            info!(
                "Successfully inserted file: {} (ID: {})",
                file.path, file.id
            );

            db.insert_job(&file.id)?;
        }
        Err(e) => {
            error!("Failed to insert file {}: {}", event.path, e);
            return Err(Box::new(e));
        }
    }

    Ok(())
}

async fn process_delete_event(
    event: &db::FileEvent,
    db: &Database,
) -> Result<(), Box<dyn std::error::Error>> {
    let file = db.get_file(&event.path)?;
    db.delete_file(&file.path)?;
    Ok(())
}

async fn init_config_dir() -> Result<Config, Box<dyn std::error::Error>> {
    let base_dirs = BaseDirs::new().expect("Couldn't find the base directory");
    let bako_config_dir = base_dirs.config_dir().join("io.tonythetaiga.bako");

    if !bako_config_dir.exists() {
        info!("Creating config directory: {}", bako_config_dir.display());
        std::fs::create_dir(bako_config_dir.clone())?;
    }

    let config_path = bako_config_dir.join("config.toml");
    if config_path.exists() {
        info!("Reading existing config from {}", config_path.display());
        let data = fs::read_to_string(&config_path).await.map_err(|e| {
            error!("Failed to read config file: {}", e);
            format!("Failed to read config toml! {}", e)
        })?;

        let cfg: Config = toml::from_str(&data).map_err(|e| {
            error!("Failed to parse config file: {}", e);
            format!("Failed to parse config toml! {}", e)
        })?;

        debug!("Config loaded successfully");
        Ok(cfg)
    } else {
        info!("Creating new default config at {}", config_path.display());
        let cfg = Config::new();
        let data = toml::to_string_pretty(&cfg).map_err(|e| {
            error!("Failed to serialize config: {}", e);
            format!("Failed to format toml! {}", e)
        })?;
        fs::write(&config_path, data).await?;

        info!("Default config created successfully");
        Ok(cfg)
    }
}

async fn init_app() -> Result<(Database, embeddings::Embedder), Box<dyn std::error::Error>> {
    logging::init()?;
    let _config = init_config_dir().await?;

    let db_path = Path::new("bako.db");
    info!("Initializing database at {}", db_path.display());
    let db = Database::new(db_path)?;

    info!("Initializing OpenAI embeddings client");
    let embedder = match embeddings::Embedder::new().await {
        Ok(embedder) => {
            info!("Embeddings client initialized successfully");
            embedder
        }
        Err(e) => {
            warn!(
                "Failed to initialize embeddings client: {}. Running without embeddings support.",
                e
            );
            return Err(e);
        }
    };
    Ok((db, embedder))
}

async fn process_event_queue(
    db: &Database,
    embedder: &embeddings::Embedder,
    batch_size: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    let queue_size = db.get_queue_size()?;
    info!(
        "Processing event queue (queue size: {}, batch size: {})",
        queue_size, batch_size
    );
    Ok(())
}

async fn run_main_event_loop(
    mut fs_event_receiver: mpsc::Receiver<db::FileEvent>,
    db: &Database,
    embedder: &embeddings::Embedder,
    process_interval: std::time::Duration,
    batch_size: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    info!("Starting main event loop");
    let mut interval = tokio::time::interval(process_interval);
    loop {
        tokio::select! {
            Some(event) = fs_event_receiver.recv() => {
                debug!("Received file system event: {} for {}", event.event_type, event.path);
                if let Err(e) = handle_file_event(event, db).await {
                    error!("Error handling event: {:?}", e);
                }
            }

            _ = interval.tick() => {
                if let Err(e) = process_event_queue(db, embedder, batch_size).await {
                    error!("Error processing event queue: {}", e);
                }
            }

            else => {
                info!("All channels closed, exiting main loop");
                break;
            }
        }
    }

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let (db, embedder) = init_app().await?;
    let target_dir = Path::new("/Users/taigaishida/workspace/bako/data");
    let fs_event_receiver = watcher::setup_file_watcher(target_dir, 5)?;
    let process_interval = std::time::Duration::from_secs(5);
    let batch_size = 10;

    info!(
        "Starting queue-based event processing (interval: {:?}, batch size: {})",
        process_interval, batch_size
    );

    run_main_event_loop(
        fs_event_receiver,
        &db,
        &embedder,
        process_interval,
        batch_size,
    )
    .await?;

    info!("Exiting application");

    Ok(())
}
