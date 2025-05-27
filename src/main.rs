use blake3::Hasher;
use directories::BaseDirs;
use notify::{RecursiveMode, Watcher, recommended_watcher};
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
    db.queue_file_event(&event.path, event.event_type)?;
    info!("Queued {} event for {}", event.event_type, event.path);
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

fn setup_file_watcher() -> Result<mpsc::Receiver<db::FileEvent>, Box<dyn std::error::Error>> {
    info!("Initializing file watcher");
    let (filewatcher_tx, filewatcher_rx) = mpsc::channel::<db::FileEvent>(32);
    tokio::task::spawn_blocking(move || {
        let watcher_init_result =
            recommended_watcher(move |res: notify::Result<notify::Event>| match res {
                Ok(event) => {
                    let file_events = db::FileEvent::from_notify_event(event);
                    for file_event in file_events {
                        if filewatcher_tx.blocking_send(file_event).is_err() {
                            error!(
                                "Failed to send file event to channel. Receiver likely dropped."
                            );
                            break;
                        }
                    }
                }
                Err(e) => {
                    error!("File watcher error: {}", e);
                }
            });

        match watcher_init_result {
            Ok(mut watcher) => {
                let watch_path = Path::new("/Users/taigaishida/workspace/bako/data"); // TODO: Make this configurable
                info!("Watching directory: {}", watch_path.display());
                if let Err(e) = watcher.watch(watch_path, RecursiveMode::Recursive) {
                    error!("Failed to watch path {}: {}", watch_path.display(), e);
                } else {
                    std::thread::park();
                }
            }
            Err(e) => {
                error!("Failed to initialize watcher: {}", e);
            }
        }
    });
    Ok(filewatcher_rx)
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

    let events = db.get_pending_events(batch_size)?;
    let event_count = events.len();

    if event_count == 0 {
        debug!("No pending events to process");
        return Ok(());
    }

    info!("Found {} pending events to process", event_count);

    let mut file_operations = Vec::new();
    let mut mark_processed_operations = Vec::new();
    let mut delete_operations = Vec::new();
    let mut files_for_embeddings = Vec::new();

    for event in events {
        match event.event_type {
            db::FileEventType::Create | db::FileEventType::Modify => {
                let path_str = &event.path;

                // Check if file still exists
                if !Path::new(path_str).exists() {
                    warn!("File no longer exists: {}, skipping processing", path_str);
                    mark_processed_operations.push((
                        event.id.clone(),
                        event.path.clone(),
                        event.created_at,
                    ));
                    continue;
                }

                // Get file type and hash
                match get_file_type(path_str) {
                    Ok(file_type) => match hash_file(path_str).await {
                        Ok(hash) => {
                            file_operations.push((event.path.clone(), file_type, hash));
                            files_for_embeddings.push((event.path.clone(), event.event_type));
                            mark_processed_operations.push((
                                event.id,
                                event.path,
                                event.created_at,
                            ));
                        }
                        Err(e) => {
                            error!("Failed to hash file {}: {}", path_str, e);
                            mark_processed_operations.push((
                                event.id,
                                event.path,
                                event.created_at,
                            ));
                        }
                    },
                    Err(e) => {
                        error!("Failed to get file type for {}: {}", path_str, e);
                        mark_processed_operations.push((event.id, event.path, event.created_at));
                    }
                }
            }
            db::FileEventType::Delete => {
                delete_operations.push(event.path.clone());
                mark_processed_operations.push((event.id, event.path, event.created_at));
            }
        }
    }

    // Execute batch operations
    if !file_operations.is_empty() {
        match db.batch_create_or_update_files(file_operations) {
            Ok(files) => {
                info!("Batch created/updated {} files", files.len());

                // Process embeddings for successfully created/updated files
                for (path, event_type) in files_for_embeddings {
                    let event_type_str = match event_type {
                        db::FileEventType::Create => "new",
                        db::FileEventType::Modify => "modified",
                        _ => continue,
                    };

                    // Generate embeddings for this file
                    if let Ok(file_content) = tokio::fs::read_to_string(&path).await {
                        debug!(
                            "Read {} file content: {} characters",
                            event_type_str,
                            file_content.len()
                        );
                        info!(
                            "Generating embeddings for text ({} chars)",
                            file_content.len()
                        );

                        match embedder.genereate_embeddings(&file_content).await {
                            Ok(embeddings) => {
                                info!(
                                    "Generated embeddings with dimension: {}",
                                    embeddings.data[0].embedding.len()
                                );
                                debug!("Embedding response: {:?}", embeddings);
                            }
                            Err(e) => error!("Failed to generate embeddings for {}: {}", path, e),
                        }
                    } else {
                        error!("Failed to read file content for embeddings: {}", path);
                    }
                }
            }
            Err(e) => error!("Batch file operations failed: {}", e),
        }
    }

    for path in delete_operations {
        match db.delete_file(&path) {
            Ok(_) => info!("Deleted file record for {}", path),
            Err(e) => error!("Failed to delete file record for {}: {}", path, e),
        }
    }

    if !mark_processed_operations.is_empty() {
        match db.batch_mark_processed(mark_processed_operations) {
            Ok(updated_count) => info!("Marked {} events as processed", updated_count),
            Err(e) => error!("Failed to mark events as processed: {}", e),
        }
    }

    Ok(())
}

async fn run_main_event_loop(
    mut filewatcher_rx: mpsc::Receiver<db::FileEvent>,
    db: &Database,
    embedder: &embeddings::Embedder,
    process_interval: std::time::Duration,
    batch_size: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    info!("Starting main event loop");

    // Create a periodic timer for processing the event queue
    let mut interval = tokio::time::interval(process_interval);

    loop {
        tokio::select! {
            // Process file system events
            Some(event) = filewatcher_rx.recv() => {
                debug!("Received file system event: {} for {}", event.event_type, event.path);
                if let Err(e) = handle_file_event(event, db).await {
                    error!("Error handling event: {:?}", e);
                }
            }

            // Process the event queue on interval
            _ = interval.tick() => {
                if let Err(e) = process_event_queue(db, embedder, batch_size).await {
                    error!("Error processing event queue: {}", e);
                }
            }

            // Exit if both channels are closed
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
    let filewatcher_rx = setup_file_watcher()?;
    let process_interval = std::time::Duration::from_secs(60);
    let batch_size = 10;

    info!(
        "Starting queue-based event processing (interval: {:?}, batch size: {})",
        process_interval, batch_size
    );

    run_main_event_loop(filewatcher_rx, &db, &embedder, process_interval, batch_size).await?;

    info!("Exiting application");

    Ok(())
}
