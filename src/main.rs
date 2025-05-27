use blake3::Hasher;
use directories::BaseDirs;
use notify::{RecursiveMode, Watcher, recommended_watcher};
use std::path::{Path, PathBuf};
use tokio::fs;
use tokio::io::{self, AsyncReadExt};
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};
use tracing_subscriber::{EnvFilter, fmt, prelude::*};

mod db;
use db::Database;
mod config;
use config::Config;
mod embeddings;
mod file;
mod logging;

#[derive(Debug)]
struct FileEvent {
    kind: FileEventKind,
    file: file::File,
}

#[derive(Debug)]
enum FileEventKind {
    NewFile,
    Delete,
    Modify,
}

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

async fn queue_create_event(
    paths: Vec<PathBuf>,
    db: &Database,
) -> Result<(), Box<dyn std::error::Error>> {
    info!("File created event received");
    for path in paths {
        if let Ok(meta) = fs::metadata(&path).await {
            if meta.is_file() {
                if let Some(path_str) = path.to_str() {
                    // Queue the create event for later processing
                    db.queue_file_event(path_str, db::FileEventType::Create)?;
                    info!("Queued create event for {}", path_str);
                    return Ok(());
                } else {
                    error!("Invalid UTF-8 path in create event");
                    return Err("Invalid UTF-8 path".into());
                }
            }
        }
    }
    warn!("No valid files found in Create event");
    Err("No valid files found in Create event".into())
}

async fn queue_modify_event(
    paths: Vec<PathBuf>,
    db: &Database,
) -> Result<(), Box<dyn std::error::Error>> {
    info!("File modified event received");
    for path in paths {
        if let Ok(meta) = fs::metadata(&path).await {
            if meta.is_file() {
                if let Some(path_str) = path.to_str() {
                    // Queue the modify event for later processing
                    db.queue_file_event(path_str, db::FileEventType::Modify)?;
                    info!("Queued modify event for {}", path_str);
                    return Ok(());
                } else {
                    error!("Invalid UTF-8 path in modify event");
                    return Err("Invalid UTF-8 path".into());
                }
            }
        }
    }
    warn!("No valid files found in Modify event");
    Err("No valid files found in Modify event".into())
}

async fn queue_remove_event(
    paths: Vec<PathBuf>,
    db: &Database,
) -> Result<(), Box<dyn std::error::Error>> {
    info!("File removed event received");
    for path in paths {
        if let Some(path_str) = path.to_str() {
            // Queue the delete event for later processing
            db.queue_file_event(path_str, db::FileEventType::Delete)?;
            info!("Queued delete event for {}", path_str);
            return Ok(());
        } else {
            error!("Invalid UTF-8 path in remove event");
            return Err("Invalid UTF-8 path in remove event".into());
        }
    }
    warn!("No valid path found in Remove event");
    Err("No valid path found in Remove event".into())
}

async fn handle_event(
    event: notify::Event,
    db: &Database,
) -> Result<(), Box<dyn std::error::Error>> {
    match event.kind {
        notify::EventKind::Create(_) => queue_create_event(event.paths, db).await,
        notify::EventKind::Modify(_) => queue_modify_event(event.paths, db).await,
        notify::EventKind::Remove(_) => queue_remove_event(event.paths, db).await,
        notify::EventKind::Access(_) => {
            debug!("File accessed - ignoring");
            Ok(()) // Just ignore access events
        }
        notify::EventKind::Other => {
            debug!("Other event - ignoring");
            Ok(()) // Just ignore other events
        }
        _ => {
            warn!("Unhandled event: {:?} - ignoring", event.kind);
            Ok(()) // Ignore unhandled events
        }
    }
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

fn setup_file_watcher()
-> Result<mpsc::Receiver<notify::Result<notify::Event>>, Box<dyn std::error::Error>> {
    info!("Initializing file watcher");
    let (filewatcher_tx, filewatcher_rx) = mpsc::channel::<notify::Result<notify::Event>>(32);
    tokio::task::spawn_blocking(move || {
        let watcher_init_result = recommended_watcher(move |res: notify::Result<notify::Event>| {
            if filewatcher_tx.blocking_send(res).is_err() {
                error!("Failed to send event from watcher to channel. Receiver likely dropped.");
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

async fn process_file_events(
    mut eventhandler_rx: mpsc::Receiver<FileEvent>,
    embedder: embeddings::Embedder,
) {
    info!("Event handler task started");
    loop {
        let file_event = match eventhandler_rx.recv().await {
            Some(event) => event,
            None => {
                info!("Event handler channel closed, exiting task.");
                break;
            }
        };

        match file_event.kind {
            FileEventKind::NewFile | FileEventKind::Modify => {
                let event_type = match file_event.kind {
                    FileEventKind::NewFile => "new",
                    FileEventKind::Modify => "modified",
                    _ => unreachable!(),
                };

                info!(
                    "Processing {} file event: {:?}",
                    event_type, file_event.file.path
                );
                match file_event.file.read().await {
                    Ok(file_dat) => {
                        debug!("Read file content: {} characters", file_dat.len());
                        info!("Generating embeddings for text ({} chars)", file_dat.len());
                        debug!("Sending request to OpenAI embeddings API");
                        match embedder.genereate_embeddings(&file_dat).await {
                            Ok(embeddings) => {
                                debug!("Parsing embeddings response");
                                info!(
                                    "Generated embeddings with dimension: {}",
                                    embeddings.data[0].embedding.len()
                                );
                                debug!("Embedding response: {:?}", embeddings);
                            }
                            Err(e) => error!("Failed to generate embeddings: {}", e),
                        }
                    }
                    Err(e) => error!("Failed to read file: {}", e),
                }
            }
            FileEventKind::Delete => info!("File deleted: {}", file_event.file.path),
        }
    }
}

// Process a single queued event
async fn process_queued_event(
    event: db::QueuedEvent,
    db: &Database,
    embedder: &embeddings::Embedder,
) -> Result<(), Box<dyn std::error::Error>> {
    info!(
        "Processing queued event: {} for path: {}",
        event.event_type.to_string(),
        event.path
    );

    match event.event_type {
        db::FileEventType::Create | db::FileEventType::Modify => {
            let path_str = &event.path;
            let event_type_str = match event.event_type {
                db::FileEventType::Create => "new",
                db::FileEventType::Modify => "modified",
                _ => unreachable!(),
            };

            // Check if file still exists
            if !Path::new(path_str).exists() {
                warn!("File no longer exists: {}, skipping processing", path_str);
                db.mark_event_processed(&event.id, &event.path, event.created_at)?;
                return Ok(());
            }

            // Get file type and hash
            let file_type = get_file_type(path_str)?;
            let hash = hash_file(path_str).await?;

            // Update or create file record
            let file = match event.event_type {
                db::FileEventType::Create => {
                    debug!("New file hash: {:?}", hash);
                    let file = db.create_file(path_str, &file_type, &hash)?;
                    info!("Created file record for {}", path_str);
                    file
                }
                db::FileEventType::Modify => {
                    debug!("Modified file hash: {:?}", hash);
                    let file = db.update_file(path_str, &file_type, &hash)?;
                    info!("Updated file record for {}", path_str);
                    file
                }
                _ => unreachable!(),
            };

            // Generate embeddings
            match file.read().await {
                Ok(file_dat) => {
                    debug!(
                        "Read {} file content: {} characters",
                        event_type_str,
                        file_dat.len()
                    );
                    info!("Generating embeddings for text ({} chars)", file_dat.len());
                    debug!("Sending request to OpenAI embeddings API");
                    match embedder.genereate_embeddings(&file_dat).await {
                        Ok(embeddings) => {
                            debug!("Parsing embeddings response");
                            info!(
                                "Generated embeddings with dimension: {}",
                                embeddings.data[0].embedding.len()
                            );
                            debug!("Embedding response: {:?}", embeddings);
                        }
                        Err(e) => error!("Failed to generate embeddings: {}", e),
                    }
                }
                Err(e) => error!("Failed to read file: {}", e),
            }
        }
        db::FileEventType::Delete => {
            let path_str = &event.path;
            // Delete file from database
            match db.delete_file(path_str) {
                Ok(file) => info!("Deleted file record for {}", path_str),
                Err(e) => {
                    error!("Failed to delete file record: {}", e);
                    // Still mark as processed even if there was an error
                }
            }
        }
    }

    // Mark event as processed
    db.mark_event_processed(&event.id, &event.path, event.created_at)?;
    Ok(())
}

// Process the event queue periodically
async fn process_event_queue(
    db: &Database,
    embedder: &embeddings::Embedder,
    batch_size: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    // Get the total queue size
    let queue_size = db.get_queue_size()?;
    info!(
        "Processing event queue (queue size: {}, batch size: {})",
        queue_size, batch_size
    );

    // Get pending events
    let events = db.get_pending_events(batch_size)?;
    let event_count = events.len();

    if event_count == 0 {
        debug!("No pending events to process");
        return Ok(());
    }

    info!("Found {} pending events to process", event_count);

    // Process each event
    for event in events {
        if let Err(e) = process_queued_event(event, db, embedder).await {
            error!("Error processing event: {}", e);
            // Continue processing other events even if one fails
        }
    }

    Ok(())
}

async fn run_main_event_loop(
    mut filewatcher_rx: mpsc::Receiver<notify::Result<notify::Event>>,
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
            Some(result) = filewatcher_rx.recv() => {
                match result {
                    Ok(event) => {
                        debug!("Received file system event: {:?}", event.kind);
                        if let Err(e) = handle_event(event, db).await {
                            error!("Error handling event: {:?}", e);
                        }
                    }
                    Err(error) => error!("Failed to retrieve event: {:?}", error),
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

    let mut filewatcher_rx = setup_file_watcher()?;

    // Configure the queue processing parameters
    let process_interval = std::time::Duration::from_secs(60); // Process queue every 5 seconds
    let batch_size = 10; // Process up to 10 events per batch

    info!(
        "Starting queue-based event processing (interval: {:?}, batch size: {})",
        process_interval, batch_size
    );

    // Run the main event loop with queue processing
    run_main_event_loop(filewatcher_rx, &db, &embedder, process_interval, batch_size).await?;

    info!("Exiting application");

    Ok(())
}
