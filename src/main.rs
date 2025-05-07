use blake3::Hasher;
use directories::BaseDirs;
use notify::{RecursiveMode, Watcher, recommended_watcher};
use std::path::Path;
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

#[derive(Debug)]
struct FileEvent {
    kind: FileEventKind,
    file: file::File,
}

#[derive(Debug)]
enum FileEventKind {
    NewFile,
    Delete,
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

async fn handle_event(
    event: notify::Event,
    db: &Database,
) -> Result<FileEvent, Box<dyn std::error::Error>> {
    match event.kind {
        notify::EventKind::Create(_) => {
            info!("File created");

            for path in event.paths {
                if let Ok(meta) = fs::metadata(&path).await {
                    if meta.is_file() {
                        if let Some(path_str) = path.to_str() {
                            let file_type = get_file_type(path_str)?;
                            let hash = hash_file(path_str).await?;
                            debug!("New file hash: {:?}", hash);
                            let file = db.create_file(path_str, &file_type, &hash)?;
                            info!("Created file record for {}", path_str);
                            return Ok(FileEvent {
                                kind: FileEventKind::NewFile,
                                file,
                            });
                        } else {
                            error!("Invalid UTF-8 path");
                            return Err("Invalid UTF-8 path".into());
                        }
                    }
                }
            }

            warn!("No valid files found in Create event");
            Err("No valid files found in Create event".into())
        }

        notify::EventKind::Modify(_) => {
            debug!("File modified: {:?}", event);
            for path in event.paths {
                let metadata = fs::metadata(&path).await.map_err(|e| {
                    error!("Could not read metadata: {}", e);
                    format!("Could not read metadata {}", e)
                })?;

                debug!("File metadata: {:?}", metadata);
            }

            debug!("Modify not handled");
            Err("Modify not handled".into())
        }

        notify::EventKind::Remove(_) => {
            info!("File removed");
            for path in event.paths {
                if let Some(path_str) = path.to_str() {
                    let file = db.delete_file(path_str)?;
                    info!("Deleted file record for {}", path_str);
                    return Ok(FileEvent {
                        kind: FileEventKind::Delete,
                        file,
                    });
                }
            }
            warn!("Remove event processed, but no FileEvent returned");
            Err("Remove event processed, but no FileEvent returned".into())
        }

        notify::EventKind::Access(_) => {
            debug!("File accessed");
            Err("Access event not handled".into())
        }

        notify::EventKind::Other => {
            debug!("Other event");
            Err("Other event not handled".into())
        }

        _ => {
            warn!("Unhandled event: {:?}", event.kind);
            Err("Unhandled event type".into())
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

fn init_logging() -> Result<(), Box<dyn std::error::Error>> {
    let fmt_layer = fmt::layer()
        .pretty()
        .with_target(false)
        .with_level(true)
        .with_ansi(true)
        .with_thread_ids(false)
        .with_thread_names(false)
        .with_file(false)
        .with_line_number(false)
        .with_writer(std::io::stdout);

    let subscriber = tracing_subscriber::registry()
        .with(EnvFilter::from_default_env())
        .with(fmt_layer);

    tracing::subscriber::set_global_default(subscriber)?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    init_logging()?;
    let _ = init_config_dir().await?;

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

    info!("Initializing file watcher");
    let (filewatcher_tx, mut filewatcher_rx) = mpsc::channel::<notify::Result<notify::Event>>(32);
    tokio::task::spawn_blocking(move || {
        let watcher_init = recommended_watcher(move |res: notify::Result<notify::Event>| {
            let _ = filewatcher_tx.blocking_send(res);
        });

        if let Ok(mut watcher) = watcher_init {
            let watch_path = Path::new("/Users/taigaishida/workspace/bako/data");
            info!("Watching directory: {}", watch_path.display());
            watcher.watch(watch_path, RecursiveMode::Recursive).unwrap();
            std::thread::park();
        } else {
            error!("Failed to initialize watcher");
        }
    });

    info!("Setting up event handler");
    let (eventhandler_tx, mut eventhandler_rx) = mpsc::channel::<FileEvent>(32);
    tokio::spawn(async move {
        loop {
            let file_event = eventhandler_rx
                .recv()
                .await
                .expect("Empty message received!");

            match file_event.kind {
                FileEventKind::NewFile => {
                    info!("Processing new file event: {:?}", file_event.file.path);
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
    });

    info!("Starting main event loop");
    while let Some(result) = filewatcher_rx.recv().await {
        match result {
            Ok(event) => {
                debug!("Received file system event: {:?}", event.kind);
                match handle_event(event, &db).await {
                    Ok(fe) => match eventhandler_tx.send(fe).await {
                        Ok(_) => debug!("Event sent to handler"),
                        Err(e) => error!("Failed to send event to handler: {}", e),
                    },
                    Err(e) => error!("Error handling event: {:?}", e),
                }
            }
            Err(error) => error!("Failed to retrieve event: {:?}", error),
        }
    }

    info!("Exiting application");

    Ok(())
}
