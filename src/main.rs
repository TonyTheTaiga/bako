use serde_json;
use std::path::Path;
use tokio::fs;
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

mod db;
use db::Database;
use db::job_repo::Job;
mod config;
mod embeddings;
mod file;
mod logging;
mod watcher;
mod utils;

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
            if let Err(e) = process_create_event(&event, db).await {
                error!("Failed to process create event for {}: {}", event.path, e);
                return Err(e);
            }
        }
        db::FileEventType::Modify => {
            if let Err(e) = process_modify_event(&event, db).await {
                error!("Failed to process modify event for {}: {}", event.path, e);
                return Err(e);
            }
        }
        db::FileEventType::Delete => {
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
    let file_type = utils::get_file_type(&event.path)?;
    let hash = utils::hash_file(&event.path).await?;
    let size = std::fs::metadata(&event.path)?.len() as i64;

    match db.files().upsert_file(&event.path, &file_type, &hash, size) {
        Ok(file) => {
            info!(
                "Successfully inserted file: {} (ID: {})",
                file.path, file.id
            );
            db.jobs().insert_job(&file.id)?;
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
    db.files().delete_file(&event.path)?;
    Ok(())
}

async fn process_modify_event(
    event: &db::FileEvent,
    db: &Database,
) -> Result<(), Box<dyn std::error::Error>> {
    let file_type = utils::get_file_type(&event.path)?;
    let hash = utils::hash_file(&event.path).await?;
    let size = std::fs::metadata(&event.path)?.len() as i64;
    let file = db.files().upsert_file(&event.path, &file_type, &hash, size)?;
    if db.jobs().get_jobs_by_file_id(&file.id, "pending").map_or(true, |jobs| jobs.is_empty()) {
        db.jobs().insert_job(&file.id)?;
    }
    Ok(())
}

async fn init_app() -> Result<(Database, embeddings::Embedder, config::Config), Box<dyn std::error::Error>> {
    logging::init()?;
    let config = config::Config::load_or_init().await?;
    info!("Configuration loaded: {:?}", config);

    let db_path = Path::new(&config.db_path);
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
    Ok((db, embedder, config))
}

async fn process_event_queue(
    db: &Database,
    embedder: &embeddings::Embedder,
    batch_size: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    let queue_size = db.jobs().get_queue_size()?;
    info!(
        "Processing event queue (queue size: {}, batch size: {})",
        queue_size, batch_size
    );

    let jobs = db.jobs().get_jobs("pending")?;
    let mut completed_jobs: Vec<Job> = vec![];
    for job in jobs {
        info!("Processing job: {}", job.id);
        let file = db.files().get_file(&job.file_id)?;
        let content = fs::read_to_string(&file.path).await?;
        let embeddings_response = embedder.genereate_embeddings(&content).await?;
        let embedding_vector = &embeddings_response.data[0].embedding;
        let embedding_json = serde_json::to_string(embedding_vector)
            .map_err(|e| format!("Failed to serialize embedding to JSON: {}", e))?;
        db.embeddings().insert_embedding(&job.file_id, &embedding_json)?;
        completed_jobs.push(job);
    }

    if !completed_jobs.is_empty() {
        db.jobs().update_job_batch(
            completed_jobs.iter().map(|job| job.id.clone()).collect(),
            "completed",
            None,
        )?;
    }

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
    let (db, embedder, config) = init_app().await?;
    let target_dir = Path::new(&config.watch_directory);
    let fs_event_receiver = watcher::setup_file_watcher(target_dir, config.watcher_poll_duration_secs)?;
    let process_interval = std::time::Duration::from_secs(config.queue_process_interval_secs);
    let batch_size = config.queue_batch_size;

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
