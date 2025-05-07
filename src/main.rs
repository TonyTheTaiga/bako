use blake3::Hasher;
use directories::BaseDirs;
use notify::{RecursiveMode, Watcher, recommended_watcher};
use std::path::Path;
use tokio::fs;
use tokio::io::{self, AsyncReadExt};
use tokio::sync::mpsc;

mod db;
use db::Database;

mod config;
use config::Config;

#[derive(Debug)]
struct FileEvent {
    kind: String,
    file: db::File,
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
            // println!("File created");

            for path in event.paths {
                if let Ok(meta) = fs::metadata(&path).await {
                    if meta.is_file() {
                        if let Some(path_str) = path.to_str() {
                            let file_type = get_file_type(path_str)?;
                            let hash = hash_file(path_str).await?;
                            println!("\x1b[32mnew hash: {:?}\x1b[0m", hash);
                            let file = db.create_file(path_str, &file_type, &hash)?;
                            return Ok(FileEvent {
                                kind: "NewFile".into(),
                                file,
                            });
                        } else {
                            return Err("Invalid UTF-8 path".into());
                        }
                    }
                }
            }

            Err("No valid files found in Create event".into())
        }

        notify::EventKind::Modify(_) => {
            println!("File modified");
            Err("Modify event not handled yet".into())
        }

        notify::EventKind::Remove(_) => {
            // println!("File removed");
            for path in event.paths {
                if let Some(path_str) = path.to_str() {
                    let file = db.delete_file(path_str)?;
                    return Ok(FileEvent {
                        kind: "Delete".into(),
                        file,
                    });
                }
            }
            Err("Remove event processed, but no FileEvent returned".into())
        }

        notify::EventKind::Access(_) => {
            println!("File accessed");
            Err("Access event not handled".into())
        }

        notify::EventKind::Other => {
            println!("Other event");
            Err("Other event not handled".into())
        }

        _ => {
            println!("Unhandled event: {:?}", event.kind);
            Err("Unhandled event type".into())
        }
    }
}

async fn init_config_dir() -> Result<Config, Box<dyn std::error::Error>> {
    let base_dirs = BaseDirs::new().expect("Couldn't find the base directory");
    let bako_config_dir = base_dirs.config_dir().join("io.tonythetaiga.bako");
    if !bako_config_dir.exists() {
        std::fs::create_dir(bako_config_dir.clone())?;
    }

    if bako_config_dir.join("config.toml").exists() {
        let data = fs::read_to_string(bako_config_dir.join("config.toml"))
            .await
            .map_err(|e| format!("Failed to read config toml! {}", e))?;

        let cfg: Config =
            toml::from_str(&data).map_err(|e| format!("Failed to parse config toml! {}", e))?;

        Ok(cfg)
    } else {
        let cfg = Config::new();
        let data =
            toml::to_string_pretty(&cfg).map_err(|e| format!("Failed to format toml! {}", e))?;
        fs::write(bako_config_dir.join("config.toml"), data).await?;

        Ok(cfg)
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Init / Read Config
    // let cfg = init_config_dir().await?;

    // Initialize database
    let db_path = Path::new("bako.db");
    let db = Database::new(db_path)?;

    // File Watcher
    let (filewatcher_tx, mut filewatcher_rx) = mpsc::channel::<notify::Result<notify::Event>>(32);
    tokio::task::spawn_blocking(move || {
        let watcher_init = recommended_watcher(move |res: notify::Result<notify::Event>| {
            let _ = filewatcher_tx.blocking_send(res);
        });

        if let Ok(mut watcher) = watcher_init {
            watcher
                .watch(
                    Path::new("/Users/taigaishida/workspace/bako/data"),
                    RecursiveMode::Recursive,
                )
                .unwrap();
            std::thread::park();
        } else {
            println!("Failed to initialize watcher");
        }
    });

    // Event Handler(s)
    let (eventhandler_tx, mut eventhandler_rx) = mpsc::channel::<FileEvent>(32);
    tokio::spawn(async move {
        loop {
            let file_event = eventhandler_rx
                .recv()
                .await
                .expect("Empty message received!");

            match file_event.kind.as_str() {
                "NewFile" => {
                    println!("New Event - {:?}", file_event);
                }
                "Delete" => println!("New Event - {:?}", file_event),
                _ => {}
            }
        }
    });

    // Main Loop
    while let Some(result) = filewatcher_rx.recv().await {
        match result {
            Ok(event) => match handle_event(event, &db).await {
                Ok(fe) => {
                    eventhandler_tx.send(fe).await?;
                }
                Err(e) => println!("Error handling event: {:?}", e),
            },
            Err(error) => println!("Failed to initialize watcher! {:?}", error),
        }
    }

    Ok(())
}
