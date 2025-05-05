use blake3::Hasher;
use notify::{RecursiveMode, Watcher, recommended_watcher};
use std::path::Path;
use tokio::fs;
use tokio::io::{self, AsyncReadExt};
use tokio::sync::mpsc;

mod db;
use db::Database;

fn get_file_type(path_str: &str) -> std::result::Result<&'static str, Box<dyn std::error::Error>> {
    match infer::get_from_path(path_str) {
        Ok(Some(kind)) => Ok(kind.mime_type()),
        Ok(None) => Ok("text/plain"),
        Err(_) => Err("failed to get file type".into()),
    }
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
                            let file = db.create_file(path_str, file_type, &hash)?;
                            return Ok(FileEvent {
                                kind: "NewFile".into(),
                                file: file.into(),
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
                        file: file.into(),
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

#[derive(Debug)]
struct FileEvent {
    kind: String,
    file: db::File,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize database
    let db_path = Path::new("bako.db");
    let db = Database::new(db_path)?;

    // File Watcher
    let (fw_tx, mut wf_rx) = mpsc::channel::<notify::Result<notify::Event>>(32);
    let tx_clone = fw_tx.clone();
    tokio::task::spawn_blocking(move || {
        let watcher_init = recommended_watcher(move |res: notify::Result<notify::Event>| {
            let _ = tx_clone.blocking_send(res);
        });

        if let Ok(mut watcher) = watcher_init {
            watcher
                .watch(
                    Path::new("/Users/taiga/Code/data"),
                    RecursiveMode::Recursive,
                )
                .unwrap();
            std::thread::park();
        } else {
            eprintln!("Failed to initialize watcher");
        }
    });

    // Event Handler(s)
    let (eh_tx, mut eh_rx) = mpsc::channel::<FileEvent>(32);
    tokio::spawn(async move {
        while let Some(result) = eh_rx.recv().await {
            println!("New Event - {:?}", result)
        }
    });

    // Main Loop
    while let Some(result) = wf_rx.recv().await {
        match result {
            Ok(event) => match handle_event(event, &db).await {
                Ok(fe) => {
                    eh_tx.send(fe).await?;
                }
                Err(e) => eprintln!("Error handling event: {:?}", e),
            },
            Err(error) => println!("Failed to initialize watcher! {:?}", error),
        }
    }

    Ok(())
}
