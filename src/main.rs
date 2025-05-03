use notify::{RecursiveMode, Watcher, recommended_watcher};
use std::path::Path;
use tokio::fs;
use tokio::sync::mpsc;

fn get_file_type(path_str: &str) -> std::result::Result<&'static str, Box<dyn std::error::Error>> {
    match infer::get_from_path(path_str) {
        Ok(Some(kind)) => Ok(kind.mime_type()),
        Ok(None) => Ok("text/plain"), // unreliable
        Err(_) => Err("failed to get file type".into()),
    }
}

async fn handle_event(event: notify::Event) -> std::result::Result<(), Box<dyn std::error::Error>> {
    println!("event: {:?}", event);
    for path in event.paths {
        // Use async metadata to avoid blocking
        if let Ok(meta) = fs::metadata(&path).await {
            if meta.is_file() {
                if let Some(path_str) = path.to_str() {
                    let file_type = get_file_type(path_str)?;
                    println!("detected file type: {:?}", file_type);
                } else {
                    return Err("Failed to handle event".into());
                }
            }
        }
    }
    Ok(())
}

#[tokio::main]
async fn main() {
    let (tx, mut rx) = mpsc::channel::<notify::Result<notify::Event>>(32);
    let tx_clone = tx.clone();
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

    while let Some(result) = rx.recv().await {
        match result {
            Ok(event) => {
                let _ = handle_event(event).await;
            }
            Err(error) => println!("Failed to initialize watcher! {:?}", error),
        }
    }
}
