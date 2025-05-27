use notify::{RecursiveMode, Watcher, recommended_watcher};
use std::path::Path;
use tokio::sync::mpsc;
use tracing::{error, info};

use crate::db;

pub fn setup_file_watcher() -> Result<mpsc::Receiver<db::FileEvent>, Box<dyn std::error::Error>> {
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
