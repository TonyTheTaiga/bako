use notify::{Config, PollWatcher, RecursiveMode, Watcher};
use std::{path::Path, time::Duration};
use tokio::sync::mpsc;
use tracing::{error, info};

use crate::db;

pub fn setup_file_watcher(
    watch_path: &Path,
    poll_duration: u64,
) -> Result<mpsc::Receiver<db::FileEvent>, Box<dyn std::error::Error>> {
    info!("Initializing file watcher for path: {:?}", watch_path);
    let (sender, receiver) = mpsc::channel::<db::FileEvent>(32);
    let watch_path = watch_path.to_path_buf();

    tokio::task::spawn_blocking(
        move || -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
            let watcher_config =
                Config::default().with_poll_interval(Duration::from_secs(poll_duration));
            let mut watcher = PollWatcher::new(
                move |res: notify::Result<notify::Event>| match res {
                    Ok(event) => {
                        // from notify event only returns event corresponding to a file
                        let file_events = db::FileEvent::from_notify_event(event);
                        for file_event in file_events {
                            if let Err(e) = sender.blocking_send(file_event) {
                                error!("Failed to send file event: {}", e);
                            }
                        }
                    }
                    Err(e) => error!("File watcher error: {}", e),
                },
                watcher_config,
            )?;
            watcher.watch(&watch_path, RecursiveMode::Recursive)?;
            info!("File watcher started successfully");
            std::thread::park();
            Ok(())
        },
    );

    Ok(receiver)
}
