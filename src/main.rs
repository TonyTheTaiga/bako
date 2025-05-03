use notify::{Event, RecursiveMode, Result, Watcher, recommended_watcher};
use std::path::Path;
use tokio::sync::mpsc;

fn handle_event(event: Event) {
    println!("GOT = {:?}", event);
}

#[tokio::main]
async fn main() {
    let (tx, mut rx) = mpsc::channel::<Result<Event>>(32);
    let tx_clone = tx.clone();
    tokio::task::spawn_blocking(move || {
        let watcher_init = recommended_watcher(move |res: Result<Event>| {
            let _ = tx_clone.blocking_send(res);
        });

        if let Ok(mut watcher) = watcher_init {
            watcher
                .watch(Path::new("./data"), RecursiveMode::Recursive)
                .unwrap();
            std::thread::park();
        } else {
            eprintln!("Failed to initialize watcher");
        }
    });

    while let Some(result) = rx.recv().await {
        if let Ok(event) = result {
            handle_event(event);
        }
    }
}
