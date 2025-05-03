use std::path::Path;

use rusqlite::Connection;

pub mod job_repo;
pub mod file_repo;
pub mod embedding_repo;

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum FileEventType {
    Create,
    Modify,
    Delete,
}

impl std::fmt::Display for FileEventType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            FileEventType::Create => "create",
            FileEventType::Modify => "modify",
            FileEventType::Delete => "delete",
        };
        write!(f, "{}", s)
    }
}

impl FileEventType {
    pub fn from_string(s: &str) -> Option<FileEventType> {
        match s {
            "create" => Some(FileEventType::Create),
            "modify" => Some(FileEventType::Modify),
            "delete" => Some(FileEventType::Delete),
            _ => None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct FileEvent {
    pub path: String,
    pub event_type: FileEventType,
}

impl FileEvent {
    pub fn from_notify_event(event: notify::Event) -> Vec<FileEvent> {
        let mut file_events = Vec::new();

        let event_type = match event.kind {
            notify::EventKind::Create(_) => Some(FileEventType::Create),
            notify::EventKind::Modify(_) => Some(FileEventType::Modify),
            notify::EventKind::Remove(_) => Some(FileEventType::Delete),
            notify::EventKind::Access(_) | notify::EventKind::Other => None,
            _ => None,
        };

        if let Some(event_type) = event_type {
            for path in event.paths {
                if let Some(path_str) = path.to_str() {
                    if path.is_file() || event_type == FileEventType::Delete {
                        file_events.push(FileEvent {
                            path: path_str.to_string(),
                            event_type,
                        });
                    }
                }
            }
        }

        file_events
    }
}

pub struct Database {
    pub(crate) conn: Connection,
}

impl Database {
    pub fn new(path_str: &Path) -> rusqlite::Result<Database> {
        let conn = Connection::open(path_str)?;
        conn.execute_batch("PRAGMA foreign_keys = ON;")?;
        conn.execute_batch(
            r#"
            CREATE TABLE IF NOT EXISTS files (
                id TEXT PRIMARY KEY,
                path TEXT NOT NULL UNIQUE,
                file_type TEXT NOT NULL,
                hash TEXT NOT NULL,
                size INTEGER NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TRIGGER IF NOT EXISTS update_files_updated_at
            AFTER UPDATE ON files
            FOR EACH ROW
            BEGIN
                UPDATE files
                SET updated_at = CURRENT_TIMESTAMP
                WHERE id = OLD.id;
            END;
            
            CREATE TABLE IF NOT EXISTS jobs (
                id TEXT PRIMARY KEY,
                file_id TEXT NOT NULL,
                status TEXT NOT NULL CHECK(status IN ('pending', 'running', 'completed', 'failed')),
                error_message TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (file_id) REFERENCES files(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS embeddings (
                id TEXT PRIMARY KEY,
                file_id TEXT NOT NULL,
                embedding TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (file_id) REFERENCES files(id) ON DELETE CASCADE
            );
            "#,
        )?;

        Ok(Database { conn })
    }

    pub fn jobs(&self) -> job_repo::JobRepository {
        job_repo::JobRepository::new(self)
    }

    pub fn files(&self) -> file_repo::FileRepository {
        file_repo::FileRepository::new(self)
    }

    pub fn embeddings(&self) -> embedding_repo::EmbeddingRepository {
        embedding_repo::EmbeddingRepository::new(self)
    }
}
