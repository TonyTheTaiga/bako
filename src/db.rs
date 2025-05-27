use std::path::Path;

use crate::file;
use rusqlite::{Connection, params};
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::{debug, warn};
use uuid::Uuid;

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

pub struct QueuedEvent {
    pub id: String,
    pub path: String,
    pub event_type: FileEventType,
    pub created_at: i64,
    pub processed: bool,
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
                    if path.is_file() {
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
    conn: Connection,
}

impl Database {
    pub fn new(path_str: &Path) -> rusqlite::Result<Database> {
        let conn = Connection::open(path_str)?;
        conn.execute_batch(
            r#"
            CREATE TABLE IF NOT EXISTS files (
                id TEXT PRIMARY KEY,
                path TEXT NOT NULL UNIQUE,
                file_type TEXT NOT NULL,
                hash TEXT NOT NULL,
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
            
            CREATE TABLE IF NOT EXISTS file_events (
                id TEXT PRIMARY KEY,
                path TEXT NOT NULL,
                event_type TEXT NOT NULL,
                created_at INTEGER NOT NULL,
                processed BOOLEAN NOT NULL DEFAULT 0
            );
            
            CREATE INDEX IF NOT EXISTS idx_file_events_processed 
            ON file_events(processed, created_at);
            
            CREATE INDEX IF NOT EXISTS idx_file_events_path 
            ON file_events(path, created_at);
            "#,
        )?;

        Ok(Database { conn })
    }

    pub fn delete_file(&self, path: &str) -> rusqlite::Result<file::File> {
        let file = self.conn.query_row(
            "DELETE FROM files WHERE path = ?1 RETURNING id, path, file_type, hash, created_at, updated_at",
            [path],
            |row| {
                Ok(file::File {
                    id: row.get(0)?,
                    path: row.get(1)?,
                    file_type: row.get(2)?,
                    hash: row.get(3)?,
                    created_at: row.get(4)?,
                    updated_at: row.get(5)?,
                })
            },
        )?;
        Ok(file)
    }

    pub fn queue_file_event(&self, path: &str, event_type: FileEventType) -> rusqlite::Result<()> {
        let id = Uuid::new_v4().to_string();
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;

        self.conn.execute(
            "INSERT INTO file_events (id, path, event_type, created_at, processed) VALUES (?, ?, ?, ?, 0)",
            params![id, path, event_type.to_string(), now],
        )?;

        Ok(())
    }

    pub fn get_pending_events(&self, limit: usize) -> rusqlite::Result<Vec<QueuedEvent>> {
        let mut stmt = self.conn.prepare(
            "
            WITH ranked_events AS (
                SELECT
                    id,
                    path,
                    event_type,
                    created_at,
                    processed,
                    ROW_NUMBER() OVER (PARTITION BY path ORDER BY created_at DESC, id DESC) as rn
                FROM file_events
                WHERE processed = 0
            )
            SELECT id, path, event_type, created_at, processed
            FROM ranked_events
            WHERE rn = 1
            ORDER BY created_at ASC
            LIMIT ?
        ",
        )?;

        let events = stmt
            .query_map([limit as i64], |row| {
                Ok(QueuedEvent {
                    id: row.get(0)?,
                    path: row.get(1)?,
                    event_type: FileEventType::from_string(&row.get::<_, String>(2)?)
                        .unwrap_or(FileEventType::Create),
                    created_at: row.get(3)?,
                    processed: row.get(4)?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;

        Ok(events)
    }

    pub fn get_queue_size(&self) -> rusqlite::Result<usize> {
        let count: i64 = self.conn.query_row(
            "SELECT COUNT(*) FROM file_events WHERE processed = 0",
            [],
            |row| row.get(0),
        )?;

        Ok(count as usize)
    }

    pub fn batch_create_or_update_files(
        &self,
        operations: Vec<(String, String, String)>, // (path, file_type, hash)
    ) -> rusqlite::Result<Vec<file::File>> {
        let mut files = Vec::new();

        let tx = self.conn.unchecked_transaction()?;

        for (path_str, file_type, hash) in operations {
            let id = Uuid::new_v4();
            let file = tx.query_row(
                r#"
                INSERT INTO files (id, path, file_type, hash)
                VALUES (?1, ?2, ?3, ?4)
                ON CONFLICT(path) DO UPDATE SET
                    file_type = excluded.file_type,
                    hash = excluded.hash,
                    updated_at = CURRENT_TIMESTAMP
                RETURNING id, path, file_type, hash, created_at, updated_at;
                "#,
                [&id.to_string(), &path_str, &file_type, &hash],
                |row| {
                    Ok(file::File {
                        id: row.get(0)?,
                        path: row.get(1)?,
                        file_type: row.get(2)?,
                        hash: row.get(3)?,
                        created_at: row.get(4)?,
                        updated_at: row.get(5)?,
                    })
                },
            )?;
            files.push(file);
        }

        tx.commit()?;
        Ok(files)
    }

    pub fn batch_mark_processed(
        &self,
        operations: Vec<(String, String, i64)>, // (event_id, path, created_at)
    ) -> rusqlite::Result<usize> {
        let tx = self.conn.unchecked_transaction()?;
        let mut total_updated = 0;

        for (event_id, path, created_at) in operations {
            debug!(
                "Marking event id {} (path: {}, created_at: {}) and older events for this path as processed.",
                event_id, path, created_at
            );

            let rows_updated = tx.execute(
                "UPDATE file_events
                 SET processed = 1
                 WHERE path = ?1 AND created_at <= ?2 AND processed = 0",
                params![path, created_at],
            )?;

            if rows_updated == 0 {
                warn!(
                    "batch_mark_processed: No rows updated for path '{}' at/before timestamp {}. Event id {} was targeted.",
                    path, created_at, event_id
                );
            }

            total_updated += rows_updated;
        }

        tx.commit()?;
        Ok(total_updated)
    }
}
