use std::path::Path;

use crate::file;
use rusqlite::{Connection, params};
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::{debug, warn};
use uuid::Uuid;

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

    pub fn create_file(
        &self,
        path_str: &str,
        file_type: &str,
        hash: &str,
    ) -> rusqlite::Result<file::File> {
        let id = Uuid::new_v4();
        let file = self.conn.query_row(
            r#"
            INSERT INTO files (id, path, file_type, hash)
            VALUES (?1, ?2, ?3, ?4)
            ON CONFLICT(path) DO UPDATE SET
                file_type = excluded.file_type,
                hash = excluded.hash,
                updated_at = CURRENT_TIMESTAMP
            RETURNING id, path, file_type, hash, created_at, updated_at;
            "#,
            [&id.to_string(), path_str, file_type, hash],
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

    pub fn update_file(
        &self,
        path_str: &str,
        file_type: &str,
        hash: &str,
    ) -> rusqlite::Result<file::File> {
        let rows_updated = self.conn.execute(
            "UPDATE files SET file_type = ?2, hash = ?3, updated_at = CURRENT_TIMESTAMP WHERE path = ?1",
            params![path_str, file_type, hash],
        )?;

        if rows_updated == 0 {
            // File didn't exist for update, so treat as create
            warn!(
                "Path {} not found for update in 'files' table, attempting to create.",
                path_str
            );
            self.create_file(path_str, file_type, hash)
        } else {
            // File was updated, now fetch it to return
            debug!(
                "Path {} updated successfully in 'files' table, fetching.",
                path_str
            );
            self.conn.query_row(
                "SELECT id, path, file_type, hash, created_at, updated_at FROM files WHERE path = ?1",
                [path_str],
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
            )
        }
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

    // Mark an event as processed
    pub fn mark_event_processed(
        &self,
        processed_event_id: &str,
        processed_event_path: &str,
        processed_event_created_at: i64,
    ) -> rusqlite::Result<usize> {
        debug!(
            "Marking event id {} (path: {}, created_at: {}) and older events for this path as processed.",
            processed_event_id, processed_event_path, processed_event_created_at
        );
        let rows_updated = self.conn.execute(
            "UPDATE file_events
             SET processed = 1
             WHERE path = ?1 AND created_at <= ?2 AND processed = 0",
            params![processed_event_path, processed_event_created_at],
        )?;

        if rows_updated == 0 {
            // This might happen if the event was already marked processed by another thread/instance (if scaled),
            // or if there's a logic mismatch. For a single-threaded processor, this implies it might have already been marked.
            // We can log this as a warning, as it's unexpected if get_pending_events just returned it as unprocessed.
            warn!(
                "mark_event_processed: No rows updated for path '{}' at/before timestamp {}. Specific event id {} was targeted. It might have been processed concurrently or already processed.",
                processed_event_path, processed_event_created_at, processed_event_id
            );
            // As a safeguard, ensure the specific event ID is marked if the broader query missed it for some reason.
            // This could happen if `created_at` wasn't precise enough or if there's an unexpected state.
            // However, if the main query is correct, this specific update should ideally not be necessary or only update 0 rows.
            // return self.conn.execute("UPDATE file_events SET processed = 1 WHERE id = ?1 AND processed = 0", params![processed_event_id]);
        }
        Ok(rows_updated)
    }

    // Get the total number of unprocessed events in the queue
    pub fn get_queue_size(&self) -> rusqlite::Result<usize> {
        let count: i64 = self.conn.query_row(
            "SELECT COUNT(*) FROM file_events WHERE processed = 0",
            [],
            |row| row.get(0),
        )?;

        Ok(count as usize)
    }
}
