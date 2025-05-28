use std::path::Path;

use crate::file;
use rusqlite::{Connection, params};
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

#[derive(Debug, Clone)]
pub struct FileEvent {
    pub path: String,
    pub event_type: FileEventType,
}

#[derive(Debug, Clone)]
pub struct Job {
    pub id: String,
    pub file_id: String,
    pub status: String,
    pub error_message: Option<String>,
    pub created_at: String,
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
    conn: Connection,
}

fn row_to_file(row: &rusqlite::Row) -> rusqlite::Result<file::File> {
    Ok(file::File {
        id: row.get(0)?,
        path: row.get(1)?,
        file_type: row.get(2)?,
        hash: row.get(3)?,
        size: row.get(4)?,
        created_at: row.get(5)?,
        updated_at: row.get(6)?,
    })
}

fn row_to_job(row: &rusqlite::Row) -> rusqlite::Result<Job> {
    Ok(Job {
        id: row.get(0)?,
        file_id: row.get(1)?,
        status: row.get(2)?,
        error_message: row.get(3)?,
        created_at: row.get(4)?,
    })
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
            "#,
        )?;

        Ok(Database { conn })
    }

    pub fn upsert_file(
        &self,
        path: &str,
        file_type: &str,
        hash: &str,
        size: i64,
    ) -> rusqlite::Result<file::File> {
        let id = Uuid::new_v4().to_string();
        let file = self.conn.query_row(
            r#"
            INSERT INTO files (id, path, file_type, hash, size) 
            VALUES (?1, ?2, ?3, ?4, ?5)
            ON CONFLICT(path) DO UPDATE SET
                file_type = excluded.file_type,
                hash = excluded.hash,
                size = excluded.size,
                updated_at = CURRENT_TIMESTAMP
            RETURNING id, path, file_type, hash, size, created_at, updated_at
            "#,
            params![&id, path, file_type, hash, size],
            row_to_file,
        )?;
        Ok(file)
    }

    pub fn get_file(&self, id: &str) -> rusqlite::Result<file::File> {
        let file =
            self.conn
                .query_row("SELECT * FROM files WHERE id = ($1)", [id], row_to_file)?;
        Ok(file)
    }

    pub fn delete_file(&self, path: &str) -> rusqlite::Result<file::File> {
        let file = self.conn.query_row(
            "DELETE FROM files WHERE path = ?1 RETURNING id, path, file_type, hash, size, created_at, updated_at",
            [path],
            row_to_file,
        )?;
        Ok(file)
    }

    pub fn insert_job(&self, file_id: &str) -> rusqlite::Result<String> {
        let id = Uuid::new_v4().to_string();
        self.conn.execute(
            "INSERT INTO jobs (id, file_id, status, error_message) VALUES (?1, ?2, ?3, ?4)",
            params![&id, file_id, "pending", None::<String>],
        )?;
        Ok(id)
    }

    pub fn get_jobs_by_file_id(&self, file_id: &str, status: &str) -> rusqlite::Result<Vec<Job>> {
        let base_query = "SELECT id, file_id, status, error_message, created_at FROM jobs WHERE file_id = ?1";
        let jobs = if status.eq_ignore_ascii_case("any") {
            let mut stmt = self.conn.prepare(base_query)?;
            stmt.query_map(params![file_id], row_to_job)?
                .collect::<Result<Vec<_>, _>>()?
        } else {
            let full_query = format!("{} AND status = ?2", base_query);
            let mut stmt = self.conn.prepare(&full_query)?;
            stmt.query_map(params![file_id, status], row_to_job)?
                .collect::<Result<Vec<_>, _>>()?
        };
        Ok(jobs)
    }

    pub fn get_jobs(&self, status: &str) -> rusqlite::Result<Vec<Job>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, file_id, status, error_message, created_at FROM jobs WHERE status = ?1",
        )?;
        let jobs = stmt
            .query_map([status], row_to_job)?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(jobs)
    }

    pub fn update_job(&self, job_id: &str, status: &str, error_message: Option<&str>) -> rusqlite::Result<()> {
        self.conn.execute(
            "UPDATE jobs SET status = ?1, error_message = ?2 WHERE id = ?3",
            params![status, error_message, job_id],
        )?;
        Ok(())
    }

    pub fn update_job_batch(&mut self, job_ids: Vec<String>, status: &str, error_message: Option<&str>) -> rusqlite::Result<()> {
        let tx = self.conn.transaction()?;
        for job_id in job_ids {
            tx.execute(
                "UPDATE jobs SET status = ?1, error_message = ?2 WHERE id = ?3",
                params![status, error_message, job_id],
            )?;
        }
        tx.commit()
    }

    pub fn get_queue_size(&self) -> rusqlite::Result<usize> {
        let count: i64 =
            self.conn
                .query_row("SELECT COUNT(*) FROM jobs WHERE status = 'pending'", [], |row| {
                    row.get(0)
                })?;

        Ok(count as usize)
    }
}
