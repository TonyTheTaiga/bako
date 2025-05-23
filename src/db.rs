use std::path::Path;

use rusqlite::Connection;
use uuid::Uuid;

#[derive(Debug)]
pub struct File {
    id: String,
    path: String,
    file_type: String,
    hash: String,
    created_at: String,
    updated_at: String,
}

pub struct Database {
    conn: Connection,
}

impl Database {
    pub fn new(path_str: &Path) -> Result<Database, rusqlite::Error> {
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
            "#,
        )?;

        Ok(Database { conn })
    }

    pub fn create_file(
        &self,
        path_str: &str,
        file_type: &str,
        hash: &str,
    ) -> std::result::Result<File, rusqlite::Error> {
        let id = Uuid::new_v4();
        use rusqlite::Row;

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
            |row: &Row| {
                Ok(File {
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

    pub fn delete_file(&self, path: &str) -> std::result::Result<File, rusqlite::Error> {
        let file = self.conn.query_row(
            "DELETE FROM files WHERE path = ?1 RETURNING id, path, file_type, hash, created_at, updated_at",
            [path],
            |row| {
                Ok(File {
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
}
