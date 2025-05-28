use crate::db::Database;
use crate::file::File;
use rusqlite::{params, Result, Row};
use uuid::Uuid;

fn row_to_file(row: &Row) -> Result<File> {
    Ok(File {
        id: row.get(0)?,
        path: row.get(1)?,
        file_type: row.get(2)?,
        hash: row.get(3)?,
        size: row.get(4)?,
        created_at: row.get(5)?,
        updated_at: row.get(6)?,
    })
}

pub struct FileRepository<'db> {
    db: &'db Database,
}

impl<'db> FileRepository<'db> {
    pub fn new(db: &'db Database) -> Self {
        Self { db }
    }

    pub fn upsert_file(
        &self,
        path: &str,
        file_type: &str,
        hash: &str,
        size: i64,
    ) -> Result<File> {
        let id = Uuid::new_v4().to_string();
        let file = self.db.conn.query_row(
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

    pub fn get_file(&self, id: &str) -> Result<File> {
        let file =
            self.db.conn
                .query_row("SELECT * FROM files WHERE id = ($1)", [id], row_to_file)?;
        Ok(file)
    }

    pub fn delete_file(&self, path: &str) -> Result<File> {
        let file = self.db.conn.query_row(
            "DELETE FROM files WHERE path = ?1 RETURNING id, path, file_type, hash, size, created_at, updated_at",
            [path],
            row_to_file,
        )?;
        Ok(file)
    }
}
