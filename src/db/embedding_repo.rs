// Placeholder for embedding_repo.rs
use crate::db::Database;
use rusqlite::{params, Result};
use uuid::Uuid;

pub struct EmbeddingRepository<'db> {
    db: &'db Database,
}

impl<'db> EmbeddingRepository<'db> {
    pub fn new(db: &'db Database) -> Self {
        Self { db }
    }

    pub fn insert_embedding(&self, file_id: &str, embedding: &str) -> Result<()> {
        let id = Uuid::new_v4().to_string();
        self.db.conn.execute(
            "INSERT INTO embeddings (id, file_id, embedding) VALUES (?1, ?2, ?3)",
            params![&id, file_id, embedding],
        )?;
        Ok(())
    }
}
