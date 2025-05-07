use std::io;

#[derive(Debug)]
pub struct File {
    pub id: String,
    pub path: String,
    pub file_type: String,
    pub hash: String,
    pub created_at: String,
    pub updated_at: String,
}

impl File {
    pub async fn read(&self) -> io::Result<String> {
        let file_content = tokio::fs::read_to_string(&self.path).await?;
        Ok(file_content)
    }
}
