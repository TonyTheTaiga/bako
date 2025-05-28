use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct EmbeddingResponse {
    pub object: String,
    pub data: Vec<EmbeddingData>,
    pub model: String,
    pub usage: Usage,
}

#[derive(Debug, Deserialize)]
pub struct EmbeddingData {
    pub object: String,
    pub embedding: Vec<f32>,
    pub index: usize,
}

#[derive(Debug, Deserialize)]
pub struct Usage {
    pub prompt_tokens: usize,
    pub total_tokens: usize,
}

pub struct Embedder {
    client: reqwest::Client,
    model: String,
    dimensions: usize,
}

fn get_openai_api_key() -> std::result::Result<String, Box<dyn std::error::Error>> {
    std::env::var("OPENAI_API_KEY")
        .map_err(|_| "Missing OPENAI_API_KEY environment variable".into())
}

impl Embedder {
    pub async fn new() -> std::result::Result<Self, Box<dyn std::error::Error>> {
        let api_key = get_openai_api_key()?;

        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert(
            reqwest::header::AUTHORIZATION,
            format!("Bearer {}", &api_key).parse().unwrap(),
        );
        headers.insert(
            reqwest::header::CONTENT_TYPE,
            "application/json".parse().unwrap(),
        );

        let client = reqwest::Client::builder()
            .default_headers(headers)
            .build()
            .map_err(|e| format!("Failed to create HTTP client: {}", e))?;

        Ok(Embedder {
            client,
            model: "text-embedding-3-small".to_string(),
            dimensions: 512,
        })
    }

    pub async fn genereate_embeddings(
        &self,
        text_content: &str,
    ) -> std::result::Result<EmbeddingResponse, Box<dyn std::error::Error>> {
        let res = self
            .client
            .post("https://api.openai.com/v1/embeddings")
            .json(&serde_json::json!({
                "model": self.model,
                "input": text_content,
                "dimensions": self.dimensions,
            }))
            .send()
            .await
            .map_err(|e| format!("Failed to send embeddings request: {}", e))?;

        if !res.status().is_success() {
            let status = res.status();
            let error_text = res
                .text()
                .await
                .unwrap_or_else(|_| "Could not read error response".to_string());
            return Err(format!("OpenAI API error: {} - {}", status, error_text).into());
        }

        let embedding_response: EmbeddingResponse = res
            .json()
            .await
            .map_err(|e| format!("Failed to parse embeddings response: {}", e))?;

        Ok(embedding_response)
    }
}
