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
    pub embedding: Vec<f32>, // 1536 floats for ada-002
    pub index: usize,
}

#[derive(Debug, Deserialize)]
pub struct Usage {
    pub prompt_tokens: usize,
    pub total_tokens: usize,
}

pub struct Embedder {
    client: reqwest::Client,
}

fn get_openai_api_key() -> std::result::Result<String, Box<dyn std::error::Error>> {
    let api_key = std::env::var("OPENAI_API_KEY")
        .map_err(|_| "Missing OPENAI_API_KEY environment variable")?;

    Ok(api_key)
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
            .map_err(|e| format!("failed to create http client {}", e))?;

        Ok(Embedder { client })
    }

    pub async fn genereate_embeddings(
        &self,
        text_content: &str,
    ) -> std::result::Result<EmbeddingResponse, Box<dyn std::error::Error>> {
        let res = self
            .client
            .post("https://api.openai.com/v1/embeddings")
            .json(&serde_json::json!({
                "model": "text-embedding-3-small",
                "input": text_content,
            }))
            .send()
            .await?;
        let embedding_response: EmbeddingResponse = res.json().await?;
        Ok(embedding_response)
    }
}
