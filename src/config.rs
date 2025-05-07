use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize)]
pub struct Config {
    tracked: Vec<String>,
}

impl Config {
    pub fn new() -> Self {
        Self { tracked: vec![] }
    }
}
