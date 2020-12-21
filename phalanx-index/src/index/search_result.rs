use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use tantivy::schema::NamedFieldDocument;

#[derive(Debug, Deserialize, Serialize)]
pub struct ScoredNamedFieldDocument {
    pub fields: NamedFieldDocument,
    pub score: f32,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct SearchResult {
    pub count: i64,
    pub docs: Vec<ScoredNamedFieldDocument>,
    pub facet: HashMap<String, HashMap<String, u64>>,
}
