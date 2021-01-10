use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum CollectionType {
    CountAndTopDocs,
    Count,
    TopDocs,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SearchRequest {
    pub query: String,
    pub from: u64,
    pub limit: u64,
    pub collection_type: CollectionType,
    pub facet_field: Option<String>,
    pub facet_prefixes: Option<Vec<String>>,
}
