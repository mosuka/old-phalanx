pub const DEFAULT_INDEX_DIRECTORY: &str = "/var/lib/phalanx/index";
pub const DEFAULT_SCHEMA_FILE: &str = "/etc/phalanx/schema.json";
pub const DEFAULT_TOKENIZER_FILE: &str = "/etc/phalanx/tokenizer.json";
pub const DEFAULT_INDEXER_THREADS: usize = 1;
pub const DEFAULT_INDEXER_MEMORY_SIZE: usize = 500_000_000;
pub const DEFAULT_UNIQUE_KEY_FIELD: &str = "id";

#[derive(Clone)]
pub struct IndexConfig {
    pub index_dir: String,
    pub schema_file: String,
    pub tokenizer_file: String,
    pub indexer_threads: usize,
    pub indexer_memory_size: usize,
    pub unique_key_field: String,
}

impl IndexConfig {
    pub fn new() -> IndexConfig {
        IndexConfig {
            index_dir: String::from(DEFAULT_INDEX_DIRECTORY),
            schema_file: String::from(DEFAULT_SCHEMA_FILE),
            tokenizer_file: String::from(DEFAULT_TOKENIZER_FILE),
            indexer_threads: DEFAULT_INDEXER_THREADS,
            indexer_memory_size: DEFAULT_INDEXER_MEMORY_SIZE,
            unique_key_field: String::from(DEFAULT_UNIQUE_KEY_FIELD),
        }
    }
}
