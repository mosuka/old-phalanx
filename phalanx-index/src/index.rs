pub mod config;
pub mod search_request;
pub mod search_result;

use std::collections::HashMap;
use std::error::Error;
use std::fs;
use std::io::{Error as IOError, ErrorKind};
use std::path::Path;
use std::sync::Arc;

use log::*;
use serde_json::Value;
use tantivy::collector::{Count, FacetCollector, MultiCollector, TopDocs};
use tantivy::directory::MmapDirectory;
use tantivy::merge_policy::LogMergePolicy;
use tantivy::query::{QueryParser, TermQuery};
use tantivy::schema::{Field, FieldType, IndexRecordOption, Schema};
use tantivy::{Index as TantivyIndex, IndexWriter, Term};
use tokio::fs::File;
use tokio::io;
use tokio::io::AsyncReadExt;
use tokio::sync::Mutex;
use walkdir::WalkDir;

use phalanx_discovery::discovery::DiscoveryContainer;
use phalanx_storage::storage::StorageContainer;

use crate::index::config::IndexConfig;
use crate::index::search_request::{CollectionType, SearchRequest};
use crate::index::search_result::{ScoredNamedFieldDocument, SearchResult};
use crate::tokenizer::tokenizer_initializer::TokenizerInitializer;

pub struct Index {
    index_name: String,
    shard_name: String,
    node_name: String,
    index_config: IndexConfig,
    discovery_container: DiscoveryContainer,
    storage_container: StorageContainer,
    index: TantivyIndex,
    index_writer: Arc<Mutex<IndexWriter>>,
    unique_key_field: Field,
}

impl Index {
    pub fn new(
        index_name: String,
        shard_name: String,
        node_name: String,
        index_config: IndexConfig,
        discovery_container: DiscoveryContainer,
        storage_container: StorageContainer,
    ) -> Index {
        // create index directory
        fs::create_dir_all(&index_config.index_dir).unwrap_or_default();

        // create index
        let dir = MmapDirectory::open(&index_config.index_dir).unwrap();
        let index = if TantivyIndex::exists(&dir) {
            // open index if the index exists in local file system
            match TantivyIndex::open(dir) {
                Ok(index) => index,
                Err(e) => {
                    error!("failed to open mmap index: error = {:?}", e);
                    panic!();
                }
            }
        } else {
            // create index if the index doesn't exist in local file system
            match fs::read_to_string(&index_config.schema_file) {
                Ok(content) => match serde_json::from_str::<Schema>(&content) {
                    Ok(schema) => match TantivyIndex::create(dir, schema) {
                        Ok(index) => index,
                        Err(e) => {
                            error!("failed to create mmap index: error = {:?}", e);
                            panic!();
                        }
                    },
                    Err(e) => {
                        error!("failed to parse schema JSON: error = {:?}", e);
                        panic!();
                    }
                },
                Err(e) => {
                    error!("failed to read schema file: error = {:?}", e);
                    panic!();
                }
            }
        };

        // initialize tokenizers
        if !index_config.tokenizer_file.is_empty() {
            let tokenizer_content = match fs::read_to_string(&index_config.tokenizer_file) {
                Ok(content) => content,
                Err(e) => {
                    error!("failed to read tokenizer file: error = {:?}", e);
                    panic!();
                }
            };
            let mut tokenizer_initializer = TokenizerInitializer::new();
            tokenizer_initializer.configure(index.tokenizers(), tokenizer_content.as_str());
        }

        // create index writer
        let index_writer =
            match if index_config.indexer_threads > 0 && index_config.indexer_memory_size > 0 {
                index.writer_with_num_threads(
                    index_config.indexer_threads,
                    index_config.indexer_memory_size,
                )
            } else {
                index.writer(index_config.indexer_memory_size)
            } {
                Ok(index_writer) => index_writer,
                Err(e) => {
                    error!("failed to open or create index writer: error = {:?}", e);
                    panic!();
                }
            };
        index_writer.set_merge_policy(Box::new(LogMergePolicy::default()));

        let unique_key_field = match index
            .schema()
            .get_field(index_config.unique_key_field.as_str())
        {
            Some(field) => field,
            None => {
                error!(
                    "failed to get unique key field: name={}",
                    index_config.unique_key_field
                );
                panic!();
            }
        };

        Index {
            index_name,
            shard_name,
            node_name,
            index_config,
            discovery_container,
            storage_container,
            index,
            index_writer: Arc::new(Mutex::new(index_writer)),
            unique_key_field,
        }
    }

    pub async fn get(&self, id: &str) -> Result<Option<String>, Box<dyn Error + Send + Sync>> {
        let term = Term::from_field_text(self.unique_key_field, id);
        let term_query = TermQuery::new(term, IndexRecordOption::Basic);

        let searcher = match self.index.reader() {
            Ok(reader) => reader.searcher(),
            Err(e) => {
                return Err(Box::new(IOError::new(
                    ErrorKind::Other,
                    format!("failed to get searcher: error={:?}", e),
                )));
            }
        };

        let named_doc = match searcher.search(&term_query, &TopDocs::with_limit(1)) {
            Ok(top_docs) => match top_docs.first() {
                Some((_score, doc_address)) => match searcher.doc(*doc_address) {
                    Ok(doc) => self.index.schema().to_named_doc(&doc),
                    Err(e) => {
                        return Err(Box::new(IOError::new(
                            ErrorKind::Other,
                            format!("failed to get document: error={:?}", e),
                        )));
                    }
                },
                None => return Ok(None),
            },
            Err(e) => {
                return Err(Box::new(IOError::new(
                    ErrorKind::Other,
                    format!("failed to get document: error={:?}", e),
                )));
            }
        };

        match serde_json::to_string(&named_doc) {
            Ok(doc_str) => Ok(Some(doc_str)),
            Err(e) => {
                return Err(Box::new(IOError::new(
                    ErrorKind::Other,
                    format!("failed to serialize document: error={:?}", e),
                )));
            }
        }
    }

    pub async fn set(&self, doc: &str) -> Result<(), Box<dyn Error + Send + Sync>> {
        let doc = match self.index.schema().parse_document(doc) {
            Ok(doc) => doc,
            Err(e) => {
                return Err(Box::new(IOError::new(
                    ErrorKind::Other,
                    format!("failed to parse document JSON: error={:?}", e),
                )));
            }
        };

        let value_count = doc.get_all(self.unique_key_field).len();
        if value_count < 1 {
            return Err(Box::new(IOError::new(
                ErrorKind::Other,
                format!("unique key field not included"),
            )));
        } else if value_count > 1 {
            return Err(Box::new(IOError::new(
                ErrorKind::Other,
                format!("multiple unique key fields included"),
            )));
        }

        let id = match doc.get_first(self.unique_key_field).unwrap().text() {
            Some(id) => id,
            None => {
                return Err(Box::new(IOError::new(
                    ErrorKind::Other,
                    format!("failed to get unique key field value"),
                )));
            }
        };

        let term = Term::from_field_text(self.unique_key_field, id);

        let index_writer = self.index_writer.lock().await;
        let _opstamp = index_writer.delete_term(term);
        let _opstamp = index_writer.add_document(doc);

        Ok(())
    }

    pub async fn delete(&self, id: &str) -> Result<(), Box<dyn Error + Send + Sync>> {
        let term = Term::from_field_text(self.unique_key_field, id);

        let index_writer = self.index_writer.lock().await;
        let _opstamp = index_writer.delete_term(term);

        Ok(())
    }

    pub async fn bulk_set(&self, docs: Vec<&str>) -> Result<(), Box<dyn Error + Send + Sync>> {
        let mut success_count = 0;
        let mut error_count = 0;
        let mut total_count = 0;

        let index_writer = self.index_writer.lock().await;
        for doc in docs {
            total_count += 1;

            let doc_obj = match self.index.schema().parse_document(doc) {
                Ok(doc_obj) => doc_obj,
                Err(e) => {
                    error!("failed to parse document JSON: error={:?}", e);
                    error_count += 1;
                    continue;
                }
            };

            let fields = doc_obj.get_all(self.unique_key_field);
            let value_count = fields.len();
            if value_count < 1 {
                error!("unique key field not included: doc={}", doc);
                error_count += 1;
                continue;
            } else if value_count > 1 {
                error!("multiple unique key fields included: doc={}", doc);
                error_count += 1;
                continue;
            }
            let id = match fields.first().unwrap().text() {
                Some(id) => id,
                None => {
                    error!("failed to get unique key field value: doc={}", doc);
                    error_count += 1;
                    continue;
                }
            };

            let term = Term::from_field_text(self.unique_key_field, id);
            let _opstamp = index_writer.delete_term(term);
            let _opstamp = index_writer.add_document(doc_obj);
            success_count += 1;
        }

        info!(
            "{} documents have been indexed: success documents={}, error documents={}",
            total_count, success_count, error_count
        );

        if error_count > 0 {
            return Err(Box::new(IOError::new(
                ErrorKind::Other,
                format!("some errors have occurred"),
            )));
        }

        Ok(())
    }

    pub async fn bulk_delete(&self, ids: Vec<&str>) -> Result<(), Box<dyn Error + Send + Sync>> {
        let mut total_count = 0;

        let index_writer = self.index_writer.lock().await;

        for id in ids {
            total_count += 1;

            let term = Term::from_field_text(self.unique_key_field, id);
            let _opstamp = index_writer.delete_term(term);
        }

        info!("{} documents have been deleted", total_count);

        Ok(())
    }

    pub async fn commit(&self) -> Result<(), Box<dyn Error + Send + Sync>> {
        let mut index_writer = self.index_writer.lock().await;
        match index_writer.commit() {
            Ok(_opstamp) => Ok(()),
            Err(e) => {
                return Err(Box::new(IOError::new(
                    ErrorKind::Other,
                    format!("failed to commit index: error={:?}", e),
                )));
            }
        }

        // TODO: If the node is a leader, it pushes the index to object storage.
    }

    pub async fn rollback(&self) -> Result<(), Box<dyn Error + Send + Sync>> {
        let mut index_writer = self.index_writer.lock().await;
        match index_writer.rollback() {
            Ok(_opstamp) => Ok(()),
            Err(e) => {
                return Err(Box::new(IOError::new(
                    ErrorKind::Other,
                    format!("failed to commit index: error={:?}", e),
                )));
            }
        }
    }

    pub async fn merge(&self) -> Result<(), Box<dyn Error + Send + Sync>> {
        let segment_ids = match self.index.searchable_segment_ids() {
            Ok(segment_ids) => {
                if segment_ids.len() <= 0 {
                    return Ok(());
                } else {
                    segment_ids
                }
            }
            Err(e) => {
                return Err(Box::new(IOError::new(
                    ErrorKind::Other,
                    format!("failed to get segment ids: error={:?}", e),
                )));
            }
        };

        let mut index_writer = self.index_writer.lock().await;
        match index_writer.merge(&segment_ids).await {
            Ok(segment_meta) => {
                debug!("merge index: segment_meta={:?}", segment_meta);
                Ok(())
            }
            Err(e) => Err(Box::new(IOError::new(
                ErrorKind::Other,
                format!("failed to merge index ids: error={:?}", e),
            ))),
        }

        // TODO: If the node is a leader, it pushes the index to object storage.

    }

    pub fn schema(&self) -> Schema {
        self.index.schema()
    }

    pub async fn push(&self) -> Result<(), Box<dyn Error + Send + Sync>> {
        // list file names
        let mut file_names = Vec::new();
        for entry in WalkDir::new(&self.index_config.index_dir)
            .follow_links(true)
            .into_iter()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_type().is_file())
        {
            let file_name = entry.file_name().to_str().unwrap();
            // exclude lock files
            if !file_name.ends_with(".lock") {
                file_names.push(String::from(file_name));
            }
        }

        // push files to object storage
        for file_name in &file_names {
            // read file
            let file_path = String::from(
                Path::new(&self.index_config.index_dir)
                    .join(&file_name)
                    .to_str()
                    .unwrap(),
            );

            let mut file = match File::open(&file_path).await {
                Ok(file) => file,
                Err(e) => return Err(Box::new(e)),
            };
            // let mut file = File::open(&file_path).await.unwrap();
            let mut content: Vec<u8> = Vec::new();
            match file.read_to_end(&mut content).await {
                Ok(_) => (),
                Err(e) => return Err(Box::new(e)),
            };
            // file.read_to_end(&mut content).await.unwrap();

            // put object
            let object_key = format!("{}/{}/{}", self.index_name, self.shard_name, &file_name);
            info!("set {} to {}", &file_path, &object_key);
            match self
                .storage_container
                .storage
                .set(object_key.as_str(), content.as_slice())
                .await
            {
                Ok(_) => (),
                Err(e) => {
                    error!("failed to set object: error={:?}", e);
                    return Err(e);
                }
            };
        }

        // list object names
        let prefix = format!("{}/{}/", self.index_name, self.shard_name);
        let object_names = match self.storage_container.storage.list(&prefix).await {
            Ok(object_keys) => {
                let mut object_names = Vec::new();
                for object_key in object_keys {
                    // cluster1/shard1/meta.json -> meta.json
                    let object_name = Path::new(&object_key).strip_prefix(&prefix).unwrap();
                    object_names.push(String::from(object_name.to_str().unwrap()));
                }
                object_names
            }
            Err(e) => return Err(e),
        };

        // remove unnecessary objects
        for object_name in object_names {
            if !file_names.contains(&object_name) {
                // e.g. meta.json -> cluster1/shard1/meta.json
                let object_key =
                    format!("{}/{}/{}", self.index_name, self.shard_name, &object_name);
                match self
                    .storage_container
                    .storage
                    .delete(object_key.as_str())
                    .await
                {
                    Ok(_output) => (),
                    Err(e) => return Err(e),
                };
            }
        }

        Ok(())
    }

    pub async fn pull(&self) -> Result<(), Box<dyn Error + Send + Sync>> {
        // list objects
        let managed_json_key = format!("{}/{}/.managed.json", self.index_name, self.shard_name);
        let mut object_names = match self
            .storage_container
            .storage
            .get(managed_json_key.as_str())
            .await
        {
            Ok(resp) => {
                match resp {
                    Some(content) => {
                        // parse content
                        let value: Value =
                            match serde_json::from_slice::<Value>(&content.as_slice()) {
                                Ok(value) => value,
                                Err(e) => {
                                    return Err(Box::new(IOError::new(
                                        ErrorKind::Other,
                                        format!("failed to parse .managed.json : error={:?}", e),
                                    )));
                                }
                            };

                        // create object name list
                        let mut object_names = Vec::new();
                        for object_name in value.as_array().unwrap() {
                            object_names.push(String::from(object_name.as_str().unwrap()));
                        }
                        object_names
                    }
                    None => {
                        return Err(Box::new(IOError::new(
                            ErrorKind::Other,
                            format!("content is None"),
                        )))
                    }
                }
            }
            Err(e) => return Err(e),
        };
        object_names.push(".managed.json".to_string());

        // pull objects
        for object_name in object_names.clone() {
            // get object
            let object_key = format!("{}/{}/{}", self.index_name, self.shard_name, &object_name);
            match self
                .storage_container
                .storage
                .get(object_key.as_str())
                .await
            {
                Ok(resp) => {
                    match resp {
                        Some(content) => {
                            let file_path = String::from(
                                Path::new(&self.index_config.index_dir)
                                    .join(&object_name)
                                    .to_str()
                                    .unwrap(),
                            );
                            info!("pull {} to {}", &object_key, &file_path);
                            let mut file = match File::create(&file_path).await {
                                Ok(file) => file,
                                Err(e) => {
                                    return Err(Box::new(IOError::new(
                                        ErrorKind::Other,
                                        format!(
                                            "failed to create file {}: error={:?}",
                                            &file_path, e
                                        ),
                                    )));
                                }
                            };
                            io::copy(&mut content.as_slice(), &mut file).await.unwrap();
                        }
                        None => {
                            return Err(Box::new(IOError::new(
                                ErrorKind::Other,
                                format!("content is None"),
                            )));
                        }
                    };
                }
                Err(e) => return Err(e),
            };
        }

        // list files
        let mut file_names = Vec::new();
        for entry in WalkDir::new(&self.index_config.index_dir)
            .follow_links(true)
            .into_iter()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_type().is_file())
        {
            let file_name = entry.file_name().to_str().unwrap();
            // exclude lock files
            if !file_name.ends_with(".lock") {
                file_names.push(String::from(file_name));
            }
        }

        // remove unnecessary files
        for file_name in file_names {
            if !object_names.contains(&file_name) {
                let file_path = String::from(
                    Path::new(&self.index_config.index_dir)
                        .join(&file_name)
                        .to_str()
                        .unwrap(),
                );
                match fs::remove_file(&file_path) {
                    Ok(()) => debug!("delete: {}", &file_path),
                    Err(e) => {
                        return Err(Box::new(IOError::new(
                            ErrorKind::Other,
                            format!("failed to delete file: error={:?}", e),
                        )));
                    }
                };
            }
        }

        Ok(())
    }

    pub async fn search(
        &self,
        search_request: SearchRequest,
    ) -> Result<SearchResult, Box<dyn Error + Send + Sync>> {
        let default_search_fields: Vec<Field> = self
            .index
            .schema()
            .fields()
            .flat_map(|(field, field_entry)| {
                if let FieldType::Str(text_field_options) = field_entry.field_type() {
                    if text_field_options.get_indexing_options().is_some() {
                        return Some(field);
                    }
                }
                None
            })
            .collect();

        let limit = search_request.from + search_request.limit;

        let query_parser = QueryParser::for_index(&self.index, default_search_fields);
        let query = query_parser.parse_query(&search_request.query).unwrap();

        let mut multi_collector = MultiCollector::new();

        let count_handle;
        let top_docs_handle;
        match search_request.collection_type {
            CollectionType::CountAndTopDocs => {
                count_handle = Some(multi_collector.add_collector(Count));
                top_docs_handle =
                    Some(multi_collector.add_collector(TopDocs::with_limit(limit as usize)));
            }
            CollectionType::Count => {
                count_handle = Some(multi_collector.add_collector(Count));
                top_docs_handle = None;
            }
            CollectionType::TopDocs => {
                count_handle = None;
                top_docs_handle =
                    Some(multi_collector.add_collector(TopDocs::with_limit(limit as usize)));
            }
        }

        let facet_handle;
        match search_request.facet_field {
            Some(ref field_name) => match self.index.schema().get_field(&field_name) {
                Some(field) => {
                    if search_request.facet_prefixes.len() > 0 {
                        let mut facet_collector = FacetCollector::for_field(field);
                        for facet_prefix in &search_request.facet_prefixes {
                            facet_collector.add_facet(facet_prefix);
                        }
                        facet_handle = Some(multi_collector.add_collector(facet_collector));
                    } else {
                        facet_handle = None;
                    }
                }
                None => {
                    facet_handle = None;
                }
            },
            None => {
                facet_handle = None;
            }
        }

        let searcher = match self.index.reader() {
            Ok(reader) => reader.searcher(),
            Err(e) => {
                return Err(Box::new(IOError::new(
                    ErrorKind::Other,
                    format!("failed to get index reader: error={:?}", e),
                )));
            }
        };

        match searcher.search(&query, &multi_collector) {
            Ok(mut multi_fruit) => {
                // count
                let mut count: i64 = -1;
                if let Some(ch) = count_handle {
                    count = ch.extract(&mut multi_fruit) as i64;
                }

                // docs
                let mut top_docs = Vec::new();
                if let Some(tdh) = top_docs_handle {
                    top_docs = tdh.extract(&mut multi_fruit);
                }

                // facet
                let mut facet: HashMap<String, HashMap<String, u64>> = HashMap::new();
                if let Some(fh) = facet_handle {
                    let facet_counts = fh.extract(&mut multi_fruit);
                    let mut facet_kv: HashMap<String, u64> = HashMap::new();
                    for facet_prefix in &search_request.facet_prefixes {
                        for (facet_key, facet_value) in facet_counts.get(facet_prefix) {
                            facet_kv.insert(facet_key.to_string(), facet_value);
                        }
                    }
                    match &search_request.facet_field {
                        Some(facet_field) => {
                            facet.insert(facet_field.to_string(), facet_kv);
                        }
                        None => {}
                    }
                }

                // docs
                let mut docs: Vec<ScoredNamedFieldDocument> = Vec::new();
                let mut doc_pos: u64 = 0;
                for (score, doc_address) in top_docs {
                    if doc_pos >= search_request.from as u64 {
                        let doc = searcher.doc(doc_address).unwrap();
                        let named_doc = self.index.schema().to_named_doc(&doc);
                        let scored_doc = ScoredNamedFieldDocument {
                            fields: named_doc,
                            score,
                        };
                        docs.push(scored_doc);
                    }
                    doc_pos += 1;
                }

                // search result
                Ok(SearchResult { docs, count, facet })
            }
            Err(e) => Err(Box::new(IOError::new(
                ErrorKind::Other,
                format!("failed to search index: error={:?}", e),
            ))),
        }
    }
}
