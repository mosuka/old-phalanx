use std::collections::HashMap;
use std::error::Error;
use std::fs;
use std::io::{Error as IOError, ErrorKind};
use std::path::Path;

use lazy_static::lazy_static;
use log::*;
use prometheus::{register_counter_vec, register_histogram_vec, CounterVec, HistogramVec};
use tantivy::collector::{Count, FacetCollector, MultiCollector, TopDocs};
use tantivy::directory::MmapDirectory;
use tantivy::merge_policy::LogMergePolicy;
use tantivy::query::{QueryParser, TermQuery};
use tantivy::schema::{Field, FieldType, IndexRecordOption, Schema};
use tantivy::{Index, IndexWriter, Term};
use tokio::sync::Mutex;
use tonic::codegen::Arc;
use tonic::{Code, Request, Response, Status};

use phalanx_proto::phalanx::index_service_server::IndexService as ProtoIndexService;
use phalanx_proto::phalanx::{
    BulkDeleteReply, BulkDeleteReq, BulkSetReply, BulkSetReq, CommitReply, CommitReq, DeleteReply,
    DeleteReq, GetReply, GetReq, MergeReply, MergeReq, ReadinessReply, ReadinessReq, RollbackReply,
    RollbackReq, SchemaReply, SchemaReq, SearchReply, SearchReq, SetReply, SetReq, State,
    UnwatchReply, UnwatchReq, WatchReply, WatchReq,
};

use crate::index::config::IndexConfig;
use crate::index::search_request::{CollectionType, SearchRequest};
use crate::index::search_result::{ScoredNamedFieldDocument, SearchResult};
use crate::index::watcher::Watcher;
use crate::tokenizer::tokenizer_initializer::TokenizerInitializer;

lazy_static! {
    static ref REQUEST_COUNTER: CounterVec = register_counter_vec!(
        "phalanx_index_requests_total",
        "Total number of requests.",
        &["func"]
    )
    .unwrap();
    static ref REQUEST_HISTOGRAM: HistogramVec = register_histogram_vec!(
        "phalanx_index_request_duration_seconds",
        "The request latencies in seconds.",
        &["func"]
    )
    .unwrap();
}

pub struct IndexService {
    index: Index,
    index_writer: Arc<Mutex<IndexWriter>>,
    unique_key_field: Field,
    watcher: Arc<Mutex<Watcher>>,
}

impl IndexService {
    pub fn new(index_config: IndexConfig, watcher: Watcher) -> IndexService {
        // create index directory
        let p = Path::new(&index_config.index_dir);
        fs::create_dir_all(p).unwrap_or_default();

        // create index
        // let dir = MmapDirectory::open(&index_config.index_dir).unwrap();
        let dir = MmapDirectory::open(p).unwrap();
        let index = if Index::exists(&dir) {
            // open index if the index exists in local file system
            match Index::open(dir) {
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
                    Ok(schema) => match Index::create(dir, schema) {
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

        IndexService {
            index,
            index_writer: Arc::new(Mutex::new(index_writer)),
            unique_key_field,
            watcher: Arc::new(Mutex::new(watcher)),
        }
    }

    pub async fn get(&self, id: &str) -> Result<Option<Vec<u8>>, Box<dyn Error + Send + Sync>> {
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

        match serde_json::to_vec(&named_doc) {
            Ok(doc) => Ok(Some(doc)),
            Err(e) => {
                return Err(Box::new(IOError::new(
                    ErrorKind::Other,
                    format!("failed to serialize document: error={:?}", e),
                )));
            }
        }
    }

    pub async fn set(&self, doc: Vec<u8>) -> Result<(), Box<dyn Error + Send + Sync>> {
        let doc_json = match String::from_utf8(doc) {
            Ok(doc_json) => doc_json,
            Err(e) => {
                return Err(Box::new(IOError::new(
                    ErrorKind::Other,
                    format!("failed to convert document JSON: error={:?}", e),
                )));
            }
        };
        let doc = match self.index.schema().parse_document(&doc_json) {
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

    pub async fn bulk_set(&self, docs: Vec<Vec<u8>>) -> Result<(), Box<dyn Error + Send + Sync>> {
        let index_writer = self.index_writer.lock().await;
        let mut success_count = 0;
        let mut error_count = 0;
        let mut total_count = 0;
        for doc in docs {
            total_count += 1;

            let doc_json = match String::from_utf8(doc) {
                Ok(doc_json) => doc_json,
                Err(e) => {
                    error!("failed to convert document JSON: error={:?}", e);
                    error_count += 1;
                    continue;
                }
            };

            let doc_obj = match self.index.schema().parse_document(&doc_json) {
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
                error!("unique key field not included: doc={}", &doc_json);
                error_count += 1;
                continue;
            } else if value_count > 1 {
                error!("multiple unique key fields included: doc={}", &doc_json);
                error_count += 1;
                continue;
            }
            let id = match fields.first().unwrap().text() {
                Some(id) => id,
                None => {
                    error!("failed to get unique key field value: doc={}", &doc_json);
                    error_count += 1;
                    continue;
                }
            };

            let term = Term::from_field_text(self.unique_key_field, id);
            let _opstamp = index_writer.delete_term(term);
            let _opstamp = index_writer.add_document(doc_obj);
            success_count += 1;
        }

        debug!(
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

        debug!("{} documents have been deleted", total_count);

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

#[tonic::async_trait]
impl ProtoIndexService for IndexService {
    async fn readiness(
        &self,
        _request: Request<ReadinessReq>,
    ) -> Result<Response<ReadinessReply>, Status> {
        REQUEST_COUNTER.with_label_values(&["readiness"]).inc();
        let timer = REQUEST_HISTOGRAM
            .with_label_values(&["readiness"])
            .start_timer();

        let state = State::Ready as i32;

        let reply = ReadinessReply { state };

        timer.observe_duration();

        Ok(Response::new(reply))
    }

    async fn watch(&self, _request: Request<WatchReq>) -> Result<Response<WatchReply>, Status> {
        REQUEST_COUNTER.with_label_values(&["watch"]).inc();
        let timer = REQUEST_HISTOGRAM
            .with_label_values(&["watch"])
            .start_timer();

        let watcher_arc = Arc::clone(&self.watcher);
        let mut watcher = watcher_arc.lock().await;
        match watcher.watch().await {
            Ok(_) => (),
            Err(err) => {
                timer.observe_duration();

                return Err(Status::new(
                    Code::Internal,
                    format!("failed to watch: error = {:?}", err),
                ));
            }
        }

        let reply = WatchReply {};

        timer.observe_duration();

        Ok(Response::new(reply))
    }

    async fn unwatch(
        &self,
        _request: Request<UnwatchReq>,
    ) -> Result<Response<UnwatchReply>, Status> {
        REQUEST_COUNTER.with_label_values(&["unwatch"]).inc();
        let timer = REQUEST_HISTOGRAM
            .with_label_values(&["unwatch"])
            .start_timer();

        let watcher_arc = Arc::clone(&self.watcher);
        let mut watcher = watcher_arc.lock().await;
        match watcher.unwatch().await {
            Ok(_) => (),
            Err(err) => {
                timer.observe_duration();

                return Err(Status::new(
                    Code::Internal,
                    format!("failed to unwatch: error = {:?}", err),
                ));
            }
        }

        let reply = UnwatchReply {};

        timer.observe_duration();

        Ok(Response::new(reply))
    }

    async fn get(&self, request: Request<GetReq>) -> Result<Response<GetReply>, Status> {
        REQUEST_COUNTER.with_label_values(&["get"]).inc();
        let timer = REQUEST_HISTOGRAM.with_label_values(&["get"]).start_timer();

        let req = request.into_inner();

        match self.get(&req.id).await {
            Ok(resp) => match resp {
                Some(doc) => {
                    let reply = GetReply { doc };

                    timer.observe_duration();

                    Ok(Response::new(reply))
                }
                None => {
                    timer.observe_duration();

                    Err(Status::new(Code::NotFound, "document is None"))
                }
            },
            Err(e) => {
                timer.observe_duration();

                Err(Status::new(
                    Code::Internal,
                    format!("failed to get document: error = {:?}", e),
                ))
            }
        }
    }

    async fn set(&self, request: Request<SetReq>) -> Result<Response<SetReply>, Status> {
        REQUEST_COUNTER.with_label_values(&["set"]).inc();
        let timer = REQUEST_HISTOGRAM.with_label_values(&["set"]).start_timer();

        let req = request.into_inner();

        match self.set(req.doc).await {
            Ok(_) => {
                let reply = SetReply {};

                timer.observe_duration();
                Ok(Response::new(reply))
            }
            Err(e) => {
                timer.observe_duration();

                Err(Status::new(
                    Code::InvalidArgument,
                    format!("failed to set document: err = {:?}", e),
                ))
            }
        }
    }

    async fn delete(&self, request: Request<DeleteReq>) -> Result<Response<DeleteReply>, Status> {
        REQUEST_COUNTER.with_label_values(&["delete"]).inc();
        let timer = REQUEST_HISTOGRAM
            .with_label_values(&["delete"])
            .start_timer();

        let req = request.into_inner();

        match self.delete(&req.id).await {
            Ok(_) => {
                let reply = DeleteReply {};

                timer.observe_duration();
                Ok(Response::new(reply))
            }
            Err(e) => {
                timer.observe_duration();

                Err(Status::new(
                    Code::InvalidArgument,
                    format!("failed to delete document: err = {:?}", e),
                ))
            }
        }
    }

    async fn bulk_set(
        &self,
        request: Request<BulkSetReq>,
    ) -> Result<Response<BulkSetReply>, Status> {
        REQUEST_COUNTER.with_label_values(&["bulk_set"]).inc();
        let timer = REQUEST_HISTOGRAM
            .with_label_values(&["bulk_set"])
            .start_timer();

        let req = request.into_inner();

        // let docs = req.docs.iter().map(|doc| doc.as_str()).collect();
        match self.bulk_set(req.docs).await {
            Ok(_) => {
                let reply = BulkSetReply {};

                timer.observe_duration();
                Ok(Response::new(reply))
            }
            Err(e) => {
                timer.observe_duration();

                Err(Status::new(
                    Code::InvalidArgument,
                    format!("failed to set documents in bulk: err = {:?}", e),
                ))
            }
        }
    }

    async fn bulk_delete(
        &self,
        request: Request<BulkDeleteReq>,
    ) -> Result<Response<BulkDeleteReply>, Status> {
        REQUEST_COUNTER.with_label_values(&["bulk_delete"]).inc();
        let timer = REQUEST_HISTOGRAM
            .with_label_values(&["bulk_delete"])
            .start_timer();

        let req = request.into_inner();

        let ids = req.ids.iter().map(|id| id.as_str()).collect();
        match self.bulk_delete(ids).await {
            Ok(_) => {
                let reply = BulkDeleteReply {};

                timer.observe_duration();
                Ok(Response::new(reply))
            }
            Err(e) => {
                timer.observe_duration();

                Err(Status::new(
                    Code::InvalidArgument,
                    format!("failed to delete documents in bulk: err = {:?}", e),
                ))
            }
        }
    }

    async fn commit(&self, _request: Request<CommitReq>) -> Result<Response<CommitReply>, Status> {
        REQUEST_COUNTER.with_label_values(&["commit"]).inc();
        let timer = REQUEST_HISTOGRAM
            .with_label_values(&["commit"])
            .start_timer();

        match self.commit().await {
            Ok(_) => {
                let reply = CommitReply {};

                timer.observe_duration();

                Ok(Response::new(reply))
            }
            Err(e) => {
                timer.observe_duration();

                Err(Status::new(
                    Code::Internal,
                    format!("failed to commit index: error = {:?}", e),
                ))
            }
        }
    }

    async fn rollback(
        &self,
        _request: Request<RollbackReq>,
    ) -> Result<Response<RollbackReply>, Status> {
        REQUEST_COUNTER.with_label_values(&["rollback"]).inc();
        let timer = REQUEST_HISTOGRAM
            .with_label_values(&["rollback"])
            .start_timer();

        match self.rollback().await {
            Ok(_) => {
                let reply = RollbackReply {};

                timer.observe_duration();

                Ok(Response::new(reply))
            }
            Err(e) => {
                timer.observe_duration();

                Err(Status::new(
                    Code::Internal,
                    format!("failed to rollback index: error = {:?}", e),
                ))
            }
        }
    }

    async fn merge(&self, _request: Request<MergeReq>) -> Result<Response<MergeReply>, Status> {
        REQUEST_COUNTER.with_label_values(&["merge"]).inc();
        let timer = REQUEST_HISTOGRAM
            .with_label_values(&["merge"])
            .start_timer();

        match self.merge().await {
            Ok(_) => {
                let reply = MergeReply {};

                timer.observe_duration();

                Ok(Response::new(reply))
            }
            Err(e) => {
                timer.observe_duration();

                Err(Status::new(
                    Code::Internal,
                    format!("failed to merge index: error = {:?}", e),
                ))
            }
        }
    }

    async fn schema(&self, _request: Request<SchemaReq>) -> Result<Response<SchemaReply>, Status> {
        REQUEST_COUNTER.with_label_values(&["schema"]).inc();
        let timer = REQUEST_HISTOGRAM
            .with_label_values(&["schema"])
            .start_timer();

        match serde_json::to_vec(&self.schema()) {
            Ok(schema) => {
                let reply = SchemaReply { schema };

                timer.observe_duration();

                Ok(Response::new(reply))
            }
            Err(e) => {
                timer.observe_duration();

                Err(Status::new(
                    Code::Internal,
                    format!("failed to get schema: error = {:?}", e),
                ))
            }
        }
    }

    async fn search(&self, request: Request<SearchReq>) -> Result<Response<SearchReply>, Status> {
        REQUEST_COUNTER.with_label_values(&["search"]).inc();
        let timer = REQUEST_HISTOGRAM
            .with_label_values(&["search"])
            .start_timer();

        let req = request.into_inner();

        let search_request = match serde_json::from_slice::<SearchRequest>(req.request.as_slice()) {
            Ok(search_request) => search_request,
            Err(e) => {
                timer.observe_duration();

                return Err(Status::new(
                    Code::Internal,
                    format!("failed to parse search request: error = {:?}", e),
                ));
            }
        };

        match self.search(search_request).await {
            Ok(result) => match serde_json::to_vec(&result) {
                Ok(result) => {
                    let reply = SearchReply { result };

                    timer.observe_duration();

                    Ok(Response::new(reply))
                }
                Err(e) => {
                    timer.observe_duration();

                    Err(Status::new(
                        Code::Internal,
                        format!("failed to serialize to JSON: error = {:?}", e),
                    ))
                }
            },
            Err(e) => {
                timer.observe_duration();

                Err(Status::new(
                    Code::Internal,
                    format!("failed to search index: error = {:?}", e),
                ))
            }
        }
    }
}
