use std::collections::HashMap;
use std::fs;
use std::sync::Arc;

use async_std::task::block_on;
use log::*;
use prometheus::{CounterVec, HistogramVec};
use tantivy::collector::{Count, FacetCollector, MultiCollector, TopDocs};
use tantivy::directory::MmapDirectory;
use tantivy::merge_policy::LogMergePolicy;
use tantivy::query::{QueryParser, TermQuery};
use tantivy::schema::{Field, FieldType, IndexRecordOption, Schema};
use tantivy::{Document, Index, IndexWriter, Term};
use tokio::sync::Mutex;
use tonic::{Code, Request, Response, Status};

use phalanx_discovery::discovery::Discovery;
use phalanx_proto::phalanx::index_service_server::IndexService as ProtoIndexService;
use phalanx_proto::phalanx::{
    BulkDeleteReply, BulkDeleteReq, BulkSetReply, BulkSetReq, CommitReply, CommitReq, DeleteReply,
    DeleteReq, GetReply, GetReq, MergeReply, MergeReq, PullReply, PullReq, PushReply, PushReq,
    ReadinessReply, ReadinessReq, Role, RollbackReply, RollbackReq, SchemaReply, SchemaReq,
    SearchReply, SearchReq, SetReply, SetReq, State, UnwatchReply, UnwatchReq, WatchReply,
    WatchReq,
};
use phalanx_storage::storage::Storage;

use crate::index::config::IndexConfig;
use crate::index::search_result::{ScoredNamedFieldDocument, SearchResult};
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
    index_config: IndexConfig,
    index: Arc<Index>,
    index_writer: Arc<Mutex<IndexWriter>>,
    index_name: String,
    shard_name: String,
    node_name: String,
    discovery: Arc<Mutex<Box<dyn Discovery>>>,
    storage: Box<dyn Storage>,
}

impl IndexService {
    pub fn new(
        index_config: IndexConfig,
        index_name: &str,
        shard_name: &str,
        node_name: &str,
        discovery: Box<dyn Discovery>,
        storage: Box<dyn Storage>,
    ) -> IndexService {
        // create index directory
        fs::create_dir_all(&index_config.index_dir).unwrap_or_default();

        // check if the index exists in storage
        let exist_future = storage.exist(index_name, shard_name);
        let exist = match block_on(exist_future) {
            Ok(exist) => exist,
            Err(e) => {
                error!("failed to check index in object storage: error = {:?}", e);
                panic!()
            }
        };

        // pull index if the index exists in storage
        if exist {
            let pull_index_future = storage.pull_index(index_name, shard_name);
            match block_on(pull_index_future) {
                Ok(_) => (),
                Err(e) => {
                    error!("failed to pull index from object storage: error = {:?}", e);
                    panic!()
                }
            };
        }

        let dir = MmapDirectory::open(&index_config.index_dir).unwrap();
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
            let schema_content = match fs::read_to_string(&index_config.schema_file) {
                Ok(content) => content,
                Err(e) => {
                    error!("failed to read schema file: error = {:?}", e);
                    panic!();
                }
            };
            let schema: Schema = match serde_json::from_str(&schema_content) {
                Ok(schema) => schema,
                Err(e) => {
                    error!("failed to parse schema JSON: error = {:?}", e);
                    panic!();
                }
            };
            match Index::create(dir, schema) {
                Ok(index) => index,
                Err(e) => {
                    error!("failed to create mmap index: error = {:?}", e);
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

        IndexService {
            index_config,
            index: Arc::new(index),
            index_writer: Arc::new(Mutex::new(index_writer)),
            index_name: String::from(index_name),
            shard_name: String::from(shard_name),
            node_name: String::from(node_name),
            discovery: Arc::new(Mutex::new(discovery)),
            storage,
        }
    }

    fn get_id_field(&self) -> Option<Field> {
        self.index
            .schema()
            .get_field(&self.index_config.unique_key_field)
    }

    async fn push_index(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        match self
            .storage
            .push_index(&self.index_name, &self.shard_name)
            .await
        {
            Ok(_) => Ok(()),
            Err(e) => Err(e),
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

        let discovery = Arc::clone(&self.discovery);
        let mut discovery = discovery.lock().await;

        match discovery.watch_cluster().await {
            Ok(_) => (),
            Err(e) => {
                error!("failed to start watch thread: error = {:?}", e);
            }
        };

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

        let discovery = Arc::clone(&self.discovery);
        let mut discovery = discovery.lock().await;

        match discovery.unwatch_cluster().await {
            Ok(_) => (),
            Err(e) => {
                error!("failed to stop watch thread: error = {:?}", e);
            }
        };

        let reply = UnwatchReply {};

        timer.observe_duration();

        Ok(Response::new(reply))
    }
    async fn get(&self, request: Request<GetReq>) -> Result<Response<GetReply>, Status> {
        REQUEST_COUNTER.with_label_values(&["get"]).inc();
        let timer = REQUEST_HISTOGRAM.with_label_values(&["get"]).start_timer();

        let req = request.into_inner();

        let id_field = match self.get_id_field() {
            Some(field) => field,
            None => {
                timer.observe_duration();

                return Err(Status::new(Code::Internal, "ID field is missing"));
            }
        };

        let term = Term::from_field_text(id_field, &req.id);
        let term_query = TermQuery::new(term, IndexRecordOption::Basic);

        let searcher = self.index.reader().unwrap().searcher();

        let named_doc = match searcher.search(&term_query, &TopDocs::with_limit(1)) {
            Ok(top_docs) => {
                if top_docs.len() > 0 {
                    let mut doc = Document::default();
                    for (_score, doc_address) in top_docs {
                        doc = searcher.doc(doc_address).unwrap();
                    }
                    self.index.schema().to_named_doc(&doc)
                } else {
                    timer.observe_duration();

                    return Err(Status::new(Code::NotFound, "document not found"));
                }
            }
            Err(e) => {
                timer.observe_duration();

                return Err(Status::new(
                    Code::Internal,
                    format!("failed to get document: error = {:?}", e),
                ));
            }
        };

        let doc = match serde_json::to_string(&named_doc) {
            Ok(doc_str) => doc_str,
            Err(e) => {
                timer.observe_duration();

                return Err(Status::new(
                    Code::Internal,
                    format!("failed to convert document to JSON string: error = {:?}", e),
                ));
            }
        };

        let reply = GetReply { doc };

        timer.observe_duration();

        Ok(Response::new(reply))
    }

    async fn set(&self, request: Request<SetReq>) -> Result<Response<SetReply>, Status> {
        REQUEST_COUNTER.with_label_values(&["set"]).inc();
        let timer = REQUEST_HISTOGRAM.with_label_values(&["set"]).start_timer();

        let req = request.into_inner();

        let id_field = match self.get_id_field() {
            Some(field) => field,
            None => {
                timer.observe_duration();

                return Err(Status::new(Code::Internal, "the ID field is missing"));
            }
        };

        let doc = match self.index.schema().parse_document(&req.doc) {
            Ok(doc) => doc,
            Err(e) => {
                timer.observe_duration();

                return Err(Status::new(
                    Code::InvalidArgument,
                    format!("failed to parse document: err = {:?}", e),
                ));
            }
        };
        let id = match doc.get_first(id_field).unwrap().text() {
            Some(id) => id,
            None => {
                timer.observe_duration();

                return Err(Status::new(
                    Code::InvalidArgument,
                    "failed to get document ID value",
                ));
            }
        };

        let index_writer = self.index_writer.lock().await;
        let _opstamp = index_writer.delete_term(Term::from_field_text(id_field, id));
        let _opstamp = index_writer.add_document(doc);

        let reply = SetReply {};

        timer.observe_duration();
        Ok(Response::new(reply))
    }

    async fn delete(&self, request: Request<DeleteReq>) -> Result<Response<DeleteReply>, Status> {
        REQUEST_COUNTER.with_label_values(&["delete"]).inc();
        let timer = REQUEST_HISTOGRAM
            .with_label_values(&["delete"])
            .start_timer();

        let req = request.into_inner();

        let id_field = match self.get_id_field() {
            Some(field) => field,
            None => {
                timer.observe_duration();

                return Err(Status::new(Code::Internal, "the ID field is missing"));
            }
        };

        let term = Term::from_field_text(id_field, &req.id);

        let index_writer = self.index_writer.lock().await;
        let _opstamp = index_writer.delete_term(term);

        let reply = DeleteReply {};

        timer.observe_duration();

        Ok(Response::new(reply))
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

        let id_field = match self.get_id_field() {
            Some(field) => field,
            None => {
                timer.observe_duration();

                return Err(Status::new(Code::Internal, "the ID field is missing"));
            }
        };

        for r in req.requests {
            let doc = match self.index.schema().parse_document(&r.doc) {
                Ok(doc) => doc,
                Err(e) => {
                    timer.observe_duration();

                    return Err(Status::new(
                        Code::InvalidArgument,
                        format!("failed to parse document: err = {:?}", e),
                    ));
                }
            };
            let id = match doc.get_first(id_field).unwrap().text() {
                Some(id) => id,
                None => {
                    timer.observe_duration();

                    return Err(Status::new(
                        Code::InvalidArgument,
                        "failed to get document ID value",
                    ));
                }
            };

            let index_writer = self.index_writer.lock().await;
            let _opstamp = index_writer.delete_term(Term::from_field_text(id_field, id));
            let _opstamp = index_writer.add_document(doc);
        }

        let reply = BulkSetReply {};

        timer.observe_duration();

        Ok(Response::new(reply))
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

        let id_field = match self.get_id_field() {
            Some(field) => field,
            None => {
                timer.observe_duration();

                return Err(Status::new(Code::Internal, "the ID field is missing"));
            }
        };

        for r in req.requests {
            let term = Term::from_field_text(id_field, &r.id);

            let index_writer = self.index_writer.lock().await;
            let _opstamp = index_writer.delete_term(term);
        }

        let reply = BulkDeleteReply {};

        timer.observe_duration();

        Ok(Response::new(reply))
    }

    async fn commit(&self, _request: Request<CommitReq>) -> Result<Response<CommitReply>, Status> {
        REQUEST_COUNTER.with_label_values(&["commit"]).inc();
        let timer = REQUEST_HISTOGRAM
            .with_label_values(&["commit"])
            .start_timer();

        let mut index_writer = self.index_writer.lock().await;
        let _opstamp = match index_writer.commit() {
            Ok(opstamp) => opstamp,
            Err(e) => {
                timer.observe_duration();

                return Err(Status::new(
                    Code::Internal,
                    format!("failed to commit index: error = {:?}", e),
                ));
            }
        };

        let discovery = Arc::clone(&self.discovery);
        let mut discovery = discovery.lock().await;
        match discovery
            .get_node(&self.index_name, &self.shard_name, &self.node_name)
            .await
        {
            Ok(result) => match result {
                Some(node_details) => {
                    if node_details.role == Role::Primary as i32 {
                        match self.push_index().await {
                            Ok(_) => (),
                            Err(e) => error!("failed to push index: error:{:?}", e),
                        }
                    }
                }
                None => debug!("the node does not exist"),
            },
            Err(e) => {
                timer.observe_duration();

                return Err(Status::new(
                    Code::Internal,
                    format!("failed to get node: error = {:?}", e),
                ));
            }
        };

        let reply = CommitReply {};

        timer.observe_duration();

        Ok(Response::new(reply))
    }

    async fn rollback(
        &self,
        _request: Request<RollbackReq>,
    ) -> Result<Response<RollbackReply>, Status> {
        REQUEST_COUNTER.with_label_values(&["rollback"]).inc();
        let timer = REQUEST_HISTOGRAM
            .with_label_values(&["rollback"])
            .start_timer();

        let mut index_writer = self.index_writer.lock().await;
        let _opstamp = match index_writer.rollback() {
            Ok(opstamp) => opstamp,
            Err(e) => {
                timer.observe_duration();

                return Err(Status::new(
                    Code::Internal,
                    format!("failed to rollback index: error = {:?}", e),
                ));
            }
        };

        let reply = RollbackReply {};

        timer.observe_duration();

        Ok(Response::new(reply))
    }

    async fn merge(&self, _request: Request<MergeReq>) -> Result<Response<MergeReply>, Status> {
        REQUEST_COUNTER.with_label_values(&["merge"]).inc();
        let timer = REQUEST_HISTOGRAM
            .with_label_values(&["merge"])
            .start_timer();

        let segment_ids = self.index.searchable_segment_ids().unwrap();
        if segment_ids.len() <= 0 {
            debug!("there are no segment files that can be merged");
            let reply = MergeReply {};

            timer.observe_duration();

            return Ok(Response::new(reply));
        }

        let mut index_writer = self.index_writer.lock().await;
        let segment_meta = match index_writer.merge(&segment_ids).await {
            Ok(segment_meta) => segment_meta,
            Err(e) => {
                timer.observe_duration();

                return Err(Status::new(
                    Code::Internal,
                    format!("failed to merge index: error = {:?}", e),
                ));
            }
        };
        debug!("merge index: segment_meta={:?}", segment_meta);

        let discovery = Arc::clone(&self.discovery);
        let mut discovery = discovery.lock().await;
        match discovery
            .get_node(&self.index_name, &self.shard_name, &self.node_name)
            .await
        {
            Ok(result) => match result {
                Some(node_details) => {
                    if node_details.role == Role::Primary as i32 {
                        match self.push_index().await {
                            Ok(_) => (),
                            Err(e) => error!("failed to push index: error:{:?}", e),
                        }
                    }
                }
                None => debug!("the node does not exist"),
            },
            Err(e) => {
                timer.observe_duration();

                return Err(Status::new(
                    Code::Internal,
                    format!("failed to get node: error = {:?}", e),
                ));
            }
        };

        let reply = MergeReply {};

        timer.observe_duration();

        Ok(Response::new(reply))
    }

    async fn push(&self, _request: Request<PushReq>) -> Result<Response<PushReply>, Status> {
        REQUEST_COUNTER.with_label_values(&["push"]).inc();
        let timer = REQUEST_HISTOGRAM.with_label_values(&["push"]).start_timer();

        match self.push_index().await {
            Ok(_) => (),
            Err(e) => {
                timer.observe_duration();

                return Err(Status::new(
                    Code::Internal,
                    format!("failed to set object: {:?}", e),
                ));
            }
        };

        let reply = PushReply {};

        timer.observe_duration();

        Ok(Response::new(reply))
    }

    async fn pull(&self, _request: Request<PullReq>) -> Result<Response<PullReply>, Status> {
        REQUEST_COUNTER.with_label_values(&["pull"]).inc();
        let timer = REQUEST_HISTOGRAM.with_label_values(&["pull"]).start_timer();

        match self
            .storage
            .pull_index(&self.index_name, &self.shard_name)
            .await
        {
            Ok(_) => (),
            Err(e) => {
                timer.observe_duration();

                return Err(Status::new(
                    Code::Internal,
                    format!("failed to remove unnecessary files: error = {:?}", e),
                ));
            }
        };

        let reply = PullReply {};

        timer.observe_duration();

        Ok(Response::new(reply))
    }

    async fn schema(&self, _request: Request<SchemaReq>) -> Result<Response<SchemaReply>, Status> {
        REQUEST_COUNTER.with_label_values(&["schema"]).inc();
        let timer = REQUEST_HISTOGRAM
            .with_label_values(&["schema"])
            .start_timer();

        let schema = match serde_json::to_string(&self.index.schema()) {
            Ok(schema) => schema,
            Err(e) => {
                timer.observe_duration();

                return Err(Status::new(
                    Code::Internal,
                    format!("failed to get schema: error = {:?}", e),
                ));
            }
        };

        let reply = SchemaReply { schema };

        timer.observe_duration();

        Ok(Response::new(reply))
    }

    async fn search(&self, request: Request<SearchReq>) -> Result<Response<SearchReply>, Status> {
        REQUEST_COUNTER.with_label_values(&["search"]).inc();
        let timer = REQUEST_HISTOGRAM
            .with_label_values(&["search"])
            .start_timer();

        let req = request.into_inner();

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

        let limit = &req.from + &req.limit;

        let query_parser = QueryParser::for_index(&self.index, default_search_fields);
        let query = query_parser.parse_query(&req.query).unwrap();

        let mut multi_collector = MultiCollector::new();
        let count_handle = if req.exclude_count {
            None
        } else {
            Some(multi_collector.add_collector(Count))
        };
        let top_docs_handle = if req.exclude_docs {
            None
        } else {
            Some(multi_collector.add_collector(TopDocs::with_limit(limit as usize)))
        };
        let facet_handle = if req.facet_field.is_empty() {
            None
        } else {
            let mut facet_collector =
                FacetCollector::for_field(self.index.schema().get_field(&req.facet_field).unwrap());
            for facet_prefix in &req.facet_prefixes {
                facet_collector.add_facet(facet_prefix);
            }
            Some(multi_collector.add_collector(facet_collector))
        };

        // search index
        let searcher = self.index.reader().unwrap().searcher();
        let result = match searcher.search(&query, &multi_collector) {
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
                    for facet_prefix in &req.facet_prefixes {
                        for (facet_key, facet_value) in facet_counts.get(facet_prefix) {
                            facet_kv.insert(facet_key.to_string(), facet_value);
                        }
                    }
                    facet.insert(String::from(&req.facet_field), facet_kv);
                }

                // docs
                let mut docs: Vec<ScoredNamedFieldDocument> = Vec::new();
                let mut doc_pos: u64 = 0;
                for (score, doc_address) in top_docs {
                    if doc_pos >= req.from as u64 {
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
                let search_result = SearchResult { docs, count, facet };

                serde_json::to_string(&search_result).unwrap()
            }
            Err(e) => {
                timer.observe_duration();

                return Err(Status::new(
                    Code::Internal,
                    format!("failed to search documents: error = {:?}", e),
                ));
            }
        };

        let reply = SearchReply { result };

        timer.observe_duration();

        Ok(Response::new(reply))
    }
}
