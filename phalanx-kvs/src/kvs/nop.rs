use std::error::Error;
use std::io::Error as IOError;

use async_trait::async_trait;
use crossbeam::channel::Sender;
use serde::{Deserialize, Serialize};

use crate::kvs::{Event, KeyValuePair, KeyValueStore};

pub const TYPE: &str = "nop";

#[derive(Clone, Serialize, Deserialize)]
pub struct NopConfig {}

#[derive(Clone)]
pub struct Nop {}

impl Nop {
    pub fn new() -> Nop {
        Nop {}
    }
}

#[async_trait]
impl KeyValueStore for Nop {
    fn get_type(&self) -> &str {
        TYPE
    }

    fn export_config_json(&self) -> Result<String, IOError> {
        Ok("{}".to_string())
    }

    async fn get(&mut self, _key: &str) -> Result<Option<Vec<u8>>, Box<dyn Error + Send + Sync>> {
        Ok(None)
    }

    async fn list(
        &mut self,
        _prefix: &str,
    ) -> Result<Vec<KeyValuePair>, Box<dyn Error + Send + Sync>> {
        Ok(Vec::new())
    }

    async fn put(
        &mut self,
        _key: &str,
        _value: Vec<u8>,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        Ok(())
    }

    async fn delete(&mut self, _key: &str) -> Result<(), Box<dyn Error + Send + Sync>> {
        Ok(())
    }

    async fn watch(
        &mut self,
        _sender: Sender<Event>,
        _key: &str,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        Ok(())
    }

    async fn unwatch(&mut self) -> Result<(), Box<dyn Error + Send + Sync>> {
        Ok(())
    }
}
