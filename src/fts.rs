//! Full text indexers
//! Indexers are registered for a given mime type.

use futures::{AsyncRead, AsyncReadExt};
use serde_json::Value;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum IndexerError {
    #[error("Indexer Error: {0}")]
    IndexingFailed(String),
    #[error("Unsupported mime type: {0}")]
    UnsupportedMime(String),
    #[error("I/O error")]
    IO(#[from] std::io::Error),
    #[error("serde Json error")]
    SerdeJson(#[from] serde_json::Error),
}

/// text/plain indexer: read all the content available.
pub async fn text_plain_indexer<C: AsyncRead + Unpin>(
    content: &mut C,
) -> Result<String, IndexerError> {
    let mut text = String::new();
    content.read_to_string(&mut text).await?;
    Ok(text)
}

/// A generic indexer for flat Json data structures.
/// Indexed properties are strings and string arrays members.

/// Indexing function, taking the property name and value as input,
/// returning the string to add to the full text index instead of the
/// raw property value.
type JsonCustomIndex = dyn Fn(&str, &str) -> Vec<String> + Send + Sync;

pub struct FlatJsonIndexer {
    fields: Vec<String>,
    #[allow(clippy::type_complexity)]
    custom_fn: Option<Box<JsonCustomIndex>>,
}

impl FlatJsonIndexer {
    #[allow(clippy::type_complexity)]
    pub fn new(fields: &[&str], custom_fn: Option<Box<JsonCustomIndex>>) -> Self {
        Self {
            fields: fields.iter().map(|e| (*e).to_owned()).collect(),
            custom_fn,
        }
    }

    fn maybe_index(&self, field: &str, value: &str, current: &mut Vec<String>) {
        if let Some(func) = &self.custom_fn {
            for item in func(field, value) {
                current.push(item.to_owned());
            }
        } else {
            current.push(value.to_owned());
        }
    }

    pub async fn get_text<C: AsyncRead + Unpin>(
        &self,
        content: &mut C,
    ) -> Result<String, IndexerError> {
        let mut result: Vec<String> = vec![];

        // 1. Read the content as json.
        let mut buffer = vec![];
        content.read_to_end(&mut buffer).await?;
        let v: Value = serde_json::from_slice(&buffer)?;

        // 2. Index each available field.
        for field in &self.fields {
            match v.get(field) {
                Some(Value::String(text)) => {
                    self.maybe_index(field, text, &mut result);
                }
                Some(Value::Array(array)) => {
                    for item in array {
                        if let Value::String(text) = item {
                            self.maybe_index(field, text, &mut result);
                        }
                    }
                }
                _ => {}
            }
        }

        Ok(result.join(" "))
    }
}

/// Indexer for the content of a "Places" object.
/// This is a json value with the following format:
/// { url: "...", title: "...", icon: "..." }
pub fn new_places_indexer() -> FlatJsonIndexer {
    FlatJsonIndexer::new(&["url", "title"], None)
}

pub async fn json_indexer<C: AsyncRead + Unpin>(
    content: &mut C,
    mime: &str,
) -> Result<String, IndexerError> {
    let json_indexer = match mime {
        "application/x-places+json" => new_places_indexer(),
        _ => return Err(IndexerError::UnsupportedMime(mime.to_owned())),
    };
    json_indexer.get_text(content).await
}
