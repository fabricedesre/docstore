//! Full text indexers
//! Indexers are registered for a given mime type.

use futures::{AsyncRead, AsyncReadExt};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum IndexerError {
    #[error("Indexer Error: {0}")]
    IndexingFailed(String),
    #[error("I/O error")]
    IO(#[from] std::io::Error),
}

// text/plain indexer: read all the content available.
pub async fn text_plain_indexer<C: AsyncRead + Unpin>(
    content: &mut C,
) -> Result<String, IndexerError> {
    let mut text = String::new();
    content.read_to_string(&mut text).await?;
    Ok(text)
}
