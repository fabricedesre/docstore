//! Variant transformers: code that runs when we create,
//! update or delete default variants.

use self::thumbnailer::Thumbnailer;
use crate::resource::{ContentReader, VariantMetadata};
use async_trait::async_trait;
use futures::io::AsyncSeek;
use futures::task::{Context, Poll};
use futures::AsyncRead;
use std::pin::Pin;

pub mod thumbnailer;

/// A wrapper holding the returned content for a variant
/// transform.
/// This let us implement the ContentReader trait.
pub struct TransformedContent {
    inner: Box<dyn ContentReader>,
}

impl TransformedContent {
    fn new(inner: Box<dyn ContentReader>) -> Self {
        Self { inner }
    }
}

impl AsyncRead for TransformedContent {
    fn poll_read(
        mut self: Pin<&mut Self>,
        ctxt: &mut Context<'_>,
        param: &mut [u8],
    ) -> Poll<std::result::Result<usize, std::io::Error>> {
        Pin::new(&mut self.inner).poll_read(ctxt, param)
    }
}

impl AsyncSeek for TransformedContent {
    fn poll_seek(
        mut self: Pin<&mut Self>,
        ctxt: &mut Context<'_>,
        from: std::io::SeekFrom,
    ) -> Poll<std::result::Result<u64, std::io::Error>> {
        Pin::new(&mut self.inner).poll_seek(ctxt, from)
    }
}

impl ContentReader for TransformedContent {}

pub struct TransformedVariant {
    pub(crate) name: String, // The variant name.
    pub(crate) meta: VariantMetadata,
    pub(crate) content: TransformedContent,
}

impl TransformedVariant {
    pub fn new(name: &str, meta: &VariantMetadata, content: TransformedContent) -> Self {
        Self {
            name: name.into(),
            meta: meta.clone(),
            content,
        }
    }
}

pub enum VariantChange {
    Created(VariantMetadata),
    Updated(VariantMetadata),
    Deleted(VariantMetadata),
}

impl VariantChange {
    pub fn is_created(&self) -> bool {
        matches!(self, Self::Created(_))
    }

    pub fn is_updated(&self) -> bool {
        matches!(self, Self::Updated(_))
    }

    pub fn is_deleted(&self) -> bool {
        matches!(self, Self::Deleted(_))
    }

    pub fn metadata(&self) -> VariantMetadata {
        match &self {
            VariantChange::Created(v) | VariantChange::Updated(v) | VariantChange::Deleted(v) => {
                v.clone()
            }
        }
    }
}

pub enum TransformerResult {
    Delete(String), // the variant name.
    Create(TransformedVariant),
    Update(TransformedVariant),
}

#[async_trait(?Send)]
pub trait VariantTransformer {
    async fn transform_variant<C: ContentReader>(
        &self,
        change: &mut VariantChange,
        content: &mut C,
    ) -> Vec<TransformerResult>;
}

pub async fn run_transformers<C: ContentReader>(
    change: &mut VariantChange,
    content: &mut C,
) -> Vec<TransformerResult> {
    let thumbnailer = Thumbnailer::default();
    thumbnailer.transform_variant(change, content).await
}
