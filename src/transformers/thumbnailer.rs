use super::TransformedVariant;
/// Thumbnailer transformer.
use crate::resource::{ContentReader, VariantMetadata};
use crate::transformers::{
    TransformedContent, TransformerResult, VariantChange, VariantTransformer,
};
use async_trait::async_trait;
use futures::{AsyncReadExt, AsyncSeekExt};
use image::io::Reader as ImageReader;
use log::{error, info};
use std::io::{Cursor, SeekFrom};
use tokio_util::compat::TokioAsyncReadCompatExt;

const DEFAULT_THUMBNAIL_SIZE: u32 = 128;

pub struct Thumbnailer {
    size: u32, // The size (max width & height) of the thumbnail
}

impl Default for Thumbnailer {
    fn default() -> Self {
        Self {
            size: DEFAULT_THUMBNAIL_SIZE,
        }
    }
}

fn err_nop<T: std::error::Error>(e: T) -> () {
    error!("Unexpected: {:?}", e);
    ()
}

async fn create_thumbnail<C: ContentReader>(
    content: &mut C,
    thumbnail_size: u32,
) -> Result<TransformedVariant, ()> {
    content.seek(SeekFrom::Start(0)).await.map_err(err_nop)?;
    let mut buffer = vec![];
    content.read_to_end(&mut buffer).await.map_err(err_nop)?;
    content.seek(SeekFrom::Start(0)).await.map_err(err_nop)?;

    info!("Image size is {}b", buffer.len());
    let img = ImageReader::new(Cursor::new(buffer))
        .with_guessed_format()
        .map_err(err_nop)?
        .decode()
        .map_err(err_nop)?;

    info!(
        "Creating {}x{} thumbnail for image {}x{}",
        thumbnail_size,
        thumbnail_size,
        img.width(),
        img.height(),
    );

    let thumbnail = img.thumbnail(thumbnail_size, thumbnail_size);

    let mut bytes: Vec<u8> = Vec::new();
    thumbnail
        .write_to(
            &mut Cursor::new(&mut bytes),
            image::ImageOutputFormat::Jpeg(90),
        )
        .map_err(err_nop)?;

    let v = TransformedVariant::new(
        "thumbnail",
        &VariantMetadata::new(bytes.len() as _, "image/jpeg"),
        TransformedContent::new(Box::new(Cursor::new(bytes).compat())),
    );

    Ok(v)
}

#[async_trait(?Send)]
impl VariantTransformer for Thumbnailer {
    async fn transform_variant<C: ContentReader>(
        &self,
        change: &mut VariantChange,
        content: &mut C,
    ) -> Vec<TransformerResult> {
        let meta = &change.metadata();

        // Only process variants of image/*  mime type.
        if !meta.mime_type().starts_with("image/") {
            return vec![];
        }

        if change.is_deleted() {
            return vec![TransformerResult::Delete("thumbnail".into())];
        }

        info!(
            "Will create thumbnail for variant with mimeType '{}'",
            meta.mime_type()
        );
        let res = {
            // Return a new variant.
            if let Ok(v) = create_thumbnail(content, self.size).await {
                match change {
                    VariantChange::Created(_) => {
                        info!("Thumbnail variant created");
                        vec![TransformerResult::Create(v)]
                    }
                    VariantChange::Updated(_) => {
                        info!("Thumbnail variant updated");
                        vec![TransformerResult::Update(v)]
                    }
                    _ => panic!("Unexpected variant change!"),
                }
            } else {
                vec![]
            }
        };

        res
    }
}
