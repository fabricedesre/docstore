//! Resource representation

use futures::io::AsyncSeek;
use futures::AsyncRead;
use rusqlite::types::{FromSql, FromSqlError, ToSqlOutput, ValueRef};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use tokio_util::compat::Compat;

pub trait ContentReader: AsyncRead + AsyncSeek + Unpin {}

// Default implementations for types used internally
impl<T: AsRef<[u8]> + Unpin> ContentReader for Compat<std::io::Cursor<T>> {}
impl <T: ContentReader> ContentReader for Box<T> {}
impl ContentReader for Compat<tokio::fs::File> {}

/// Type used to represent a unique id for a resource.
/// Currently using the resource path.
#[derive(Clone, Debug)]
pub struct ResourceId(String);

impl From<&[String]> for ResourceId {
    fn from(value: &[String]) -> Self {
        Self(value.join("/"))
    }
}

impl From<ResourceId> for Vec<String> {
    fn from(val: ResourceId) -> Self {
        val.0.split('/').map(|s| s.to_owned()).collect()
    }
}

impl rusqlite::ToSql for ResourceId {
    fn to_sql(&self) -> Result<ToSqlOutput<'_>, rusqlite::Error> {
        Ok(self.0.clone().into())
    }
}

impl FromSql for ResourceId {
    fn column_result(value: ValueRef<'_>) -> Result<Self, FromSqlError> {
        if let ValueRef::Text(text) = value {
            Ok(Self(String::from_utf8_lossy(text).into()))
        } else {
            Err(FromSqlError::InvalidType)
        }
    }
}

impl ToString for ResourceId {
    fn to_string(&self) -> String {
        self.0.clone()
    }
}

#[derive(Clone, Deserialize, Serialize)]
pub struct VariantMetadata {
    /// The variant size in bytes.
    size: u64,
    /// The variant mime type.
    /// TODO: Consider using a mime specific type.
    mime_type: String,
}

impl VariantMetadata {
    pub fn new(size: u64, mime_type: &str) -> Self {
        Self {
            size,
            mime_type: mime_type.to_owned(),
        }
    }

    pub fn set_size(&mut self, size: u64) {
        self.size = size;
    }

    pub fn size(&self) -> u64 {
        self.size
    }

    pub fn mime_type(&self) -> String {
        self.mime_type.to_owned()
    }

    pub fn set_mime_type(&mut self, mime_type: &str) {
        self.mime_type = mime_type.to_owned();
    }
}

#[derive(Clone, Deserialize, Serialize)]
pub struct ResourceMetadata {
    /// A short description for the resource. This can be different from the file leaf
    /// name in the FS, and is indexed by default for FTS.
    desc: String,
    /// The set of variants for this resource, keyed by the variant name.
    /// The 'default' variant is always present.
    variants: HashMap<String, VariantMetadata>,
    /// The set of tags for this resource.
    tags: HashSet<String>,
}

impl ResourceMetadata {
    pub fn new(desc: &str, default_variant: &VariantMetadata, tags: HashSet<String>) -> Self {
        let mut variants = HashMap::new();
        variants.insert("default".to_owned(), (*default_variant).clone());

        Self {
            desc: desc.to_owned(),
            variants,
            tags,
        }
    }

    pub fn set_desc(&mut self, desc: &str) {
        self.desc = desc.to_owned();
    }

    pub fn desc(&self) -> String {
        self.desc.to_owned()
    }

    pub fn get_variant(&self, name: &str) -> Option<&VariantMetadata> {
        self.variants.get(name)
    }

    pub fn has_variant(&self, name: &str) -> bool {
        self.variants.contains_key(name)
    }

    pub fn add_variant(&mut self, name: &str, variant: &VariantMetadata) {
        self.variants.insert(name.to_owned(), (*variant).clone());
    }

    pub fn remove_variant(&mut self, name: &str) -> bool {
        self.variants.remove(name).is_some()
    }

    pub fn add_tag(&mut self, tag: &str) {
        self.tags.insert(tag.into());
    }

    pub fn remove_tag(&mut self, tag: &str) {
        self.tags.remove(tag);
    }

    pub fn tags(&self) -> &HashSet<String> {
        &self.tags
    }

    pub fn variants(&self) -> &HashMap<String, VariantMetadata> {
        &self.variants
    }
}
