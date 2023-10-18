//! Resource representation

use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

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
}