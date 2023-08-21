//! Document representation

use std::collections::{HashMap, HashSet};

#[derive(Clone)]
pub struct VariantMetadata {
    /// The variant size in bytes.
    size: u64,
    /// The variant mime type.
    /// TODO: Consider using a mime specific type.
    mime_type: String,
}

impl VariantMetadata {
    fn new(size: u64, mime_type: &str) -> Self {
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

#[derive(Clone)]
pub struct DocumentMetadata {
    /// The document name. This can be different from the file leaf
    /// name in the FS, and is indexed by default for FTS.
    name: String,
    /// The set of variants for this document, keyed by the variant name.
    /// The 'default' variant is always present.
    variants: HashMap<String, VariantMetadata>,
    /// The set of tags for this document.
    tags: HashSet<String>,
}

impl DocumentMetadata {
    fn new(name: &str, default_variant: &VariantMetadata, tags: HashSet<String>) -> Self {
        let mut variants = HashMap::new();
        variants.insert("default".to_owned(), (*default_variant).clone());

        Self {
            name: name.to_owned(),
            variants,
            tags,
        }
    }
}
