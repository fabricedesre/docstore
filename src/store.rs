//! Private resources store api

use crate::indexer::{Indexer, SqliteDbError};
use crate::resource::{ContentReader, ResourceId, VariantMetadata};
use crate::transformers::{run_transformers, TransformerResult, VariantChange};
use crate::{file_store::FileStore, resource::ResourceMetadata};
use async_stream::stream;
use chrono::Utc;
use futures::stream::LocalBoxStream;
use libipld::Cid;
use log::debug;
use rand::{rngs::ThreadRng, thread_rng};
use serde::{de::DeserializeOwned, Serialize};
use std::collections::HashSet;
use std::ffi::OsStr;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use thiserror::Error;
use tokio::fs;
use tokio_util::compat::TokioAsyncReadCompatExt;
use wnfs::{
    common::BlockStore,
    nameaccumulator::AccumulatorSetup,
    private::{
        forest::{hamt::HamtForest, traits::PrivateForest},
        AccessKey, PrivateDirectory, PrivateFile, PrivateForestContent, PrivateNode,
    },
    rand_core::CryptoRngCore,
};

#[derive(Error, Debug)]
pub enum StoreError {
    #[error("No such resource in store: {0:?}")]
    NoSuchResource(Vec<String>),
    #[error("Invalid variant name: {0}")]
    InvalidVariant(String),
    #[error("No variant '{0}' for this resource: {1:?}")]
    NoSuchVariant(String, Vec<String>),
    #[error("No content found for the '{0}' variant for {1:?}")]
    NoVariantContent(String, Vec<String>),
    #[error("No metadata found for this resource: {0:?}")]
    NoResourceMetadata(Vec<String>),
    #[error("I/O error")]
    IO(#[from] std::io::Error),
    #[error("serde_cbor error")]
    SerdeCBOR(#[from] serde_cbor::Error),
    #[error("IPLD error")]
    IPLD(#[from] libipld::error::Error),
    #[error("SQlite error")]
    Sqlite(#[from] SqliteDbError),
}

type Result<T> = std::result::Result<T, StoreError>;
type IpldResult<T> = std::result::Result<T, libipld::error::Error>;

// Deserialize cbor from a file to an arbitrary type.
async fn from_cbor<T, P: AsRef<Path>>(path: P) -> Result<T>
where
    T: DeserializeOwned,
{
    match fs::read(path).await {
        Ok(bytes) => {
            let res = serde_cbor::from_slice(&bytes)?;
            Ok(res)
        }
        Err(e) => Err(e.into()),
    }
}

// Serialize an object as cbor to a file
async fn to_cbor<T, P: AsRef<Path>>(path: P, value: T) -> Result<()>
where
    T: Serialize,
{
    fs::write(path, serde_cbor::to_vec(&value)?).await?;
    Ok(())
}

fn subpath<P: AsRef<Path>>(root: P, leaf: &str) -> PathBuf {
    let mut path: PathBuf = root.as_ref().into();
    path.push(leaf);
    path
}

pub struct ResourceStore {
    forest: HamtForest,
    block_store: FileStore,
    access_key: AccessKey,
    rng: ThreadRng,
    root_dir: PathBuf,
    indexer: Indexer,
}

impl ResourceStore {
    async fn init_forest<P: AsRef<Path>>(
        root_dir: P,
        store: &impl BlockStore,
        rng: &mut impl CryptoRngCore,
    ) -> Result<(Cid, AccessKey)> {
        debug!("Initializing a new forest");
        let setup = AccumulatorSetup::trusted(rng);
        let forest = &mut Rc::new(HamtForest::new(setup));
        let dir = &mut Rc::new(PrivateDirectory::new(&forest.empty_name(), Utc::now(), rng));
        let access_key = dir.as_node().store(forest, store, rng).await?;
        let forest_cid = forest.store(store).await?;

        // Save the initial access key.
        to_cbor(subpath(&root_dir, "access.key"), &access_key).await?;

        Ok((forest_cid, access_key))
    }

    /// Create a new store, with all the data stored under the root dir.
    /// The root directory and required sub directories will be created
    /// if they don't already exist.
    pub async fn new<P: AsRef<Path>>(root_dir: P) -> Result<Self> {
        if !root_dir.as_ref().exists() {
            fs::create_dir(&root_dir).await?;
        }

        let block_store = FileStore::maybe_new(subpath(&root_dir, "blockstore")).await?;

        let mut rng = thread_rng();
        // Initialize the forest and access key from serialized ones if possible.
        let (forest_cid, access_key) = match (
            from_cbor(subpath(&root_dir, "forest.cid")).await,
            from_cbor(subpath(&root_dir, "access.key")).await,
        ) {
            (Ok(cid), Ok(access_key)) => {
                debug!("Using existing access key");
                (cid, access_key)
            }
            _ => ResourceStore::init_forest(&root_dir, &block_store, &mut rng).await?,
        };

        let forest = HamtForest::load(&forest_cid, &block_store).await?;

        let indexer = Indexer::new(root_dir.as_ref().to_path_buf(), "index.sqlite")?;

        let mut store = Self {
            forest,
            block_store,
            access_key,
            rng,
            root_dir: root_dir.as_ref().into(),
            indexer,
        };

        store.mkdir(&[".resources".to_owned()]).await?;
        store.mkdir(&[".index".to_owned()]).await?;

        Ok(store)
    }

    /// Get a handle to the root of the file system.
    pub async fn root(&self) -> Result<Rc<PrivateDirectory>> {
        let root = PrivateNode::load(&self.access_key, &self.forest, &self.block_store, None)
            .await?
            .search_latest(&self.forest, &self.block_store)
            .await?;

        Ok(root.as_dir()?)
    }

    /// Get a handle to a sub directory in the file system.
    async fn subdir(&self, path: &[String]) -> Result<Rc<PrivateDirectory>> {
        match self
            .root()
            .await?
            .get_node(path, true, &self.forest, &self.block_store)
            .await?
        {
            Some(PrivateNode::Dir(dir)) => Ok(dir),
            _ => Err(StoreError::NoSuchResource(path.to_vec())),
        }
    }

    /// Get a handle to the resources subdirectory of the file system.
    pub async fn resources_dir(&self) -> Result<Rc<PrivateDirectory>> {
        self.subdir(&[".resources".to_owned()]).await
    }

    async fn index_dir(&self) -> Result<Rc<PrivateDirectory>> {
        self.subdir(&[".index".to_owned()]).await
    }

    /// Create a new directory, starting the path from the root.
    pub async fn mkdir(&mut self, path: &[String]) -> Result<()> {
        let mut root = PrivateNode::load(&self.access_key, &self.forest, &self.block_store, None)
            .await?
            .search_latest(&self.forest, &self.block_store)
            .await?;

        let root = root.as_dir_mut()?;

        root.mkdir(
            path,
            true,
            Utc::now(),
            &self.forest,
            &self.block_store,
            &mut self.rng,
        )
        .await?;

        root.as_node()
            .store(&mut self.forest, &self.block_store, &mut self.rng)
            .await?;

        self.save_state().await
    }

    async fn save_state(&mut self) -> Result<()> {
        if self.indexer.should_update() {
            // Update <root_dir>/index.sqlite to .index/index.sqlite
            let mut dir = self.index_dir().await?;
            let dir_name = dir.header.get_name().clone();
            let now = Utc::now();
            let file = dir
                .open_file_mut(
                    &["index.sqlite".to_owned()],
                    true,
                    now,
                    &mut self.forest,
                    &self.block_store,
                    &mut self.rng,
                )
                .await?;
            let mut full_path = self.root_dir.clone();
            full_path.push("index.sqlite");
            let reader = fs::File::open(full_path).await?;
            let source = PrivateFile::with_content_streaming(
                &dir_name,
                now,
                reader.compat(),
                &mut self.forest,
                &self.block_store,
                &mut self.rng,
            )
            .await?;

            file.copy_content_from(&source, now);

            dir.as_node()
                .store(&mut self.forest, &self.block_store, &mut self.rng)
                .await?;

            self.indexer.set_updated();
        }

        to_cbor(
            subpath(&self.root_dir, "forest.cid"),
            self.forest.store(&self.block_store).await?,
        )
        .await
    }

    /// Returns the private file at this path if it exists.
    async fn maybe_file(&self, path: &[String]) -> Result<Rc<PrivateFile>> {
        match self
            .resources_dir()
            .await?
            .get_node(path, true, &self.forest, &self.block_store)
            .await?
        {
            Some(PrivateNode::File(file)) => Ok(file),
            _ => Err(StoreError::NoSuchResource(path.to_vec())),
        }
    }

    /// Apply the output of variant transformers for this resource.
    pub async fn apply_variant_transforms(
        &mut self,
        path: &[String],
        transforms: Vec<TransformerResult>,
    ) -> Result<()> {
        for transform in transforms {
            match transform {
                TransformerResult::Delete(variant_name) => {
                    self.delete_variant(path, &variant_name).await?
                }
                TransformerResult::Create(variant) => {
                    if variant.name != "default" {
                        self.add_variant(path, &variant.name, &variant.meta, variant.content)
                            .await?
                    }
                }
                TransformerResult::Update(variant) => {
                    if variant.name != "default" {
                        self.update_variant(path, &variant.name, &variant.meta, variant.content)
                            .await?
                    }
                }
            }
        }
        Ok(())
    }

    /// Add a resource with a default variant content.
    pub async fn create_resource(
        &mut self,
        path: &[String],
        desc: &str,
        default_variant: &VariantMetadata,
        tags: HashSet<String>,
        mut content: impl ContentReader,
    ) -> Result<()> {
        let mut dir = self.resources_dir().await?;
        let now = Utc::now();

        // Create the resource metadata.
        let resource_metadata = ResourceMetadata::new(desc, default_variant, tags.clone());

        let id = path.into();
        self.indexer.add_resource(&id)?;
        for tag in tags {
            self.indexer.add_tag(&id, &tag)?;
        }
        self.indexer.add_text(&id, "default", desc)?;
        self.indexer
            .add_variant(&id, "default", default_variant, &mut content)
            .await?;

        // Collect the results from the variant transformers.
        let mut variant_change = VariantChange::Created(default_variant.clone());
        let transformer_results = run_transformers(&mut variant_change, &mut content).await;

        let dir_name = dir.header.get_name().clone();
        let file = dir
            .open_file_mut(
                path,
                false,
                now,
                &mut self.forest,
                &self.block_store,
                &mut self.rng,
            )
            .await?;

        let source = PrivateFile::with_content_streaming(
            &dir_name,
            now,
            content,
            &mut self.forest,
            &self.block_store,
            &mut self.rng,
        )
        .await?;

        file.copy_content_from(&source, now);

        // Set the resource metadata
        let node_metadata = file.get_metadata_mut();
        node_metadata.put_serializable("res_meta", resource_metadata)?;

        dir.as_node()
            .store(&mut self.forest, &self.block_store, &mut self.rng)
            .await?;

        // Apply the variant transformers. This needs to be done after the
        // resource is fully created.
        self.apply_variant_transforms(path, transformer_results)
            .await?;

        self.save_state().await
    }

    /// Add a variant to an existing resource.
    pub async fn add_variant(
        &mut self,
        path: &[String],
        variant_name: &str,
        variant: &VariantMetadata,
        mut content: impl ContentReader,
    ) -> Result<()> {
        if variant_name == "default" {
            return Err(StoreError::InvalidVariant(variant_name.to_owned()));
        }

        let mut dir = self.resources_dir().await?;
        let file = dir
            .open_file_mut(
                path,
                true,
                Utc::now(),
                &mut self.forest,
                &self.block_store,
                &mut self.rng,
            )
            .await?;

        let file_name = file.header.get_name().clone();
        let file_metadata = file.get_metadata_mut();
        let maybe_resource_metadata: Option<IpldResult<ResourceMetadata>> =
            file_metadata.get_deserializable("res_meta");
        if let Some(Ok(mut resource_metadata)) = maybe_resource_metadata {
            resource_metadata.add_variant(variant_name, variant);
            file_metadata.put_serializable("res_meta", resource_metadata)?;

            self.indexer
                .add_variant(&path.into(), variant_name, variant, &mut content)
                .await?;

            let variant_content = PrivateForestContent::new_streaming(
                &file_name,
                content,
                &mut self.forest,
                &self.block_store,
                &mut self.rng,
            )
            .await?;

            file_metadata.put(
                &format!("{}_variant", variant_name),
                variant_content.as_metadata_value()?,
            );

            dir.as_node()
                .store(&mut self.forest, &self.block_store, &mut self.rng)
                .await?;

            self.save_state().await
        } else {
            Err(StoreError::NoResourceMetadata(path.to_vec()))
        }
    }

    /// Update a variant of an existing resource.
    pub async fn update_variant(
        &mut self,
        path: &[String],
        variant_name: &str,
        variant: &VariantMetadata,
        mut content: impl ContentReader,
    ) -> Result<()> {
        let mut dir = self.resources_dir().await?;
        let dir_name = dir.header.get_name().clone();
        let file = dir
            .open_file_mut(
                path,
                true,
                Utc::now(),
                &mut self.forest,
                &self.block_store,
                &mut self.rng,
            )
            .await?;

        if variant_name == "default" {
            let now = Utc::now();

            self.indexer
                .update_variant(&path.into(), variant_name, variant, &mut content)
                .await?;

            // Special case for the default variant, updating the main file content.
            let source = PrivateFile::with_content_streaming(
                &dir_name,
                now,
                content,
                &mut self.forest,
                &self.block_store,
                &mut self.rng,
            )
            .await?;

            file.copy_content_from(&source, now);

            dir.as_node()
                .store(&mut self.forest, &self.block_store, &mut self.rng)
                .await?;

            return self.save_state().await;
        }

        let file_name = file.header.get_name().clone();
        let file_metadata = file.get_metadata_mut();
        let maybe_resource_metadata: Option<IpldResult<ResourceMetadata>> =
            file_metadata.get_deserializable("res_meta");
        if let Some(Ok(mut resource_metadata)) = maybe_resource_metadata {
            resource_metadata.add_variant(variant_name, variant);
            file_metadata.put_serializable("res_meta", resource_metadata)?;

            self.indexer
                .update_variant(&path.into(), variant_name, variant, &mut content)
                .await?;

            let variant_content = PrivateForestContent::new_streaming(
                &file_name,
                content,
                &mut self.forest,
                &self.block_store,
                &mut self.rng,
            )
            .await?;

            file_metadata.put(
                &format!("{}_variant", variant_name),
                variant_content.as_metadata_value()?,
            );

            dir.as_node()
                .store(&mut self.forest, &self.block_store, &mut self.rng)
                .await?;

            self.save_state().await
        } else {
            Err(StoreError::NoResourceMetadata(path.to_vec()))
        }
    }

    /// Deletes a single variant from an existing resource.
    pub async fn delete_variant(&mut self, path: &[String], variant_name: &str) -> Result<()> {
        // Deleting the default variant is not allowed.
        if variant_name == "default" {
            return Err(StoreError::InvalidVariant(variant_name.to_owned()));
        }

        let mut dir = self.resources_dir().await?;

        let file = dir
            .open_file_mut(
                path,
                true,
                Utc::now(),
                &mut self.forest,
                &self.block_store,
                &mut self.rng,
            )
            .await?;

        let file_metadata = file.get_metadata_mut();

        // Get the private forest content for that resource.
        let maybe_resource_metadata: Option<IpldResult<ResourceMetadata>> =
            file_metadata.get_deserializable("res_meta");
        if let Some(Ok(mut resource_metadata)) = maybe_resource_metadata {
            if !resource_metadata.has_variant(variant_name) {
                return Err(StoreError::NoSuchVariant(
                    variant_name.to_owned(),
                    path.to_vec(),
                ));
            }

            resource_metadata.remove_variant(variant_name);
            file_metadata.put_serializable("res_meta", resource_metadata)?;

            let _ = file_metadata.delete(&format!("{}_variant", variant_name));
        } else {
            return Err(StoreError::NoResourceMetadata(path.to_vec()));
        }

        dir.as_node()
            .store(&mut self.forest, &self.block_store, &mut self.rng)
            .await?;

        self.indexer.delete_variant(&path.into(), variant_name)?;

        self.save_state().await
    }

    /// Removes a resource and all its variants from the store.
    pub async fn delete_resource(&mut self, path: &[String]) -> Result<()> {
        let mut dir = self.resources_dir().await?;

        dir.rm(path, true, &self.forest, &self.block_store).await?;
        dir.as_node()
            .store(&mut self.forest, &self.block_store, &mut self.rng)
            .await?;

        self.indexer.delete_resource(&path.into())?;

        self.save_state().await
    }

    /// Add a tag to this resource.
    pub async fn add_tag(&mut self, path: &[String], tag: &str) -> Result<()> {
        let mut dir = self.resources_dir().await?;

        let file = dir
            .open_file_mut(
                path,
                true,
                Utc::now(),
                &mut self.forest,
                &self.block_store,
                &mut self.rng,
            )
            .await?;

        let file_metadata = file.get_metadata_mut();

        // Get the private forest content for that resource.
        let maybe_resource_metadata: Option<IpldResult<ResourceMetadata>> =
            file_metadata.get_deserializable("res_meta");
        if let Some(Ok(mut resource_metadata)) = maybe_resource_metadata {
            resource_metadata.add_tag(tag);
            file_metadata.put_serializable("res_meta", resource_metadata)?;
        } else {
            return Err(StoreError::NoResourceMetadata(path.to_vec()));
        }

        dir.as_node()
            .store(&mut self.forest, &self.block_store, &mut self.rng)
            .await?;

        self.indexer.add_tag(&path.into(), tag)?;

        self.save_state().await
    }

    /// Remove a tag from this resource.
    pub async fn remove_tag(&mut self, path: &[String], tag: &str) -> Result<()> {
        let mut dir = self.resources_dir().await?;

        let file = dir
            .open_file_mut(
                path,
                true,
                Utc::now(),
                &mut self.forest,
                &self.block_store,
                &mut self.rng,
            )
            .await?;

        let file_metadata = file.get_metadata_mut();

        // Get the private forest content for that resource.
        let maybe_resource_metadata: Option<IpldResult<ResourceMetadata>> =
            file_metadata.get_deserializable("res_meta");
        if let Some(Ok(mut resource_metadata)) = maybe_resource_metadata {
            resource_metadata.remove_tag(tag);
            file_metadata.put_serializable("res_meta", resource_metadata)?;
        } else {
            return Err(StoreError::NoResourceMetadata(path.to_vec()));
        }

        dir.as_node()
            .store(&mut self.forest, &self.block_store, &mut self.rng)
            .await?;

        self.indexer.remove_tag(&path.into(), tag)?;

        self.save_state().await
    }

    /// Retrieves the content for this path and variant as a bytes vector.
    /// Should only be used for small variant sizes.
    pub async fn get_variant_vec(&self, variant_name: &str, path: &[String]) -> Result<Vec<u8>> {
        let file = self.maybe_file(path).await?;

        if variant_name == "default" {
            // For the default variant, get the "main" file content.
            file.get_content(&self.forest, &self.block_store)
                .await
                .map_err(|e| e.into())
        } else {
            // Fetch the variant content from the node metadata.
            let file_metadata = file.get_metadata();
            let maybe_resource_metadata: Option<IpldResult<ResourceMetadata>> =
                file_metadata.get_deserializable("res_meta");
            if let Some(Ok(resource_metadata)) = maybe_resource_metadata {
                if !resource_metadata.has_variant(variant_name) {
                    return Err(StoreError::NoSuchVariant(
                        variant_name.to_owned(),
                        path.to_vec(),
                    ));
                }
                match file_metadata.get(&format!("{}_variant", variant_name)) {
                    Some(variant_ipld) => {
                        let content = PrivateForestContent::from_metadata_value(variant_ipld)?;
                        content
                            .get_content(&self.forest, &self.block_store)
                            .await
                            .map_err(|e| e.into())
                    }
                    None => Err(StoreError::NoVariantContent(
                        variant_name.to_owned(),
                        path.to_vec(),
                    )),
                }
            } else {
                Err(StoreError::NoResourceMetadata(path.to_vec()))
            }
        }
    }

    /// Retrieves the content for this path and variant as a stream of byte chunks.
    pub async fn get_variant<'a>(
        &'a self,
        variant_name: &str,
        path: &[String],
    ) -> Result<LocalBoxStream<'a, Result<Vec<u8>>>> {
        let file = self.maybe_file(path).await?;

        if variant_name == "default" {
            // For the default variant, get the "main" file content.
            Ok(Box::pin(stream! {
                for await value in file.stream_content(0, &self.forest, &self.block_store) {
                    yield value.map_err(|e| e.into());
                }
            }))
        } else {
            // Fetch the variant content from the node metadata.
            let file_metadata = file.get_metadata();
            let maybe_resource_metadata: Option<IpldResult<ResourceMetadata>> =
                file_metadata.get_deserializable("res_meta");
            if let Some(Ok(resource_metadata)) = maybe_resource_metadata {
                if !resource_metadata.has_variant(variant_name) {
                    return Err(StoreError::NoSuchVariant(
                        variant_name.to_owned(),
                        path.to_vec(),
                    ));
                }
                match file_metadata.get(&format!("variant_{}", variant_name)) {
                    Some(variant_ipld) => {
                        let content = PrivateForestContent::from_metadata_value(variant_ipld)?;
                        Ok(Box::pin(stream! {
                            for await value in content.stream(0, &self.forest, &self.block_store) {
                                yield value.map_err(|e| e.into());
                            }
                        }))
                    }
                    None => Err(StoreError::NoVariantContent(
                        variant_name.to_owned(),
                        path.to_vec(),
                    )),
                }
            } else {
                Err(StoreError::NoResourceMetadata(path.to_vec()))
            }
        }
    }

    /// Imports a local file to the private store.
    pub async fn import_file<P: AsRef<Path>>(&mut self, path: P) -> Result<()> {
        let full_path = path.as_ref();

        let file_name = full_path
            .file_name()
            .unwrap_or(OsStr::new("noname.txt"))
            .to_string_lossy();

        let reader = fs::File::open(full_path).await?;
        let reader_meta = reader.metadata().await?;
        let mime = mime_guess::from_path(path.as_ref()).first_or_octet_stream();

        debug!("Mime type for {} is {}", path.as_ref().display(), mime);
        let variant = VariantMetadata::new(reader_meta.len(), mime.as_ref());

        self.create_resource(
            &[file_name.to_string()],
            &full_path.display().to_string(),
            &variant,
            HashSet::new(),
            reader.compat(),
        )
        .await
    }

    pub async fn ls(&self, dir: Rc<PrivateDirectory>) -> Result<Vec<(String, ResourceMetadata)>> {
        let children = dir.ls(&[], true, &self.forest, &self.block_store).await?;

        let mut results = vec![];
        for (path, metadata) in children {
            let maybe_resource_metadata: Option<IpldResult<ResourceMetadata>> =
                metadata.get_deserializable("res_meta");
            if let Some(Ok(resource_metadata)) = maybe_resource_metadata {
                results.push((path, resource_metadata));
            } else {
                return Err(StoreError::NoResourceMetadata(vec![path]));
            }
        }
        Ok(results)
    }

    pub async fn get_metadata(&self, path: &[String]) -> Result<ResourceMetadata> {
        let file = self.maybe_file(path).await?;

        let file_metadata = file.get_metadata();
        let maybe_resource_metadata: Option<IpldResult<ResourceMetadata>> =
            file_metadata.get_deserializable("res_meta");
        if let Some(Ok(resource_metadata)) = maybe_resource_metadata {
            Ok(resource_metadata)
        } else {
            Err(StoreError::NoResourceMetadata(path.to_vec()))
        }
    }

    pub async fn search(&self, text: &str) -> Result<Vec<(ResourceId, ResourceMetadata)>> {
        let ids = self.indexer.search(text)?;

        let mut result = vec![];
        for id in ids {
            let path: Vec<String> = id.clone().into();
            result.push((id, self.get_metadata(&path).await?))
        }
        Ok(result)
    }
}
