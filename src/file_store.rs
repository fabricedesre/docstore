//! A file backed store for wnfs

use anyhow::Result;
use async_std::fs;
use async_std::path::{Path, PathBuf};
use async_trait::async_trait;
use bytes::Bytes;
use libipld::Cid;
use wnfs::common::BlockStore;

pub struct FileStore {
    root: PathBuf,
}

impl FileStore {
    pub async fn maybe_new<P: AsRef<Path>>(root: P) -> Result<Self> {
        // Check if the root directory exists, or try to create it.
        let root = root.as_ref();
        if !root.exists().await {
            fs::create_dir(root).await?;
        }

        Ok(Self { root: root.into() })
    }

    fn path_for_cid(&self, cid: &Cid) -> PathBuf {
        let filename = cid.to_string();
        self.root.join(filename)
    }
}

#[async_trait(?Send)]
impl BlockStore for FileStore {
    async fn get_block(&self, cid: &Cid) -> Result<Bytes> {
        let bytes = fs::read(self.path_for_cid(cid)).await?;
        Ok(bytes.into())
    }

    async fn put_block(&self, bytes: impl Into<Bytes>, codec: u64) -> Result<Cid> {
        let bytes: Bytes = bytes.into();
        let cid = self.create_cid(&bytes, codec)?;
        fs::write(self.path_for_cid(&cid), bytes).await?;
        Ok(cid)
    }
}
