//! A file backed store for wnfs

use async_trait::async_trait;
use bytes::Bytes;
use libipld::Cid;
use std::path::{Path, PathBuf};
use tokio::fs;
use wnfs::common::BlockStore;

type IpldError = libipld::error::Error;

pub struct FileStore {
    root: PathBuf,
}

impl FileStore {
    pub async fn maybe_new<P: AsRef<Path>>(root: P) -> Result<Self, std::io::Error> {
        // Check if the root directory exists, or try to create it.
        let root = root.as_ref();
        if !root.exists() {
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
    async fn get_block(&self, cid: &Cid) -> Result<Bytes, IpldError> {
        let bytes = fs::read(self.path_for_cid(cid)).await?;
        Ok(bytes.into())
    }

    async fn put_block(&self, bytes: impl Into<Bytes>, codec: u64) -> Result<Cid, IpldError> {
        let bytes: Bytes = bytes.into();
        let cid = self.create_cid(&bytes, codec)?;
        fs::write(self.path_for_cid(&cid), bytes).await?;
        Ok(cid)
    }
}
