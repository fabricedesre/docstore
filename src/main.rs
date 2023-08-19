mod file_store;

use anyhow::Result;
use async_std::{fs, path::Path};
use chrono::Utc;
use file_store::FileStore;
use libipld::Cid;
use rand::thread_rng;
use serde::{de::DeserializeOwned, Serialize};
use std::{ffi::OsStr, rc::Rc};
use wnfs::{
    common::BlockStore,
    nameaccumulator::AccumulatorSetup,
    private::{
        forest::{hamt::HamtForest, traits::PrivateForest},
        AccessKey, PrivateDirectory, PrivateNode,
    },
    rand_core::CryptoRngCore,
};

async fn init_forest(
    store: &impl BlockStore,
    rng: &mut impl CryptoRngCore,
) -> Result<(Cid, AccessKey)> {
    println!("Initializing a new forest");
    let setup = AccumulatorSetup::trusted(rng);
    let forest = &mut Rc::new(HamtForest::new(setup));
    let dir = &mut Rc::new(PrivateDirectory::new(&forest.empty_name(), Utc::now(), rng));
    let access_key = dir.as_node().store(forest, store, rng).await?;
    let forest_cid = forest.store(store).await?;

    // Save the initial access key.
    to_cbor("access.key", &access_key).await?;

    Ok((forest_cid, access_key))
}

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

async fn store_file<P: AsRef<Path>>(
    path: P,
    dir: &mut Rc<PrivateDirectory>,
    forest: &mut impl PrivateForest,
    store: &impl BlockStore,
    rng: &mut impl CryptoRngCore,
) -> Result<()> {
    let full_path = path.as_ref();

    let content = fs::read(full_path).await?;
    let file_name = full_path
        .file_name()
        .unwrap_or(OsStr::new("noname.txt"))
        .to_string_lossy();

    dir.write(
        &[file_name.to_string()],
        true,
        Utc::now(),
        content,
        forest,
        store,
        rng,
    )
    .await?;

    Ok(())
}

async fn list_dir(
    dir: &mut Rc<PrivateDirectory>,
    forest: &impl PrivateForest,
    store: &impl BlockStore,
) -> Result<()> {
    // List the nodes in the directory.
    println!("\n=========================");
    println!("Private directory content");
    println!("=========================");
    let files = dir.ls(&[], true, forest, store).await?;
    println!("{} files:", files.len());
    for file in files {
        println!("{}", file.0);
    }
    println!("=========================\n");
    Ok(())
}

#[async_std::main]
async fn main() -> Result<()> {
    let store = &FileStore::maybe_new("blockstore").await?;

    let rng = &mut thread_rng();

    // Initialize the forest and access key from serialized ones if possible.
    let (forest_cid, access_key) =
        match (from_cbor("forest.cid").await, from_cbor("access.key").await) {
            (Ok(cid), Ok(access_key)) => {
                println!("Using existing access key");
                (cid, access_key)
            }
            _ => init_forest(store, rng).await?,
        };

    let mut forest = HamtForest::load(&forest_cid, store).await?;

    // Get the latest version of the root node.
    let mut dir = PrivateNode::load(&access_key, &forest, store, None)
        .await?
        .search_latest(&forest, store)
        .await?;

    let dir = dir.as_dir_mut()?;

    list_dir(dir, &forest, store).await?;

    if let Some(arg) = std::env::args().nth(1) {
        if arg == "put" {
            if let Some(file_name) = std::env::args().nth(2) {
                println!("Will store {}", file_name);
                store_file(&file_name, dir, &mut forest, store, rng).await?;
                println!("Successfully stored {}", file_name);

                list_dir(dir, &forest, store).await?;
                dir.as_node().store(&mut forest, store, rng).await?;
            }
        } else if arg == "get" {
            if let Some(file_name) = std::env::args().nth(2) {
                let result = dir.read(&[file_name], true, &forest, store).await?;
                println!("{}", String::from_utf8_lossy(&result));
            }
        }
    }

    // Save the forest.
    to_cbor("forest.cid", forest.store(store).await?).await?;

    Ok(())
}
