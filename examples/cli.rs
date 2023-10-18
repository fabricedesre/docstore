use docstore::store::ResourceStore;
use anyhow::Result;
use core::future;
use futures::TryStreamExt;

#[async_std::main]
async fn main() -> Result<()> {
    let mut doc_store = ResourceStore::new("./data").await?;

    if let Some(arg) = std::env::args().nth(1) {
        if arg == "put" {
            if let Some(file_name) = std::env::args().nth(2) {
                println!("Will store {}", file_name);
                doc_store.import_file(&file_name).await?;
                println!("File stored successfully!");
            }
        } else if arg == "ls" {
            let files = doc_store.ls(doc_store.resources_dir().await?).await?;
            println!("{} files:", files.len());
            for file in files {
                println!("{}", file.0);
            }
        } else if arg == "get" {
            if let Some(file_name) = std::env::args().nth(2) {
                let stream = doc_store.get_variant("default", &[file_name]).await?;
                stream
                    .try_for_each(|chunk| {
                        print!("{}", String::from_utf8_lossy(&chunk));
                        future::ready(Ok(()))
                    })
                    .await?;

                println!("");
            }
        }
    }

    Ok(())
}
