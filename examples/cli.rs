use core::future;
use docstore::store::{ResourceStore, StoreError};
use futures::TryStreamExt;
use std::time::Instant;

#[async_std::main]
async fn main() -> Result<(), StoreError> {
    let mut doc_store = ResourceStore::new("./data").await?;

    if let Some(arg) = std::env::args().nth(1) {
        let start = Instant::now();
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
                let mut size = 0;
                let variants = file.1.variants();
                for (_variant_name, variant_meta) in variants {
                    size += variant_meta.size();
                }
                println!("{} - {}b [{} variants]", file.0, size, variants.len());
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
        } else if arg == "search" {
            if let Some(text) = std::env::args().nth(2) {
                let files = doc_store.search(&text).await?;

                println!("{} search results:", files.len(),);
                for file in files {
                    let mut size = 0;
                    let variants = file.1.variants();
                    for (_variant_name, variant_meta) in variants {
                        size += variant_meta.size();
                    }
                    println!(
                        "{} - {}b [{} variants]",
                        file.0.to_string(),
                        size,
                        variants.len()
                    );
                }
            }
        }
        println!("Done in {}ms", start.elapsed().as_millis());
    }

    Ok(())
}
