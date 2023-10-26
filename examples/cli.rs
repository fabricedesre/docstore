use core::future;
use docstore::{
    resource::ResourceMetadata,
    store::{ResourceStore, StoreError},
};
use futures::TryStreamExt;
use std::time::Instant;

fn print_resource_details(id: &str, meta: &ResourceMetadata) {
    let mut size = 0;
    let variants = meta.variants();
    for variant_meta in variants.values() {
        size += variant_meta.size();
    }

    let mut out = format!("{} - {}b ", id, size);
    for (name, variant_meta) in variants {
        out.push_str(&format!(
            "[{}: {} {}b] ",
            name,
            variant_meta.mime_type(),
            variant_meta.size()
        ));
    }

    println!("{}", out);
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), StoreError> {
    env_logger::init();
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
                print_resource_details(&file.0, &file.1);
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

                println!();
            }
        } else if arg == "search" {
            if let Some(text) = std::env::args().nth(2) {
                let files = doc_store.search(&text).await?;

                println!("{} search results:", files.len(),);
                for file in files {
                    print_resource_details(&file.0.to_string(), &file.1);
                }
            }
        }
        println!("Done in {}ms", start.elapsed().as_millis());
    }

    Ok(())
}
