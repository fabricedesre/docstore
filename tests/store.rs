use async_std::io::Cursor;
use core::future;
use docstore::resource::VariantMetadata;
use docstore::store::ResourceStore;
use futures::TryStreamExt;
use std::collections::HashSet;
use std::path::PathBuf;

async fn get_test_store(num: u32) -> ResourceStore {
    ResourceStore::new(&format!("./tests/data{}", num))
        .await
        .expect("Failed to create resource store!")
}

async fn init_test(num: u32) -> ResourceStore {
    let path = PathBuf::from(format!("./tests/data{}", num));

    // Remove the directory if it exists.
    if path.exists() {
        let _ = std::fs::remove_dir_all(&path);
    }

    get_test_store(num).await
}

#[async_std::test]
async fn store_empty_file() {
    let num_test = 0;
    let path = ["empty".to_owned()];
    {
        // Step 1: store an empty file and read it back.
        let mut store = init_test(num_test).await;

        let variant = VariantMetadata::new(0, "application/octet-stream");

        store
            .create_resource(
                &path,
                "empty file",
                &variant,
                HashSet::new(),
                Cursor::new(vec![]),
            )
            .await
            .unwrap();

        let content = store.get_variant_vec("default", &path).await.unwrap();
        assert_eq!(content.len(), 0);
    }

    {
        // Step 2. Re-open the store and read as a Vec<>
        let store: ResourceStore = get_test_store(num_test).await;
        let content = store.get_variant_vec("default", &path).await.unwrap();
        assert_eq!(content.len(), 0);
    }

    {
        // Step 3. Re-open the store and read as a stream
        let store: ResourceStore = get_test_store(num_test).await;
        let stream = store.get_variant("default", &path).await.unwrap();

        stream
            .try_for_each(|chunk| {
                assert_eq!(chunk.len(), 0);
                future::ready(Ok(()))
            })
            .await
            .unwrap();
    }
}

#[async_std::test]
async fn store_variant() {
    let path = ["small file".to_owned()];
    let content = b"abcdef0123456789".as_slice();
    let variant_content = b"9876543210fedcba".as_slice();

    let num_test = 1;
    {
        // Step 1: store a file with a variant and read it back.
        let mut store = init_test(num_test).await;

        let variant = VariantMetadata::new(16, "text/plain");

        store
            .create_resource(
                &path,
                "small file",
                &variant,
                HashSet::new(),
                Cursor::new(content),
            )
            .await
            .unwrap();

        let variant = VariantMetadata::new(16, "text/plain");

        store
            .add_variant(&path, "reverse", &variant, Cursor::new(variant_content))
            .await
            .unwrap();

        let default_variant = store.get_variant_vec("default", &path).await.unwrap();
        assert_eq!(default_variant, content.to_vec());

        let reverse_variant = store.get_variant_vec("reverse", &path).await.unwrap();
        assert_eq!(reverse_variant, variant_content.to_vec());
    }

    {
        // Step 2. Re-open the store and read as a Vec<>
        let store: ResourceStore = get_test_store(num_test).await;

        let default_variant = store.get_variant_vec("default", &path).await.unwrap();
        assert_eq!(default_variant, content.to_vec());

        let reverse_variant = store.get_variant_vec("reverse", &path).await.unwrap();
        assert_eq!(reverse_variant, variant_content.to_vec());
    }
}

#[async_std::test]
async fn import_file() {
    let path = ["hello.txt".to_owned()];

    let num_test = 2;
    {
        // Step 1: store a file with a variant and read it back.
        let mut store = init_test(num_test).await;

        store
            .import_file("./tests/fixtures/hello.txt")
            .await
            .unwrap();

        let content = store.get_variant_vec("default", &path).await.unwrap();
        assert_eq!(content.len(), 13);
    }

    {
        // Step 2. Re-open the store and read as a Vec<>
        let store: ResourceStore = get_test_store(num_test).await;

        let content = store.get_variant_vec("default", &path).await.unwrap();
        assert_eq!(content.len(), 13);
    }
}

#[async_std::test]
async fn get_metadata() {
    let path = ["small file".to_owned()];
    let content = Cursor::new(b"abcdef0123456789".as_slice());
    let variant_content = Cursor::new(b"9876543210fedcba".as_slice());

    let num_test = 3;
    {
        // Step 1: store a file with a variant and check the metadata
        let mut store = init_test(num_test).await;

        let variant = VariantMetadata::new(16, "text/plain");

        let mut tags = HashSet::new();
        tags.insert("tag_1".to_owned());
        tags.insert("tag_2".to_owned());

        store
            .create_resource(&path, "small file", &variant, tags, content)
            .await
            .unwrap();

        let variant = VariantMetadata::new(16, "text/plain");

        store
            .add_variant(&path, "reverse", &variant, variant_content)
            .await
            .unwrap();

        let meta = store.get_metadata(&path).await.unwrap();
        assert!(meta.has_variant("default"));
        assert!(meta.has_variant("reverse"));
        assert_eq!(meta.tags().len(), 2);
    }

    {
        // Step 2. Re-open the store and check the metadata
        let store: ResourceStore = get_test_store(num_test).await;

        let meta = store.get_metadata(&path).await.unwrap();
        assert!(meta.has_variant("default"));
        assert!(meta.has_variant("reverse"));
        assert_eq!(meta.tags().len(), 2);
    }
}

#[async_std::test]
async fn search() {
    let path = ["small file".to_owned()];
    let content = Cursor::new(b"abcdef0123456789".as_slice());
    let variant_content = Cursor::new(b"9876543210fedcba".as_slice());

    let num_test = 4;
    {
        // Step 1: store a file with a variant and search it.
        let mut store = init_test(num_test).await;

        let variant = VariantMetadata::new(16, "text/plain");

        let mut tags = HashSet::new();
        tags.insert("tag_1".to_owned());
        tags.insert("tag_2".to_owned());

        store
            .create_resource(&path, "small file", &variant, tags, content)
            .await
            .unwrap();

        let variant = VariantMetadata::new(16, "text/plain");

        store
            .add_variant(&path, "reverse", &variant, variant_content)
            .await
            .unwrap();

        let results = store.search("small").await.unwrap();
        assert_eq!(results.len(), 1);
        let result = &results[0];
        let meta = &result.1;
        assert!(meta.has_variant("default"));
        assert!(meta.has_variant("reverse"));
        assert_eq!(meta.tags().len(), 2);

        let results = store.search("big").await.unwrap();
        assert_eq!(results.len(), 0);
    }

    {
        // Step 2. Re-open the store and check the search results.
        let store: ResourceStore = get_test_store(num_test).await;

        let results = store.search("small").await.unwrap();
        assert_eq!(results.len(), 1);
        let result = &results[0];
        let meta = &result.1;
        assert!(meta.has_variant("default"));
        assert!(meta.has_variant("reverse"));
        assert_eq!(meta.tags().len(), 2);

        let results = store.search("big").await.unwrap();
        assert_eq!(results.len(), 0);
    }
}
