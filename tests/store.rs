use core::future;
use docstore::resource::VariantMetadata;
use docstore::store::ResourceStore;
use futures::TryStreamExt;
use std::collections::HashSet;
use std::io::{Cursor, Read};
use std::path::{Path, PathBuf};
use tokio_util::compat::TokioAsyncReadCompatExt;

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

fn fixture_file<P: AsRef<Path>>(path: P) -> Cursor<Vec<u8>> {
    let mut file = std::fs::File::open(path).unwrap();
    let mut buffer = vec![];
    file.read_to_end(&mut buffer).unwrap();
    Cursor::new(buffer)
}

#[tokio::test]
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
                Cursor::new(vec![]).compat(),
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

#[tokio::test]
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
                Cursor::new(content).compat(),
            )
            .await
            .unwrap();

        let variant = VariantMetadata::new(16, "text/plain");

        store
            .add_variant(
                &path,
                "reverse",
                &variant,
                Cursor::new(variant_content).compat(),
            )
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

#[tokio::test]
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

#[tokio::test]
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
            .create_resource(&path, "small file", &variant, tags, content.compat())
            .await
            .unwrap();

        let variant = VariantMetadata::new(16, "text/plain");

        store
            .add_variant(&path, "reverse", &variant, variant_content.compat())
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

#[tokio::test]
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
            .create_resource(&path, "small file", &variant, tags, content.compat())
            .await
            .unwrap();

        let variant = VariantMetadata::new(16, "text/plain");

        store
            .add_variant(&path, "reverse", &variant, variant_content.compat())
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

#[tokio::test]
async fn index_place() {
    let path = ["place test".to_owned()];

    let num_test = 5;
    {
        // Step 1: store a file with a variant and search it.
        let mut store = init_test(num_test).await;

        let content = fixture_file("./tests/fixtures/places-1.json");
        let variant = VariantMetadata::new(16, "application/x-places+json");

        store
            .create_resource(
                &path,
                "sample place document",
                &variant,
                HashSet::new(),
                content.compat(),
            )
            .await
            .unwrap();

        let results = store.search("example").await.unwrap();
        assert_eq!(results.len(), 1);

        let results = store.search("unknown").await.unwrap();
        assert_eq!(results.len(), 0);
    }

    {
        // Step 2. Re-open the store and check the search results.
        let store: ResourceStore = get_test_store(num_test).await;
        let results = store.search("example").await.unwrap();
        assert_eq!(results.len(), 1);

        let results = store.search("unknown").await.unwrap();
        assert_eq!(results.len(), 0);
    }
}

#[tokio::test]
async fn index_contact() {
    let path = ["contact test".to_owned()];

    let num_test = 6;
    {
        let mut store = init_test(num_test).await;

        let content = fixture_file("./tests/fixtures/contacts-1.json");
        let variant = VariantMetadata::new(16, "application/x-contact+json");

        store
            .create_resource(
                &path,
                "sample contact",
                &variant,
                HashSet::new(),
                content.compat(),
            )
            .await
            .unwrap();

        // Search name
        let results = store.search("dupont").await.unwrap();
        assert_eq!(results.len(), 1);

        // Search first name letter
        let results = store.search("^^^^j").await.unwrap();
        assert_eq!(results.len(), 1);

        // Search first name letter
        let results = store.search("^^^^t").await.unwrap();
        assert_eq!(results.len(), 0);

        // Search phone number
        let results = store.search("012345").await.unwrap();
        assert_eq!(results.len(), 1);

        // Search email
        let results = store.search("secret@").await.unwrap();
        assert_eq!(results.len(), 1);

        let results = store.search("unknown").await.unwrap();
        assert_eq!(results.len(), 0);
    }
}

#[tokio::test]
async fn delete_resource() {
    let path = ["contact test".to_owned()];

    let num_test = 7;
    {
        let mut store = init_test(num_test).await;

        let content = fixture_file("./tests/fixtures/contacts-1.json");
        let variant = VariantMetadata::new(16, "application/x-contact+json");

        store
            .create_resource(
                &path,
                "sample contact",
                &variant,
                HashSet::new(),
                content.compat(),
            )
            .await
            .unwrap();

        // Search name
        let results = store.search("dupont").await.unwrap();
        assert_eq!(results.len(), 1);

        // Get content.
        let content = store.get_variant_vec("default", &path).await.unwrap();
        assert_eq!(content.len(), 127);

        store.delete_resource(&path).await.unwrap();

        // Search name
        let results = store.search("dupont").await.unwrap();
        assert_eq!(results.len(), 0);

        // Fail to list resource.
        let files = store
            .ls(store.resources_dir().await.unwrap())
            .await
            .unwrap();
        assert_eq!(files.len(), 0);

        // Fail to get content.
        assert!(store.get_variant_vec("default", &path).await.is_err());
    }
}

#[tokio::test]
async fn delete_variant() {
    let path = ["small file".to_owned()];
    let content = b"abcdef0123456789".as_slice();
    let variant_content = b"9876543210fedcba".as_slice();

    let num_test = 8;
    {
        // Create a resource with 2 variants.
        let mut store = init_test(num_test).await;

        let variant = VariantMetadata::new(16, "text/plain");

        store
            .create_resource(
                &path,
                "small file",
                &variant,
                HashSet::new(),
                Cursor::new(content).compat(),
            )
            .await
            .unwrap();

        let variant = VariantMetadata::new(16, "text/plain");

        store
            .add_variant(
                &path,
                "reverse",
                &variant,
                Cursor::new(variant_content).compat(),
            )
            .await
            .unwrap();

        let default_variant = store.get_variant_vec("default", &path).await.unwrap();
        assert_eq!(default_variant, content.to_vec());

        let reverse_variant = store.get_variant_vec("reverse", &path).await.unwrap();
        assert_eq!(reverse_variant, variant_content.to_vec());

        // Check that the medata has 2 variants.
        let meta = store.get_metadata(&path).await.unwrap();
        assert_eq!(meta.variants().len(), 2);

        // Search for the "reverse" variant content.
        let result = store.search("fedcba").await.unwrap();
        assert_eq!(result.len(), 1);

        // Delete the "reverse" variant.
        store.delete_variant(&path, "reverse").await.unwrap();

        // Check that the medata has 1 variant.
        let meta = store.get_metadata(&path).await.unwrap();
        assert_eq!(meta.variants().len(), 1);

        // Search for the "reverse" variant content.
        let result = store.search("fedcba").await.unwrap();
        assert_eq!(result.len(), 0);

        // Fail to get the remove variant content.
        assert!(store.get_variant_vec("reverse", &path).await.is_err());
    }

    {
        // Re-open the store and check the state.
        let store: ResourceStore = get_test_store(num_test).await;

        // Check that the medata has 1 variant.
        let meta = store.get_metadata(&path).await.unwrap();
        assert_eq!(meta.variants().len(), 1);

        // Search for the "reverse" variant content.
        let result = store.search("fedcba").await.unwrap();
        assert_eq!(result.len(), 0);

        // Fail to get the remove variant content.
        assert!(store.get_variant_vec("reverse", &path).await.is_err());
    }
}

#[tokio::test]
async fn update_variant() {
    let path = ["small file".to_owned()];
    let content = b"abcdef0123456789".as_slice();
    let variant_content = b"9876543210fedcba".as_slice();
    let updated_content = b"this is updated content".as_slice();

    let num_test = 9;
    {
        // Create a resource with 2 variants.
        let mut store = init_test(num_test).await;

        let variant = VariantMetadata::new(content.len() as _, "text/plain");

        store
            .create_resource(
                &path,
                "small file",
                &variant,
                HashSet::new(),
                Cursor::new(content).compat(),
            )
            .await
            .unwrap();

        let variant = VariantMetadata::new(content.len() as _, "text/plain");

        store
            .add_variant(
                &path,
                "reverse",
                &variant,
                Cursor::new(variant_content).compat(),
            )
            .await
            .unwrap();

        let default_variant = store.get_variant_vec("default", &path).await.unwrap();
        assert_eq!(default_variant, content.to_vec());

        let reverse_variant = store.get_variant_vec("reverse", &path).await.unwrap();
        assert_eq!(reverse_variant, variant_content.to_vec());

        // Check that the medata has 2 variants.
        let meta = store.get_metadata(&path).await.unwrap();
        assert_eq!(meta.variants().len(), 2);

        // Search for the "reverse" variant content.
        let result = store.search("fedcba").await.unwrap();
        assert_eq!(result.len(), 1);

        // Update the "reverse" variant.
        let variant = VariantMetadata::new(updated_content.len() as _, "text/plain");

        store
            .update_variant(
                &path,
                "reverse",
                &variant,
                Cursor::new(updated_content).compat(),
            )
            .await
            .unwrap();

        // Check that the medata still has 2 variants.
        let meta = store.get_metadata(&path).await.unwrap();
        assert_eq!(meta.variants().len(), 2);

        // Search for the old "reverse" variant content.
        let result = store.search("fedcba").await.unwrap();
        assert_eq!(result.len(), 0);

        // Search for the new "reverse" variant content.
        let result = store.search("updated").await.unwrap();
        assert_eq!(result.len(), 1);

        // Get the updated variant content.
        let fetched = store.get_variant_vec("reverse", &path).await.unwrap();
        assert_eq!(&fetched, updated_content);
    }

    {
        // Re-open the store and check the state.
        let store: ResourceStore = get_test_store(num_test).await;

        // Check that the medata still has 2 variants.
        let meta = store.get_metadata(&path).await.unwrap();
        assert_eq!(meta.variants().len(), 2);

        // Search for the old "reverse" variant content.
        let result = store.search("fedcba").await.unwrap();
        assert_eq!(result.len(), 0);

        // Search for the new "reverse" variant content.
        let result = store.search("updated").await.unwrap();
        assert_eq!(result.len(), 1);

        // Get the updated variant content.
        let fetched = store.get_variant_vec("reverse", &path).await.unwrap();
        assert_eq!(&fetched, updated_content);
    }
}

#[tokio::test]
async fn update_default_variant() {
    let path = ["small file".to_owned()];
    let content = b"abcdef0123456789".as_slice();
    let variant_content = b"9876543210fedcba".as_slice();
    let updated_content = b"this is updated content".as_slice();

    let num_test = 10;
    {
        // Create a resource with 2 variants.
        let mut store = init_test(num_test).await;

        let variant = VariantMetadata::new(content.len() as _, "text/plain");

        store
            .create_resource(
                &path,
                "small file",
                &variant,
                HashSet::new(),
                Cursor::new(content).compat(),
            )
            .await
            .unwrap();

        let variant = VariantMetadata::new(content.len() as _, "text/plain");

        store
            .add_variant(
                &path,
                "reverse",
                &variant,
                Cursor::new(variant_content).compat(),
            )
            .await
            .unwrap();

        let default_variant = store.get_variant_vec("default", &path).await.unwrap();
        assert_eq!(default_variant, content.to_vec());

        let reverse_variant = store.get_variant_vec("reverse", &path).await.unwrap();
        assert_eq!(reverse_variant, variant_content.to_vec());

        // Check that the medata has 2 variants.
        let meta = store.get_metadata(&path).await.unwrap();
        assert_eq!(meta.variants().len(), 2);

        // Search for the "default" variant content.
        let result = store.search("abcdef").await.unwrap();
        assert_eq!(result.len(), 1);

        // Update the "default" variant.
        let variant = VariantMetadata::new(updated_content.len() as _, "text/plain");

        store
            .update_variant(
                &path,
                "default",
                &variant,
                Cursor::new(updated_content).compat(),
            )
            .await
            .unwrap();

        // Check that the medata still has 2 variants.
        let meta = store.get_metadata(&path).await.unwrap();
        assert_eq!(meta.variants().len(), 2);

        // Search for the old "reverse" variant content.
        let result = store.search("abcdef").await.unwrap();
        assert_eq!(result.len(), 0);

        // Search for the new "reverse" variant content.
        let result = store.search("updated").await.unwrap();
        assert_eq!(result.len(), 1);

        // Get the updated variant content.
        let fetched = store.get_variant_vec("default", &path).await.unwrap();
        assert_eq!(&fetched, updated_content);
    }

    {
        // Re-open the store and check the state.
        let store: ResourceStore = get_test_store(num_test).await;

        // Check that the medata still has 2 variants.
        let meta = store.get_metadata(&path).await.unwrap();
        assert_eq!(meta.variants().len(), 2);

        // Search for the old "reverse" variant content.
        let result = store.search("abcdef").await.unwrap();
        assert_eq!(result.len(), 0);

        // Search for the new "reverse" variant content.
        let result = store.search("updated").await.unwrap();
        assert_eq!(result.len(), 1);

        // Get the updated variant content.
        let fetched = store.get_variant_vec("default", &path).await.unwrap();
        assert_eq!(&fetched, updated_content);
    }
}

#[tokio::test]
async fn add_remove_tags() {
    let num_test = 11;
    let path = ["tag demo".to_owned()];
    {
        let mut store = init_test(num_test).await;

        let variant = VariantMetadata::new(0, "application/octet-stream");

        store
            .create_resource(
                &path,
                "empty file",
                &variant,
                HashSet::new(),
                Cursor::new(vec![]).compat(),
            )
            .await
            .unwrap();

        let content = store.get_variant_vec("default", &path).await.unwrap();
        assert_eq!(content.len(), 0);

        let meta = store.get_metadata(&path).await.unwrap();
        assert_eq!(meta.tags().len(), 0);

        store.add_tag(&path, "tag-1").await.unwrap();
        let meta = store.get_metadata(&path).await.unwrap();
        assert_eq!(meta.tags().len(), 1);

        store.add_tag(&path, "tag-2").await.unwrap();
        let meta = store.get_metadata(&path).await.unwrap();
        assert_eq!(meta.tags().len(), 2);

        store.remove_tag(&path, "tag-1").await.unwrap();
        let meta = store.get_metadata(&path).await.unwrap();
        assert_eq!(meta.tags().len(), 1);
    }

    {
        let store: ResourceStore = get_test_store(num_test).await;
        let meta = store.get_metadata(&path).await.unwrap();
        assert_eq!(meta.tags().len(), 1);
        assert_eq!(meta.tags().iter().next(), Some(&"tag-2".to_owned()));
    }
}

#[tokio::test]
async fn image_transformer() {
    let path = ["sticker_logo_small.png".to_owned()];

    let num_test = 12;
    {
        let mut store = init_test(num_test).await;

        store
            .import_file("./tests/fixtures/sticker_logo_small.png")
            .await
            .unwrap();

        let metadata = store.get_metadata(&path).await.unwrap();
        let variants = metadata.variants();
        assert_eq!(variants.len(), 2);
        assert!(variants.contains_key("default"));
        assert!(variants.contains_key("thumbnail"));
    }

    {
        let store = get_test_store(num_test).await;

        let metadata = store.get_metadata(&path).await.unwrap();
        let variants = metadata.variants();
        assert_eq!(variants.len(), 2);
        assert!(variants.contains_key("default"));
        assert!(variants.contains_key("thumbnail"));
    }
}
