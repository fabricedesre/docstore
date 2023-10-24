//! An indexer for resources
//! It is responsible to provide fast and flexible search
//! capabilities to the resource store:
//! - Full Text Index of resource description and mime type specific extraction.
//! - Tag indexing

use crate::fts::{json_indexer, text_plain_indexer};
use crate::resource::{ResourceId, VariantMetadata};
use crate::timer::Timer;
use futures::io::AsyncSeekExt;
use futures::AsyncRead;
use log::{error, info};
use rusqlite::{Connection, OpenFlags, TransactionBehavior};
use std::io::SeekFrom;
use std::path::Path;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum SqliteDbError {
    #[error("Rusqlite Error")]
    Rusqlite(#[from] rusqlite::Error),
    #[error("Error upgrading db schema from version `{0}` to version `{1}`")]
    SchemaUpgrade(u32, u32),
    #[error("Indexer Error")]
    Indexer(#[from] crate::fts::IndexerError),
}

static UPGRADE_0_1_SQL: [&str; 5] = [
    r#"CREATE TABLE IF NOT EXISTS resources(
        id       TEXT     PRIMARY KEY NOT NULL, -- Unique id mapping with the wnfs side.
        frecency INTEGER,                       -- Frecency score for this resource.
        modified DATETIME NOT NULL              -- Used for "most recently modified" queries.
    );"#,
    r#"CREATE INDEX IF NOT EXISTS idx_resource_modified ON resources(modified);"#,
    r#"CREATE TABLE IF NOT EXISTS tags(
        id  TEXT KEY NOT NULL,
        tag TEXT NOT NULL,
        FOREIGN KEY(id) REFERENCES resources(id) ON DELETE CASCADE
    );"#,
    r#"CREATE INDEX IF NOT EXISTS idx_tag_name ON tags(tag);"#,
    r#"CREATE VIRTUAL TABLE fts USING fts5(id UNINDEXED, variant UNINDEXED, content, tokenize="trigram");"#,
];

static LATEST_VERSION: u32 = 1;

pub struct Indexer {
    conn: Connection,
    should_update: bool,
}

impl Indexer {
    pub fn new<P: AsRef<Path>>(root_dir: P, name: &str) -> Result<Self, SqliteDbError> {
        let mut path = root_dir.as_ref().to_path_buf();
        path.push(name);
        let mut conn = Connection::open_with_flags(&path, OpenFlags::default())?;

        let mut version: u32 =
            conn.query_row("SELECT user_version FROM pragma_user_version", [], |r| {
                r.get(0)
            })?;

        info!("Indexer sql current version: {}", version);

        while version < LATEST_VERSION {
            // Create a scoped transaction to run the schema update steps and the pragma update.
            // The default drop behavior of Transaction is to rollback changes, so we
            // explicitely commit it once all the operations succeeded.
            let transaction = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;

            if version == 0 {
                for sql in UPGRADE_0_1_SQL {
                    transaction.execute(sql, [])?;
                }
                version = 1;
            } else {
                error!("Unexpected version required: {}", version);
                return Err(SqliteDbError::SchemaUpgrade(version, version));
            }

            if let Err(err) = transaction.pragma_update(None, "user_version", version) {
                return Err(err.into());
            }

            transaction.commit()?;
        }

        conn.pragma_update(None, "journal_mode", "WAL".to_string())?;

        Ok(Self {
            conn,
            should_update: false,
        })
    }

    pub fn add_resource(&mut self, id: &ResourceId) -> Result<(), SqliteDbError> {
        let _timer = Timer::start(&format!("Indexer add resource {}", id.to_string()));
        let now = chrono::Utc::now();
        self.conn
            .execute(
                "INSERT INTO resources (id, frecency, modified) VALUES (?1, ?2, ?3)",
                (id, 0, now),
            )
            .map(|_| ())?;
        self.should_update = true;
        Ok(())
    }

    pub fn delete_resource(&mut self, id: &ResourceId) -> Result<(), SqliteDbError> {
        let _timer = Timer::start(&format!("Indexer delete resource {}", id.to_string()));
        self.conn
            .execute("DELETE FROM resources  WHERE id = ?", [id])
            .map(|_| ())?;
        self.conn
            .execute("DELETE FROM fts  WHERE id = ?", [id])
            .map(|_| ())?;
        self.should_update = true;
        Ok(())
    }

    pub fn delete_variant(&mut self, id: &ResourceId, variant: &str) -> Result<(), SqliteDbError> {
        let _timer = Timer::start(&format!(
            "Indexer delete variant {} from {}",
            variant,
            id.to_string()
        ));
        self.conn
            .execute(
                "DELETE FROM fts  WHERE id = ?1 AND variant = ?2",
                (id, variant),
            )
            .map(|_| ())?;
        self.should_update = true;
        Ok(())
    }

    pub fn add_tag(&mut self, id: &ResourceId, tag: &str) -> Result<(), SqliteDbError> {
        let _timer = Timer::start(&format!("Indexer add tag {} to {}", tag, id.to_string()));
        self.conn
            .execute("INSERT INTO tags (id, tag) VALUES (?1, ?2)", (id, tag))
            .map(|_| ())?;
        self.should_update = true;
        Ok(())
    }

    pub fn add_text(
        &mut self,
        id: &ResourceId,
        variant_name: &str,
        text: &str,
    ) -> Result<(), SqliteDbError> {
        let _timer = Timer::start(&format!(
            "Indexer add text to {} [{}]",
            id.to_string(),
            variant_name
        ));

        // Remove diacritics since the trigram tokenizer of SQlite doesn't have this option.
        let content = secular::lower_lay_string(text);
        self.conn
            .execute(
                "INSERT INTO fts (id, variant, content) VALUES (?1, ?2, ?3)",
                (id, variant_name, &content),
            )
            .map(|_| ())?;
        self.should_update = true;
        Ok(())
    }

    pub async fn add_variant<C: AsyncRead + AsyncSeekExt + Unpin>(
        &mut self,
        id: &ResourceId,
        variant_name: &str,
        variant: &VariantMetadata,
        content: &mut C,
    ) -> Result<(), SqliteDbError> {
        let _timer = Timer::start(&format!(
            "Indexer add content to {} [{}]",
            id.to_string(),
            variant_name
        ));

        let mime = variant.mime_type().to_owned();
        let text = if mime.ends_with("json") {
            Some(json_indexer(content, &mime).await?)
        } else {
            match mime.as_str() {
                "text/plain" => Some(text_plain_indexer(content).await?),
                _ => None,
            }
        };

        if let Some(text) = text {
            {
                self.add_text(id, variant_name, &text)?;
            }
        }

        content
            .seek(SeekFrom::Start(0))
            .await
            .expect("Failed to seek!!");

        Ok(())
    }

    pub fn search(&self, text: &str) -> Result<Vec<ResourceId>, SqliteDbError> {
        let _timer = Timer::start(&format!("Indexer search {}", text));

        let search = format!("%{}%", secular::lower_lay_string(text));

        let mut stmt = self
            .conn
            .prepare("SELECT DISTINCT id from fts WHERE content LIKE ?")?;
        let mut rows = stmt.query([search])?;
        let mut result = vec![];
        while let Some(row) = rows.next()? {
            result.push(row.get(0).unwrap());
        }

        Ok(result)
    }

    pub fn set_updated(&mut self) {
        self.should_update = false;
    }

    #[inline(always)]
    pub fn should_update(&self) -> bool {
        self.should_update
    }
}
