//! An indexer for resources
//! It is responsible to provide fast and flexible search
//! capabilities to the resource store:
//! - Full Text Index of resource description and mime type specific extraction.
//! - Tag indexing

use log::{error, info};
use rusqlite::{Connection, OpenFlags, TransactionBehavior};
use std::path::Path;
use thiserror::Error;
use crate::resource::ResourceId;

#[derive(Error, Debug)]
pub enum SqliteDbError {
    #[error("Rusqlite Error")]
    Rusqlite(#[from] rusqlite::Error),
    #[error("Error upgrading db schema from version `{0}` to version `{1}`")]
    SchemaUpgrade(u32, u32),
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
}

impl Indexer {
    pub fn new<P: AsRef<Path>>(root_dir: P) -> Result<Self, SqliteDbError> {
        let mut path = root_dir.as_ref().to_path_buf();
        path.push("index.sqlite");
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

        Ok(Self { conn })
    }

    pub fn add_resource(&mut self, id: &ResourceId) -> Result<(), SqliteDbError> {
        let now = chrono::Utc::now();
        self.conn
            .execute(
                "INSERT INTO resources (id, frecency, modified) VALUES (?1, ?2, ?3)",
                (id, 0, now),
            )
            .map(|_| ())
            .map_err(|e| e.into())
    }

    pub fn add_tag(&mut self, id: &ResourceId, tag: &str) -> Result<(), SqliteDbError> {
        self.conn
            .execute("INSERT INTO tags (id, tag) VALUES (?1, ?2)", (id, tag))
            .map(|_| ())
            .map_err(|e| e.into())
    }

    pub fn add_text(&mut self, id: &ResourceId, variant: &str, text: &str) -> Result<(), SqliteDbError> {
        // Remove diacritics since the trigram tokenizer of SQlite doesn't have this option.
        let content = secular::lower_lay_string(text);
        self.conn
            .execute(
                "INSERT INTO fts (id, variant, content) VALUES (?1, ?2, ?3)",
                (id, variant, &content),
            )
            .map(|_| ())
            .map_err(|e| e.into())
    }

    pub fn search(&self, text: &str) -> Result<Vec<ResourceId>, SqliteDbError> {
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
}