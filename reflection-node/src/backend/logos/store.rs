//! Persistence of operations for the Logos backend.
//!
//! One row per operation. Operations are p2panda operations; this table only indexes them by
//! topic, author and sequence number, which is all catch-up and replay need.
use p2panda::{Hash, Topic, VerifyingKey};
use sqlx::{Row, SqlitePool};

use super::Op;

#[derive(Clone, Debug)]
pub(crate) struct OpStore {
    pool: SqlitePool,
}

impl OpStore {
    pub async fn new(pool: SqlitePool) -> Result<Self, sqlx::Error> {
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS logos_operations (
                hash    TEXT    NOT NULL PRIMARY KEY,
                topic   TEXT    NOT NULL,
                author  TEXT    NOT NULL,
                seq     INTEGER NOT NULL,
                header  BLOB    NOT NULL,
                body    BLOB
            )",
        )
        .execute(&pool)
        .await?;
        sqlx::query(
            "CREATE INDEX IF NOT EXISTS logos_operations_log
                ON logos_operations (topic, author, seq)",
        )
        .execute(&pool)
        .await?;
        Ok(Self { pool })
    }

    pub async fn has(&self, hash: &Hash) -> Result<bool, sqlx::Error> {
        let row = sqlx::query("SELECT 1 FROM logos_operations WHERE hash = ?")
            .bind(hash.to_hex())
            .fetch_optional(&self.pool)
            .await?;
        Ok(row.is_some())
    }

    /// Insert an operation. If it prunes, earlier operations of the same log are removed.
    pub async fn insert(&self, topic: &Topic, op: &Op) -> Result<(), sqlx::Error> {
        let mut tx = self.pool.begin().await?;
        sqlx::query(
            "INSERT OR IGNORE INTO logos_operations (hash, topic, author, seq, header, body)
                VALUES (?, ?, ?, ?, ?, ?)",
        )
        .bind(op.hash.to_hex())
        .bind(topic_key(topic))
        .bind(op.author().to_hex())
        .bind(op.seq() as i64)
        .bind(&op.header_bytes)
        .bind(op.body.as_deref())
        .execute(&mut *tx)
        .await?;
        if op.prunes() {
            sqlx::query("DELETE FROM logos_operations WHERE topic = ? AND author = ? AND seq < ?")
                .bind(topic_key(topic))
                .bind(op.author().to_hex())
                .bind(op.seq() as i64)
                .execute(&mut *tx)
                .await?;
        }
        tx.commit().await
    }

    /// The latest operation of every author on a topic.
    pub async fn heads(&self, topic: &Topic) -> Result<Vec<Op>, sqlx::Error> {
        let rows = sqlx::query(
            "SELECT header, body FROM logos_operations o
                WHERE topic = ?1 AND seq = (
                    SELECT MAX(seq) FROM logos_operations
                    WHERE topic = ?1 AND author = o.author
                )",
        )
        .bind(topic_key(topic))
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.iter().filter_map(row_to_op).collect())
    }

    /// The latest operation of one author on a topic.
    pub async fn head(
        &self,
        topic: &Topic,
        author: &VerifyingKey,
    ) -> Result<Option<Op>, sqlx::Error> {
        let row = sqlx::query(
            "SELECT header, body FROM logos_operations
                WHERE topic = ? AND author = ? ORDER BY seq DESC LIMIT 1",
        )
        .bind(topic_key(topic))
        .bind(author.to_hex())
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.as_ref().and_then(row_to_op))
    }

    /// Operations of one author after a sequence number (all of them for `None`), in order.
    pub async fn after(
        &self,
        topic: &Topic,
        author: &VerifyingKey,
        after: Option<u64>,
    ) -> Result<Vec<Op>, sqlx::Error> {
        let rows = sqlx::query(
            "SELECT header, body FROM logos_operations
                WHERE topic = ? AND author = ? AND seq > ? ORDER BY seq",
        )
        .bind(topic_key(topic))
        .bind(author.to_hex())
        .bind(after.map(|seq| seq as i64).unwrap_or(-1))
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.iter().filter_map(row_to_op).collect())
    }

    /// Every operation on a topic, each author's log in order.
    pub async fn all(&self, topic: &Topic) -> Result<Vec<Op>, sqlx::Error> {
        let rows = sqlx::query(
            "SELECT header, body FROM logos_operations WHERE topic = ? ORDER BY author, seq",
        )
        .bind(topic_key(topic))
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.iter().filter_map(row_to_op).collect())
    }
}

fn topic_key(topic: &Topic) -> String {
    hex::encode(topic.as_bytes())
}

fn row_to_op(row: &sqlx::sqlite::SqliteRow) -> Option<Op> {
    let header: Vec<u8> = row.try_get("header").ok()?;
    let body: Option<Vec<u8>> = row.try_get("body").ok()?;
    Op::decode(header, body)
}
