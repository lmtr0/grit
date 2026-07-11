//! SQLx PostgreSQL/YugaByteDB backend.
//!
//! The schema is intentionally append-friendly and transaction-oriented: objects are content
//! addressed and immutable, while refs are the primary compare-and-swap mutation point.

use async_trait::async_trait;
use grit_lib::objects::{ObjectId, ObjectKind};
use sqlx::{PgPool, Row};

use crate::error::{Error, Result};
use crate::ids::{RepositoryId, TenantId};
use crate::storage::{
    BrowseIndex, ConfigStore, IndexedTreeEntry, ObjectStore, RefStore, ReflogEntry, ReflogStore,
    StoredObject, StoredRef,
};

/// SQL migration statements for the initial server storage schema.
pub const MIGRATIONS: &[&str] = &[
    "create table if not exists grit_repositories (
        tenant_id text not null,
        repository_id text not null,
        hash_algo text not null default 'sha1',
        created_at timestamptz not null default now(),
        primary key (tenant_id, repository_id)
    )",
    "create table if not exists grit_objects (
        tenant_id text not null,
        repository_id text not null,
        oid text not null,
        kind text not null,
        data bytea not null,
        created_at timestamptz not null default now(),
        primary key (tenant_id, repository_id, oid)
    )",
    "create table if not exists grit_refs (
        tenant_id text not null,
        repository_id text not null,
        refname text not null,
        target_oid text,
        symbolic_target text,
        updated_at timestamptz not null default now(),
        primary key (tenant_id, repository_id, refname),
        check ((target_oid is null) <> (symbolic_target is null))
    )",
    "create table if not exists grit_reflog (
        tenant_id text not null,
        repository_id text not null,
        refname text not null,
        sequence bigserial primary key,
        old_oid text not null,
        new_oid text not null,
        actor text not null,
        message text not null,
        written_at timestamptz not null default now()
    )",
    "create index if not exists grit_reflog_repo_ref_idx
        on grit_reflog (tenant_id, repository_id, refname, sequence)",
    "create table if not exists grit_config (
        tenant_id text not null,
        repository_id text not null,
        key text not null,
        value text not null,
        primary key (tenant_id, repository_id, key)
    )",
    "create table if not exists grit_tree_entries (
        tenant_id text not null,
        repository_id text not null,
        tree_oid text not null,
        path text not null,
        mode integer not null,
        oid text not null,
        kind text not null,
        size bigint,
        primary key (tenant_id, repository_id, tree_oid, path)
    )",
];

/// SQLx-backed repository storage using PostgreSQL-compatible SQL.
#[derive(Clone)]
pub struct PgServerStorage {
    pool: PgPool,
}

impl PgServerStorage {
    /// Create storage from a PostgreSQL pool.
    #[must_use]
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Borrow the underlying pool.
    #[must_use]
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Apply the built-in schema migrations.
    ///
    /// # Errors
    ///
    /// Returns SQLx errors from migration statements.
    pub async fn migrate(&self) -> Result<()> {
        for statement in MIGRATIONS {
            sqlx::query(statement).execute(&self.pool).await?;
        }
        Ok(())
    }
}

fn kind_to_name(kind: ObjectKind) -> &'static str {
    match kind {
        ObjectKind::Blob => "blob",
        ObjectKind::Tree => "tree",
        ObjectKind::Commit => "commit",
        ObjectKind::Tag => "tag",
    }
}

fn name_to_kind(name: &str) -> Result<ObjectKind> {
    match name {
        "blob" => Ok(ObjectKind::Blob),
        "tree" => Ok(ObjectKind::Tree),
        "commit" => Ok(ObjectKind::Commit),
        "tag" => Ok(ObjectKind::Tag),
        other => Err(Error::Backend(format!("unknown object kind '{other}'"))),
    }
}

fn row_to_ref(row: &sqlx::postgres::PgRow, refname: &str) -> Result<StoredRef> {
    let target_oid: Option<String> = row.try_get("target_oid")?;
    let symbolic_target: Option<String> = row.try_get("symbolic_target")?;
    match (target_oid, symbolic_target) {
        (Some(oid), None) => Ok(StoredRef::Direct(ObjectId::from_hex(&oid)?)),
        (None, Some(target)) => Ok(StoredRef::Symbolic(target)),
        _ => Err(Error::Backend(format!("invalid ref row for '{refname}'"))),
    }
}

#[async_trait]
impl ObjectStore for PgServerStorage {
    async fn read_object(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        oid: &ObjectId,
    ) -> Result<Option<StoredObject>> {
        let row = sqlx::query(
            "select kind, data from grit_objects
             where tenant_id = $1 and repository_id = $2 and oid = $3",
        )
        .bind(tenant.as_str())
        .bind(repository.as_str())
        .bind(oid.to_hex())
        .fetch_optional(&self.pool)
        .await?;

        row.map(|row| {
            let kind: String = row.try_get("kind")?;
            let data: Vec<u8> = row.try_get("data")?;
            Ok(StoredObject {
                kind: name_to_kind(&kind)?,
                data,
            })
        })
        .transpose()
    }

    async fn write_object(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        oid: &ObjectId,
        object: &StoredObject,
    ) -> Result<()> {
        sqlx::query(
            "insert into grit_objects (tenant_id, repository_id, oid, kind, data)
             values ($1, $2, $3, $4, $5)
             on conflict (tenant_id, repository_id, oid) do nothing",
        )
        .bind(tenant.as_str())
        .bind(repository.as_str())
        .bind(oid.to_hex())
        .bind(kind_to_name(object.kind))
        .bind(&object.data)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn object_exists(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        oid: &ObjectId,
    ) -> Result<bool> {
        let exists: bool = sqlx::query_scalar(
            "select exists(
                select 1 from grit_objects
                where tenant_id = $1 and repository_id = $2 and oid = $3
             )",
        )
        .bind(tenant.as_str())
        .bind(repository.as_str())
        .bind(oid.to_hex())
        .fetch_one(&self.pool)
        .await?;
        Ok(exists)
    }
}

#[async_trait]
impl RefStore for PgServerStorage {
    async fn read_ref(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        refname: &str,
    ) -> Result<Option<StoredRef>> {
        let row = sqlx::query(
            "select target_oid, symbolic_target from grit_refs
             where tenant_id = $1 and repository_id = $2 and refname = $3",
        )
        .bind(tenant.as_str())
        .bind(repository.as_str())
        .bind(refname)
        .fetch_optional(&self.pool)
        .await?;

        row.as_ref().map(|row| row_to_ref(row, refname)).transpose()
    }

    async fn write_ref(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        refname: &str,
        value: &StoredRef,
        expected: Option<Option<StoredRef>>,
    ) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        let current_row = sqlx::query(
            "select target_oid, symbolic_target from grit_refs
             where tenant_id = $1 and repository_id = $2 and refname = $3
             for update",
        )
        .bind(tenant.as_str())
        .bind(repository.as_str())
        .bind(refname)
        .fetch_optional(&mut *tx)
        .await?;
        let current = current_row
            .as_ref()
            .map(|row| row_to_ref(row, refname))
            .transpose()?;
        if let Some(expected) = expected {
            if current != expected {
                return Err(Error::RefConflict(refname.to_owned()));
            }
        }
        let (target_oid, symbolic_target) = match value {
            StoredRef::Direct(oid) => (Some(oid.to_hex()), None),
            StoredRef::Symbolic(target) => (None, Some(target.clone())),
        };
        sqlx::query(
            "insert into grit_refs
                (tenant_id, repository_id, refname, target_oid, symbolic_target, updated_at)
             values ($1, $2, $3, $4, $5, now())
             on conflict (tenant_id, repository_id, refname)
             do update set target_oid = excluded.target_oid,
                           symbolic_target = excluded.symbolic_target,
                           updated_at = now()",
        )
        .bind(tenant.as_str())
        .bind(repository.as_str())
        .bind(refname)
        .bind(target_oid)
        .bind(symbolic_target)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(())
    }

    async fn delete_ref(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        refname: &str,
        expected: Option<StoredRef>,
    ) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        let current_row = sqlx::query(
            "select target_oid, symbolic_target from grit_refs
             where tenant_id = $1 and repository_id = $2 and refname = $3
             for update",
        )
        .bind(tenant.as_str())
        .bind(repository.as_str())
        .bind(refname)
        .fetch_optional(&mut *tx)
        .await?;
        let current = current_row
            .as_ref()
            .map(|row| row_to_ref(row, refname))
            .transpose()?;
        if let Some(expected) = expected {
            if current != Some(expected) {
                return Err(Error::RefConflict(refname.to_owned()));
            }
        }
        sqlx::query(
            "delete from grit_refs
             where tenant_id = $1 and repository_id = $2 and refname = $3",
        )
        .bind(tenant.as_str())
        .bind(repository.as_str())
        .bind(refname)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(())
    }

    async fn list_refs(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        prefix: &str,
    ) -> Result<Vec<(String, StoredRef)>> {
        let rows = sqlx::query(
            "select refname, target_oid, symbolic_target from grit_refs
             where tenant_id = $1 and repository_id = $2 and refname like $3
             order by refname",
        )
        .bind(tenant.as_str())
        .bind(repository.as_str())
        .bind(format!("{prefix}%"))
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter()
            .map(|row| {
                let refname: String = row.try_get("refname")?;
                let value = row_to_ref(&row, &refname)?;
                Ok((refname, value))
            })
            .collect()
    }
}

#[async_trait]
impl ReflogStore for PgServerStorage {
    async fn append_reflog(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        entry: &ReflogEntry,
    ) -> Result<()> {
        sqlx::query(
            "insert into grit_reflog
                (tenant_id, repository_id, refname, old_oid, new_oid, actor, message, written_at)
             values ($1, $2, $3, $4, $5, $6, $7, $8)",
        )
        .bind(tenant.as_str())
        .bind(repository.as_str())
        .bind(&entry.refname)
        .bind(entry.old_oid.to_hex())
        .bind(entry.new_oid.to_hex())
        .bind(&entry.actor)
        .bind(&entry.message)
        .bind(entry.timestamp)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn read_reflog(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        refname: &str,
    ) -> Result<Vec<ReflogEntry>> {
        let rows = sqlx::query(
            "select old_oid, new_oid, actor, message, written_at from grit_reflog
             where tenant_id = $1 and repository_id = $2 and refname = $3
             order by sequence",
        )
        .bind(tenant.as_str())
        .bind(repository.as_str())
        .bind(refname)
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter()
            .map(|row| {
                let old_oid: String = row.try_get("old_oid")?;
                let new_oid: String = row.try_get("new_oid")?;
                Ok(ReflogEntry {
                    refname: refname.to_owned(),
                    old_oid: ObjectId::from_hex(&old_oid)?,
                    new_oid: ObjectId::from_hex(&new_oid)?,
                    actor: row.try_get("actor")?,
                    timestamp: row.try_get("written_at")?,
                    message: row.try_get("message")?,
                })
            })
            .collect()
    }
}

#[async_trait]
impl ConfigStore for PgServerStorage {
    async fn get_config(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        key: &str,
    ) -> Result<Option<String>> {
        sqlx::query_scalar(
            "select value from grit_config
             where tenant_id = $1 and repository_id = $2 and key = $3",
        )
        .bind(tenant.as_str())
        .bind(repository.as_str())
        .bind(key)
        .fetch_optional(&self.pool)
        .await
        .map_err(Into::into)
    }

    async fn set_config(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        key: &str,
        value: &str,
    ) -> Result<()> {
        sqlx::query(
            "insert into grit_config (tenant_id, repository_id, key, value)
             values ($1, $2, $3, $4)
             on conflict (tenant_id, repository_id, key)
             do update set value = excluded.value",
        )
        .bind(tenant.as_str())
        .bind(repository.as_str())
        .bind(key)
        .bind(value)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn list_config(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        prefix: &str,
    ) -> Result<Vec<(String, String)>> {
        let rows = sqlx::query(
            "select key, value from grit_config
             where tenant_id = $1 and repository_id = $2 and key like $3
             order by key",
        )
        .bind(tenant.as_str())
        .bind(repository.as_str())
        .bind(format!("{prefix}%"))
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter()
            .map(|row| Ok((row.try_get("key")?, row.try_get("value")?)))
            .collect()
    }
}

#[async_trait]
impl BrowseIndex for PgServerStorage {
    async fn upsert_tree_entries(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        entries: &[IndexedTreeEntry],
    ) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        for entry in entries {
            let size = entry
                .size
                .map(i64::try_from)
                .transpose()
                .map_err(|_| Error::Backend("tree entry size exceeds i64".to_owned()))?;
            sqlx::query(
                "insert into grit_tree_entries
                    (tenant_id, repository_id, tree_oid, path, mode, oid, kind, size)
                 values ($1, $2, $3, $4, $5, $6, $7, $8)
                 on conflict (tenant_id, repository_id, tree_oid, path)
                 do update set mode = excluded.mode,
                               oid = excluded.oid,
                               kind = excluded.kind,
                               size = excluded.size",
            )
            .bind(tenant.as_str())
            .bind(repository.as_str())
            .bind(entry.tree_oid.to_hex())
            .bind(&entry.path)
            .bind(entry.mode as i32)
            .bind(entry.oid.to_hex())
            .bind(kind_to_name(entry.kind))
            .bind(size)
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        Ok(())
    }

    async fn list_tree_entries(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        tree_oid: &ObjectId,
        prefix: &str,
    ) -> Result<Vec<IndexedTreeEntry>> {
        let rows = sqlx::query(
            "select tree_oid, path, mode, oid, kind, size from grit_tree_entries
             where tenant_id = $1 and repository_id = $2 and tree_oid = $3 and path like $4
             order by path",
        )
        .bind(tenant.as_str())
        .bind(repository.as_str())
        .bind(tree_oid.to_hex())
        .bind(format!("{prefix}%"))
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter()
            .map(|row| {
                let tree_oid: String = row.try_get("tree_oid")?;
                let oid: String = row.try_get("oid")?;
                let kind: String = row.try_get("kind")?;
                let mode: i32 = row.try_get("mode")?;
                let size: Option<i64> = row.try_get("size")?;
                if mode < 0 {
                    return Err(Error::Backend("tree entry mode is negative".to_owned()));
                }
                Ok(IndexedTreeEntry {
                    tree_oid: ObjectId::from_hex(&tree_oid)?,
                    path: row.try_get("path")?,
                    mode: mode as u32,
                    oid: ObjectId::from_hex(&oid)?,
                    kind: name_to_kind(&kind)?,
                    size: size.map(|value| value as u64),
                })
            })
            .collect()
    }

    async fn read_blob_at_path(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        tree_oid: &ObjectId,
        path: &str,
    ) -> Result<Option<StoredObject>> {
        let oid: Option<String> = sqlx::query_scalar(
            "select oid from grit_tree_entries
             where tenant_id = $1 and repository_id = $2 and tree_oid = $3 and path = $4",
        )
        .bind(tenant.as_str())
        .bind(repository.as_str())
        .bind(tree_oid.to_hex())
        .bind(path)
        .fetch_optional(&self.pool)
        .await?;

        match oid {
            Some(oid) => {
                self.read_object(tenant, repository, &ObjectId::from_hex(&oid)?)
                    .await
            }
            None => Ok(None),
        }
    }
}
