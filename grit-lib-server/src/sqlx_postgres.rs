//! SQLx PostgreSQL/YugaByteDB backend.
//!
//! The schema is intentionally append-friendly and transaction-oriented: objects are content
//! addressed and immutable, while refs are the primary compare-and-swap mutation point.

use async_trait::async_trait;
use grit_lib::objects::{HashAlgo, ObjectId, ObjectKind};
use sqlx::{PgPool, Postgres, Row, Transaction};
use time::OffsetDateTime;

use crate::error::{Error, Result};
use crate::ids::{RepositoryId, TenantId};
use crate::storage::{
    BrowseIndex, CommitGraphStore, ConfigStore, IndexedCommit, IndexedTreeEntry, ObjectStore,
    RefStore, ReflogEntry, ReflogStore, StoredObject, StoredRef,
};

/// SQL migration statements for the initial server storage schema.
pub const MIGRATIONS: &[&str] = &[
    "create table if not exists grit_repositories (
        tenant_id text not null,
        repository_id text not null,
        hash_algo text not null default 'sha1',
        archived_at timestamptz,
        deleted_at timestamptz,
        created_at timestamptz not null default now(),
        updated_at timestamptz not null default now(),
        primary key (tenant_id, repository_id)
    )",
    "alter table grit_repositories
        add column if not exists archived_at timestamptz",
    "alter table grit_repositories
        add column if not exists deleted_at timestamptz",
    "alter table grit_repositories
        add column if not exists updated_at timestamptz not null default now()",
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
    "create table if not exists grit_commits (
        tenant_id text not null,
        repository_id text not null,
        commit_oid text not null,
        tree_oid text not null,
        commit_time bigint not null,
        generation integer not null,
        primary key (tenant_id, repository_id, commit_oid)
    )",
    "create table if not exists grit_commit_parents (
        tenant_id text not null,
        repository_id text not null,
        commit_oid text not null,
        parent_oid text not null,
        parent_order integer not null,
        primary key (tenant_id, repository_id, commit_oid, parent_order)
    )",
    "create index if not exists grit_repositories_listing_idx
        on grit_repositories (tenant_id, archived_at, repository_id)
        where deleted_at is null",
    "create index if not exists grit_objects_repo_kind_idx
        on grit_objects (tenant_id, repository_id, kind, oid)",
    "create index if not exists grit_refs_repo_prefix_idx
        on grit_refs (tenant_id, repository_id, refname text_pattern_ops)",
    "create index if not exists grit_tree_entries_repo_prefix_idx
        on grit_tree_entries (tenant_id, repository_id, tree_oid, path text_pattern_ops)",
    "create index if not exists grit_commits_repo_time_idx
        on grit_commits (tenant_id, repository_id, commit_time desc, commit_oid)",
    "create index if not exists grit_commit_parents_parent_idx
        on grit_commit_parents (tenant_id, repository_id, parent_oid, commit_oid)",
];

/// Repository metadata stored by the SQL backend.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PgRepositoryRow {
    /// Tenant that owns the repository row.
    pub tenant: TenantId,
    /// Repository identifier within the tenant.
    pub repository: RepositoryId,
    /// Object hash algorithm used by the repository.
    pub hash_algo: HashAlgo,
    /// Timestamp when the repository row was created.
    pub created_at: OffsetDateTime,
    /// Timestamp when repository metadata was last changed.
    pub updated_at: OffsetDateTime,
    /// Timestamp when the repository was archived, if archived.
    pub archived_at: Option<OffsetDateTime>,
    /// Timestamp when the repository was soft-deleted, if retained.
    pub deleted_at: Option<OffsetDateTime>,
}

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

    /// Create a repository metadata row for a tenant-scoped repository.
    ///
    /// # Errors
    ///
    /// Returns [`Error::RepositoryAlreadyExists`] if the row already exists, or SQLx errors from
    /// the backend.
    pub async fn create_repository(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        hash_algo: HashAlgo,
    ) -> Result<PgRepositoryRow> {
        let row = sqlx::query(
            "insert into grit_repositories
                (tenant_id, repository_id, hash_algo, created_at, updated_at)
             values ($1, $2, $3, now(), now())
             on conflict (tenant_id, repository_id) do nothing
             returning tenant_id, repository_id, hash_algo, created_at, updated_at,
                       archived_at, deleted_at",
        )
        .bind(tenant.as_str())
        .bind(repository.as_str())
        .bind(hash_algo.name())
        .fetch_optional(&self.pool)
        .await?;

        let Some(row) = row else {
            return Err(Error::RepositoryAlreadyExists(format!(
                "{tenant}/{repository}"
            )));
        };
        row_to_repository(&row)
    }

    /// Read a repository metadata row when it exists and has not been deleted.
    ///
    /// # Errors
    ///
    /// Returns SQLx errors from the backend, invalid stored identifiers, or invalid hash
    /// algorithm metadata.
    pub async fn read_repository(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
    ) -> Result<Option<PgRepositoryRow>> {
        let row = sqlx::query(
            "select tenant_id, repository_id, hash_algo, created_at, updated_at,
                    archived_at, deleted_at
             from grit_repositories
             where tenant_id = $1 and repository_id = $2 and deleted_at is null",
        )
        .bind(tenant.as_str())
        .bind(repository.as_str())
        .fetch_optional(&self.pool)
        .await?;
        row.as_ref().map(row_to_repository).transpose()
    }

    /// List repository metadata rows for a tenant.
    ///
    /// # Errors
    ///
    /// Returns SQLx errors from the backend, invalid stored identifiers, or invalid hash
    /// algorithm metadata.
    pub async fn list_repositories(&self, tenant: &TenantId) -> Result<Vec<PgRepositoryRow>> {
        let rows = sqlx::query(
            "select tenant_id, repository_id, hash_algo, created_at, updated_at,
                    archived_at, deleted_at
             from grit_repositories
             where tenant_id = $1 and deleted_at is null
             order by repository_id",
        )
        .bind(tenant.as_str())
        .fetch_all(&self.pool)
        .await?;

        rows.iter().map(row_to_repository).collect()
    }

    /// Rename a repository and every scoped SQL row owned by it.
    ///
    /// # Errors
    ///
    /// Returns [`Error::RepositoryNotFound`] if the source repository does not exist,
    /// [`Error::RepositoryAlreadyExists`] if the destination exists, or SQLx errors from the
    /// backend.
    pub async fn rename_repository(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        new_repository: &RepositoryId,
    ) -> Result<PgRepositoryRow> {
        let mut tx = self.pool.begin().await?;
        let row =
            rename_repository_in_transaction(&mut tx, tenant, repository, new_repository).await?;
        tx.commit().await?;
        Ok(row)
    }

    /// Mark a repository as archived.
    ///
    /// # Errors
    ///
    /// Returns [`Error::RepositoryNotFound`] if the repository does not exist, or SQLx errors
    /// from the backend.
    pub async fn archive_repository(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
    ) -> Result<PgRepositoryRow> {
        let row = sqlx::query(
            "update grit_repositories
             set archived_at = coalesce(archived_at, now()), updated_at = now()
             where tenant_id = $1 and repository_id = $2 and deleted_at is null
             returning tenant_id, repository_id, hash_algo, created_at, updated_at,
                       archived_at, deleted_at",
        )
        .bind(tenant.as_str())
        .bind(repository.as_str())
        .fetch_optional(&self.pool)
        .await?;

        let Some(row) = row else {
            return Err(Error::RepositoryNotFound(format!("{tenant}/{repository}")));
        };
        row_to_repository(&row)
    }

    /// Hard-delete a repository and every scoped SQL row owned by it.
    ///
    /// # Errors
    ///
    /// Returns [`Error::RepositoryNotFound`] if the repository does not exist, or SQLx errors
    /// from the backend.
    pub async fn delete_repository(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
    ) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        delete_repository_in_transaction(&mut tx, tenant, repository).await?;
        tx.commit().await?;
        Ok(())
    }

    /// Start a SQL transaction for multi-table repository writes.
    ///
    /// # Errors
    ///
    /// Returns SQLx errors when the backend cannot acquire a transaction.
    pub async fn transaction(&self) -> Result<PgServerStorageTransaction> {
        Ok(PgServerStorageTransaction {
            tx: self.pool.begin().await?,
        })
    }

    /// Write a ref and append its reflog entry in one SQL transaction.
    ///
    /// # Errors
    ///
    /// Returns [`Error::RefConflict`] when the compare-and-swap guard fails, backend validation
    /// errors when the reflog does not match the ref, or SQLx errors from the backend.
    pub async fn write_ref_with_reflog(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        refname: &str,
        value: &StoredRef,
        expected: Option<Option<StoredRef>>,
        entry: &ReflogEntry,
    ) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        write_ref_with_reflog_in_transaction(
            &mut tx, tenant, repository, refname, value, expected, entry,
        )
        .await?;
        tx.commit().await?;
        Ok(())
    }
}

/// SQL transaction wrapper for multi-table writes.
pub struct PgServerStorageTransaction {
    tx: Transaction<'static, Postgres>,
}

impl PgServerStorageTransaction {
    /// Commit all writes performed through this transaction.
    ///
    /// # Errors
    ///
    /// Returns SQLx errors from the backend if commit fails.
    pub async fn commit(self) -> Result<()> {
        self.tx.commit().await?;
        Ok(())
    }

    /// Roll back all writes performed through this transaction.
    ///
    /// # Errors
    ///
    /// Returns SQLx errors from the backend if rollback fails.
    pub async fn rollback(self) -> Result<()> {
        self.tx.rollback().await?;
        Ok(())
    }

    /// Rename a repository and every scoped SQL row owned by it.
    ///
    /// # Errors
    ///
    /// Returns repository lifecycle errors or SQLx errors from the backend.
    pub async fn rename_repository(
        &mut self,
        tenant: &TenantId,
        repository: &RepositoryId,
        new_repository: &RepositoryId,
    ) -> Result<PgRepositoryRow> {
        rename_repository_in_transaction(&mut self.tx, tenant, repository, new_repository).await
    }

    /// Hard-delete a repository and every scoped SQL row owned by it.
    ///
    /// # Errors
    ///
    /// Returns repository lifecycle errors or SQLx errors from the backend.
    pub async fn delete_repository(
        &mut self,
        tenant: &TenantId,
        repository: &RepositoryId,
    ) -> Result<()> {
        delete_repository_in_transaction(&mut self.tx, tenant, repository).await
    }

    /// Insert an object idempotently within the transaction.
    ///
    /// # Errors
    ///
    /// Returns SQLx errors from the backend.
    pub async fn write_object(
        &mut self,
        tenant: &TenantId,
        repository: &RepositoryId,
        oid: &ObjectId,
        object: &StoredObject,
    ) -> Result<()> {
        write_object_in_transaction(&mut self.tx, tenant, repository, oid, object).await
    }

    /// Write a ref within the transaction using compare-and-swap semantics.
    ///
    /// # Errors
    ///
    /// Returns [`Error::RefConflict`] when the compare-and-swap guard fails, or SQLx errors from
    /// the backend.
    pub async fn write_ref(
        &mut self,
        tenant: &TenantId,
        repository: &RepositoryId,
        refname: &str,
        value: &StoredRef,
        expected: Option<Option<StoredRef>>,
    ) -> Result<()> {
        write_ref_in_transaction(&mut self.tx, tenant, repository, refname, value, expected).await
    }

    /// Delete a ref within the transaction using compare-and-swap semantics.
    ///
    /// # Errors
    ///
    /// Returns [`Error::RefConflict`] when the compare-and-swap guard fails, or SQLx errors from
    /// the backend.
    pub async fn delete_ref(
        &mut self,
        tenant: &TenantId,
        repository: &RepositoryId,
        refname: &str,
        expected: Option<StoredRef>,
    ) -> Result<()> {
        delete_ref_in_transaction(&mut self.tx, tenant, repository, refname, expected).await
    }

    /// Append a reflog entry within the transaction.
    ///
    /// # Errors
    ///
    /// Returns SQLx errors from the backend.
    pub async fn append_reflog(
        &mut self,
        tenant: &TenantId,
        repository: &RepositoryId,
        entry: &ReflogEntry,
    ) -> Result<()> {
        append_reflog_in_transaction(&mut self.tx, tenant, repository, entry).await
    }

    /// Write a ref and append its reflog entry in this transaction.
    ///
    /// # Errors
    ///
    /// Returns [`Error::RefConflict`] when the compare-and-swap guard fails, backend validation
    /// errors when the reflog does not match the ref, or SQLx errors from the backend.
    pub async fn write_ref_with_reflog(
        &mut self,
        tenant: &TenantId,
        repository: &RepositoryId,
        refname: &str,
        value: &StoredRef,
        expected: Option<Option<StoredRef>>,
        entry: &ReflogEntry,
    ) -> Result<()> {
        write_ref_with_reflog_in_transaction(
            &mut self.tx,
            tenant,
            repository,
            refname,
            value,
            expected,
            entry,
        )
        .await
    }

    /// Set or replace a config key within the transaction.
    ///
    /// # Errors
    ///
    /// Returns SQLx errors from the backend.
    pub async fn set_config(
        &mut self,
        tenant: &TenantId,
        repository: &RepositoryId,
        key: &str,
        value: &str,
    ) -> Result<()> {
        set_config_in_transaction(&mut self.tx, tenant, repository, key, value).await
    }

    /// Upsert tree entries within the transaction.
    ///
    /// # Errors
    ///
    /// Returns backend validation errors when entry sizes exceed SQL limits, or SQLx errors from
    /// the backend.
    pub async fn upsert_tree_entries(
        &mut self,
        tenant: &TenantId,
        repository: &RepositoryId,
        entries: &[IndexedTreeEntry],
    ) -> Result<()> {
        upsert_tree_entries_in_transaction(&mut self.tx, tenant, repository, entries).await
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

fn row_to_repository(row: &sqlx::postgres::PgRow) -> Result<PgRepositoryRow> {
    let tenant: String = row.try_get("tenant_id")?;
    let repository: String = row.try_get("repository_id")?;
    let hash_algo: String = row.try_get("hash_algo")?;
    let hash_algo = HashAlgo::from_name(&hash_algo)
        .ok_or_else(|| Error::Backend(format!("unknown hash algorithm '{hash_algo}'")))?;
    Ok(PgRepositoryRow {
        tenant: TenantId::new(tenant)?,
        repository: RepositoryId::new(repository)?,
        hash_algo,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
        archived_at: row.try_get("archived_at")?,
        deleted_at: row.try_get("deleted_at")?,
    })
}

fn repository_key(tenant: &TenantId, repository: &RepositoryId) -> String {
    format!("{tenant}/{repository}")
}

fn ref_columns(value: &StoredRef) -> (Option<String>, Option<String>) {
    match value {
        StoredRef::Direct(oid) => (Some(oid.to_hex()), None),
        StoredRef::Symbolic(target) => (None, Some(target.clone())),
    }
}

async fn rename_repository_in_transaction(
    tx: &mut Transaction<'static, Postgres>,
    tenant: &TenantId,
    repository: &RepositoryId,
    new_repository: &RepositoryId,
) -> Result<PgRepositoryRow> {
    let source_exists: Option<i32> = sqlx::query_scalar(
        "select 1 from grit_repositories
         where tenant_id = $1 and repository_id = $2 and deleted_at is null
         for update",
    )
    .bind(tenant.as_str())
    .bind(repository.as_str())
    .fetch_optional(&mut **tx)
    .await?;
    if source_exists.is_none() {
        return Err(Error::RepositoryNotFound(repository_key(
            tenant, repository,
        )));
    }

    let destination_exists: Option<i32> = sqlx::query_scalar(
        "select 1 from grit_repositories
         where tenant_id = $1 and repository_id = $2
         for update",
    )
    .bind(tenant.as_str())
    .bind(new_repository.as_str())
    .fetch_optional(&mut **tx)
    .await?;
    if destination_exists.is_some() {
        return Err(Error::RepositoryAlreadyExists(repository_key(
            tenant,
            new_repository,
        )));
    }

    for table in [
        "grit_objects",
        "grit_refs",
        "grit_reflog",
        "grit_config",
        "grit_tree_entries",
        "grit_commits",
        "grit_commit_parents",
    ] {
        let sql = format!(
            "update {table}
             set repository_id = $3
             where tenant_id = $1 and repository_id = $2"
        );
        sqlx::query(&sql)
            .bind(tenant.as_str())
            .bind(repository.as_str())
            .bind(new_repository.as_str())
            .execute(&mut **tx)
            .await?;
    }

    let row = sqlx::query(
        "update grit_repositories
         set repository_id = $3, updated_at = now()
         where tenant_id = $1 and repository_id = $2 and deleted_at is null
         returning tenant_id, repository_id, hash_algo, created_at, updated_at,
                   archived_at, deleted_at",
    )
    .bind(tenant.as_str())
    .bind(repository.as_str())
    .bind(new_repository.as_str())
    .fetch_one(&mut **tx)
    .await?;
    row_to_repository(&row)
}

async fn delete_repository_in_transaction(
    tx: &mut Transaction<'static, Postgres>,
    tenant: &TenantId,
    repository: &RepositoryId,
) -> Result<()> {
    let row: Option<i32> = sqlx::query_scalar(
        "select 1 from grit_repositories
         where tenant_id = $1 and repository_id = $2 and deleted_at is null
         for update",
    )
    .bind(tenant.as_str())
    .bind(repository.as_str())
    .fetch_optional(&mut **tx)
    .await?;
    if row.is_none() {
        return Err(Error::RepositoryNotFound(repository_key(
            tenant, repository,
        )));
    }

    for table in [
        "grit_objects",
        "grit_refs",
        "grit_reflog",
        "grit_config",
        "grit_tree_entries",
        "grit_commits",
        "grit_commit_parents",
    ] {
        let sql = format!("delete from {table} where tenant_id = $1 and repository_id = $2");
        sqlx::query(&sql)
            .bind(tenant.as_str())
            .bind(repository.as_str())
            .execute(&mut **tx)
            .await?;
    }
    sqlx::query(
        "delete from grit_repositories
         where tenant_id = $1 and repository_id = $2",
    )
    .bind(tenant.as_str())
    .bind(repository.as_str())
    .execute(&mut **tx)
    .await?;
    Ok(())
}

async fn write_object_in_transaction(
    tx: &mut Transaction<'static, Postgres>,
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
    .execute(&mut **tx)
    .await?;
    Ok(())
}

async fn write_ref_in_transaction(
    tx: &mut Transaction<'static, Postgres>,
    tenant: &TenantId,
    repository: &RepositoryId,
    refname: &str,
    value: &StoredRef,
    expected: Option<Option<StoredRef>>,
) -> Result<()> {
    let (target_oid, symbolic_target) = ref_columns(value);
    match expected {
        None => {
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
            .execute(&mut **tx)
            .await?;
            Ok(())
        }
        Some(None) => {
            let rows = sqlx::query(
                "insert into grit_refs
                    (tenant_id, repository_id, refname, target_oid, symbolic_target, updated_at)
                 values ($1, $2, $3, $4, $5, now())
                 on conflict (tenant_id, repository_id, refname) do nothing",
            )
            .bind(tenant.as_str())
            .bind(repository.as_str())
            .bind(refname)
            .bind(target_oid)
            .bind(symbolic_target)
            .execute(&mut **tx)
            .await?;
            if rows.rows_affected() == 1 {
                Ok(())
            } else {
                Err(Error::RefConflict(refname.to_owned()))
            }
        }
        Some(Some(expected)) => {
            let rows = match expected {
                StoredRef::Direct(expected_oid) => {
                    sqlx::query(
                        "update grit_refs
                         set target_oid = $4, symbolic_target = $5, updated_at = now()
                         where tenant_id = $1 and repository_id = $2 and refname = $3
                           and target_oid = $6 and symbolic_target is null",
                    )
                    .bind(tenant.as_str())
                    .bind(repository.as_str())
                    .bind(refname)
                    .bind(target_oid)
                    .bind(symbolic_target)
                    .bind(expected_oid.to_hex())
                    .execute(&mut **tx)
                    .await?
                }
                StoredRef::Symbolic(expected_target) => {
                    sqlx::query(
                        "update grit_refs
                         set target_oid = $4, symbolic_target = $5, updated_at = now()
                         where tenant_id = $1 and repository_id = $2 and refname = $3
                           and target_oid is null and symbolic_target = $6",
                    )
                    .bind(tenant.as_str())
                    .bind(repository.as_str())
                    .bind(refname)
                    .bind(target_oid)
                    .bind(symbolic_target)
                    .bind(expected_target)
                    .execute(&mut **tx)
                    .await?
                }
            };
            if rows.rows_affected() == 1 {
                Ok(())
            } else {
                Err(Error::RefConflict(refname.to_owned()))
            }
        }
    }
}

async fn delete_ref_in_transaction(
    tx: &mut Transaction<'static, Postgres>,
    tenant: &TenantId,
    repository: &RepositoryId,
    refname: &str,
    expected: Option<StoredRef>,
) -> Result<()> {
    let guarded = expected.is_some();
    let rows = match expected {
        None => {
            sqlx::query(
                "delete from grit_refs
                 where tenant_id = $1 and repository_id = $2 and refname = $3",
            )
            .bind(tenant.as_str())
            .bind(repository.as_str())
            .bind(refname)
            .execute(&mut **tx)
            .await?
        }
        Some(StoredRef::Direct(expected_oid)) => {
            sqlx::query(
                "delete from grit_refs
                 where tenant_id = $1 and repository_id = $2 and refname = $3
                   and target_oid = $4 and symbolic_target is null",
            )
            .bind(tenant.as_str())
            .bind(repository.as_str())
            .bind(refname)
            .bind(expected_oid.to_hex())
            .execute(&mut **tx)
            .await?
        }
        Some(StoredRef::Symbolic(expected_target)) => {
            sqlx::query(
                "delete from grit_refs
                 where tenant_id = $1 and repository_id = $2 and refname = $3
                   and target_oid is null and symbolic_target = $4",
            )
            .bind(tenant.as_str())
            .bind(repository.as_str())
            .bind(refname)
            .bind(expected_target)
            .execute(&mut **tx)
            .await?
        }
    };
    if guarded && rows.rows_affected() != 1 {
        return Err(Error::RefConflict(refname.to_owned()));
    }
    Ok(())
}

async fn append_reflog_in_transaction(
    tx: &mut Transaction<'static, Postgres>,
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
    .execute(&mut **tx)
    .await?;
    Ok(())
}

async fn write_ref_with_reflog_in_transaction(
    tx: &mut Transaction<'static, Postgres>,
    tenant: &TenantId,
    repository: &RepositoryId,
    refname: &str,
    value: &StoredRef,
    expected: Option<Option<StoredRef>>,
    entry: &ReflogEntry,
) -> Result<()> {
    if entry.refname != refname {
        return Err(Error::Backend(format!(
            "reflog entry ref '{}' does not match update ref '{refname}'",
            entry.refname
        )));
    }
    if let StoredRef::Direct(oid) = value {
        if entry.new_oid != *oid {
            return Err(Error::Backend(format!(
                "reflog new oid does not match direct ref '{refname}'"
            )));
        }
    }
    write_ref_in_transaction(tx, tenant, repository, refname, value, expected).await?;
    append_reflog_in_transaction(tx, tenant, repository, entry).await
}

async fn set_config_in_transaction(
    tx: &mut Transaction<'static, Postgres>,
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
    .execute(&mut **tx)
    .await?;
    Ok(())
}

async fn upsert_tree_entries_in_transaction(
    tx: &mut Transaction<'static, Postgres>,
    tenant: &TenantId,
    repository: &RepositoryId,
    entries: &[IndexedTreeEntry],
) -> Result<()> {
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
        .execute(&mut **tx)
        .await?;
    }
    Ok(())
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
        let mut tx = self.pool.begin().await?;
        write_object_in_transaction(&mut tx, tenant, repository, oid, object).await?;
        tx.commit().await?;
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

    async fn count_objects(&self, tenant: &TenantId, repository: &RepositoryId) -> Result<usize> {
        let count: i64 = sqlx::query_scalar(
            "select count(*) from grit_objects
             where tenant_id = $1 and repository_id = $2",
        )
        .bind(tenant.as_str())
        .bind(repository.as_str())
        .fetch_one(&self.pool)
        .await?;
        usize::try_from(count).map_err(|_| Error::Backend("object count exceeds usize".to_owned()))
    }

    async fn list_object_ids(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        kind: Option<ObjectKind>,
    ) -> Result<Vec<(ObjectId, ObjectKind)>> {
        let rows = if let Some(kind) = kind {
            sqlx::query(
                "select oid, kind from grit_objects
                 where tenant_id = $1 and repository_id = $2 and kind = $3
                 order by oid",
            )
            .bind(tenant.as_str())
            .bind(repository.as_str())
            .bind(kind_to_name(kind))
            .fetch_all(&self.pool)
            .await?
        } else {
            sqlx::query(
                "select oid, kind from grit_objects
                 where tenant_id = $1 and repository_id = $2
                 order by oid",
            )
            .bind(tenant.as_str())
            .bind(repository.as_str())
            .fetch_all(&self.pool)
            .await?
        };

        rows.into_iter()
            .map(|row| {
                let oid: String = row.try_get("oid")?;
                let kind: String = row.try_get("kind")?;
                Ok((ObjectId::from_hex(&oid)?, name_to_kind(&kind)?))
            })
            .collect()
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
        write_ref_in_transaction(&mut tx, tenant, repository, refname, value, expected).await?;
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
        delete_ref_in_transaction(&mut tx, tenant, repository, refname, expected).await?;
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
        let mut tx = self.pool.begin().await?;
        append_reflog_in_transaction(&mut tx, tenant, repository, entry).await?;
        tx.commit().await?;
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
        let mut tx = self.pool.begin().await?;
        set_config_in_transaction(&mut tx, tenant, repository, key, value).await?;
        tx.commit().await?;
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
        upsert_tree_entries_in_transaction(&mut tx, tenant, repository, entries).await?;
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

#[async_trait]
impl CommitGraphStore for PgServerStorage {
    async fn upsert_commits(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        commits: &[IndexedCommit],
    ) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        for commit in commits {
            let generation = i32::try_from(commit.generation)
                .map_err(|_| Error::Backend("commit generation exceeds i32".to_owned()))?;
            sqlx::query(
                "insert into grit_commits
                    (tenant_id, repository_id, commit_oid, tree_oid, commit_time, generation)
                 values ($1, $2, $3, $4, $5, $6)
                 on conflict (tenant_id, repository_id, commit_oid)
                 do update set tree_oid = excluded.tree_oid,
                               commit_time = excluded.commit_time,
                               generation = excluded.generation",
            )
            .bind(tenant.as_str())
            .bind(repository.as_str())
            .bind(commit.oid.to_hex())
            .bind(commit.tree.to_hex())
            .bind(commit.commit_time)
            .bind(generation)
            .execute(&mut *tx)
            .await?;

            sqlx::query(
                "delete from grit_commit_parents
                 where tenant_id = $1 and repository_id = $2 and commit_oid = $3",
            )
            .bind(tenant.as_str())
            .bind(repository.as_str())
            .bind(commit.oid.to_hex())
            .execute(&mut *tx)
            .await?;

            for (position, parent) in commit.parents.iter().enumerate() {
                let parent_order = i32::try_from(position)
                    .map_err(|_| Error::Backend("parent order exceeds i32".to_owned()))?;
                sqlx::query(
                    "insert into grit_commit_parents
                        (tenant_id, repository_id, commit_oid, parent_oid, parent_order)
                     values ($1, $2, $3, $4, $5)",
                )
                .bind(tenant.as_str())
                .bind(repository.as_str())
                .bind(commit.oid.to_hex())
                .bind(parent.to_hex())
                .bind(parent_order)
                .execute(&mut *tx)
                .await?;
            }
        }
        tx.commit().await?;
        Ok(())
    }

    async fn read_indexed_commit(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        oid: &ObjectId,
    ) -> Result<Option<IndexedCommit>> {
        let row = sqlx::query(
            "select commit_oid, tree_oid, commit_time, generation from grit_commits
             where tenant_id = $1 and repository_id = $2 and commit_oid = $3",
        )
        .bind(tenant.as_str())
        .bind(repository.as_str())
        .bind(oid.to_hex())
        .fetch_optional(&self.pool)
        .await?;

        let Some(row) = row else {
            return Ok(None);
        };
        let commit_oid: String = row.try_get("commit_oid")?;
        let tree_oid: String = row.try_get("tree_oid")?;
        let generation: i32 = row.try_get("generation")?;
        if generation < 0 {
            return Err(Error::Backend("commit generation is negative".to_owned()));
        }
        Ok(Some(IndexedCommit {
            oid: ObjectId::from_hex(&commit_oid)?,
            tree: ObjectId::from_hex(&tree_oid)?,
            parents: self.commit_parents(tenant, repository, oid).await?,
            commit_time: row.try_get("commit_time")?,
            generation: generation as u32,
        }))
    }

    async fn commit_parents(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        oid: &ObjectId,
    ) -> Result<Vec<ObjectId>> {
        let rows = sqlx::query(
            "select parent_oid from grit_commit_parents
             where tenant_id = $1 and repository_id = $2 and commit_oid = $3
             order by parent_order",
        )
        .bind(tenant.as_str())
        .bind(repository.as_str())
        .bind(oid.to_hex())
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter()
            .map(|row| {
                let parent_oid: String = row.try_get("parent_oid")?;
                Ok(ObjectId::from_hex(&parent_oid)?)
            })
            .collect()
    }

    async fn commit_children(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        oid: &ObjectId,
    ) -> Result<Vec<ObjectId>> {
        let rows = sqlx::query(
            "select commit_oid from grit_commit_parents
             where tenant_id = $1 and repository_id = $2 and parent_oid = $3
             order by commit_oid",
        )
        .bind(tenant.as_str())
        .bind(repository.as_str())
        .bind(oid.to_hex())
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter()
            .map(|row| {
                let commit_oid: String = row.try_get("commit_oid")?;
                Ok(ObjectId::from_hex(&commit_oid)?)
            })
            .collect()
    }

    async fn list_indexed_commits(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
    ) -> Result<Vec<IndexedCommit>> {
        let rows = sqlx::query(
            "select commit_oid, tree_oid, commit_time, generation from grit_commits
             where tenant_id = $1 and repository_id = $2
             order by commit_time desc, commit_oid",
        )
        .bind(tenant.as_str())
        .bind(repository.as_str())
        .fetch_all(&self.pool)
        .await?;

        let mut commits = Vec::with_capacity(rows.len());
        for row in rows {
            let commit_oid: String = row.try_get("commit_oid")?;
            let tree_oid: String = row.try_get("tree_oid")?;
            let oid = ObjectId::from_hex(&commit_oid)?;
            let generation: i32 = row.try_get("generation")?;
            if generation < 0 {
                return Err(Error::Backend("commit generation is negative".to_owned()));
            }
            commits.push(IndexedCommit {
                oid,
                tree: ObjectId::from_hex(&tree_oid)?,
                parents: self.commit_parents(tenant, repository, &oid).await?,
                commit_time: row.try_get("commit_time")?,
                generation: generation as u32,
            });
        }
        Ok(commits)
    }
}
