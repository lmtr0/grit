//! SQLx PostgreSQL/YugaByteDB backend.
//!
//! The schema is intentionally append-friendly and transaction-oriented: objects are content
//! addressed and immutable, while refs are the primary compare-and-swap mutation point.

use async_trait::async_trait;
use grit_lib::objects::{HashAlgo, ObjectId, ObjectKind};
use sqlx::{PgPool, Postgres, QueryBuilder, Row, Transaction};
use std::collections::HashMap;
use time::OffsetDateTime;

use crate::error::{Error, Result};
use crate::ids::{RepositoryId, TenantId};
use crate::storage::{
    BrowseIndex, CommitGraphStore, ConfigStore, ImportPublication, ImportPublicationResult,
    ImportSession, ImportStateStore, IndexedCommit, IndexedTreeEntry, ObjectStore, PackMetadata,
    PackObjectIndex, PackStore, RefStore, ReflogEntry, ReflogStore, StoredObject, StoredPack,
    StoredRef,
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
        data bytea,
        storage_backend text not null default 'database',
        storage_key text,
        created_at timestamptz not null default now(),
        primary key (tenant_id, repository_id, oid),
        check ((storage_backend = 'database' and data is not null and storage_key is null)
            or (storage_backend <> 'database' and data is null and storage_key is not null))
    )",
    "alter table grit_objects
        alter column data drop not null",
    "alter table grit_objects
        add column if not exists storage_backend text not null default 'database'",
    "alter table grit_objects
        add column if not exists storage_key text",
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
    "create sequence if not exists grit_import_session_token_seq",
    "create table if not exists grit_import_state (
        tenant_id text not null,
        repository_id text not null,
        generation bigint not null,
        token bigint not null,
        complete boolean not null,
        published boolean not null,
        primary key (tenant_id, repository_id)
    )",
    "alter table grit_import_state
        add column if not exists published boolean not null default false",
    "alter table grit_import_state
        add column if not exists token bigint not null default 0",
    "create table if not exists grit_import_trusted_objects (
        tenant_id text not null,
        repository_id text not null,
        oid text not null,
        kind text not null,
        primary key (tenant_id, repository_id, oid)
    )",
    "create table if not exists grit_packs (
        tenant_id text not null,
        repository_id text not null,
        pack_checksum text not null,
        index_checksum text not null,
        data bytea,
        storage_backend text not null default 'database',
        storage_key text,
        object_count integer not null,
        size_bytes bigint not null,
        storage_order bigserial not null,
        primary key (tenant_id, repository_id, pack_checksum),
        check ((storage_backend = 'database' and data is not null and storage_key is null)
            or (storage_backend <> 'database' and data is null and storage_key is not null))
    )",
    "alter table grit_packs
        alter column data drop not null",
    "alter table grit_packs
        add column if not exists storage_backend text not null default 'database'",
    "alter table grit_packs
        add column if not exists storage_key text",
    "create table if not exists grit_pack_objects (
        tenant_id text not null,
        repository_id text not null,
        pack_checksum text not null,
        oid text not null,
        kind text not null,
        offset bigint not null,
        size bigint not null,
        compressed_size bigint not null,
        primary key (tenant_id, repository_id, pack_checksum, offset)
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
    "create index if not exists grit_pack_objects_oid_idx
        on grit_pack_objects (tenant_id, repository_id, oid)",
    "create index if not exists grit_packs_repo_order_idx
        on grit_packs (tenant_id, repository_id, storage_order)",
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
        let mut tx = self.pool.begin().await?;
        lock_repository_name(&mut tx, tenant, repository).await?;
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
        .fetch_optional(&mut *tx)
        .await?;

        let Some(row) = row else {
            return Err(Error::RepositoryAlreadyExists(format!(
                "{tenant}/{repository}"
            )));
        };
        let repository = row_to_repository(&row)?;
        tx.commit().await?;
        Ok(repository)
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
            repository_scope: PgTransactionScope::Unscoped,
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
        lock_import_repository(&mut tx, tenant, repository).await?;
        write_ref_with_reflog_in_transaction(
            &mut tx, tenant, repository, refname, value, expected, entry,
        )
        .await?;
        tx.commit().await?;
        Ok(())
    }
}

/// SQL transaction wrapper for multi-table writes.
///
/// Child-table mutations are restricted to one tenant/repository identity. This keeps advisory
/// name locks and repository row locks in canonical order instead of allowing callers to acquire
/// arbitrary multi-repository lock sequences. Rename must be the first scoped operation; after a
/// successful rename the transaction is scoped to the destination identity. A failed rename
/// poisons the lifecycle scope so only [`Self::commit`] or [`Self::rollback`] may follow.
pub struct PgServerStorageTransaction {
    tx: Transaction<'static, Postgres>,
    repository_scope: PgTransactionScope,
}

enum PgTransactionScope {
    Unscoped,
    Repository(TenantId, RepositoryId),
    Lifecycle,
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
        let requested = format!(
            "{} -> {}",
            repository_key(tenant, repository),
            repository_key(tenant, new_repository)
        );
        match &self.repository_scope {
            PgTransactionScope::Unscoped => {
                self.repository_scope = PgTransactionScope::Lifecycle;
            }
            PgTransactionScope::Repository(locked_tenant, locked_repository) => {
                return Err(Error::TransactionRepositoryScope {
                    locked: repository_key(locked_tenant, locked_repository),
                    requested,
                });
            }
            PgTransactionScope::Lifecycle => {
                return Err(Error::TransactionLifecyclePoisoned { requested });
            }
        }
        let row =
            rename_repository_in_transaction(&mut self.tx, tenant, repository, new_repository)
                .await?;
        self.repository_scope =
            PgTransactionScope::Repository(tenant.clone(), new_repository.clone());
        Ok(row)
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
        self.bind_repository_scope(tenant, repository)?;
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
        self.bind_repository_scope(tenant, repository)?;
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
        self.bind_repository_scope(tenant, repository)?;
        lock_import_repository(&mut self.tx, tenant, repository).await?;
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
        self.bind_repository_scope(tenant, repository)?;
        lock_import_repository(&mut self.tx, tenant, repository).await?;
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
        self.bind_repository_scope(tenant, repository)?;
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
        self.bind_repository_scope(tenant, repository)?;
        lock_import_repository(&mut self.tx, tenant, repository).await?;
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
        self.bind_repository_scope(tenant, repository)?;
        lock_import_repository(&mut self.tx, tenant, repository).await?;
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
        self.bind_repository_scope(tenant, repository)?;
        upsert_tree_entries_in_transaction(&mut self.tx, tenant, repository, entries).await
    }

    /// Store a pack and its object index rows within the transaction.
    ///
    /// # Errors
    ///
    /// Returns repository lifecycle or backend validation errors, or SQLx errors from the
    /// backend.
    pub async fn write_pack(
        &mut self,
        tenant: &TenantId,
        repository: &RepositoryId,
        pack: &StoredPack,
    ) -> Result<PackMetadata> {
        self.bind_repository_scope(tenant, repository)?;
        write_pack_in_transaction(&mut self.tx, tenant, repository, pack).await
    }

    fn bind_repository_scope(
        &mut self,
        tenant: &TenantId,
        repository: &RepositoryId,
    ) -> Result<()> {
        let requested = (tenant.clone(), repository.clone());
        match &self.repository_scope {
            PgTransactionScope::Unscoped => {
                self.repository_scope = PgTransactionScope::Repository(requested.0, requested.1);
                Ok(())
            }
            PgTransactionScope::Repository(locked_tenant, locked_repository)
                if locked_tenant == &requested.0 && locked_repository == &requested.1 =>
            {
                Ok(())
            }
            PgTransactionScope::Repository(locked_tenant, locked_repository) => {
                Err(Error::TransactionRepositoryScope {
                    locked: repository_key(locked_tenant, locked_repository),
                    requested: repository_key(tenant, repository),
                })
            }
            PgTransactionScope::Lifecycle => Err(Error::TransactionLifecyclePoisoned {
                requested: repository_key(tenant, repository),
            }),
        }
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

fn row_to_pack_metadata(row: &sqlx::postgres::PgRow) -> Result<PackMetadata> {
    let pack_checksum: String = row.try_get("pack_checksum")?;
    let index_checksum: String = row.try_get("index_checksum")?;
    let object_count: i32 = row.try_get("object_count")?;
    let size_bytes: i64 = row.try_get("size_bytes")?;
    let storage_order: i64 = row.try_get("storage_order")?;
    if object_count < 0 || size_bytes < 0 || storage_order < 0 {
        return Err(Error::Backend(
            "pack metadata contains negative numeric fields".to_owned(),
        ));
    }
    Ok(PackMetadata {
        pack_checksum: hex_to_bytes(&pack_checksum)?,
        index_checksum: hex_to_bytes(&index_checksum)?,
        object_count: object_count as u32,
        size_bytes: size_bytes as u64,
        storage_order: storage_order as u64,
    })
}

fn row_to_pack_object_index(row: &sqlx::postgres::PgRow) -> Result<PackObjectIndex> {
    let oid: String = row.try_get("oid")?;
    let kind: String = row.try_get("kind")?;
    let offset: i64 = row.try_get("offset")?;
    let size: i64 = row.try_get("size")?;
    let compressed_size: i64 = row.try_get("compressed_size")?;
    if offset < 0 || size < 0 || compressed_size < 0 {
        return Err(Error::Backend(
            "pack object index contains negative numeric fields".to_owned(),
        ));
    }
    Ok(PackObjectIndex {
        oid: ObjectId::from_hex(&oid)?,
        kind: name_to_kind(&kind)?,
        offset: offset as u64,
        size: size as u64,
        compressed_size: compressed_size as u64,
    })
}

fn bytes_to_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len().saturating_mul(2));
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

fn hex_to_bytes(value: &str) -> Result<Vec<u8>> {
    if !value.len().is_multiple_of(2) {
        return Err(Error::Backend("hex value has odd length".to_owned()));
    }
    value
        .as_bytes()
        .chunks(2)
        .map(|chunk| {
            let text = std::str::from_utf8(chunk)
                .map_err(|err| Error::Backend(format!("invalid hex bytes: {err}")))?;
            u8::from_str_radix(text, 16)
                .map_err(|err| Error::Backend(format!("invalid hex byte '{text}': {err}")))
        })
        .collect()
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
    let (first, second) = if repository.as_str() <= new_repository.as_str() {
        (repository, new_repository)
    } else {
        (new_repository, repository)
    };
    lock_repository_name(tx, tenant, first).await?;
    if first != second {
        lock_repository_name(tx, tenant, second).await?;
    }
    let mut source_is_live = false;
    let mut destination_exists = false;
    for candidate in [first, second] {
        let is_live: Option<bool> = sqlx::query_scalar(
            "select deleted_at is null from grit_repositories
             where tenant_id = $1 and repository_id = $2
             for update",
        )
        .bind(tenant.as_str())
        .bind(candidate.as_str())
        .fetch_optional(&mut **tx)
        .await?;
        if candidate == repository {
            source_is_live = is_live.unwrap_or(false);
        }
        if candidate == new_repository {
            destination_exists = is_live.is_some();
        }
    }
    if !source_is_live {
        return Err(Error::RepositoryNotFound(repository_key(
            tenant, repository,
        )));
    }
    if destination_exists {
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
        "grit_import_state",
        "grit_import_trusted_objects",
        "grit_packs",
        "grit_pack_objects",
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
    .await
    .map_err(|error| {
        if is_unique_violation(&error) {
            Error::RepositoryAlreadyExists(repository_key(tenant, new_repository))
        } else {
            error.into()
        }
    })?;
    row_to_repository(&row)
}

async fn delete_repository_in_transaction(
    tx: &mut Transaction<'static, Postgres>,
    tenant: &TenantId,
    repository: &RepositoryId,
) -> Result<()> {
    lock_repository_name(tx, tenant, repository).await?;
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
        "grit_import_trusted_objects",
        "grit_import_state",
        "grit_packs",
        "grit_pack_objects",
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

/// Lock one tenant-scoped repository name until the current transaction ends.
///
/// PostgreSQL cannot row-lock an absent name, so lifecycle operations additionally serialize on
/// a 64-bit advisory key derived with `hashtextextended`. The length-prefixed, domain-separated
/// input prevents ambiguous tenant/repository concatenations. A theoretical hash collision only
/// serializes unrelated names and cannot allow conflicting lifecycle operations to overlap.
async fn lock_repository_name(
    tx: &mut Transaction<'static, Postgres>,
    tenant: &TenantId,
    repository: &RepositoryId,
) -> Result<()> {
    const REPOSITORY_NAME_LOCK_SEED: i64 = 0x4752_4954_5245_504f;
    let tenant = tenant.as_str();
    let repository = repository.as_str();
    let identity = format!(
        "grit:repository-name:v1:{}:{tenant}:{}:{repository}",
        tenant.len(),
        repository.len()
    );
    sqlx::query("select pg_advisory_xact_lock(hashtextextended($1, $2))")
        .bind(identity)
        .bind(REPOSITORY_NAME_LOCK_SEED)
        .execute(&mut **tx)
        .await?;
    Ok(())
}

fn is_unique_violation(error: &sqlx::Error) -> bool {
    matches!(
        error,
        sqlx::Error::Database(database) if database.code().as_deref() == Some("23505")
    )
}

async fn write_object_in_transaction(
    tx: &mut Transaction<'static, Postgres>,
    tenant: &TenantId,
    repository: &RepositoryId,
    oid: &ObjectId,
    object: &StoredObject,
) -> Result<()> {
    lock_import_repository(tx, tenant, repository).await?;
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

/// One loose-object row prepared for an importer bulk insert.
pub(crate) struct ImportedObjectRow {
    oid: ObjectId,
    kind: ObjectKind,
    payload: ImportedObjectPayload,
}

enum ImportedObjectPayload {
    Database(Vec<u8>),
    External { backend: String, key: String },
}

impl ImportedObjectRow {
    /// Prepare one object whose payload remains in PostgreSQL.
    pub(crate) fn database(oid: ObjectId, object: StoredObject) -> Self {
        Self {
            oid,
            kind: object.kind,
            payload: ImportedObjectPayload::Database(object.data),
        }
    }

    /// Prepare one object whose payload has been placed in an external byte store.
    pub(crate) fn external(oid: ObjectId, kind: ObjectKind, backend: String, key: String) -> Self {
        Self {
            oid,
            kind,
            payload: ImportedObjectPayload::External { backend, key },
        }
    }
}

/// Insert imported loose-object rows in bind-limit-safe, set-based statements.
///
/// All chunks execute through `tx`, so the caller retains one transaction boundary for the
/// complete importer batch. An empty `rows` slice is a successful no-op.
///
/// # Errors
///
/// Returns SQLx errors from executing any object-row chunk.
pub(crate) async fn write_imported_object_rows_in_transaction(
    tx: &mut Transaction<'static, Postgres>,
    tenant: &TenantId,
    repository: &RepositoryId,
    rows: &[ImportedObjectRow],
) -> Result<()> {
    lock_import_repository(tx, tenant, repository).await?;
    const POSTGRES_BIND_LIMIT: usize = 65_535;
    const BINDS_PER_ROW: usize = 7;

    for chunk in rows.chunks(POSTGRES_BIND_LIMIT / BINDS_PER_ROW) {
        let mut query = QueryBuilder::<Postgres>::new(
            "insert into grit_objects
                (tenant_id, repository_id, oid, kind, data, storage_backend, storage_key) ",
        );
        query.push_values(chunk, |mut row, object| {
            row.push_bind(tenant.as_str())
                .push_bind(repository.as_str())
                .push_bind(object.oid.to_hex())
                .push_bind(kind_to_name(object.kind));
            match &object.payload {
                ImportedObjectPayload::Database(data) => {
                    row.push_bind(Some(data.as_slice()))
                        .push_bind("database")
                        .push_bind(None::<&str>);
                }
                ImportedObjectPayload::External { backend, key } => {
                    row.push_bind(None::<&[u8]>)
                        .push_bind(backend.as_str())
                        .push_bind(Some(key.as_str()));
                }
            }
        });
        query.push(" on conflict (tenant_id, repository_id, oid) do nothing");
        query.build().persistent(false).execute(&mut **tx).await?;
    }
    Ok(())
}

async fn begin_import_in_transaction(
    tx: &mut Transaction<'static, Postgres>,
    tenant: &TenantId,
    repository: &RepositoryId,
) -> Result<ImportSession> {
    lock_import_repository(tx, tenant, repository).await?;
    sqlx::query(
        "insert into grit_import_state
            (tenant_id, repository_id, generation, token, complete, published)
         values ($1, $2, 0, 0, false, false)
         on conflict (tenant_id, repository_id) do nothing",
    )
    .bind(tenant.as_str())
    .bind(repository.as_str())
    .execute(&mut **tx)
    .await?;
    let state = sqlx::query(
        "select generation, complete from grit_import_state
         where tenant_id = $1 and repository_id = $2
         for update",
    )
    .bind(tenant.as_str())
    .bind(repository.as_str())
    .fetch_optional(&mut **tx)
    .await?;

    let row = state.ok_or_else(|| Error::Backend("import state row disappeared".to_owned()))?;
    let current: i64 = row.try_get("generation")?;
    let generation = current
        .checked_add(1)
        .ok_or_else(|| Error::Backend("import generation overflow".to_owned()))?;
    let token: i64 = sqlx::query_scalar("select nextval('grit_import_session_token_seq')")
        .fetch_one(&mut **tx)
        .await?;
    let was_complete: bool = row.try_get("complete")?;
    sqlx::query(
        "update grit_import_state
         set generation = $3, token = $4, complete = false, published = false
         where tenant_id = $1 and repository_id = $2",
    )
    .bind(tenant.as_str())
    .bind(repository.as_str())
    .bind(generation)
    .bind(token)
    .execute(&mut **tx)
    .await?;

    let trusted_objects = if was_complete {
        sqlx::query(
            "select oid, kind from grit_import_trusted_objects
             where tenant_id = $1 and repository_id = $2
             order by oid",
        )
        .bind(tenant.as_str())
        .bind(repository.as_str())
        .fetch_all(&mut **tx)
        .await?
        .into_iter()
        .map(|row| {
            let oid: String = row.try_get("oid")?;
            let kind: String = row.try_get("kind")?;
            Ok((ObjectId::from_hex(&oid)?, name_to_kind(&kind)?))
        })
        .collect::<Result<Vec<_>>>()?
    } else {
        Vec::new()
    };

    Ok(ImportSession::new(
        tenant.clone(),
        repository.clone(),
        u64::try_from(generation)
            .map_err(|_| Error::Backend("import generation is negative".to_owned()))?,
        u64::try_from(token)
            .map_err(|_| Error::Backend("import session token is negative".to_owned()))?,
        trusted_objects,
    ))
}

async fn publish_import_in_transaction(
    tx: &mut Transaction<'static, Postgres>,
    tenant: &TenantId,
    repository: &RepositoryId,
    session: &ImportSession,
    publication: &ImportPublication,
) -> Result<ImportPublicationResult> {
    lock_import_repository(tx, tenant, repository).await?;
    if session.tenant() != tenant || session.repository() != repository {
        return Err(stale_import_session(tenant, repository, session));
    }
    let state = sqlx::query(
        "select generation, token, complete, published from grit_import_state
         where tenant_id = $1 and repository_id = $2
         for update",
    )
    .bind(tenant.as_str())
    .bind(repository.as_str())
    .fetch_optional(&mut **tx)
    .await?;
    let session_generation = i64::try_from(session.generation())
        .map_err(|_| Error::Backend("import generation exceeds i64".to_owned()))?;
    let session_token = i64::try_from(session.token())
        .map_err(|_| Error::Backend("import session token exceeds i64".to_owned()))?;
    let valid = state
        .map(|row| {
            Ok::<_, sqlx::Error>(
                row.try_get::<i64, _>("generation")? == session_generation
                    && row.try_get::<i64, _>("token")? == session_token
                    && !row.try_get::<bool, _>("complete")?
                    && !row.try_get::<bool, _>("published")?,
            )
        })
        .transpose()?
        .unwrap_or(false);
    if !valid {
        return Err(stale_import_session(tenant, repository, session));
    }

    for (key, value) in &publication.config_entries {
        set_config_in_transaction(tx, tenant, repository, key, value).await?;
    }
    let desired_refs = publication
        .refs
        .iter()
        .map(|(refname, _)| refname.as_str())
        .collect::<std::collections::HashSet<_>>();
    let pruned_refs = if publication.prune_deleted_refs {
        let existing = sqlx::query_scalar::<_, String>(
            "select refname from grit_refs
             where tenant_id = $1 and repository_id = $2
             order by refname",
        )
        .bind(tenant.as_str())
        .bind(repository.as_str())
        .fetch_all(&mut **tx)
        .await?;
        existing
            .into_iter()
            .filter(|refname| !desired_refs.contains(refname.as_str()))
            .collect::<Vec<_>>()
    } else {
        Vec::new()
    };
    for refname in &pruned_refs {
        delete_ref_in_transaction(tx, tenant, repository, refname, None).await?;
    }
    for (refname, value) in &publication.refs {
        write_ref_in_transaction(tx, tenant, repository, refname, value, None).await?;
    }

    const POSTGRES_BIND_LIMIT: usize = 65_535;
    const BINDS_PER_ROW: usize = 4;
    for chunk in publication
        .newly_trusted
        .chunks(POSTGRES_BIND_LIMIT / BINDS_PER_ROW)
    {
        let mut query = QueryBuilder::<Postgres>::new(
            "insert into grit_import_trusted_objects
                (tenant_id, repository_id, oid, kind) ",
        );
        query.push_values(chunk, |mut row, (oid, kind)| {
            row.push_bind(tenant.as_str())
                .push_bind(repository.as_str())
                .push_bind(oid.to_hex())
                .push_bind(kind_to_name(*kind));
        });
        query.push(" on conflict (tenant_id, repository_id, oid) do nothing");
        query.build().persistent(false).execute(&mut **tx).await?;
    }

    sqlx::query(
        "update grit_import_state
         set published = true
         where tenant_id = $1 and repository_id = $2 and generation = $3",
    )
    .bind(tenant.as_str())
    .bind(repository.as_str())
    .bind(session_generation)
    .execute(&mut **tx)
    .await?;
    Ok(ImportPublicationResult { pruned_refs })
}

async fn complete_import_in_transaction(
    tx: &mut Transaction<'static, Postgres>,
    tenant: &TenantId,
    repository: &RepositoryId,
    session: &ImportSession,
) -> Result<()> {
    lock_import_repository(tx, tenant, repository).await?;
    if session.tenant() != tenant || session.repository() != repository {
        return Err(stale_import_session(tenant, repository, session));
    }
    let state = sqlx::query(
        "select generation, token, complete, published from grit_import_state
         where tenant_id = $1 and repository_id = $2
         for update",
    )
    .bind(tenant.as_str())
    .bind(repository.as_str())
    .fetch_optional(&mut **tx)
    .await?;
    let session_generation = i64::try_from(session.generation())
        .map_err(|_| Error::Backend("import generation exceeds i64".to_owned()))?;
    let session_token = i64::try_from(session.token())
        .map_err(|_| Error::Backend("import session token exceeds i64".to_owned()))?;
    let valid = state
        .map(|row| {
            Ok::<_, sqlx::Error>(
                row.try_get::<i64, _>("generation")? == session_generation
                    && row.try_get::<i64, _>("token")? == session_token
                    && !row.try_get::<bool, _>("complete")?
                    && row.try_get::<bool, _>("published")?,
            )
        })
        .transpose()?
        .unwrap_or(false);
    if !valid {
        return Err(stale_import_session(tenant, repository, session));
    }
    sqlx::query(
        "update grit_import_state
         set complete = true
         where tenant_id = $1 and repository_id = $2 and generation = $3",
    )
    .bind(tenant.as_str())
    .bind(repository.as_str())
    .bind(session_generation)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

/// Acquire the canonical advisory-name then live-row lock pair for a repository mutation.
///
/// Repeated calls for the same identity in one transaction are safe. Callers that need multiple
/// repository identities must acquire them in deterministic identity order; the public
/// transaction wrapper prevents arbitrary multi-repository child mutations.
pub(crate) async fn lock_import_repository(
    tx: &mut Transaction<'static, Postgres>,
    tenant: &TenantId,
    repository: &RepositoryId,
) -> Result<()> {
    lock_repository_name(tx, tenant, repository).await?;
    let exists: Option<i32> = sqlx::query_scalar(
        "select 1 from grit_repositories
         where tenant_id = $1 and repository_id = $2 and deleted_at is null
         for update",
    )
    .bind(tenant.as_str())
    .bind(repository.as_str())
    .fetch_optional(&mut **tx)
    .await?;
    if exists.is_none() {
        return Err(Error::RepositoryNotFound(repository_key(
            tenant, repository,
        )));
    }
    Ok(())
}

fn stale_import_session(
    tenant: &TenantId,
    repository: &RepositoryId,
    session: &ImportSession,
) -> Error {
    Error::StaleImportSession {
        tenant: tenant.as_str().to_owned(),
        repository: repository.as_str().to_owned(),
        generation: session.generation(),
    }
}

async fn write_ref_in_transaction(
    tx: &mut Transaction<'static, Postgres>,
    tenant: &TenantId,
    repository: &RepositoryId,
    refname: &str,
    value: &StoredRef,
    expected: Option<Option<StoredRef>>,
) -> Result<()> {
    lock_import_repository(tx, tenant, repository).await?;
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
    lock_import_repository(tx, tenant, repository).await?;
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
    lock_import_repository(tx, tenant, repository).await?;
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
    lock_import_repository(tx, tenant, repository).await?;
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
    let values = prepare_tree_entry_values(entries)?;
    lock_import_repository(tx, tenant, repository).await?;
    insert_tree_entries_in_transaction(tx, tenant, repository, entries, &values).await
}

#[derive(Clone, Copy)]
struct PgTreeEntryValues {
    mode: i32,
    size: Option<i64>,
}

fn prepare_tree_entry_values(entries: &[IndexedTreeEntry]) -> Result<Vec<PgTreeEntryValues>> {
    entries
        .iter()
        .map(|entry| {
            let mode = i32::try_from(entry.mode)
                .map_err(|_| Error::Backend("tree entry mode exceeds i32".to_owned()))?;
            let size = entry
                .size
                .map(i64::try_from)
                .transpose()
                .map_err(|_| Error::Backend("tree entry size exceeds i64".to_owned()))?;
            Ok(PgTreeEntryValues { mode, size })
        })
        .collect()
}

async fn insert_tree_entries_in_transaction(
    tx: &mut Transaction<'static, Postgres>,
    tenant: &TenantId,
    repository: &RepositoryId,
    entries: &[IndexedTreeEntry],
    values: &[PgTreeEntryValues],
) -> Result<()> {
    const POSTGRES_BIND_LIMIT: usize = 65_535;
    const BINDS_PER_ROW: usize = 8;
    let rows_per_chunk = POSTGRES_BIND_LIMIT / BINDS_PER_ROW;
    let chunk_capacity = rows_per_chunk.min(entries.len());
    let mut final_rows = HashMap::with_capacity(chunk_capacity);
    let mut final_row_indexes = Vec::with_capacity(chunk_capacity);
    for (entry_chunk, value_chunk) in entries
        .chunks(rows_per_chunk)
        .zip(values.chunks(rows_per_chunk))
    {
        // PostgreSQL cannot update the same conflict target twice in one statement. Keep the
        // final occurrence in each chunk to preserve the previous row-at-a-time semantics.
        final_rows.clear();
        for (index, entry) in entry_chunk.iter().enumerate() {
            final_rows.insert((entry.tree_oid, entry.path.as_str()), index);
        }
        final_row_indexes.clear();
        final_row_indexes.extend(final_rows.values().copied());
        final_row_indexes.sort_unstable();

        let mut query = QueryBuilder::<Postgres>::new(
            "insert into grit_tree_entries
                (tenant_id, repository_id, tree_oid, path, mode, oid, kind, size) ",
        );
        query.push_values(final_row_indexes.iter().copied(), |mut row, index| {
            let entry = &entry_chunk[index];
            let value = value_chunk[index];
            row.push_bind(tenant.as_str())
                .push_bind(repository.as_str())
                .push_bind(entry.tree_oid.to_hex())
                .push_bind(entry.path.as_str())
                .push_bind(value.mode)
                .push_bind(entry.oid.to_hex())
                .push_bind(kind_to_name(entry.kind))
                .push_bind(value.size);
        });
        query.push(
            " on conflict (tenant_id, repository_id, tree_oid, path)
              do update set mode = excluded.mode,
                            oid = excluded.oid,
                            kind = excluded.kind,
                            size = excluded.size",
        );
        query.build().persistent(false).execute(&mut **tx).await?;
    }
    Ok(())
}

async fn write_pack_in_transaction(
    tx: &mut Transaction<'static, Postgres>,
    tenant: &TenantId,
    repository: &RepositoryId,
    pack: &StoredPack,
) -> Result<PackMetadata> {
    lock_import_repository(tx, tenant, repository).await?;
    let object_count = i32::try_from(pack.metadata.object_count)
        .map_err(|_| Error::Backend("pack object count exceeds i32".to_owned()))?;
    let size_bytes = i64::try_from(pack.metadata.size_bytes)
        .map_err(|_| Error::Backend("pack size exceeds i64".to_owned()))?;
    let pack_checksum = bytes_to_hex(&pack.metadata.pack_checksum);
    let index_checksum = bytes_to_hex(&pack.metadata.index_checksum);
    let row = sqlx::query(
        "insert into grit_packs
            (tenant_id, repository_id, pack_checksum, index_checksum, data,
             object_count, size_bytes)
         values ($1, $2, $3, $4, $5, $6, $7)
         on conflict (tenant_id, repository_id, pack_checksum)
         do update set pack_checksum = excluded.pack_checksum
         returning pack_checksum, index_checksum, object_count, size_bytes, storage_order",
    )
    .bind(tenant.as_str())
    .bind(repository.as_str())
    .bind(&pack_checksum)
    .bind(&index_checksum)
    .bind(&pack.data)
    .bind(object_count)
    .bind(size_bytes)
    .fetch_one(&mut **tx)
    .await?;

    sqlx::query(
        "delete from grit_pack_objects
         where tenant_id = $1 and repository_id = $2 and pack_checksum = $3",
    )
    .bind(tenant.as_str())
    .bind(repository.as_str())
    .bind(&pack_checksum)
    .execute(&mut **tx)
    .await?;

    for entry in &pack.index {
        let offset = i64::try_from(entry.offset)
            .map_err(|_| Error::Backend("pack object offset exceeds i64".to_owned()))?;
        let size = i64::try_from(entry.size)
            .map_err(|_| Error::Backend("pack object size exceeds i64".to_owned()))?;
        let compressed_size = i64::try_from(entry.compressed_size)
            .map_err(|_| Error::Backend("pack object compressed size exceeds i64".to_owned()))?;
        sqlx::query(
            "insert into grit_pack_objects
                (tenant_id, repository_id, pack_checksum, oid, kind, offset, size,
                 compressed_size)
             values ($1, $2, $3, $4, $5, $6, $7, $8)",
        )
        .bind(tenant.as_str())
        .bind(repository.as_str())
        .bind(&pack_checksum)
        .bind(entry.oid.to_hex())
        .bind(kind_to_name(entry.kind))
        .bind(offset)
        .bind(size)
        .bind(compressed_size)
        .execute(&mut **tx)
        .await?;
    }

    row_to_pack_metadata(&row)
}

#[async_trait]
impl ImportStateStore for PgServerStorage {
    async fn begin_import(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
    ) -> Result<ImportSession> {
        let mut tx = self.pool.begin().await?;
        let session = begin_import_in_transaction(&mut tx, tenant, repository).await?;
        tx.commit().await?;
        Ok(session)
    }

    async fn publish_import(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        session: &ImportSession,
        publication: &ImportPublication,
    ) -> Result<ImportPublicationResult> {
        let mut tx = self.pool.begin().await?;
        let result =
            publish_import_in_transaction(&mut tx, tenant, repository, session, publication)
                .await?;
        tx.commit().await?;
        Ok(result)
    }

    async fn complete_import(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        session: &ImportSession,
    ) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        complete_import_in_transaction(&mut tx, tenant, repository, session).await?;
        tx.commit().await?;
        Ok(())
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
            "select kind, data, storage_backend, storage_key from grit_objects
             where tenant_id = $1 and repository_id = $2 and oid = $3",
        )
        .bind(tenant.as_str())
        .bind(repository.as_str())
        .bind(oid.to_hex())
        .fetch_optional(&self.pool)
        .await?;

        if let Some(object) = row
            .map(|row| {
                let kind: String = row.try_get("kind")?;
                let data: Option<Vec<u8>> = row.try_get("data")?;
                let storage_backend: String = row.try_get("storage_backend")?;
                let storage_key: Option<String> = row.try_get("storage_key")?;
                let Some(data) = data else {
                    return Err(Error::Backend(format!(
                        "object {} is stored in external backend {} at {}; use externalized storage",
                        oid.to_hex(),
                        storage_backend,
                        storage_key.unwrap_or_else(|| "<missing key>".to_owned())
                    )));
                };
                Ok::<StoredObject, Error>(StoredObject {
                    kind: name_to_kind(&kind)?,
                    data,
                })
            })
            .transpose()?
        {
            return Ok(Some(object));
        }
        let Some((metadata, index)) = self.find_packed_object(tenant, repository, oid).await?
        else {
            return Ok(None);
        };
        let data = self
            .read_pack_data(tenant, repository, &metadata.pack_checksum)
            .await?
            .ok_or_else(|| Error::Backend("pack index points at missing pack data".to_owned()))?;
        crate::packfile::read_object_at_offset(&data, index.offset, oid.algo()).map(Some)
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

    async fn write_imported_object(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        oid: &ObjectId,
        object: &StoredObject,
    ) -> Result<()> {
        let row = ImportedObjectRow::database(*oid, object.clone());
        let mut tx = self.pool.begin().await?;
        write_imported_object_rows_in_transaction(&mut tx, tenant, repository, &[row]).await?;
        tx.commit().await?;
        Ok(())
    }

    async fn write_imported_objects(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        objects: Vec<(ObjectId, StoredObject)>,
    ) -> Result<()> {
        if objects.is_empty() {
            return Ok(());
        }
        let rows = objects
            .into_iter()
            .map(|(oid, object)| ImportedObjectRow::database(oid, object))
            .collect::<Vec<_>>();
        let mut tx = self.pool.begin().await?;
        write_imported_object_rows_in_transaction(&mut tx, tenant, repository, &rows).await?;
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
                union all
                select 1 from grit_pack_objects
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
            "select count(*) from (
                select oid from grit_objects
                where tenant_id = $1 and repository_id = $2
                union
                select oid from grit_pack_objects
                where tenant_id = $1 and repository_id = $2
             ) objects",
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
                "select oid, min(kind) as kind from (
                    select oid, kind from grit_objects
                    where tenant_id = $1 and repository_id = $2 and kind = $3
                    union
                    select oid, kind from grit_pack_objects
                    where tenant_id = $1 and repository_id = $2 and kind = $3
                 ) objects
                 group by oid
                 order by oid",
            )
            .bind(tenant.as_str())
            .bind(repository.as_str())
            .bind(kind_to_name(kind))
            .fetch_all(&self.pool)
            .await?
        } else {
            sqlx::query(
                "select oid, min(kind) as kind from (
                    select oid, kind from grit_objects
                    where tenant_id = $1 and repository_id = $2
                    union
                    select oid, kind from grit_pack_objects
                    where tenant_id = $1 and repository_id = $2
                 ) objects
                 group by oid
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
        lock_import_repository(&mut tx, tenant, repository).await?;
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
        lock_import_repository(&mut tx, tenant, repository).await?;
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
        lock_import_repository(&mut tx, tenant, repository).await?;
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
        let values = prepare_tree_entry_values(entries)?;
        let mut tx = self.pool.begin().await?;
        lock_import_repository(&mut tx, tenant, repository).await?;
        insert_tree_entries_in_transaction(&mut tx, tenant, repository, entries, &values).await?;
        tx.commit().await?;
        Ok(())
    }

    async fn replace_tree_entries(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        entries: &[IndexedTreeEntry],
    ) -> Result<()> {
        let values = prepare_tree_entry_values(entries)?;
        let mut tx = self.pool.begin().await?;
        lock_import_repository(&mut tx, tenant, repository).await?;
        sqlx::query(
            "delete from grit_tree_entries
             where tenant_id = $1 and repository_id = $2",
        )
        .bind(tenant.as_str())
        .bind(repository.as_str())
        .execute(&mut *tx)
        .await?;
        insert_tree_entries_in_transaction(&mut tx, tenant, repository, entries, &values).await?;
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
        lock_import_repository(&mut tx, tenant, repository).await?;
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

#[async_trait]
impl PackStore for PgServerStorage {
    async fn write_pack(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        pack: &StoredPack,
    ) -> Result<PackMetadata> {
        let mut tx = self.pool.begin().await?;
        let metadata = write_pack_in_transaction(&mut tx, tenant, repository, pack).await?;
        tx.commit().await?;
        Ok(metadata)
    }

    async fn read_pack_metadata(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        pack_checksum: &[u8],
    ) -> Result<Option<PackMetadata>> {
        let row = sqlx::query(
            "select pack_checksum, index_checksum, object_count, size_bytes, storage_order
             from grit_packs
             where tenant_id = $1 and repository_id = $2 and pack_checksum = $3",
        )
        .bind(tenant.as_str())
        .bind(repository.as_str())
        .bind(bytes_to_hex(pack_checksum))
        .fetch_optional(&self.pool)
        .await?;
        row.as_ref().map(row_to_pack_metadata).transpose()
    }

    async fn read_pack_data(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        pack_checksum: &[u8],
    ) -> Result<Option<Vec<u8>>> {
        let row = sqlx::query(
            "select data, storage_backend, storage_key from grit_packs
             where tenant_id = $1 and repository_id = $2 and pack_checksum = $3",
        )
        .bind(tenant.as_str())
        .bind(repository.as_str())
        .bind(bytes_to_hex(pack_checksum))
        .fetch_optional(&self.pool)
        .await?;

        let Some(row) = row else {
            return Ok(None);
        };
        let data: Option<Vec<u8>> = row.try_get("data")?;
        if let Some(data) = data {
            return Ok(Some(data));
        }
        let storage_backend: String = row.try_get("storage_backend")?;
        let storage_key: Option<String> = row.try_get("storage_key")?;
        Err(Error::Backend(format!(
            "pack {} is stored in external backend {} at {}; use externalized storage",
            bytes_to_hex(pack_checksum),
            storage_backend,
            storage_key.unwrap_or_else(|| "<missing key>".to_owned())
        )))
    }

    async fn find_packed_object(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        oid: &ObjectId,
    ) -> Result<Option<(PackMetadata, PackObjectIndex)>> {
        let row = sqlx::query(
            "select p.pack_checksum, p.index_checksum, p.object_count, p.size_bytes,
                    p.storage_order, o.oid, o.kind, o.offset, o.size, o.compressed_size
             from grit_pack_objects o
             join grit_packs p
               on p.tenant_id = o.tenant_id
              and p.repository_id = o.repository_id
              and p.pack_checksum = o.pack_checksum
             where o.tenant_id = $1 and o.repository_id = $2 and o.oid = $3
             order by p.storage_order desc
             limit 1",
        )
        .bind(tenant.as_str())
        .bind(repository.as_str())
        .bind(oid.to_hex())
        .fetch_optional(&self.pool)
        .await?;
        row.as_ref()
            .map(|row| Ok((row_to_pack_metadata(row)?, row_to_pack_object_index(row)?)))
            .transpose()
    }

    async fn read_pack_index_at_offset(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        pack_checksum: &[u8],
        offset: u64,
    ) -> Result<Option<PackObjectIndex>> {
        let offset = i64::try_from(offset)
            .map_err(|_| Error::Backend("pack object offset exceeds i64".to_owned()))?;
        let row = sqlx::query(
            "select oid, kind, offset, size, compressed_size
             from grit_pack_objects
             where tenant_id = $1 and repository_id = $2 and pack_checksum = $3 and offset = $4",
        )
        .bind(tenant.as_str())
        .bind(repository.as_str())
        .bind(bytes_to_hex(pack_checksum))
        .bind(offset)
        .fetch_optional(&self.pool)
        .await?;
        row.as_ref().map(row_to_pack_object_index).transpose()
    }

    async fn list_packs(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
    ) -> Result<Vec<PackMetadata>> {
        let rows = sqlx::query(
            "select pack_checksum, index_checksum, object_count, size_bytes, storage_order
             from grit_packs
             where tenant_id = $1 and repository_id = $2
             order by storage_order",
        )
        .bind(tenant.as_str())
        .bind(repository.as_str())
        .fetch_all(&self.pool)
        .await?;
        rows.iter().map(row_to_pack_metadata).collect()
    }

    async fn list_pack_objects(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        pack_checksum: Option<&[u8]>,
    ) -> Result<Vec<(PackMetadata, PackObjectIndex)>> {
        let rows = if let Some(pack_checksum) = pack_checksum {
            sqlx::query(
                "select p.pack_checksum, p.index_checksum, p.object_count, p.size_bytes,
                        p.storage_order, o.oid, o.kind, o.offset, o.size, o.compressed_size
                 from grit_pack_objects o
                 join grit_packs p
                   on p.tenant_id = o.tenant_id
                  and p.repository_id = o.repository_id
                  and p.pack_checksum = o.pack_checksum
                 where o.tenant_id = $1 and o.repository_id = $2 and o.pack_checksum = $3
                 order by p.storage_order, o.offset",
            )
            .bind(tenant.as_str())
            .bind(repository.as_str())
            .bind(bytes_to_hex(pack_checksum))
            .fetch_all(&self.pool)
            .await?
        } else {
            sqlx::query(
                "select p.pack_checksum, p.index_checksum, p.object_count, p.size_bytes,
                        p.storage_order, o.oid, o.kind, o.offset, o.size, o.compressed_size
                 from grit_pack_objects o
                 join grit_packs p
                   on p.tenant_id = o.tenant_id
                  and p.repository_id = o.repository_id
                  and p.pack_checksum = o.pack_checksum
                 where o.tenant_id = $1 and o.repository_id = $2
                 order by p.storage_order, o.offset",
            )
            .bind(tenant.as_str())
            .bind(repository.as_str())
            .fetch_all(&self.pool)
            .await?
        };

        rows.iter()
            .map(|row| Ok((row_to_pack_metadata(row)?, row_to_pack_object_index(row)?)))
            .collect()
    }
}
