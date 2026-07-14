//! PostgreSQL metadata storage with externalized immutable object bytes.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use grit_lib::objects::{ObjectId, ObjectKind};
use sqlx::{PgPool, Postgres, QueryBuilder, Row, Transaction};
use time::OffsetDateTime;

use crate::error::{Error, Result};
use crate::external::ExternalByteStore;
use crate::ids::{RepositoryId, TenantId};
use crate::sqlx_postgres::{
    install_external_pack_in_transaction, lock_import_repository, prepare_imported_pack_batch,
    prepare_pack_values, write_imported_object_rows_in_transaction, ImportedObjectRow,
    PgServerStorage,
};
use crate::storage::{
    BrowseIndex, CommitGraphStore, ConfigStore, ImportPublication, ImportPublicationResult,
    ImportSession, ImportStateStore, ImportedPack, IndexedCommit, IndexedTreeEntry, ObjectStore,
    PackMetadata, PackObjectIndex, PackStore, RefStore, ReflogEntry, ReflogStore, StoredObject,
    StoredPack, StoredRef,
};

/// Configuration for a PostgreSQL-backed repository that externalizes immutable bytes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExternalStorageOptions {
    /// Name written to SQL rows that are stored in the external byte backend.
    pub backend_name: String,
    /// Prefix prepended to generated external byte keys.
    pub key_prefix: String,
    /// Store loose object payload bytes in the external byte backend.
    pub write_loose_objects_externally: bool,
    /// Store raw packfile bytes in the external byte backend.
    pub write_packs_externally: bool,
}

/// Result counters from one repository-scoped external orphan sweep.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ExternalOrphanSweepReport {
    /// Candidates whose first observation timestamp was recorded by this sweep.
    pub newly_observed: u64,
    /// Expired candidates examined after the grace period.
    pub examined: u64,
    /// Expected bytes represented by expired candidates examined by this sweep.
    pub examined_bytes: u64,
    /// Candidate rows cleared because live SQL metadata references their key.
    pub live_cleared: u64,
    /// Candidate rows cleared because the external value was already missing.
    pub missing_cleared: u64,
    /// Unreferenced external values deleted by this sweep.
    pub deleted: u64,
    /// Bytes deleted, based on metadata-only size reads immediately before deletion.
    pub deleted_bytes: u64,
}

impl Default for ExternalStorageOptions {
    fn default() -> Self {
        Self {
            backend_name: "s3".to_owned(),
            key_prefix: String::new(),
            write_loose_objects_externally: true,
            write_packs_externally: true,
        }
    }
}

/// PostgreSQL metadata storage plus an external byte store for object and pack payloads.
#[derive(Clone)]
pub struct PgExternalizedStorage<B> {
    sql: PgServerStorage,
    bytes: Arc<B>,
    options: ExternalStorageOptions,
}

struct StoredBytes {
    data: Option<Vec<u8>>,
    storage_backend: String,
    storage_key: Option<String>,
}

struct ExternalOrphanCandidate {
    storage_key: String,
    size_bytes: u64,
}

impl<B> PgExternalizedStorage<B> {
    /// Create hybrid storage from SQL metadata storage, a byte store, and options.
    #[must_use]
    pub fn new(sql: PgServerStorage, bytes: Arc<B>, options: ExternalStorageOptions) -> Self {
        Self {
            sql,
            bytes,
            options,
        }
    }

    /// Borrow the SQL metadata storage.
    #[must_use]
    pub fn sql(&self) -> &PgServerStorage {
        &self.sql
    }

    /// Borrow the external byte store.
    #[must_use]
    pub fn bytes(&self) -> &Arc<B> {
        &self.bytes
    }

    /// Borrow the external storage options.
    #[must_use]
    pub fn options(&self) -> &ExternalStorageOptions {
        &self.options
    }

    fn pool(&self) -> &PgPool {
        self.sql.pool()
    }

    fn object_key(&self, tenant: &TenantId, repository: &RepositoryId, oid: &ObjectId) -> String {
        scoped_key(
            &self.options.key_prefix,
            tenant,
            repository,
            "objects",
            oid.algo().name(),
            &oid.to_hex(),
        )
    }

    fn pack_key(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        pack_checksum: &[u8],
        algo_name: &str,
    ) -> String {
        scoped_key(
            &self.options.key_prefix,
            tenant,
            repository,
            "packs",
            algo_name,
            &format!("{}.pack", bytes_to_hex(pack_checksum)),
        )
    }
}

#[async_trait]
impl<B> ObjectStore for PgExternalizedStorage<B>
where
    B: ExternalByteStore,
{
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
        .fetch_optional(self.pool())
        .await?;

        if let Some(row) = row {
            let kind: String = row.try_get("kind")?;
            let bytes = stored_bytes_from_row(&row)?;
            return self
                .read_stored_object_bytes(oid, name_to_kind(&kind)?, bytes)
                .await
                .map(Some);
        }
        self.read_packed_object(tenant, repository, oid).await
    }

    async fn write_object(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        oid: &ObjectId,
        object: &StoredObject,
    ) -> Result<()> {
        if !self.options.write_loose_objects_externally {
            return self.sql.write_object(tenant, repository, oid, object).await;
        }

        let mut tx = self.pool().begin().await?;
        lock_import_repository(&mut tx, tenant, repository).await?;
        let key = self.object_key(tenant, repository, oid);
        self.bytes.put_if_absent(&key, &object.data).await?;
        let row =
            ImportedObjectRow::external(*oid, object.kind, self.options.backend_name.clone(), key);
        write_imported_object_rows_in_transaction(&mut tx, tenant, repository, &[row]).await?;
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
        if !self.options.write_loose_objects_externally {
            return self
                .sql
                .write_imported_object(tenant, repository, oid, object)
                .await;
        }
        let mut tx = self.pool().begin().await?;
        lock_import_repository(&mut tx, tenant, repository).await?;
        let key = self.object_key(tenant, repository, oid);
        self.bytes.put_if_absent(&key, &object.data).await?;
        let row =
            ImportedObjectRow::external(*oid, object.kind, self.options.backend_name.clone(), key);
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
        if !self.options.write_loose_objects_externally {
            return self
                .sql
                .write_imported_objects(tenant, repository, objects)
                .await;
        }
        if objects.is_empty() {
            return Ok(());
        }

        let mut tx = self.pool().begin().await?;
        lock_import_repository(&mut tx, tenant, repository).await?;
        let mut rows = Vec::with_capacity(objects.len());
        for (oid, object) in objects {
            let key = self.object_key(tenant, repository, &oid);
            self.bytes.put_if_absent(&key, &object.data).await?;
            rows.push(ImportedObjectRow::external(
                oid,
                object.kind,
                self.options.backend_name.clone(),
                key,
            ));
        }

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
        self.sql.object_exists(tenant, repository, oid).await
    }

    async fn count_objects(&self, tenant: &TenantId, repository: &RepositoryId) -> Result<usize> {
        self.sql.count_objects(tenant, repository).await
    }

    async fn list_object_ids(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        kind: Option<ObjectKind>,
    ) -> Result<Vec<(ObjectId, ObjectKind)>> {
        self.sql.list_object_ids(tenant, repository, kind).await
    }
}

impl<B> PgExternalizedStorage<B>
where
    B: ExternalByteStore,
{
    async fn read_stored_object_bytes(
        &self,
        oid: &ObjectId,
        kind: ObjectKind,
        stored: StoredBytes,
    ) -> Result<StoredObject> {
        if let Some(data) = stored.data {
            return Ok(StoredObject { kind, data });
        }
        let key = self.external_key("object", &oid.to_hex(), stored)?;
        let data = self.bytes.get(&key).await?.ok_or_else(|| {
            Error::Backend(format!(
                "object {} points at missing external bytes {key}",
                oid.to_hex()
            ))
        })?;
        Ok(StoredObject { kind, data })
    }

    async fn read_packed_object(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        oid: &ObjectId,
    ) -> Result<Option<StoredObject>> {
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

    fn external_key(&self, label: &str, id: &str, stored: StoredBytes) -> Result<String> {
        if stored.storage_backend != self.options.backend_name {
            return Err(Error::Backend(format!(
                "{label} {id} is stored in unsupported external backend {}",
                stored.storage_backend
            )));
        }
        stored
            .storage_key
            .ok_or_else(|| Error::Backend(format!("{label} {id} is missing external storage key")))
    }

    async fn pack_storage(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        pack_checksum: &[u8],
    ) -> Result<Option<StoredBytes>> {
        let row = sqlx::query(
            "select data, storage_backend, storage_key from grit_packs
             where tenant_id = $1 and repository_id = $2 and pack_checksum = $3",
        )
        .bind(tenant.as_str())
        .bind(repository.as_str())
        .bind(bytes_to_hex(pack_checksum))
        .fetch_optional(self.pool())
        .await?;
        row.as_ref().map(stored_bytes_from_row).transpose()
    }

    /// Sweep expired external-pack orphan candidates for one repository.
    ///
    /// A candidate without an observation timestamp is only marked as first seen during this
    /// call. Candidates observed by earlier calls are eligible once `grace_period` has elapsed.
    /// Every decision is made under the repository mutation lock and live pack and loose-object
    /// metadata are rechecked immediately before an external deletion.
    ///
    /// # Parameters
    ///
    /// - `tenant`: Tenant whose candidate namespace is swept.
    /// - `repository`: Repository whose candidate namespace is swept.
    /// - `observed_at`: Explicit wall-clock observation time used for both first sightings and the
    ///   grace-period cutoff.
    /// - `grace_period`: Minimum time a candidate must remain observed before deletion.
    ///
    /// # Returns
    ///
    /// Returns counters for observations, live/missing cleanup, and deleted external bytes.
    ///
    /// # Errors
    ///
    /// Returns SQL, external byte-store, duration conversion, or timestamp range errors. A failed
    /// external deletion leaves the candidate registered for a retry. If SQL commit fails after a
    /// successful deletion, the next sweep idempotently clears the now-missing candidate. Loose
    /// object publication does not yet register candidates and is outside this sweeper's scope.
    pub async fn sweep_external_orphans(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        observed_at: OffsetDateTime,
        grace_period: Duration,
    ) -> Result<ExternalOrphanSweepReport> {
        let grace_period = time::Duration::try_from(grace_period)
            .map_err(|err| Error::Backend(format!("external orphan grace period: {err}")))?;
        let cutoff = observed_at.checked_sub(grace_period).ok_or_else(|| {
            Error::Backend("external orphan grace cutoff exceeds timestamp range".to_owned())
        })?;

        let mut tx = self.pool().begin().await?;
        lock_import_repository(&mut tx, tenant, repository).await?;
        let rows = sqlx::query(
            "select storage_key, size_bytes
             from grit_external_orphan_candidates
             where tenant_id = $1 and repository_id = $2 and storage_backend = $3
               and first_observed_at is not null and first_observed_at <= $4
             order by storage_key",
        )
        .bind(tenant.as_str())
        .bind(repository.as_str())
        .bind(&self.options.backend_name)
        .bind(cutoff)
        .fetch_all(&mut *tx)
        .await?;
        let candidates = rows
            .into_iter()
            .map(|row| {
                let size_bytes: i64 = row.try_get("size_bytes")?;
                Ok(ExternalOrphanCandidate {
                    storage_key: row.try_get("storage_key")?,
                    size_bytes: u64::try_from(size_bytes).map_err(|_| {
                        Error::Backend("external orphan candidate has negative size".to_owned())
                    })?,
                })
            })
            .collect::<Result<Vec<_>>>()?;

        let newly_observed = sqlx::query(
            "update grit_external_orphan_candidates
             set first_observed_at = $4
             where tenant_id = $1 and repository_id = $2 and storage_backend = $3
               and first_observed_at is null",
        )
        .bind(tenant.as_str())
        .bind(repository.as_str())
        .bind(&self.options.backend_name)
        .bind(observed_at)
        .execute(&mut *tx)
        .await?
        .rows_affected();

        let examined = u64::try_from(candidates.len()).map_err(|_| {
            Error::Backend("external orphan candidate count exceeds u64".to_owned())
        })?;
        let mut report = ExternalOrphanSweepReport {
            newly_observed,
            examined,
            examined_bytes: candidates.iter().fold(0_u64, |sum, candidate| {
                sum.saturating_add(candidate.size_bytes)
            }),
            ..ExternalOrphanSweepReport::default()
        };
        for candidate in candidates {
            if external_key_is_live(
                &mut tx,
                tenant,
                repository,
                &self.options.backend_name,
                &candidate.storage_key,
            )
            .await?
            {
                clear_external_orphan_candidate(
                    &mut tx,
                    tenant,
                    repository,
                    &self.options.backend_name,
                    &candidate.storage_key,
                )
                .await?;
                report.live_cleared = report.live_cleared.saturating_add(1);
                continue;
            }

            let Some(size_bytes) = self.bytes.content_length(&candidate.storage_key).await? else {
                clear_external_orphan_candidate(
                    &mut tx,
                    tenant,
                    repository,
                    &self.options.backend_name,
                    &candidate.storage_key,
                )
                .await?;
                report.missing_cleared = report.missing_cleared.saturating_add(1);
                continue;
            };
            self.bytes.delete(&candidate.storage_key).await?;
            clear_external_orphan_candidate(
                &mut tx,
                tenant,
                repository,
                &self.options.backend_name,
                &candidate.storage_key,
            )
            .await?;
            report.deleted = report.deleted.saturating_add(1);
            report.deleted_bytes = report.deleted_bytes.saturating_add(size_bytes);
        }
        tx.commit().await?;
        Ok(report)
    }

    async fn register_external_orphan_candidates(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        candidates: &[(String, u64)],
    ) -> Result<()> {
        if candidates.is_empty() {
            return Ok(());
        }
        let candidates = candidates
            .iter()
            .map(|(storage_key, size_bytes)| {
                i64::try_from(*size_bytes)
                    .map(|size_bytes| (storage_key, size_bytes))
                    .map_err(|_| Error::Backend("external orphan size exceeds i64".to_owned()))
            })
            .collect::<Result<Vec<_>>>()?;
        let mut tx = self.pool().begin().await?;
        lock_import_repository(&mut tx, tenant, repository).await?;
        const POSTGRES_BIND_LIMIT: usize = 65_535;
        const BINDS_PER_ROW: usize = 6;
        for chunk in candidates.chunks(POSTGRES_BIND_LIMIT / BINDS_PER_ROW) {
            let mut query = QueryBuilder::<Postgres>::new(
                "insert into grit_external_orphan_candidates
                    (tenant_id, repository_id, storage_backend, storage_key, size_bytes,
                     first_observed_at) ",
            );
            query.push_values(chunk, |mut row, (storage_key, size_bytes)| {
                row.push_bind(tenant.as_str())
                    .push_bind(repository.as_str())
                    .push_bind(&self.options.backend_name)
                    .push_bind(storage_key)
                    .push_bind(*size_bytes)
                    .push_bind(Option::<OffsetDateTime>::None);
            });
            query.push(
                " on conflict (tenant_id, repository_id, storage_backend, storage_key)
                  do update set size_bytes = excluded.size_bytes, first_observed_at = null",
            );
            query.build().persistent(false).execute(&mut *tx).await?;
        }
        tx.commit().await?;
        Ok(())
    }
}

#[async_trait]
impl<B> ImportStateStore for PgExternalizedStorage<B>
where
    B: ExternalByteStore,
{
    async fn begin_import(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
    ) -> Result<ImportSession> {
        self.sql.begin_import(tenant, repository).await
    }

    async fn publish_import(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        session: &ImportSession,
        publication: &ImportPublication,
    ) -> Result<ImportPublicationResult> {
        self.sql
            .publish_import(tenant, repository, session, publication)
            .await
    }

    async fn complete_import(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        session: &ImportSession,
    ) -> Result<()> {
        self.sql.complete_import(tenant, repository, session).await
    }
}

#[async_trait]
impl<B> RefStore for PgExternalizedStorage<B>
where
    B: ExternalByteStore,
{
    async fn read_ref(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        refname: &str,
    ) -> Result<Option<StoredRef>> {
        self.sql.read_ref(tenant, repository, refname).await
    }

    async fn write_ref(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        refname: &str,
        value: &StoredRef,
        expected: Option<Option<StoredRef>>,
    ) -> Result<()> {
        self.sql
            .write_ref(tenant, repository, refname, value, expected)
            .await
    }

    async fn delete_ref(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        refname: &str,
        expected: Option<StoredRef>,
    ) -> Result<()> {
        self.sql
            .delete_ref(tenant, repository, refname, expected)
            .await
    }

    async fn list_refs(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        prefix: &str,
    ) -> Result<Vec<(String, StoredRef)>> {
        self.sql.list_refs(tenant, repository, prefix).await
    }
}

#[async_trait]
impl<B> ReflogStore for PgExternalizedStorage<B>
where
    B: ExternalByteStore,
{
    async fn append_reflog(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        entry: &ReflogEntry,
    ) -> Result<()> {
        self.sql.append_reflog(tenant, repository, entry).await
    }

    async fn read_reflog(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        refname: &str,
    ) -> Result<Vec<ReflogEntry>> {
        self.sql.read_reflog(tenant, repository, refname).await
    }
}

#[async_trait]
impl<B> ConfigStore for PgExternalizedStorage<B>
where
    B: ExternalByteStore,
{
    async fn get_config(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        key: &str,
    ) -> Result<Option<String>> {
        self.sql.get_config(tenant, repository, key).await
    }

    async fn set_config(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        key: &str,
        value: &str,
    ) -> Result<()> {
        self.sql.set_config(tenant, repository, key, value).await
    }

    async fn list_config(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        prefix: &str,
    ) -> Result<Vec<(String, String)>> {
        self.sql.list_config(tenant, repository, prefix).await
    }
}

#[async_trait]
impl<B> BrowseIndex for PgExternalizedStorage<B>
where
    B: ExternalByteStore,
{
    async fn upsert_tree_entries(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        entries: &[IndexedTreeEntry],
    ) -> Result<()> {
        self.sql
            .upsert_tree_entries(tenant, repository, entries)
            .await
    }

    async fn replace_tree_entries(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        entries: &[IndexedTreeEntry],
    ) -> Result<()> {
        self.sql
            .replace_tree_entries(tenant, repository, entries)
            .await
    }

    async fn list_tree_entries(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        tree_oid: &ObjectId,
        prefix: &str,
    ) -> Result<Vec<IndexedTreeEntry>> {
        self.sql
            .list_tree_entries(tenant, repository, tree_oid, prefix)
            .await
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
        .fetch_optional(self.pool())
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
impl<B> CommitGraphStore for PgExternalizedStorage<B>
where
    B: ExternalByteStore,
{
    async fn upsert_commits(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        commits: &[IndexedCommit],
    ) -> Result<()> {
        self.sql.upsert_commits(tenant, repository, commits).await
    }

    async fn replace_commit_graph(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        commits: &[IndexedCommit],
    ) -> Result<()> {
        self.sql
            .replace_commit_graph(tenant, repository, commits)
            .await
    }

    async fn read_indexed_commit(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        oid: &ObjectId,
    ) -> Result<Option<IndexedCommit>> {
        self.sql.read_indexed_commit(tenant, repository, oid).await
    }

    async fn commit_parents(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        oid: &ObjectId,
    ) -> Result<Vec<ObjectId>> {
        self.sql.commit_parents(tenant, repository, oid).await
    }

    async fn commit_children(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        oid: &ObjectId,
    ) -> Result<Vec<ObjectId>> {
        self.sql.commit_children(tenant, repository, oid).await
    }

    async fn list_indexed_commits(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
    ) -> Result<Vec<IndexedCommit>> {
        self.sql.list_indexed_commits(tenant, repository).await
    }
}

#[async_trait]
impl<B> PackStore for PgExternalizedStorage<B>
where
    B: ExternalByteStore,
{
    fn supports_native_pack_import(&self) -> bool {
        self.options.write_packs_externally || self.sql.supports_native_pack_import()
    }

    async fn write_imported_packs(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        packs: Vec<ImportedPack>,
    ) -> Result<Vec<PackMetadata>> {
        if !self.options.write_packs_externally {
            return self
                .sql
                .write_imported_packs(tenant, repository, packs)
                .await;
        }

        let (prepared, result_indexes) = prepare_imported_pack_batch(packs)?;
        let mut storage_keys = Vec::with_capacity(prepared.len());
        for prepared_pack in &prepared {
            let pack = &prepared_pack.pack;
            let key = self.pack_key(
                tenant,
                repository,
                &pack.metadata.pack_checksum,
                validated_pack_algo_name(&pack.metadata, &pack.index),
            );
            storage_keys.push(key);
        }
        let candidates = prepared
            .iter()
            .zip(&storage_keys)
            .map(|(prepared_pack, key)| (key.clone(), prepared_pack.pack.metadata.size_bytes))
            .collect::<Vec<_>>();
        self.register_external_orphan_candidates(tenant, repository, &candidates)
            .await?;
        for (prepared_pack, key) in prepared.iter().zip(&storage_keys) {
            self.bytes
                .put_large_if_absent(key, prepared_pack.pack.data.as_slice())
                .await?;
        }
        // Refresh registration after potentially long uploads. This recreates a candidate that a
        // concurrent sweep may have cleared while bytes were still in flight.
        self.register_external_orphan_candidates(tenant, repository, &candidates)
            .await?;

        // SQL is the visibility boundary. A later transaction failure may leave immutable,
        // content-addressed bytes for the orphan sweeper, but cannot expose partial pack rows.
        let mut tx = self.pool().begin().await?;
        lock_import_repository(&mut tx, tenant, repository).await?;
        let mut unique_metadata = Vec::with_capacity(prepared.len());
        let mut live_storage_keys = Vec::with_capacity(storage_keys.len());
        for (prepared_pack, storage_key) in prepared.iter().zip(&storage_keys) {
            verify_external_pack_size(
                self.bytes.as_ref(),
                storage_key,
                prepared_pack.pack.metadata.size_bytes,
            )
            .await?;
            unique_metadata.push(
                install_external_pack_in_transaction(
                    &mut tx,
                    tenant,
                    repository,
                    &prepared_pack.pack.metadata,
                    &prepared_pack.pack.index,
                    &prepared_pack.values,
                    &self.options.backend_name,
                    storage_key,
                )
                .await?,
            );
            if external_key_is_live(
                &mut tx,
                tenant,
                repository,
                &self.options.backend_name,
                storage_key,
            )
            .await?
            {
                live_storage_keys.push(storage_key.clone());
            }
        }
        clear_external_orphan_candidates(
            &mut tx,
            tenant,
            repository,
            &self.options.backend_name,
            &live_storage_keys,
        )
        .await?;
        tx.commit().await?;

        Ok(result_indexes
            .into_iter()
            .map(|index| unique_metadata[index].clone())
            .collect())
    }

    async fn write_pack(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        pack: &StoredPack,
    ) -> Result<PackMetadata> {
        if !self.options.write_packs_externally {
            return self.sql.write_pack(tenant, repository, pack).await;
        }

        let values = prepare_pack_values(&pack.metadata, pack.data.len(), &pack.index)?;
        let key = self.pack_key(
            tenant,
            repository,
            &pack.metadata.pack_checksum,
            validated_pack_algo_name(&pack.metadata, &pack.index),
        );
        self.register_external_orphan_candidates(
            tenant,
            repository,
            &[(key.clone(), pack.metadata.size_bytes)],
        )
        .await?;
        self.bytes.put_large_if_absent(&key, &pack.data).await?;
        self.register_external_orphan_candidates(
            tenant,
            repository,
            &[(key.clone(), pack.metadata.size_bytes)],
        )
        .await?;
        let mut tx = self.pool().begin().await?;
        lock_import_repository(&mut tx, tenant, repository).await?;
        verify_external_pack_size(self.bytes.as_ref(), &key, pack.metadata.size_bytes).await?;
        let metadata = install_external_pack_in_transaction(
            &mut tx,
            tenant,
            repository,
            &pack.metadata,
            &pack.index,
            &values,
            &self.options.backend_name,
            &key,
        )
        .await?;
        if external_key_is_live(
            &mut tx,
            tenant,
            repository,
            &self.options.backend_name,
            &key,
        )
        .await?
        {
            clear_external_orphan_candidate(
                &mut tx,
                tenant,
                repository,
                &self.options.backend_name,
                &key,
            )
            .await?;
        }
        tx.commit().await?;
        Ok(metadata)
    }

    async fn read_pack_metadata(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        pack_checksum: &[u8],
    ) -> Result<Option<PackMetadata>> {
        self.sql
            .read_pack_metadata(tenant, repository, pack_checksum)
            .await
    }

    async fn read_pack_data(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        pack_checksum: &[u8],
    ) -> Result<Option<Vec<u8>>> {
        let Some(stored) = self.pack_storage(tenant, repository, pack_checksum).await? else {
            return Ok(None);
        };
        if let Some(data) = stored.data {
            return Ok(Some(data));
        }
        let key = self.external_key("pack", &bytes_to_hex(pack_checksum), stored)?;
        self.bytes.get(&key).await?.map(Some).ok_or_else(|| {
            Error::Backend(format!(
                "pack {} points at missing external bytes {key}",
                bytes_to_hex(pack_checksum)
            ))
        })
    }

    async fn read_pack_range(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        pack_checksum: &[u8],
        start: u64,
        len: u64,
    ) -> Result<Option<Vec<u8>>> {
        let Some(stored) = self.pack_storage(tenant, repository, pack_checksum).await? else {
            return Ok(None);
        };
        if let Some(data) = stored.data {
            let start = usize::try_from(start)
                .map_err(|_| Error::Backend("pack range start exceeds usize".to_owned()))?;
            let len = usize::try_from(len)
                .map_err(|_| Error::Backend("pack range length exceeds usize".to_owned()))?;
            let end = start
                .checked_add(len)
                .ok_or_else(|| Error::Backend("pack range overflow".to_owned()))?;
            if start >= data.len() {
                return Ok(Some(Vec::new()));
            }
            return Ok(Some(data[start..end.min(data.len())].to_vec()));
        }
        let key = self.external_key("pack", &bytes_to_hex(pack_checksum), stored)?;
        self.bytes.get_range(&key, start, len).await
    }

    async fn find_packed_object(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        oid: &ObjectId,
    ) -> Result<Option<(PackMetadata, PackObjectIndex)>> {
        self.sql.find_packed_object(tenant, repository, oid).await
    }

    async fn read_pack_index_at_offset(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        pack_checksum: &[u8],
        offset: u64,
    ) -> Result<Option<PackObjectIndex>> {
        self.sql
            .read_pack_index_at_offset(tenant, repository, pack_checksum, offset)
            .await
    }

    async fn list_packs(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
    ) -> Result<Vec<PackMetadata>> {
        self.sql.list_packs(tenant, repository).await
    }

    async fn list_pack_objects(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        pack_checksum: Option<&[u8]>,
    ) -> Result<Vec<(PackMetadata, PackObjectIndex)>> {
        self.sql
            .list_pack_objects(tenant, repository, pack_checksum)
            .await
    }
}

async fn verify_external_pack_size<B>(bytes: &B, storage_key: &str, expected: u64) -> Result<()>
where
    B: ExternalByteStore,
{
    let actual = bytes.content_length(storage_key).await?.ok_or_else(|| {
        Error::Backend(format!(
            "uploaded external pack is missing at {storage_key}"
        ))
    })?;
    if actual != expected {
        return Err(Error::Backend(format!(
            "uploaded external pack at {storage_key} has length {actual}, expected {expected}"
        )));
    }
    Ok(())
}

async fn external_key_is_live(
    tx: &mut Transaction<'static, Postgres>,
    tenant: &TenantId,
    repository: &RepositoryId,
    backend_name: &str,
    storage_key: &str,
) -> Result<bool> {
    sqlx::query_scalar(
        "select exists(
            select 1 from grit_packs
            where tenant_id = $1 and repository_id = $2
              and storage_backend = $3 and storage_key = $4
            union all
            select 1 from grit_objects
            where tenant_id = $1 and repository_id = $2
              and storage_backend = $3 and storage_key = $4
         )",
    )
    .bind(tenant.as_str())
    .bind(repository.as_str())
    .bind(backend_name)
    .bind(storage_key)
    .fetch_one(&mut **tx)
    .await
    .map_err(Into::into)
}

async fn clear_external_orphan_candidate(
    tx: &mut Transaction<'static, Postgres>,
    tenant: &TenantId,
    repository: &RepositoryId,
    backend_name: &str,
    storage_key: &str,
) -> Result<()> {
    sqlx::query(
        "delete from grit_external_orphan_candidates
         where tenant_id = $1 and repository_id = $2 and storage_backend = $3
           and storage_key = $4",
    )
    .bind(tenant.as_str())
    .bind(repository.as_str())
    .bind(backend_name)
    .bind(storage_key)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

async fn clear_external_orphan_candidates(
    tx: &mut Transaction<'static, Postgres>,
    tenant: &TenantId,
    repository: &RepositoryId,
    backend_name: &str,
    storage_keys: &[String],
) -> Result<()> {
    const POSTGRES_BIND_LIMIT: usize = 65_535;
    const FIXED_BINDS: usize = 3;
    for keys in storage_keys.chunks(POSTGRES_BIND_LIMIT - FIXED_BINDS) {
        if keys.is_empty() {
            continue;
        }
        let mut query = QueryBuilder::<Postgres>::new(
            "delete from grit_external_orphan_candidates where tenant_id = ",
        );
        query
            .push_bind(tenant.as_str())
            .push(" and repository_id = ")
            .push_bind(repository.as_str())
            .push(" and storage_backend = ")
            .push_bind(backend_name)
            .push(" and storage_key in (");
        let mut separated = query.separated(", ");
        for key in keys {
            separated.push_bind(key);
        }
        separated.push_unseparated(")");
        query.build().persistent(false).execute(&mut **tx).await?;
    }
    Ok(())
}

fn validated_pack_algo_name(metadata: &PackMetadata, index: &[PackObjectIndex]) -> &'static str {
    index.first().map_or_else(
        || {
            if metadata.pack_checksum.len() == 32 {
                "sha256"
            } else {
                "sha1"
            }
        },
        |entry| entry.oid.algo().name(),
    )
}

fn stored_bytes_from_row(row: &sqlx::postgres::PgRow) -> Result<StoredBytes> {
    Ok(StoredBytes {
        data: row.try_get("data")?,
        storage_backend: row.try_get("storage_backend")?,
        storage_key: row.try_get("storage_key")?,
    })
}

fn scoped_key(
    prefix: &str,
    tenant: &TenantId,
    repository: &RepositoryId,
    category: &str,
    algo_name: &str,
    name: &str,
) -> String {
    let key = format!(
        "tenants/{}/repositories/{}/{}/{}/{}",
        tenant.as_str(),
        repository.as_str(),
        category,
        algo_name,
        name
    );
    if prefix.is_empty() {
        key
    } else {
        format!("{}/{key}", prefix.trim_matches('/'))
    }
}

fn name_to_kind(name: &str) -> Result<ObjectKind> {
    match name {
        "blob" => Ok(ObjectKind::Blob),
        "tree" => Ok(ObjectKind::Tree),
        "commit" => Ok(ObjectKind::Commit),
        "tag" => Ok(ObjectKind::Tag),
        other => Err(Error::Backend(format!("unknown object kind {other}"))),
    }
}

fn bytes_to_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}
