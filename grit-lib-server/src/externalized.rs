//! PostgreSQL metadata storage with externalized immutable object bytes.

use std::sync::Arc;

use async_trait::async_trait;
use grit_lib::objects::{ObjectId, ObjectKind};
use sqlx::{PgPool, Postgres, Row, Transaction};

use crate::error::{Error, Result};
use crate::external::ExternalByteStore;
use crate::ids::{RepositoryId, TenantId};
use crate::sqlx_postgres::{
    lock_import_repository, write_imported_object_rows_in_transaction, ImportedObjectRow,
    PgServerStorage,
};
use crate::storage::{
    BrowseIndex, CommitGraphStore, ConfigStore, ImportPublication, ImportPublicationResult,
    ImportSession, ImportStateStore, IndexedCommit, IndexedTreeEntry, ObjectStore, PackMetadata,
    PackObjectIndex, PackStore, RefStore, ReflogEntry, ReflogStore, StoredObject, StoredPack,
    StoredRef,
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
    async fn write_pack(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        pack: &StoredPack,
    ) -> Result<PackMetadata> {
        if !self.options.write_packs_externally {
            return self.sql.write_pack(tenant, repository, pack).await;
        }

        let mut tx = self.pool().begin().await?;
        lock_import_repository(&mut tx, tenant, repository).await?;
        let key = self.pack_key(
            tenant,
            repository,
            &pack.metadata.pack_checksum,
            pack.index
                .first()
                .map(|entry| entry.oid.algo().name())
                .unwrap_or("sha1"),
        );
        self.bytes.put_if_absent(&key, &pack.data).await?;
        let metadata = write_external_pack_metadata_in_transaction(
            &mut tx,
            tenant,
            repository,
            pack,
            &self.options.backend_name,
            &key,
        )
        .await?;
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

async fn write_external_pack_metadata_in_transaction(
    tx: &mut Transaction<'static, Postgres>,
    tenant: &TenantId,
    repository: &RepositoryId,
    pack: &StoredPack,
    backend_name: &str,
    storage_key: &str,
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
             storage_backend, storage_key, object_count, size_bytes)
         values ($1, $2, $3, $4, null, $5, $6, $7, $8)
         on conflict (tenant_id, repository_id, pack_checksum)
         do update set pack_checksum = excluded.pack_checksum
         returning pack_checksum, index_checksum, object_count, size_bytes, storage_order",
    )
    .bind(tenant.as_str())
    .bind(repository.as_str())
    .bind(&pack_checksum)
    .bind(&index_checksum)
    .bind(backend_name)
    .bind(storage_key)
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

fn stored_bytes_from_row(row: &sqlx::postgres::PgRow) -> Result<StoredBytes> {
    Ok(StoredBytes {
        data: row.try_get("data")?,
        storage_backend: row.try_get("storage_backend")?,
        storage_key: row.try_get("storage_key")?,
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
            "pack metadata contains negative numeric values".to_owned(),
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
        other => Err(Error::Backend(format!("unknown object kind {other}"))),
    }
}

fn bytes_to_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn hex_to_bytes(hex: &str) -> Result<Vec<u8>> {
    if !hex.len().is_multiple_of(2) {
        return Err(Error::Backend("hex byte string has odd length".to_owned()));
    }
    let mut out = Vec::with_capacity(hex.len() / 2);
    for chunk in hex.as_bytes().chunks(2) {
        let text = std::str::from_utf8(chunk)
            .map_err(|err| Error::Backend(format!("invalid hex byte utf8: {err}")))?;
        out.push(
            u8::from_str_radix(text, 16)
                .map_err(|err| Error::Backend(format!("invalid hex byte: {err}")))?,
        );
    }
    Ok(out)
}
