//! Read-through cache wrapper for server storage backends.

use std::sync::Arc;

use async_trait::async_trait;
use grit_lib::objects::{ObjectId, ObjectKind};

use crate::cache::{Cache, CacheKey, CacheValue};
use crate::error::{Error, Result};
use crate::ids::{RepositoryId, TenantId};
use crate::storage::{
    BrowseIndex, ConfigStore, IndexedTreeEntry, ObjectStore, RefStore, ReflogEntry, ReflogStore,
    StoredObject, StoredRef,
};

/// Storage wrapper that caches hot object and ref reads.
pub struct CachedStorage<S, C> {
    storage: Arc<S>,
    cache: Arc<C>,
}

impl<S, C> CachedStorage<S, C> {
    /// Create a cached storage wrapper.
    #[must_use]
    pub fn new(storage: Arc<S>, cache: Arc<C>) -> Self {
        Self { storage, cache }
    }

    /// Borrow the durable storage backend.
    #[must_use]
    pub fn storage(&self) -> &Arc<S> {
        &self.storage
    }

    /// Borrow the cache backend.
    #[must_use]
    pub fn cache(&self) -> &Arc<C> {
        &self.cache
    }
}

#[async_trait]
impl<S, C> ObjectStore for CachedStorage<S, C>
where
    S: ObjectStore,
    C: Cache,
{
    async fn read_object(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        oid: &ObjectId,
    ) -> Result<Option<StoredObject>> {
        let key = CacheKey::Object(oid.to_hex());
        if let Some(value) = self.cache.get(tenant, repository, &key).await? {
            return decode_object(&value.bytes).map(Some);
        }
        let object = self.storage.read_object(tenant, repository, oid).await?;
        if let Some(object) = object.as_ref() {
            self.cache
                .put(
                    tenant,
                    repository,
                    &key,
                    CacheValue::new(encode_object(object)),
                )
                .await?;
        }
        Ok(object)
    }

    async fn write_object(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        oid: &ObjectId,
        object: &StoredObject,
    ) -> Result<()> {
        self.storage
            .write_object(tenant, repository, oid, object)
            .await?;
        self.cache
            .put(
                tenant,
                repository,
                &CacheKey::Object(oid.to_hex()),
                CacheValue::new(encode_object(object)),
            )
            .await
    }

    async fn object_exists(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        oid: &ObjectId,
    ) -> Result<bool> {
        if self
            .cache
            .get(tenant, repository, &CacheKey::Object(oid.to_hex()))
            .await?
            .is_some()
        {
            return Ok(true);
        }
        self.storage.object_exists(tenant, repository, oid).await
    }

    async fn count_objects(&self, tenant: &TenantId, repository: &RepositoryId) -> Result<usize> {
        self.storage.count_objects(tenant, repository).await
    }

    async fn list_object_ids(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        kind: Option<ObjectKind>,
    ) -> Result<Vec<(ObjectId, ObjectKind)>> {
        self.storage.list_object_ids(tenant, repository, kind).await
    }
}

#[async_trait]
impl<S, C> RefStore for CachedStorage<S, C>
where
    S: RefStore,
    C: Cache,
{
    async fn read_ref(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        refname: &str,
    ) -> Result<Option<StoredRef>> {
        let key = CacheKey::Ref(refname.to_owned());
        if let Some(value) = self.cache.get(tenant, repository, &key).await? {
            return decode_ref(&value.bytes).map(Some);
        }
        let value = self.storage.read_ref(tenant, repository, refname).await?;
        if let Some(value) = value.as_ref() {
            self.cache
                .put(tenant, repository, &key, CacheValue::new(encode_ref(value)))
                .await?;
        }
        Ok(value)
    }

    async fn write_ref(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        refname: &str,
        value: &StoredRef,
        expected: Option<Option<StoredRef>>,
    ) -> Result<()> {
        self.storage
            .write_ref(tenant, repository, refname, value, expected)
            .await?;
        self.cache
            .put(
                tenant,
                repository,
                &CacheKey::Ref(refname.to_owned()),
                CacheValue::new(encode_ref(value)),
            )
            .await?;
        self.cache
            .invalidate(
                tenant,
                repository,
                &CacheKey::RefList(ref_parent_prefix(refname)),
            )
            .await
    }

    async fn delete_ref(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        refname: &str,
        expected: Option<StoredRef>,
    ) -> Result<()> {
        self.storage
            .delete_ref(tenant, repository, refname, expected)
            .await?;
        self.cache
            .invalidate(tenant, repository, &CacheKey::Ref(refname.to_owned()))
            .await?;
        self.cache
            .invalidate(
                tenant,
                repository,
                &CacheKey::RefList(ref_parent_prefix(refname)),
            )
            .await
    }

    async fn list_refs(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        prefix: &str,
    ) -> Result<Vec<(String, StoredRef)>> {
        self.storage.list_refs(tenant, repository, prefix).await
    }
}

#[async_trait]
impl<S, C> ReflogStore for CachedStorage<S, C>
where
    S: ReflogStore,
    C: Cache,
{
    async fn append_reflog(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        entry: &ReflogEntry,
    ) -> Result<()> {
        self.storage.append_reflog(tenant, repository, entry).await
    }

    async fn read_reflog(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        refname: &str,
    ) -> Result<Vec<ReflogEntry>> {
        self.storage.read_reflog(tenant, repository, refname).await
    }
}

#[async_trait]
impl<S, C> ConfigStore for CachedStorage<S, C>
where
    S: ConfigStore,
    C: Cache,
{
    async fn get_config(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        key: &str,
    ) -> Result<Option<String>> {
        self.storage.get_config(tenant, repository, key).await
    }

    async fn set_config(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        key: &str,
        value: &str,
    ) -> Result<()> {
        self.storage
            .set_config(tenant, repository, key, value)
            .await
    }

    async fn list_config(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        prefix: &str,
    ) -> Result<Vec<(String, String)>> {
        self.storage.list_config(tenant, repository, prefix).await
    }
}

#[async_trait]
impl<S, C> BrowseIndex for CachedStorage<S, C>
where
    S: BrowseIndex,
    C: Cache,
{
    async fn upsert_tree_entries(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        entries: &[IndexedTreeEntry],
    ) -> Result<()> {
        self.storage
            .upsert_tree_entries(tenant, repository, entries)
            .await
    }

    async fn list_tree_entries(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        tree_oid: &ObjectId,
        prefix: &str,
    ) -> Result<Vec<IndexedTreeEntry>> {
        self.storage
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
        self.storage
            .read_blob_at_path(tenant, repository, tree_oid, path)
            .await
    }
}

fn encode_object(object: &StoredObject) -> Vec<u8> {
    let mut out = Vec::with_capacity(object.data.len() + 1);
    out.push(kind_code(object.kind));
    out.extend_from_slice(&object.data);
    out
}

fn decode_object(bytes: &[u8]) -> Result<StoredObject> {
    let Some((&kind, data)) = bytes.split_first() else {
        return Err(Error::Cache("cached object payload is empty".to_owned()));
    };
    Ok(StoredObject {
        kind: code_kind(kind)?,
        data: data.to_vec(),
    })
}

fn encode_ref(value: &StoredRef) -> Vec<u8> {
    match value {
        StoredRef::Direct(oid) => {
            let mut out = Vec::with_capacity(1 + oid.algo().hex_len());
            out.push(b'D');
            out.extend_from_slice(oid.to_hex().as_bytes());
            out
        }
        StoredRef::Symbolic(target) => {
            let mut out = Vec::with_capacity(1 + target.len());
            out.push(b'S');
            out.extend_from_slice(target.as_bytes());
            out
        }
    }
}

fn decode_ref(bytes: &[u8]) -> Result<StoredRef> {
    let Some((&kind, data)) = bytes.split_first() else {
        return Err(Error::Cache("cached ref payload is empty".to_owned()));
    };
    match kind {
        b'D' => {
            let hex = std::str::from_utf8(data)
                .map_err(|_| Error::Cache("cached direct ref is not UTF-8".to_owned()))?;
            Ok(StoredRef::Direct(ObjectId::from_hex(hex)?))
        }
        b'S' => {
            let target = std::str::from_utf8(data)
                .map_err(|_| Error::Cache("cached symbolic ref is not UTF-8".to_owned()))?;
            Ok(StoredRef::Symbolic(target.to_owned()))
        }
        other => Err(Error::Cache(format!("unknown cached ref kind {other}"))),
    }
}

fn kind_code(kind: ObjectKind) -> u8 {
    match kind {
        ObjectKind::Blob => b'B',
        ObjectKind::Tree => b'T',
        ObjectKind::Commit => b'C',
        ObjectKind::Tag => b'G',
    }
}

fn code_kind(code: u8) -> Result<ObjectKind> {
    match code {
        b'B' => Ok(ObjectKind::Blob),
        b'T' => Ok(ObjectKind::Tree),
        b'C' => Ok(ObjectKind::Commit),
        b'G' => Ok(ObjectKind::Tag),
        other => Err(Error::Cache(format!("unknown cached object kind {other}"))),
    }
}

fn ref_parent_prefix(refname: &str) -> String {
    match refname.rsplit_once('/') {
        Some((parent, _)) => format!("{parent}/"),
        None => String::new(),
    }
}
