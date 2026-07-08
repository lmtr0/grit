//! Read-through cache wrapper for server storage backends.

use std::sync::Arc;

use async_trait::async_trait;
use grit_lib::objects::{ObjectId, ObjectKind};
use serde::{Deserialize, Serialize};

use crate::cache::{Cache, CacheKey, CacheValue, CacheValueKind};
use crate::error::{Error, Result};
use crate::ids::{RepositoryId, TenantId};
use crate::storage::{
    BrowseIndex, CommitGraphStore, ConfigStore, IndexedCommit, IndexedTreeEntry, ObjectStore,
    PackMetadata, PackObjectIndex, PackStore, RefStore, ReflogEntry, ReflogStore, StoredObject,
    StoredPack, StoredRef,
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
            return decode_object(&value).map(Some);
        }
        let object = self.storage.read_object(tenant, repository, oid).await?;
        if let Some(object) = object.as_ref() {
            self.cache
                .put(
                    tenant,
                    repository,
                    &key,
                    CacheValue::typed(CacheValueKind::Object, encode_object(object)?),
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
                CacheValue::typed(CacheValueKind::Object, encode_object(object)?),
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
            return decode_ref(&value).map(Some);
        }
        let value = self.storage.read_ref(tenant, repository, refname).await?;
        if let Some(value) = value.as_ref() {
            self.cache
                .put(
                    tenant,
                    repository,
                    &key,
                    CacheValue::typed(CacheValueKind::Ref, encode_ref(value)?),
                )
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
                CacheValue::typed(CacheValueKind::Ref, encode_ref(value)?),
            )
            .await?;
        invalidate_ref_lists(self.cache.as_ref(), tenant, repository, refname).await
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
        invalidate_ref_lists(self.cache.as_ref(), tenant, repository, refname).await
    }

    async fn list_refs(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        prefix: &str,
    ) -> Result<Vec<(String, StoredRef)>> {
        let key = CacheKey::RefList(prefix.to_owned());
        if let Some(value) = self.cache.get(tenant, repository, &key).await? {
            return decode_ref_list(&value);
        }
        let refs = self.storage.list_refs(tenant, repository, prefix).await?;
        self.cache
            .put(
                tenant,
                repository,
                &key,
                CacheValue::typed(CacheValueKind::RefList, encode_ref_list(&refs)?),
            )
            .await?;
        Ok(refs)
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
        let cache_key = CacheKey::Config(key.to_owned());
        if let Some(value) = self.cache.get(tenant, repository, &cache_key).await? {
            return decode_config(&value).map(Some);
        }
        let value = self.storage.get_config(tenant, repository, key).await?;
        if let Some(value) = value.as_ref() {
            self.cache
                .put(
                    tenant,
                    repository,
                    &cache_key,
                    CacheValue::typed(CacheValueKind::Config, encode_config(value)?),
                )
                .await?;
        }
        Ok(value)
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
            .await?;
        self.cache
            .put(
                tenant,
                repository,
                &CacheKey::Config(key.to_owned()),
                CacheValue::typed(CacheValueKind::Config, encode_config(value)?),
            )
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
            .await?;
        self.cache.invalidate_repository(tenant, repository).await
    }

    async fn replace_tree_entries(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        entries: &[IndexedTreeEntry],
    ) -> Result<()> {
        self.storage
            .replace_tree_entries(tenant, repository, entries)
            .await?;
        self.cache.invalidate_repository(tenant, repository).await
    }

    async fn list_tree_entries(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        tree_oid: &ObjectId,
        prefix: &str,
    ) -> Result<Vec<IndexedTreeEntry>> {
        let key = CacheKey::TreeList {
            tree: tree_oid.to_hex(),
            prefix: prefix.to_owned(),
        };
        if let Some(value) = self.cache.get(tenant, repository, &key).await? {
            return decode_tree_list(&value);
        }
        let entries = self
            .storage
            .list_tree_entries(tenant, repository, tree_oid, prefix)
            .await?;
        self.cache
            .put(
                tenant,
                repository,
                &key,
                CacheValue::typed(CacheValueKind::TreeList, encode_tree_list(&entries)?),
            )
            .await?;
        Ok(entries)
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

#[async_trait]
impl<S, C> CommitGraphStore for CachedStorage<S, C>
where
    S: CommitGraphStore,
    C: Cache,
{
    async fn upsert_commits(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        commits: &[IndexedCommit],
    ) -> Result<()> {
        self.storage
            .upsert_commits(tenant, repository, commits)
            .await?;
        for commit in commits {
            self.cache
                .invalidate(
                    tenant,
                    repository,
                    &CacheKey::CommitSummary(commit.oid.to_hex()),
                )
                .await?;
        }
        Ok(())
    }

    async fn replace_commit_graph(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        commits: &[IndexedCommit],
    ) -> Result<()> {
        self.storage
            .replace_commit_graph(tenant, repository, commits)
            .await?;
        self.cache.invalidate_repository(tenant, repository).await
    }

    async fn read_indexed_commit(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        oid: &ObjectId,
    ) -> Result<Option<IndexedCommit>> {
        let key = CacheKey::CommitSummary(oid.to_hex());
        if let Some(value) = self.cache.get(tenant, repository, &key).await? {
            return decode_indexed_commit(&value).map(Some);
        }
        let commit = self
            .storage
            .read_indexed_commit(tenant, repository, oid)
            .await?;
        if let Some(commit) = commit.as_ref() {
            self.cache
                .put(
                    tenant,
                    repository,
                    &key,
                    CacheValue::typed(
                        CacheValueKind::CommitSummary,
                        encode_indexed_commit(commit)?,
                    ),
                )
                .await?;
        }
        Ok(commit)
    }

    async fn commit_parents(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        oid: &ObjectId,
    ) -> Result<Vec<ObjectId>> {
        self.storage.commit_parents(tenant, repository, oid).await
    }

    async fn commit_children(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        oid: &ObjectId,
    ) -> Result<Vec<ObjectId>> {
        self.storage.commit_children(tenant, repository, oid).await
    }

    async fn list_indexed_commits(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
    ) -> Result<Vec<IndexedCommit>> {
        self.storage.list_indexed_commits(tenant, repository).await
    }
}

#[async_trait]
impl<S, C> PackStore for CachedStorage<S, C>
where
    S: PackStore,
    C: Cache,
{
    async fn write_pack(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        pack: &StoredPack,
    ) -> Result<PackMetadata> {
        let metadata = self.storage.write_pack(tenant, repository, pack).await?;
        self.cache.invalidate_repository(tenant, repository).await?;
        Ok(metadata)
    }

    async fn read_pack_metadata(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        pack_checksum: &[u8],
    ) -> Result<Option<PackMetadata>> {
        self.storage
            .read_pack_metadata(tenant, repository, pack_checksum)
            .await
    }

    async fn read_pack_data(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        pack_checksum: &[u8],
    ) -> Result<Option<Vec<u8>>> {
        self.storage
            .read_pack_data(tenant, repository, pack_checksum)
            .await
    }

    async fn read_pack_range(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        pack_checksum: &[u8],
        start: u64,
        len: u64,
    ) -> Result<Option<Vec<u8>>> {
        self.storage
            .read_pack_range(tenant, repository, pack_checksum, start, len)
            .await
    }

    async fn find_packed_object(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        oid: &ObjectId,
    ) -> Result<Option<(PackMetadata, PackObjectIndex)>> {
        self.storage
            .find_packed_object(tenant, repository, oid)
            .await
    }

    async fn read_pack_index_at_offset(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        pack_checksum: &[u8],
        offset: u64,
    ) -> Result<Option<PackObjectIndex>> {
        self.storage
            .read_pack_index_at_offset(tenant, repository, pack_checksum, offset)
            .await
    }

    async fn list_packs(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
    ) -> Result<Vec<PackMetadata>> {
        self.storage.list_packs(tenant, repository).await
    }

    async fn list_pack_objects(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        pack_checksum: Option<&[u8]>,
    ) -> Result<Vec<(PackMetadata, PackObjectIndex)>> {
        self.storage
            .list_pack_objects(tenant, repository, pack_checksum)
            .await
    }
}

fn encode_object(object: &StoredObject) -> Result<Vec<u8>> {
    serde_json::to_vec(&StoredObjectWire::from(object))
        .map_err(|err| Error::Cache(format!("serialize cached object: {err}")))
}

fn decode_object(value: &CacheValue) -> Result<StoredObject> {
    require_kind(value, CacheValueKind::Object)?;
    let wire: StoredObjectWire = serde_json::from_slice(&value.bytes)
        .map_err(|err| Error::Cache(format!("decode cached object: {err}")))?;
    wire.try_into()
}

fn encode_ref(value: &StoredRef) -> Result<Vec<u8>> {
    serde_json::to_vec(&StoredRefWire::from(value))
        .map_err(|err| Error::Cache(format!("serialize cached ref: {err}")))
}

fn decode_ref(value: &CacheValue) -> Result<StoredRef> {
    require_kind(value, CacheValueKind::Ref)?;
    let wire: StoredRefWire = serde_json::from_slice(&value.bytes)
        .map_err(|err| Error::Cache(format!("decode cached ref: {err}")))?;
    wire.try_into()
}

fn encode_ref_list(refs: &[(String, StoredRef)]) -> Result<Vec<u8>> {
    let wire = refs
        .iter()
        .map(|(name, value)| (name.clone(), StoredRefWire::from(value)))
        .collect::<Vec<_>>();
    serde_json::to_vec(&wire)
        .map_err(|err| Error::Cache(format!("serialize cached ref list: {err}")))
}

fn decode_ref_list(value: &CacheValue) -> Result<Vec<(String, StoredRef)>> {
    require_kind(value, CacheValueKind::RefList)?;
    let wire: Vec<(String, StoredRefWire)> = serde_json::from_slice(&value.bytes)
        .map_err(|err| Error::Cache(format!("decode cached ref list: {err}")))?;
    wire.into_iter()
        .map(|(name, value)| Ok((name, value.try_into()?)))
        .collect()
}

fn encode_config(value: &str) -> Result<Vec<u8>> {
    serde_json::to_vec(value).map_err(|err| Error::Cache(format!("serialize cached config: {err}")))
}

fn decode_config(value: &CacheValue) -> Result<String> {
    require_kind(value, CacheValueKind::Config)?;
    serde_json::from_slice(&value.bytes)
        .map_err(|err| Error::Cache(format!("decode cached config: {err}")))
}

fn encode_tree_list(entries: &[IndexedTreeEntry]) -> Result<Vec<u8>> {
    let wire = entries
        .iter()
        .map(IndexedTreeEntryWire::from)
        .collect::<Vec<_>>();
    serde_json::to_vec(&wire)
        .map_err(|err| Error::Cache(format!("serialize cached tree list: {err}")))
}

fn decode_tree_list(value: &CacheValue) -> Result<Vec<IndexedTreeEntry>> {
    require_kind(value, CacheValueKind::TreeList)?;
    let wire: Vec<IndexedTreeEntryWire> = serde_json::from_slice(&value.bytes)
        .map_err(|err| Error::Cache(format!("decode cached tree list: {err}")))?;
    wire.into_iter().map(TryInto::try_into).collect()
}

fn encode_indexed_commit(commit: &IndexedCommit) -> Result<Vec<u8>> {
    serde_json::to_vec(&IndexedCommitWire::from(commit))
        .map_err(|err| Error::Cache(format!("serialize cached commit summary: {err}")))
}

fn decode_indexed_commit(value: &CacheValue) -> Result<IndexedCommit> {
    require_kind(value, CacheValueKind::CommitSummary)?;
    let wire: IndexedCommitWire = serde_json::from_slice(&value.bytes)
        .map_err(|err| Error::Cache(format!("decode cached commit summary: {err}")))?;
    wire.try_into()
}

fn require_kind(value: &CacheValue, expected: CacheValueKind) -> Result<()> {
    if value.version != crate::cache::CACHE_VALUE_VERSION {
        return Err(Error::Cache(format!(
            "unsupported cache value version {}",
            value.version
        )));
    }
    if value.kind != expected {
        return Err(Error::Cache(format!(
            "cached value kind {:?} does not match expected {:?}",
            value.kind, expected
        )));
    }
    Ok(())
}

fn kind_code(kind: ObjectKind) -> &'static str {
    match kind {
        ObjectKind::Blob => "blob",
        ObjectKind::Tree => "tree",
        ObjectKind::Commit => "commit",
        ObjectKind::Tag => "tag",
    }
}

fn code_kind(code: &str) -> Result<ObjectKind> {
    match code {
        "blob" => Ok(ObjectKind::Blob),
        "tree" => Ok(ObjectKind::Tree),
        "commit" => Ok(ObjectKind::Commit),
        "tag" => Ok(ObjectKind::Tag),
        other => Err(Error::Cache(format!("unknown cached object kind {other}"))),
    }
}

async fn invalidate_ref_lists<C>(
    cache: &C,
    tenant: &TenantId,
    repository: &RepositoryId,
    refname: &str,
) -> Result<()>
where
    C: Cache,
{
    for prefix in ref_list_prefixes(refname) {
        cache
            .invalidate(tenant, repository, &CacheKey::RefList(prefix))
            .await?;
    }
    Ok(())
}

fn ref_list_prefixes(refname: &str) -> Vec<String> {
    let mut prefixes = vec![String::new()];
    let mut prefix = String::new();
    for component in refname
        .split('/')
        .take_while(|component| !component.is_empty())
    {
        prefix.push_str(component);
        prefix.push('/');
        prefixes.push(prefix.clone());
    }
    prefixes
}

#[derive(Serialize, Deserialize)]
struct StoredObjectWire {
    kind: String,
    data: Vec<u8>,
}

impl From<&StoredObject> for StoredObjectWire {
    fn from(object: &StoredObject) -> Self {
        Self {
            kind: kind_code(object.kind).to_owned(),
            data: object.data.clone(),
        }
    }
}

impl TryFrom<StoredObjectWire> for StoredObject {
    type Error = Error;

    fn try_from(value: StoredObjectWire) -> Result<Self> {
        Ok(Self {
            kind: code_kind(&value.kind)?,
            data: value.data,
        })
    }
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "type", content = "value", rename_all = "snake_case")]
enum StoredRefWire {
    Direct(String),
    Symbolic(String),
}

impl From<&StoredRef> for StoredRefWire {
    fn from(value: &StoredRef) -> Self {
        match value {
            StoredRef::Direct(oid) => Self::Direct(oid.to_hex()),
            StoredRef::Symbolic(target) => Self::Symbolic(target.clone()),
        }
    }
}

impl TryFrom<StoredRefWire> for StoredRef {
    type Error = Error;

    fn try_from(value: StoredRefWire) -> Result<Self> {
        match value {
            StoredRefWire::Direct(oid) => Ok(Self::Direct(ObjectId::from_hex(&oid)?)),
            StoredRefWire::Symbolic(target) => Ok(Self::Symbolic(target)),
        }
    }
}

#[derive(Serialize, Deserialize)]
struct IndexedTreeEntryWire {
    tree_oid: String,
    path: String,
    mode: u32,
    oid: String,
    kind: String,
    size: Option<u64>,
}

impl From<&IndexedTreeEntry> for IndexedTreeEntryWire {
    fn from(entry: &IndexedTreeEntry) -> Self {
        Self {
            tree_oid: entry.tree_oid.to_hex(),
            path: entry.path.clone(),
            mode: entry.mode,
            oid: entry.oid.to_hex(),
            kind: kind_code(entry.kind).to_owned(),
            size: entry.size,
        }
    }
}

impl TryFrom<IndexedTreeEntryWire> for IndexedTreeEntry {
    type Error = Error;

    fn try_from(value: IndexedTreeEntryWire) -> Result<Self> {
        Ok(Self {
            tree_oid: ObjectId::from_hex(&value.tree_oid)?,
            path: value.path,
            mode: value.mode,
            oid: ObjectId::from_hex(&value.oid)?,
            kind: code_kind(&value.kind)?,
            size: value.size,
        })
    }
}

#[derive(Serialize, Deserialize)]
struct IndexedCommitWire {
    oid: String,
    tree: String,
    parents: Vec<String>,
    commit_time: i64,
    generation: u32,
}

impl From<&IndexedCommit> for IndexedCommitWire {
    fn from(commit: &IndexedCommit) -> Self {
        Self {
            oid: commit.oid.to_hex(),
            tree: commit.tree.to_hex(),
            parents: commit.parents.iter().map(ObjectId::to_hex).collect(),
            commit_time: commit.commit_time,
            generation: commit.generation,
        }
    }
}

impl TryFrom<IndexedCommitWire> for IndexedCommit {
    type Error = Error;

    fn try_from(value: IndexedCommitWire) -> Result<Self> {
        Ok(Self {
            oid: ObjectId::from_hex(&value.oid)?,
            tree: ObjectId::from_hex(&value.tree)?,
            parents: value
                .parents
                .iter()
                .map(|oid| ObjectId::from_hex(oid).map_err(Error::from))
                .collect::<Result<Vec<_>>>()?,
            commit_time: value.commit_time,
            generation: value.generation,
        })
    }
}
