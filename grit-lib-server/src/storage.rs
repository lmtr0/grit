//! Backend traits for server-hosted Git repository data.

use async_trait::async_trait;
use grit_lib::objects::{HashAlgo, ObjectId, ObjectKind};
use sha1::{Digest as _, Sha1};
use sha2::Sha256;
use std::sync::Arc;
use time::OffsetDateTime;

use crate::error::Result;
use crate::ids::{RepositoryId, TenantId};

fn object_kind_name(kind: ObjectKind) -> &'static str {
    match kind {
        ObjectKind::Blob => "blob",
        ObjectKind::Tree => "tree",
        ObjectKind::Commit => "commit",
        ObjectKind::Tag => "tag",
    }
}

/// Git object payload stored by a server backend.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoredObject {
    /// Git object kind.
    pub kind: ObjectKind,
    /// Header-stripped object bytes.
    pub data: Vec<u8>,
}

/// Metadata for one stored packfile.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PackMetadata {
    /// Pack trailing checksum bytes.
    pub pack_checksum: Vec<u8>,
    /// Checksum of the object index rows associated with this pack.
    pub index_checksum: Vec<u8>,
    /// Number of objects recorded in the pack header.
    pub object_count: u32,
    /// Number of bytes in the complete pack, including the trailing checksum.
    pub size_bytes: u64,
    /// Monotonic storage order assigned by the backend.
    pub storage_order: u64,
}

/// One object-to-pack index row.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PackObjectIndex {
    /// Object id for this packed representation.
    pub oid: ObjectId,
    /// Git object kind.
    pub kind: ObjectKind,
    /// Offset of the object header inside the pack.
    pub offset: u64,
    /// Uncompressed object payload size.
    pub size: u64,
    /// Compressed object stream size, or a conservative complete entry span for a retained native
    /// source pack. Callers may safely use it as a range-read lower bound.
    pub compressed_size: u64,
}

/// Pack bytes with metadata and object index rows.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoredPack {
    /// Pack metadata.
    pub metadata: PackMetadata,
    /// Complete raw PACK bytes.
    pub data: Vec<u8>,
    /// Object index rows in pack order.
    pub index: Vec<PackObjectIndex>,
}

/// Validated source pack retained through a shared immutable byte allocation during import.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ImportedPack {
    /// Pack metadata.
    pub metadata: PackMetadata,
    /// Complete raw PACK bytes shared with the source object database's pack cache.
    pub data: Arc<Vec<u8>>,
    /// Object index rows in pack order.
    pub index: Vec<PackObjectIndex>,
}

/// Packed object resolved through a pack index row.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PackedObject {
    /// Pack metadata that owns this object representation.
    pub pack: PackMetadata,
    /// Index row that located the object.
    pub index: PackObjectIndex,
    /// Decoded object payload.
    pub object: StoredObject,
}

/// Planned repository repack and garbage-collection inputs.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RepackPlan {
    /// Object ids reachable from refs and retained by a repack.
    pub reachable_objects: Vec<ObjectId>,
    /// Stored object ids not reachable from refs and eligible for pruning.
    pub unreachable_objects: Vec<ObjectId>,
    /// Loose object ids that should be included in the next pack.
    pub loose_objects: Vec<ObjectId>,
    /// Existing packs that contain reachable objects and should be rewritten.
    pub packs_to_rewrite: Vec<PackMetadata>,
    /// Existing packs whose indexed objects are all unreachable.
    pub packs_to_delete: Vec<PackMetadata>,
}

impl StoredObject {
    /// Create a stored object from its kind and raw data.
    #[must_use]
    pub fn new(kind: ObjectKind, data: impl Into<Vec<u8>>) -> Self {
        Self {
            kind,
            data: data.into(),
        }
    }

    /// Compute the Git object id for this payload with `algo`.
    #[must_use]
    pub fn object_id(&self, algo: HashAlgo) -> ObjectId {
        let mut store_bytes = Vec::new();
        store_bytes.extend_from_slice(object_kind_name(self.kind).as_bytes());
        store_bytes.push(b' ');
        store_bytes.extend_from_slice(self.data.len().to_string().as_bytes());
        store_bytes.push(0);
        store_bytes.extend_from_slice(&self.data);

        match algo {
            HashAlgo::Sha1 => {
                let mut hasher = Sha1::new();
                hasher.update(&store_bytes);
                ObjectId::from_bytes(&hasher.finalize()).unwrap_or_else(|_| ObjectId::zero())
            }
            HashAlgo::Sha256 => {
                let mut hasher = Sha256::new();
                hasher.update(&store_bytes);
                ObjectId::from_bytes(&hasher.finalize())
                    .unwrap_or_else(|_| ObjectId::null(HashAlgo::Sha256))
            }
        }
    }
}

/// Stored direct or symbolic reference.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StoredRef {
    /// A direct ref pointing at an object id.
    Direct(ObjectId),
    /// A symbolic ref pointing at another ref name.
    Symbolic(String),
}

/// One reflog entry for a ref update.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReflogEntry {
    /// Ref name whose log received the entry.
    pub refname: String,
    /// Previous object id.
    pub old_oid: ObjectId,
    /// New object id.
    pub new_oid: ObjectId,
    /// Identity line used for the actor.
    pub actor: String,
    /// Entry timestamp supplied by the caller.
    pub timestamp: OffsetDateTime,
    /// Reflog message.
    pub message: String,
}

/// Object database operations for one hosted repository.
#[async_trait]
pub trait ObjectStore: Send + Sync {
    /// Read an object by id.
    async fn read_object(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        oid: &ObjectId,
    ) -> Result<Option<StoredObject>>;

    /// Write an object under its computed id.
    async fn write_object(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        oid: &ObjectId,
        object: &StoredObject,
    ) -> Result<()>;

    /// Write an object whose commit metadata will be indexed separately by an importer.
    ///
    /// Backends that derive commit graph rows in [`Self::write_object`] should override this
    /// method to store only the object. Backends with independent object and commit stores can use
    /// the default behavior.
    ///
    /// # Errors
    ///
    /// Returns backend errors encountered while storing the object.
    async fn write_imported_object(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        oid: &ObjectId,
        object: &StoredObject,
    ) -> Result<()> {
        self.write_object(tenant, repository, oid, object).await
    }

    /// Write a batch of owned objects whose commit metadata will be indexed separately.
    ///
    /// `objects` contains object ids paired with their header-stripped payloads. Ownership lets
    /// backends move payload bytes directly into storage rather than clone them. The default
    /// implementation safely delegates each value to [`Self::write_imported_object`]; backends
    /// should override this method when they can amortize locks, transactions, or remote calls.
    ///
    /// # Parameters
    ///
    /// - `tenant`: Tenant that owns the repository.
    /// - `repository`: Repository that receives the objects.
    /// - `objects`: Owned object ids and payloads to store.
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` after the complete batch has been stored.
    ///
    /// # Errors
    ///
    /// Returns backend errors encountered while storing any object in the batch. Backends whose
    /// storage supports transactions should make the batch atomic.
    async fn write_imported_objects(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        objects: Vec<(ObjectId, StoredObject)>,
    ) -> Result<()> {
        for (oid, object) in objects {
            self.write_imported_object(tenant, repository, &oid, &object)
                .await?;
        }
        Ok(())
    }

    /// Return whether an object exists.
    async fn object_exists(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        oid: &ObjectId,
    ) -> Result<bool>;

    /// Count objects stored for a repository.
    async fn count_objects(&self, tenant: &TenantId, repository: &RepositoryId) -> Result<usize>;

    /// List object ids, optionally restricted by Git object kind.
    async fn list_object_ids(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        kind: Option<ObjectKind>,
    ) -> Result<Vec<(ObjectId, ObjectKind)>>;
}

/// Reference storage operations for one hosted repository.
#[async_trait]
pub trait RefStore: Send + Sync {
    /// Read a direct or symbolic ref.
    async fn read_ref(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        refname: &str,
    ) -> Result<Option<StoredRef>>;

    /// Write a direct or symbolic ref.
    ///
    /// `expected` is a compare-and-swap guard. `Some(None)` means the ref must not exist,
    /// `Some(Some(value))` means the current value must equal `value`, and `None` disables the
    /// guard.
    async fn write_ref(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        refname: &str,
        value: &StoredRef,
        expected: Option<Option<StoredRef>>,
    ) -> Result<()>;

    /// Delete a ref.
    async fn delete_ref(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        refname: &str,
        expected: Option<StoredRef>,
    ) -> Result<()>;

    /// List refs matching `prefix`.
    async fn list_refs(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        prefix: &str,
    ) -> Result<Vec<(String, StoredRef)>>;
}

/// Reflog storage operations for one hosted repository.
#[async_trait]
pub trait ReflogStore: Send + Sync {
    /// Append a reflog entry.
    async fn append_reflog(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        entry: &ReflogEntry,
    ) -> Result<()>;

    /// Read all reflog entries for `refname` in storage order.
    async fn read_reflog(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        refname: &str,
    ) -> Result<Vec<ReflogEntry>>;
}

/// Git config storage operations for one hosted repository.
#[async_trait]
pub trait ConfigStore: Send + Sync {
    /// Get a config key.
    async fn get_config(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        key: &str,
    ) -> Result<Option<String>>;

    /// Set or replace a config key.
    async fn set_config(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        key: &str,
        value: &str,
    ) -> Result<()>;

    /// List config entries whose keys start with `prefix`.
    async fn list_config(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        prefix: &str,
    ) -> Result<Vec<(String, String)>>;
}

/// Direct child entry indexed for hosting UI path browsing.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IndexedTreeEntry {
    /// Tree object id that owns this entry.
    pub tree_oid: ObjectId,
    /// Direct child name relative to the owning tree.
    pub path: String,
    /// Entry mode.
    pub mode: u32,
    /// Entry object id.
    pub oid: ObjectId,
    /// Entry kind.
    pub kind: ObjectKind,
    /// Blob size when known.
    pub size: Option<u64>,
}

/// Commit metadata indexed for graph traversal and history queries.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IndexedCommit {
    /// Commit object id.
    pub oid: ObjectId,
    /// Root tree object id named by the commit.
    pub tree: ObjectId,
    /// Parent commit ids in commit object order.
    pub parents: Vec<ObjectId>,
    /// Committer timestamp as seconds since the Unix epoch.
    pub commit_time: i64,
    /// Topological generation, with root commits starting at one.
    pub generation: u32,
}

/// Repository import session created before importer mutations begin.
pub struct ImportSession {
    /// Monotonic repository-local generation used to reject stale concurrent completions.
    generation: u64,
    /// Backend-global non-replay token that survives repository deletion and recreation.
    token: u64,
    /// Tenant identity bound to this session.
    tenant: TenantId,
    /// Repository identity bound to this session.
    repository: RepositoryId,
    /// Object ids trusted from the immediately preceding completed import.
    trusted_objects: Vec<(ObjectId, ObjectKind)>,
}

impl ImportSession {
    /// Create a backend-issued import session.
    pub(crate) fn new(
        tenant: TenantId,
        repository: RepositoryId,
        generation: u64,
        token: u64,
        trusted_objects: Vec<(ObjectId, ObjectKind)>,
    ) -> Self {
        Self {
            generation,
            token,
            tenant,
            repository,
            trusted_objects,
        }
    }

    /// Return this session's repository-local generation.
    #[must_use]
    pub(crate) fn generation(&self) -> u64 {
        self.generation
    }

    /// Return this session's backend-issued non-replay token.
    #[must_use]
    pub(crate) fn token(&self) -> u64 {
        self.token
    }

    /// Return the tenant bound to this session.
    #[must_use]
    pub(crate) fn tenant(&self) -> &TenantId {
        &self.tenant
    }

    /// Return the repository bound to this session.
    #[must_use]
    pub(crate) fn repository(&self) -> &RepositoryId {
        &self.repository
    }

    /// Return object ids trusted by the preceding completed import.
    #[must_use]
    pub(crate) fn trusted_objects(&self) -> &[(ObjectId, ObjectKind)] {
        &self.trusted_objects
    }
}

impl std::fmt::Debug for ImportSession {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ImportSession")
            .finish_non_exhaustive()
    }
}

/// Final visible repository mutations staged by an import.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ImportPublication {
    /// Repository-local config keys and values to publish.
    pub config_entries: Vec<(String, String)>,
    /// Complete desired ref snapshot, including `HEAD` when present.
    pub refs: Vec<(String, StoredRef)>,
    /// Whether refs absent from `refs` should be deleted.
    pub prune_deleted_refs: bool,
    /// Objects read and verified from the source during this import session.
    pub newly_trusted: Vec<(ObjectId, ObjectKind)>,
}

/// Result of guarded import publication.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ImportPublicationResult {
    /// Ref names deleted because they were absent from the desired snapshot.
    pub pruned_refs: Vec<String>,
}

/// Durable completion-state operations for incremental repository imports.
#[async_trait]
pub trait ImportStateStore: Send + Sync {
    /// Begin an import and atomically mark its repository incomplete.
    ///
    /// Trusted objects are returned only when the preceding import completed. If a prior import
    /// stopped early, the returned snapshot is empty so retries traverse and verify the source
    /// closure again. Starting another concurrent import advances the generation and makes older
    /// sessions ineligible to complete.
    ///
    /// # Returns
    ///
    /// Returns the new generation and any object ids trusted from the previous completed import.
    ///
    /// # Errors
    ///
    /// Returns backend errors if the incomplete marker cannot be stored atomically.
    async fn begin_import(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
    ) -> Result<ImportSession>;

    /// Guard and atomically publish the import's visible repository mutations.
    ///
    /// Publication must verify that `session` is the active incomplete generation bound to
    /// `tenant` and `repository`. A successful call records newly trusted objects but deliberately
    /// leaves the session incomplete until [`Self::complete_import`] is called after progress
    /// callbacks accept the durable publication.
    ///
    /// # Errors
    ///
    /// Returns [`crate::error::Error::StaleImportSession`] before applying any visible mutation
    /// when the session is stale, replayed, cross-repository, or already published.
    async fn publish_import(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        session: &ImportSession,
        publication: &ImportPublication,
    ) -> Result<ImportPublicationResult>;

    /// Mark a successfully published import complete and enable its trusted manifest.
    ///
    /// # Errors
    ///
    /// Returns [`crate::error::Error::StaleImportSession`] when `session` is not the active,
    /// published, incomplete generation for this exact tenant and repository. A session can be
    /// completed only once.
    async fn complete_import(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        session: &ImportSession,
    ) -> Result<()>;
}

/// Query index operations for repository-browsing UI.
#[async_trait]
pub trait BrowseIndex: Send + Sync {
    /// Upsert direct child entries for their owning trees.
    async fn upsert_tree_entries(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        entries: &[IndexedTreeEntry],
    ) -> Result<()>;

    /// Replace all direct-tree browse-index entries for a repository with `entries`.
    ///
    /// Backends that cannot perform a repository-wide replacement atomically may fall back to an
    /// idempotent upsert, but repair-capable backends should remove stale rows before inserting
    /// the supplied snapshot.
    async fn replace_tree_entries(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        entries: &[IndexedTreeEntry],
    ) -> Result<()> {
        self.upsert_tree_entries(tenant, repository, entries).await
    }

    /// List direct child entries of `tree_oid` whose names start with `prefix`.
    async fn list_tree_entries(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        tree_oid: &ObjectId,
        prefix: &str,
    ) -> Result<Vec<IndexedTreeEntry>>;

    /// Read blob contents for a direct child name resolved through the browse index.
    ///
    /// Repository-level callers should use [`crate::repository::ServerRepository`] for paths with
    /// multiple components; this low-level operation addresses exactly one owning tree.
    async fn read_blob_at_path(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        tree_oid: &ObjectId,
        path: &str,
    ) -> Result<Option<StoredObject>>;
}

/// Commit graph index operations for history and reachability queries.
#[async_trait]
pub trait CommitGraphStore: Send + Sync {
    /// Upsert commit graph rows.
    async fn upsert_commits(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        commits: &[IndexedCommit],
    ) -> Result<()>;

    /// Replace the commit graph for a repository with `commits`.
    async fn replace_commit_graph(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        commits: &[IndexedCommit],
    ) -> Result<()> {
        self.upsert_commits(tenant, repository, commits).await
    }

    /// Read one indexed commit.
    async fn read_indexed_commit(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        oid: &ObjectId,
    ) -> Result<Option<IndexedCommit>>;

    /// Return parent commit ids for `oid` in commit object order.
    async fn commit_parents(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        oid: &ObjectId,
    ) -> Result<Vec<ObjectId>>;

    /// Return commit ids that name `oid` as a parent.
    async fn commit_children(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        oid: &ObjectId,
    ) -> Result<Vec<ObjectId>>;

    /// List all indexed commits for a repository.
    async fn list_indexed_commits(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
    ) -> Result<Vec<IndexedCommit>>;
}

/// Packfile storage operations for one hosted repository.
#[async_trait]
pub trait PackStore: Send + Sync {
    /// Return whether this backend can ingest and resolve delta-capable source packs directly.
    ///
    /// The default is `false`; importers must retain their loose-object traversal path unless a
    /// backend explicitly opts in.
    fn supports_native_pack_import(&self) -> bool {
        false
    }

    /// Atomically install an owned batch of source packs during repository import.
    ///
    /// This method is called only when [`Self::supports_native_pack_import`] returns `true` and
    /// every pack has been validated as self-contained and compatible with the repository hash
    /// algorithm. The shared byte allocation allows capable backends to retain the source ODB
    /// cache entry without another full copy. The default rejects the operation so unsupported
    /// backends continue through the importer's loose-object fallback.
    ///
    /// # Errors
    ///
    /// Returns a backend or protocol error if the complete batch cannot be installed.
    async fn write_imported_packs(
        &self,
        _tenant: &TenantId,
        _repository: &RepositoryId,
        _packs: Vec<ImportedPack>,
    ) -> Result<Vec<PackMetadata>> {
        Err(crate::error::Error::Backend(
            "native pack import is not supported by this backend".to_owned(),
        ))
    }

    /// Store a packfile and its object index rows.
    async fn write_pack(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        pack: &StoredPack,
    ) -> Result<PackMetadata>;

    /// Read pack metadata by trailing checksum.
    async fn read_pack_metadata(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        pack_checksum: &[u8],
    ) -> Result<Option<PackMetadata>>;

    /// Read complete raw pack bytes by trailing checksum.
    async fn read_pack_data(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        pack_checksum: &[u8],
    ) -> Result<Option<Vec<u8>>>;

    /// Read a byte range from a raw packfile.
    ///
    /// Backends with native ranged reads should override this method. The default implementation
    /// reads the complete pack and slices the requested range locally.
    async fn read_pack_range(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        pack_checksum: &[u8],
        start: u64,
        len: u64,
    ) -> Result<Option<Vec<u8>>> {
        let Some(data) = self
            .read_pack_data(tenant, repository, pack_checksum)
            .await?
        else {
            return Ok(None);
        };
        let start = usize::try_from(start).map_err(|_| {
            crate::error::Error::Backend("pack range start exceeds usize".to_owned())
        })?;
        let len = usize::try_from(len).map_err(|_| {
            crate::error::Error::Backend("pack range length exceeds usize".to_owned())
        })?;
        let end = start
            .checked_add(len)
            .ok_or_else(|| crate::error::Error::Backend("pack range overflow".to_owned()))?;
        if start >= data.len() {
            return Ok(Some(Vec::new()));
        }
        Ok(Some(data[start..end.min(data.len())].to_vec()))
    }

    /// Decode one object from a stored pack using backend-native shared data and indexes.
    ///
    /// Returning `None` asks the repository layer to use its portable range/full-pack decoder.
    /// Backends that opt into native source-pack import should override this so delta-capable
    /// reads do not copy the complete pack.
    async fn read_packed_object_data(
        &self,
        _tenant: &TenantId,
        _repository: &RepositoryId,
        _pack_checksum: &[u8],
        _oid: &ObjectId,
    ) -> Result<Option<StoredObject>> {
        Ok(None)
    }

    /// Return the newest packed representation for an object id.
    async fn find_packed_object(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        oid: &ObjectId,
    ) -> Result<Option<(PackMetadata, PackObjectIndex)>>;

    /// Read an object index row by pack checksum and pack offset.
    async fn read_pack_index_at_offset(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        pack_checksum: &[u8],
        offset: u64,
    ) -> Result<Option<PackObjectIndex>>;

    /// List pack metadata rows for a repository.
    async fn list_packs(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
    ) -> Result<Vec<PackMetadata>>;

    /// List packed object index rows, optionally restricted to one pack.
    async fn list_pack_objects(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        pack_checksum: Option<&[u8]>,
    ) -> Result<Vec<(PackMetadata, PackObjectIndex)>>;
}

/// Combined storage contract for a full server repository backend.
pub trait ServerStorage:
    ObjectStore
    + RefStore
    + ReflogStore
    + ConfigStore
    + BrowseIndex
    + CommitGraphStore
    + ImportStateStore
    + PackStore
    + Send
    + Sync
{
}

impl<T> ServerStorage for T where
    T: ObjectStore
        + RefStore
        + ReflogStore
        + ConfigStore
        + BrowseIndex
        + CommitGraphStore
        + ImportStateStore
        + PackStore
        + Send
        + Sync
{
}

pub(crate) fn commit_time_from_identity(identity: &str) -> i64 {
    identity
        .split_whitespace()
        .rev()
        .nth(1)
        .and_then(|timestamp| timestamp.parse().ok())
        .unwrap_or_default()
}
