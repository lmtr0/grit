//! Backend traits for server-hosted Git repository data.

use async_trait::async_trait;
use grit_lib::objects::{HashAlgo, ObjectId, ObjectKind};
use sha1::{Digest as _, Sha1};
use sha2::Sha256;
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

    /// Return whether an object exists.
    async fn object_exists(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        oid: &ObjectId,
    ) -> Result<bool>;
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

/// Tree entry indexed for hosting UI path browsing.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IndexedTreeEntry {
    /// Tree object id that owns this entry.
    pub tree_oid: ObjectId,
    /// Entry path relative to the tree root.
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

/// Query index operations for repository-browsing UI.
#[async_trait]
pub trait BrowseIndex: Send + Sync {
    /// Upsert indexed entries for a tree.
    async fn upsert_tree_entries(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        entries: &[IndexedTreeEntry],
    ) -> Result<()>;

    /// List direct entries below `tree_oid` and `prefix`.
    async fn list_tree_entries(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        tree_oid: &ObjectId,
        prefix: &str,
    ) -> Result<Vec<IndexedTreeEntry>>;

    /// Read blob contents for a path resolved through the browse index.
    async fn read_blob_at_path(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        tree_oid: &ObjectId,
        path: &str,
    ) -> Result<Option<StoredObject>>;
}

/// Combined storage contract for a full server repository backend.
pub trait ServerStorage:
    ObjectStore + RefStore + ReflogStore + ConfigStore + BrowseIndex + Send + Sync
{
}

impl<T> ServerStorage for T where
    T: ObjectStore + RefStore + ReflogStore + ConfigStore + BrowseIndex + Send + Sync
{
}
