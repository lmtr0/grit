//! Repository handle for server-backed storage.

use std::sync::Arc;

use grit_lib::objects::{parse_commit, HashAlgo, ObjectId, ObjectKind};

use crate::cache::{CacheKey, EventPublisher, InvalidationEvent};
use crate::error::{Error, Result};
use crate::ids::{RepositoryId, TenantId};
use crate::storage::{IndexedTreeEntry, ServerStorage, StoredObject, StoredRef};
use crate::views::{BlobView, CommitSummary, TreeEntryView};

/// Server-backed repository handle.
#[derive(Clone)]
pub struct ServerRepository<S> {
    tenant: TenantId,
    repository: RepositoryId,
    hash_algo: HashAlgo,
    storage: Arc<S>,
}

impl<S> ServerRepository<S>
where
    S: ServerStorage,
{
    /// Create a server-backed repository handle.
    #[must_use]
    pub fn new(
        tenant: TenantId,
        repository: RepositoryId,
        hash_algo: HashAlgo,
        storage: Arc<S>,
    ) -> Self {
        Self {
            tenant,
            repository,
            hash_algo,
            storage,
        }
    }

    /// Tenant identifier for this repository.
    #[must_use]
    pub fn tenant(&self) -> &TenantId {
        &self.tenant
    }

    /// Repository identifier for this repository.
    #[must_use]
    pub fn repository(&self) -> &RepositoryId {
        &self.repository
    }

    /// Hash algorithm used by this repository.
    #[must_use]
    pub fn hash_algo(&self) -> HashAlgo {
        self.hash_algo
    }

    /// Shared storage backend.
    #[must_use]
    pub fn storage(&self) -> &Arc<S> {
        &self.storage
    }

    /// Write an object and return its computed object id.
    ///
    /// # Errors
    ///
    /// Returns backend errors from the configured object store.
    pub async fn write_object(&self, object: &StoredObject) -> Result<ObjectId> {
        let oid = object.object_id(self.hash_algo);
        self.storage
            .write_object(&self.tenant, &self.repository, &oid, object)
            .await?;
        Ok(oid)
    }

    /// Read an object by id.
    ///
    /// # Errors
    ///
    /// Returns backend errors from the configured object store.
    pub async fn read_object(&self, oid: &ObjectId) -> Result<Option<StoredObject>> {
        self.storage
            .read_object(&self.tenant, &self.repository, oid)
            .await
    }

    /// Read a ref by name.
    ///
    /// # Errors
    ///
    /// Returns backend errors from the configured ref store.
    pub async fn read_ref(&self, refname: &str) -> Result<Option<StoredRef>> {
        self.storage
            .read_ref(&self.tenant, &self.repository, refname)
            .await
    }

    /// List refs matching `prefix`.
    ///
    /// # Errors
    ///
    /// Returns backend errors from the configured ref store.
    pub async fn list_refs(&self, prefix: &str) -> Result<Vec<(String, StoredRef)>> {
        self.storage
            .list_refs(&self.tenant, &self.repository, prefix)
            .await
    }

    /// Resolve a direct or symbolic ref to an object id.
    ///
    /// # Errors
    ///
    /// Returns backend errors from the configured ref store.
    pub async fn resolve_ref(&self, refname: &str) -> Result<Option<ObjectId>> {
        let mut name = refname.to_owned();
        for _ in 0..10 {
            match self.read_ref(&name).await? {
                Some(StoredRef::Direct(oid)) => return Ok(Some(oid)),
                Some(StoredRef::Symbolic(target)) => name = target,
                None => return Ok(None),
            }
        }
        Err(Error::Backend(format!(
            "symbolic ref cycle while resolving '{refname}'"
        )))
    }

    /// Read a commit and return hosting-oriented metadata.
    ///
    /// # Errors
    ///
    /// Returns backend errors or object parse errors.
    pub async fn commit_summary(&self, oid: &ObjectId) -> Result<Option<CommitSummary>> {
        let Some(object) = self.read_object(oid).await? else {
            return Ok(None);
        };
        if object.kind != ObjectKind::Commit {
            return Err(Error::UnexpectedObjectKind {
                expected: "commit",
                actual: object_kind_name(object.kind),
            });
        }
        let commit = parse_commit(&object.data)?;
        let subject = commit.message.lines().next().unwrap_or_default().to_owned();
        Ok(Some(CommitSummary {
            oid: *oid,
            tree: commit.tree,
            parents: commit.parents,
            author: commit.author,
            committer: commit.committer,
            subject,
            message: commit.message,
        }))
    }

    /// List indexed tree entries below `prefix`.
    ///
    /// # Errors
    ///
    /// Returns backend errors from the configured browse index.
    pub async fn list_tree(&self, tree_oid: &ObjectId, prefix: &str) -> Result<Vec<TreeEntryView>> {
        let entries = self
            .storage
            .list_tree_entries(&self.tenant, &self.repository, tree_oid, prefix)
            .await?;
        Ok(entries.into_iter().map(TreeEntryView::from).collect())
    }

    /// Read blob contents at an indexed tree path.
    ///
    /// # Errors
    ///
    /// Returns backend errors from the configured browse index and object store.
    pub async fn read_blob_at_path(
        &self,
        tree_oid: &ObjectId,
        path: &str,
    ) -> Result<Option<BlobView>> {
        let entries = self
            .storage
            .list_tree_entries(&self.tenant, &self.repository, tree_oid, path)
            .await?;
        let Some(entry) = entries.into_iter().find(|entry| entry.path == path) else {
            return Ok(None);
        };
        let Some(object) = self
            .storage
            .read_blob_at_path(&self.tenant, &self.repository, tree_oid, path)
            .await?
        else {
            return Ok(None);
        };
        if object.kind != ObjectKind::Blob {
            return Err(Error::UnexpectedObjectKind {
                expected: "blob",
                actual: object_kind_name(object.kind),
            });
        }
        Ok(Some(BlobView {
            oid: entry.oid,
            path: entry.path,
            mode: entry.mode,
            data: object.data,
        }))
    }

    /// Write a ref and publish invalidations through `publisher`.
    ///
    /// # Errors
    ///
    /// Returns backend or publisher errors.
    pub async fn write_ref_and_publish<P>(
        &self,
        refname: &str,
        value: &StoredRef,
        expected: Option<Option<StoredRef>>,
        publisher: &P,
    ) -> Result<()>
    where
        P: EventPublisher,
    {
        self.storage
            .write_ref(&self.tenant, &self.repository, refname, value, expected)
            .await?;
        publisher
            .publish_invalidation(InvalidationEvent {
                tenant: self.tenant.clone(),
                repository: self.repository.clone(),
                keys: vec![
                    CacheKey::Ref(refname.to_owned()),
                    CacheKey::RefList(ref_parent_prefix(refname)),
                ],
            })
            .await
    }
}

fn ref_parent_prefix(refname: &str) -> String {
    match refname.rsplit_once('/') {
        Some((parent, _)) => format!("{parent}/"),
        None => String::new(),
    }
}

impl From<IndexedTreeEntry> for TreeEntryView {
    fn from(entry: IndexedTreeEntry) -> Self {
        Self {
            path: entry.path,
            mode: entry.mode,
            oid: entry.oid,
            kind: entry.kind,
            size: entry.size,
        }
    }
}

fn object_kind_name(kind: ObjectKind) -> &'static str {
    match kind {
        ObjectKind::Blob => "blob",
        ObjectKind::Tree => "tree",
        ObjectKind::Commit => "commit",
        ObjectKind::Tag => "tag",
    }
}
