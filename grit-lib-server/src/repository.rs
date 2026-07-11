//! Repository handle for server-backed storage.

use std::sync::Arc;

use grit_lib::objects::{parse_commit, parse_tag, HashAlgo, ObjectId, ObjectKind};

use crate::cache::{CacheKey, EventPublisher, InvalidationEvent};
use crate::error::{Error, Result};
use crate::ids::{RepositoryId, TenantId};
use crate::storage::{IndexedTreeEntry, ServerStorage, StoredObject, StoredRef};
use crate::views::{
    BlobView, BranchView, CommitSummary, CompareInputs, DiscoveredFile, RepositorySummary, TagView,
    TreeEntryView, TreeView,
};

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

    /// Return the branch named by symbolic `HEAD`.
    ///
    /// # Errors
    ///
    /// Returns backend errors, object parsing errors, or an error if `HEAD` resolves through a
    /// symbolic cycle.
    pub async fn default_branch(&self) -> Result<Option<BranchView>> {
        let Some(StoredRef::Symbolic(refname)) = self.read_ref("HEAD").await? else {
            return Ok(None);
        };
        if !refname.starts_with("refs/heads/") {
            return Ok(None);
        }
        self.branch_view(refname).await
    }

    /// Return repository metadata for landing pages.
    ///
    /// # Errors
    ///
    /// Returns backend errors, object parsing errors, or an error if `HEAD` resolves through a
    /// symbolic cycle.
    pub async fn summary(&self) -> Result<RepositorySummary> {
        let refs_count = self.list_refs("").await?.len();
        let object_count = self
            .storage
            .count_objects(&self.tenant, &self.repository)
            .await?;
        let default_branch = self.default_branch().await?;
        let latest_commit = match self.resolve_ref("HEAD").await? {
            Some(oid) => self.peel_to_commit(&oid, 0).await?,
            None => None,
        };
        Ok(RepositorySummary {
            default_branch,
            refs_count,
            object_count,
            latest_commit,
        })
    }

    /// List local branches with commit metadata when available.
    ///
    /// # Errors
    ///
    /// Returns backend errors, object parsing errors, or an error if a branch resolves through a
    /// symbolic cycle.
    pub async fn branches(&self) -> Result<Vec<BranchView>> {
        let mut branches = Vec::new();
        for (refname, _) in self.list_refs("refs/heads/").await? {
            if let Some(branch) = self.branch_view(refname).await? {
                branches.push(branch);
            }
        }
        Ok(branches)
    }

    /// List tags with peeled commit metadata when available.
    ///
    /// # Errors
    ///
    /// Returns backend errors, object parsing errors, or an error if a symbolic tag ref resolves
    /// through a cycle.
    pub async fn tags(&self) -> Result<Vec<TagView>> {
        let mut tags = Vec::new();
        for (refname, value) in self.list_refs("refs/tags/").await? {
            let Some(oid) = self.resolve_stored_ref(&refname, value).await? else {
                continue;
            };
            let Some(object) = self.read_object(&oid).await? else {
                continue;
            };
            let (target, target_kind, tagger, message) = if object.kind == ObjectKind::Tag {
                let tag = parse_tag(&object.data)?;
                let target_kind = ObjectKind::from_tag_type_field(tag.object_type.as_bytes())
                    .ok_or_else(|| {
                        Error::Backend(format!("unknown tag object type '{}'", tag.object_type))
                    })?;
                (tag.object, target_kind, tag.tagger, Some(tag.message))
            } else {
                (oid, object.kind, None, None)
            };
            tags.push(TagView {
                name: strip_ref_prefix(&refname, "refs/tags/"),
                refname,
                oid,
                target,
                target_kind,
                peeled_commit: self.peel_to_commit(&oid, 0).await?,
                tagger,
                message,
            });
        }
        Ok(tags)
    }

    /// Return commit metadata for a full object id, exact ref name, `HEAD`, or short branch/tag
    /// name.
    ///
    /// # Errors
    ///
    /// Returns [`Error::ObjectNotFound`] for missing object ids, [`Error::RefNotFound`] for missing
    /// refs, [`Error::UnexpectedObjectKind`] for non-commit inputs, or backend/object parsing
    /// errors.
    pub async fn commit(&self, object_or_ref: &str) -> Result<CommitSummary> {
        let oid = self.resolve_object_or_ref(object_or_ref).await?;
        self.peel_to_commit(&oid, 0)
            .await?
            .ok_or_else(|| Error::ObjectNotFound(oid.to_hex()))
    }

    /// Return direct tree entries for `revision` at `path`.
    ///
    /// `revision` may be a commit, annotated tag, tree object id, exact ref name, `HEAD`, or short
    /// branch/tag name. `path` is interpreted relative to the revision root tree; an empty path
    /// browses the root.
    ///
    /// # Errors
    ///
    /// Returns typed not-found and wrong-kind errors for missing paths or blob paths, plus backend
    /// and object parsing errors.
    pub async fn tree_at(&self, revision: &str, path: &str) -> Result<TreeView> {
        let root = self.resolve_tree_root(revision).await?;
        let path = normalize_tree_path(path);
        let (oid, prefix) = if path.is_empty() {
            (root, String::new())
        } else {
            let entry = self
                .tree_entry(&root, &path)
                .await?
                .ok_or_else(|| Error::PathNotFound(path.clone()))?;
            if entry.kind != ObjectKind::Tree {
                return Err(Error::UnexpectedObjectKind {
                    expected: "tree",
                    actual: object_kind_name(entry.kind),
                });
            }
            (entry.oid, format!("{path}/"))
        };
        let entries = self
            .storage
            .list_tree_entries(&self.tenant, &self.repository, &root, &prefix)
            .await?
            .into_iter()
            .filter(|entry| is_direct_child(&entry.path, &path))
            .map(TreeEntryView::from)
            .collect();
        Ok(TreeView {
            root,
            path,
            oid,
            entries,
        })
    }

    /// Return blob contents and metadata for `revision` at `path`.
    ///
    /// `revision` may be a commit, annotated tag, tree object id, exact ref name, `HEAD`, or short
    /// branch/tag name. `path` is interpreted relative to the revision root tree.
    ///
    /// # Errors
    ///
    /// Returns [`Error::PathNotFound`] for missing paths, [`Error::UnexpectedObjectKind`] when the
    /// path names a tree or non-blob object, plus backend and object parsing errors.
    pub async fn blob_at(&self, revision: &str, path: &str) -> Result<BlobView> {
        let root = self.resolve_tree_root(revision).await?;
        let path = normalize_tree_path(path);
        let entry = self
            .tree_entry(&root, &path)
            .await?
            .ok_or_else(|| Error::PathNotFound(path.clone()))?;
        if entry.kind != ObjectKind::Blob {
            return Err(Error::UnexpectedObjectKind {
                expected: "blob",
                actual: object_kind_name(entry.kind),
            });
        }
        let object = self
            .storage
            .read_blob_at_path(&self.tenant, &self.repository, &root, &path)
            .await?
            .ok_or_else(|| Error::PathNotFound(path.clone()))?;
        if object.kind != ObjectKind::Blob {
            return Err(Error::UnexpectedObjectKind {
                expected: "blob",
                actual: object_kind_name(object.kind),
            });
        }
        Ok(BlobView {
            oid: entry.oid,
            path,
            mode: entry.mode,
            data: object.data,
        })
    }

    /// Discover conventional files in a revision.
    ///
    /// Each candidate is interpreted as a repository-relative path. Missing candidates are skipped
    /// and returned candidates preserve the input order.
    ///
    /// # Errors
    ///
    /// Returns revision resolution, backend, and object parsing errors. Missing candidate paths do
    /// not fail discovery.
    pub async fn discover_files(
        &self,
        revision: &str,
        candidates: &[impl AsRef<str>],
    ) -> Result<Vec<DiscoveredFile>> {
        let mut discovered = Vec::new();
        for candidate in candidates {
            let candidate = candidate.as_ref();
            match self.blob_at(revision, candidate).await {
                Ok(blob) => discovered.push(DiscoveredFile {
                    candidate: candidate.to_owned(),
                    blob,
                }),
                Err(Error::PathNotFound(_)) | Err(Error::UnexpectedObjectKind { .. }) => {}
                Err(err) => return Err(err),
            }
        }
        Ok(discovered)
    }

    /// Resolve compare endpoints to commit summaries and root trees.
    ///
    /// # Errors
    ///
    /// Returns revision resolution, backend, and object parsing errors.
    pub async fn compare_inputs(&self, base: &str, head: &str) -> Result<CompareInputs> {
        let base = self.commit(base).await?;
        let head = self.commit(head).await?;
        Ok(CompareInputs {
            base_tree: base.tree,
            head_tree: head.tree,
            base,
            head,
        })
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

    async fn branch_view(&self, refname: String) -> Result<Option<BranchView>> {
        let Some(target) = self.resolve_ref(&refname).await? else {
            return Ok(None);
        };
        Ok(Some(BranchView {
            name: strip_ref_prefix(&refname, "refs/heads/"),
            refname,
            target,
            commit: self.peel_to_commit(&target, 0).await?,
        }))
    }

    async fn resolve_stored_ref(
        &self,
        refname: &str,
        value: StoredRef,
    ) -> Result<Option<ObjectId>> {
        match value {
            StoredRef::Direct(oid) => Ok(Some(oid)),
            StoredRef::Symbolic(_) => self.resolve_ref(refname).await,
        }
    }

    async fn resolve_object_or_ref(&self, object_or_ref: &str) -> Result<ObjectId> {
        if ObjectId::is_full_hex(object_or_ref) {
            return ObjectId::from_hex(object_or_ref).map_err(Into::into);
        }
        for candidate in ref_candidates(object_or_ref) {
            if let Some(oid) = self.resolve_ref(&candidate).await? {
                return Ok(oid);
            }
        }
        Err(Error::RefNotFound(object_or_ref.to_owned()))
    }

    async fn peel_to_commit(
        &self,
        oid: &ObjectId,
        starting_depth: usize,
    ) -> Result<Option<CommitSummary>> {
        let mut current = *oid;
        for depth in starting_depth..=10 {
            let Some(object) = self.read_object(&current).await? else {
                return Ok(None);
            };
            match object.kind {
                ObjectKind::Commit => return self.commit_summary(&current).await,
                ObjectKind::Tag => {
                    if depth == 10 {
                        return Err(Error::Backend(format!(
                            "tag cycle while peeling '{}'",
                            oid.to_hex()
                        )));
                    }
                    let tag = parse_tag(&object.data)?;
                    current = tag.object;
                }
                _ => {
                    return Err(Error::UnexpectedObjectKind {
                        expected: "commit",
                        actual: object_kind_name(object.kind),
                    });
                }
            }
        }
        Err(Error::Backend(format!(
            "tag cycle while peeling '{}'",
            oid.to_hex()
        )))
    }

    async fn resolve_tree_root(&self, revision: &str) -> Result<ObjectId> {
        let oid = self.resolve_object_or_ref(revision).await?;
        self.tree_root_for_object(&oid, 0).await
    }

    async fn tree_root_for_object(
        &self,
        oid: &ObjectId,
        starting_depth: usize,
    ) -> Result<ObjectId> {
        let mut current = *oid;
        for depth in starting_depth..=10 {
            let object = self
                .read_object(&current)
                .await?
                .ok_or_else(|| Error::ObjectNotFound(current.to_hex()))?;
            match object.kind {
                ObjectKind::Commit => {
                    let commit = parse_commit(&object.data)?;
                    return Ok(commit.tree);
                }
                ObjectKind::Tree => return Ok(current),
                ObjectKind::Tag => {
                    if depth == 10 {
                        return Err(Error::Backend(format!(
                            "tag cycle while resolving tree for '{}'",
                            oid.to_hex()
                        )));
                    }
                    let tag = parse_tag(&object.data)?;
                    current = tag.object;
                }
                ObjectKind::Blob => {
                    return Err(Error::UnexpectedObjectKind {
                        expected: "commit or tree",
                        actual: "blob",
                    });
                }
            }
        }
        Err(Error::Backend(format!(
            "tag cycle while resolving tree for '{}'",
            oid.to_hex()
        )))
    }

    async fn tree_entry(&self, root: &ObjectId, path: &str) -> Result<Option<IndexedTreeEntry>> {
        Ok(self
            .storage
            .list_tree_entries(&self.tenant, &self.repository, root, path)
            .await?
            .into_iter()
            .find(|entry| entry.path == path))
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

fn ref_candidates(input: &str) -> Vec<String> {
    if input == "HEAD" || input.starts_with("refs/") {
        vec![input.to_owned()]
    } else {
        vec![
            input.to_owned(),
            format!("refs/heads/{input}"),
            format!("refs/tags/{input}"),
        ]
    }
}

fn strip_ref_prefix(refname: &str, prefix: &str) -> String {
    refname
        .strip_prefix(prefix)
        .map_or_else(|| refname.to_owned(), ToOwned::to_owned)
}

fn normalize_tree_path(path: &str) -> String {
    match path {
        "" | "." => String::new(),
        other => other.trim_matches('/').to_owned(),
    }
}

fn is_direct_child(candidate: &str, parent: &str) -> bool {
    if parent.is_empty() {
        return !candidate.is_empty() && !candidate.contains('/');
    }
    let Some(rest) = candidate.strip_prefix(parent) else {
        return false;
    };
    let Some(rest) = rest.strip_prefix('/') else {
        return false;
    };
    !rest.is_empty() && !rest.contains('/')
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
