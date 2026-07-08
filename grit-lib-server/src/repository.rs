//! Repository handle for server-backed storage.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use grit_lib::objects::{parse_commit, parse_tag, HashAlgo, ObjectId, ObjectKind};

use crate::cache::{EventPublisher, InvalidationEvent, InvalidationEventKind};
use crate::error::{Error, Result};
use crate::ids::{RepositoryId, TenantId};
use crate::protocol::receive_pack::{
    PushPlan, PushPolicy, ReceivePackReport, ReceivePackRequest, ReceivePackService,
};
use crate::protocol::upload_pack::{
    FetchPackPlan, FetchPackResponse, RefAdvertisement, UploadPackRequest, UploadPackService,
};
use crate::storage::{
    commit_time_from_identity, IndexedCommit, IndexedTreeEntry, ServerStorage, StoredObject,
    StoredRef,
};
use crate::views::{
    BlobView, BranchView, CommitComparison, CommitHistoryOptions, CommitHistoryPage, CommitSummary,
    CompareInputs, DiscoveredFile, RepositorySummary, TagView, TreeEntryView, TreeView,
};

/// Server-backed repository handle.
pub struct ServerRepository<S> {
    tenant: TenantId,
    repository: RepositoryId,
    hash_algo: HashAlgo,
    storage: Arc<S>,
}

impl<S> Clone for ServerRepository<S> {
    fn clone(&self) -> Self {
        Self {
            tenant: self.tenant.clone(),
            repository: self.repository.clone(),
            hash_algo: self.hash_algo,
            storage: self.storage.clone(),
        }
    }
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

    /// Return a paginated commit history from a ref, commit id, or tag.
    ///
    /// `options.offset` skips matching commits, `options.limit` caps the returned page, and
    /// `options.path` limits results to commits that change the repository-relative path when the
    /// commit tree has been indexed for browsing.
    ///
    /// # Errors
    ///
    /// Returns revision resolution, backend, graph index, path index, or object parsing errors.
    pub async fn commit_history(
        &self,
        start: &str,
        options: CommitHistoryOptions,
    ) -> Result<CommitHistoryPage> {
        let start = self.commit(start).await?;
        let path = options.path.as_deref().map(normalize_tree_path);
        let walked = self.walk_commit_oids(&[start.oid]).await?;
        let mut commits = Vec::new();
        for oid in walked {
            let summary = self
                .commit_summary(&oid)
                .await?
                .ok_or_else(|| Error::ObjectNotFound(oid.to_hex()))?;
            if self.history_path_matches(&summary, path.as_deref()).await? {
                commits.push(summary);
            }
        }

        let total_estimate = commits.len();
        let end = options.limit.map_or(total_estimate, |limit| {
            options.offset.saturating_add(limit).min(total_estimate)
        });
        let page = commits
            .into_iter()
            .skip(options.offset)
            .take(end.saturating_sub(options.offset))
            .collect::<Vec<_>>();
        let next_offset = (end < total_estimate).then_some(end);
        Ok(CommitHistoryPage {
            commits: page,
            next_offset,
            total_estimate,
        })
    }

    /// Return parent commits for a ref, commit id, or tag.
    ///
    /// # Errors
    ///
    /// Returns revision resolution, backend, graph index, or object parsing errors.
    pub async fn commit_parents(&self, commit: &str) -> Result<Vec<CommitSummary>> {
        let commit = self.commit(commit).await?;
        let parents = self.parent_ids(&commit.oid).await?;
        self.commit_summaries(parents).await
    }

    /// Return child commits for a ref, commit id, or tag.
    ///
    /// # Errors
    ///
    /// Returns revision resolution, backend, graph index, or object parsing errors.
    pub async fn commit_children(&self, commit: &str) -> Result<Vec<CommitSummary>> {
        let commit = self.commit(commit).await?;
        let children = self
            .storage
            .commit_children(&self.tenant, &self.repository, &commit.oid)
            .await?;
        self.commit_summaries(children).await
    }

    /// Return whether `ancestor` is reachable from `descendant` by following parents.
    ///
    /// # Errors
    ///
    /// Returns revision resolution, backend, graph index, or object parsing errors.
    pub async fn is_ancestor(&self, ancestor: &str, descendant: &str) -> Result<bool> {
        let ancestor = self.commit(ancestor).await?;
        let descendant = self.commit(descendant).await?;
        self.is_ancestor_oid(ancestor.oid, descendant.oid).await
    }

    /// Return a best merge base for two refs, commit ids, or tags.
    ///
    /// # Errors
    ///
    /// Returns revision resolution, backend, graph index, or object parsing errors.
    pub async fn merge_base(&self, left: &str, right: &str) -> Result<Option<CommitSummary>> {
        let left = self.commit(left).await?;
        let right = self.commit(right).await?;
        match self.merge_base_oid(left.oid, right.oid).await? {
            Some(oid) => self.commit_summary(&oid).await,
            None => Ok(None),
        }
    }

    /// Return commits reachable from the supplied refs, commit ids, or tags.
    ///
    /// # Errors
    ///
    /// Returns revision resolution, backend, graph index, or object parsing errors.
    pub async fn reachable_from(&self, refs: &[impl AsRef<str>]) -> Result<Vec<CommitSummary>> {
        let mut starts = Vec::with_capacity(refs.len());
        for refname in refs {
            starts.push(self.commit(refname.as_ref()).await?.oid);
        }
        self.commit_summaries(self.walk_commit_oids(&starts).await?)
            .await
    }

    /// Estimate the number of commits reachable from `start`.
    ///
    /// # Errors
    ///
    /// Returns revision resolution, backend, graph index, or object parsing errors.
    pub async fn commit_count_estimate(&self, start: &str) -> Result<usize> {
        let start = self.commit(start).await?;
        Ok(self.walk_commit_oids(&[start.oid]).await?.len())
    }

    /// Compare two refs, commit ids, or tags using graph reachability.
    ///
    /// # Errors
    ///
    /// Returns revision resolution, backend, graph index, or object parsing errors.
    pub async fn compare_commits(&self, base: &str, head: &str) -> Result<CommitComparison> {
        let base = self.commit(base).await?;
        let head = self.commit(head).await?;
        let base_reachable = self.reachable_set(&[base.oid]).await?;
        let head_reachable = self.reachable_set(&[head.oid]).await?;
        let merge_base = match self.merge_base_oid(base.oid, head.oid).await? {
            Some(oid) => self.commit_summary(&oid).await?,
            None => None,
        };
        Ok(CommitComparison {
            ahead_by: head_reachable.difference(&base_reachable).count(),
            behind_by: base_reachable.difference(&head_reachable).count(),
            base,
            head,
            merge_base,
        })
    }

    /// Rebuild the commit graph index from stored commit objects.
    ///
    /// # Errors
    ///
    /// Returns backend or object parsing errors.
    pub async fn repair_commit_graph(&self) -> Result<usize> {
        let commit_ids = self
            .storage
            .list_object_ids(&self.tenant, &self.repository, Some(ObjectKind::Commit))
            .await?;
        let mut commits = HashMap::new();
        for (oid, _) in commit_ids {
            let Some(object) = self.read_object(&oid).await? else {
                continue;
            };
            if object.kind != ObjectKind::Commit {
                continue;
            }
            let commit = parse_commit(&object.data)?;
            commits.insert(
                oid,
                IndexedCommit {
                    oid,
                    tree: commit.tree,
                    parents: commit.parents,
                    commit_time: commit_time_from_identity(&commit.committer),
                    generation: 1,
                },
            );
        }

        let mut memo = HashMap::new();
        let keys = commits.keys().copied().collect::<Vec<_>>();
        for oid in keys {
            let generation = compute_generation(oid, &commits, &mut memo, &mut HashSet::new());
            if let Some(commit) = commits.get_mut(&oid) {
                commit.generation = generation;
            }
        }
        let mut repaired = commits.into_values().collect::<Vec<_>>();
        repaired.sort_by_key(|left| left.oid);
        let count = repaired.len();
        self.storage
            .replace_commit_graph(&self.tenant, &self.repository, &repaired)
            .await?;
        Ok(count)
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

    /// Advertise refs for upload-pack protocol v0/v1 clients.
    ///
    /// # Errors
    ///
    /// Returns backend or object parsing errors while resolving refs and peeled tags.
    pub async fn advertise_refs(&self) -> Result<RefAdvertisement> {
        UploadPackService::new(self.clone()).advertise_refs().await
    }

    /// Negotiate an upload-pack fetch request into an object plan.
    ///
    /// # Errors
    ///
    /// Returns protocol, backend, or object parsing errors if the request cannot be served.
    pub async fn negotiate_fetch(&self, request: UploadPackRequest) -> Result<FetchPackPlan> {
        UploadPackService::new(self.clone())
            .negotiate_fetch(request)
            .await
    }

    /// Build a pack and wire response for an upload-pack fetch plan.
    ///
    /// # Errors
    ///
    /// Returns backend, object parsing, compression, or protocol framing errors.
    pub async fn build_fetch_pack(&self, plan: FetchPackPlan) -> Result<FetchPackResponse> {
        UploadPackService::new(self.clone())
            .build_fetch_pack(plan)
            .await
    }

    /// Parse, quarantine, and validate a receive-pack push request.
    ///
    /// # Errors
    ///
    /// Returns protocol, object closure, fast-forward, ref conflict, policy, backend, or object
    /// parsing errors.
    pub async fn prepare_push<P>(&self, request: ReceivePackRequest, policy: &P) -> Result<PushPlan>
    where
        P: PushPolicy,
    {
        ReceivePackService::new(self.clone())
            .prepare_push(request, policy)
            .await
    }

    /// Apply a prepared receive-pack push and publish ref invalidations.
    ///
    /// # Errors
    ///
    /// Returns backend, compare-and-swap conflict, reflog, or invalidation publishing errors.
    pub async fn apply_push<P>(
        &self,
        plan: PushPlan,
        actor: &str,
        timestamp: time::OffsetDateTime,
        publisher: &P,
    ) -> Result<ReceivePackReport>
    where
        P: EventPublisher,
    {
        ReceivePackService::new(self.clone())
            .apply_push(plan, actor, timestamp, publisher)
            .await
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

    async fn commit_summaries(&self, oids: Vec<ObjectId>) -> Result<Vec<CommitSummary>> {
        let mut commits = Vec::with_capacity(oids.len());
        for oid in oids {
            commits.push(
                self.commit_summary(&oid)
                    .await?
                    .ok_or_else(|| Error::ObjectNotFound(oid.to_hex()))?,
            );
        }
        Ok(commits)
    }

    async fn indexed_commits_by_oid(&self) -> Result<HashMap<ObjectId, IndexedCommit>> {
        Ok(self
            .storage
            .list_indexed_commits(&self.tenant, &self.repository)
            .await?
            .into_iter()
            .map(|commit| (commit.oid, commit))
            .collect())
    }

    async fn ensure_indexed_commit(
        &self,
        indexed: &mut HashMap<ObjectId, IndexedCommit>,
        oid: ObjectId,
    ) -> Result<IndexedCommit> {
        if let Some(commit) = indexed.get(&oid) {
            return Ok(commit.clone());
        }
        if let Some(commit) = self
            .storage
            .read_indexed_commit(&self.tenant, &self.repository, &oid)
            .await?
        {
            indexed.insert(oid, commit.clone());
            return Ok(commit);
        }
        let summary = self
            .commit_summary(&oid)
            .await?
            .ok_or_else(|| Error::ObjectNotFound(oid.to_hex()))?;
        let generation = summary
            .parents
            .iter()
            .filter_map(|parent| indexed.get(parent))
            .map(|parent| parent.generation.saturating_add(1))
            .max()
            .unwrap_or(1);
        let commit = IndexedCommit {
            oid,
            tree: summary.tree,
            parents: summary.parents,
            commit_time: commit_time_from_identity(&summary.committer),
            generation,
        };
        self.storage
            .upsert_commits(
                &self.tenant,
                &self.repository,
                std::slice::from_ref(&commit),
            )
            .await?;
        indexed.insert(oid, commit.clone());
        Ok(commit)
    }

    async fn parent_ids(&self, oid: &ObjectId) -> Result<Vec<ObjectId>> {
        if let Some(commit) = self
            .storage
            .read_indexed_commit(&self.tenant, &self.repository, oid)
            .await?
        {
            return Ok(commit.parents);
        }
        let summary = self
            .commit_summary(oid)
            .await?
            .ok_or_else(|| Error::ObjectNotFound(oid.to_hex()))?;
        Ok(summary.parents)
    }

    async fn walk_commit_oids(&self, starts: &[ObjectId]) -> Result<Vec<ObjectId>> {
        let mut indexed = self.indexed_commits_by_oid().await?;
        for start in starts {
            self.ensure_indexed_commit(&mut indexed, *start).await?;
        }

        let mut seen = HashSet::new();
        let mut frontier = starts.to_vec();
        let mut walked = Vec::new();
        while !frontier.is_empty() {
            let next = best_frontier_index(&frontier, &indexed);
            let oid = frontier.swap_remove(next);
            if !seen.insert(oid) {
                continue;
            }
            let commit = self.ensure_indexed_commit(&mut indexed, oid).await?;
            walked.push(oid);
            for parent in commit.parents {
                if !seen.contains(&parent) {
                    self.ensure_indexed_commit(&mut indexed, parent).await?;
                    frontier.push(parent);
                }
            }
        }
        Ok(walked)
    }

    async fn reachable_set(&self, starts: &[ObjectId]) -> Result<HashSet<ObjectId>> {
        Ok(self.walk_commit_oids(starts).await?.into_iter().collect())
    }

    async fn is_ancestor_oid(&self, ancestor: ObjectId, descendant: ObjectId) -> Result<bool> {
        if ancestor == descendant {
            return Ok(true);
        }
        Ok(self.reachable_set(&[descendant]).await?.contains(&ancestor))
    }

    async fn merge_base_oid(&self, left: ObjectId, right: ObjectId) -> Result<Option<ObjectId>> {
        let left_reachable = self.reachable_set(&[left]).await?;
        let right_reachable = self.reachable_set(&[right]).await?;
        let common = left_reachable
            .intersection(&right_reachable)
            .copied()
            .collect::<Vec<_>>();
        if common.is_empty() {
            return Ok(None);
        }

        let mut best = Vec::new();
        for candidate in &common {
            let mut dominated = false;
            for other in &common {
                if candidate != other && self.is_ancestor_oid(*candidate, *other).await? {
                    dominated = true;
                    break;
                }
            }
            if !dominated {
                best.push(*candidate);
            }
        }

        let indexed = self.indexed_commits_by_oid().await?;
        best.sort_by(|left, right| commit_order(*left, *right, &indexed));
        Ok(best.into_iter().next())
    }

    async fn history_path_matches(
        &self,
        commit: &CommitSummary,
        path: Option<&str>,
    ) -> Result<bool> {
        let Some(path) = path else {
            return Ok(true);
        };
        if path.is_empty() {
            return Ok(true);
        }

        let current = self.tree_entry_signature(&commit.tree, path).await?;
        if commit.parents.is_empty() {
            return Ok(current.is_some());
        }
        for parent in &commit.parents {
            let Some(parent) = self.commit_summary(parent).await? else {
                continue;
            };
            if self.tree_entry_signature(&parent.tree, path).await? != current {
                return Ok(true);
            }
        }
        Ok(false)
    }

    async fn tree_entry_signature(
        &self,
        root: &ObjectId,
        path: &str,
    ) -> Result<Option<(ObjectId, u32, ObjectKind)>> {
        Ok(self
            .tree_entry(root, path)
            .await?
            .map(|entry| (entry.oid, entry.mode, entry.kind)))
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
            .publish_invalidation(InvalidationEvent::new(
                self.tenant.clone(),
                self.repository.clone(),
                InvalidationEventKind::RefWrite {
                    refname: refname.to_owned(),
                },
            ))
            .await
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

fn best_frontier_index(frontier: &[ObjectId], indexed: &HashMap<ObjectId, IndexedCommit>) -> usize {
    let mut best = 0;
    for index in 1..frontier.len() {
        if commit_order(frontier[index], frontier[best], indexed).is_lt() {
            best = index;
        }
    }
    best
}

fn commit_order(
    left: ObjectId,
    right: ObjectId,
    indexed: &HashMap<ObjectId, IndexedCommit>,
) -> std::cmp::Ordering {
    let left_commit = indexed.get(&left);
    let right_commit = indexed.get(&right);
    right_commit
        .map(|commit| commit.commit_time)
        .unwrap_or_default()
        .cmp(
            &left_commit
                .map(|commit| commit.commit_time)
                .unwrap_or_default(),
        )
        .then_with(|| {
            right_commit
                .map(|commit| commit.generation)
                .unwrap_or_default()
                .cmp(
                    &left_commit
                        .map(|commit| commit.generation)
                        .unwrap_or_default(),
                )
        })
        .then_with(|| left.cmp(&right))
}

fn compute_generation(
    oid: ObjectId,
    commits: &HashMap<ObjectId, IndexedCommit>,
    memo: &mut HashMap<ObjectId, u32>,
    visiting: &mut HashSet<ObjectId>,
) -> u32 {
    if let Some(generation) = memo.get(&oid) {
        return *generation;
    }
    if !visiting.insert(oid) {
        return 1;
    }
    let generation = commits
        .get(&oid)
        .map(|commit| {
            commit
                .parents
                .iter()
                .filter(|parent| commits.contains_key(parent))
                .map(|parent| compute_generation(*parent, commits, memo, visiting))
                .max()
                .unwrap_or_default()
                .saturating_add(1)
        })
        .unwrap_or(1);
    visiting.remove(&oid);
    memo.insert(oid, generation);
    generation
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
