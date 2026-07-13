//! In-memory backend for tests and local prototyping.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use grit_lib::objects::{parse_commit, ObjectId, ObjectKind};

use crate::cache::{Cache, CacheKey, CacheValue, EventPublisher, InvalidationEvent};
use crate::error::{Error, Result};
use crate::ids::{RepositoryId, TenantId};
use crate::storage::{
    commit_time_from_identity, BrowseIndex, CommitGraphStore, ConfigStore, ImportPublication,
    ImportPublicationResult, ImportSession, ImportStateStore, IndexedCommit, IndexedTreeEntry,
    ObjectStore, PackMetadata, PackObjectIndex, PackStore, RefStore, ReflogEntry, ReflogStore,
    StoredObject, StoredPack, StoredRef,
};

type TreeKey = (ObjectId, String);

#[derive(Default)]
struct MemoryImportState {
    generation: u64,
    token: u64,
    complete: bool,
    published: bool,
    trusted_objects: HashMap<ObjectId, ObjectKind>,
}

#[derive(Default)]
struct RepoState {
    objects: RwLock<HashMap<ObjectId, StoredObject>>,
    refs: RwLock<BTreeMap<String, StoredRef>>,
    reflogs: RwLock<BTreeMap<String, Vec<ReflogEntry>>>,
    config: RwLock<BTreeMap<String, String>>,
    trees: RwLock<BTreeMap<TreeKey, IndexedTreeEntry>>,
    commits: RwLock<BTreeMap<ObjectId, IndexedCommit>>,
    import: RwLock<MemoryImportState>,
    packs: RwLock<HashMap<Vec<u8>, StoredPack>>,
    cache: RwLock<HashMap<CacheKey, CacheValue>>,
}

/// In-memory repository backend with repository-scoped row storage.
///
/// Tenant and repository identifiers occur only in the nested repository index; child
/// collections use their natural keys without repeating repository identifiers in every row.
#[derive(Default)]
pub struct MemoryBackend {
    repositories: RwLock<HashMap<TenantId, HashMap<RepositoryId, Arc<RepoState>>>>,
    import_sequence: RwLock<u64>,
    pack_sequence: RwLock<u64>,
    events: RwLock<Vec<InvalidationEvent>>,
}

#[async_trait]
impl ImportStateStore for MemoryBackend {
    async fn begin_import(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
    ) -> Result<ImportSession> {
        let token = {
            let mut sequence = self
                .import_sequence
                .write()
                .map_err(|_| Error::Backend("memory import sequence lock poisoned".to_owned()))?;
            *sequence = sequence
                .checked_add(1)
                .ok_or_else(|| Error::Backend("import session token overflow".to_owned()))?;
            *sequence
        };
        let repo = self.repo_state(tenant, repository)?;
        let mut state = repo
            .import
            .write()
            .map_err(|_| Error::Backend("memory import state lock poisoned".to_owned()))?;
        let mut trusted_objects = if state.complete {
            state
                .trusted_objects
                .iter()
                .map(|(oid, kind)| (*oid, *kind))
                .collect()
        } else {
            Vec::new()
        };
        trusted_objects.sort_by_key(|(oid, _)| *oid);
        state.generation = state
            .generation
            .checked_add(1)
            .ok_or_else(|| Error::Backend("import generation overflow".to_owned()))?;
        state.complete = false;
        state.published = false;
        state.token = token;
        Ok(ImportSession::new(
            tenant.clone(),
            repository.clone(),
            state.generation,
            token,
            trusted_objects,
        ))
    }

    async fn publish_import(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        session: &ImportSession,
        publication: &ImportPublication,
    ) -> Result<ImportPublicationResult> {
        let Some(repo) = self.existing_repo_state(tenant, repository)? else {
            return Err(stale_import_session(tenant, repository, session));
        };
        let mut state = repo
            .import
            .write()
            .map_err(|_| Error::Backend("memory import state lock poisoned".to_owned()))?;
        if session.tenant() != tenant
            || session.repository() != repository
            || state.generation != session.generation()
            || state.token != session.token()
            || state.complete
            || state.published
        {
            return Err(stale_import_session(tenant, repository, session));
        }

        let mut config = repo
            .config
            .write()
            .map_err(|_| Error::Backend("memory config lock poisoned".to_owned()))?;
        let mut refs = repo
            .refs
            .write()
            .map_err(|_| Error::Backend("memory ref lock poisoned".to_owned()))?;
        for (key, value) in &publication.config_entries {
            config.insert(key.clone(), value.clone());
        }
        let desired = publication
            .refs
            .iter()
            .map(|(refname, _)| refname.as_str())
            .collect::<HashSet<_>>();
        let pruned_refs = if publication.prune_deleted_refs {
            refs.keys()
                .filter(|refname| !desired.contains(refname.as_str()))
                .cloned()
                .collect::<Vec<_>>()
        } else {
            Vec::new()
        };
        for refname in &pruned_refs {
            refs.remove(refname);
        }
        for (refname, value) in &publication.refs {
            refs.insert(refname.clone(), value.clone());
        }
        state
            .trusted_objects
            .extend(publication.newly_trusted.iter().copied());
        state.published = true;
        Ok(ImportPublicationResult { pruned_refs })
    }

    async fn complete_import(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        session: &ImportSession,
    ) -> Result<()> {
        let Some(repo) = self.existing_repo_state(tenant, repository)? else {
            return Err(stale_import_session(tenant, repository, session));
        };
        let mut state = repo
            .import
            .write()
            .map_err(|_| Error::Backend("memory import state lock poisoned".to_owned()))?;
        if session.tenant() != tenant
            || session.repository() != repository
            || state.generation != session.generation()
            || state.token != session.token()
            || state.complete
            || !state.published
        {
            return Err(stale_import_session(tenant, repository, session));
        }
        state.complete = true;
        Ok(())
    }
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

impl MemoryBackend {
    /// Create an empty in-memory backend.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Return published invalidation events.
    ///
    /// # Errors
    ///
    /// Returns an error if the event log lock is poisoned.
    pub fn events(&self) -> Result<Vec<InvalidationEvent>> {
        self.events
            .read()
            .map(|guard| guard.clone())
            .map_err(|_| Error::Backend("memory event log lock poisoned".to_owned()))
    }

    fn repo_state(&self, tenant: &TenantId, repository: &RepositoryId) -> Result<Arc<RepoState>> {
        if let Some(state) = self
            .repositories
            .read()
            .map_err(|_| Error::Backend("memory repository lock poisoned".to_owned()))?
            .get(tenant)
            .and_then(|repositories| repositories.get(repository))
            .cloned()
        {
            return Ok(state);
        }
        self.repositories
            .write()
            .map(|mut repositories| {
                repositories
                    .entry(tenant.clone())
                    .or_default()
                    .entry(repository.clone())
                    .or_insert_with(|| Arc::new(RepoState::default()))
                    .clone()
            })
            .map_err(|_| Error::Backend("memory repository lock poisoned".to_owned()))
    }

    fn existing_repo_state(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
    ) -> Result<Option<Arc<RepoState>>> {
        self.repositories
            .read()
            .map(|tenants| {
                tenants
                    .get(tenant)
                    .and_then(|repositories| repositories.get(repository))
                    .cloned()
            })
            .map_err(|_| Error::Backend("memory repository lock poisoned".to_owned()))
    }

    fn store_object(repo: &RepoState, oid: ObjectId, object: &StoredObject) -> Result<()> {
        repo.objects
            .write()
            .map(|mut objects| {
                objects.entry(oid).or_insert_with(|| object.clone());
            })
            .map_err(|_| Error::Backend("memory object lock poisoned".to_owned()))
    }

    fn indexed_commit(
        repo: &RepoState,
        oid: ObjectId,
        object: &StoredObject,
    ) -> Result<Option<IndexedCommit>> {
        if object.kind != ObjectKind::Commit {
            return Ok(None);
        }
        let commit = parse_commit(&object.data)?;
        let commits = repo
            .commits
            .read()
            .map_err(|_| Error::Backend("memory commit graph lock poisoned".to_owned()))?;
        let generation = commit
            .parents
            .iter()
            .filter_map(|parent| commits.get(parent))
            .map(|parent| parent.generation.saturating_add(1))
            .max()
            .unwrap_or(1);
        Ok(Some(IndexedCommit {
            oid,
            tree: commit.tree,
            parents: commit.parents,
            commit_time: commit_time_from_identity(&commit.committer),
            generation,
        }))
    }

    fn next_pack_order(&self) -> Result<u64> {
        let mut sequence = self
            .pack_sequence
            .write()
            .map_err(|_| Error::Backend("memory pack sequence lock poisoned".to_owned()))?;
        *sequence = sequence.saturating_add(1);
        Ok(*sequence)
    }

    fn newest_packed_object(
        repo: &RepoState,
        oid: &ObjectId,
    ) -> Result<Option<(PackMetadata, PackObjectIndex, Vec<u8>)>> {
        repo.packs
            .read()
            .map(|packs| {
                packs
                    .values()
                    .filter_map(|pack| {
                        pack.index
                            .iter()
                            .find(|entry| entry.oid == *oid)
                            .map(|entry| (pack.metadata.clone(), entry.clone(), pack.data.clone()))
                    })
                    .max_by_key(|(metadata, _, _)| metadata.storage_order)
            })
            .map_err(|_| Error::Backend("memory pack lock poisoned".to_owned()))
    }

    fn read_object_from_state(repo: &RepoState, oid: &ObjectId) -> Result<Option<StoredObject>> {
        if let Some((_, index, data)) = Self::newest_packed_object(repo, oid)? {
            return crate::packfile::read_object_at_offset(&data, index.offset, oid.algo())
                .map(Some);
        }
        repo.objects
            .read()
            .map(|objects| objects.get(oid).cloned())
            .map_err(|_| Error::Backend("memory object lock poisoned".to_owned()))
    }
}

#[async_trait]
impl ObjectStore for MemoryBackend {
    async fn read_object(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        oid: &ObjectId,
    ) -> Result<Option<StoredObject>> {
        let Some(repo) = self.existing_repo_state(tenant, repository)? else {
            return Ok(None);
        };
        Self::read_object_from_state(&repo, oid)
    }

    async fn write_object(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        oid: &ObjectId,
        object: &StoredObject,
    ) -> Result<()> {
        let repo = self.repo_state(tenant, repository)?;
        Self::store_object(&repo, *oid, object)?;
        if let Some(commit) = Self::indexed_commit(&repo, *oid, object)? {
            repo.commits
                .write()
                .map(|mut commits| {
                    commits.insert(*oid, commit);
                })
                .map_err(|_| Error::Backend("memory commit graph lock poisoned".to_owned()))?;
        }
        Ok(())
    }

    async fn write_imported_object(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        oid: &ObjectId,
        object: &StoredObject,
    ) -> Result<()> {
        let repo = self.repo_state(tenant, repository)?;
        Self::store_object(&repo, *oid, object)
    }

    async fn write_imported_objects(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        imported: Vec<(ObjectId, StoredObject)>,
    ) -> Result<()> {
        let repo = self.repo_state(tenant, repository)?;
        repo.objects
            .write()
            .map(|mut objects| {
                objects.reserve(imported.len());
                for (oid, object) in imported {
                    objects.entry(oid).or_insert(object);
                }
            })
            .map_err(|_| Error::Backend("memory object lock poisoned".to_owned()))
    }

    async fn object_exists(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        oid: &ObjectId,
    ) -> Result<bool> {
        let Some(repo) = self.existing_repo_state(tenant, repository)? else {
            return Ok(false);
        };
        if Self::newest_packed_object(&repo, oid)?.is_some() {
            return Ok(true);
        }
        repo.objects
            .read()
            .map(|objects| objects.contains_key(oid))
            .map_err(|_| Error::Backend("memory object lock poisoned".to_owned()))
    }

    async fn count_objects(&self, tenant: &TenantId, repository: &RepositoryId) -> Result<usize> {
        Ok(self.list_object_ids(tenant, repository, None).await?.len())
    }

    async fn list_object_ids(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        kind: Option<ObjectKind>,
    ) -> Result<Vec<(ObjectId, ObjectKind)>> {
        let Some(repo) = self.existing_repo_state(tenant, repository)? else {
            return Ok(Vec::new());
        };
        let mut ids = repo
            .objects
            .read()
            .map(|objects| {
                objects
                    .iter()
                    .filter(|(_, object)| kind.is_none_or(|kind| object.kind == kind))
                    .map(|(oid, object)| (*oid, object.kind))
                    .collect::<HashMap<_, _>>()
            })
            .map_err(|_| Error::Backend("memory object lock poisoned".to_owned()))?;
        for pack in repo
            .packs
            .read()
            .map_err(|_| Error::Backend("memory pack lock poisoned".to_owned()))?
            .values()
            .cloned()
        {
            for entry in pack.index {
                if kind.is_none_or(|kind| entry.kind == kind) {
                    ids.entry(entry.oid).or_insert(entry.kind);
                }
            }
        }
        let mut ids = ids.into_iter().collect::<Vec<_>>();
        ids.sort_by_key(|(oid, _)| *oid);
        Ok(ids)
    }
}

#[async_trait]
impl RefStore for MemoryBackend {
    async fn read_ref(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        refname: &str,
    ) -> Result<Option<StoredRef>> {
        let Some(repo) = self.existing_repo_state(tenant, repository)? else {
            return Ok(None);
        };
        repo.refs
            .read()
            .map(|refs| refs.get(refname).cloned())
            .map_err(|_| Error::Backend("memory ref lock poisoned".to_owned()))
    }

    async fn write_ref(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        refname: &str,
        value: &StoredRef,
        expected: Option<Option<StoredRef>>,
    ) -> Result<()> {
        let repo = self.repo_state(tenant, repository)?;
        let mut refs = repo
            .refs
            .write()
            .map_err(|_| Error::Backend("memory ref lock poisoned".to_owned()))?;
        if let Some(expected) = expected {
            let current = refs.get(refname).cloned();
            if current != expected {
                return Err(Error::RefConflict(refname.to_owned()));
            }
        }
        refs.insert(refname.to_owned(), value.clone());
        Ok(())
    }

    async fn delete_ref(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        refname: &str,
        expected: Option<StoredRef>,
    ) -> Result<()> {
        let Some(repo) = self.existing_repo_state(tenant, repository)? else {
            if expected.is_some() {
                return Err(Error::RefConflict(refname.to_owned()));
            }
            return Ok(());
        };
        let mut refs = repo
            .refs
            .write()
            .map_err(|_| Error::Backend("memory ref lock poisoned".to_owned()))?;
        if let Some(expected) = expected {
            let current = refs.get(refname).cloned();
            if current != Some(expected) {
                return Err(Error::RefConflict(refname.to_owned()));
            }
        }
        refs.remove(refname);
        Ok(())
    }

    async fn list_refs(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        prefix: &str,
    ) -> Result<Vec<(String, StoredRef)>> {
        let Some(repo) = self.existing_repo_state(tenant, repository)? else {
            return Ok(Vec::new());
        };
        repo.refs
            .read()
            .map(|refs| {
                refs.iter()
                    .filter(|(refname, _)| refname.starts_with(prefix))
                    .map(|(refname, value)| (refname.clone(), value.clone()))
                    .collect()
            })
            .map_err(|_| Error::Backend("memory ref lock poisoned".to_owned()))
    }
}

#[async_trait]
impl ReflogStore for MemoryBackend {
    async fn append_reflog(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        entry: &ReflogEntry,
    ) -> Result<()> {
        let repo = self.repo_state(tenant, repository)?;
        repo.reflogs
            .write()
            .map(|mut reflogs| {
                reflogs
                    .entry(entry.refname.clone())
                    .or_default()
                    .push(entry.clone());
            })
            .map_err(|_| Error::Backend("memory reflog lock poisoned".to_owned()))
    }

    async fn read_reflog(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        refname: &str,
    ) -> Result<Vec<ReflogEntry>> {
        let Some(repo) = self.existing_repo_state(tenant, repository)? else {
            return Ok(Vec::new());
        };
        repo.reflogs
            .read()
            .map(|reflogs| reflogs.get(refname).cloned().unwrap_or_default())
            .map_err(|_| Error::Backend("memory reflog lock poisoned".to_owned()))
    }
}

#[async_trait]
impl ConfigStore for MemoryBackend {
    async fn get_config(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        key: &str,
    ) -> Result<Option<String>> {
        let Some(repo) = self.existing_repo_state(tenant, repository)? else {
            return Ok(None);
        };
        repo.config
            .read()
            .map(|config| config.get(key).cloned())
            .map_err(|_| Error::Backend("memory config lock poisoned".to_owned()))
    }

    async fn set_config(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        key: &str,
        value: &str,
    ) -> Result<()> {
        let repo = self.repo_state(tenant, repository)?;
        repo.config
            .write()
            .map(|mut config| {
                config.insert(key.to_owned(), value.to_owned());
            })
            .map_err(|_| Error::Backend("memory config lock poisoned".to_owned()))
    }

    async fn list_config(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        prefix: &str,
    ) -> Result<Vec<(String, String)>> {
        let Some(repo) = self.existing_repo_state(tenant, repository)? else {
            return Ok(Vec::new());
        };
        repo.config
            .read()
            .map(|config| {
                config
                    .iter()
                    .filter(|(key, _)| key.starts_with(prefix))
                    .map(|(key, value)| (key.clone(), value.clone()))
                    .collect()
            })
            .map_err(|_| Error::Backend("memory config lock poisoned".to_owned()))
    }
}

#[async_trait]
impl BrowseIndex for MemoryBackend {
    async fn upsert_tree_entries(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        entries: &[IndexedTreeEntry],
    ) -> Result<()> {
        let repo = self.repo_state(tenant, repository)?;
        repo.trees
            .write()
            .map(|mut trees| {
                for entry in entries {
                    trees.insert((entry.tree_oid, entry.path.clone()), entry.clone());
                }
            })
            .map_err(|_| Error::Backend("memory tree lock poisoned".to_owned()))
    }

    async fn replace_tree_entries(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        entries: &[IndexedTreeEntry],
    ) -> Result<()> {
        let repo = self.repo_state(tenant, repository)?;
        repo.trees
            .write()
            .map(|mut trees| {
                trees.clear();
                for entry in entries {
                    trees.insert((entry.tree_oid, entry.path.clone()), entry.clone());
                }
            })
            .map_err(|_| Error::Backend("memory tree lock poisoned".to_owned()))
    }

    async fn list_tree_entries(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        tree_oid: &ObjectId,
        prefix: &str,
    ) -> Result<Vec<IndexedTreeEntry>> {
        let Some(repo) = self.existing_repo_state(tenant, repository)? else {
            return Ok(Vec::new());
        };
        repo.trees
            .read()
            .map(|trees| {
                trees
                    .range((*tree_oid, prefix.to_owned())..)
                    .take_while(|((candidate_tree, path), _)| {
                        candidate_tree == tree_oid && path.starts_with(prefix)
                    })
                    .map(|(_, entry)| entry.clone())
                    .collect()
            })
            .map_err(|_| Error::Backend("memory tree lock poisoned".to_owned()))
    }

    async fn read_blob_at_path(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        tree_oid: &ObjectId,
        path: &str,
    ) -> Result<Option<StoredObject>> {
        let Some(repo) = self.existing_repo_state(tenant, repository)? else {
            return Ok(None);
        };
        let oid = repo
            .trees
            .read()
            .map_err(|_| Error::Backend("memory tree lock poisoned".to_owned()))?
            .get(&(*tree_oid, path.to_owned()))
            .map(|entry| entry.oid);
        match oid {
            Some(oid) => Self::read_object_from_state(&repo, &oid),
            None => Ok(None),
        }
    }
}

#[async_trait]
impl CommitGraphStore for MemoryBackend {
    async fn upsert_commits(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        commits: &[IndexedCommit],
    ) -> Result<()> {
        let repo = self.repo_state(tenant, repository)?;
        repo.commits
            .write()
            .map(|mut stored| {
                for commit in commits {
                    stored.insert(commit.oid, commit.clone());
                }
            })
            .map_err(|_| Error::Backend("memory commit graph lock poisoned".to_owned()))
    }

    async fn replace_commit_graph(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        commits: &[IndexedCommit],
    ) -> Result<()> {
        let repo = self.repo_state(tenant, repository)?;
        repo.commits
            .write()
            .map(|mut stored| {
                stored.clear();
                for commit in commits {
                    stored.insert(commit.oid, commit.clone());
                }
            })
            .map_err(|_| Error::Backend("memory commit graph lock poisoned".to_owned()))
    }

    async fn read_indexed_commit(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        oid: &ObjectId,
    ) -> Result<Option<IndexedCommit>> {
        let Some(repo) = self.existing_repo_state(tenant, repository)? else {
            return Ok(None);
        };
        repo.commits
            .read()
            .map(|commits| commits.get(oid).cloned())
            .map_err(|_| Error::Backend("memory commit graph lock poisoned".to_owned()))
    }

    async fn commit_parents(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        oid: &ObjectId,
    ) -> Result<Vec<ObjectId>> {
        Ok(self
            .read_indexed_commit(tenant, repository, oid)
            .await?
            .map(|commit| commit.parents)
            .unwrap_or_default())
    }

    async fn commit_children(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        oid: &ObjectId,
    ) -> Result<Vec<ObjectId>> {
        let Some(repo) = self.existing_repo_state(tenant, repository)? else {
            return Ok(Vec::new());
        };
        repo.commits
            .read()
            .map(|commits| {
                let mut children = commits
                    .iter()
                    .filter(|(_, commit)| commit.parents.contains(oid))
                    .map(|(child, _)| *child)
                    .collect::<Vec<_>>();
                children.sort();
                children
            })
            .map_err(|_| Error::Backend("memory commit graph lock poisoned".to_owned()))
    }

    async fn list_indexed_commits(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
    ) -> Result<Vec<IndexedCommit>> {
        let Some(repo) = self.existing_repo_state(tenant, repository)? else {
            return Ok(Vec::new());
        };
        repo.commits
            .read()
            .map(|commits| {
                let mut commits = commits.values().cloned().collect::<Vec<_>>();
                commits.sort_by(|left, right| {
                    right
                        .commit_time
                        .cmp(&left.commit_time)
                        .then_with(|| left.oid.cmp(&right.oid))
                });
                commits
            })
            .map_err(|_| Error::Backend("memory commit graph lock poisoned".to_owned()))
    }
}

#[async_trait]
impl PackStore for MemoryBackend {
    async fn write_pack(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        pack: &StoredPack,
    ) -> Result<PackMetadata> {
        let repo = self.repo_state(tenant, repository)?;
        let key = pack.metadata.pack_checksum.clone();
        let mut packs = repo
            .packs
            .write()
            .map_err(|_| Error::Backend("memory pack lock poisoned".to_owned()))?;
        if let Some(existing) = packs.get(&key) {
            return Ok(existing.metadata.clone());
        }
        let mut stored = pack.clone();
        stored.metadata.storage_order = self.next_pack_order()?;
        let metadata = stored.metadata.clone();
        packs.insert(key, stored);
        Ok(metadata)
    }

    async fn read_pack_metadata(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        pack_checksum: &[u8],
    ) -> Result<Option<PackMetadata>> {
        let Some(repo) = self.existing_repo_state(tenant, repository)? else {
            return Ok(None);
        };
        repo.packs
            .read()
            .map(|packs| packs.get(pack_checksum).map(|pack| pack.metadata.clone()))
            .map_err(|_| Error::Backend("memory pack lock poisoned".to_owned()))
    }

    async fn read_pack_data(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        pack_checksum: &[u8],
    ) -> Result<Option<Vec<u8>>> {
        let Some(repo) = self.existing_repo_state(tenant, repository)? else {
            return Ok(None);
        };
        repo.packs
            .read()
            .map(|packs| packs.get(pack_checksum).map(|pack| pack.data.clone()))
            .map_err(|_| Error::Backend("memory pack lock poisoned".to_owned()))
    }

    async fn find_packed_object(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        oid: &ObjectId,
    ) -> Result<Option<(PackMetadata, PackObjectIndex)>> {
        let Some(repo) = self.existing_repo_state(tenant, repository)? else {
            return Ok(None);
        };
        Ok(Self::newest_packed_object(&repo, oid)?.map(|(metadata, index, _)| (metadata, index)))
    }

    async fn read_pack_index_at_offset(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        pack_checksum: &[u8],
        offset: u64,
    ) -> Result<Option<PackObjectIndex>> {
        let Some(repo) = self.existing_repo_state(tenant, repository)? else {
            return Ok(None);
        };
        repo.packs
            .read()
            .map(|packs| {
                packs.get(pack_checksum).and_then(|pack| {
                    pack.index
                        .iter()
                        .find(|entry| entry.offset == offset)
                        .cloned()
                })
            })
            .map_err(|_| Error::Backend("memory pack lock poisoned".to_owned()))
    }

    async fn list_packs(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
    ) -> Result<Vec<PackMetadata>> {
        let Some(repo) = self.existing_repo_state(tenant, repository)? else {
            return Ok(Vec::new());
        };
        repo.packs
            .read()
            .map(|packs| {
                let mut metadata = packs
                    .values()
                    .map(|pack| pack.metadata.clone())
                    .collect::<Vec<_>>();
                metadata.sort_by_key(|pack| pack.storage_order);
                metadata
            })
            .map_err(|_| Error::Backend("memory pack lock poisoned".to_owned()))
    }

    async fn list_pack_objects(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        pack_checksum: Option<&[u8]>,
    ) -> Result<Vec<(PackMetadata, PackObjectIndex)>> {
        let Some(repo) = self.existing_repo_state(tenant, repository)? else {
            return Ok(Vec::new());
        };
        repo.packs
            .read()
            .map(|packs| {
                let mut rows = packs
                    .iter()
                    .filter(|(checksum, _)| {
                        pack_checksum.is_none_or(|wanted| checksum.as_slice() == wanted)
                    })
                    .flat_map(|(_, pack)| {
                        pack.index
                            .iter()
                            .map(|entry| (pack.metadata.clone(), entry.clone()))
                    })
                    .collect::<Vec<_>>();
                rows.sort_by(|left, right| {
                    left.0
                        .storage_order
                        .cmp(&right.0.storage_order)
                        .then_with(|| left.1.offset.cmp(&right.1.offset))
                });
                rows
            })
            .map_err(|_| Error::Backend("memory pack lock poisoned".to_owned()))
    }
}

#[async_trait]
impl Cache for MemoryBackend {
    async fn get(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        key: &CacheKey,
    ) -> Result<Option<CacheValue>> {
        let Some(repo) = self.existing_repo_state(tenant, repository)? else {
            return Ok(None);
        };
        repo.cache
            .read()
            .map(|cache| cache.get(key).cloned())
            .map_err(|_| Error::Cache("memory cache lock poisoned".to_owned()))
    }

    async fn put(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        key: &CacheKey,
        value: CacheValue,
    ) -> Result<()> {
        let repo = self.repo_state(tenant, repository)?;
        repo.cache
            .write()
            .map(|mut cache| {
                cache.insert(key.clone(), value);
            })
            .map_err(|_| Error::Cache("memory cache lock poisoned".to_owned()))
    }

    async fn invalidate(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        key: &CacheKey,
    ) -> Result<()> {
        let Some(repo) = self.existing_repo_state(tenant, repository)? else {
            return Ok(());
        };
        repo.cache
            .write()
            .map(|mut cache| {
                cache.remove(key);
            })
            .map_err(|_| Error::Cache("memory cache lock poisoned".to_owned()))
    }

    async fn invalidate_repository(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
    ) -> Result<()> {
        let Some(repo) = self.existing_repo_state(tenant, repository)? else {
            return Ok(());
        };
        repo.cache
            .write()
            .map(|mut cache| {
                cache.clear();
            })
            .map_err(|_| Error::Cache("memory cache lock poisoned".to_owned()))
    }
}

#[async_trait]
impl EventPublisher for MemoryBackend {
    async fn publish_invalidation(&self, event: InvalidationEvent) -> Result<()> {
        self.events
            .write()
            .map(|mut events| events.push(event))
            .map_err(|_| Error::Backend("memory event log lock poisoned".to_owned()))
    }
}
