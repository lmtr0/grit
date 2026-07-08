//! In-memory backend for tests and local prototyping.

use std::collections::{BTreeMap, HashMap};
use std::sync::RwLock;

use async_trait::async_trait;
use grit_lib::objects::{parse_commit, ObjectId, ObjectKind};

use crate::cache::{Cache, CacheKey, CacheValue, EventPublisher, InvalidationEvent};
use crate::error::{Error, Result};
use crate::ids::{RepositoryId, TenantId};
use crate::storage::{
    commit_time_from_identity, BrowseIndex, CommitGraphStore, ConfigStore, IndexedCommit,
    IndexedTreeEntry, ObjectStore, RefStore, ReflogEntry, ReflogStore, StoredObject, StoredRef,
};

type RepoKey = (TenantId, RepositoryId);
type ObjectKey = (RepoKey, ObjectId);
type RefKey = (RepoKey, String);
type ConfigKey = (RepoKey, String);
type TreeKey = (RepoKey, ObjectId, String);
type CommitKey = (RepoKey, ObjectId);
type CacheEntryKey = (RepoKey, CacheKey);

/// In-memory repository backend.
#[derive(Default)]
pub struct MemoryBackend {
    objects: RwLock<HashMap<ObjectKey, StoredObject>>,
    refs: RwLock<BTreeMap<RefKey, StoredRef>>,
    reflogs: RwLock<BTreeMap<RefKey, Vec<ReflogEntry>>>,
    config: RwLock<BTreeMap<ConfigKey, String>>,
    trees: RwLock<BTreeMap<TreeKey, IndexedTreeEntry>>,
    commits: RwLock<BTreeMap<CommitKey, IndexedCommit>>,
    cache: RwLock<HashMap<CacheEntryKey, CacheValue>>,
    events: RwLock<Vec<InvalidationEvent>>,
}

fn repo_key(tenant: &TenantId, repository: &RepositoryId) -> RepoKey {
    (tenant.clone(), repository.clone())
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

    fn indexed_commit(
        &self,
        repo: RepoKey,
        oid: ObjectId,
        object: &StoredObject,
    ) -> Result<Option<IndexedCommit>> {
        if object.kind != ObjectKind::Commit {
            return Ok(None);
        }
        let commit = parse_commit(&object.data)?;
        let generation = self
            .commits
            .read()
            .map_err(|_| Error::Backend("memory commit graph lock poisoned".to_owned()))?
            .iter()
            .filter(|((candidate_repo, candidate_oid), _)| {
                candidate_repo == &repo && commit.parents.contains(candidate_oid)
            })
            .map(|(_, parent)| parent.generation.saturating_add(1))
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
}

#[async_trait]
impl ObjectStore for MemoryBackend {
    async fn read_object(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        oid: &ObjectId,
    ) -> Result<Option<StoredObject>> {
        self.objects
            .read()
            .map(|objects| objects.get(&(repo_key(tenant, repository), *oid)).cloned())
            .map_err(|_| Error::Backend("memory object lock poisoned".to_owned()))
    }

    async fn write_object(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        oid: &ObjectId,
        object: &StoredObject,
    ) -> Result<()> {
        let repo = repo_key(tenant, repository);
        self.objects
            .write()
            .map(|mut objects| {
                objects
                    .entry((repo.clone(), *oid))
                    .or_insert_with(|| object.clone());
            })
            .map_err(|_| Error::Backend("memory object lock poisoned".to_owned()))?;

        if let Some(commit) = self.indexed_commit(repo.clone(), *oid, object)? {
            self.commits
                .write()
                .map(|mut commits| {
                    commits.insert((repo, *oid), commit);
                })
                .map_err(|_| Error::Backend("memory commit graph lock poisoned".to_owned()))?;
        }
        Ok(())
    }

    async fn object_exists(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        oid: &ObjectId,
    ) -> Result<bool> {
        self.objects
            .read()
            .map(|objects| objects.contains_key(&(repo_key(tenant, repository), *oid)))
            .map_err(|_| Error::Backend("memory object lock poisoned".to_owned()))
    }

    async fn count_objects(&self, tenant: &TenantId, repository: &RepositoryId) -> Result<usize> {
        let repo = repo_key(tenant, repository);
        self.objects
            .read()
            .map(|objects| {
                objects
                    .keys()
                    .filter(|(candidate_repo, _)| candidate_repo == &repo)
                    .count()
            })
            .map_err(|_| Error::Backend("memory object lock poisoned".to_owned()))
    }

    async fn list_object_ids(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        kind: Option<ObjectKind>,
    ) -> Result<Vec<(ObjectId, ObjectKind)>> {
        let repo = repo_key(tenant, repository);
        self.objects
            .read()
            .map(|objects| {
                let mut ids = objects
                    .iter()
                    .filter(|((candidate_repo, _), object)| {
                        candidate_repo == &repo && kind.is_none_or(|kind| object.kind == kind)
                    })
                    .map(|((_, oid), object)| (*oid, object.kind))
                    .collect::<Vec<_>>();
                ids.sort_by_key(|(oid, _)| *oid);
                ids
            })
            .map_err(|_| Error::Backend("memory object lock poisoned".to_owned()))
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
        self.refs
            .read()
            .map(|refs| {
                refs.get(&(repo_key(tenant, repository), refname.to_owned()))
                    .cloned()
            })
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
        let key = (repo_key(tenant, repository), refname.to_owned());
        let mut refs = self
            .refs
            .write()
            .map_err(|_| Error::Backend("memory ref lock poisoned".to_owned()))?;
        if let Some(expected) = expected {
            let current = refs.get(&key).cloned();
            if current != expected {
                return Err(Error::RefConflict(refname.to_owned()));
            }
        }
        refs.insert(key, value.clone());
        Ok(())
    }

    async fn delete_ref(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        refname: &str,
        expected: Option<StoredRef>,
    ) -> Result<()> {
        let key = (repo_key(tenant, repository), refname.to_owned());
        let mut refs = self
            .refs
            .write()
            .map_err(|_| Error::Backend("memory ref lock poisoned".to_owned()))?;
        if let Some(expected) = expected {
            let current = refs.get(&key).cloned();
            if current != Some(expected) {
                return Err(Error::RefConflict(refname.to_owned()));
            }
        }
        refs.remove(&key);
        Ok(())
    }

    async fn list_refs(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        prefix: &str,
    ) -> Result<Vec<(String, StoredRef)>> {
        let repo = repo_key(tenant, repository);
        self.refs
            .read()
            .map(|refs| {
                refs.iter()
                    .filter(|((candidate_repo, refname), _)| {
                        candidate_repo == &repo && refname.starts_with(prefix)
                    })
                    .map(|((_, refname), value)| (refname.clone(), value.clone()))
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
        self.reflogs
            .write()
            .map(|mut reflogs| {
                reflogs
                    .entry((repo_key(tenant, repository), entry.refname.clone()))
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
        self.reflogs
            .read()
            .map(|reflogs| {
                reflogs
                    .get(&(repo_key(tenant, repository), refname.to_owned()))
                    .cloned()
                    .unwrap_or_default()
            })
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
        self.config
            .read()
            .map(|config| {
                config
                    .get(&(repo_key(tenant, repository), key.to_owned()))
                    .cloned()
            })
            .map_err(|_| Error::Backend("memory config lock poisoned".to_owned()))
    }

    async fn set_config(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        key: &str,
        value: &str,
    ) -> Result<()> {
        self.config
            .write()
            .map(|mut config| {
                config.insert(
                    (repo_key(tenant, repository), key.to_owned()),
                    value.to_owned(),
                );
            })
            .map_err(|_| Error::Backend("memory config lock poisoned".to_owned()))
    }

    async fn list_config(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        prefix: &str,
    ) -> Result<Vec<(String, String)>> {
        let repo = repo_key(tenant, repository);
        self.config
            .read()
            .map(|config| {
                config
                    .iter()
                    .filter(|((candidate_repo, key), _)| {
                        candidate_repo == &repo && key.starts_with(prefix)
                    })
                    .map(|((_, key), value)| (key.clone(), value.clone()))
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
        let repo = repo_key(tenant, repository);
        self.trees
            .write()
            .map(|mut trees| {
                for entry in entries {
                    trees.insert(
                        (repo.clone(), entry.tree_oid, entry.path.clone()),
                        entry.clone(),
                    );
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
        let repo = repo_key(tenant, repository);
        self.trees
            .read()
            .map(|trees| {
                trees
                    .iter()
                    .filter(|((candidate_repo, candidate_tree, path), _)| {
                        candidate_repo == &repo
                            && candidate_tree == tree_oid
                            && path.starts_with(prefix)
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
        let repo = repo_key(tenant, repository);
        let oid = self
            .trees
            .read()
            .map_err(|_| Error::Backend("memory tree lock poisoned".to_owned()))?
            .get(&(repo.clone(), *tree_oid, path.to_owned()))
            .map(|entry| entry.oid);
        match oid {
            Some(oid) => self.read_object(tenant, repository, &oid).await,
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
        let repo = repo_key(tenant, repository);
        self.commits
            .write()
            .map(|mut stored| {
                for commit in commits {
                    stored.insert((repo.clone(), commit.oid), commit.clone());
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
        let repo = repo_key(tenant, repository);
        self.commits
            .write()
            .map(|mut stored| {
                stored.retain(|(candidate_repo, _), _| candidate_repo != &repo);
                for commit in commits {
                    stored.insert((repo.clone(), commit.oid), commit.clone());
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
        self.commits
            .read()
            .map(|commits| commits.get(&(repo_key(tenant, repository), *oid)).cloned())
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
        let repo = repo_key(tenant, repository);
        self.commits
            .read()
            .map(|commits| {
                let mut children = commits
                    .iter()
                    .filter(|((candidate_repo, _), commit)| {
                        candidate_repo == &repo && commit.parents.contains(oid)
                    })
                    .map(|((_, child), _)| *child)
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
        let repo = repo_key(tenant, repository);
        self.commits
            .read()
            .map(|commits| {
                let mut commits = commits
                    .iter()
                    .filter(|((candidate_repo, _), _)| candidate_repo == &repo)
                    .map(|(_, commit)| commit.clone())
                    .collect::<Vec<_>>();
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
impl Cache for MemoryBackend {
    async fn get(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        key: &CacheKey,
    ) -> Result<Option<CacheValue>> {
        self.cache
            .read()
            .map(|cache| {
                cache
                    .get(&(repo_key(tenant, repository), key.clone()))
                    .cloned()
            })
            .map_err(|_| Error::Cache("memory cache lock poisoned".to_owned()))
    }

    async fn put(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        key: &CacheKey,
        value: CacheValue,
    ) -> Result<()> {
        self.cache
            .write()
            .map(|mut cache| {
                cache.insert((repo_key(tenant, repository), key.clone()), value);
            })
            .map_err(|_| Error::Cache("memory cache lock poisoned".to_owned()))
    }

    async fn invalidate(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        key: &CacheKey,
    ) -> Result<()> {
        self.cache
            .write()
            .map(|mut cache| {
                cache.remove(&(repo_key(tenant, repository), key.clone()));
            })
            .map_err(|_| Error::Cache("memory cache lock poisoned".to_owned()))
    }

    async fn invalidate_repository(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
    ) -> Result<()> {
        let repo = repo_key(tenant, repository);
        self.cache
            .write()
            .map(|mut cache| {
                cache.retain(|(candidate_repo, _), _| candidate_repo != &repo);
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
