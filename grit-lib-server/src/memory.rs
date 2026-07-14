//! In-memory backend for tests and local prototyping.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::PathBuf;
use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use grit_lib::check_ref_format::{check_refname_format, RefNameOptions};
use grit_lib::objects::{parse_commit, ObjectId, ObjectKind};
use grit_lib::pack::{PackIndex, PackIndexEntry};

use crate::cache::{Cache, CacheKey, CacheValue, EventPublisher, InvalidationEvent};
use crate::error::{Error, Result};
use crate::ids::{RepositoryId, TenantId};
use crate::protocol::memory_pack_promotion::{
    canonical_promoted_object_id, MemoryPackPromotionError, MemoryPackPromotionReceipt,
    MemoryPackPromotionStage,
};
use crate::protocol::push_atomic_publication::{
    command_reflog, PromotionReceiptBinding, PushAtomicPublicationBackend, PushPromotionReceipt,
    PushPublicationAudit, PushPublicationBackendError, PushPublicationCommand,
    PushPublicationCommandOutcome, PushPublicationCommandStatus, PushPublicationOutboxEvent,
    PushPublicationRejection, PushPublicationReport, PushPublicationTransaction,
};
use crate::storage::{
    commit_time_from_identity, BrowseIndex, CommitGraphStore, ConfigStore, ImportPublication,
    ImportPublicationResult, ImportSession, ImportStateStore, ImportedPack, IndexedCommit,
    IndexedTreeEntry, ObjectReadResult, ObjectStore, PackMetadata, PackObjectIndex, PackStore,
    RefStore, ReflogEntry, ReflogStore, StoredObject, StoredPack, StoredRef,
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
    packs: RwLock<MemoryPackState>,
    cache: RwLock<HashMap<CacheKey, CacheValue>>,
}

#[derive(Default)]
struct MemoryPackState {
    repository_generation: u64,
    storage_order: u64,
    by_checksum: HashMap<Vec<u8>, Arc<MemoryPack>>,
    newest_by_oid: HashMap<ObjectId, PackLocation>,
    promotion_receipts: HashMap<MemoryPromotionKey, MemoryPackPromotionReceipt>,
    publication_receipts: HashMap<ObjectId, MemoryPublicationRecord>,
    rejection_audits: HashMap<ObjectId, MemoryRejectionRecord>,
}

#[derive(Clone)]
struct MemoryPublicationRecord {
    transaction: PushPublicationTransaction,
    report: PushPublicationReport,
}

#[derive(Clone, PartialEq, Eq)]
struct MemoryRejectionRecord {
    audit: PushPublicationAudit,
    reason: PushPublicationRejection,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
struct MemoryPromotionKey {
    quarantine_id: crate::protocol::push_quarantine::QuarantineId,
    quarantine_generation: u64,
    prepared_fingerprint: ObjectId,
}

struct MemoryPack {
    metadata: PackMetadata,
    data: Arc<Vec<u8>>,
    entries: Vec<PackObjectIndex>,
    decode_index: PackIndex,
}

#[derive(Clone)]
struct PackLocation {
    pack: Arc<MemoryPack>,
    entry_index: usize,
}

struct PreparedMemoryPack {
    metadata: PackMetadata,
    data: Arc<Vec<u8>>,
    entries: Vec<PackObjectIndex>,
    decode_index: PackIndex,
}

/// In-memory repository backend with repository-scoped row storage.
///
/// Tenant and repository identifiers occur only in the nested repository index; child
/// collections use their natural keys without repeating repository identifiers in every row.
#[derive(Default)]
pub struct MemoryBackend {
    repositories: RwLock<HashMap<TenantId, HashMap<RepositoryId, Arc<RepoState>>>>,
    import_sequence: RwLock<u64>,
    events: RwLock<Vec<InvalidationEvent>>,
    publication_outbox: RwLock<Vec<PushPublicationOutboxEvent>>,
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
        let repository_generation = {
            let mut packs = repo
                .packs
                .write()
                .map_err(|_| Error::Backend("memory pack lock poisoned".to_owned()))?;
            packs.repository_generation = packs
                .repository_generation
                .checked_add(1)
                .ok_or_else(|| Error::Backend("repository generation overflow".to_owned()))?;
            packs.repository_generation
        };
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
        state.generation = repository_generation;
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

    /// Return durable publication outbox records in commit order.
    ///
    /// # Errors
    ///
    /// Returns an error if the outbox lock is poisoned.
    pub fn publication_outbox(&self) -> Result<Vec<PushPublicationOutboxEvent>> {
        self.publication_outbox
            .read()
            .map(|outbox| outbox.clone())
            .map_err(|_| Error::Backend("memory publication outbox lock poisoned".to_owned()))
    }

    /// Return the repository generation to bind into a prepared memory push snapshot.
    ///
    /// The first observation initializes generation one. Import starts advance the same value,
    /// causing any previously prepared pack promotion to fail its under-lock recheck.
    ///
    /// # Errors
    ///
    /// Returns a backend error when repository or pack-state locking fails.
    pub fn push_repository_generation(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
    ) -> Result<u64> {
        let repo = self.repo_state(tenant, repository)?;
        let mut packs = repo
            .packs
            .write()
            .map_err(|_| Error::Backend("memory pack lock poisoned".to_owned()))?;
        if packs.repository_generation == 0 {
            packs.repository_generation = 1;
        }
        Ok(packs.repository_generation)
    }

    pub(crate) fn install_staged_push_pack(
        &self,
        stage: MemoryPackPromotionStage,
    ) -> std::result::Result<MemoryPackPromotionReceipt, MemoryPackPromotionError> {
        let prepared =
            Self::prepare_memory_pack(stage.pack).map_err(|_| MemoryPackPromotionError::Backend)?;
        let object_count = prepared.metadata.object_count;
        let size_bytes = prepared.metadata.size_bytes;
        let repo = self
            .existing_repo_state(&stage.tenant, &stage.repository)
            .map_err(|_| MemoryPackPromotionError::Backend)?
            .ok_or(MemoryPackPromotionError::StaleGeneration)?;
        let loose_objects = repo
            .objects
            .read()
            .map_err(|_| MemoryPackPromotionError::Backend)?;
        for entry in &prepared.entries {
            let Some(loose) = loose_objects.get(&entry.oid) else {
                continue;
            };
            if entry.kind != loose.kind
                || canonical_promoted_object_id(entry.kind, &loose.data, entry.oid.algo())?
                    != entry.oid
            {
                return Err(MemoryPackPromotionError::Collision);
            }
        }
        let mut packs = repo
            .packs
            .write()
            .map_err(|_| MemoryPackPromotionError::Backend)?;
        if packs.repository_generation != stage.repository_generation {
            return Err(MemoryPackPromotionError::StaleGeneration);
        }
        let key = MemoryPromotionKey {
            quarantine_id: stage.quarantine_id,
            quarantine_generation: stage.quarantine_generation,
            prepared_fingerprint: stage.prepared_fingerprint,
        };
        if let Some(receipt) = packs.promotion_receipts.get(&key) {
            if receipt.tenant() == &stage.tenant
                && receipt.repository() == &stage.repository
                && receipt.quarantine_id() == stage.quarantine_id
                && receipt.quarantine_generation() == stage.quarantine_generation
                && receipt.prepared_fingerprint() == stage.prepared_fingerprint
                && receipt.pack_checksum() == stage.pack_checksum
                && receipt.index_checksum() == stage.index_checksum
                && receipt.repository_generation() == stage.repository_generation
                && receipt.object_count() == object_count
                && receipt.size_bytes() == size_bytes
            {
                return Ok(receipt.clone());
            }
            return Err(MemoryPackPromotionError::Collision);
        }

        let existing = packs
            .by_checksum
            .get(stage.pack_checksum.as_bytes())
            .cloned();
        if let Some(existing) = &existing {
            let metadata_matches = existing.metadata.index_checksum
                == stage.index_checksum.as_bytes()
                && existing.metadata.object_count == prepared.metadata.object_count
                && existing.metadata.size_bytes == prepared.metadata.size_bytes;
            if !metadata_matches
                || existing.data.as_slice() != prepared.data.as_slice()
                || existing.entries != prepared.entries
            {
                return Err(MemoryPackPromotionError::Collision);
            }
        }
        if prepared.entries.iter().any(|entry| {
            packs
                .newest_by_oid
                .get(&entry.oid)
                .map(|location| &location.pack.entries[location.entry_index])
                .is_some_and(|current| current.kind != entry.kind || current.size != entry.size)
        }) {
            return Err(MemoryPackPromotionError::Collision);
        }
        packs
            .promotion_receipts
            .try_reserve(1)
            .map_err(|_| MemoryPackPromotionError::Allocation)?;
        if existing.is_none() {
            packs
                .by_checksum
                .try_reserve(1)
                .map_err(|_| MemoryPackPromotionError::Allocation)?;
            packs
                .newest_by_oid
                .try_reserve(prepared.entries.len())
                .map_err(|_| MemoryPackPromotionError::Allocation)?;
        }

        let deduplicated = existing.is_some();
        if !deduplicated {
            let storage_order = Self::reserve_pack_orders(&mut packs, 1)
                .map_err(|_| MemoryPackPromotionError::Backend)?;
            Self::install_prepared_pack(&mut packs, prepared, storage_order);
        }
        let receipt = MemoryPackPromotionReceipt::new(
            stage.tenant,
            stage.repository,
            stage.quarantine_id,
            stage.quarantine_generation,
            stage.repository_generation,
            stage.pack_checksum,
            stage.index_checksum,
            stage.prepared_fingerprint,
            object_count,
            size_bytes,
            deduplicated,
        );
        packs.promotion_receipts.insert(key, receipt.clone());
        drop(loose_objects);
        Ok(receipt)
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

    fn reserve_pack_orders(packs: &mut MemoryPackState, count: usize) -> Result<u64> {
        let count = u64::try_from(count)
            .map_err(|_| Error::Backend("memory pack count exceeds u64".to_owned()))?;
        let first = packs
            .storage_order
            .checked_add(1)
            .ok_or_else(|| Error::Backend("memory pack storage order overflow".to_owned()))?;
        packs.storage_order = packs
            .storage_order
            .checked_add(count)
            .ok_or_else(|| Error::Backend("memory pack storage order overflow".to_owned()))?;
        Ok(first)
    }

    fn newest_pack_location(repo: &RepoState, oid: &ObjectId) -> Result<Option<PackLocation>> {
        repo.packs
            .read()
            .map(|packs| packs.newest_by_oid.get(oid).cloned())
            .map_err(|_| Error::Backend("memory pack lock poisoned".to_owned()))
    }

    fn prepare_memory_pack(pack: StoredPack) -> Result<PreparedMemoryPack> {
        Self::prepare_shared_memory_pack(pack.metadata, Arc::new(pack.data), pack.index)
    }

    fn prepare_shared_memory_pack(
        metadata: PackMetadata,
        data: Arc<Vec<u8>>,
        index: Vec<PackObjectIndex>,
    ) -> Result<PreparedMemoryPack> {
        if usize::try_from(metadata.object_count).ok() != Some(index.len()) {
            return Err(Error::Protocol(
                "pack metadata object count does not match its index".to_owned(),
            ));
        }
        if u64::try_from(data.len()).ok() != Some(metadata.size_bytes) {
            return Err(Error::Protocol(
                "pack metadata size does not match its bytes".to_owned(),
            ));
        }
        let hash_bytes = index
            .first()
            .map(|entry| entry.oid.as_bytes().len())
            .unwrap_or_else(|| metadata.pack_checksum.len());
        if !matches!(hash_bytes, 20 | 32)
            || metadata.pack_checksum.len() != hash_bytes
            || metadata.index_checksum.len() != hash_bytes
            || index
                .iter()
                .any(|entry| entry.oid.as_bytes().len() != hash_bytes)
        {
            return Err(Error::Protocol(
                "pack index mixes incompatible object hash algorithms".to_owned(),
            ));
        }
        let mut seen_oids = HashSet::new();
        seen_oids
            .try_reserve(index.len())
            .map_err(|_| Error::Backend("cannot reserve pack OID validation set".to_owned()))?;
        let mut seen_offsets = HashSet::new();
        seen_offsets
            .try_reserve(index.len())
            .map_err(|_| Error::Backend("cannot reserve pack offset validation set".to_owned()))?;
        if index
            .iter()
            .any(|entry| !seen_oids.insert(entry.oid) || !seen_offsets.insert(entry.offset))
        {
            return Err(Error::Protocol(
                "pack index contains duplicate object ids or offsets".to_owned(),
            ));
        }

        let mut decode_entries = Vec::new();
        decode_entries
            .try_reserve_exact(index.len())
            .map_err(|_| Error::Backend("cannot reserve decoded pack index".to_owned()))?;
        for entry in &index {
            let mut oid = Vec::new();
            oid.try_reserve_exact(hash_bytes)
                .map_err(|_| Error::Backend("cannot reserve pack index OID".to_owned()))?;
            oid.extend_from_slice(entry.oid.as_bytes());
            decode_entries.push(PackIndexEntry {
                oid,
                offset: entry.offset,
                crc32: None,
            });
        }
        decode_entries.sort_by(|left, right| left.oid.cmp(&right.oid));
        let mut fanout = [0u32; 256];
        for entry in &decode_entries {
            if let Some(first) = entry.oid.first() {
                fanout[usize::from(*first)] = fanout[usize::from(*first)].saturating_add(1);
            }
        }
        let mut cumulative = 0u32;
        for count in &mut fanout {
            cumulative = cumulative.saturating_add(*count);
            *count = cumulative;
        }
        let pack_name = metadata
            .pack_checksum
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        let pack_path = PathBuf::from(format!("memory-packs/{pack_name}.pack"));
        let decode_index = PackIndex {
            idx_path: pack_path.with_extension("idx"),
            pack_path,
            hash_bytes,
            entries: decode_entries,
            fanout,
        };
        Ok(PreparedMemoryPack {
            metadata,
            data,
            entries: index,
            decode_index,
        })
    }

    fn install_prepared_pack(
        packs: &mut MemoryPackState,
        mut prepared: PreparedMemoryPack,
        storage_order: u64,
    ) -> PackMetadata {
        prepared.metadata.storage_order = storage_order;
        let stored = Arc::new(MemoryPack {
            metadata: prepared.metadata,
            data: prepared.data,
            entries: prepared.entries,
            decode_index: prepared.decode_index,
        });
        for (entry_index, entry) in stored.entries.iter().enumerate() {
            packs.newest_by_oid.insert(
                entry.oid,
                PackLocation {
                    pack: Arc::clone(&stored),
                    entry_index,
                },
            );
        }
        let metadata = stored.metadata.clone();
        packs
            .by_checksum
            .insert(metadata.pack_checksum.clone(), stored);
        metadata
    }

    fn read_object_from_state(repo: &RepoState, oid: &ObjectId) -> Result<Option<StoredObject>> {
        if let Some(location) = Self::newest_pack_location(repo, oid)? {
            let object = grit_lib::pack::read_object_from_pack_bytes(
                &location.pack.data,
                &location.pack.decode_index,
                oid.as_bytes(),
            )?;
            return Ok(Some(StoredObject::new(object.kind, object.data)));
        }
        repo.objects
            .read()
            .map(|objects| objects.get(oid).cloned())
            .map_err(|_| Error::Backend("memory object lock poisoned".to_owned()))
    }
}

#[async_trait]
impl PushAtomicPublicationBackend for MemoryBackend {
    async fn publish_transaction(
        &self,
        transaction: PushPublicationTransaction,
    ) -> std::result::Result<PushPublicationReport, PushPublicationBackendError> {
        let repo = self
            .existing_repo_state(&transaction.tenant, &transaction.repository)
            .map_err(|_| PushPublicationBackendError)?
            .ok_or(PushPublicationBackendError)?;
        let mut packs = repo
            .packs
            .write()
            .map_err(|_| PushPublicationBackendError)?;
        if let Some(record) = packs
            .publication_receipts
            .get(&transaction.prepared_fingerprint)
        {
            if record.transaction == transaction {
                return Ok(record.report.clone());
            }
            return Err(PushPublicationBackendError);
        }
        if packs
            .rejection_audits
            .contains_key(&transaction.prepared_fingerprint)
        {
            return Err(PushPublicationBackendError);
        }
        validate_memory_publication_transaction(&transaction)?;
        let mut refs = repo.refs.write().map_err(|_| PushPublicationBackendError)?;
        let mut reflogs = repo
            .reflogs
            .write()
            .map_err(|_| PushPublicationBackendError)?;
        let mut outbox = self
            .publication_outbox
            .write()
            .map_err(|_| PushPublicationBackendError)?;

        let commands = &transaction.commands;
        let mut lock_order = Vec::new();
        lock_order
            .try_reserve_exact(commands.len())
            .map_err(|_| PushPublicationBackendError)?;
        lock_order.extend(commands.iter().map(|command| command.refname.as_str()));
        lock_order.sort_unstable();
        if lock_order.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(PushPublicationBackendError);
        }

        let generation_matches = packs.repository_generation == transaction.repository_generation;
        let promotion_matches = match &transaction.promotion {
            Some(binding) => memory_promotion_available(&packs, binding),
            None => true,
        };
        let mut statuses = Vec::new();
        statuses
            .try_reserve_exact(commands.len())
            .map_err(|_| PushPublicationBackendError)?;
        let mut evaluation_refs = refs.clone();
        for command in commands {
            let rejection = if !command.policy_allowed {
                Some(PushPublicationRejection::Policy)
            } else if !generation_matches {
                Some(PushPublicationRejection::StaleGeneration)
            } else if !promotion_matches {
                Some(PushPublicationRejection::PromotionMissing)
            } else if !memory_ref_matches(&evaluation_refs, command) {
                Some(PushPublicationRejection::RefConflict)
            } else if memory_namespace_conflicts(&evaluation_refs, command) {
                Some(PushPublicationRejection::RefConflict)
            } else if !memory_fast_forward_matches(command) {
                Some(PushPublicationRejection::NonFastForward)
            } else {
                None
            };
            statuses.push(PushPublicationCommandStatus {
                ordinal: command.ordinal,
                refname: command.refname.clone(),
                kind: command.kind,
                outcome: rejection.map_or(
                    PushPublicationCommandOutcome::Applied,
                    PushPublicationCommandOutcome::Rejected,
                ),
            });
            if rejection.is_none() && !transaction.atomic {
                apply_memory_ref(&mut evaluation_refs, command);
            }
        }
        let first_rejection = statuses.iter().find_map(|status| match status.outcome {
            PushPublicationCommandOutcome::Rejected(reason) => Some(reason),
            PushPublicationCommandOutcome::Applied => None,
        });
        if transaction.atomic && first_rejection.is_some() {
            for status in &mut statuses {
                if status.outcome == PushPublicationCommandOutcome::Applied {
                    status.outcome = PushPublicationCommandOutcome::Rejected(
                        PushPublicationRejection::AtomicAborted,
                    );
                }
            }
        }

        let mut applied = HashSet::new();
        applied
            .try_reserve(statuses.len())
            .map_err(|_| PushPublicationBackendError)?;
        applied.extend(statuses.iter().filter_map(|status| {
            (status.outcome == PushPublicationCommandOutcome::Applied).then_some(status.ordinal)
        }));
        let next_generation = if applied.is_empty() {
            packs.repository_generation
        } else {
            packs
                .repository_generation
                .checked_add(1)
                .ok_or(PushPublicationBackendError)?
        };
        let mut next_refs = refs.clone();
        let mut next_reflogs = reflogs.clone();
        let mut events = Vec::new();
        events
            .try_reserve_exact(applied.len().saturating_add(2))
            .map_err(|_| PushPublicationBackendError)?;
        for command in commands
            .iter()
            .filter(|command| applied.contains(&command.ordinal))
        {
            match command.new_oid {
                Some(new_oid) => {
                    next_refs.insert(command.refname.clone(), StoredRef::Direct(new_oid));
                }
                None => {
                    next_refs.remove(&command.refname);
                }
            }
            next_reflogs
                .entry(command.refname.clone())
                .or_default()
                .push(command_reflog(
                    command,
                    transaction.hash_algo,
                    &transaction.reflog_identity,
                    transaction.timestamp,
                ));
            events.push(PushPublicationOutboxEvent::RefChanged {
                refname: command.refname.clone(),
                deleted: command.new_oid.is_none(),
            });
        }
        if !applied.is_empty() {
            let mut ordinals = Vec::new();
            ordinals
                .try_reserve_exact(applied.len())
                .map_err(|_| PushPublicationBackendError)?;
            ordinals.extend(
                commands
                    .iter()
                    .filter(|command| applied.contains(&command.ordinal))
                    .map(|command| command.ordinal),
            );
            events.push(PushPublicationOutboxEvent::PostReceive { ordinals });
            events.push(PushPublicationOutboxEvent::AcceptedAudit(
                transaction.audit.clone(),
            ));
        } else if let Some(reason) = first_rejection {
            events.push(PushPublicationOutboxEvent::RejectedAudit {
                audit: transaction.audit.clone(),
                reason,
            });
        }
        let report = PushPublicationReport {
            prepared_fingerprint: transaction.prepared_fingerprint,
            repository_generation: next_generation,
            statuses,
            structural: transaction.structural,
        };
        packs
            .publication_receipts
            .try_reserve(1)
            .map_err(|_| PushPublicationBackendError)?;
        outbox
            .try_reserve(events.len())
            .map_err(|_| PushPublicationBackendError)?;
        let receipt_report = report.clone();

        *refs = next_refs;
        *reflogs = next_reflogs;
        packs.repository_generation = next_generation;
        outbox.extend(events);
        packs.publication_receipts.insert(
            transaction.prepared_fingerprint,
            MemoryPublicationRecord {
                transaction,
                report: receipt_report,
            },
        );
        Ok(report)
    }

    async fn record_rejection(
        &self,
        audit: PushPublicationAudit,
        reason: PushPublicationRejection,
    ) -> std::result::Result<(), PushPublicationBackendError> {
        let repo = self
            .existing_repo_state(&audit.tenant, &audit.repository)
            .map_err(|_| PushPublicationBackendError)?
            .ok_or(PushPublicationBackendError)?;
        let mut packs = repo
            .packs
            .write()
            .map_err(|_| PushPublicationBackendError)?;
        if let Some(record) = packs.rejection_audits.get(&audit.prepared_fingerprint) {
            if record.audit == audit && record.reason == reason {
                return Ok(());
            }
            return Err(PushPublicationBackendError);
        }
        let mut outbox = self
            .publication_outbox
            .write()
            .map_err(|_| PushPublicationBackendError)?;
        outbox
            .try_reserve(1)
            .map_err(|_| PushPublicationBackendError)?;
        packs
            .rejection_audits
            .try_reserve(1)
            .map_err(|_| PushPublicationBackendError)?;
        packs.rejection_audits.insert(
            audit.prepared_fingerprint,
            MemoryRejectionRecord {
                audit: audit.clone(),
                reason,
            },
        );
        outbox.push(PushPublicationOutboxEvent::RejectedAudit { audit, reason });
        Ok(())
    }
}

fn memory_promotion_available(packs: &MemoryPackState, binding: &PromotionReceiptBinding) -> bool {
    let key = MemoryPromotionKey {
        quarantine_id: binding.quarantine_id,
        quarantine_generation: binding.quarantine_generation,
        prepared_fingerprint: binding.prepared_fingerprint,
    };
    packs
        .promotion_receipts
        .get(&key)
        .is_some_and(|receipt| receipt.publication_binding() == *binding)
}

fn validate_memory_publication_transaction(
    transaction: &PushPublicationTransaction,
) -> std::result::Result<(), PushPublicationBackendError> {
    if transaction.commands.is_empty()
        || transaction.repository_generation == 0
        || transaction.prepared_fingerprint.algo() != transaction.hash_algo
        || transaction.audit.tenant != transaction.tenant
        || transaction.audit.repository != transaction.repository
        || transaction.audit.prepared_fingerprint != transaction.prepared_fingerprint
        || transaction.audit.timestamp != transaction.timestamp
        || matches!(
            transaction.structural,
            crate::protocol::push_atomic_publication::PushStructuralPublicationPrerequisite::DeferredCanonicalIndex {
                objects: 0
            }
        )
    {
        return Err(PushPublicationBackendError);
    }
    if let Some(binding) = &transaction.promotion {
        if binding.tenant != transaction.tenant
            || binding.repository != transaction.repository
            || binding.repository_generation != transaction.repository_generation
            || binding.prepared_fingerprint != transaction.prepared_fingerprint
            || binding.quarantine_generation == 0
            || binding.pack_checksum.algo() != transaction.hash_algo
            || binding.index_checksum.algo() != transaction.hash_algo
            || binding.size_bytes == 0
        {
            return Err(PushPublicationBackendError);
        }
    }

    for (index, command) in transaction.commands.iter().enumerate() {
        let ordinal_matches = usize::try_from(command.ordinal).ok() == Some(index);
        let refname_valid = command.refname.starts_with("refs/")
            && check_refname_format(&command.refname, &RefNameOptions::default()).is_ok();
        let expected_valid = command
            .expected
            .is_none_or(|oid| oid.algo() == transaction.hash_algo && !oid.is_zero());
        let new_valid = command
            .new_oid
            .is_none_or(|oid| oid.algo() == transaction.hash_algo && !oid.is_zero());
        let kind_matches = match command.kind {
            crate::protocol::receive_pack::PushCommandKind::Create => {
                command.expected.is_none() && command.new_oid.is_some()
            }
            crate::protocol::receive_pack::PushCommandKind::Update => {
                command.expected.is_some() && command.new_oid.is_some()
            }
            crate::protocol::receive_pack::PushCommandKind::Delete => {
                command.expected.is_some() && command.new_oid.is_none()
            }
        };
        let ancestry_valid = command.ancestry.command_ordinal == command.ordinal
            && command
                .ancestry
                .old_commit
                .is_none_or(|oid| oid.algo() == transaction.hash_algo && !oid.is_zero())
            && command
                .ancestry
                .new_commit
                .is_none_or(|oid| oid.algo() == transaction.hash_algo && !oid.is_zero());
        if !ordinal_matches
            || !refname_valid
            || !expected_valid
            || !new_valid
            || !kind_matches
            || !ancestry_valid
        {
            return Err(PushPublicationBackendError);
        }
    }
    if transaction
        .commands
        .iter()
        .enumerate()
        .any(|(left, command)| {
            transaction.commands[left + 1..].iter().any(|other| {
                command.refname == other.refname
                    || ref_namespace_conflicts(&command.refname, &other.refname)
            })
        })
    {
        return Err(PushPublicationBackendError);
    }
    Ok(())
}

fn apply_memory_ref(refs: &mut BTreeMap<String, StoredRef>, command: &PushPublicationCommand) {
    match command.new_oid {
        Some(new_oid) => {
            refs.insert(command.refname.clone(), StoredRef::Direct(new_oid));
        }
        None => {
            refs.remove(&command.refname);
        }
    }
}

fn memory_ref_matches(
    refs: &BTreeMap<String, StoredRef>,
    command: &PushPublicationCommand,
) -> bool {
    match (refs.get(&command.refname), command.expected) {
        (None, None) => true,
        (Some(StoredRef::Direct(current)), Some(expected)) => *current == expected,
        _ => false,
    }
}

fn memory_namespace_conflicts(
    refs: &BTreeMap<String, StoredRef>,
    command: &PushPublicationCommand,
) -> bool {
    command.kind == crate::protocol::receive_pack::PushCommandKind::Create
        && refs.keys().any(|existing| {
            existing != &command.refname && ref_namespace_conflicts(existing, &command.refname)
        })
}

fn ref_namespace_conflicts(left: &str, right: &str) -> bool {
    left.strip_prefix(right)
        .is_some_and(|suffix| suffix.starts_with('/'))
        || right
            .strip_prefix(left)
            .is_some_and(|suffix| suffix.starts_with('/'))
}

fn memory_fast_forward_matches(command: &PushPublicationCommand) -> bool {
    if command.kind != crate::protocol::receive_pack::PushCommandKind::Update
        || !command.refname.starts_with("refs/heads/")
    {
        return true;
    }
    command.ancestry.old_commit == command.expected
        && command.ancestry.new_commit == command.new_oid
        && command.ancestry.old_is_ancestor == Some(true)
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

    async fn read_objects_batch(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        oids: &[ObjectId],
    ) -> Result<Vec<ObjectReadResult>> {
        crate::storage::validate_object_read_batch(oids)?;
        let Some(repo) = self.existing_repo_state(tenant, repository)? else {
            let mut missing = Vec::new();
            missing
                .try_reserve_exact(oids.len())
                .map_err(|_| Error::Backend("cannot reserve memory object batch".to_owned()))?;
            missing.extend(oids.iter().map(|oid| ObjectReadResult {
                oid: *oid,
                object: None,
            }));
            return Ok(missing);
        };
        let pack_locations = {
            let packs = repo
                .packs
                .read()
                .map_err(|_| Error::Backend("memory pack lock poisoned".to_owned()))?;
            let mut locations = Vec::new();
            locations
                .try_reserve_exact(oids.len())
                .map_err(|_| Error::Backend("cannot reserve memory object batch".to_owned()))?;
            locations.extend(oids.iter().map(|oid| packs.newest_by_oid.get(oid).cloned()));
            locations
        };
        let loose_objects = {
            let objects = repo
                .objects
                .read()
                .map_err(|_| Error::Backend("memory object lock poisoned".to_owned()))?;
            let mut loose = Vec::new();
            loose
                .try_reserve_exact(oids.len())
                .map_err(|_| Error::Backend("cannot reserve memory object batch".to_owned()))?;
            loose.extend(oids.iter().zip(&pack_locations).map(|(oid, location)| {
                if location.is_some() {
                    None
                } else {
                    objects.get(oid).cloned()
                }
            }));
            loose
        };
        let mut results = Vec::new();
        results
            .try_reserve_exact(oids.len())
            .map_err(|_| Error::Backend("cannot reserve memory object batch".to_owned()))?;
        for ((oid, location), loose) in oids.iter().zip(pack_locations).zip(loose_objects) {
            let object = if let Some(location) = location {
                let object = grit_lib::pack::read_object_from_pack_bytes(
                    &location.pack.data,
                    &location.pack.decode_index,
                    oid.as_bytes(),
                )?;
                Some(StoredObject::new(object.kind, object.data))
            } else {
                loose
            };
            results.push(ObjectReadResult { oid: *oid, object });
        }
        Ok(results)
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
        let packed = repo
            .packs
            .read()
            .map(|packs| packs.newest_by_oid.contains_key(oid))
            .map_err(|_| Error::Backend("memory pack lock poisoned".to_owned()))?;
        if packed {
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
        for (oid, location) in &repo
            .packs
            .read()
            .map_err(|_| Error::Backend("memory pack lock poisoned".to_owned()))?
            .newest_by_oid
        {
            let entry = &location.pack.entries[location.entry_index];
            if kind.is_none_or(|kind| entry.kind == kind) {
                ids.entry(*oid).or_insert(entry.kind);
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
    fn supports_native_pack_import(&self) -> bool {
        true
    }

    async fn write_imported_packs(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        packs: Vec<ImportedPack>,
    ) -> Result<Vec<PackMetadata>> {
        let prepared = packs
            .into_iter()
            .map(|pack| Self::prepare_shared_memory_pack(pack.metadata, pack.data, pack.index))
            .collect::<Result<Vec<_>>>()?;
        let repo = self.repo_state(tenant, repository)?;
        let mut stored = repo
            .packs
            .write()
            .map_err(|_| Error::Backend("memory pack lock poisoned".to_owned()))?;
        let mut metadata = Vec::with_capacity(prepared.len());
        let mut known_checksums = stored.by_checksum.keys().cloned().collect::<HashSet<_>>();
        let new_pack_count = prepared
            .iter()
            .filter(|pack| known_checksums.insert(pack.metadata.pack_checksum.clone()))
            .count();
        let mut next_order = if new_pack_count == 0 {
            0
        } else {
            Self::reserve_pack_orders(&mut stored, new_pack_count)?
        };
        for pack in prepared {
            if let Some(existing) = stored.by_checksum.get(&pack.metadata.pack_checksum) {
                metadata.push(existing.metadata.clone());
                continue;
            }
            metadata.push(Self::install_prepared_pack(&mut stored, pack, next_order));
            next_order = next_order.saturating_add(1);
        }
        Ok(metadata)
    }

    async fn write_pack(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        pack: &StoredPack,
    ) -> Result<PackMetadata> {
        let prepared = Self::prepare_memory_pack(pack.clone())?;
        let repo = self.repo_state(tenant, repository)?;
        let mut packs = repo
            .packs
            .write()
            .map_err(|_| Error::Backend("memory pack lock poisoned".to_owned()))?;
        if let Some(existing) = packs.by_checksum.get(&prepared.metadata.pack_checksum) {
            return Ok(existing.metadata.clone());
        }
        let storage_order = Self::reserve_pack_orders(&mut packs, 1)?;
        Ok(Self::install_prepared_pack(
            &mut packs,
            prepared,
            storage_order,
        ))
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
            .map(|packs| {
                packs
                    .by_checksum
                    .get(pack_checksum)
                    .map(|pack| pack.metadata.clone())
            })
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
            .map(|packs| {
                packs
                    .by_checksum
                    .get(pack_checksum)
                    .map(|pack| pack.data.to_vec())
            })
            .map_err(|_| Error::Backend("memory pack lock poisoned".to_owned()))
    }

    async fn read_pack_range(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        pack_checksum: &[u8],
        start: u64,
        len: u64,
    ) -> Result<Option<Vec<u8>>> {
        let Some(repo) = self.existing_repo_state(tenant, repository)? else {
            return Ok(None);
        };
        let start = usize::try_from(start)
            .map_err(|_| Error::Backend("pack range start exceeds usize".to_owned()))?;
        let len = usize::try_from(len)
            .map_err(|_| Error::Backend("pack range length exceeds usize".to_owned()))?;
        let end = start
            .checked_add(len)
            .ok_or_else(|| Error::Backend("pack range overflow".to_owned()))?;
        repo.packs
            .read()
            .map(|packs| {
                packs.by_checksum.get(pack_checksum).map(|pack| {
                    if start >= pack.data.len() {
                        Vec::new()
                    } else {
                        pack.data[start..end.min(pack.data.len())].to_vec()
                    }
                })
            })
            .map_err(|_| Error::Backend("memory pack lock poisoned".to_owned()))
    }

    async fn read_packed_object_data(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        pack_checksum: &[u8],
        oid: &ObjectId,
    ) -> Result<Option<StoredObject>> {
        let Some(repo) = self.existing_repo_state(tenant, repository)? else {
            return Ok(None);
        };
        let pack = repo
            .packs
            .read()
            .map_err(|_| Error::Backend("memory pack lock poisoned".to_owned()))?
            .by_checksum
            .get(pack_checksum)
            .cloned();
        let Some(pack) = pack else {
            return Ok(None);
        };
        if pack.decode_index.find_offset(oid).is_none() {
            return Ok(None);
        }
        let object = grit_lib::pack::read_object_from_pack_bytes(
            &pack.data,
            &pack.decode_index,
            oid.as_bytes(),
        )?;
        Ok(Some(StoredObject::new(object.kind, object.data)))
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
        Ok(Self::newest_pack_location(&repo, oid)?.map(|location| {
            (
                location.pack.metadata.clone(),
                location.pack.entries[location.entry_index].clone(),
            )
        }))
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
                packs.by_checksum.get(pack_checksum).and_then(|pack| {
                    pack.entries
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
                    .by_checksum
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
                    .by_checksum
                    .iter()
                    .filter(|(checksum, _)| {
                        pack_checksum.is_none_or(|wanted| checksum.as_slice() == wanted)
                    })
                    .flat_map(|(_, pack)| {
                        pack.entries
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
