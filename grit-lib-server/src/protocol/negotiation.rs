//! Bounded generation-aware fetch negotiation and metadata-only object closure expansion.

use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashSet, VecDeque};

use async_trait::async_trait;
use grit_lib::objects::{ObjectId, ObjectKind};

use crate::protocol::pack_stream::CancellationProbe;

/// Stable repository-local object ordinal hint tied to an explicit index generation.
///
/// Exact object IDs remain authoritative because this API does not carry an immutable-index proof
/// strong enough to make a compact bitmap collision-safe.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct StableObjectOrdinal {
    /// Explicit ordinal-index generation; ordinals from different generations never alias.
    pub index_generation: u64,
    /// Zero-based stable object ordinal.
    pub ordinal: u64,
}

/// Indexed commit metadata sufficient for negotiation without reading commit payloads.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NegotiationCommit {
    /// Commit object ID.
    pub oid: ObjectId,
    /// Root tree named by the commit.
    pub tree: ObjectId,
    /// Optional stable ordinal for the root tree.
    pub tree_ordinal: Option<StableObjectOrdinal>,
    /// Parents in canonical commit order with indexed queue metadata.
    pub parents: Vec<NegotiationCommitParent>,
    /// Optional topological generation; `None` activates the correctness-safe queue fallback.
    pub generation: Option<u32>,
    /// Header-stripped commit payload size used for selected-byte accounting.
    pub size_bytes: u64,
    /// Optional stable ordinal tied to an explicit index generation.
    pub ordinal: Option<StableObjectOrdinal>,
}

/// Indexed parent edge used to queue ancestors without rereading already excluded commits.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NegotiationCommitParent {
    /// Parent commit object ID.
    pub oid: ObjectId,
    /// Optional indexed parent generation.
    pub generation: Option<u32>,
    /// Optional stable parent ordinal.
    pub ordinal: Option<StableObjectOrdinal>,
}

/// Direct tree child metadata used without reading blob payloads.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NegotiationTreeEntry {
    /// Child object ID.
    pub oid: ObjectId,
    /// Child object kind.
    pub kind: ObjectKind,
    /// Header-stripped child payload size; required for blobs and ignored for trees.
    pub size_bytes: Option<u64>,
    /// Entry mode; gitlinks (`160000`) are excluded from the transferred object closure.
    pub mode: u32,
    /// Optional stable ordinal tied to an explicit index generation.
    pub ordinal: Option<StableObjectOrdinal>,
}

/// Indexed direct-tree metadata response.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NegotiationTree {
    /// Tree object ID.
    pub oid: ObjectId,
    /// Header-stripped encoded tree size.
    pub size_bytes: u64,
    /// Optional stable ordinal for the tree itself.
    pub ordinal: Option<StableObjectOrdinal>,
    /// Direct children only, in canonical tree order.
    pub entries: Vec<NegotiationTreeEntry>,
}

/// Backend boundary that never needs blob payloads during negotiation or closure expansion.
#[async_trait]
pub trait NegotiationMetadataSource: Send + Sync {
    /// Backend-specific metadata failure.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Read one indexed commit row without returning more than `max_parents` parent edges.
    ///
    /// `None` means that the requested ID is not an indexed commit. Implementations must enforce
    /// `max_parents` before materializing an unbounded parent vector.
    async fn read_commit(
        &self,
        oid: &ObjectId,
        max_parents: usize,
    ) -> Result<Option<NegotiationCommit>, Self::Error>;

    /// Read one indexed direct-tree row set without transferring child blob contents.
    ///
    /// Implementations must enforce `max_entries` before materializing an unbounded entry vector.
    async fn read_tree(
        &self,
        oid: &ObjectId,
        max_entries: usize,
    ) -> Result<Option<NegotiationTree>, Self::Error>;
}

/// Backend-neutral membership spill set keyed only by binary object IDs.
///
/// Implementations may use bounded scratch files or databases. They must not derive paths/keys
/// from tenant names, ref names, URLs, or other secrets.
pub trait NegotiationSpillSet: Send {
    /// Adapter-specific spill failure.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Return whether an object ID is present.
    fn contains(&mut self, oid: &ObjectId) -> Result<bool, Self::Error>;

    /// Insert an object ID and return whether it was newly inserted.
    fn insert(&mut self, oid: ObjectId) -> Result<bool, Self::Error>;

    /// Remove all temporary state. Repeated cleanup must be harmless.
    fn clear(&mut self) -> Result<(), Self::Error>;
}

/// Factory for isolated spill sets used by distinct negotiation membership roles.
///
/// One factory instance must be scoped to one authorized repository request. A role is only a
/// non-sensitive discriminator inside that request and must never be used as a global spill key.
pub trait NegotiationSpillFactory: Send {
    /// Spill-set implementation.
    type Set: NegotiationSpillSet<Error = Self::Error>;
    /// Adapter-specific spill failure.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Create an empty isolated set identified only by a non-sensitive role.
    fn create(&mut self, role: NegotiationSpillRole) -> Result<Self::Set, Self::Error>;
}

/// Non-sensitive purpose of an isolated spill set.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NegotiationSpillRole {
    /// Commits reachable from accepted haves.
    ExcludedCommits,
    /// Commits visited from wants.
    VisitedCommits,
    /// Selected commit/tree/blob object closure.
    SelectedObjects,
}

/// Explicit deterministic work budget for negotiation checkpoints.
pub trait NegotiationWorkBudget: Send {
    /// Charge deterministic work units and return `false` when traversal must stop.
    fn charge(&mut self, units: u64) -> bool;
}

/// Counter-based work budget with no implicit time source.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NegotiationWorkLimit {
    remaining: u64,
    checkpoints: u64,
}

impl NegotiationWorkLimit {
    /// Construct an exact work-unit allowance.
    #[must_use]
    pub const fn new(units: u64) -> Self {
        Self {
            remaining: units,
            checkpoints: 0,
        }
    }

    /// Remaining deterministic work units.
    #[must_use]
    pub const fn remaining(&self) -> u64 {
        self.remaining
    }

    /// Number of charge checkpoints, including the rejecting checkpoint.
    #[must_use]
    pub const fn checkpoints(&self) -> u64 {
        self.checkpoints
    }
}

impl NegotiationWorkBudget for NegotiationWorkLimit {
    fn charge(&mut self, units: u64) -> bool {
        self.checkpoints = self.checkpoints.saturating_add(1);
        let Some(remaining) = self.remaining.checked_sub(units) else {
            return false;
        };
        self.remaining = remaining;
        true
    }
}

/// Hard bounded negotiation limits.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NegotiationLimits {
    /// Maximum distinct wants.
    pub max_wants: usize,
    /// Maximum distinct haves.
    pub max_haves: usize,
    /// Maximum commit metadata rows visited across both walks.
    pub max_visited_commits: usize,
    /// Maximum parent edges returned by one indexed commit lookup.
    pub max_commit_parents: usize,
    /// Maximum selected commit/tree/blob objects.
    pub max_selected_objects: usize,
    /// Maximum selected header-stripped payload bytes from metadata.
    pub max_selected_bytes: u64,
    /// Maximum direct tree entries returned by one indexed tree lookup.
    pub max_tree_entries: usize,
    /// Hash membership entries retained before migration to spill.
    pub spill_threshold_entries: usize,
    /// Maximum bytes estimated across all exact in-memory membership sets.
    pub max_membership_bytes: u64,
    /// Maximum accepted stable ordinal metadata value.
    pub max_stable_ordinal: u64,
}

impl Default for NegotiationLimits {
    fn default() -> Self {
        Self {
            max_wants: 65_536,
            max_haves: 262_144,
            max_visited_commits: 2_000_000,
            max_commit_parents: 1_000_000,
            max_selected_objects: 5_000_000,
            max_selected_bytes: 128 * 1024 * 1024 * 1024,
            max_tree_entries: 1_000_000,
            spill_threshold_entries: 250_000,
            max_membership_bytes: 128 * 1024 * 1024,
            max_stable_ordinal: 100_000_000,
        }
    }
}

impl NegotiationLimits {
    fn validate(self) -> Result<Self, NegotiationLimitKind> {
        if self.max_wants == 0
            || self.max_haves == 0
            || self.max_visited_commits == 0
            || self.max_commit_parents == 0
            || self.max_selected_objects == 0
            || self.max_selected_bytes == 0
            || self.max_tree_entries == 0
            || self.spill_threshold_entries == 0
            || self.max_membership_bytes == 0
            || self.max_stable_ordinal == 0
        {
            return Err(NegotiationLimitKind::Configuration);
        }
        Ok(self)
    }
}

/// Limit category that stopped planning before output.
#[derive(Clone, Copy, Debug, thiserror::Error, PartialEq, Eq)]
pub enum NegotiationLimitKind {
    /// Limits are zero or inconsistent.
    #[error("invalid negotiation limits")]
    Configuration,
    /// Want count exceeded its bound.
    #[error("negotiation want limit exceeded")]
    Wants,
    /// Have count exceeded its bound.
    #[error("negotiation have limit exceeded")]
    Haves,
    /// Commit visit count exceeded its bound.
    #[error("negotiation commit visit limit exceeded")]
    VisitedCommits,
    /// One indexed commit row exceeded its parent-edge bound.
    #[error("negotiation commit parent limit exceeded")]
    CommitParents,
    /// Selected object count exceeded its bound.
    #[error("negotiation selected object limit exceeded")]
    SelectedObjects,
    /// Selected payload-byte estimate exceeded its bound.
    #[error("negotiation selected byte limit exceeded")]
    SelectedBytes,
    /// One indexed tree row set exceeded its bound.
    #[error("negotiation tree entry limit exceeded")]
    TreeEntries,
    /// Membership state exceeded its in-memory bound without usable spill.
    #[error("negotiation membership limit exceeded")]
    Membership,
    /// Stable ordinal metadata exceeded its configured bound.
    #[error("negotiation stable ordinal limit exceeded")]
    StableOrdinal,
    /// Injected deterministic work budget was exhausted.
    #[error("negotiation work budget exhausted")]
    Work,
}

/// Typed pre-output negotiation failure.
#[derive(Debug, thiserror::Error)]
pub enum NegotiationError<ME, SE>
where
    ME: std::error::Error + 'static,
    SE: std::error::Error + 'static,
{
    /// Metadata backend rejected a read.
    #[error("negotiation metadata read failed")]
    Metadata(#[source] ME),
    /// Spill adapter rejected state migration/read/cleanup.
    #[error("negotiation spill operation failed")]
    Spill(#[source] SE),
    /// A hard request/traversal/memory/work limit was reached.
    #[error(transparent)]
    Limit(#[from] NegotiationLimitKind),
    /// Required indexed metadata was missing.
    #[error("required negotiation metadata is missing")]
    MissingMetadata,
    /// Indexed metadata disagreed with the requested ID, kind, generation, or ordinal epoch.
    #[error("indexed negotiation metadata is corrupt")]
    CorruptMetadata,
    /// Cancellation was observed at an explicit checkpoint.
    #[error("negotiation cancelled")]
    Cancelled,
}

/// Successful separated commit and object-closure plan.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NegotiationPlan {
    /// Accepted common commit haves in caller order after deduplication.
    pub common_haves: Vec<ObjectId>,
    /// Selected commits in deterministic generation-aware traversal order.
    pub commits: Vec<ObjectId>,
    /// Deterministic commit/tree/blob closure rooted in the selected commit wants.
    pub objects: Vec<ObjectId>,
    /// Exact selected payload-byte estimate from indexed metadata.
    pub selected_bytes: u64,
    /// Commit metadata reads performed across excluded and selected walks.
    pub commit_metadata_reads: u64,
    /// Tree metadata reads performed only after commit negotiation completes.
    pub tree_metadata_reads: u64,
    /// Whether at least one valid indexed generation was assigned to a queued parent.
    pub used_generations: bool,
    /// Whether any membership set migrated from hash state to spill.
    pub spilled: bool,
}

/// Plan a fetch in two phases using only indexed commit/tree metadata.
///
/// Have ancestry is materialized once into shared exclusion membership, so many haves never cause
/// repeated excluded-ancestor walks. Commit generations prioritize traversal when present; missing
/// generations safely fall back to stable insertion order. Blob payloads are never read.
///
/// # Errors
///
/// Returns typed metadata, corruption, spill, cancellation, work, count, byte, or memory failures
/// before any protocol output is emitted.
pub async fn negotiate_bounded<M, F, B, C>(
    source: &M,
    wants: &[ObjectId],
    haves: &[ObjectId],
    limits: NegotiationLimits,
    spill_factory: &mut F,
    budget: &mut B,
    cancellation: &C,
) -> Result<NegotiationPlan, NegotiationError<M::Error, F::Error>>
where
    M: NegotiationMetadataSource,
    F: NegotiationSpillFactory,
    B: NegotiationWorkBudget,
    C: CancellationProbe,
{
    let limits = limits.validate()?;
    if wants.len() > limits.max_wants {
        return Err(NegotiationLimitKind::Wants.into());
    }
    if haves.len() > limits.max_haves {
        return Err(NegotiationLimitKind::Haves.into());
    }
    validate_request_ids(wants, haves)?;
    let wants = dedupe_roots(
        wants,
        limits.max_wants,
        limits.max_membership_bytes,
        budget,
        cancellation,
    )?;
    let common_haves = dedupe_roots(
        haves,
        limits.max_haves,
        limits.max_membership_bytes,
        budget,
        cancellation,
    )?;
    let mut excluded = Membership::<F::Set>::new(NegotiationSpillRole::ExcludedCommits);
    let mut visited = Membership::<F::Set>::new(NegotiationSpillRole::VisitedCommits);
    let mut selected = Membership::<F::Set>::new(NegotiationSpillRole::SelectedObjects);
    let mut membership_memory = MembershipMemory::default();
    let mut commit_reads = 0_u64;
    let mut used_generations = false;

    let mut have_queue = BinaryHeap::new();
    let mut have_sequence = 0_u64;
    for oid in common_haves.iter().copied() {
        checkpoint(budget, cancellation)?;
        let sequence = take_sequence(&mut have_sequence)?;
        push_commit_queue(
            &mut have_queue,
            CommitQueueItem::new(oid, None, sequence),
            limits.max_visited_commits,
        )?;
    }
    while let Some(item) = have_queue.pop() {
        checkpoint(budget, cancellation)?;
        if excluded.contains(&item.oid, item.ordinal, limits, spill_factory)? {
            continue;
        }
        ensure_commit_read_available(commit_reads, limits)?;
        let commit = read_commit(source, item.oid, limits.max_commit_parents).await?;
        commit_reads = commit_reads
            .checked_add(1)
            .ok_or(NegotiationLimitKind::VisitedCommits)?;
        validate_commit(&commit, item, limits)?;
        if !excluded.insert(
            commit.oid,
            commit.ordinal,
            limits,
            spill_factory,
            &mut membership_memory,
            budget,
            cancellation,
        )? {
            continue;
        }
        if commit
            .parents
            .iter()
            .any(|parent| parent.generation.is_some())
        {
            used_generations = true;
        }
        for parent in commit.parents.iter().copied() {
            checkpoint(budget, cancellation)?;
            let sequence = take_sequence(&mut have_sequence)?;
            push_commit_queue(
                &mut have_queue,
                CommitQueueItem::parent(parent, sequence),
                limits.max_visited_commits,
            )?;
        }
    }

    let mut commit_queue = BinaryHeap::new();
    let mut commit_sequence = 0_u64;
    for oid in wants.iter().copied() {
        checkpoint(budget, cancellation)?;
        let sequence = take_sequence(&mut commit_sequence)?;
        push_commit_queue(
            &mut commit_queue,
            CommitQueueItem::new(oid, None, sequence),
            limits.max_visited_commits,
        )?;
    }
    let mut commit_rows = Vec::new();
    while let Some(item) = commit_queue.pop() {
        checkpoint(budget, cancellation)?;
        if excluded.contains(&item.oid, item.ordinal, limits, spill_factory)?
            || visited.contains(&item.oid, item.ordinal, limits, spill_factory)?
        {
            continue;
        }
        ensure_commit_read_available(commit_reads, limits)?;
        let commit = read_commit(source, item.oid, limits.max_commit_parents).await?;
        commit_reads = commit_reads
            .checked_add(1)
            .ok_or(NegotiationLimitKind::VisitedCommits)?;
        validate_commit(&commit, item, limits)?;
        if excluded.contains(&commit.oid, commit.ordinal, limits, spill_factory)? {
            continue;
        }
        if !visited.insert(
            commit.oid,
            commit.ordinal,
            limits,
            spill_factory,
            &mut membership_memory,
            budget,
            cancellation,
        )? {
            continue;
        }
        if commit
            .parents
            .iter()
            .any(|parent| parent.generation.is_some())
        {
            used_generations = true;
        }
        for parent in commit.parents.iter().copied() {
            checkpoint(budget, cancellation)?;
            let sequence = take_sequence(&mut commit_sequence)?;
            push_commit_queue(
                &mut commit_queue,
                CommitQueueItem::parent(parent, sequence),
                limits.max_visited_commits,
            )?;
        }
        commit_rows
            .try_reserve(1)
            .map_err(|_| NegotiationLimitKind::Membership)?;
        commit_rows.push(commit);
    }

    let mut objects = Vec::new();
    let mut selected_bytes = 0_u64;
    let mut tree_queue = VecDeque::new();
    for commit in &commit_rows {
        checkpoint(budget, cancellation)?;
        select_object(
            commit.oid,
            commit.ordinal,
            commit.size_bytes,
            &mut selected,
            &mut objects,
            &mut selected_bytes,
            limits,
            spill_factory,
            &mut membership_memory,
            budget,
            cancellation,
        )?;
        push_tree_queue(
            &mut tree_queue,
            (commit.tree, commit.tree_ordinal),
            limits.max_selected_objects,
        )?;
    }
    let mut tree_reads = 0_u64;
    while let Some((tree_oid, tree_ordinal)) = tree_queue.pop_front() {
        checkpoint(budget, cancellation)?;
        if selected.contains(&tree_oid, tree_ordinal, limits, spill_factory)? {
            continue;
        }
        if objects.len() >= limits.max_selected_objects {
            return Err(NegotiationLimitKind::SelectedObjects.into());
        }
        let tree = source
            .read_tree(&tree_oid, limits.max_tree_entries)
            .await
            .map_err(NegotiationError::Metadata)?
            .ok_or(NegotiationError::MissingMetadata)?;
        tree_reads = tree_reads
            .checked_add(1)
            .ok_or(NegotiationLimitKind::SelectedObjects)?;
        if tree.oid != tree_oid || tree.entries.len() > limits.max_tree_entries {
            return Err(if tree.entries.len() > limits.max_tree_entries {
                NegotiationLimitKind::TreeEntries.into()
            } else {
                NegotiationError::CorruptMetadata
            });
        }
        select_object(
            tree.oid,
            tree.ordinal,
            tree.size_bytes,
            &mut selected,
            &mut objects,
            &mut selected_bytes,
            limits,
            spill_factory,
            &mut membership_memory,
            budget,
            cancellation,
        )?;
        for entry in tree.entries {
            checkpoint(budget, cancellation)?;
            if entry.oid.is_zero() || entry.oid.algo() != tree_oid.algo() {
                return Err(NegotiationError::CorruptMetadata);
            }
            match (entry.mode, entry.kind) {
                (0o160000, ObjectKind::Commit) => continue,
                (0o040000, ObjectKind::Tree) => push_tree_queue(
                    &mut tree_queue,
                    (entry.oid, entry.ordinal),
                    limits.max_selected_objects,
                )?,
                (0o100644 | 0o100755 | 0o120000, ObjectKind::Blob) => select_object(
                    entry.oid,
                    entry.ordinal,
                    entry.size_bytes.ok_or(NegotiationError::CorruptMetadata)?,
                    &mut selected,
                    &mut objects,
                    &mut selected_bytes,
                    limits,
                    spill_factory,
                    &mut membership_memory,
                    budget,
                    cancellation,
                )?,
                _ => return Err(NegotiationError::CorruptMetadata),
            }
        }
    }
    let spilled = excluded.spilled() || visited.spilled() || selected.spilled();
    excluded.cleanup().map_err(NegotiationError::Spill)?;
    visited.cleanup().map_err(NegotiationError::Spill)?;
    selected.cleanup().map_err(NegotiationError::Spill)?;
    Ok(NegotiationPlan {
        common_haves,
        commits: commit_rows.iter().map(|commit| commit.oid).collect(),
        objects,
        selected_bytes,
        commit_metadata_reads: commit_reads,
        tree_metadata_reads: tree_reads,
        used_generations,
        spilled,
    })
}

fn validate_request_ids<ME, SE>(
    wants: &[ObjectId],
    haves: &[ObjectId],
) -> Result<(), NegotiationError<ME, SE>>
where
    ME: std::error::Error + 'static,
    SE: std::error::Error + 'static,
{
    let algorithm = wants.first().or_else(|| haves.first()).map(ObjectId::algo);
    if wants
        .iter()
        .chain(haves)
        .any(|oid| oid.is_zero() || Some(oid.algo()) != algorithm)
    {
        return Err(NegotiationError::CorruptMetadata);
    }
    Ok(())
}

fn dedupe_roots<ME, SE, B, C>(
    roots: &[ObjectId],
    maximum: usize,
    max_membership_bytes: u64,
    budget: &mut B,
    cancellation: &C,
) -> Result<Vec<ObjectId>, NegotiationError<ME, SE>>
where
    ME: std::error::Error + 'static,
    SE: std::error::Error + 'static,
    B: NegotiationWorkBudget,
    C: CancellationProbe,
{
    let estimated = roots
        .len()
        .min(maximum)
        .checked_mul(MEMBERSHIP_ENTRY_BYTES)
        .and_then(|bytes| u64::try_from(bytes).ok())
        .ok_or(NegotiationLimitKind::Membership)?;
    if estimated > max_membership_bytes {
        return Err(NegotiationLimitKind::Membership.into());
    }
    let mut seen = HashSet::new();
    let mut ordered = Vec::new();
    seen.try_reserve(roots.len().min(maximum))
        .map_err(|_| NegotiationLimitKind::Membership)?;
    ordered
        .try_reserve(roots.len().min(maximum))
        .map_err(|_| NegotiationLimitKind::Membership)?;
    for oid in roots {
        checkpoint(budget, cancellation)?;
        if seen.insert(*oid) {
            ordered.push(*oid);
        }
    }
    Ok(ordered)
}

async fn read_commit<M, SE>(
    source: &M,
    oid: ObjectId,
    max_parents: usize,
) -> Result<NegotiationCommit, NegotiationError<M::Error, SE>>
where
    M: NegotiationMetadataSource,
    SE: std::error::Error + 'static,
{
    source
        .read_commit(&oid, max_parents)
        .await
        .map_err(NegotiationError::Metadata)?
        .ok_or(NegotiationError::MissingMetadata)
}

fn validate_commit<ME, SE>(
    commit: &NegotiationCommit,
    queued: CommitQueueItem,
    limits: NegotiationLimits,
) -> Result<(), NegotiationError<ME, SE>>
where
    ME: std::error::Error + 'static,
    SE: std::error::Error + 'static,
{
    if commit.oid != queued.oid
        || commit.oid.is_zero()
        || commit.tree.is_zero()
        || commit.parents.len() > limits.max_commit_parents
        || queued
            .ordinal
            .is_some_and(|ordinal| commit.ordinal != Some(ordinal))
        || queued
            .indexed_generation
            .is_some_and(|generation| commit.generation != Some(generation))
        || commit.tree.algo() != queued.oid.algo()
        || commit.parents.iter().any(|parent| {
            parent.oid.is_zero()
                || parent.oid.algo() != queued.oid.algo()
                || parent.generation == Some(0)
                || commit
                    .generation
                    .zip(parent.generation)
                    .is_some_and(|(child, parent)| parent > child)
        })
        || commit.generation == Some(0)
    {
        return Err(NegotiationError::CorruptMetadata);
    }
    Ok(())
}

fn checkpoint<ME, SE, B, C>(
    budget: &mut B,
    cancellation: &C,
) -> Result<(), NegotiationError<ME, SE>>
where
    ME: std::error::Error + 'static,
    SE: std::error::Error + 'static,
    B: NegotiationWorkBudget,
    C: CancellationProbe,
{
    if cancellation.is_cancelled() {
        return Err(NegotiationError::Cancelled);
    }
    if !budget.charge(1) {
        return Err(NegotiationLimitKind::Work.into());
    }
    Ok(())
}

fn ensure_commit_read_available<ME, SE>(
    reads: u64,
    limits: NegotiationLimits,
) -> Result<(), NegotiationError<ME, SE>>
where
    ME: std::error::Error + 'static,
    SE: std::error::Error + 'static,
{
    if u64::try_from(limits.max_visited_commits)
        .ok()
        .is_none_or(|maximum| reads >= maximum)
    {
        return Err(NegotiationLimitKind::VisitedCommits.into());
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn select_object<ME, F, B, C>(
    oid: ObjectId,
    ordinal: Option<StableObjectOrdinal>,
    size: u64,
    membership: &mut Membership<F::Set>,
    objects: &mut Vec<ObjectId>,
    selected_bytes: &mut u64,
    limits: NegotiationLimits,
    factory: &mut F,
    membership_memory: &mut MembershipMemory,
    budget: &mut B,
    cancellation: &C,
) -> Result<(), NegotiationError<ME, F::Error>>
where
    ME: std::error::Error + 'static,
    F: NegotiationSpillFactory,
    B: NegotiationWorkBudget,
    C: CancellationProbe,
{
    if membership.contains(&oid, ordinal, limits, factory)? {
        return Ok(());
    }
    if objects.len() >= limits.max_selected_objects {
        return Err(NegotiationLimitKind::SelectedObjects.into());
    }
    let next_bytes = selected_bytes
        .checked_add(size)
        .filter(|bytes| *bytes <= limits.max_selected_bytes)
        .ok_or(NegotiationLimitKind::SelectedBytes)?;
    objects
        .try_reserve(1)
        .map_err(|_| NegotiationLimitKind::Membership)?;
    if !membership.insert(
        oid,
        ordinal,
        limits,
        factory,
        membership_memory,
        budget,
        cancellation,
    )? {
        return Ok(());
    }
    objects.push(oid);
    *selected_bytes = next_bytes;
    Ok(())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct CommitQueueItem {
    oid: ObjectId,
    generation: u32,
    indexed_generation: Option<u32>,
    ordinal: Option<StableObjectOrdinal>,
    sequence: u64,
}

impl CommitQueueItem {
    fn new(oid: ObjectId, generation: Option<u32>, sequence: u64) -> Self {
        Self {
            oid,
            generation: generation.unwrap_or_default(),
            indexed_generation: generation,
            ordinal: None,
            sequence,
        }
    }

    fn parent(parent: NegotiationCommitParent, sequence: u64) -> Self {
        Self {
            oid: parent.oid,
            generation: parent.generation.unwrap_or_default(),
            indexed_generation: parent.generation,
            ordinal: parent.ordinal,
            sequence,
        }
    }
}

impl Ord for CommitQueueItem {
    fn cmp(&self, other: &Self) -> Ordering {
        self.generation
            .cmp(&other.generation)
            .then_with(|| other.sequence.cmp(&self.sequence))
            .then_with(|| other.oid.cmp(&self.oid))
    }
}

impl PartialOrd for CommitQueueItem {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

const MEMBERSHIP_ENTRY_BYTES: usize = 64;

fn take_sequence(sequence: &mut u64) -> Result<u64, NegotiationLimitKind> {
    let current = *sequence;
    *sequence = sequence
        .checked_add(1)
        .ok_or(NegotiationLimitKind::VisitedCommits)?;
    Ok(current)
}

fn push_commit_queue(
    queue: &mut BinaryHeap<CommitQueueItem>,
    item: CommitQueueItem,
    maximum: usize,
) -> Result<(), NegotiationLimitKind> {
    if queue.len() >= maximum {
        return Err(NegotiationLimitKind::VisitedCommits);
    }
    queue
        .try_reserve(1)
        .map_err(|_| NegotiationLimitKind::Membership)?;
    queue.push(item);
    Ok(())
}

fn push_tree_queue(
    queue: &mut VecDeque<(ObjectId, Option<StableObjectOrdinal>)>,
    item: (ObjectId, Option<StableObjectOrdinal>),
    maximum: usize,
) -> Result<(), NegotiationLimitKind> {
    if queue.len() >= maximum {
        return Err(NegotiationLimitKind::SelectedObjects);
    }
    queue
        .try_reserve(1)
        .map_err(|_| NegotiationLimitKind::Membership)?;
    queue.push_back(item);
    Ok(())
}

#[derive(Default)]
struct MembershipMemory {
    estimated_bytes: u64,
}

impl MembershipMemory {
    fn can_reserve_entry(&self, limits: NegotiationLimits) -> bool {
        u64::try_from(MEMBERSHIP_ENTRY_BYTES)
            .ok()
            .and_then(|bytes| self.estimated_bytes.checked_add(bytes))
            .is_some_and(|bytes| bytes <= limits.max_membership_bytes)
    }

    fn reserve_entry(&mut self, limits: NegotiationLimits) -> Result<(), NegotiationLimitKind> {
        let bytes =
            u64::try_from(MEMBERSHIP_ENTRY_BYTES).map_err(|_| NegotiationLimitKind::Membership)?;
        self.estimated_bytes = self
            .estimated_bytes
            .checked_add(bytes)
            .filter(|total| *total <= limits.max_membership_bytes)
            .ok_or(NegotiationLimitKind::Membership)?;
        Ok(())
    }

    fn release_entries(&mut self, entries: usize) -> Result<(), NegotiationLimitKind> {
        let bytes = entries
            .checked_mul(MEMBERSHIP_ENTRY_BYTES)
            .and_then(|value| u64::try_from(value).ok())
            .ok_or(NegotiationLimitKind::Membership)?;
        self.estimated_bytes = self
            .estimated_bytes
            .checked_sub(bytes)
            .ok_or(NegotiationLimitKind::Membership)?;
        Ok(())
    }
}

struct Membership<S: NegotiationSpillSet> {
    role: NegotiationSpillRole,
    hash: HashSet<ObjectId>,
    ordinal_generation: Option<u64>,
    spill: Option<S>,
}

impl<S: NegotiationSpillSet> Membership<S> {
    fn new(role: NegotiationSpillRole) -> Self {
        Self {
            role,
            hash: HashSet::new(),
            ordinal_generation: None,
            spill: None,
        }
    }

    fn spilled(&self) -> bool {
        self.spill.is_some()
    }

    fn cleanup(&mut self) -> Result<(), S::Error> {
        if let Some(spill) = &mut self.spill {
            spill.clear()?;
        }
        self.spill = None;
        Ok(())
    }

    fn contains<ME, F>(
        &mut self,
        oid: &ObjectId,
        ordinal: Option<StableObjectOrdinal>,
        limits: NegotiationLimits,
        _factory: &mut F,
    ) -> Result<bool, NegotiationError<ME, F::Error>>
    where
        ME: std::error::Error + 'static,
        F: NegotiationSpillFactory<Set = S>,
        S: NegotiationSpillSet<Error = F::Error>,
    {
        self.validate_ordinal(ordinal, limits)?;
        if self.hash.contains(oid) {
            return Ok(true);
        }
        self.spill
            .as_mut()
            .map_or(Ok(false), |spill| spill.contains(oid))
            .map_err(NegotiationError::Spill)
    }

    #[allow(clippy::too_many_arguments)]
    fn insert<ME, F, B, C>(
        &mut self,
        oid: ObjectId,
        ordinal: Option<StableObjectOrdinal>,
        limits: NegotiationLimits,
        factory: &mut F,
        memory: &mut MembershipMemory,
        budget: &mut B,
        cancellation: &C,
    ) -> Result<bool, NegotiationError<ME, F::Error>>
    where
        ME: std::error::Error + 'static,
        F: NegotiationSpillFactory<Set = S>,
        S: NegotiationSpillSet<Error = F::Error>,
        B: NegotiationWorkBudget,
        C: CancellationProbe,
    {
        self.validate_ordinal(ordinal, limits)?;
        if self.hash.contains(&oid) {
            return Ok(false);
        }
        if let Some(spill) = &mut self.spill {
            return match spill.insert(oid) {
                Ok(true) => Ok(true),
                Ok(false) => Err(NegotiationError::CorruptMetadata),
                Err(error) => Err(NegotiationError::Spill(error)),
            };
        }
        if self.hash.len() >= limits.spill_threshold_entries || !memory.can_reserve_entry(limits) {
            return self.migrate_and_insert(oid, factory, memory, budget, cancellation);
        }
        memory.reserve_entry(limits)?;
        self.hash
            .try_reserve(1)
            .map_err(|_| NegotiationLimitKind::Membership)
            .inspect_err(|_| {
                let _ = memory.release_entries(1);
            })?;
        let inserted = self.hash.insert(oid);
        if !inserted {
            memory.release_entries(1)?;
        }
        Ok(inserted)
    }

    fn validate_ordinal<ME, SE>(
        &mut self,
        ordinal: Option<StableObjectOrdinal>,
        limits: NegotiationLimits,
    ) -> Result<(), NegotiationError<ME, SE>>
    where
        ME: std::error::Error + 'static,
        SE: std::error::Error + 'static,
    {
        let Some(ordinal) = ordinal else {
            return Ok(());
        };
        if ordinal.ordinal > limits.max_stable_ordinal {
            return Err(NegotiationLimitKind::StableOrdinal.into());
        }
        if self.ordinal_generation.is_none() {
            self.ordinal_generation = Some(ordinal.index_generation);
        }
        if self.ordinal_generation != Some(ordinal.index_generation) {
            return Err(NegotiationError::CorruptMetadata);
        }
        Ok(())
    }

    fn migrate_and_insert<ME, F, B, C>(
        &mut self,
        oid: ObjectId,
        factory: &mut F,
        memory: &mut MembershipMemory,
        budget: &mut B,
        cancellation: &C,
    ) -> Result<bool, NegotiationError<ME, F::Error>>
    where
        ME: std::error::Error + 'static,
        F: NegotiationSpillFactory<Set = S>,
        S: NegotiationSpillSet<Error = F::Error>,
        B: NegotiationWorkBudget,
        C: CancellationProbe,
    {
        let mut spill = factory.create(self.role).map_err(NegotiationError::Spill)?;
        for existing in &self.hash {
            if let Err(error) = checkpoint(budget, cancellation) {
                let _ = spill.clear();
                return Err(error);
            }
            match spill.insert(*existing) {
                Ok(true) => {}
                Ok(false) => {
                    let _ = spill.clear();
                    return Err(NegotiationError::CorruptMetadata);
                }
                Err(error) => {
                    let _ = spill.clear();
                    return Err(NegotiationError::Spill(error));
                }
            }
        }
        let inserted = match spill.insert(oid) {
            Ok(inserted) => inserted,
            Err(error) => {
                let _ = spill.clear();
                return Err(NegotiationError::Spill(error));
            }
        };
        if !inserted {
            let _ = spill.clear();
            return Err(NegotiationError::CorruptMetadata);
        }
        let entries = self.hash.len();
        if let Err(error) = memory.release_entries(entries) {
            let _ = spill.clear();
            return Err(error.into());
        }
        self.hash = HashSet::new();
        self.spill = Some(spill);
        Ok(true)
    }
}

impl<S: NegotiationSpillSet> Drop for Membership<S> {
    fn drop(&mut self) {
        if let Some(spill) = &mut self.spill {
            let _ = spill.clear();
        }
    }
}
