//! Bounded metadata-only preparation of validated receive-pack updates.
//!
//! Preparation verifies command preconditions and complete object connectivity without reading the
//! quarantined PACK again. The resulting value contains structural metadata and immutable binding
//! evidence, but never blob payloads, push-option contents, an ownership token, or publication
//! authority.

use std::cell::Cell;
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::fmt;
use std::mem::size_of;

use async_trait::async_trait;
use grit_lib::check_ref_format::{check_refname_format, RefNameOptions};
use grit_lib::objects::{HashAlgo, ObjectId, ObjectKind};
use sha1::{Digest as Sha1Digest, Sha1};
use sha2::{Digest as Sha2Digest, Sha256};
use time::OffsetDateTime;

use crate::ids::{RepositoryId, TenantId};
use crate::protocol::push_metrics::ReceivePackLimits;
use crate::protocol::push_pack_validation::{
    PushPackIndexRow, PushStructuralMetadata, PushValidationSummary, ValidatedPushPack,
};
use crate::protocol::push_quarantine::{
    QuarantineId, QuarantineIndexAttestation, QuarantineManifest, QuarantineSnapshot,
    QuarantineState,
};
use crate::protocol::receive_pack::{PushCommandKind, ReceivePackCapability, ReceivePackCommand};
use crate::protocol::receive_pack_stream::ReceivePackEnvelope;

const PREPARED_OBJECT_OVERHEAD: u64 = 128;
const PREPARED_COMMAND_OVERHEAD: u64 = 192;
const PREPARED_TREE_ENTRY_OVERHEAD: u64 = 64;
const MAX_TAG_PEEL_DEPTH_HARD: u32 = 16_384;

/// One repository ref captured at an explicit preflight generation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PushSnapshotRef {
    /// Full ref name.
    pub name: String,
    /// Resolved object ID at the snapshot generation.
    pub oid: ObjectId,
}

/// Explicit repository state against which command compare-and-swap checks are prepared.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PushRepositorySnapshot {
    /// Owning tenant.
    pub tenant: TenantId,
    /// Owning repository.
    pub repository: RepositoryId,
    /// Repository object format.
    pub hash_algo: HashAlgo,
    /// Monotonic repository/import generation observed by the caller.
    pub generation: u64,
    /// Resolved refs at that generation.
    pub refs: Vec<PushSnapshotRef>,
}

/// Policy for validating tree gitlink entries in the superproject closure.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PreparedGitlinkPolicy {
    /// Treat gitlinks as boundaries owned by another repository.
    Boundary,
    /// Require each gitlink target to resolve to commit metadata in this repository.
    RequireCommit,
}

/// Metadata link from a tree object supplied by the existing repository.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AuthorizedTreeLink {
    /// Canonical Git tree mode.
    pub mode: u32,
    /// Linked object ID.
    pub oid: ObjectId,
}

/// Canonical, payload-free metadata for one authorized existing object.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AuthorizedExistingObjectLinks {
    /// A blob has no object-closure dependencies.
    Blob,
    /// A tree's entries, excluding path bytes not needed for connectivity.
    Tree(Vec<AuthorizedTreeLink>),
    /// A commit's root tree and ordered parents.
    Commit {
        /// Root tree.
        tree: ObjectId,
        /// Ordered parents.
        parents: Vec<ObjectId>,
    },
    /// An annotated tag's declared target.
    Tag {
        /// Tagged object.
        target: ObjectId,
        /// Declared target kind.
        target_kind: ObjectKind,
    },
}

/// One canonical existing object returned by an authorized metadata provider.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AuthorizedExistingObjectMetadata {
    /// Canonical object ID whose payload was verified when metadata was installed.
    pub oid: ObjectId,
    /// Payload-free typed links.
    pub links: AuthorizedExistingObjectLinks,
}

/// Non-sensitive existing-metadata provider failure.
#[derive(Clone, Copy, Debug, thiserror::Error, PartialEq, Eq)]
#[error("authorized push metadata provider failed")]
pub struct AuthorizedPushMetadataError;

/// Repository-scoped metadata-only source for closure and ancestry validation.
#[async_trait]
pub trait AuthorizedPushMetadataProvider: Send {
    /// Read a bounded batch of canonical metadata.
    ///
    /// Returned rows may be in any order but must contain no unrequested or duplicate OID. Missing
    /// requested rows are treated as missing dependencies. Implementations must enforce
    /// `max_metadata_bytes` before materializing the response.
    async fn read_metadata(
        &mut self,
        tenant: &TenantId,
        repository: &RepositoryId,
        oids: &[ObjectId],
        max_metadata_bytes: u64,
    ) -> Result<Vec<AuthorizedExistingObjectMetadata>, AuthorizedPushMetadataError>;
}

/// Explicit preparation work checkpoint.
pub trait PreparedPushWork {
    /// Charge deterministic validation, traversal, and fingerprint work units.
    fn charge(&mut self, units: u64) -> bool;
}

/// Counter-backed preparation work allowance.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PreparedPushWorkLimit {
    remaining: u64,
}

impl PreparedPushWorkLimit {
    /// Construct an exact work-unit allowance.
    #[must_use]
    pub const fn new(units: u64) -> Self {
        Self { remaining: units }
    }

    /// Return remaining work units.
    #[must_use]
    pub const fn remaining(&self) -> u64 {
        self.remaining
    }
}

impl PreparedPushWork for PreparedPushWorkLimit {
    fn charge(&mut self, units: u64) -> bool {
        let Some(remaining) = self.remaining.checked_sub(units) else {
            return false;
        };
        self.remaining = remaining;
        true
    }
}

/// Caller-observed cancellation and monotonic time.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PreparedPushObservation {
    /// Explicit observation time.
    pub observed_at: OffsetDateTime,
    /// Explicit cancellation signal.
    pub cancelled: bool,
}

/// Runtime-neutral cancellation/time provider.
pub trait PreparedPushControl {
    /// Return the latest explicit observation.
    fn observe(&mut self) -> PreparedPushObservation;
}

/// Bounded preparation policy and explicit request times.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PreparedPushOptions {
    /// Shared receive-pack limits.
    pub limits: ReceivePackLimits,
    /// Maximum objects expanded while checking new-root closure.
    pub max_closure_objects: usize,
    /// Maximum commits expanded in new-root history.
    pub max_history_objects: usize,
    /// Maximum annotated-tag peel depth per root.
    pub max_tag_depth: u32,
    /// Maximum OIDs in one existing-metadata request.
    pub metadata_batch_size: usize,
    /// Gitlink validation policy.
    pub gitlink_policy: PreparedGitlinkPolicy,
    /// Explicit phase start.
    pub started_at: OffsetDateTime,
    /// Explicit phase deadline.
    pub deadline: OffsetDateTime,
}

impl PreparedPushOptions {
    /// Validate hard bounds and duration relationships.
    ///
    /// # Errors
    ///
    /// Returns [`PreparedPushRejectionReason::InvalidLimits`] for inconsistent values.
    pub fn validate(self) -> Result<Self, PreparedPushRejectionReason> {
        let limits = self
            .limits
            .validate()
            .map_err(|_| PreparedPushRejectionReason::InvalidLimits)?;
        let duration = time::Duration::try_from(limits.max_duration)
            .map_err(|_| PreparedPushRejectionReason::InvalidLimits)?;
        if self.max_closure_objects == 0
            || self.max_closure_objects > limits.max_objects
            || self.max_history_objects == 0
            || self.max_history_objects > limits.max_objects
            || self.max_tag_depth == 0
            || self.max_tag_depth > MAX_TAG_PEEL_DEPTH_HARD
            || self.metadata_batch_size == 0
            || self.metadata_batch_size > limits.max_objects
            || self.deadline <= self.started_at
            || self.deadline - self.started_at > duration
        {
            return Err(PreparedPushRejectionReason::InvalidLimits);
        }
        Ok(Self { limits, ..self })
    }
}

/// Stable preparation failure category suitable for a rejection audit record.
#[derive(Clone, Copy, Debug, thiserror::Error, PartialEq, Eq)]
pub enum PreparedPushRejectionReason {
    /// Limits are zero, inconsistent, or beyond hard ceilings.
    #[error("invalid prepared-push limits")]
    InvalidLimits,
    /// Command/capability/push-option envelope is invalid.
    #[error("invalid prepared-push command envelope")]
    InvalidEnvelope,
    /// Snapshot, quarantine, manifest, fence, or attestation bindings disagree.
    #[error("prepared-push input binding mismatch")]
    BindingMismatch,
    /// Ref compare-and-swap or namespace preconditions do not match the snapshot.
    #[error("prepared-push ref precondition failed")]
    RefPrecondition,
    /// A command target or reachable dependency is unavailable.
    #[error("prepared-push object closure is incomplete")]
    MissingObject,
    /// Metadata kind conflicts with the referencing commit, tree, tag, or index row.
    #[error("prepared-push object kind is invalid")]
    ObjectKind,
    /// Annotated-tag peeling is cyclic or exceeds its bound.
    #[error("prepared-push tag peel is invalid")]
    TagPeel,
    /// Closure, history, command, or metadata memory exceeded configured bounds.
    #[error("prepared-push bounded metadata limit exceeded")]
    MetadataLimit,
    /// Existing repository metadata source failed or returned invalid rows.
    #[error("prepared-push authorized metadata is invalid")]
    ExistingMetadata,
    /// Deterministic work allowance was exhausted.
    #[error("prepared-push work budget exhausted")]
    WorkBudget,
    /// Explicit cancellation was observed.
    #[error("prepared-push cancelled")]
    Cancelled,
    /// Explicit deadline elapsed or time moved backwards.
    #[error("prepared-push deadline elapsed")]
    Deadline,
    /// A bounded allocation failed.
    #[error("prepared-push bounded allocation failed")]
    Allocation,
}

/// Non-sensitive validation and rejection counts prepared without PACK re-decoding.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PreparedPushAuditSummary {
    /// Prior validated PACK summary.
    pub pack: PushValidationSummary,
    /// Requested ref commands.
    pub commands: u64,
    /// Non-delete command roots.
    pub roots: u64,
    /// Objects expanded for new-root closure.
    pub closure_objects: u64,
    /// Existing metadata objects loaded.
    pub existing_objects: u64,
    /// Existing metadata batch operations.
    pub metadata_operations: u64,
    /// Reachable commits introduced by this PACK.
    pub pushed_commits: u64,
}

/// Preparation failure carrying an audit summary that requires no PACK retry.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PreparedPushError {
    reason: PreparedPushRejectionReason,
    audit: PreparedPushAuditSummary,
}

impl PreparedPushError {
    /// Return the stable failure category.
    #[must_use]
    pub const fn reason(&self) -> PreparedPushRejectionReason {
        self.reason
    }

    /// Return the non-sensitive partial preparation summary.
    #[must_use]
    pub const fn audit_summary(&self) -> PreparedPushAuditSummary {
        self.audit
    }
}

impl fmt::Display for PreparedPushError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.reason.fmt(formatter)
    }
}

impl std::error::Error for PreparedPushError {}

/// Exact compare-and-swap evidence for one command.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PreparedRefPrecondition {
    refname: String,
    expected: Option<ObjectId>,
    snapshot_generation: u64,
}

impl PreparedRefPrecondition {
    /// Return the ref name.
    #[must_use]
    pub fn refname(&self) -> &str {
        &self.refname
    }

    /// Return the exact resolved snapshot value, or `None` for create.
    #[must_use]
    pub const fn expected(&self) -> Option<ObjectId> {
        self.expected
    }

    /// Return the snapshot generation to re-check before publication.
    #[must_use]
    pub const fn snapshot_generation(&self) -> u64 {
        self.snapshot_generation
    }
}

/// One command retained in original wire order.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PreparedPushCommand {
    ordinal: u32,
    command: ReceivePackCommand,
    precondition: PreparedRefPrecondition,
}

impl PreparedPushCommand {
    /// Return the original zero-based command ordinal.
    #[must_use]
    pub const fn ordinal(&self) -> u32 {
        self.ordinal
    }

    /// Borrow the validated command.
    #[must_use]
    pub const fn command(&self) -> &ReceivePackCommand {
        &self.command
    }

    /// Borrow its exact snapshot precondition.
    #[must_use]
    pub const fn precondition(&self) -> &PreparedRefPrecondition {
        &self.precondition
    }
}

/// Publication grouping that preserves atomic versus independent command semantics.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PreparedCommandGroup {
    atomic: bool,
    command_ordinals: Vec<u32>,
}

impl PreparedCommandGroup {
    /// Return whether every ordinal belongs to one all-or-none group.
    #[must_use]
    pub const fn is_atomic(&self) -> bool {
        self.atomic
    }

    /// Borrow original command ordinals in wire order.
    #[must_use]
    pub fn command_ordinals(&self) -> &[u32] {
        &self.command_ordinals
    }
}

/// Capability shape bound to the prepared result without retaining agent/extension strings.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PreparedCapabilitySummary {
    /// Total requested tokens, including duplicates.
    pub count: u32,
    /// Exact token-order fingerprint.
    pub fingerprint: ObjectId,
    /// Atomic update requested.
    pub atomic: bool,
    /// Push-options capability requested.
    pub push_options: bool,
    /// OFS_DELTA capability requested.
    pub ofs_delta: bool,
    /// Negotiated object format.
    pub object_format: HashAlgo,
}

/// Push-option shape retained without option contents or a content fingerprint.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PreparedPushOptionSummary {
    /// Option count in the validated envelope.
    pub count: u32,
    /// Aggregate option bytes.
    pub bytes: u64,
}

/// One retained tree entry for structural policy and indexing.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PreparedTreeEntry {
    mode: u32,
    name: Vec<u8>,
    oid: ObjectId,
}

impl PreparedTreeEntry {
    /// Return the canonical tree mode.
    #[must_use]
    pub const fn mode(&self) -> u32 {
        self.mode
    }

    /// Borrow the untrusted path component bytes.
    #[must_use]
    pub fn name(&self) -> &[u8] {
        &self.name
    }

    /// Return the linked object ID.
    #[must_use]
    pub const fn oid(&self) -> ObjectId {
        self.oid
    }
}

/// Payload-light structural metadata retained for policy/hooks and bulk indexing.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PreparedStructuralMetadata {
    /// Commit graph links, excluding identities and message payload.
    Commit {
        /// Root tree.
        tree: ObjectId,
        /// Ordered parents.
        parents: Vec<ObjectId>,
    },
    /// Ordered tree entries including bounded path components.
    Tree(Vec<PreparedTreeEntry>),
    /// Annotated-tag link, excluding tagger and message payload.
    Tag {
        /// Tagged object.
        target: ObjectId,
        /// Declared target kind.
        target_kind: ObjectKind,
    },
}

/// One newly pushed structural object retained without blob/message payloads.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PreparedStructuralObject {
    oid: ObjectId,
    metadata: PreparedStructuralMetadata,
}

impl PreparedStructuralObject {
    /// Return the canonical object ID.
    #[must_use]
    pub const fn oid(&self) -> ObjectId {
        self.oid
    }

    /// Borrow payload-light structural metadata.
    #[must_use]
    pub const fn metadata(&self) -> &PreparedStructuralMetadata {
        &self.metadata
    }
}

/// Command-specific fast-forward preflight context.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PreparedAncestryContext {
    /// Original command ordinal.
    pub command_ordinal: u32,
    /// Peeled old commit when the old target is commit-ish.
    pub old_commit: Option<ObjectId>,
    /// Peeled new commit when the new target is commit-ish.
    pub new_commit: Option<ObjectId>,
    /// Whether the old commit is known to be an ancestor of the new commit.
    pub old_is_ancestor: Option<bool>,
}

/// Immutable quarantine binding retained without its ownership capability.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PreparedQuarantineBinding {
    /// Opaque quarantine identity.
    pub id: QuarantineId,
    /// Validated/attested fencing generation.
    pub generation: u64,
    /// Immutable manifest.
    pub manifest: QuarantineManifest,
    /// Incremental index attestation.
    pub attestation: QuarantineIndexAttestation,
}

/// Immutable metadata plan ready for policy and guarded publication preflight.
#[derive(Clone)]
pub struct PreparedPush {
    tenant: TenantId,
    repository: RepositoryId,
    repository_generation: u64,
    hash_algo: HashAlgo,
    commands: Vec<PreparedPushCommand>,
    groups: Vec<PreparedCommandGroup>,
    capabilities: PreparedCapabilitySummary,
    push_options: PreparedPushOptionSummary,
    gitlink_policy: PreparedGitlinkPolicy,
    quarantine: PreparedQuarantineBinding,
    pack_index: Vec<PushPackIndexRow>,
    structural_objects: Vec<PreparedStructuralObject>,
    pushed_commits: Vec<ObjectId>,
    ancestry: Vec<PreparedAncestryContext>,
    audit: PreparedPushAuditSummary,
    fingerprint: ObjectId,
}

impl fmt::Debug for PreparedPush {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PreparedPush")
            .field("tenant", &self.tenant)
            .field("repository", &self.repository)
            .field("repository_generation", &self.repository_generation)
            .field("hash_algo", &self.hash_algo)
            .field("commands", &self.commands.len())
            .field("groups", &self.groups.len())
            .field("quarantine", &self.quarantine)
            .field("pack_objects", &self.pack_index.len())
            .field("structural_objects", &self.structural_objects.len())
            .field("pushed_commits", &self.pushed_commits.len())
            .field("fingerprint", &self.fingerprint)
            .finish()
    }
}

impl PreparedPush {
    /// Borrow the tenant scope.
    #[must_use]
    pub const fn tenant(&self) -> &TenantId {
        &self.tenant
    }

    /// Borrow the repository scope.
    #[must_use]
    pub const fn repository(&self) -> &RepositoryId {
        &self.repository
    }

    /// Return the repository generation used for all ref preconditions.
    #[must_use]
    pub const fn repository_generation(&self) -> u64 {
        self.repository_generation
    }

    /// Return the repository object format.
    #[must_use]
    pub const fn hash_algo(&self) -> HashAlgo {
        self.hash_algo
    }

    /// Borrow commands in original wire order.
    #[must_use]
    pub fn commands(&self) -> &[PreparedPushCommand] {
        &self.commands
    }

    /// Borrow exact atomic or independent publication groups.
    #[must_use]
    pub fn groups(&self) -> &[PreparedCommandGroup] {
        &self.groups
    }

    /// Return the bounded capability summary.
    #[must_use]
    pub const fn capabilities(&self) -> PreparedCapabilitySummary {
        self.capabilities
    }

    /// Return push-option counts without exposing option contents.
    #[must_use]
    pub const fn push_options(&self) -> PreparedPushOptionSummary {
        self.push_options
    }

    /// Return the gitlink closure policy used during preparation.
    #[must_use]
    pub const fn gitlink_policy(&self) -> PreparedGitlinkPolicy {
        self.gitlink_policy
    }

    /// Borrow the quarantine binding without an ownership token.
    #[must_use]
    pub const fn quarantine(&self) -> &PreparedQuarantineBinding {
        &self.quarantine
    }

    /// Borrow ordered validated pack index rows.
    #[must_use]
    pub fn pack_index(&self) -> &[PushPackIndexRow] {
        &self.pack_index
    }

    /// Borrow payload-light newly pushed structural metadata.
    #[must_use]
    pub fn structural_objects(&self) -> &[PreparedStructuralObject] {
        &self.structural_objects
    }

    /// Borrow reachable commits introduced by this PACK in index order.
    #[must_use]
    pub fn pushed_commits(&self) -> &[ObjectId] {
        &self.pushed_commits
    }

    /// Borrow command-specific ancestry evidence.
    #[must_use]
    pub fn ancestry(&self) -> &[PreparedAncestryContext] {
        &self.ancestry
    }

    /// Return non-sensitive audit counts.
    #[must_use]
    pub const fn audit_summary(&self) -> PreparedPushAuditSummary {
        self.audit
    }

    /// Return the deterministic checksum over prepared public metadata and bindings.
    #[must_use]
    pub const fn fingerprint(&self) -> ObjectId {
        self.fingerprint
    }
}

#[derive(Clone, Debug)]
enum ObjectLinks {
    Blob,
    Tree(Vec<AuthorizedTreeLink>),
    Commit {
        tree: ObjectId,
        parents: Vec<ObjectId>,
    },
    Tag {
        target: ObjectId,
        target_kind: ObjectKind,
    },
}

impl ObjectLinks {
    fn kind(&self) -> ObjectKind {
        match self {
            Self::Blob => ObjectKind::Blob,
            Self::Tree(_) => ObjectKind::Tree,
            Self::Commit { .. } => ObjectKind::Commit,
            Self::Tag { .. } => ObjectKind::Tag,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TraversalMode {
    Closure,
    PeelOnly,
}

#[derive(Clone, Copy, Debug)]
struct TraversalItem {
    oid: ObjectId,
    expected: Option<ObjectKind>,
    mode: TraversalMode,
    peel_root: Option<u32>,
    peel_depth: u32,
}

struct PreparationState {
    audit: PreparedPushAuditSummary,
    retained_bytes: u64,
    metadata_operations: u64,
    last_observed_at: Cell<Option<OffsetDateTime>>,
}

impl PreparationState {
    fn fail(&self, reason: PreparedPushRejectionReason) -> PreparedPushError {
        PreparedPushError {
            reason,
            audit: self.audit,
        }
    }

    fn retain(&mut self, bytes: u64, limit: u64) -> Result<(), PreparedPushError> {
        self.retained_bytes = self
            .retained_bytes
            .checked_add(bytes)
            .ok_or_else(|| self.fail(PreparedPushRejectionReason::MetadataLimit))?;
        if self.retained_bytes > limit {
            return Err(self.fail(PreparedPushRejectionReason::MetadataLimit));
        }
        Ok(())
    }
}

fn enqueue_traversal(
    queue: &mut VecDeque<TraversalItem>,
    item: TraversalItem,
    state: &mut PreparationState,
    memory_limit: u64,
) -> Result<(), PreparedPushError> {
    state.retain(
        u64::try_from(size_of::<TraversalItem>() + 32)
            .map_err(|_| state.fail(PreparedPushRejectionReason::MetadataLimit))?,
        memory_limit,
    )?;
    queue
        .try_reserve(1)
        .map_err(|_| state.fail(PreparedPushRejectionReason::Allocation))?;
    queue.push_back(item);
    Ok(())
}

/// Build an immutable, payload-light push plan from validated quarantine metadata.
///
/// `repository_snapshot` and `quarantine_snapshot` must be captured explicitly by the caller.
/// `existing` may expose only authorized canonical metadata and no object payloads. The function
/// performs no promotion, ref write, policy callback, audit write, or hidden clock read.
///
/// # Errors
///
/// Returns [`PreparedPushError`] with a non-sensitive audit summary for invalid bindings,
/// preconditions, missing dependencies, kind mismatches, exhausted bounds, cancellation, or a
/// provider failure.
#[allow(clippy::too_many_arguments)]
pub async fn prepare_validated_push<M, W, C>(
    envelope: ReceivePackEnvelope,
    validated_pack: ValidatedPushPack,
    quarantine_snapshot: QuarantineSnapshot,
    repository_snapshot: PushRepositorySnapshot,
    existing: &mut M,
    options: PreparedPushOptions,
    work: &mut W,
    control: &mut C,
) -> Result<PreparedPush, PreparedPushError>
where
    M: AuthorizedPushMetadataProvider,
    W: PreparedPushWork,
    C: PreparedPushControl,
{
    let mut state = PreparationState {
        audit: PreparedPushAuditSummary {
            pack: validated_pack.summary,
            commands: u64::try_from(envelope.commands.len()).unwrap_or(u64::MAX),
            roots: u64::try_from(
                envelope
                    .commands
                    .iter()
                    .filter(|command| !command.new_oid.is_zero())
                    .count(),
            )
            .unwrap_or(u64::MAX),
            ..PreparedPushAuditSummary::default()
        },
        retained_bytes: 0,
        metadata_operations: 0,
        last_observed_at: Cell::new(None),
    };
    let options = options.validate().map_err(|reason| state.fail(reason))?;
    checkpoint(&options, work, control, 1, &state)?;
    envelope
        .validate(options.limits)
        .map_err(|_| state.fail(PreparedPushRejectionReason::InvalidEnvelope))?;

    validate_bindings(
        &envelope,
        &validated_pack,
        &quarantine_snapshot,
        &repository_snapshot,
        &state,
    )?;
    let snapshot_refs = validate_snapshot_and_commands(
        &envelope,
        &repository_snapshot,
        options.limits,
        &mut state,
    )?;
    checkpoint(
        &options,
        work,
        control,
        u64::try_from(envelope.commands.len()).unwrap_or(u64::MAX),
        &state,
    )?;

    let (new_links, structural_objects) = build_new_object_metadata(
        &validated_pack,
        repository_snapshot.hash_algo,
        options.limits.max_prepared_memory_bytes,
        &mut state,
    )?;
    let mut known = new_links;
    let mut external_oids = HashSet::new();
    external_oids
        .try_reserve(options.metadata_batch_size)
        .map_err(|_| state.fail(PreparedPushRejectionReason::Allocation))?;
    let mut missing_external = HashSet::new();
    missing_external
        .try_reserve(options.metadata_batch_size)
        .map_err(|_| state.fail(PreparedPushRejectionReason::Allocation))?;
    let mut closure_expanded = HashSet::new();
    closure_expanded
        .try_reserve(validated_pack.index.len().min(options.max_closure_objects))
        .map_err(|_| state.fail(PreparedPushRejectionReason::Allocation))?;
    let mut queue = VecDeque::new();
    for (ordinal, command) in envelope.commands.iter().enumerate() {
        if !command.new_oid.is_zero() {
            enqueue_traversal(
                &mut queue,
                TraversalItem {
                    oid: command.new_oid,
                    expected: None,
                    mode: TraversalMode::Closure,
                    peel_root: None,
                    peel_depth: 0,
                },
                &mut state,
                options.limits.max_prepared_memory_bytes,
            )?;
        }
        if !command.old_oid.is_zero() {
            let root = u32::try_from(ordinal)
                .map_err(|_| state.fail(PreparedPushRejectionReason::MetadataLimit))?;
            enqueue_traversal(
                &mut queue,
                TraversalItem {
                    oid: command.old_oid,
                    expected: None,
                    mode: TraversalMode::PeelOnly,
                    peel_root: Some(root),
                    peel_depth: 0,
                },
                &mut state,
                options.limits.max_prepared_memory_bytes,
            )?;
        }
    }

    let mut peel_seen = HashSet::new();
    peel_seen
        .try_reserve(envelope.commands.len().min(options.max_closure_objects))
        .map_err(|_| state.fail(PreparedPushRejectionReason::Allocation))?;

    let mut history_objects = 0_usize;
    while let Some(item) = queue.pop_front() {
        checkpoint(&options, work, control, 1, &state)?;
        if !known.contains_key(&item.oid) {
            if missing_external.contains(&item.oid) {
                return Err(state.fail(PreparedPushRejectionReason::MissingObject));
            }
            load_existing_batch(
                item,
                &mut queue,
                &mut known,
                &mut external_oids,
                &mut missing_external,
                &repository_snapshot,
                existing,
                &options,
                &mut state,
                work,
                control,
            )
            .await?;
        }
        let links = known
            .get(&item.oid)
            .ok_or_else(|| state.fail(PreparedPushRejectionReason::MissingObject))?;
        if item
            .expected
            .is_some_and(|expected| expected != links.kind())
        {
            return Err(state.fail(PreparedPushRejectionReason::ObjectKind));
        }
        if item.mode == TraversalMode::PeelOnly {
            let peel_root = item
                .peel_root
                .ok_or_else(|| state.fail(PreparedPushRejectionReason::TagPeel))?;
            peel_seen
                .try_reserve(1)
                .map_err(|_| state.fail(PreparedPushRejectionReason::Allocation))?;
            state.retain(64, options.limits.max_prepared_memory_bytes)?;
            if !peel_seen.insert((peel_root, item.oid)) {
                return Err(state.fail(PreparedPushRejectionReason::TagPeel));
            }
            if let ObjectLinks::Tag {
                target,
                target_kind,
            } = links
            {
                if item.peel_depth >= options.max_tag_depth {
                    return Err(state.fail(PreparedPushRejectionReason::TagPeel));
                }
                let next_depth = item
                    .peel_depth
                    .checked_add(1)
                    .ok_or_else(|| state.fail(PreparedPushRejectionReason::TagPeel))?;
                enqueue_traversal(
                    &mut queue,
                    TraversalItem {
                        oid: *target,
                        expected: Some(*target_kind),
                        mode: TraversalMode::PeelOnly,
                        peel_root: Some(peel_root),
                        peel_depth: next_depth,
                    },
                    &mut state,
                    options.limits.max_prepared_memory_bytes,
                )?;
            }
            continue;
        }
        closure_expanded
            .try_reserve(1)
            .map_err(|_| state.fail(PreparedPushRejectionReason::Allocation))?;
        state.retain(64, options.limits.max_prepared_memory_bytes)?;
        if !closure_expanded.insert(item.oid) {
            continue;
        }
        if closure_expanded.len() > options.max_closure_objects {
            return Err(state.fail(PreparedPushRejectionReason::MetadataLimit));
        }
        state.audit.closure_objects = u64::try_from(closure_expanded.len())
            .map_err(|_| state.fail(PreparedPushRejectionReason::MetadataLimit))?;
        match links {
            ObjectLinks::Blob => {}
            ObjectLinks::Tree(entries) => {
                for entry in entries {
                    let expected = expected_tree_kind(entry.mode, options.gitlink_policy)
                        .map_err(|reason| state.fail(reason))?;
                    if let Some(expected) = expected {
                        enqueue_traversal(
                            &mut queue,
                            TraversalItem {
                                oid: entry.oid,
                                expected: Some(expected),
                                mode: TraversalMode::Closure,
                                peel_root: None,
                                peel_depth: 0,
                            },
                            &mut state,
                            options.limits.max_prepared_memory_bytes,
                        )?;
                    }
                }
            }
            ObjectLinks::Commit { tree, parents } => {
                history_objects = history_objects
                    .checked_add(1)
                    .ok_or_else(|| state.fail(PreparedPushRejectionReason::MetadataLimit))?;
                if history_objects > options.max_history_objects {
                    return Err(state.fail(PreparedPushRejectionReason::MetadataLimit));
                }
                enqueue_traversal(
                    &mut queue,
                    TraversalItem {
                        oid: *tree,
                        expected: Some(ObjectKind::Tree),
                        mode: TraversalMode::Closure,
                        peel_root: None,
                        peel_depth: 0,
                    },
                    &mut state,
                    options.limits.max_prepared_memory_bytes,
                )?;
                for parent in parents {
                    enqueue_traversal(
                        &mut queue,
                        TraversalItem {
                            oid: *parent,
                            expected: Some(ObjectKind::Commit),
                            mode: TraversalMode::Closure,
                            peel_root: None,
                            peel_depth: 0,
                        },
                        &mut state,
                        options.limits.max_prepared_memory_bytes,
                    )?;
                }
            }
            ObjectLinks::Tag {
                target,
                target_kind,
            } => enqueue_traversal(
                &mut queue,
                TraversalItem {
                    oid: *target,
                    expected: Some(*target_kind),
                    mode: TraversalMode::Closure,
                    peel_root: None,
                    peel_depth: 0,
                },
                &mut state,
                options.limits.max_prepared_memory_bytes,
            )?,
        }
    }

    state.audit.existing_objects = u64::try_from(external_oids.len())
        .map_err(|_| state.fail(PreparedPushRejectionReason::MetadataLimit))?;
    state.audit.metadata_operations = state.metadata_operations;
    let mut pushed_commits = Vec::new();
    pushed_commits
        .try_reserve(validated_pack.index.len().min(options.max_history_objects))
        .map_err(|_| state.fail(PreparedPushRejectionReason::Allocation))?;
    for row in &validated_pack.index {
        if row.kind == ObjectKind::Commit && closure_expanded.contains(&row.oid) {
            pushed_commits.push(row.oid);
        }
    }
    state.audit.pushed_commits = u64::try_from(pushed_commits.len())
        .map_err(|_| state.fail(PreparedPushRejectionReason::MetadataLimit))?;
    state.retain(
        u64::try_from(pushed_commits.len())
            .ok()
            .and_then(|count| count.checked_mul(size_of::<ObjectId>() as u64))
            .ok_or_else(|| state.fail(PreparedPushRejectionReason::MetadataLimit))?,
        options.limits.max_prepared_memory_bytes,
    )?;

    let ancestry = build_ancestry(&envelope.commands, &known, &options, &state, work, control)?;
    state.retain(
        u64::try_from(ancestry.len())
            .ok()
            .and_then(|count| count.checked_mul(size_of::<PreparedAncestryContext>() as u64))
            .ok_or_else(|| state.fail(PreparedPushRejectionReason::MetadataLimit))?,
        options.limits.max_prepared_memory_bytes,
    )?;
    let commands = prepare_commands(
        &envelope.commands,
        &snapshot_refs,
        repository_snapshot.generation,
        &state,
    )?;
    let atomic = envelope
        .capabilities
        .contains(&ReceivePackCapability::Atomic);
    let groups = prepare_groups(commands.len(), atomic, &state)?;
    state.retain(
        u64::try_from(groups.len())
            .ok()
            .and_then(|count| count.checked_mul(size_of::<PreparedCommandGroup>() as u64))
            .and_then(|bytes| {
                u64::try_from(commands.len())
                    .ok()?
                    .checked_mul(size_of::<u32>() as u64)?
                    .checked_add(bytes)
            })
            .ok_or_else(|| state.fail(PreparedPushRejectionReason::MetadataLimit))?,
        options.limits.max_prepared_memory_bytes,
    )?;
    let capabilities = capability_summary(&envelope.capabilities, repository_snapshot.hash_algo)
        .map_err(|reason| state.fail(reason))?;
    let push_options =
        push_option_summary(&envelope.push_options).map_err(|reason| state.fail(reason))?;
    let manifest = quarantine_snapshot
        .manifest
        .clone()
        .ok_or_else(|| state.fail(PreparedPushRejectionReason::BindingMismatch))?;
    let quarantine = PreparedQuarantineBinding {
        id: quarantine_snapshot.id,
        generation: validated_pack.fence.generation(),
        manifest,
        attestation: validated_pack.attestation,
    };
    let fingerprint = prepared_fingerprint(
        &repository_snapshot,
        &commands,
        &groups,
        capabilities,
        push_options,
        options.gitlink_policy,
        &quarantine,
        &structural_objects,
        &pushed_commits,
        &ancestry,
    )
    .map_err(|reason| state.fail(reason))?;
    checkpoint(&options, work, control, state.retained_bytes, &state)?;

    Ok(PreparedPush {
        tenant: repository_snapshot.tenant,
        repository: repository_snapshot.repository,
        repository_generation: repository_snapshot.generation,
        hash_algo: repository_snapshot.hash_algo,
        commands,
        groups,
        capabilities,
        push_options,
        gitlink_policy: options.gitlink_policy,
        quarantine,
        pack_index: validated_pack.index,
        structural_objects,
        pushed_commits,
        ancestry,
        audit: state.audit,
        fingerprint,
    })
}

fn checkpoint<W, C>(
    options: &PreparedPushOptions,
    work: &mut W,
    control: &mut C,
    units: u64,
    state: &PreparationState,
) -> Result<(), PreparedPushError>
where
    W: PreparedPushWork,
    C: PreparedPushControl,
{
    let observation = control.observe();
    if observation.cancelled {
        return Err(state.fail(PreparedPushRejectionReason::Cancelled));
    }
    if observation.observed_at < options.started_at
        || observation.observed_at >= options.deadline
        || state
            .last_observed_at
            .get()
            .is_some_and(|previous| observation.observed_at < previous)
    {
        return Err(state.fail(PreparedPushRejectionReason::Deadline));
    }
    state.last_observed_at.set(Some(observation.observed_at));
    if !work.charge(units) {
        return Err(state.fail(PreparedPushRejectionReason::WorkBudget));
    }
    Ok(())
}

fn validate_bindings(
    envelope: &ReceivePackEnvelope,
    validated: &ValidatedPushPack,
    quarantine: &QuarantineSnapshot,
    repository: &PushRepositorySnapshot,
    state: &PreparationState,
) -> Result<(), PreparedPushError> {
    let manifest = quarantine
        .manifest
        .as_ref()
        .ok_or_else(|| state.fail(PreparedPushRejectionReason::BindingMismatch))?;
    let count = u32::try_from(validated.index.len())
        .map_err(|_| state.fail(PreparedPushRejectionReason::BindingMismatch))?;
    let summary_objects = u64::try_from(validated.index.len())
        .map_err(|_| state.fail(PreparedPushRejectionReason::BindingMismatch))?;
    let capability_format_matches = envelope.capabilities.iter().all(|capability| {
        !matches!(capability, ReceivePackCapability::ObjectFormat(algo) if *algo != repository.hash_algo)
    });
    let command_formats_match = envelope.commands.iter().all(|command| {
        command.old_oid.algo() == repository.hash_algo
            && command.new_oid.algo() == repository.hash_algo
    });
    if repository.generation == 0
        || quarantine.state != QuarantineState::Validated
        || quarantine.tenant != repository.tenant
        || quarantine.repository != repository.repository
        || quarantine.id != validated.fence.id()
        || quarantine.generation != validated.fence.generation()
        || manifest.hash_algo != repository.hash_algo
        || manifest.pack_checksum != Some(validated.attestation.pack_checksum())
        || manifest.index_checksum != Some(validated.attestation.index_checksum())
        || manifest.object_count != count
        || validated.attestation.object_count() != count
        || !validated.attestation.validation_complete()
        || manifest.self_contained != validated.attestation.self_contained()
        || validated.summary.objects != summary_objects
        || !capability_format_matches
        || !command_formats_match
    {
        return Err(state.fail(PreparedPushRejectionReason::BindingMismatch));
    }
    Ok(())
}

fn validate_snapshot_and_commands(
    envelope: &ReceivePackEnvelope,
    repository: &PushRepositorySnapshot,
    limits: ReceivePackLimits,
    state: &mut PreparationState,
) -> Result<BTreeMap<String, ObjectId>, PreparedPushError> {
    if envelope.commands.is_empty() || envelope.commands.len() > limits.max_commands {
        return Err(state.fail(PreparedPushRejectionReason::InvalidEnvelope));
    }
    let mut refs = BTreeMap::new();
    for snapshot_ref in &repository.refs {
        validate_refname(&snapshot_ref.name, state)?;
        if snapshot_ref.oid.is_zero() || snapshot_ref.oid.algo() != repository.hash_algo {
            return Err(state.fail(PreparedPushRejectionReason::BindingMismatch));
        }
        if refs
            .insert(snapshot_ref.name.clone(), snapshot_ref.oid)
            .is_some()
        {
            return Err(state.fail(PreparedPushRejectionReason::BindingMismatch));
        }
        state.retain(
            PREPARED_OBJECT_OVERHEAD
                .checked_add(
                    u64::try_from(snapshot_ref.name.len())
                        .map_err(|_| state.fail(PreparedPushRejectionReason::MetadataLimit))?,
                )
                .ok_or_else(|| state.fail(PreparedPushRejectionReason::MetadataLimit))?,
            limits.max_prepared_memory_bytes,
        )?;
    }
    validate_ref_namespace(refs.keys().map(String::as_str), state)?;

    let mut seen = HashSet::new();
    seen.try_reserve(envelope.commands.len())
        .map_err(|_| state.fail(PreparedPushRejectionReason::Allocation))?;
    let delete_allowed = envelope
        .capabilities
        .contains(&ReceivePackCapability::DeleteRefs);
    let atomic = envelope
        .capabilities
        .contains(&ReceivePackCapability::Atomic);
    let mut final_refs = refs.clone();
    for command in &envelope.commands {
        validate_refname(&command.refname, state)?;
        if (command.old_oid.is_zero() && command.new_oid.is_zero())
            || !seen.insert(command.refname.clone())
            || command.old_oid.algo() != repository.hash_algo
            || command.new_oid.algo() != repository.hash_algo
            || (command.kind() == PushCommandKind::Delete && !delete_allowed)
        {
            return Err(state.fail(PreparedPushRejectionReason::InvalidEnvelope));
        }
        let current = refs.get(&command.refname).copied();
        match command.kind() {
            PushCommandKind::Create if current.is_none() => {
                final_refs.insert(command.refname.clone(), command.new_oid);
            }
            PushCommandKind::Update if current == Some(command.old_oid) => {
                final_refs.insert(command.refname.clone(), command.new_oid);
            }
            PushCommandKind::Delete if current == Some(command.old_oid) => {
                final_refs.remove(&command.refname);
            }
            PushCommandKind::Create | PushCommandKind::Update | PushCommandKind::Delete => {
                return Err(state.fail(PreparedPushRejectionReason::RefPrecondition));
            }
        }
        if !atomic
            && !command.new_oid.is_zero()
            && refs.keys().any(|name| {
                name != &command.refname && ref_namespace_conflicts(name, &command.refname)
            })
        {
            return Err(state.fail(PreparedPushRejectionReason::RefPrecondition));
        }
        state.retain(
            PREPARED_COMMAND_OVERHEAD
                .checked_add(
                    u64::try_from(command.refname.len())
                        .map_err(|_| state.fail(PreparedPushRejectionReason::MetadataLimit))?,
                )
                .ok_or_else(|| state.fail(PreparedPushRejectionReason::MetadataLimit))?,
            limits.max_prepared_memory_bytes,
        )?;
    }
    if atomic {
        validate_ref_namespace(final_refs.keys().map(String::as_str), state)?;
    }
    Ok(refs)
}

fn validate_ref_namespace<'a>(
    refnames: impl Iterator<Item = &'a str>,
    state: &PreparationState,
) -> Result<(), PreparedPushError> {
    let mut prior = None;
    for refname in refnames {
        if prior.is_some_and(|left| ref_namespace_conflicts(left, refname)) {
            return Err(state.fail(PreparedPushRejectionReason::RefPrecondition));
        }
        prior = Some(refname);
    }
    Ok(())
}

fn validate_refname(refname: &str, state: &PreparationState) -> Result<(), PreparedPushError> {
    if !refname.starts_with("refs/")
        || check_refname_format(refname, &RefNameOptions::default()).is_err()
    {
        return Err(state.fail(PreparedPushRejectionReason::InvalidEnvelope));
    }
    Ok(())
}

fn ref_namespace_conflicts(left: &str, right: &str) -> bool {
    left == right
        || left
            .strip_prefix(right)
            .is_some_and(|suffix| suffix.starts_with('/'))
        || right
            .strip_prefix(left)
            .is_some_and(|suffix| suffix.starts_with('/'))
}

fn build_new_object_metadata(
    validated: &ValidatedPushPack,
    hash_algo: HashAlgo,
    memory_limit: u64,
    state: &mut PreparationState,
) -> Result<
    (
        HashMap<ObjectId, ObjectLinks>,
        Vec<PreparedStructuralObject>,
    ),
    PreparedPushError,
> {
    state.retain(
        u64::try_from(validated.index.len())
            .ok()
            .and_then(|count| count.checked_mul(size_of::<PushPackIndexRow>() as u64))
            .and_then(|bytes| bytes.checked_add(PREPARED_OBJECT_OVERHEAD))
            .ok_or_else(|| state.fail(PreparedPushRejectionReason::MetadataLimit))?,
        memory_limit,
    )?;
    state.retain(
        u64::try_from(validated.index.len())
            .ok()
            .and_then(|count| count.checked_mul(size_of::<(ObjectId, ObjectKind)>() as u64 + 64))
            .ok_or_else(|| state.fail(PreparedPushRejectionReason::MetadataLimit))?,
        memory_limit,
    )?;
    let mut links = HashMap::new();
    links
        .try_reserve(validated.index.len())
        .map_err(|_| state.fail(PreparedPushRejectionReason::Allocation))?;
    let mut index_kinds = HashMap::new();
    index_kinds
        .try_reserve(validated.index.len())
        .map_err(|_| state.fail(PreparedPushRejectionReason::Allocation))?;
    for row in &validated.index {
        if row.oid.algo() != hash_algo
            || row.oid.is_zero()
            || index_kinds.insert(row.oid, row.kind).is_some()
        {
            return Err(state.fail(PreparedPushRejectionReason::BindingMismatch));
        }
        if row.kind == ObjectKind::Blob {
            links.insert(row.oid, ObjectLinks::Blob);
        }
    }
    let mut prepared = Vec::new();
    prepared
        .try_reserve(validated.structural_objects.len())
        .map_err(|_| state.fail(PreparedPushRejectionReason::Allocation))?;
    for structural in &validated.structural_objects {
        if structural.oid.algo() != hash_algo {
            return Err(state.fail(PreparedPushRejectionReason::BindingMismatch));
        }
        let index_kind = index_kinds
            .get(&structural.oid)
            .copied()
            .ok_or_else(|| state.fail(PreparedPushRejectionReason::BindingMismatch))?;
        let (object_links, metadata, retained) = match &structural.metadata {
            PushStructuralMetadata::Commit(commit) => {
                validate_oids(
                    std::iter::once(commit.tree).chain(commit.parents.iter().copied()),
                    hash_algo,
                    state,
                )?;
                let parents = commit.parents.clone();
                let retained = PREPARED_OBJECT_OVERHEAD
                    .checked_add(
                        u64::try_from(parents.len())
                            .ok()
                            .and_then(|count| count.checked_mul(size_of::<ObjectId>() as u64))
                            .and_then(|bytes| bytes.checked_mul(2))
                            .ok_or_else(|| {
                                state.fail(PreparedPushRejectionReason::MetadataLimit)
                            })?,
                    )
                    .ok_or_else(|| state.fail(PreparedPushRejectionReason::MetadataLimit))?;
                (
                    ObjectLinks::Commit {
                        tree: commit.tree,
                        parents: parents.clone(),
                    },
                    PreparedStructuralMetadata::Commit {
                        tree: commit.tree,
                        parents,
                    },
                    retained,
                )
            }
            PushStructuralMetadata::Tree(tree) => {
                let mut authorized = Vec::new();
                let mut entries = Vec::new();
                authorized
                    .try_reserve(tree.len())
                    .map_err(|_| state.fail(PreparedPushRejectionReason::Allocation))?;
                entries
                    .try_reserve(tree.len())
                    .map_err(|_| state.fail(PreparedPushRejectionReason::Allocation))?;
                let mut retained = PREPARED_OBJECT_OVERHEAD;
                for entry in tree {
                    if entry.oid.algo() != hash_algo {
                        return Err(state.fail(PreparedPushRejectionReason::ObjectKind));
                    }
                    retained = retained
                        .checked_add(PREPARED_TREE_ENTRY_OVERHEAD)
                        .and_then(|value| {
                            u64::try_from(entry.name.len())
                                .ok()
                                .and_then(|name| value.checked_add(name))
                        })
                        .ok_or_else(|| state.fail(PreparedPushRejectionReason::MetadataLimit))?;
                    authorized.push(AuthorizedTreeLink {
                        mode: entry.mode,
                        oid: entry.oid,
                    });
                    entries.push(PreparedTreeEntry {
                        mode: entry.mode,
                        name: entry.name.clone(),
                        oid: entry.oid,
                    });
                }
                (
                    ObjectLinks::Tree(authorized),
                    PreparedStructuralMetadata::Tree(entries),
                    retained,
                )
            }
            PushStructuralMetadata::Tag(tag) => {
                let target_kind = ObjectKind::from_tag_type_field(tag.object_type.as_bytes())
                    .ok_or_else(|| state.fail(PreparedPushRejectionReason::ObjectKind))?;
                if tag.object.algo() != hash_algo {
                    return Err(state.fail(PreparedPushRejectionReason::ObjectKind));
                }
                (
                    ObjectLinks::Tag {
                        target: tag.object,
                        target_kind,
                    },
                    PreparedStructuralMetadata::Tag {
                        target: tag.object,
                        target_kind,
                    },
                    PREPARED_OBJECT_OVERHEAD,
                )
            }
        };
        if object_links.kind() != index_kind || links.insert(structural.oid, object_links).is_some()
        {
            return Err(state.fail(PreparedPushRejectionReason::BindingMismatch));
        }
        state.retain(retained, memory_limit)?;
        prepared.push(PreparedStructuralObject {
            oid: structural.oid,
            metadata,
        });
    }
    if links.len() != validated.index.len() {
        return Err(state.fail(PreparedPushRejectionReason::BindingMismatch));
    }
    Ok((links, prepared))
}

fn validate_oids(
    oids: impl Iterator<Item = ObjectId>,
    hash_algo: HashAlgo,
    state: &PreparationState,
) -> Result<(), PreparedPushError> {
    if oids.into_iter().any(|oid| oid.algo() != hash_algo) {
        return Err(state.fail(PreparedPushRejectionReason::ObjectKind));
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn load_existing_batch<M, W, C>(
    required: TraversalItem,
    queue: &mut VecDeque<TraversalItem>,
    known: &mut HashMap<ObjectId, ObjectLinks>,
    external_oids: &mut HashSet<ObjectId>,
    missing_external: &mut HashSet<ObjectId>,
    repository: &PushRepositorySnapshot,
    provider: &mut M,
    options: &PreparedPushOptions,
    state: &mut PreparationState,
    work: &mut W,
    control: &mut C,
) -> Result<(), PreparedPushError>
where
    M: AuthorizedPushMetadataProvider,
    W: PreparedPushWork,
    C: PreparedPushControl,
{
    let mut requested = Vec::new();
    requested
        .try_reserve(options.metadata_batch_size)
        .map_err(|_| state.fail(PreparedPushRejectionReason::Allocation))?;
    let mut unique = HashSet::new();
    unique
        .try_reserve(options.metadata_batch_size)
        .map_err(|_| state.fail(PreparedPushRejectionReason::Allocation))?;
    if required.oid.algo() != repository.hash_algo {
        return Err(state.fail(PreparedPushRejectionReason::ObjectKind));
    }
    requested.push(required.oid);
    unique.insert(required.oid);
    for item in queue.iter() {
        if requested.len() == options.metadata_batch_size {
            break;
        }
        if !known.contains_key(&item.oid)
            && !missing_external.contains(&item.oid)
            && unique.insert(item.oid)
        {
            if item.oid.algo() != repository.hash_algo {
                return Err(state.fail(PreparedPushRejectionReason::ObjectKind));
            }
            requested.push(item.oid);
        }
    }
    state.metadata_operations = state
        .metadata_operations
        .checked_add(1)
        .ok_or_else(|| state.fail(PreparedPushRejectionReason::MetadataLimit))?;
    if state.metadata_operations > options.limits.max_object_operations {
        return Err(state.fail(PreparedPushRejectionReason::MetadataLimit));
    }
    checkpoint(
        options,
        work,
        control,
        u64::try_from(requested.len()).unwrap_or(u64::MAX),
        state,
    )?;
    let remaining = options
        .limits
        .max_prepared_memory_bytes
        .checked_sub(state.retained_bytes)
        .ok_or_else(|| state.fail(PreparedPushRejectionReason::MetadataLimit))?;
    let rows = provider
        .read_metadata(
            &repository.tenant,
            &repository.repository,
            &requested,
            remaining,
        )
        .await
        .map_err(|_| state.fail(PreparedPushRejectionReason::ExistingMetadata))?;
    checkpoint(options, work, control, 1, state)?;
    let mut requested_set = HashSet::new();
    requested_set
        .try_reserve(requested.len())
        .map_err(|_| state.fail(PreparedPushRejectionReason::Allocation))?;
    requested_set.extend(requested);
    let mut returned = HashSet::new();
    returned
        .try_reserve(requested_set.len())
        .map_err(|_| state.fail(PreparedPushRejectionReason::Allocation))?;
    for row in rows {
        if !requested_set.contains(&row.oid)
            || !returned.insert(row.oid)
            || row.oid.algo() != repository.hash_algo
        {
            return Err(state.fail(PreparedPushRejectionReason::ExistingMetadata));
        }
        let (links, retained) = authorized_links(row.links, repository.hash_algo, state)?;
        state.retain(retained, options.limits.max_prepared_memory_bytes)?;
        known
            .try_reserve(1)
            .map_err(|_| state.fail(PreparedPushRejectionReason::Allocation))?;
        external_oids
            .try_reserve(1)
            .map_err(|_| state.fail(PreparedPushRejectionReason::Allocation))?;
        if known.insert(row.oid, links).is_some() || !external_oids.insert(row.oid) {
            return Err(state.fail(PreparedPushRejectionReason::ExistingMetadata));
        }
    }
    for missing in requested_set.difference(&returned) {
        missing_external
            .try_reserve(1)
            .map_err(|_| state.fail(PreparedPushRejectionReason::Allocation))?;
        if !missing_external.insert(*missing) {
            return Err(state.fail(PreparedPushRejectionReason::ExistingMetadata));
        }
    }
    if !known.contains_key(&required.oid) {
        return Err(state.fail(PreparedPushRejectionReason::MissingObject));
    }
    Ok(())
}

fn authorized_links(
    links: AuthorizedExistingObjectLinks,
    hash_algo: HashAlgo,
    state: &PreparationState,
) -> Result<(ObjectLinks, u64), PreparedPushError> {
    match links {
        AuthorizedExistingObjectLinks::Blob => Ok((ObjectLinks::Blob, PREPARED_OBJECT_OVERHEAD)),
        AuthorizedExistingObjectLinks::Tree(entries) => {
            validate_oids(entries.iter().map(|entry| entry.oid), hash_algo, state)?;
            let retained = u64::try_from(entries.len())
                .ok()
                .and_then(|count| count.checked_mul(PREPARED_TREE_ENTRY_OVERHEAD))
                .and_then(|bytes| bytes.checked_add(PREPARED_OBJECT_OVERHEAD))
                .ok_or_else(|| state.fail(PreparedPushRejectionReason::MetadataLimit))?;
            Ok((ObjectLinks::Tree(entries), retained))
        }
        AuthorizedExistingObjectLinks::Commit { tree, parents } => {
            validate_oids(
                std::iter::once(tree).chain(parents.iter().copied()),
                hash_algo,
                state,
            )?;
            let retained = u64::try_from(parents.len())
                .ok()
                .and_then(|count| count.checked_mul(size_of::<ObjectId>() as u64))
                .and_then(|bytes| bytes.checked_add(PREPARED_OBJECT_OVERHEAD))
                .ok_or_else(|| state.fail(PreparedPushRejectionReason::MetadataLimit))?;
            Ok((ObjectLinks::Commit { tree, parents }, retained))
        }
        AuthorizedExistingObjectLinks::Tag {
            target,
            target_kind,
        } => {
            validate_oids(std::iter::once(target), hash_algo, state)?;
            Ok((
                ObjectLinks::Tag {
                    target,
                    target_kind,
                },
                PREPARED_OBJECT_OVERHEAD,
            ))
        }
    }
}

fn expected_tree_kind(
    mode: u32,
    gitlink_policy: PreparedGitlinkPolicy,
) -> Result<Option<ObjectKind>, PreparedPushRejectionReason> {
    match mode {
        0o040000 => Ok(Some(ObjectKind::Tree)),
        0o100644 | 0o100755 | 0o120000 => Ok(Some(ObjectKind::Blob)),
        0o160000 if gitlink_policy == PreparedGitlinkPolicy::Boundary => Ok(None),
        0o160000 => Ok(Some(ObjectKind::Commit)),
        _ => Err(PreparedPushRejectionReason::ObjectKind),
    }
}

fn build_ancestry<W, C>(
    commands: &[ReceivePackCommand],
    known: &HashMap<ObjectId, ObjectLinks>,
    options: &PreparedPushOptions,
    state: &PreparationState,
    work: &mut W,
    control: &mut C,
) -> Result<Vec<PreparedAncestryContext>, PreparedPushError>
where
    W: PreparedPushWork,
    C: PreparedPushControl,
{
    let mut ancestry = Vec::new();
    ancestry
        .try_reserve(commands.len())
        .map_err(|_| state.fail(PreparedPushRejectionReason::Allocation))?;
    for (ordinal, command) in commands.iter().enumerate() {
        let old_commit = if command.old_oid.is_zero() {
            None
        } else {
            peel_commit(command.old_oid, known, options, state, work, control)?
        };
        let new_commit = if command.new_oid.is_zero() {
            None
        } else {
            peel_commit(command.new_oid, known, options, state, work, control)?
        };
        let old_is_ancestor = match (old_commit, new_commit) {
            (Some(old), Some(new)) => {
                Some(is_ancestor(old, new, known, options, state, work, control)?)
            }
            _ => None,
        };
        ancestry.push(PreparedAncestryContext {
            command_ordinal: u32::try_from(ordinal)
                .map_err(|_| state.fail(PreparedPushRejectionReason::MetadataLimit))?,
            old_commit,
            new_commit,
            old_is_ancestor,
        });
    }
    Ok(ancestry)
}

fn peel_commit<W, C>(
    start: ObjectId,
    known: &HashMap<ObjectId, ObjectLinks>,
    options: &PreparedPushOptions,
    state: &PreparationState,
    work: &mut W,
    control: &mut C,
) -> Result<Option<ObjectId>, PreparedPushError>
where
    W: PreparedPushWork,
    C: PreparedPushControl,
{
    let mut current = start;
    let mut seen = HashSet::new();
    seen.try_reserve(
        usize::try_from(options.max_tag_depth)
            .unwrap_or(usize::MAX)
            .min(1_024),
    )
    .map_err(|_| state.fail(PreparedPushRejectionReason::Allocation))?;
    for _ in 0..options.max_tag_depth {
        checkpoint(options, work, control, 1, state)?;
        seen.try_reserve(1)
            .map_err(|_| state.fail(PreparedPushRejectionReason::Allocation))?;
        if !seen.insert(current) {
            return Err(state.fail(PreparedPushRejectionReason::TagPeel));
        }
        let links = known
            .get(&current)
            .ok_or_else(|| state.fail(PreparedPushRejectionReason::MissingObject))?;
        match links {
            ObjectLinks::Commit { .. } => return Ok(Some(current)),
            ObjectLinks::Tag {
                target,
                target_kind,
            } => {
                let actual = known
                    .get(target)
                    .ok_or_else(|| state.fail(PreparedPushRejectionReason::MissingObject))?
                    .kind();
                if actual != *target_kind {
                    return Err(state.fail(PreparedPushRejectionReason::ObjectKind));
                }
                current = *target;
            }
            ObjectLinks::Blob | ObjectLinks::Tree(_) => return Ok(None),
        }
    }
    Err(state.fail(PreparedPushRejectionReason::TagPeel))
}

fn is_ancestor<W, C>(
    ancestor: ObjectId,
    descendant: ObjectId,
    known: &HashMap<ObjectId, ObjectLinks>,
    options: &PreparedPushOptions,
    state: &PreparationState,
    work: &mut W,
    control: &mut C,
) -> Result<bool, PreparedPushError>
where
    W: PreparedPushWork,
    C: PreparedPushControl,
{
    if ancestor == descendant {
        return Ok(true);
    }
    let mut pending = Vec::new();
    pending
        .try_reserve(known.len().min(options.max_history_objects).max(1))
        .map_err(|_| state.fail(PreparedPushRejectionReason::Allocation))?;
    pending.push(descendant);
    let mut seen = HashSet::new();
    seen.try_reserve(known.len().min(options.max_history_objects))
        .map_err(|_| state.fail(PreparedPushRejectionReason::Allocation))?;
    while let Some(current) = pending.pop() {
        checkpoint(options, work, control, 1, state)?;
        if !seen.insert(current) {
            continue;
        }
        if seen.len() > options.max_history_objects {
            return Err(state.fail(PreparedPushRejectionReason::MetadataLimit));
        }
        let ObjectLinks::Commit { parents, .. } = known
            .get(&current)
            .ok_or_else(|| state.fail(PreparedPushRejectionReason::MissingObject))?
        else {
            return Err(state.fail(PreparedPushRejectionReason::ObjectKind));
        };
        if parents.contains(&ancestor) {
            return Ok(true);
        }
        pending
            .try_reserve(parents.len())
            .map_err(|_| state.fail(PreparedPushRejectionReason::Allocation))?;
        pending.extend(parents.iter().copied());
    }
    Ok(false)
}

fn prepare_commands(
    commands: &[ReceivePackCommand],
    snapshot_refs: &BTreeMap<String, ObjectId>,
    snapshot_generation: u64,
    state: &PreparationState,
) -> Result<Vec<PreparedPushCommand>, PreparedPushError> {
    let mut prepared = Vec::new();
    prepared
        .try_reserve(commands.len())
        .map_err(|_| state.fail(PreparedPushRejectionReason::Allocation))?;
    for (ordinal, command) in commands.iter().enumerate() {
        prepared.push(PreparedPushCommand {
            ordinal: u32::try_from(ordinal)
                .map_err(|_| state.fail(PreparedPushRejectionReason::MetadataLimit))?,
            command: command.clone(),
            precondition: PreparedRefPrecondition {
                refname: command.refname.clone(),
                expected: snapshot_refs.get(&command.refname).copied(),
                snapshot_generation,
            },
        });
    }
    Ok(prepared)
}

fn prepare_groups(
    command_count: usize,
    atomic: bool,
    state: &PreparationState,
) -> Result<Vec<PreparedCommandGroup>, PreparedPushError> {
    if atomic {
        let mut ordinals = Vec::new();
        ordinals
            .try_reserve(command_count)
            .map_err(|_| state.fail(PreparedPushRejectionReason::Allocation))?;
        for ordinal in 0..command_count {
            ordinals.push(
                u32::try_from(ordinal)
                    .map_err(|_| state.fail(PreparedPushRejectionReason::MetadataLimit))?,
            );
        }
        return Ok(vec![PreparedCommandGroup {
            atomic: true,
            command_ordinals: ordinals,
        }]);
    }
    let mut groups = Vec::new();
    groups
        .try_reserve(command_count)
        .map_err(|_| state.fail(PreparedPushRejectionReason::Allocation))?;
    for ordinal in 0..command_count {
        groups.push(PreparedCommandGroup {
            atomic: false,
            command_ordinals: vec![u32::try_from(ordinal)
                .map_err(|_| state.fail(PreparedPushRejectionReason::MetadataLimit))?],
        });
    }
    Ok(groups)
}

fn capability_summary(
    capabilities: &[ReceivePackCapability],
    hash_algo: HashAlgo,
) -> Result<PreparedCapabilitySummary, PreparedPushRejectionReason> {
    let mut hasher = PreparedHasher::new(hash_algo);
    hasher.field(b"capabilities-v1");
    hasher.u64(
        u64::try_from(capabilities.len())
            .map_err(|_| PreparedPushRejectionReason::MetadataLimit)?,
    );
    for capability in capabilities {
        hasher.field(capability.as_wire_token().as_bytes());
    }
    Ok(PreparedCapabilitySummary {
        count: u32::try_from(capabilities.len())
            .map_err(|_| PreparedPushRejectionReason::MetadataLimit)?,
        fingerprint: hasher.finish()?,
        atomic: capabilities.contains(&ReceivePackCapability::Atomic),
        push_options: capabilities.contains(&ReceivePackCapability::PushOptions),
        ofs_delta: capabilities.contains(&ReceivePackCapability::OfsDelta),
        object_format: hash_algo,
    })
}

fn push_option_summary(
    push_options: &[String],
) -> Result<PreparedPushOptionSummary, PreparedPushRejectionReason> {
    let bytes = push_options.iter().try_fold(0_u64, |total, option| {
        total
            .checked_add(
                u64::try_from(option.len())
                    .map_err(|_| PreparedPushRejectionReason::MetadataLimit)?,
            )
            .ok_or(PreparedPushRejectionReason::MetadataLimit)
    })?;
    Ok(PreparedPushOptionSummary {
        count: u32::try_from(push_options.len())
            .map_err(|_| PreparedPushRejectionReason::MetadataLimit)?,
        bytes,
    })
}

#[allow(clippy::too_many_arguments)]
fn prepared_fingerprint(
    repository: &PushRepositorySnapshot,
    commands: &[PreparedPushCommand],
    groups: &[PreparedCommandGroup],
    capabilities: PreparedCapabilitySummary,
    push_options: PreparedPushOptionSummary,
    gitlink_policy: PreparedGitlinkPolicy,
    quarantine: &PreparedQuarantineBinding,
    structural: &[PreparedStructuralObject],
    pushed_commits: &[ObjectId],
    ancestry: &[PreparedAncestryContext],
) -> Result<ObjectId, PreparedPushRejectionReason> {
    let mut hasher = PreparedHasher::new(repository.hash_algo);
    hasher.field(b"grit-prepared-push-v1");
    hasher.field(repository.tenant.as_str().as_bytes());
    hasher.field(repository.repository.as_str().as_bytes());
    hasher.u64(repository.generation);
    hasher.field(repository.hash_algo.name().as_bytes());
    hasher.u64(
        u64::try_from(commands.len()).map_err(|_| PreparedPushRejectionReason::MetadataLimit)?,
    );
    for command in commands {
        hasher.u32(command.ordinal);
        hasher.oid(command.command.old_oid);
        hasher.oid(command.command.new_oid);
        hasher.field(command.command.refname.as_bytes());
        match command.precondition.expected {
            Some(expected) => {
                hasher.byte(1);
                hasher.oid(expected);
            }
            None => hasher.byte(0),
        }
        hasher.u64(command.precondition.snapshot_generation);
    }
    hasher
        .u64(u64::try_from(groups.len()).map_err(|_| PreparedPushRejectionReason::MetadataLimit)?);
    for group in groups {
        hasher.byte(u8::from(group.atomic));
        hasher.u64(
            u64::try_from(group.command_ordinals.len())
                .map_err(|_| PreparedPushRejectionReason::MetadataLimit)?,
        );
        for ordinal in &group.command_ordinals {
            hasher.u32(*ordinal);
        }
    }
    hasher.oid(capabilities.fingerprint);
    hasher.u32(capabilities.count);
    hasher.byte(u8::from(capabilities.atomic));
    hasher.byte(u8::from(capabilities.push_options));
    hasher.byte(u8::from(capabilities.ofs_delta));
    hasher.field(capabilities.object_format.name().as_bytes());
    hasher.u32(push_options.count);
    hasher.u64(push_options.bytes);
    hasher.byte(match gitlink_policy {
        PreparedGitlinkPolicy::Boundary => 0,
        PreparedGitlinkPolicy::RequireCommit => 1,
    });
    hasher.field(quarantine.id.as_bytes());
    hasher.u64(quarantine.generation);
    hasher.u64(quarantine.manifest.pack_bytes);
    hasher.u32(quarantine.manifest.object_count);
    hash_optional_oid(&mut hasher, quarantine.manifest.pack_checksum);
    hash_optional_oid(&mut hasher, quarantine.manifest.index_checksum);
    hasher.u64(quarantine.manifest.metadata_bytes);
    hasher.byte(u8::from(quarantine.manifest.self_contained));
    hasher.oid(quarantine.attestation.pack_checksum());
    hasher.oid(quarantine.attestation.index_checksum());
    hasher.u32(quarantine.attestation.object_count());
    hasher.byte(u8::from(quarantine.attestation.validation_complete()));
    hasher.byte(u8::from(quarantine.attestation.self_contained()));
    hasher.u64(
        u64::try_from(structural.len()).map_err(|_| PreparedPushRejectionReason::MetadataLimit)?,
    );
    for object in structural {
        hasher.oid(object.oid);
        match &object.metadata {
            PreparedStructuralMetadata::Commit { tree, parents } => {
                hasher.byte(1);
                hasher.oid(*tree);
                hasher.u64(
                    u64::try_from(parents.len())
                        .map_err(|_| PreparedPushRejectionReason::MetadataLimit)?,
                );
                for parent in parents {
                    hasher.oid(*parent);
                }
            }
            PreparedStructuralMetadata::Tree(entries) => {
                hasher.byte(2);
                hasher.u64(
                    u64::try_from(entries.len())
                        .map_err(|_| PreparedPushRejectionReason::MetadataLimit)?,
                );
                for entry in entries {
                    hasher.u32(entry.mode);
                    hasher.field(&entry.name);
                    hasher.oid(entry.oid);
                }
            }
            PreparedStructuralMetadata::Tag {
                target,
                target_kind,
            } => {
                hasher.byte(3);
                hasher.oid(*target);
                hasher.byte(object_kind_code(*target_kind));
            }
        }
    }
    hasher.u64(
        u64::try_from(pushed_commits.len())
            .map_err(|_| PreparedPushRejectionReason::MetadataLimit)?,
    );
    for oid in pushed_commits {
        hasher.oid(*oid);
    }
    hasher.u64(
        u64::try_from(ancestry.len()).map_err(|_| PreparedPushRejectionReason::MetadataLimit)?,
    );
    for context in ancestry {
        hasher.u32(context.command_ordinal);
        hash_optional_oid(&mut hasher, context.old_commit);
        hash_optional_oid(&mut hasher, context.new_commit);
        hasher.byte(match context.old_is_ancestor {
            None => 0,
            Some(false) => 1,
            Some(true) => 2,
        });
    }
    hasher.finish()
}

fn hash_optional_oid(hasher: &mut PreparedHasher, oid: Option<ObjectId>) {
    match oid {
        Some(oid) => {
            hasher.byte(1);
            hasher.oid(oid);
        }
        None => hasher.byte(0),
    }
}

fn object_kind_code(kind: ObjectKind) -> u8 {
    match kind {
        ObjectKind::Blob => 1,
        ObjectKind::Tree => 2,
        ObjectKind::Commit => 3,
        ObjectKind::Tag => 4,
    }
}

enum PreparedHasher {
    Sha1(Sha1),
    Sha256(Sha256),
}

impl PreparedHasher {
    fn new(hash_algo: HashAlgo) -> Self {
        match hash_algo {
            HashAlgo::Sha1 => Self::Sha1(Sha1::new()),
            HashAlgo::Sha256 => Self::Sha256(Sha256::new()),
        }
    }

    fn update(&mut self, bytes: &[u8]) {
        match self {
            Self::Sha1(hasher) => Sha1Digest::update(hasher, bytes),
            Self::Sha256(hasher) => Sha2Digest::update(hasher, bytes),
        }
    }

    fn byte(&mut self, value: u8) {
        self.update(&[value]);
    }

    fn u32(&mut self, value: u32) {
        self.update(&value.to_be_bytes());
    }

    fn u64(&mut self, value: u64) {
        self.update(&value.to_be_bytes());
    }

    fn field(&mut self, bytes: &[u8]) {
        self.u64(u64::try_from(bytes.len()).unwrap_or(u64::MAX));
        self.update(bytes);
    }

    fn oid(&mut self, oid: ObjectId) {
        self.field(oid.as_bytes());
    }

    fn finish(self) -> Result<ObjectId, PreparedPushRejectionReason> {
        let oid = match self {
            Self::Sha1(hasher) => ObjectId::from_bytes(&hasher.finalize())
                .map_err(|_| PreparedPushRejectionReason::BindingMismatch),
            Self::Sha256(hasher) => ObjectId::from_bytes(&hasher.finalize())
                .map_err(|_| PreparedPushRejectionReason::BindingMismatch),
        }?;
        if oid.is_zero() {
            return Err(PreparedPushRejectionReason::BindingMismatch);
        }
        Ok(oid)
    }
}
