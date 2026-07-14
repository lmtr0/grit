//! Metadata-only policy evaluation and guarded atomic receive-pack publication.
//!
//! User policy awaits finish before the backend transaction begins. The short transaction then
//! rechecks a promotion receipt, generation, ref CAS, namespace, and prepared ancestry evidence;
//! it writes refs, reflogs, the new generation, an idempotency record, and durable success/reject
//! outbox events together. No PACK or blob payload is decoded by this phase.

use async_trait::async_trait;
use grit_lib::check_ref_format::{check_refname_format, RefNameOptions};
use grit_lib::objects::{HashAlgo, ObjectId};
use grit_lib::pkt_line;
use time::OffsetDateTime;

use crate::ids::{RepositoryId, TenantId};
use crate::policy::{
    PolicyActor, PolicyDecision, PolicyRefUpdate, PushPolicy, PushPolicyContext,
    RefUpdatePolicyContext,
};
use crate::protocol::memory_pack_promotion::MemoryPackPromotionReceipt;
#[cfg(feature = "sqlx-postgres")]
use crate::protocol::pg_pack_promotion::PgPackPromotionReceipt;
use crate::protocol::push_prepared::{
    PreparedAncestryContext, PreparedPush, PreparedPushAuditSummary,
};
use crate::protocol::push_quarantine::QuarantineId;
use crate::protocol::receive_pack::PushCommandKind;
use crate::storage::ReflogEntry;

/// Owned exact promotion evidence consumed by a backend transaction.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PromotionReceiptBinding {
    /// Owning tenant.
    pub tenant: TenantId,
    /// Owning repository.
    pub repository: RepositoryId,
    /// Source quarantine identity.
    pub quarantine_id: QuarantineId,
    /// Exact fenced quarantine generation.
    pub quarantine_generation: u64,
    /// Repository generation at promotion.
    pub repository_generation: u64,
    /// PACK body/trailer checksum.
    pub pack_checksum: ObjectId,
    /// Ordered validated index checksum.
    pub index_checksum: ObjectId,
    /// Prepared metadata fingerprint.
    pub prepared_fingerprint: ObjectId,
    /// Installed object count.
    pub object_count: u32,
    /// Complete installed PACK bytes.
    pub size_bytes: u64,
    /// Whether the pack storage was already present during promotion.
    pub deduplicated: bool,
}

/// Typed conversion from a backend-specific promotion receipt.
pub trait PushPromotionReceipt {
    /// Copy immutable binding evidence into a backend-neutral value.
    fn publication_binding(&self) -> PromotionReceiptBinding;
}

impl PushPromotionReceipt for MemoryPackPromotionReceipt {
    fn publication_binding(&self) -> PromotionReceiptBinding {
        PromotionReceiptBinding {
            tenant: self.tenant().clone(),
            repository: self.repository().clone(),
            quarantine_id: self.quarantine_id(),
            quarantine_generation: self.quarantine_generation(),
            repository_generation: self.repository_generation(),
            pack_checksum: self.pack_checksum(),
            index_checksum: self.index_checksum(),
            prepared_fingerprint: self.prepared_fingerprint(),
            object_count: self.object_count(),
            size_bytes: self.size_bytes(),
            deduplicated: self.deduplicated(),
        }
    }
}

#[cfg(feature = "sqlx-postgres")]
impl PushPromotionReceipt for PgPackPromotionReceipt {
    fn publication_binding(&self) -> PromotionReceiptBinding {
        PromotionReceiptBinding {
            tenant: self.tenant().clone(),
            repository: self.repository().clone(),
            quarantine_id: self.quarantine_id(),
            quarantine_generation: self.quarantine_generation(),
            repository_generation: self.repository_generation(),
            pack_checksum: self.pack_checksum(),
            index_checksum: self.index_checksum(),
            prepared_fingerprint: self.prepared_fingerprint(),
            object_count: self.object_count(),
            size_bytes: self.size_bytes(),
            deduplicated: self.deduplicated(),
        }
    }
}

/// Stable per-command rejection category.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PushPublicationRejection {
    /// Pre-receive, authorization, protected-ref, or update policy rejected the command.
    Policy,
    /// Repository generation no longer matches preparation and promotion.
    StaleGeneration,
    /// Exact old-value compare-and-swap failed.
    RefConflict,
    /// Promoted pack receipt is absent or bound to different evidence.
    PromotionMissing,
    /// A branch update is not a prepared fast-forward.
    NonFastForward,
    /// Another command caused the requested atomic group to abort.
    AtomicAborted,
}

impl PushPublicationRejection {
    fn wire_message(self) -> &'static str {
        match self {
            Self::Policy => "policy rejected",
            Self::StaleGeneration => "stale repository generation",
            Self::RefConflict => "reference changed",
            Self::PromotionMissing => "promoted pack unavailable",
            Self::NonFastForward => "non-fast-forward",
            Self::AtomicAborted => "atomic push failed",
        }
    }
}

/// Exact command result returned in original wire order.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PushPublicationCommandOutcome {
    /// Ref and reflog were committed.
    Applied,
    /// Ref was not changed.
    Rejected(PushPublicationRejection),
}

/// One command status at the report-status boundary.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PushPublicationCommandStatus {
    /// Original zero-based command ordinal.
    pub ordinal: u32,
    /// Original ref name.
    pub refname: String,
    /// Create, update, or delete.
    pub kind: PushCommandKind,
    /// Applied or rejected outcome.
    pub outcome: PushPublicationCommandOutcome,
}

/// Negotiated receive-pack status shape.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PushReportStatusVersion {
    /// `report-status` command lines.
    V1,
    /// `report-status-v2`; direct non-rewritten refs need no option lines.
    V2,
}

/// Structural-index state accompanying ref publication.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PushStructuralPublicationPrerequisite {
    /// No pushed structural object requires indexing.
    None,
    /// Correct DAG generations and raw path bytes require a canonical indexing transaction.
    DeferredCanonicalIndex {
        /// Prepared structural objects awaiting indexing.
        objects: u32,
    },
}

/// Complete deterministic publication report.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PushPublicationReport {
    /// Prepared fingerprint serving as the idempotency identity.
    pub prepared_fingerprint: ObjectId,
    /// Repository generation after publication, or the unchanged generation on full rejection.
    pub repository_generation: u64,
    /// Command statuses in original wire order.
    pub statuses: Vec<PushPublicationCommandStatus>,
    /// Explicit prerequisite; fabricated legacy structural rows are never reported as installed.
    pub structural: PushStructuralPublicationPrerequisite,
}

impl PushPublicationReport {
    /// Serialize v1 or v2 report-status pkt-lines.
    ///
    /// Direct refs are not rewritten, so v2 has the same command lines as v1 and emits no option
    /// lines.
    ///
    /// # Errors
    ///
    /// Returns protocol errors for an invalid/injectable ref name or oversized line, allocation
    /// errors while constructing a bounded packet, and framing I/O errors.
    pub fn to_pkt_lines(&self, version: PushReportStatusVersion) -> crate::error::Result<Vec<u8>> {
        let mut out = Vec::new();
        write_report_line(&mut out, &[b"unpack ok"])?;
        for status in &self.statuses {
            if !status.refname.starts_with("refs/")
                || check_refname_format(&status.refname, &RefNameOptions::default()).is_err()
            {
                return Err(crate::error::Error::Protocol(
                    "publication report contains an invalid ref name".to_owned(),
                ));
            }
            match status.outcome {
                PushPublicationCommandOutcome::Applied => {
                    write_report_line(&mut out, &[b"ok ", status.refname.as_bytes()])?;
                    match version {
                        PushReportStatusVersion::V1 | PushReportStatusVersion::V2 => {}
                    }
                }
                PushPublicationCommandOutcome::Rejected(reason) => write_report_line(
                    &mut out,
                    &[
                        b"ng ",
                        status.refname.as_bytes(),
                        b" ",
                        reason.wire_message().as_bytes(),
                    ],
                )?,
            }
        }
        out.try_reserve_exact(4).map_err(|_| {
            crate::error::Error::Backend("cannot reserve publication report flush".to_owned())
        })?;
        pkt_line::write_flush(&mut out)?;
        Ok(out)
    }

    /// Return whether at least one ref was committed.
    #[must_use]
    pub fn any_applied(&self) -> bool {
        self.statuses
            .iter()
            .any(|status| status.outcome == PushPublicationCommandOutcome::Applied)
    }
}

fn write_report_line(out: &mut Vec<u8>, fields: &[&[u8]]) -> crate::error::Result<()> {
    const MAX_PAYLOAD_BYTES: usize = 65_516;
    let payload_bytes = fields
        .iter()
        .try_fold(1_usize, |total, field| total.checked_add(field.len()))
        .filter(|total| *total <= MAX_PAYLOAD_BYTES)
        .ok_or_else(|| {
            crate::error::Error::Protocol("publication report line exceeds pkt-line bounds".into())
        })?;
    let mut payload = Vec::new();
    payload.try_reserve_exact(payload_bytes).map_err(|_| {
        crate::error::Error::Backend("cannot reserve publication report packet".to_owned())
    })?;
    for field in fields {
        payload.extend_from_slice(field);
    }
    payload.push(b'\n');
    out.try_reserve(payload_bytes.checked_add(4).ok_or_else(|| {
        crate::error::Error::Protocol("publication report packet size overflow".to_owned())
    })?)
    .map_err(|_| {
        crate::error::Error::Backend("cannot reserve publication report output".to_owned())
    })?;
    pkt_line::write_packet_raw(out, &payload)?;
    Ok(())
}

/// Bounded non-sensitive audit payload derived from [`PreparedPush`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PushPublicationAudit {
    /// Owning tenant.
    pub tenant: TenantId,
    /// Owning repository.
    pub repository: RepositoryId,
    /// Prepared metadata fingerprint.
    pub prepared_fingerprint: ObjectId,
    /// Preparation counters that require no PACK retry.
    pub summary: PreparedPushAuditSummary,
    /// Actor stable identifier.
    pub actor_id: String,
    /// Caller-supplied event time.
    pub timestamp: OffsetDateTime,
}

/// Durable work emitted by a successful or rejected transaction.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PushPublicationOutboxEvent {
    /// Invalidate one ref after durable mutation.
    RefChanged {
        /// Ref name.
        refname: String,
        /// Whether the ref was deleted.
        deleted: bool,
    },
    /// Run post-receive with the committed successful subset.
    PostReceive {
        /// Applied command ordinals.
        ordinals: Vec<u32>,
    },
    /// Persist one accepted/partially accepted audit record.
    AcceptedAudit(PushPublicationAudit),
    /// Persist one full-push rejection audit record.
    RejectedAudit {
        /// Bounded audit payload.
        audit: PushPublicationAudit,
        /// Stable rejection category.
        reason: PushPublicationRejection,
    },
}

/// One immutable command supplied to the backend transaction.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PushPublicationCommand {
    /// Original ordinal.
    pub ordinal: u32,
    /// Ref name.
    pub refname: String,
    /// Exact expected direct OID, or absence for create.
    pub expected: Option<ObjectId>,
    /// Desired direct OID, or absence for delete.
    pub new_oid: Option<ObjectId>,
    /// Command kind.
    pub kind: PushCommandKind,
    /// Whether pre-receive and update policy allowed this command.
    pub policy_allowed: bool,
    /// Prepared ancestry context for this exact ordinal.
    pub ancestry: PreparedAncestryContext,
}

/// Fully owned request for one short backend transaction.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PushPublicationTransaction {
    /// Owning tenant.
    pub tenant: TenantId,
    /// Owning repository.
    pub repository: RepositoryId,
    /// Repository object format.
    pub hash_algo: HashAlgo,
    /// Exact generation observed during preparation.
    pub repository_generation: u64,
    /// Deterministic prepared metadata fingerprint/idempotency identity.
    pub prepared_fingerprint: ObjectId,
    /// Exact promotion evidence, absent only when no PACK was required.
    pub promotion: Option<PromotionReceiptBinding>,
    /// Whether every command is one all-or-none group.
    pub atomic: bool,
    /// Commands in original wire order.
    pub commands: Vec<PushPublicationCommand>,
    /// Actor identity used only for reflogs.
    pub reflog_identity: String,
    /// Caller-supplied reflog/audit time.
    pub timestamp: OffsetDateTime,
    /// Bounded audit payload.
    pub audit: PushPublicationAudit,
    /// Explicit canonical structural-index prerequisite.
    pub structural: PushStructuralPublicationPrerequisite,
}

/// Non-sensitive backend transaction failure.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
#[error("push publication backend transaction failed")]
pub struct PushPublicationBackendError;

/// Atomic ref/reflog/generation/idempotency/outbox backend boundary.
#[async_trait]
pub trait PushAtomicPublicationBackend: Send + Sync {
    /// Recheck and publish one prepared request in a single short transaction.
    async fn publish_transaction(
        &self,
        transaction: PushPublicationTransaction,
    ) -> Result<PushPublicationReport, PushPublicationBackendError>;

    /// Durably record one policy/integration rejection exactly once by prepared fingerprint.
    async fn record_rejection(
        &self,
        audit: PushPublicationAudit,
        reason: PushPublicationRejection,
    ) -> Result<(), PushPublicationBackendError>;
}

/// Explicit pre-transaction work/time bounds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PushAtomicPublicationLimits {
    /// Maximum prepared commands.
    pub max_commands: usize,
    /// Explicit phase start.
    pub started_at: OffsetDateTime,
    /// Explicit deadline. Cancellation is no longer observed after the backend call begins.
    pub deadline: OffsetDateTime,
}

impl PushAtomicPublicationLimits {
    fn validate(self) -> Result<Self, PushAtomicPublicationError> {
        let hard = crate::protocol::push_metrics::ReceivePackLimits::HARD_MAX;
        let duration = time::Duration::try_from(hard.max_duration)
            .map_err(|_| PushAtomicPublicationError::InvalidLimits)?;
        if self.max_commands == 0
            || self.max_commands > hard.max_commands
            || self.deadline <= self.started_at
            || self.deadline - self.started_at > duration
        {
            return Err(PushAtomicPublicationError::InvalidLimits);
        }
        Ok(self)
    }
}

/// One explicit monotonic-time/cancellation observation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PushAtomicPublicationObservation {
    /// Caller-observed time.
    pub observed_at: OffsetDateTime,
    /// Cancellation signal.
    pub cancelled: bool,
}

/// Runtime-neutral pre-transaction control.
pub trait PushAtomicPublicationControl {
    /// Return the latest observation.
    fn observe(&mut self) -> PushAtomicPublicationObservation;
}

/// Deterministic pre-transaction work allowance.
pub trait PushAtomicPublicationWork {
    /// Charge policy/metadata preparation units.
    fn charge(&mut self, units: u64) -> bool;
}

/// Counter-backed publication work allowance.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PushAtomicPublicationWorkLimit {
    remaining: u64,
}

impl PushAtomicPublicationWorkLimit {
    /// Construct an exact allowance.
    #[must_use]
    pub const fn new(units: u64) -> Self {
        Self { remaining: units }
    }
    /// Return remaining units.
    #[must_use]
    pub const fn remaining(&self) -> u64 {
        self.remaining
    }
}

impl PushAtomicPublicationWork for PushAtomicPublicationWorkLimit {
    fn charge(&mut self, units: u64) -> bool {
        let Some(remaining) = self.remaining.checked_sub(units) else {
            return false;
        };
        self.remaining = remaining;
        true
    }
}

/// Typed orchestration failure outside per-command rejection statuses.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum PushAtomicPublicationError {
    /// Limits are invalid.
    #[error("invalid atomic push publication limits")]
    InvalidLimits,
    /// Prepared commands, ancestry, receipt, or grouping are inconsistent.
    #[error("atomic push publication binding mismatch")]
    Binding,
    /// A bounded allocation failed.
    #[error("atomic push publication bounded allocation failed")]
    Allocation,
    /// Work allowance was exhausted.
    #[error("atomic push publication work budget exhausted")]
    WorkBudget,
    /// Cancellation was observed before the transaction.
    #[error("atomic push publication cancelled")]
    Cancelled,
    /// Deadline elapsed or observation time moved backwards.
    #[error("atomic push publication deadline elapsed")]
    Deadline,
    /// Policy integration failed without a decision.
    #[error("atomic push publication policy integration failed")]
    Policy,
    /// Durable backend publication or rejection recording failed.
    #[error("atomic push publication backend failed")]
    Backend,
}

/// Evaluate metadata-only policy and publish through one guarded backend transaction.
///
/// Policy receives only prepared command and commit metadata. Cancellation/deadline are checked
/// before and after every policy await and once immediately before entering the backend; the
/// backend result is never reclassified as cancellation, avoiding commit ambiguity.
///
/// # Errors
///
/// Returns typed bounds, binding, allocation, control, policy-integration, or backend failures.
#[allow(clippy::too_many_arguments)]
pub async fn publish_prepared_push<B, P, R, W, C>(
    backend: &B,
    prepared: &PreparedPush,
    promotion: Option<&R>,
    actor: PolicyActor,
    timestamp: OffsetDateTime,
    policy: &P,
    limits: PushAtomicPublicationLimits,
    work: &mut W,
    control: &mut C,
) -> Result<PushPublicationReport, PushAtomicPublicationError>
where
    B: PushAtomicPublicationBackend,
    P: PushPolicy,
    R: PushPromotionReceipt,
    W: PushAtomicPublicationWork,
    C: PushAtomicPublicationControl,
{
    let limits = limits.validate()?;
    let mut last_observed = limits.started_at;
    checkpoint(&limits, work, control, &mut last_observed, 1)?;
    if prepared.commands().is_empty() || prepared.commands().len() > limits.max_commands {
        return Err(PushAtomicPublicationError::Binding);
    }
    let promotion = promotion.map(PushPromotionReceipt::publication_binding);
    validate_bindings(prepared, promotion.as_ref())?;
    let context = policy_context(prepared, actor.clone())?;
    let audit = PushPublicationAudit {
        tenant: prepared.tenant().clone(),
        repository: prepared.repository().clone(),
        prepared_fingerprint: prepared.fingerprint(),
        summary: prepared.audit_summary(),
        actor_id: actor.id.clone(),
        timestamp,
    };
    let pre_receive_result = policy.pre_receive(&context).await;
    checkpoint(&limits, work, control, &mut last_observed, 1)?;
    let pre_receive = match pre_receive_result {
        Ok(decision) => decision,
        Err(_) => {
            backend
                .record_rejection(audit.clone(), PushPublicationRejection::Policy)
                .await
                .map_err(|_| PushAtomicPublicationError::Backend)?;
            return Err(PushAtomicPublicationError::Policy);
        }
    };
    let pre_receive_allowed = matches!(pre_receive, PolicyDecision::Allow);
    let mut commands = Vec::new();
    commands
        .try_reserve_exact(prepared.commands().len())
        .map_err(|_| PushAtomicPublicationError::Allocation)?;
    for prepared_command in prepared.commands() {
        let command = prepared_command.command();
        let ancestry = prepared
            .ancestry()
            .get(
                usize::try_from(prepared_command.ordinal())
                    .map_err(|_| PushAtomicPublicationError::Binding)?,
            )
            .filter(|ancestry| ancestry.command_ordinal == prepared_command.ordinal())
            .cloned()
            .ok_or(PushAtomicPublicationError::Binding)?;
        let mut policy_allowed = pre_receive_allowed;
        if policy_allowed {
            checkpoint(
                &limits,
                work,
                control,
                &mut last_observed,
                u64::try_from(prepared.pushed_commits().len())
                    .map_err(|_| PushAtomicPublicationError::Binding)?,
            )?;
            let update_context = RefUpdatePolicyContext {
                tenant: prepared.tenant().clone(),
                repository: prepared.repository().clone(),
                actor: actor.clone(),
                update: PolicyRefUpdate::new(
                    command.refname.clone(),
                    command.old_oid,
                    command.new_oid,
                    command.kind(),
                ),
                pushed_commits: clone_object_ids(prepared.pushed_commits())?,
            };
            let update_result = policy.update(&update_context).await;
            checkpoint(&limits, work, control, &mut last_observed, 1)?;
            policy_allowed = match update_result {
                Ok(decision) => matches!(decision, PolicyDecision::Allow),
                Err(_) => {
                    backend
                        .record_rejection(audit.clone(), PushPublicationRejection::Policy)
                        .await
                        .map_err(|_| PushAtomicPublicationError::Backend)?;
                    return Err(PushAtomicPublicationError::Policy);
                }
            };
        }
        commands.push(PushPublicationCommand {
            ordinal: prepared_command.ordinal(),
            refname: command.refname.clone(),
            expected: prepared_command.precondition().expected(),
            new_oid: (!command.new_oid.is_zero()).then_some(command.new_oid),
            kind: command.kind(),
            policy_allowed,
            ancestry,
        });
    }
    let atomic = prepared.capabilities().atomic;
    checkpoint(
        &limits,
        work,
        control,
        &mut last_observed,
        u64::try_from(commands.len()).map_err(|_| PushAtomicPublicationError::Binding)?,
    )?;
    let structural = structural_prerequisite(prepared)?;
    backend
        .publish_transaction(PushPublicationTransaction {
            tenant: prepared.tenant().clone(),
            repository: prepared.repository().clone(),
            hash_algo: prepared.hash_algo(),
            repository_generation: prepared.repository_generation(),
            prepared_fingerprint: prepared.fingerprint(),
            promotion,
            atomic,
            commands,
            reflog_identity: actor.reflog_identity,
            timestamp,
            audit,
            structural,
        })
        .await
        .map_err(|_| PushAtomicPublicationError::Backend)
}

fn validate_bindings(
    prepared: &PreparedPush,
    promotion: Option<&PromotionReceiptBinding>,
) -> Result<(), PushAtomicPublicationError> {
    let manifest = &prepared.quarantine().manifest;
    let requires_receipt = manifest.pack_checksum.is_some();
    if requires_receipt != promotion.is_some() {
        return Err(PushAtomicPublicationError::Binding);
    }
    if let Some(receipt) = promotion {
        if receipt.tenant != *prepared.tenant()
            || receipt.repository != *prepared.repository()
            || receipt.quarantine_id != prepared.quarantine().id
            || receipt.quarantine_generation != prepared.quarantine().generation
            || receipt.repository_generation != prepared.repository_generation()
            || receipt.prepared_fingerprint != prepared.fingerprint()
            || Some(receipt.pack_checksum) != manifest.pack_checksum
            || Some(receipt.index_checksum) != manifest.index_checksum
            || receipt.object_count != manifest.object_count
            || receipt.size_bytes != manifest.pack_bytes
        {
            return Err(PushAtomicPublicationError::Binding);
        }
    }
    let atomic = prepared.capabilities().atomic;
    if prepared.groups().len() != if atomic { 1 } else { prepared.commands().len() }
        || prepared
            .groups()
            .iter()
            .any(|group| group.is_atomic() != atomic)
    {
        return Err(PushAtomicPublicationError::Binding);
    }
    Ok(())
}

fn policy_context(
    prepared: &PreparedPush,
    actor: PolicyActor,
) -> Result<PushPolicyContext, PushAtomicPublicationError> {
    let mut updates = Vec::new();
    updates
        .try_reserve_exact(prepared.commands().len())
        .map_err(|_| PushAtomicPublicationError::Allocation)?;
    updates.extend(prepared.commands().iter().map(|prepared| {
        let command = prepared.command();
        PolicyRefUpdate::new(
            command.refname.clone(),
            command.old_oid,
            command.new_oid,
            command.kind(),
        )
    }));
    Ok(PushPolicyContext {
        tenant: prepared.tenant().clone(),
        repository: prepared.repository().clone(),
        actor,
        updates,
        pushed_commits: clone_object_ids(prepared.pushed_commits())?,
    })
}

fn clone_object_ids(oids: &[ObjectId]) -> Result<Vec<ObjectId>, PushAtomicPublicationError> {
    let mut cloned = Vec::new();
    cloned
        .try_reserve_exact(oids.len())
        .map_err(|_| PushAtomicPublicationError::Allocation)?;
    cloned.extend_from_slice(oids);
    Ok(cloned)
}

fn structural_prerequisite(
    prepared: &PreparedPush,
) -> Result<PushStructuralPublicationPrerequisite, PushAtomicPublicationError> {
    if prepared.structural_objects().is_empty() {
        return Ok(PushStructuralPublicationPrerequisite::None);
    }
    Ok(
        PushStructuralPublicationPrerequisite::DeferredCanonicalIndex {
            objects: u32::try_from(prepared.structural_objects().len())
                .map_err(|_| PushAtomicPublicationError::Binding)?,
        },
    )
}

fn checkpoint<W, C>(
    limits: &PushAtomicPublicationLimits,
    work: &mut W,
    control: &mut C,
    last_observed: &mut OffsetDateTime,
    units: u64,
) -> Result<(), PushAtomicPublicationError>
where
    W: PushAtomicPublicationWork,
    C: PushAtomicPublicationControl,
{
    let observation = control.observe();
    if observation.observed_at < *last_observed {
        return Err(PushAtomicPublicationError::Deadline);
    }
    *last_observed = observation.observed_at;
    if observation.cancelled {
        return Err(PushAtomicPublicationError::Cancelled);
    }
    if observation.observed_at < limits.started_at || observation.observed_at >= limits.deadline {
        return Err(PushAtomicPublicationError::Deadline);
    }
    if !work.charge(units) {
        return Err(PushAtomicPublicationError::WorkBudget);
    }
    Ok(())
}

pub(crate) fn command_reflog(
    command: &PushPublicationCommand,
    hash_algo: HashAlgo,
    actor: &str,
    timestamp: OffsetDateTime,
) -> ReflogEntry {
    ReflogEntry {
        refname: command.refname.clone(),
        old_oid: command
            .expected
            .unwrap_or_else(|| ObjectId::null(hash_algo)),
        new_oid: command.new_oid.unwrap_or_else(|| ObjectId::null(hash_algo)),
        actor: actor.to_owned(),
        timestamp,
        message: match command.kind {
            PushCommandKind::Create => "push create",
            PushCommandKind::Update => "push update",
            PushCommandKind::Delete => "push delete",
        }
        .to_owned(),
    }
}
