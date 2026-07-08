//! Authorization, repository policy hooks, and audit integration for hosted writes.

use async_trait::async_trait;
use grit_lib::objects::ObjectId;
use time::OffsetDateTime;

use crate::error::{Error, Result};
use crate::ids::{RepositoryId, TenantId};
use crate::protocol::receive_pack::PushCommandKind;

/// Actor identity supplied by the hosting platform for policy and reflog decisions.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PolicyActor {
    /// Stable platform-neutral actor identifier.
    pub id: String,
    /// Git identity line written to reflogs for accepted writes.
    pub reflog_identity: String,
}

impl PolicyActor {
    /// Create an actor identity from a stable `id` and Git `reflog_identity`.
    ///
    /// `id` is intended for authorization and audit integrations. `reflog_identity` is the
    /// already-formatted identity line written to reflogs after accepted ref updates.
    #[must_use]
    pub fn new(id: impl Into<String>, reflog_identity: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            reflog_identity: reflog_identity.into(),
        }
    }
}

/// Repository-level permission being checked by an authorization provider.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum RepositoryPermission {
    /// Permission to push objects and update refs.
    Write,
}

impl RepositoryPermission {
    /// Return a stable permission label for audit records and typed errors.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Write => "write",
        }
    }
}

/// Context passed to a tenant or repository authorization provider.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AuthorizationContext {
    /// Tenant that owns the repository.
    pub tenant: TenantId,
    /// Repository being accessed.
    pub repository: RepositoryId,
    /// Actor requesting access.
    pub actor: PolicyActor,
    /// Permission being checked.
    pub permission: RepositoryPermission,
}

/// Allow or deny decision returned by authorization and policy hooks.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PolicyDecision {
    /// The operation may proceed.
    Allow,
    /// The operation is rejected with a platform-readable reason.
    Deny {
        /// Human-readable reason suitable for logs and protocol error mapping.
        reason: String,
    },
}

impl PolicyDecision {
    /// Return an allow decision.
    #[must_use]
    pub fn allow() -> Self {
        Self::Allow
    }

    /// Return a deny decision with `reason`.
    #[must_use]
    pub fn deny(reason: impl Into<String>) -> Self {
        Self::Deny {
            reason: reason.into(),
        }
    }

    /// Convert a ref-scoped push decision into a typed result.
    ///
    /// # Errors
    ///
    /// Returns [`Error::PushPolicyRejected`] when the decision denies the update.
    pub fn into_push_result(self, refname: impl Into<String>) -> Result<()> {
        match self {
            Self::Allow => Ok(()),
            Self::Deny { reason } => Err(Error::PushPolicyRejected {
                refname: refname.into(),
                reason,
            }),
        }
    }
}

/// Authorization provider supplied by an embedding hosting platform.
#[async_trait]
pub trait AuthorizationProvider: Send + Sync {
    /// Check whether `context.actor` has `context.permission` for the repository.
    ///
    /// # Errors
    ///
    /// Returns backend or integration errors from the provider.
    async fn check(&self, context: &AuthorizationContext) -> Result<PolicyDecision>;
}

/// Authorization provider that permits all requests.
#[derive(Clone, Copy, Debug, Default)]
pub struct NoAuthorization;

#[async_trait]
impl AuthorizationProvider for NoAuthorization {
    async fn check(&self, _context: &AuthorizationContext) -> Result<PolicyDecision> {
        Ok(PolicyDecision::Allow)
    }
}

/// One ref update included in a push policy or audit context.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PolicyRefUpdate {
    /// Ref being updated.
    pub refname: String,
    /// Old object id from the client command.
    pub old_oid: ObjectId,
    /// New object id from the client command.
    pub new_oid: ObjectId,
    /// Derived command kind.
    pub kind: PushCommandKind,
}

impl PolicyRefUpdate {
    /// Create a ref update policy value.
    #[must_use]
    pub fn new(
        refname: impl Into<String>,
        old_oid: ObjectId,
        new_oid: ObjectId,
        kind: PushCommandKind,
    ) -> Self {
        Self {
            refname: refname.into(),
            old_oid,
            new_oid,
            kind,
        }
    }
}

/// Batch context passed to pre-receive and post-receive hooks.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PushPolicyContext {
    /// Tenant that owns the repository.
    pub tenant: TenantId,
    /// Repository being pushed to.
    pub repository: RepositoryId,
    /// Actor performing the push.
    pub actor: PolicyActor,
    /// Ref updates requested by the push.
    pub updates: Vec<PolicyRefUpdate>,
    /// Commit objects introduced by the push pack.
    pub pushed_commits: Vec<ObjectId>,
}

/// Per-ref context passed to update hooks.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RefUpdatePolicyContext {
    /// Tenant that owns the repository.
    pub tenant: TenantId,
    /// Repository being pushed to.
    pub repository: RepositoryId,
    /// Actor performing the push.
    pub actor: PolicyActor,
    /// Ref update being checked.
    pub update: PolicyRefUpdate,
    /// Commit objects introduced by the push pack.
    pub pushed_commits: Vec<ObjectId>,
}

/// Receive-pack policy hooks for a hosted repository.
#[async_trait]
pub trait PushPolicy: Send + Sync {
    /// Run a pre-receive style check over the full push.
    ///
    /// # Errors
    ///
    /// Returns backend or integration errors from the policy implementation.
    async fn pre_receive(&self, _context: &PushPolicyContext) -> Result<PolicyDecision> {
        Ok(PolicyDecision::Allow)
    }

    /// Run an update style check for one ref update.
    ///
    /// # Errors
    ///
    /// Returns backend or integration errors from the policy implementation.
    async fn update(&self, _context: &RefUpdatePolicyContext) -> Result<PolicyDecision> {
        Ok(PolicyDecision::Allow)
    }

    /// Run a post-receive style hook after refs have been accepted.
    ///
    /// Post-receive hooks observe accepted writes and cannot reject already-applied ref updates.
    ///
    /// # Errors
    ///
    /// Returns backend or integration errors from the policy implementation.
    async fn post_receive(&self, _context: &PushPolicyContext) -> Result<()> {
        Ok(())
    }
}

/// Push policy that accepts every hook decision.
#[derive(Clone, Copy, Debug, Default)]
pub struct AllowAllPushPolicy;

#[async_trait]
impl PushPolicy for AllowAllPushPolicy {}

/// Repository policy that combines authorization with push hooks.
#[derive(Clone, Debug)]
pub struct RepositoryPolicy<A, P> {
    authorization: A,
    push_policy: P,
}

impl<A, P> RepositoryPolicy<A, P> {
    /// Create a repository policy from an `authorization` provider and `push_policy` hooks.
    #[must_use]
    pub fn new(authorization: A, push_policy: P) -> Self {
        Self {
            authorization,
            push_policy,
        }
    }
}

#[async_trait]
impl<A, P> PushPolicy for RepositoryPolicy<A, P>
where
    A: AuthorizationProvider,
    P: PushPolicy,
{
    async fn pre_receive(&self, context: &PushPolicyContext) -> Result<PolicyDecision> {
        let auth_context = AuthorizationContext {
            tenant: context.tenant.clone(),
            repository: context.repository.clone(),
            actor: context.actor.clone(),
            permission: RepositoryPermission::Write,
        };
        match self.authorization.check(&auth_context).await? {
            PolicyDecision::Allow => self.push_policy.pre_receive(context).await,
            PolicyDecision::Deny { reason } => Err(Error::AuthorizationDenied {
                tenant: auth_context.tenant.to_string(),
                repository: auth_context.repository.to_string(),
                actor: auth_context.actor.id,
                permission: auth_context.permission.as_str(),
                reason,
            }),
        }
    }

    async fn update(&self, context: &RefUpdatePolicyContext) -> Result<PolicyDecision> {
        self.push_policy.update(context).await
    }

    async fn post_receive(&self, context: &PushPolicyContext) -> Result<()> {
        self.push_policy.post_receive(context).await
    }
}

/// Simple protected-ref policy based on exact refs and prefixes.
#[derive(Clone, Debug, Default)]
pub struct ProtectedRefPolicy {
    protected_refs: Vec<ProtectedRefRule>,
}

#[derive(Clone, Debug)]
enum ProtectedRefRule {
    Exact(String),
    Prefix(String),
}

impl ProtectedRefPolicy {
    /// Create a policy that rejects updates to refs matching any prefix in `protected_prefixes`.
    #[must_use]
    pub fn new(protected_prefixes: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self {
            protected_refs: protected_prefixes
                .into_iter()
                .map(|prefix| ProtectedRefRule::Prefix(prefix.into()))
                .collect(),
        }
    }

    /// Create a policy that rejects exact branch names such as `main`.
    #[must_use]
    pub fn branches(branches: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self {
            protected_refs: branches
                .into_iter()
                .map(|branch| {
                    let branch = branch.into();
                    ProtectedRefRule::Exact(format!("refs/heads/{branch}"))
                })
                .collect(),
        }
    }

    /// Create a policy that rejects exact tag names such as `v1.0.0`.
    #[must_use]
    pub fn tags(tags: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self {
            protected_refs: tags
                .into_iter()
                .map(|tag| {
                    let tag = tag.into();
                    ProtectedRefRule::Exact(format!("refs/tags/{tag}"))
                })
                .collect(),
        }
    }
}

#[async_trait]
impl PushPolicy for ProtectedRefPolicy {
    async fn update(&self, context: &RefUpdatePolicyContext) -> Result<PolicyDecision> {
        if self
            .protected_refs
            .iter()
            .any(|rule| protected_ref_matches(rule, &context.update.refname))
        {
            return Ok(PolicyDecision::deny("protected ref"));
        }
        Ok(PolicyDecision::Allow)
    }
}

fn protected_ref_matches(rule: &ProtectedRefRule, refname: &str) -> bool {
    match rule {
        ProtectedRefRule::Exact(protected) => refname == protected,
        ProtectedRefRule::Prefix(prefix) => refname.starts_with(prefix),
    }
}

/// Outcome recorded for a hosted write audit event.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AuditOutcome {
    /// The write was accepted.
    Accepted,
    /// The write was rejected before all refs were applied.
    Rejected {
        /// Rejection reason captured from the typed error.
        reason: String,
    },
}

/// Audit record emitted for a receive-pack write attempt.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AuditEvent {
    /// Tenant that owns the repository.
    pub tenant: TenantId,
    /// Repository being pushed to.
    pub repository: RepositoryId,
    /// Actor that attempted the write.
    pub actor: PolicyActor,
    /// Ref updates requested by the write.
    pub updates: Vec<PolicyRefUpdate>,
    /// Commit objects introduced by the write pack.
    pub pushed_commits: Vec<ObjectId>,
    /// Caller-supplied event time.
    pub timestamp: OffsetDateTime,
    /// Accepted or rejected outcome.
    pub outcome: AuditOutcome,
}

impl AuditEvent {
    /// Create an accepted write audit event from a policy context and `timestamp`.
    #[must_use]
    pub fn accepted(context: &PushPolicyContext, timestamp: OffsetDateTime) -> Self {
        Self {
            tenant: context.tenant.clone(),
            repository: context.repository.clone(),
            actor: context.actor.clone(),
            updates: context.updates.clone(),
            pushed_commits: context.pushed_commits.clone(),
            timestamp,
            outcome: AuditOutcome::Accepted,
        }
    }

    /// Create a rejected write audit event from a policy context, `timestamp`, and `reason`.
    #[must_use]
    pub fn rejected(
        context: &PushPolicyContext,
        timestamp: OffsetDateTime,
        reason: impl Into<String>,
    ) -> Self {
        Self {
            tenant: context.tenant.clone(),
            repository: context.repository.clone(),
            actor: context.actor.clone(),
            updates: context.updates.clone(),
            pushed_commits: context.pushed_commits.clone(),
            timestamp,
            outcome: AuditOutcome::Rejected {
                reason: reason.into(),
            },
        }
    }
}

/// Explicit audit integration for hosted write attempts.
#[async_trait]
pub trait AuditSink: Send + Sync {
    /// Record `event` in the embedding platform's audit store.
    ///
    /// # Errors
    ///
    /// Returns backend or integration errors from the audit sink.
    async fn record(&self, event: &AuditEvent) -> Result<()>;
}

/// Audit sink that discards all events.
#[derive(Clone, Copy, Debug, Default)]
pub struct NoopAuditSink;

#[async_trait]
impl AuditSink for NoopAuditSink {
    async fn record(&self, _event: &AuditEvent) -> Result<()> {
        Ok(())
    }
}
