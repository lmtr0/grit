//! Error types for server-backed repository storage.

use thiserror::Error;

/// Result alias for `grit-lib-server` operations.
pub type Result<T> = std::result::Result<T, Error>;

/// Errors returned by server-backed repository storage.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum Error {
    /// A repository identifier was empty or otherwise invalid.
    #[error("invalid repository id: {0}")]
    InvalidRepositoryId(String),
    /// A tenant identifier was empty or otherwise invalid.
    #[error("invalid tenant id: {0}")]
    InvalidTenantId(String),
    /// A requested repository was not found.
    #[error("repository not found: {0}")]
    RepositoryNotFound(String),
    /// A repository could not be created or renamed because the destination exists.
    #[error("repository already exists: {0}")]
    RepositoryAlreadyExists(String),
    /// A multi-operation PostgreSQL transaction attempted to switch repository lock scope.
    #[error("transaction repository scope is {locked}, cannot mutate {requested}")]
    TransactionRepositoryScope {
        /// Repository identity already locked by the transaction.
        locked: String,
        /// Different repository identity or lifecycle pair requested later.
        requested: String,
    },
    /// A failed transaction-scoped lifecycle operation requires the transaction to end.
    #[error("transaction lifecycle scope is poisoned, cannot mutate {requested}")]
    TransactionLifecyclePoisoned {
        /// Child or lifecycle mutation rejected after the failed lifecycle operation.
        requested: String,
    },
    /// A requested ref was not found.
    #[error("ref not found: {0}")]
    RefNotFound(String),
    /// A compare-and-swap ref update failed because the stored value changed.
    #[error("ref update conflict: {0}")]
    RefConflict(String),
    /// A ref cannot be created because another ref occupies its namespace.
    #[error("ref namespace conflict: {refname} conflicts with {existing}")]
    RefNamespaceConflict {
        /// Ref that was being created or updated.
        refname: String,
        /// Existing or queued ref that conflicts with `refname`.
        existing: String,
    },
    /// A branch update was rejected because it is not a fast-forward.
    #[error("non-fast-forward update rejected for {refname}: {old_oid} -> {new_oid}")]
    NonFastForward {
        /// Ref that was rejected.
        refname: String,
        /// Current branch tip.
        old_oid: String,
        /// Proposed branch tip.
        new_oid: String,
    },
    /// An import session was stale, replayed, or used for a different repository.
    #[error("import session {generation} is stale for {tenant}/{repository}")]
    StaleImportSession {
        /// Tenant whose import session was superseded.
        tenant: String,
        /// Repository whose import session was superseded.
        repository: String,
        /// Superseded repository-local import generation.
        generation: u64,
    },
    /// A push policy hook rejected a ref update.
    #[error("push policy rejected {refname}: {reason}")]
    PushPolicyRejected {
        /// Ref that was rejected.
        refname: String,
        /// Policy-provided reason.
        reason: String,
    },
    /// A repository authorization provider denied access.
    #[error("authorization denied for {actor} on {tenant}/{repository} {permission}: {reason}")]
    AuthorizationDenied {
        /// Tenant whose repository was accessed.
        tenant: String,
        /// Repository that was accessed.
        repository: String,
        /// Actor that requested access.
        actor: String,
        /// Permission that was denied.
        permission: &'static str,
        /// Provider-supplied reason.
        reason: String,
    },
    /// A requested object was not found.
    #[error("object not found: {0}")]
    ObjectNotFound(String),
    /// A requested tree path was not found.
    #[error("path not found: {0}")]
    PathNotFound(String),
    /// A stored object had an unexpected Git object kind.
    #[error("expected {expected} object, found {actual}")]
    UnexpectedObjectKind {
        /// Expected object kind.
        expected: &'static str,
        /// Actual object kind.
        actual: &'static str,
    },
    /// A backend rejected the operation.
    #[error("backend error: {0}")]
    Backend(String),
    /// A cache backend rejected the operation.
    #[error("cache error: {0}")]
    Cache(String),
    /// A protocol request was malformed or cannot be served.
    #[error("protocol error: {0}")]
    Protocol(String),
    /// An I/O operation failed while framing protocol data.
    #[error(transparent)]
    Io(#[from] std::io::Error),
    /// An error from `grit-lib`.
    #[error(transparent)]
    Grit(#[from] grit_lib::error::Error),
    /// An error from SQLx.
    #[cfg(feature = "sqlx-postgres")]
    #[error(transparent)]
    Sqlx(#[from] sqlx::Error),
}
