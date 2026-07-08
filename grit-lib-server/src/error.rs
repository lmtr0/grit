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
    /// A requested ref was not found.
    #[error("ref not found: {0}")]
    RefNotFound(String),
    /// A compare-and-swap ref update failed because the stored value changed.
    #[error("ref update conflict: {0}")]
    RefConflict(String),
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
    /// An error from `grit-lib`.
    #[error(transparent)]
    Grit(#[from] grit_lib::error::Error),
    /// An error from SQLx.
    #[cfg(feature = "sqlx-postgres")]
    #[error(transparent)]
    Sqlx(#[from] sqlx::Error),
}
