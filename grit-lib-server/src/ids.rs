//! Stable identifiers for hosted repositories.

use std::fmt;

use crate::error::{Error, Result};

/// Tenant identifier used to partition hosted repositories.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TenantId(String);

impl TenantId {
    /// Create a tenant identifier.
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidTenantId`] when the value is empty after trimming.
    pub fn new(value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        if value.trim().is_empty() {
            return Err(Error::InvalidTenantId(value));
        }
        Ok(Self(value))
    }

    /// Borrow the tenant identifier as a string slice.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for TenantId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// Repository identifier scoped under a [`TenantId`].
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RepositoryId(String);

impl RepositoryId {
    /// Create a repository identifier.
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidRepositoryId`] when the value is empty after trimming.
    pub fn new(value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        if value.trim().is_empty() {
            return Err(Error::InvalidRepositoryId(value));
        }
        Ok(Self(value))
    }

    /// Borrow the repository identifier as a string slice.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for RepositoryId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}
