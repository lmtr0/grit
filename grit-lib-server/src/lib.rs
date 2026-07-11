//! Server-side storage layer for hosting Grit repositories.
//!
//! This crate defines the backend boundary needed to host Git repositories without a filesystem
//! repository as the source of truth. `grit-lib` remains the Git-compatible engine; this crate
//! supplies repository identity, storage traits, cache invalidation hooks, and backend
//! implementations that can be adapted into protocol serving and UI browsing paths.

pub mod cache;
pub mod cached;
pub mod error;
pub mod ids;
pub mod import;
pub mod memory;
pub mod repository;
#[cfg(feature = "sqlx-postgres")]
pub mod sqlx_postgres;
pub mod storage;
pub mod views;

/// Commonly used server-layer types.
pub mod prelude {
    pub use crate::cache::{Cache, CacheKey, CacheValue, EventPublisher, InvalidationEvent};
    pub use crate::cached::CachedStorage;
    pub use crate::error::{Error, Result};
    pub use crate::ids::{RepositoryId, TenantId};
    pub use crate::import::{import_repository, ImportReport};
    pub use crate::repository::ServerRepository;
    pub use crate::storage::{
        BrowseIndex, ConfigStore, ObjectStore, RefStore, ReflogEntry, ReflogStore, StoredObject,
        StoredRef,
    };
    pub use crate::views::{
        BlobView, BranchView, CommitSummary, CompareInputs, DiscoveredFile, RepositorySummary,
        TagView, TreeEntryView, TreeView,
    };
}
