//! Bounded tree-path resolution and an optional decoded tree-block cache.
//!
//! Resolution operates only on immutable direct-tree blocks. A request memoizes every loaded
//! tree OID, while the optional shared cache retains decoded blocks under explicit byte budgets.
//! Neither layer is warmed by imports, and cache absence or eviction does not affect correctness.

use crate::error::Error;
use crate::sqlx_postgres::RepositoryPk;
use crate::tree_block::{find_tree_block_entry, TreeBlockEntry};
use async_trait::async_trait;
use grit_lib::objects::ObjectId;
use std::collections::{HashMap, VecDeque};
use std::mem::size_of;
use std::sync::{Arc, Mutex};

const GIT_TREE_MODE: u32 = 0o040000;

/// One validated raw component of a Git tree path.
///
/// Components exclude slash separators and cannot be empty or contain NUL bytes.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TreePathComponent(Vec<u8>);

impl TreePathComponent {
    /// Borrow the raw component bytes.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

impl TryFrom<Vec<u8>> for TreePathComponent {
    type Error = InvalidTreePathComponent;

    /// Validate and own one direct tree-entry name.
    ///
    /// # Errors
    ///
    /// Returns [`InvalidTreePathComponent`] for an empty name or one containing slash or NUL.
    fn try_from(value: Vec<u8>) -> Result<Self, Self::Error> {
        if value.is_empty() || value.contains(&0) || value.contains(&b'/') {
            return Err(InvalidTreePathComponent::Malformed);
        }
        Ok(Self(value))
    }
}

impl TryFrom<&[u8]> for TreePathComponent {
    type Error = InvalidTreePathComponent;

    /// Validate and copy one direct tree-entry name.
    ///
    /// # Errors
    ///
    /// Returns [`InvalidTreePathComponent`] for an empty name or one containing slash or NUL.
    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        let mut owned = Vec::new();
        owned
            .try_reserve_exact(value.len())
            .map_err(|_| InvalidTreePathComponent::AllocationFailed)?;
        owned.extend_from_slice(value);
        Self::try_from(owned)
    }
}

/// Error returned for a malformed direct tree-path component.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum InvalidTreePathComponent {
    /// The component was empty or contained slash or NUL.
    #[error("tree path components must be nonempty and cannot contain slash or NUL")]
    Malformed,
    /// Memory for an owned copy of the component could not be reserved.
    #[error("could not allocate memory for the tree path component")]
    AllocationFailed,
}

/// Work and input limits applied to one tree-path resolution request.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TreePathLimits {
    max_component_bytes: usize,
    max_depth: usize,
    max_path_bytes: usize,
    max_block_entries: usize,
    max_backend_reads: usize,
}

impl TreePathLimits {
    /// Create explicit limits for one path-resolution request.
    ///
    /// `max_component_bytes` bounds each raw component, `max_depth` bounds the number of
    /// components, `max_path_bytes` includes slash separators, `max_block_entries` bounds each
    /// decoded block, and `max_backend_reads` bounds loader calls after cache misses.
    #[must_use]
    pub const fn new(
        max_component_bytes: usize,
        max_depth: usize,
        max_path_bytes: usize,
        max_block_entries: usize,
        max_backend_reads: usize,
    ) -> Self {
        Self {
            max_component_bytes,
            max_depth,
            max_path_bytes,
            max_block_entries,
            max_backend_reads,
        }
    }

    /// Return the maximum bytes permitted in one path component.
    #[must_use]
    pub const fn max_component_bytes(self) -> usize {
        self.max_component_bytes
    }

    /// Return the maximum component count permitted in one path.
    #[must_use]
    pub const fn max_depth(self) -> usize {
        self.max_depth
    }

    /// Return the maximum total path bytes, including separators.
    #[must_use]
    pub const fn max_path_bytes(self) -> usize {
        self.max_path_bytes
    }

    /// Return the maximum decoded direct entries permitted in one tree block.
    #[must_use]
    pub const fn max_block_entries(self) -> usize {
        self.max_block_entries
    }

    /// Return the maximum backend loader calls permitted in one request.
    #[must_use]
    pub const fn max_backend_reads(self) -> usize {
        self.max_backend_reads
    }
}

/// Metadata for the final entry selected by a resolved tree path.
///
/// Blob payload bytes are deliberately absent so metadata browsing cannot accidentally download
/// object content.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ResolvedTreePath {
    /// Canonical Git mode stored by the parent tree.
    pub mode: u32,
    /// Immutable object identifier selected by the path.
    pub oid: ObjectId,
    /// Object payload size when the compact tree block records it.
    pub size: Option<u64>,
}

/// Narrow loading boundary used by bounded tree-path resolution.
#[async_trait]
pub trait TreeBlockLoader: Send + Sync {
    /// Load one decoded direct-tree block.
    ///
    /// `repository` and `tree_oid` form an immutable lookup key. Implementations that decode a
    /// retained tree object lazily must verify its object hash against `tree_oid` before returning
    /// entries. Returning `None` means the durable block is unavailable.
    ///
    /// # Errors
    ///
    /// Returns backend errors encountered while reading or decoding the block.
    async fn load_tree_block(
        &self,
        repository: RepositoryPk,
        tree_oid: ObjectId,
    ) -> crate::error::Result<Option<Arc<[TreeBlockEntry]>>>;
}

/// Failure while resolving a bounded tree path.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum TreePathResolveError {
    /// A component is longer than the configured per-component limit.
    #[error("tree path component {index} has {actual} bytes, exceeding limit {limit}")]
    ComponentBytesExceeded {
        /// Zero-based component index.
        index: usize,
        /// Actual component byte count.
        actual: usize,
        /// Configured maximum component byte count.
        limit: usize,
    },
    /// The component count is greater than the configured depth limit.
    #[error("tree path depth {actual} exceeds limit {limit}")]
    DepthExceeded {
        /// Actual component count.
        actual: usize,
        /// Configured maximum component count.
        limit: usize,
    },
    /// The complete slash-separated path is longer than its configured limit.
    #[error("tree path has {actual} bytes, exceeding limit {limit}")]
    PathBytesExceeded {
        /// Actual byte count including separators.
        actual: usize,
        /// Configured maximum path byte count.
        limit: usize,
    },
    /// A loaded block contains too many direct entries.
    #[error("tree block {tree_oid} has {actual} entries, exceeding limit {limit}")]
    BlockEntriesExceeded {
        /// Tree whose decoded block exceeded the limit.
        tree_oid: ObjectId,
        /// Actual direct-entry count.
        actual: usize,
        /// Configured maximum direct-entry count.
        limit: usize,
    },
    /// A loader returned names or object IDs that are not canonical for the requested tree key.
    #[error("decoded tree block {tree_oid} is not canonical")]
    InvalidTreeBlock {
        /// Tree key whose decoded entries failed validation.
        tree_oid: ObjectId,
    },
    /// Resolving the path would exceed the loader-call budget.
    #[error("tree path resolution exceeds backend read limit {limit}")]
    BackendReadsExceeded {
        /// Configured maximum backend reads.
        limit: usize,
    },
    /// A required decoded tree block was absent from durable storage.
    #[error("decoded tree block {tree_oid} is unavailable")]
    TreeBlockUnavailable {
        /// Missing tree object identifier.
        tree_oid: ObjectId,
    },
    /// A non-final component selected an entry that is not a Git tree.
    #[error("tree path component {index} selects non-tree mode {mode:#o}")]
    IntermediateEntryNotTree {
        /// Zero-based component index.
        index: usize,
        /// Mode stored by the parent tree.
        mode: u32,
    },
    /// The total path length overflowed the platform size type.
    #[error("tree path byte count overflowed")]
    PathBytesOverflow,
    /// Memory for request-local tree-block memoization could not be reserved.
    #[error("could not allocate memory for tree path memoization")]
    AllocationFailed,
    /// The durable loader failed.
    #[error(transparent)]
    Backend(#[from] Error),
}

/// Resolve a path from a root tree with bounded reads and per-request OID memoization.
///
/// `components` must contain validated direct names. The function checks all path input limits
/// before loading data, uses `cache` only as an optional acceleration layer, memoizes blocks by
/// immutable tree OID for the duration of the request, and verifies that every non-final entry has
/// Git tree mode. The returned value contains metadata only; `None` means a component was absent.
/// An empty path returns metadata for the root tree itself.
///
/// # Errors
///
/// Returns [`TreePathResolveError`] when an input or work bound is exceeded, an intermediate entry
/// is not a tree, a required tree block is unavailable, or the loader fails.
pub async fn resolve_tree_path<L>(
    loader: &L,
    cache: Option<&DecodedTreeBlockCache>,
    repository: RepositoryPk,
    root_tree_oid: ObjectId,
    components: &[TreePathComponent],
    limits: TreePathLimits,
) -> Result<Option<ResolvedTreePath>, TreePathResolveError>
where
    L: TreeBlockLoader + ?Sized,
{
    validate_path_limits(components, limits)?;
    if components.is_empty() {
        return Ok(Some(ResolvedTreePath {
            mode: GIT_TREE_MODE,
            oid: root_tree_oid,
            size: None,
        }));
    }

    let mut request_blocks: HashMap<ObjectId, Arc<[TreeBlockEntry]>> = HashMap::new();
    request_blocks
        .try_reserve(components.len())
        .map_err(|_| TreePathResolveError::AllocationFailed)?;
    let mut backend_reads = 0usize;
    let mut tree_oid = root_tree_oid;

    for (index, component) in components.iter().enumerate() {
        let entries = if let Some(entries) = request_blocks.get(&tree_oid) {
            Arc::clone(entries)
        } else if let Some(entries) = cache.and_then(|shared| shared.get(repository, tree_oid)) {
            request_blocks.insert(tree_oid, Arc::clone(&entries));
            entries
        } else {
            if backend_reads >= limits.max_backend_reads {
                return Err(TreePathResolveError::BackendReadsExceeded {
                    limit: limits.max_backend_reads,
                });
            }
            backend_reads += 1;
            let entries = loader
                .load_tree_block(repository, tree_oid)
                .await?
                .ok_or(TreePathResolveError::TreeBlockUnavailable { tree_oid })?;
            if entries.len() > limits.max_block_entries {
                return Err(TreePathResolveError::BlockEntriesExceeded {
                    tree_oid,
                    actual: entries.len(),
                    limit: limits.max_block_entries,
                });
            }
            if !is_canonical_tree_block(&entries, tree_oid) {
                return Err(TreePathResolveError::InvalidTreeBlock { tree_oid });
            }
            if let Some(shared) = cache {
                let _ = shared.insert(repository, tree_oid, Arc::clone(&entries));
            }
            request_blocks.insert(tree_oid, Arc::clone(&entries));
            entries
        };

        if entries.len() > limits.max_block_entries {
            return Err(TreePathResolveError::BlockEntriesExceeded {
                tree_oid,
                actual: entries.len(),
                limit: limits.max_block_entries,
            });
        }
        let Some(entry) = find_tree_block_entry(&entries, component.as_bytes()) else {
            return Ok(None);
        };
        let resolved = ResolvedTreePath {
            mode: entry.mode,
            oid: entry.oid,
            size: entry.size,
        };
        if index + 1 == components.len() {
            return Ok(Some(resolved));
        }
        if entry.mode != GIT_TREE_MODE {
            return Err(TreePathResolveError::IntermediateEntryNotTree {
                index,
                mode: entry.mode,
            });
        }
        tree_oid = entry.oid;
    }

    Ok(None)
}

fn validate_path_limits(
    components: &[TreePathComponent],
    limits: TreePathLimits,
) -> Result<(), TreePathResolveError> {
    if components.len() > limits.max_depth {
        return Err(TreePathResolveError::DepthExceeded {
            actual: components.len(),
            limit: limits.max_depth,
        });
    }
    let mut path_bytes = components.len().saturating_sub(1);
    for (index, component) in components.iter().enumerate() {
        let component_bytes = component.as_bytes().len();
        if component_bytes > limits.max_component_bytes {
            return Err(TreePathResolveError::ComponentBytesExceeded {
                index,
                actual: component_bytes,
                limit: limits.max_component_bytes,
            });
        }
        path_bytes = path_bytes
            .checked_add(component_bytes)
            .ok_or(TreePathResolveError::PathBytesOverflow)?;
    }
    if path_bytes > limits.max_path_bytes {
        return Err(TreePathResolveError::PathBytesExceeded {
            actual: path_bytes,
            limit: limits.max_path_bytes,
        });
    }
    Ok(())
}

/// Explicit retained-payload budgets for a shared decoded tree-block cache.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TreeBlockCacheLimits {
    global_bytes: usize,
    per_repository_bytes: usize,
    item_bytes: usize,
}

impl TreeBlockCacheLimits {
    /// Validate and create shared decoded-block cache budgets.
    ///
    /// `global_bytes` bounds all retained payloads, `per_repository_bytes` bounds one repository,
    /// and `item_bytes` bounds one decoded block.
    ///
    /// # Errors
    ///
    /// Returns [`InvalidTreeBlockCacheLimits`] when any budget is zero, the repository budget is
    /// larger than the global budget, or the item budget is larger than the repository budget.
    pub fn new(
        global_bytes: usize,
        per_repository_bytes: usize,
        item_bytes: usize,
    ) -> Result<Self, InvalidTreeBlockCacheLimits> {
        if global_bytes == 0
            || per_repository_bytes == 0
            || item_bytes == 0
            || per_repository_bytes > global_bytes
            || item_bytes > per_repository_bytes
        {
            return Err(InvalidTreeBlockCacheLimits);
        }
        Ok(Self {
            global_bytes,
            per_repository_bytes,
            item_bytes,
        })
    }

    /// Return the process-wide retained-payload byte cap.
    #[must_use]
    pub const fn global_bytes(self) -> usize {
        self.global_bytes
    }

    /// Return the retained-payload byte cap for one repository.
    #[must_use]
    pub const fn per_repository_bytes(self) -> usize {
        self.per_repository_bytes
    }

    /// Return the retained-payload byte cap for one decoded block.
    #[must_use]
    pub const fn item_bytes(self) -> usize {
        self.item_bytes
    }
}

/// Error returned for inconsistent or zero shared-cache byte budgets.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
#[error("tree-block cache limits must be nonzero and ordered item <= repository <= global")]
pub struct InvalidTreeBlockCacheLimits;

/// Outcome of inserting one immutable decoded block into the shared cache.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TreeBlockCacheInsert {
    /// The block was inserted, possibly after deterministic eviction.
    Inserted,
    /// The immutable key was already cached and its existing value was retained.
    AlreadyPresent,
    /// The block exceeded the configured per-item retained-payload byte cap.
    ItemTooLarge,
    /// Retained-size accounting overflowed the platform size type.
    AccountingOverflow,
    /// The block was not a canonical decoded tree block for the keyed hash algorithm.
    InvalidBlock,
    /// Cache index storage could not be reserved before mutation.
    AllocationFailed,
}

/// A byte-bounded shared cache of immutable decoded direct-tree blocks.
///
/// The mutex guards entries, FIFO order, and byte counters as one coherent state. Eviction is
/// deterministic and uses insertion order without consulting a clock. Accounting covers the
/// retained entry-array and name capacities plus a conservative fixed charge for the `Arc`, cache
/// record, key indexes, and allocation headers. Every item, including an empty block, therefore
/// consumes a nonzero budget and the allocator bookkeeping excluded from the estimate remains
/// bounded by the byte limits. Shared `Arc` payloads are charged independently for each cache key,
/// which deliberately overcounts rather than allowing aliased memory to bypass a repository cap.
/// This cache does not provide singleflight, so simultaneous cold misses may issue duplicate reads.
pub struct DecodedTreeBlockCache {
    limits: TreeBlockCacheLimits,
    state: Mutex<TreeBlockCacheState>,
}

impl DecodedTreeBlockCache {
    /// Create an empty decoded-block cache using validated byte limits.
    #[must_use]
    pub fn new(limits: TreeBlockCacheLimits) -> Self {
        Self {
            limits,
            state: Mutex::new(TreeBlockCacheState::default()),
        }
    }

    /// Return the configured cache byte limits.
    #[must_use]
    pub const fn limits(&self) -> TreeBlockCacheLimits {
        self.limits
    }

    /// Return an immutable decoded block when it is cached.
    ///
    /// This read does not change FIFO order and therefore needs no clock.
    #[must_use]
    pub fn get(
        &self,
        repository: RepositoryPk,
        tree_oid: ObjectId,
    ) -> Option<Arc<[TreeBlockEntry]>> {
        let state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        state
            .entries
            .get(&TreeBlockCacheKey {
                repository,
                tree_oid,
            })
            .map(|entry| Arc::clone(&entry.block))
    }

    /// Insert one immutable decoded block under its repository and tree OID.
    ///
    /// Blocks over the per-item cap are ignored. Repository-local FIFO entries are evicted before
    /// insertion to satisfy the repository cap, followed by process-wide FIFO eviction when needed.
    #[must_use]
    pub fn insert(
        &self,
        repository: RepositoryPk,
        tree_oid: ObjectId,
        block: Arc<[TreeBlockEntry]>,
    ) -> TreeBlockCacheInsert {
        if !is_canonical_tree_block(&block, tree_oid) {
            return TreeBlockCacheInsert::InvalidBlock;
        }
        let Some(retained_bytes) = retained_tree_block_bytes(&block) else {
            return TreeBlockCacheInsert::AccountingOverflow;
        };
        if retained_bytes > self.limits.item_bytes {
            return TreeBlockCacheInsert::ItemTooLarge;
        }
        let key = TreeBlockCacheKey {
            repository,
            tree_oid,
        };
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        if state.entries.contains_key(&key) {
            return TreeBlockCacheInsert::AlreadyPresent;
        }
        if state.entries.try_reserve(1).is_err()
            || state.fifo.try_reserve(1).is_err()
            || (!state.per_repository.contains_key(&repository)
                && state.per_repository.try_reserve(1).is_err())
        {
            return TreeBlockCacheInsert::AllocationFailed;
        }

        while state.repository_bytes(repository)
            > self
                .limits
                .per_repository_bytes
                .saturating_sub(retained_bytes)
        {
            let Some(position) = state
                .fifo
                .iter()
                .position(|candidate| candidate.repository == repository)
            else {
                break;
            };
            if let Some(candidate) = state.fifo.remove(position) {
                state.remove_entry(candidate);
            }
        }
        while state.total_bytes > self.limits.global_bytes.saturating_sub(retained_bytes) {
            let Some(candidate) = state.fifo.pop_front() else {
                break;
            };
            state.remove_entry(candidate);
        }

        let Some(total_bytes) = state.total_bytes.checked_add(retained_bytes) else {
            return TreeBlockCacheInsert::AccountingOverflow;
        };
        let Some(repository_bytes) = state
            .repository_bytes(repository)
            .checked_add(retained_bytes)
        else {
            return TreeBlockCacheInsert::AccountingOverflow;
        };
        state.total_bytes = total_bytes;
        state.per_repository.insert(repository, repository_bytes);
        state.fifo.push_back(key);
        state.entries.insert(
            key,
            CachedTreeBlock {
                block,
                retained_bytes,
            },
        );
        TreeBlockCacheInsert::Inserted
    }

    /// Return the exact retained-payload bytes currently charged to the cache.
    #[must_use]
    pub fn retained_bytes(&self) -> usize {
        self.state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .total_bytes
    }

    /// Return the exact retained-payload bytes currently charged to one repository.
    #[must_use]
    pub fn repository_retained_bytes(&self, repository: RepositoryPk) -> usize {
        self.state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .repository_bytes(repository)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
struct TreeBlockCacheKey {
    repository: RepositoryPk,
    tree_oid: ObjectId,
}

struct CachedTreeBlock {
    block: Arc<[TreeBlockEntry]>,
    retained_bytes: usize,
}

#[derive(Default)]
struct TreeBlockCacheState {
    entries: HashMap<TreeBlockCacheKey, CachedTreeBlock>,
    fifo: VecDeque<TreeBlockCacheKey>,
    per_repository: HashMap<RepositoryPk, usize>,
    total_bytes: usize,
}

impl TreeBlockCacheState {
    fn repository_bytes(&self, repository: RepositoryPk) -> usize {
        self.per_repository
            .get(&repository)
            .copied()
            .unwrap_or_default()
    }

    fn remove_entry(&mut self, key: TreeBlockCacheKey) {
        let Some(removed) = self.entries.remove(&key) else {
            return;
        };
        self.total_bytes = self.total_bytes.saturating_sub(removed.retained_bytes);
        let should_remove_repository =
            if let Some(repository_bytes) = self.per_repository.get_mut(&key.repository) {
                *repository_bytes = repository_bytes.saturating_sub(removed.retained_bytes);
                *repository_bytes == 0
            } else {
                false
            };
        if should_remove_repository {
            self.per_repository.remove(&key.repository);
        }
    }
}

fn retained_tree_block_bytes(block: &[TreeBlockEntry]) -> Option<usize> {
    // Include enough fixed structural charge that zero-entry blocks cannot create an unbounded
    // number of map/FIFO records while consuming zero configured bytes. Exact allocator metadata
    // is platform-specific; this conservative estimate intentionally double-charges shared Arcs.
    let fixed_bytes = size_of::<CachedTreeBlock>()
        .checked_add(size_of::<TreeBlockCacheKey>().checked_mul(2)?)?
        .checked_add(size_of::<usize>().checked_mul(2)?)?;
    fixed_bytes.checked_add(
        block
            .len()
            .checked_mul(size_of::<TreeBlockEntry>())?
            .checked_add(block.iter().try_fold(0usize, |total, entry| {
                total.checked_add(entry.name.capacity())
            })?)?,
    )
}

fn is_canonical_tree_block(block: &[TreeBlockEntry], tree_oid: ObjectId) -> bool {
    let mut previous_name: Option<&[u8]> = None;
    block.iter().all(|entry| {
        let name = entry.name.as_slice();
        let valid = !name.is_empty()
            && !name.contains(&0)
            && !name.contains(&b'/')
            && previous_name.is_none_or(|previous| previous < name)
            && entry.oid.algo() == tree_oid.algo();
        previous_name = Some(name);
        valid
    })
}
