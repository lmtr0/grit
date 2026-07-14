//! Runtime-neutral bounded scheduling for parallel receive-pack validation.
//!
//! The sequential pack validator remains the compatibility implementation. This module provides
//! an optional scheduling layer for validators that can execute independent entry validation in
//! parallel without retaining every decoded object.

use std::collections::HashMap;

use async_trait::async_trait;
use grit_lib::objects::ObjectId;
use time::OffsetDateTime;

use crate::protocol::push_metrics::ReceivePackLimits;
use crate::protocol::push_pack_validation::{
    PushPackEntryKind, PushPackIndexRow, PushPackValidationControl,
};

/// Conservative byte charges for one validation task.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ParallelValidationByteCost {
    /// Compressed input bytes retained or read for the task.
    pub compressed_input: u64,
    /// Resolved base bytes borrowed by a delta task.
    pub base: u64,
    /// Inflated delta instruction bytes.
    pub instructions: u64,
    /// Resolved result bytes.
    pub result: u64,
    /// Structural metadata retained after payload validation.
    pub structural_metadata: u64,
}

impl ParallelValidationByteCost {
    /// Return the checked aggregate admission charge.
    ///
    /// # Errors
    ///
    /// Returns [`ParallelValidationError::InvalidCost`] when the fields overflow `u64`.
    pub fn total(self) -> Result<u64, ParallelValidationError> {
        self.compressed_input
            .checked_add(self.base)
            .and_then(|value| value.checked_add(self.instructions))
            .and_then(|value| value.checked_add(self.result))
            .and_then(|value| value.checked_add(self.structural_metadata))
            .ok_or(ParallelValidationError::InvalidCost)
    }
}

/// Admission estimate supplied for one indexed PACK entry.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ParallelValidationCost {
    /// Every material working-set component is known.
    Known(ParallelValidationByteCost),
    /// The estimate is uncertain, so the task must run alone under this hard upper bound.
    Unknown {
        /// Maximum bytes the executor is allowed to materialize.
        hard_upper_bound: u64,
    },
}

/// Validated internal dependency for one scheduler task.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ParallelValidationDependency {
    /// A direct object has no decoded base dependency.
    Direct,
    /// An OFS delta depends on the indexed entry at `base_index`.
    OfsDelta {
        /// Zero-based input index of the base entry.
        base_index: usize,
    },
    /// A REF delta depends on an object in this pack.
    InternalRefDelta {
        /// Zero-based input index of the base entry.
        base_index: usize,
    },
    /// A REF delta must read an already-authorized repository base.
    ExternalRefDelta {
        /// Canonical external base object ID.
        base_oid: ObjectId,
    },
}

impl ParallelValidationDependency {
    fn base_index(self) -> Option<usize> {
        match self {
            Self::OfsDelta { base_index } | Self::InternalRefDelta { base_index } => {
                Some(base_index)
            }
            Self::Direct | Self::ExternalRefDelta { .. } => None,
        }
    }

    fn is_delta(self) -> bool {
        !matches!(self, Self::Direct)
    }
}

/// Explicit bounds for one reusable per-push validation scheduler.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ParallelValidationLimits {
    /// Shared receive-pack safety limits.
    pub receive: ReceivePackLimits,
    /// Maximum tasks admitted in one executor wave.
    pub worker_count: usize,
    /// Maximum external-base range tasks in one wave.
    pub range_concurrency: usize,
    /// Normal aggregate task working-set bytes admitted in one wave.
    pub wave_bytes: u64,
    /// Absolute per-task ceiling, including unknown or oversized tasks run alone.
    pub hard_task_bytes: u64,
    /// Maximum decoded base bytes retained between waves.
    pub base_cache_bytes: u64,
    /// Explicit scheduler start time.
    pub started_at: OffsetDateTime,
    /// Explicit scheduler deadline.
    pub deadline: OffsetDateTime,
}

impl ParallelValidationLimits {
    /// Validate concurrency, memory, and explicit-time relationships.
    ///
    /// # Errors
    ///
    /// Returns [`ParallelValidationError::InvalidLimits`] for zero, inconsistent, or unsafe
    /// limits.
    pub fn validate(self) -> Result<Self, ParallelValidationError> {
        let receive = self
            .receive
            .validate()
            .map_err(|_| ParallelValidationError::InvalidLimits)?;
        let duration = time::Duration::try_from(receive.max_duration)
            .map_err(|_| ParallelValidationError::InvalidLimits)?;
        let hard = ReceivePackLimits::HARD_MAX;
        if self.worker_count == 0
            || self.worker_count > receive.worker_count
            || self.range_concurrency == 0
            || self.range_concurrency > self.worker_count
            || self.wave_bytes == 0
            || self.wave_bytes > receive.bytes_in_flight
            || self.hard_task_bytes == 0
            || self.hard_task_bytes > receive.bytes_in_flight
            || self.hard_task_bytes > hard.bytes_in_flight
            || self.base_cache_bytes == 0
            || self.base_cache_bytes > receive.max_delta_base_memory_bytes
            || self.deadline <= self.started_at
            || self.deadline - self.started_at > duration
        {
            return Err(ParallelValidationError::InvalidLimits);
        }
        Ok(Self { receive, ..self })
    }
}

/// Deterministic work budget charged by scheduler bookkeeping and executor admission.
pub trait ParallelValidationWork {
    /// Charge deterministic work units, returning `false` if the budget is exhausted.
    fn charge(&mut self, units: u64) -> bool;
}

/// Simple finite implementation of [`ParallelValidationWork`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ParallelValidationWorkLimit {
    remaining: u64,
}

impl ParallelValidationWorkLimit {
    /// Create a work budget containing `units` deterministic units.
    #[must_use]
    pub const fn new(units: u64) -> Self {
        Self { remaining: units }
    }

    /// Return remaining deterministic work units.
    #[must_use]
    pub const fn remaining(&self) -> u64 {
        self.remaining
    }
}

impl ParallelValidationWork for ParallelValidationWorkLimit {
    fn charge(&mut self, units: u64) -> bool {
        let Some(remaining) = self.remaining.checked_sub(units) else {
            return false;
        };
        self.remaining = remaining;
        true
    }
}

/// One decoded base retained only while unresolved dependents still need it.
#[derive(Clone, Debug)]
pub struct ParallelValidationRetainedBase<B> {
    /// Executor-specific read-only base handle.
    pub handle: B,
    /// Bytes retained by the handle and charged to the base cache.
    pub bytes: u64,
}

/// One task in a bounded deterministic executor wave.
#[derive(Clone, Debug)]
pub struct ParallelValidationTask<B> {
    /// Stable zero-based input index.
    pub index: usize,
    /// Original validated index row.
    pub row: PushPackIndexRow,
    /// Scheduler-resolved dependency.
    pub dependency: ParallelValidationDependency,
    /// Internal decoded base handle, present only for internal deltas.
    pub base: Option<B>,
    /// Delta depth the executor must produce.
    pub expected_delta_depth: u32,
    /// Hard byte ceiling for this task.
    pub byte_limit: u64,
}

/// Successful result of one parallel validation task.
#[derive(Clone, Debug)]
pub struct ParallelValidationTaskOutput<B, M> {
    /// Structural/index metadata retained for deterministic publication order.
    ///
    /// This value must not contain decoded blob payload bytes.
    pub metadata: M,
    /// Produced delta depth, checked against the scheduler's propagated depth.
    pub delta_depth: u32,
    /// Decoded result retained as a base only when later entries depend on it.
    pub retained_base: Option<ParallelValidationRetainedBase<B>>,
}

/// Non-sensitive validator rejection for one indexed entry.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ParallelValidationTaskFailure {
    /// Entry zlib data is malformed or has trailing bytes.
    CompressedStream,
    /// Canonical object hashing failed validation.
    CanonicalHash,
    /// Delta instructions or result are invalid.
    Delta,
    /// Structural parsing or metadata sizing failed.
    StructuralMetadata,
    /// An authorized external base could not be read or verified.
    ExternalBase,
    /// The executor exceeded the task's supplied hard byte ceiling.
    ByteLimit,
}

/// Tagged completion returned for every task in an executor wave.
#[derive(Clone, Debug)]
pub struct ParallelValidationTaskResult<B, M> {
    /// Stable input index copied from the task.
    pub index: usize,
    /// Successful bounded output or typed validation failure.
    pub result: Result<ParallelValidationTaskOutput<B, M>, ParallelValidationTaskFailure>,
}

/// Non-entry failure returned after an executor has reaped its active wave.
#[derive(Clone, Copy, Debug, thiserror::Error, PartialEq, Eq)]
pub enum ParallelValidationExecutorError {
    /// Explicit cancellation was observed by the executor.
    #[error("parallel validation executor cancelled")]
    Cancelled,
    /// The explicit deadline elapsed in the executor.
    #[error("parallel validation executor deadline elapsed")]
    Deadline,
    /// A worker or backend operation failed without an entry-level rejection.
    #[error("parallel validation executor failed")]
    Backend,
}

/// Pluggable parallel execution boundary for zlib, hashing, delta, and structural validation.
///
/// The scheduler calls this once per bounded wave. Implementations may complete tasks in any
/// order, but must return exactly one tagged result for every task. Before returning, including on
/// cancellation or error, an implementation must join or reap all active work from the wave; it
/// must never leave detached executor tasks. It must also enforce each task's `byte_limit` while
/// materializing input, instructions, results, bases, and metadata.
#[async_trait]
pub trait ParallelValidationExecutor: Send {
    /// Executor-specific shareable decoded-base handle.
    type Base: Clone + Send;
    /// Bounded structural/index metadata, excluding decoded blob payload bytes.
    type Metadata: Send;

    /// Execute one scheduler-admitted wave and reap all of its work before returning.
    ///
    /// `deadline` is caller-supplied; implementations must not derive a hidden deadline from the
    /// system clock.
    async fn execute_wave(
        &mut self,
        tasks: Vec<ParallelValidationTask<Self::Base>>,
        deadline: OffsetDateTime,
    ) -> Result<
        Vec<ParallelValidationTaskResult<Self::Base, Self::Metadata>>,
        ParallelValidationExecutorError,
    >;
}

/// Deterministically ordered metadata produced by parallel validation.
#[derive(Clone, Debug)]
pub struct ParallelValidationReport<M> {
    /// Metadata in original PACK index order, independent of worker completion order.
    pub metadata: Vec<M>,
    /// Number of bounded executor waves.
    pub waves: u64,
    /// Maximum tasks admitted in a wave.
    pub peak_tasks: usize,
    /// Maximum byte charge admitted in a wave.
    pub peak_wave_bytes: u64,
    /// Maximum retained decoded-base cache bytes.
    pub peak_base_cache_bytes: u64,
}

/// Typed failure from preflight DAG construction, admission, execution, or result verification.
#[derive(Clone, Copy, Debug, thiserror::Error, PartialEq, Eq)]
pub enum ParallelValidationError {
    /// Scheduler limits are zero, inconsistent, or above receive-pack hard bounds.
    #[error("invalid parallel validation limits")]
    InvalidLimits,
    /// Cost vector length differs from the input index length or a cost overflowed.
    #[error("invalid parallel validation cost")]
    InvalidCost,
    /// A task's conservative bound exceeds the configured absolute ceiling.
    #[error("parallel validation task exceeds the hard byte limit")]
    TaskTooLarge,
    /// An OFS/REF relation refers to no unique indexed base.
    #[error("parallel validation dependency is missing or ambiguous")]
    MissingDependency,
    /// The indexed dependency graph contains a cycle.
    #[error("parallel validation dependency graph contains a cycle")]
    DependencyCycle,
    /// Retained dependency bases exceeded their byte cap or were absent.
    #[error("parallel validation decoded base cache exceeded its bound")]
    BaseCache,
    /// Dependency and ready-queue bookkeeping exceeded receive-pack metadata memory.
    #[error("parallel validation scheduler metadata exceeds its bound")]
    SchedulerMemory,
    /// Explicit cancellation was observed.
    #[error("parallel validation cancelled")]
    Cancelled,
    /// Explicit deadline elapsed or observed time moved backwards.
    #[error("parallel validation deadline elapsed")]
    Deadline,
    /// Deterministic work budget was exhausted.
    #[error("parallel validation work budget exhausted")]
    WorkBudget,
    /// Executor failed after joining the active wave.
    #[error("parallel validation executor failed")]
    Executor,
    /// Executor returned missing, duplicate, or unexpected task results.
    #[error("parallel validation executor violated its wave contract")]
    ExecutorContract,
    /// Lowest-input-order validation failure in the deterministic wave.
    #[error("parallel validation entry rejected: {failure:?}")]
    Entry {
        /// Stable input index of the rejected entry.
        index: usize,
        /// Typed validation failure.
        failure: ParallelValidationTaskFailure,
    },
    /// Executor returned a depth different from dependency propagation.
    #[error("parallel validation executor returned an invalid delta depth")]
    DeltaDepth,
}

#[derive(Debug)]
struct DependencyGraph {
    dependencies: Vec<ParallelValidationDependency>,
    children: Vec<Vec<usize>>,
    indegrees: Vec<usize>,
}

#[derive(Clone, Debug)]
struct CachedBase<B> {
    handle: B,
    bytes: u64,
    remaining_dependents: usize,
}

/// Validate indexed PACK entries with deterministic bounded parallel waves.
///
/// Preflight constructs and validates the complete OFS/REF dependency DAG before starting any
/// executor work. Ready tasks are ordered by entry offset and then input index. Internal deltas
/// receive refcounted base handles only after their bases validate. The returned metadata is
/// always restored to input order.
///
/// # Errors
///
/// Returns a typed limit, graph, cancellation, work, executor-contract, cache, depth, or entry
/// validation failure. If an executor wave contains multiple entry failures, the failure with the
/// lowest original input index is returned, irrespective of completion order.
pub async fn validate_index_parallel<E, C, W>(
    rows: &[PushPackIndexRow],
    costs: &[ParallelValidationCost],
    executor: &mut E,
    control: &mut C,
    work: &mut W,
    limits: ParallelValidationLimits,
) -> Result<ParallelValidationReport<E::Metadata>, ParallelValidationError>
where
    E: ParallelValidationExecutor,
    C: PushPackValidationControl,
    W: ParallelValidationWork,
{
    let limits = limits.validate()?;
    if rows.len() != costs.len() || rows.len() > limits.receive.max_objects {
        return Err(ParallelValidationError::InvalidCost);
    }
    validate_scheduler_memory::<E::Base, E::Metadata>(
        rows.len(),
        costs,
        limits.worker_count,
        limits.receive.max_metadata_memory_bytes,
    )?;
    let mut last_observed_at = limits.started_at;
    checkpoint(
        control,
        work,
        limits.started_at,
        limits.deadline,
        &mut last_observed_at,
        1,
    )?;
    let graph = build_dependency_graph(
        rows,
        control,
        work,
        limits.started_at,
        limits.deadline,
        &mut last_observed_at,
    )?;
    let totals = validate_costs(costs, limits.hard_task_bytes)?;

    let mut indegrees = graph.indegrees;
    let mut ready = Vec::new();
    ready
        .try_reserve_exact(rows.len())
        .map_err(|_| ParallelValidationError::SchedulerMemory)?;
    for (index, indegree) in indegrees.iter().copied().enumerate() {
        if indegree == 0 {
            ready.push((rows[index].entry.offset, index));
        }
    }
    ready.sort_unstable();

    let mut metadata = Vec::new();
    metadata
        .try_reserve_exact(rows.len())
        .map_err(|_| ParallelValidationError::SchedulerMemory)?;
    metadata.resize_with(rows.len(), || None);
    let mut depths = Vec::new();
    depths
        .try_reserve_exact(rows.len())
        .map_err(|_| ParallelValidationError::SchedulerMemory)?;
    depths.resize(rows.len(), None);
    let mut cache: HashMap<usize, CachedBase<E::Base>> = HashMap::new();
    cache
        .try_reserve(rows.len())
        .map_err(|_| ParallelValidationError::SchedulerMemory)?;
    let mut cache_bytes = 0_u64;
    let mut completed = 0_usize;
    let mut waves = 0_u64;
    let mut peak_tasks = 0_usize;
    let mut peak_wave_bytes = 0_u64;
    let mut peak_base_cache_bytes = 0_u64;

    while completed < rows.len() {
        checkpoint(
            control,
            work,
            limits.started_at,
            limits.deadline,
            &mut last_observed_at,
            u64::try_from(ready.len())
                .ok()
                .and_then(|ready| ready.checked_add(1))
                .ok_or(ParallelValidationError::WorkBudget)?,
        )?;
        let available_bytes = limits
            .receive
            .bytes_in_flight
            .checked_sub(cache_bytes)
            .ok_or(ParallelValidationError::BaseCache)?;
        let selected = select_wave(
            &ready,
            costs,
            &totals,
            &graph.dependencies,
            limits.worker_count,
            limits.range_concurrency,
            limits.wave_bytes,
            available_bytes,
        )?;
        if selected.is_empty() {
            return Err(if ready.is_empty() {
                ParallelValidationError::DependencyCycle
            } else {
                ParallelValidationError::TaskTooLarge
            });
        }
        let mut selected_set = std::collections::HashSet::new();
        selected_set
            .try_reserve(selected.len())
            .map_err(|_| ParallelValidationError::SchedulerMemory)?;
        selected_set.extend(selected.iter().copied());
        ready.retain(|(_, index)| !selected_set.contains(index));

        let wave_bytes = selected.iter().try_fold(0_u64, |sum, index| {
            sum.checked_add(totals[*index])
                .ok_or(ParallelValidationError::InvalidCost)
        })?;
        let mut tasks = Vec::new();
        tasks
            .try_reserve_exact(selected.len())
            .map_err(|_| ParallelValidationError::SchedulerMemory)?;
        for index in &selected {
            let dependency = graph.dependencies[*index];
            let (base, base_depth) = match dependency.base_index() {
                Some(base_index) => {
                    let cached = cache
                        .get(&base_index)
                        .ok_or(ParallelValidationError::BaseCache)?;
                    let depth = depths[base_index].ok_or(ParallelValidationError::DeltaDepth)?;
                    (Some(cached.handle.clone()), depth)
                }
                None => (None, 0_u32),
            };
            let expected_delta_depth = if dependency.is_delta() {
                base_depth
                    .checked_add(1)
                    .ok_or(ParallelValidationError::DeltaDepth)?
            } else {
                0
            };
            if expected_delta_depth > limits.receive.max_delta_depth {
                return Err(ParallelValidationError::DeltaDepth);
            }
            tasks.push(ParallelValidationTask {
                index: *index,
                row: rows[*index].clone(),
                dependency,
                base,
                expected_delta_depth,
                byte_limit: totals[*index],
            });
        }

        peak_tasks = peak_tasks.max(tasks.len());
        peak_wave_bytes = peak_wave_bytes.max(wave_bytes);
        waves = waves
            .checked_add(1)
            .ok_or(ParallelValidationError::WorkBudget)?;
        checkpoint(
            control,
            work,
            limits.started_at,
            limits.deadline,
            &mut last_observed_at,
            1,
        )?;
        let execute_result = executor.execute_wave(tasks, limits.deadline).await;
        checkpoint(
            control,
            work,
            limits.started_at,
            limits.deadline,
            &mut last_observed_at,
            wave_bytes.max(1),
        )?;
        let results = execute_result.map_err(map_executor_error)?;
        let mut ordered = verify_results(results, &selected)?;
        if let Some((index, failure)) = ordered.iter().find_map(|result| {
            result
                .result
                .as_ref()
                .err()
                .copied()
                .map(|failure| (result.index, failure))
        }) {
            return Err(ParallelValidationError::Entry { index, failure });
        }

        let mut retained = Vec::new();
        retained
            .try_reserve_exact(ordered.len())
            .map_err(|_| ParallelValidationError::SchedulerMemory)?;
        for result in ordered.drain(..) {
            let output = result
                .result
                .map_err(|failure| ParallelValidationError::Entry {
                    index: result.index,
                    failure,
                })?;
            let expected = if graph.dependencies[result.index].is_delta() {
                let base_depth = match graph.dependencies[result.index].base_index() {
                    Some(base_index) => {
                        depths[base_index].ok_or(ParallelValidationError::DeltaDepth)?
                    }
                    None => 0_u32,
                };
                base_depth
                    .checked_add(1)
                    .ok_or(ParallelValidationError::DeltaDepth)?
            } else {
                0
            };
            if output.delta_depth != expected || output.delta_depth > limits.receive.max_delta_depth
            {
                return Err(ParallelValidationError::DeltaDepth);
            }
            depths[result.index] = Some(output.delta_depth);
            metadata[result.index] = Some(output.metadata);
            retained.push((result.index, output.retained_base));
            if let Some(base_index) = graph.dependencies[result.index].base_index() {
                let should_evict = {
                    let base = cache
                        .get_mut(&base_index)
                        .ok_or(ParallelValidationError::BaseCache)?;
                    base.remaining_dependents = base
                        .remaining_dependents
                        .checked_sub(1)
                        .ok_or(ParallelValidationError::BaseCache)?;
                    base.remaining_dependents == 0
                };
                if should_evict {
                    let removed = cache
                        .remove(&base_index)
                        .ok_or(ParallelValidationError::BaseCache)?;
                    cache_bytes = cache_bytes
                        .checked_sub(removed.bytes)
                        .ok_or(ParallelValidationError::BaseCache)?;
                }
            }
        }

        for (index, retained_base) in retained {
            let dependent_count = graph.children[index].len();
            if dependent_count > 0 {
                let retained_base = retained_base.ok_or(ParallelValidationError::BaseCache)?;
                let estimated_result_bytes = match costs[index] {
                    ParallelValidationCost::Known(cost) => cost.result,
                    ParallelValidationCost::Unknown { hard_upper_bound } => hard_upper_bound,
                };
                let next_cache = cache_bytes
                    .checked_add(retained_base.bytes)
                    .ok_or(ParallelValidationError::BaseCache)?;
                if retained_base.bytes == 0
                    || retained_base.bytes > estimated_result_bytes
                    || next_cache > limits.base_cache_bytes
                {
                    return Err(ParallelValidationError::BaseCache);
                }
                cache_bytes = next_cache;
                cache.insert(
                    index,
                    CachedBase {
                        handle: retained_base.handle,
                        bytes: retained_base.bytes,
                        remaining_dependents: dependent_count,
                    },
                );
                peak_base_cache_bytes = peak_base_cache_bytes.max(cache_bytes);
            } else if retained_base.is_some() {
                return Err(ParallelValidationError::ExecutorContract);
            }

            for child in &graph.children[index] {
                indegrees[*child] = indegrees[*child]
                    .checked_sub(1)
                    .ok_or(ParallelValidationError::DependencyCycle)?;
                if indegrees[*child] == 0 {
                    ready.push((rows[*child].entry.offset, *child));
                }
            }
            completed = completed
                .checked_add(1)
                .ok_or(ParallelValidationError::WorkBudget)?;
        }
        ready.sort_unstable();
    }

    let mut ordered_metadata = Vec::new();
    ordered_metadata
        .try_reserve_exact(metadata.len())
        .map_err(|_| ParallelValidationError::SchedulerMemory)?;
    for item in metadata {
        ordered_metadata.push(item.ok_or(ParallelValidationError::ExecutorContract)?);
    }
    Ok(ParallelValidationReport {
        metadata: ordered_metadata,
        waves,
        peak_tasks,
        peak_wave_bytes,
        peak_base_cache_bytes,
    })
}

fn build_dependency_graph<C, W>(
    rows: &[PushPackIndexRow],
    control: &mut C,
    work: &mut W,
    started_at: OffsetDateTime,
    deadline: OffsetDateTime,
    last_observed_at: &mut OffsetDateTime,
) -> Result<DependencyGraph, ParallelValidationError>
where
    C: PushPackValidationControl,
    W: ParallelValidationWork,
{
    let mut by_offset = HashMap::new();
    let mut by_oid = HashMap::new();
    by_offset
        .try_reserve(rows.len())
        .map_err(|_| ParallelValidationError::SchedulerMemory)?;
    by_oid
        .try_reserve(rows.len())
        .map_err(|_| ParallelValidationError::SchedulerMemory)?;
    for (index, row) in rows.iter().enumerate() {
        checkpoint(control, work, started_at, deadline, last_observed_at, 1)?;
        if by_offset.insert(row.entry.offset, index).is_some()
            || by_oid.insert(row.oid, index).is_some()
        {
            return Err(ParallelValidationError::MissingDependency);
        }
    }
    let mut dependencies = Vec::new();
    dependencies
        .try_reserve_exact(rows.len())
        .map_err(|_| ParallelValidationError::SchedulerMemory)?;
    let mut children = Vec::new();
    children
        .try_reserve_exact(rows.len())
        .map_err(|_| ParallelValidationError::SchedulerMemory)?;
    children.resize_with(rows.len(), Vec::new);
    let mut indegrees = Vec::new();
    indegrees
        .try_reserve_exact(rows.len())
        .map_err(|_| ParallelValidationError::SchedulerMemory)?;
    indegrees.resize(rows.len(), 0_usize);
    for (index, row) in rows.iter().enumerate() {
        checkpoint(control, work, started_at, deadline, last_observed_at, 1)?;
        let dependency = match row.representation {
            PushPackEntryKind::Direct => ParallelValidationDependency::Direct,
            PushPackEntryKind::OfsDelta { base_offset, .. } => {
                if base_offset >= row.entry.offset {
                    return Err(ParallelValidationError::MissingDependency);
                }
                let base_index = by_offset
                    .get(&base_offset)
                    .copied()
                    .ok_or(ParallelValidationError::MissingDependency)?;
                if base_index >= index {
                    return Err(ParallelValidationError::MissingDependency);
                }
                ParallelValidationDependency::OfsDelta { base_index }
            }
            PushPackEntryKind::RefDelta {
                base_oid,
                external: false,
            } => {
                let base_index = by_oid
                    .get(&base_oid)
                    .copied()
                    .ok_or(ParallelValidationError::MissingDependency)?;
                ParallelValidationDependency::InternalRefDelta { base_index }
            }
            PushPackEntryKind::RefDelta {
                base_oid,
                external: true,
            } => {
                if by_oid.contains_key(&base_oid) {
                    return Err(ParallelValidationError::MissingDependency);
                }
                ParallelValidationDependency::ExternalRefDelta { base_oid }
            }
        };
        if let Some(base_index) = dependency.base_index() {
            if base_index == index {
                return Err(ParallelValidationError::DependencyCycle);
            }
            children[base_index]
                .try_reserve(1)
                .map_err(|_| ParallelValidationError::SchedulerMemory)?;
            children[base_index].push(index);
            indegrees[index] = 1;
        }
        dependencies.push(dependency);
    }

    let mut remaining = Vec::new();
    remaining
        .try_reserve_exact(indegrees.len())
        .map_err(|_| ParallelValidationError::SchedulerMemory)?;
    remaining.extend_from_slice(&indegrees);
    let mut ready = Vec::new();
    ready
        .try_reserve_exact(rows.len())
        .map_err(|_| ParallelValidationError::SchedulerMemory)?;
    for (index, indegree) in remaining.iter().copied().enumerate() {
        if indegree == 0 {
            ready.push(index);
        }
    }
    let mut visited = 0_usize;
    while let Some(index) = ready.pop() {
        checkpoint(control, work, started_at, deadline, last_observed_at, 1)?;
        visited = visited
            .checked_add(1)
            .ok_or(ParallelValidationError::SchedulerMemory)?;
        for child in &children[index] {
            remaining[*child] = remaining[*child]
                .checked_sub(1)
                .ok_or(ParallelValidationError::DependencyCycle)?;
            if remaining[*child] == 0 {
                ready.push(*child);
            }
        }
    }
    if visited != rows.len() {
        return Err(ParallelValidationError::DependencyCycle);
    }
    Ok(DependencyGraph {
        dependencies,
        children,
        indegrees,
    })
}

fn validate_costs(
    costs: &[ParallelValidationCost],
    hard_task_bytes: u64,
) -> Result<Vec<u64>, ParallelValidationError> {
    let mut totals = Vec::new();
    totals
        .try_reserve_exact(costs.len())
        .map_err(|_| ParallelValidationError::SchedulerMemory)?;
    for cost in costs {
        let total = match cost {
            ParallelValidationCost::Known(cost) => cost.total()?,
            ParallelValidationCost::Unknown { hard_upper_bound } => *hard_upper_bound,
        };
        if total == 0 {
            return Err(ParallelValidationError::InvalidCost);
        }
        if total > hard_task_bytes {
            return Err(ParallelValidationError::TaskTooLarge);
        }
        totals.push(total);
    }
    Ok(totals)
}

fn validate_scheduler_memory<B, M>(
    rows: usize,
    costs: &[ParallelValidationCost],
    workers: usize,
    maximum: u64,
) -> Result<(), ParallelValidationError> {
    let node_bytes = 512_u64
        .checked_add(std::mem::size_of::<ParallelValidationDependency>() as u64)
        .and_then(|bytes| bytes.checked_add(std::mem::size_of::<Option<M>>() as u64))
        .and_then(|bytes| bytes.checked_add(std::mem::size_of::<CachedBase<B>>() as u64))
        .ok_or(ParallelValidationError::SchedulerMemory)?;
    let worker_bytes = 256_u64
        .checked_add(std::mem::size_of::<ParallelValidationTask<B>>() as u64)
        .and_then(|bytes| {
            bytes.checked_add(std::mem::size_of::<ParallelValidationTaskResult<B, M>>() as u64)
        })
        .ok_or(ParallelValidationError::SchedulerMemory)?;
    let fixed = u64::try_from(rows)
        .ok()
        .and_then(|rows| rows.checked_mul(node_bytes))
        .and_then(|bytes| {
            u64::try_from(workers)
                .ok()
                .and_then(|workers| workers.checked_mul(worker_bytes))
                .and_then(|workers| bytes.checked_add(workers))
        })
        .ok_or(ParallelValidationError::SchedulerMemory)?;
    let retained = costs.iter().try_fold(0_u64, |sum, cost| {
        let bytes = match cost {
            ParallelValidationCost::Known(cost) => cost.structural_metadata,
            ParallelValidationCost::Unknown { hard_upper_bound } => *hard_upper_bound,
        };
        sum.checked_add(bytes)
            .ok_or(ParallelValidationError::SchedulerMemory)
    })?;
    if fixed
        .checked_add(retained)
        .filter(|bytes| *bytes <= maximum)
        .is_none()
    {
        return Err(ParallelValidationError::SchedulerMemory);
    }
    Ok(())
}

fn select_wave(
    ready: &[(u64, usize)],
    costs: &[ParallelValidationCost],
    totals: &[u64],
    dependencies: &[ParallelValidationDependency],
    worker_count: usize,
    range_concurrency: usize,
    wave_bytes: u64,
    available_bytes: u64,
) -> Result<Vec<usize>, ParallelValidationError> {
    let mut selected = Vec::new();
    selected
        .try_reserve_exact(worker_count.min(ready.len()))
        .map_err(|_| ParallelValidationError::SchedulerMemory)?;
    let mut admitted = 0_u64;
    let mut ranges = 0_usize;
    for (_, index) in ready {
        if selected.len() >= worker_count {
            break;
        }
        let exclusive = matches!(costs[*index], ParallelValidationCost::Unknown { .. })
            || totals[*index] > wave_bytes;
        if exclusive {
            if selected.is_empty() && totals[*index] <= available_bytes {
                selected.push(*index);
                break;
            }
            if selected.is_empty() {
                continue;
            }
            break;
        }
        let uses_external_range = matches!(
            dependencies[*index],
            ParallelValidationDependency::ExternalRefDelta { .. }
        );
        if uses_external_range && ranges >= range_concurrency {
            continue;
        }
        let next = admitted
            .checked_add(totals[*index])
            .ok_or(ParallelValidationError::InvalidCost)?;
        if next > wave_bytes || next > available_bytes {
            continue;
        }
        selected.push(*index);
        admitted = next;
        if uses_external_range {
            ranges += 1;
        }
    }
    Ok(selected)
}

fn verify_results<B, M>(
    mut results: Vec<ParallelValidationTaskResult<B, M>>,
    selected: &[usize],
) -> Result<Vec<ParallelValidationTaskResult<B, M>>, ParallelValidationError> {
    if results.len() != selected.len() {
        return Err(ParallelValidationError::ExecutorContract);
    }
    let mut expected = Vec::new();
    expected
        .try_reserve_exact(selected.len())
        .map_err(|_| ParallelValidationError::SchedulerMemory)?;
    expected.extend_from_slice(selected);
    expected.sort_unstable();
    results.sort_by_key(|result| result.index);
    if results
        .iter()
        .map(|result| result.index)
        .ne(expected.into_iter())
    {
        return Err(ParallelValidationError::ExecutorContract);
    }
    Ok(results)
}

fn checkpoint<C: PushPackValidationControl, W: ParallelValidationWork>(
    control: &mut C,
    work: &mut W,
    started_at: OffsetDateTime,
    deadline: OffsetDateTime,
    last_observed_at: &mut OffsetDateTime,
    units: u64,
) -> Result<(), ParallelValidationError> {
    let observation = control.observe();
    if observation.observed_at < *last_observed_at {
        return Err(ParallelValidationError::Deadline);
    }
    *last_observed_at = observation.observed_at;
    if observation.cancelled {
        return Err(ParallelValidationError::Cancelled);
    }
    if observation.observed_at < started_at || observation.observed_at >= deadline {
        return Err(ParallelValidationError::Deadline);
    }
    if !work.charge(units) {
        return Err(ParallelValidationError::WorkBudget);
    }
    Ok(())
}

fn map_executor_error(error: ParallelValidationExecutorError) -> ParallelValidationError {
    match error {
        ParallelValidationExecutorError::Cancelled => ParallelValidationError::Cancelled,
        ParallelValidationExecutorError::Deadline => ParallelValidationError::Deadline,
        ParallelValidationExecutorError::Backend => ParallelValidationError::Executor,
    }
}
