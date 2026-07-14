//! Clone/fetch measurements and resource-limit contracts.
//!
//! This module contains no clock or runtime integration. Callers measure durations and backend
//! work at their actual adapter boundaries, then record those observations explicitly. The
//! upload-pack compatibility path enforces request shape and closure count before growth, but
//! worker, duration, in-flight byte, spill, coalescing, and concurrency limits are contracts for
//! the adapters that own those resources.

use std::time::Duration;

use grit_lib::objects::ObjectKind;

/// Stable clone/fetch processing phase.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ClonePhase {
    /// Wants, haves, and capabilities are negotiated.
    Negotiation,
    /// Wants-minus-haves object closure is discovered.
    Reachability,
    /// Object representations and pack order are planned.
    PackPlan,
    /// Time from accepted request to the first emitted response byte.
    FirstByte,
    /// Object compression and delta work.
    Compression,
    /// Complete request lifetime.
    Total,
}

/// Backend serving a clone request.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum CloneBackendKind {
    /// Backend identity was not exposed by the caller.
    #[default]
    Unspecified,
    /// In-memory storage.
    Memory,
    /// PostgreSQL with database-resident payload bytes.
    Postgres,
    /// PostgreSQL metadata with external immutable bytes.
    ExternalizedPostgres,
    /// A storage backend behind the shared cache layer.
    Cached,
}

/// Terminal clone/fetch outcome.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CloneOutcome {
    /// Response completed successfully.
    Success,
    /// Safety or admission limits rejected the request.
    Rejected,
    /// Caller cancellation or client disconnect stopped work.
    Cancelled,
    /// Storage backend work failed.
    BackendFailure,
    /// Negotiation, pack, or wire-protocol work failed.
    ProtocolFailure,
}

/// Cache behavior observed for a clone request.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CloneCacheMetrics {
    /// Successful cache lookups.
    pub hits: u64,
    /// Cache lookups that required origin work.
    pub misses: u64,
    /// Caller-measured time waiting for a shared singleflight producer.
    pub singleflight_wait: Duration,
}

/// Selected objects and payload volumes for one Git object kind.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CloneObjectKindMetrics {
    /// Selected object count.
    pub selected: u64,
    /// Decoded canonical payload bytes.
    pub decoded_bytes: u64,
    /// Compressed entry bytes written or copied.
    pub compressed_bytes: u64,
}

/// Per-kind clone payload metrics.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CloneObjectMetrics {
    /// Blob metrics.
    pub blobs: CloneObjectKindMetrics,
    /// Tree metrics.
    pub trees: CloneObjectKindMetrics,
    /// Commit metrics.
    pub commits: CloneObjectKindMetrics,
    /// Annotated-tag metrics.
    pub tags: CloneObjectKindMetrics,
}

impl CloneObjectMetrics {
    fn kind_mut(&mut self, kind: ObjectKind) -> &mut CloneObjectKindMetrics {
        match kind {
            ObjectKind::Blob => &mut self.blobs,
            ObjectKind::Tree => &mut self.trees,
            ObjectKind::Commit => &mut self.commits,
            ObjectKind::Tag => &mut self.tags,
        }
    }

    /// Return the saturating selected-object total across all kinds.
    #[must_use]
    pub fn selected_total(self) -> u64 {
        self.blobs
            .selected
            .saturating_add(self.trees.selected)
            .saturating_add(self.commits.selected)
            .saturating_add(self.tags.selected)
    }
}

/// Backend requests and returned byte volumes.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CloneBackendMetrics {
    /// Database statements attributable to this request.
    pub database_statements: u64,
    /// Database payload bytes returned.
    pub database_bytes: u64,
    /// Whole-object external GET requests.
    pub external_gets: u64,
    /// External range GET requests.
    pub external_range_gets: u64,
    /// External payload bytes returned.
    pub external_bytes: u64,
}

/// Pack encoding output metrics.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ClonePackMetrics {
    /// Raw PACK stream bytes, including header and trailer.
    pub output_bytes: u64,
    /// Delta entry count.
    pub delta_entries: u64,
    /// Largest emitted delta-chain depth.
    pub maximum_delta_depth: u32,
}

/// Peak in-flight memory observations.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CloneMemoryMetrics {
    /// Peak decoded object bytes.
    pub decoded_bytes: u64,
    /// Peak retained delta-base bytes.
    pub delta_base_bytes: u64,
    /// Peak encoded bytes waiting for output.
    pub encoded_bytes: u64,
    /// Peak separately retained wire-response bytes, including any sideband framing.
    pub response_bytes: u64,
    /// Peak total request-owned bytes.
    pub total_bytes: u64,
}

/// Cancellation responsiveness and discarded work.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CloneCancellationMetrics {
    /// Time between cancellation observation and worker quiescence.
    pub latency: Option<Duration>,
    /// Backend bytes completed after cancellation became observable.
    pub wasted_backend_bytes: u64,
    /// CPU work units completed after cancellation became observable.
    pub wasted_cpu_units: u64,
}

/// Explicit phase-duration observations.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ClonePhaseMetrics {
    /// Negotiation duration.
    pub negotiation: Option<Duration>,
    /// Reachability duration.
    pub reachability: Option<Duration>,
    /// Pack planning duration.
    pub pack_plan: Option<Duration>,
    /// Time to first byte.
    pub first_byte: Option<Duration>,
    /// Compression duration.
    pub compression: Option<Duration>,
    /// Total duration.
    pub total: Option<Duration>,
}

/// Point-in-time clone/fetch metrics report.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CloneMetricsReport {
    /// Serving backend identified by the caller.
    pub backend: CloneBackendKind,
    /// Terminal outcome, or `None` while the request remains in progress.
    pub outcome: Option<CloneOutcome>,
    /// Phase timings measured by the caller.
    pub phases: ClonePhaseMetrics,
    /// Total objects selected by the current upload-pack planner.
    pub selected_objects: u64,
    /// Per-object-kind payload work.
    pub objects: CloneObjectMetrics,
    /// Cache observations.
    pub cache: CloneCacheMetrics,
    /// Database and external-service observations.
    pub backend_io: CloneBackendMetrics,
    /// Pack output and delta shape.
    pub pack: ClonePackMetrics,
    /// Peak request-owned memory.
    pub memory: CloneMemoryMetrics,
    /// Cancellation and wasted-work observations.
    pub cancellation: CloneCancellationMetrics,
}

/// Runtime-neutral saturating recorder for one clone/fetch request.
#[derive(Clone, Debug, Default)]
pub struct CloneMetricsRecorder {
    report: CloneMetricsReport,
    pack_recorded: bool,
}

impl CloneMetricsRecorder {
    /// Create a recorder with an explicit backend classification.
    #[must_use]
    pub fn new(backend: CloneBackendKind) -> Self {
        Self {
            report: CloneMetricsReport {
                backend,
                ..CloneMetricsReport::default()
            },
            ..Self::default()
        }
    }

    /// Record a caller-measured phase duration once.
    ///
    /// Returns `false` and preserves the first observation when the phase was already recorded, or
    /// when the observation would make a component/first-byte duration exceed total duration.
    #[must_use]
    pub fn record_phase(&mut self, phase: ClonePhase, duration: Duration) -> bool {
        if phase == ClonePhase::Total {
            let phases = self.report.phases;
            if [
                phases.negotiation,
                phases.reachability,
                phases.pack_plan,
                phases.first_byte,
                phases.compression,
            ]
            .into_iter()
            .flatten()
            .any(|component| component > duration)
            {
                return false;
            }
        } else if self
            .report
            .phases
            .total
            .is_some_and(|total| duration > total)
        {
            return false;
        }
        let slot = match phase {
            ClonePhase::Negotiation => &mut self.report.phases.negotiation,
            ClonePhase::Reachability => &mut self.report.phases.reachability,
            ClonePhase::PackPlan => &mut self.report.phases.pack_plan,
            ClonePhase::FirstByte => &mut self.report.phases.first_byte,
            ClonePhase::Compression => &mut self.report.phases.compression,
            ClonePhase::Total => &mut self.report.phases.total,
        };
        if slot.is_some() {
            return false;
        }
        *slot = Some(duration);
        true
    }

    /// Record the planner's total selected object count.
    pub fn record_selected_objects(&mut self, selected: u64) {
        self.report.selected_objects = selected;
    }

    /// Add one or more objects of a known kind and their measured payload bytes.
    pub fn record_objects(
        &mut self,
        kind: ObjectKind,
        selected: u64,
        decoded_bytes: u64,
        compressed_bytes: u64,
    ) {
        let metrics = self.report.objects.kind_mut(kind);
        metrics.selected = metrics.selected.saturating_add(selected);
        metrics.decoded_bytes = metrics.decoded_bytes.saturating_add(decoded_bytes);
        metrics.compressed_bytes = metrics.compressed_bytes.saturating_add(compressed_bytes);
    }

    /// Add cache observations and explicit singleflight wait time.
    pub fn record_cache(&mut self, hits: u64, misses: u64, singleflight_wait: Duration) {
        self.report.cache.hits = self.report.cache.hits.saturating_add(hits);
        self.report.cache.misses = self.report.cache.misses.saturating_add(misses);
        self.report.cache.singleflight_wait = self
            .report
            .cache
            .singleflight_wait
            .saturating_add(singleflight_wait);
    }

    /// Add database work observed by a storage adapter.
    pub fn record_database(&mut self, statements: u64, bytes: u64) {
        self.report.backend_io.database_statements = self
            .report
            .backend_io
            .database_statements
            .saturating_add(statements);
        self.report.backend_io.database_bytes =
            self.report.backend_io.database_bytes.saturating_add(bytes);
    }

    /// Add external whole/range GET work observed by a storage adapter.
    pub fn record_external(&mut self, gets: u64, range_gets: u64, bytes: u64) {
        self.report.backend_io.external_gets =
            self.report.backend_io.external_gets.saturating_add(gets);
        self.report.backend_io.external_range_gets = self
            .report
            .backend_io
            .external_range_gets
            .saturating_add(range_gets);
        self.report.backend_io.external_bytes =
            self.report.backend_io.external_bytes.saturating_add(bytes);
    }

    /// Record final pack size and delta shape once.
    ///
    /// Returns `false` and preserves the first final observation on duplicate calls.
    #[must_use]
    pub fn record_pack(
        &mut self,
        output_bytes: u64,
        delta_entries: u64,
        maximum_depth: u32,
    ) -> bool {
        if self.pack_recorded {
            return false;
        }
        self.report.pack.output_bytes = output_bytes;
        self.report.pack.delta_entries = delta_entries;
        self.report.pack.maximum_delta_depth = maximum_depth;
        self.pack_recorded = true;
        true
    }

    /// Raise memory high-water marks without decreasing prior observations.
    pub fn observe_memory(&mut self, observation: CloneMemoryMetrics) {
        self.report.memory.decoded_bytes = self
            .report
            .memory
            .decoded_bytes
            .max(observation.decoded_bytes);
        self.report.memory.delta_base_bytes = self
            .report
            .memory
            .delta_base_bytes
            .max(observation.delta_base_bytes);
        self.report.memory.encoded_bytes = self
            .report
            .memory
            .encoded_bytes
            .max(observation.encoded_bytes);
        self.report.memory.response_bytes = self
            .report
            .memory
            .response_bytes
            .max(observation.response_bytes);
        let category_total = self
            .report
            .memory
            .decoded_bytes
            .saturating_add(self.report.memory.delta_base_bytes)
            .saturating_add(self.report.memory.encoded_bytes)
            .saturating_add(self.report.memory.response_bytes);
        self.report.memory.total_bytes = self
            .report
            .memory
            .total_bytes
            .max(observation.total_bytes)
            .max(category_total);
    }

    /// Record cancellation latency and work completed after cancellation.
    pub fn record_cancellation(&mut self, metrics: CloneCancellationMetrics) {
        self.report.cancellation = metrics;
    }

    /// Set the first terminal outcome and return a stable snapshot.
    ///
    /// A later finish call cannot rewrite an already reported terminal result.
    #[must_use]
    pub fn finish(&mut self, outcome: CloneOutcome) -> CloneMetricsReport {
        if self.report.outcome.is_none() {
            self.report.outcome = Some(outcome);
        }
        self.report.clone()
    }

    /// Return a non-terminal point-in-time report.
    #[must_use]
    pub fn snapshot(&self) -> CloneMetricsReport {
        self.report.clone()
    }
}

/// Clone/fetch allocation and execution limits.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CloneLimits {
    /// Maximum request wants.
    pub max_wants: usize,
    /// Maximum request haves.
    pub max_haves: usize,
    /// Maximum selected objects.
    pub max_selected_objects: usize,
    /// Maximum raw pack output bytes; the buffered compatibility path checks this after encoding.
    pub max_output_bytes: u64,
    /// Maximum caller-enforced request duration.
    pub max_duration: Duration,
    /// Maximum read/decode workers for a caller-owned executor.
    pub read_workers: usize,
    /// Maximum compression workers for a caller-owned executor.
    pub compression_workers: usize,
    /// Maximum decoded bytes in flight for a streaming adapter to enforce.
    pub decoded_bytes_in_flight: u64,
    /// Maximum encoded bytes queued for output for a streaming adapter to enforce.
    pub encoded_bytes_in_flight: u64,
    /// Maximum retained delta-base cache bytes for a pack encoder to enforce.
    pub delta_cache_bytes: u64,
    /// Maximum concurrent external range reads for an external-storage adapter to enforce.
    pub range_read_concurrency: usize,
    /// Maximum adjacent range coalescing gap for an external-storage adapter to enforce.
    pub range_coalescing_bytes: u64,
    /// Negotiation-state size at which a spill-capable adapter should spill.
    pub negotiation_spill_threshold: u64,
}

impl CloneLimits {
    /// Allocation-safe library ceilings accepted by [`Self::validate`].
    pub const HARD_MAX: Self = Self {
        max_wants: 65_536,
        max_haves: 4_000_000,
        max_selected_objects: 20_000_000,
        max_output_bytes: 1_u64 << 44,
        max_duration: Duration::from_secs(24 * 60 * 60),
        read_workers: 256,
        compression_workers: 256,
        decoded_bytes_in_flight: 16_u64 << 30,
        encoded_bytes_in_flight: 4_u64 << 30,
        delta_cache_bytes: 32_u64 << 30,
        range_read_concurrency: 1_024,
        range_coalescing_bytes: 1_u64 << 30,
        negotiation_spill_threshold: 16_u64 << 30,
    };

    /// Validate nonzero limits against hard ceilings before allocating workers or buffers.
    ///
    /// # Errors
    ///
    /// Returns [`CloneLimitError::Zero`] or [`CloneLimitError::ExceedsHardMaximum`].
    pub fn validate(self) -> Result<Self, CloneLimitError> {
        let hard = Self::HARD_MAX;
        if self.max_wants == 0
            || self.max_haves == 0
            || self.max_selected_objects == 0
            || self.max_output_bytes == 0
            || self.max_duration.is_zero()
            || self.read_workers == 0
            || self.compression_workers == 0
            || self.decoded_bytes_in_flight == 0
            || self.encoded_bytes_in_flight == 0
            || self.delta_cache_bytes == 0
            || self.range_read_concurrency == 0
            || self.negotiation_spill_threshold == 0
        {
            return Err(CloneLimitError::Zero);
        }
        if self.max_wants > hard.max_wants
            || self.max_haves > hard.max_haves
            || self.max_selected_objects > hard.max_selected_objects
            || self.max_output_bytes > hard.max_output_bytes
            || self.max_duration > hard.max_duration
            || self.read_workers > hard.read_workers
            || self.compression_workers > hard.compression_workers
            || self.decoded_bytes_in_flight > hard.decoded_bytes_in_flight
            || self.encoded_bytes_in_flight > hard.encoded_bytes_in_flight
            || self.delta_cache_bytes > hard.delta_cache_bytes
            || self.range_read_concurrency > hard.range_read_concurrency
            || self.range_coalescing_bytes > hard.range_coalescing_bytes
            || self.negotiation_spill_threshold > hard.negotiation_spill_threshold
        {
            return Err(CloneLimitError::ExceedsHardMaximum);
        }
        Ok(self)
    }

    /// Validate zero/pathological values and clamp oversized limits to allocation-safe ceilings.
    ///
    /// # Errors
    ///
    /// Returns [`CloneLimitError::Zero`] instead of silently turning disabled capacities on.
    pub fn clamped(self) -> Result<Self, CloneLimitError> {
        if self.max_wants == 0
            || self.max_haves == 0
            || self.max_selected_objects == 0
            || self.max_output_bytes == 0
            || self.max_duration.is_zero()
            || self.read_workers == 0
            || self.compression_workers == 0
            || self.decoded_bytes_in_flight == 0
            || self.encoded_bytes_in_flight == 0
            || self.delta_cache_bytes == 0
            || self.range_read_concurrency == 0
            || self.negotiation_spill_threshold == 0
        {
            return Err(CloneLimitError::Zero);
        }
        let hard = Self::HARD_MAX;
        Ok(Self {
            max_wants: self.max_wants.clamp(1, hard.max_wants),
            max_haves: self.max_haves.clamp(1, hard.max_haves),
            max_selected_objects: self
                .max_selected_objects
                .clamp(1, hard.max_selected_objects),
            max_output_bytes: self.max_output_bytes.clamp(1, hard.max_output_bytes),
            max_duration: self
                .max_duration
                .clamp(Duration::from_nanos(1), hard.max_duration),
            read_workers: self.read_workers.clamp(1, hard.read_workers),
            compression_workers: self.compression_workers.clamp(1, hard.compression_workers),
            decoded_bytes_in_flight: self
                .decoded_bytes_in_flight
                .clamp(1, hard.decoded_bytes_in_flight),
            encoded_bytes_in_flight: self
                .encoded_bytes_in_flight
                .clamp(1, hard.encoded_bytes_in_flight),
            delta_cache_bytes: self.delta_cache_bytes.clamp(1, hard.delta_cache_bytes),
            range_read_concurrency: self
                .range_read_concurrency
                .clamp(1, hard.range_read_concurrency),
            range_coalescing_bytes: self.range_coalescing_bytes.min(hard.range_coalescing_bytes),
            negotiation_spill_threshold: self
                .negotiation_spill_threshold
                .clamp(1, hard.negotiation_spill_threshold),
        })
    }

    /// Reject oversized request vectors before negotiation allocates closure state.
    ///
    /// # Errors
    ///
    /// Returns [`CloneLimitError::RequestShape`] when wants or haves exceed the validated limit.
    pub fn validate_request_shape(self, wants: usize, haves: usize) -> Result<(), CloneLimitError> {
        self.validate()?;
        if wants > self.max_wants || haves > self.max_haves {
            return Err(CloneLimitError::RequestShape);
        }
        Ok(())
    }
}

impl Default for CloneLimits {
    fn default() -> Self {
        Self {
            max_wants: 8_192,
            max_haves: 500_000,
            max_selected_objects: 5_000_000,
            max_output_bytes: 256_u64 << 30,
            max_duration: Duration::from_secs(2 * 60 * 60),
            read_workers: 16,
            compression_workers: 8,
            decoded_bytes_in_flight: 512_u64 << 20,
            encoded_bytes_in_flight: 64_u64 << 20,
            delta_cache_bytes: 256_u64 << 20,
            range_read_concurrency: 32,
            range_coalescing_bytes: 256 << 10,
            negotiation_spill_threshold: 64_u64 << 20,
        }
    }
}

/// Clone limit validation failure.
#[derive(Clone, Copy, Debug, thiserror::Error, PartialEq, Eq)]
pub enum CloneLimitError {
    /// A capacity needed before allocation was zero.
    #[error("clone limit must be nonzero")]
    Zero,
    /// A caller limit exceeded the absolute library ceiling.
    #[error("clone limit exceeds hard maximum")]
    ExceedsHardMaximum,
    /// Wants or haves exceeded the validated request shape.
    #[error("clone request exceeds want/have limits")]
    RequestShape,
    /// Selected object count exceeded the validated limit.
    #[error("clone selection exceeds object limit")]
    SelectedObjects,
    /// Serialized pack output exceeded the validated limit.
    #[error("clone output exceeds byte limit")]
    OutputBytes,
}
