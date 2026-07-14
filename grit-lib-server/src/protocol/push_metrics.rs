//! Runtime-neutral receive-pack measurements and safety-limit contracts.
//!
//! This module never reads a clock or request identity. Callers supply durations and aggregate
//! backend observations at the boundaries that own those measurements.

use std::time::Duration;

/// Receive-pack safety-limit dimension.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReceivePackLimitKind {
    /// Complete buffered request bytes.
    RequestBytes,
    /// Ref update commands.
    Commands,
    /// Non-delete object wants introduced by commands.
    WantedObjects,
    /// Advertised/requested capability tokens.
    Capabilities,
    /// Push-option count.
    PushOptions,
    /// Aggregate push-option bytes.
    PushOptionBytes,
    /// Compressed PACK bytes.
    PackBytes,
    /// PACK object entries.
    Objects,
    /// One inflated direct object.
    ObjectBytes,
    /// Aggregate inflated direct and delta-instruction bytes.
    InflatedBytes,
    /// One inflated delta-instruction stream.
    DeltaInstructionBytes,
    /// Resolved-to-instruction delta expansion ratio.
    DeltaExpansionRatio,
    /// Resolved delta chain depth.
    DeltaDepth,
    /// Decode worker count.
    Workers,
    /// Request-owned bytes concurrently in flight.
    BytesInFlight,
    /// Structural parsing memory.
    StructuralMemory,
    /// Prepared push memory.
    PreparedMemory,
    /// Retained delta-base memory.
    DeltaBaseMemory,
    /// Prepared metadata memory.
    MetadataMemory,
    /// Complete request duration.
    Duration,
    /// SQL statements attributable to one request.
    SqlStatements,
    /// Object backend operations attributable to one request.
    ObjectOperations,
    /// Backend range operations attributable to one request.
    RangeOperations,
    /// External-store operations attributable to one request.
    ExternalOperations,
}

/// Typed receive-pack limit validation or enforcement failure.
#[derive(Clone, Copy, Debug, thiserror::Error, PartialEq, Eq)]
pub enum ReceivePackLimitError {
    /// A capacity required for safe execution was zero.
    #[error("receive-pack limit {0:?} must be nonzero")]
    Zero(ReceivePackLimitKind),
    /// A configured value exceeded the allocation-safe library ceiling.
    #[error("receive-pack limit {0:?} exceeds the hard maximum")]
    ExceedsHardMaximum(ReceivePackLimitKind),
    /// Request work exceeded its configured limit.
    #[error("receive-pack request exceeded {0:?}")]
    Exceeded(ReceivePackLimitKind),
    /// Limit relationships were internally inconsistent.
    #[error("receive-pack limits are inconsistent")]
    Inconsistent,
}

/// Explicit receive-pack allocation and execution limits.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReceivePackLimits {
    /// Maximum complete buffered request bytes.
    pub max_request_bytes: u64,
    /// Maximum ref update commands.
    pub max_commands: usize,
    /// Maximum non-delete command roots.
    pub max_wanted_objects: usize,
    /// Maximum capability tokens.
    pub max_capabilities: usize,
    /// Maximum push-option count for a push-options-aware decoder.
    pub max_push_options: usize,
    /// Maximum aggregate push-option bytes.
    pub max_push_option_bytes: u64,
    /// Maximum compressed PACK bytes.
    pub max_pack_bytes: u64,
    /// Maximum PACK object entries.
    pub max_objects: usize,
    /// Maximum resolved bytes for one direct or delta object.
    pub max_object_bytes: u64,
    /// Maximum aggregate inflated object and delta-instruction bytes.
    pub max_inflated_bytes: u64,
    /// Maximum inflated bytes for one delta-instruction stream.
    pub max_delta_instruction_bytes: u64,
    /// Maximum resolved bytes divided by delta-instruction bytes.
    pub max_delta_expansion_ratio: u64,
    /// Maximum resolved delta chain depth.
    pub max_delta_depth: u32,
    /// Maximum caller-owned decode workers.
    pub worker_count: usize,
    /// Maximum request-owned bytes concurrently in flight.
    pub bytes_in_flight: u64,
    /// Maximum structural parsing memory.
    pub max_structural_memory_bytes: u64,
    /// Maximum prepared push memory.
    pub max_prepared_memory_bytes: u64,
    /// Maximum retained delta-base memory.
    pub max_delta_base_memory_bytes: u64,
    /// Maximum prepared metadata memory.
    pub max_metadata_memory_bytes: u64,
    /// Maximum complete request duration.
    pub max_duration: Duration,
    /// Maximum SQL statements attributable to one request.
    pub max_sql_statements: u64,
    /// Maximum object backend operations attributable to one request.
    pub max_object_operations: u64,
    /// Maximum backend range operations attributable to one request.
    pub max_range_operations: u64,
    /// Maximum external-store operations attributable to one request.
    pub max_external_operations: u64,
}

impl ReceivePackLimits {
    /// Allocation-safe ceilings accepted by [`Self::validate`].
    pub const HARD_MAX: Self = Self {
        max_request_bytes: 1_u64 << 44,
        max_commands: 65_536,
        max_wanted_objects: 65_536,
        max_capabilities: 4_096,
        max_push_options: 65_536,
        max_push_option_bytes: 1_u64 << 30,
        max_pack_bytes: 1_u64 << 44,
        max_objects: 20_000_000,
        max_object_bytes: 1_u64 << 40,
        max_inflated_bytes: 1_u64 << 44,
        max_delta_instruction_bytes: 4_u64 << 30,
        max_delta_expansion_ratio: 1_000_000,
        max_delta_depth: 4_096,
        worker_count: 256,
        bytes_in_flight: 16_u64 << 30,
        max_structural_memory_bytes: 16_u64 << 30,
        max_prepared_memory_bytes: 32_u64 << 30,
        max_delta_base_memory_bytes: 16_u64 << 30,
        max_metadata_memory_bytes: 16_u64 << 30,
        max_duration: Duration::from_secs(24 * 60 * 60),
        max_sql_statements: 100_000_000,
        max_object_operations: 100_000_000,
        max_range_operations: 100_000_000,
        max_external_operations: 100_000_000,
    };

    /// Validate nonzero limits, relationships, and hard ceilings.
    ///
    /// # Errors
    ///
    /// Returns a typed zero, hard-maximum, or relationship failure.
    pub fn validate(self) -> Result<Self, ReceivePackLimitError> {
        let values = [
            (self.max_request_bytes, ReceivePackLimitKind::RequestBytes),
            (
                self.max_push_option_bytes,
                ReceivePackLimitKind::PushOptionBytes,
            ),
            (self.max_pack_bytes, ReceivePackLimitKind::PackBytes),
            (self.max_object_bytes, ReceivePackLimitKind::ObjectBytes),
            (self.max_inflated_bytes, ReceivePackLimitKind::InflatedBytes),
            (
                self.max_delta_instruction_bytes,
                ReceivePackLimitKind::DeltaInstructionBytes,
            ),
            (
                self.max_delta_expansion_ratio,
                ReceivePackLimitKind::DeltaExpansionRatio,
            ),
            (self.bytes_in_flight, ReceivePackLimitKind::BytesInFlight),
            (
                self.max_structural_memory_bytes,
                ReceivePackLimitKind::StructuralMemory,
            ),
            (
                self.max_prepared_memory_bytes,
                ReceivePackLimitKind::PreparedMemory,
            ),
            (
                self.max_delta_base_memory_bytes,
                ReceivePackLimitKind::DeltaBaseMemory,
            ),
            (
                self.max_metadata_memory_bytes,
                ReceivePackLimitKind::MetadataMemory,
            ),
            (self.max_sql_statements, ReceivePackLimitKind::SqlStatements),
            (
                self.max_object_operations,
                ReceivePackLimitKind::ObjectOperations,
            ),
            (
                self.max_range_operations,
                ReceivePackLimitKind::RangeOperations,
            ),
            (
                self.max_external_operations,
                ReceivePackLimitKind::ExternalOperations,
            ),
        ];
        if let Some((_, kind)) = values.into_iter().find(|(value, _)| *value == 0) {
            return Err(ReceivePackLimitError::Zero(kind));
        }
        let counts = [
            (self.max_commands, ReceivePackLimitKind::Commands),
            (self.max_wanted_objects, ReceivePackLimitKind::WantedObjects),
            (self.max_capabilities, ReceivePackLimitKind::Capabilities),
            (self.max_push_options, ReceivePackLimitKind::PushOptions),
            (self.max_objects, ReceivePackLimitKind::Objects),
            (self.worker_count, ReceivePackLimitKind::Workers),
        ];
        if let Some((_, kind)) = counts.into_iter().find(|(value, _)| *value == 0) {
            return Err(ReceivePackLimitError::Zero(kind));
        }
        if self.max_delta_depth == 0 {
            return Err(ReceivePackLimitError::Zero(
                ReceivePackLimitKind::DeltaDepth,
            ));
        }
        if self.max_duration.is_zero() {
            return Err(ReceivePackLimitError::Zero(ReceivePackLimitKind::Duration));
        }
        let hard = Self::HARD_MAX;
        let hard_values = [
            (
                self.max_request_bytes,
                hard.max_request_bytes,
                ReceivePackLimitKind::RequestBytes,
            ),
            (
                self.max_push_option_bytes,
                hard.max_push_option_bytes,
                ReceivePackLimitKind::PushOptionBytes,
            ),
            (
                self.max_pack_bytes,
                hard.max_pack_bytes,
                ReceivePackLimitKind::PackBytes,
            ),
            (
                self.max_object_bytes,
                hard.max_object_bytes,
                ReceivePackLimitKind::ObjectBytes,
            ),
            (
                self.max_inflated_bytes,
                hard.max_inflated_bytes,
                ReceivePackLimitKind::InflatedBytes,
            ),
            (
                self.max_delta_instruction_bytes,
                hard.max_delta_instruction_bytes,
                ReceivePackLimitKind::DeltaInstructionBytes,
            ),
            (
                self.max_delta_expansion_ratio,
                hard.max_delta_expansion_ratio,
                ReceivePackLimitKind::DeltaExpansionRatio,
            ),
            (
                self.bytes_in_flight,
                hard.bytes_in_flight,
                ReceivePackLimitKind::BytesInFlight,
            ),
            (
                self.max_structural_memory_bytes,
                hard.max_structural_memory_bytes,
                ReceivePackLimitKind::StructuralMemory,
            ),
            (
                self.max_prepared_memory_bytes,
                hard.max_prepared_memory_bytes,
                ReceivePackLimitKind::PreparedMemory,
            ),
            (
                self.max_delta_base_memory_bytes,
                hard.max_delta_base_memory_bytes,
                ReceivePackLimitKind::DeltaBaseMemory,
            ),
            (
                self.max_metadata_memory_bytes,
                hard.max_metadata_memory_bytes,
                ReceivePackLimitKind::MetadataMemory,
            ),
            (
                self.max_sql_statements,
                hard.max_sql_statements,
                ReceivePackLimitKind::SqlStatements,
            ),
            (
                self.max_object_operations,
                hard.max_object_operations,
                ReceivePackLimitKind::ObjectOperations,
            ),
            (
                self.max_range_operations,
                hard.max_range_operations,
                ReceivePackLimitKind::RangeOperations,
            ),
            (
                self.max_external_operations,
                hard.max_external_operations,
                ReceivePackLimitKind::ExternalOperations,
            ),
        ];
        if let Some((_, _, kind)) = hard_values
            .into_iter()
            .find(|(value, maximum, _)| value > maximum)
        {
            return Err(ReceivePackLimitError::ExceedsHardMaximum(kind));
        }
        let hard_counts = [
            (
                self.max_commands,
                hard.max_commands,
                ReceivePackLimitKind::Commands,
            ),
            (
                self.max_wanted_objects,
                hard.max_wanted_objects,
                ReceivePackLimitKind::WantedObjects,
            ),
            (
                self.max_capabilities,
                hard.max_capabilities,
                ReceivePackLimitKind::Capabilities,
            ),
            (
                self.max_push_options,
                hard.max_push_options,
                ReceivePackLimitKind::PushOptions,
            ),
            (
                self.max_objects,
                hard.max_objects,
                ReceivePackLimitKind::Objects,
            ),
            (
                self.worker_count,
                hard.worker_count,
                ReceivePackLimitKind::Workers,
            ),
        ];
        if let Some((_, _, kind)) = hard_counts
            .into_iter()
            .find(|(value, maximum, _)| value > maximum)
        {
            return Err(ReceivePackLimitError::ExceedsHardMaximum(kind));
        }
        if self.max_delta_depth > hard.max_delta_depth {
            return Err(ReceivePackLimitError::ExceedsHardMaximum(
                ReceivePackLimitKind::DeltaDepth,
            ));
        }
        if self.max_duration > hard.max_duration {
            return Err(ReceivePackLimitError::ExceedsHardMaximum(
                ReceivePackLimitKind::Duration,
            ));
        }
        if self.max_pack_bytes > self.max_request_bytes
            || self.max_push_option_bytes > self.max_request_bytes
            || self.max_wanted_objects > self.max_commands
            || self.max_object_bytes > self.max_inflated_bytes
            || self.max_delta_instruction_bytes > self.max_inflated_bytes
        {
            return Err(ReceivePackLimitError::Inconsistent);
        }
        Ok(self)
    }

    /// Validate buffered request shape before copying pack bytes or growing command vectors.
    ///
    /// `request_bytes` and `pack_bytes` are already-observed buffered lengths. The remaining
    /// arguments are decoded counts; all may be zero, including for an empty or delete-only push.
    ///
    /// # Errors
    ///
    /// Returns the first exceeded request dimension.
    pub fn validate_request(
        self,
        request_bytes: usize,
        commands: usize,
        wanted_objects: usize,
        capabilities: usize,
        push_options: usize,
        push_option_bytes: u64,
        pack_bytes: usize,
    ) -> Result<(), ReceivePackLimitError> {
        self.validate()?;
        let request_bytes = u64::try_from(request_bytes)
            .map_err(|_| ReceivePackLimitError::Exceeded(ReceivePackLimitKind::RequestBytes))?;
        let pack_bytes = u64::try_from(pack_bytes)
            .map_err(|_| ReceivePackLimitError::Exceeded(ReceivePackLimitKind::PackBytes))?;
        let commands = u64::try_from(commands)
            .map_err(|_| ReceivePackLimitError::Exceeded(ReceivePackLimitKind::Commands))?;
        let wanted_objects = u64::try_from(wanted_objects)
            .map_err(|_| ReceivePackLimitError::Exceeded(ReceivePackLimitKind::WantedObjects))?;
        let capabilities = u64::try_from(capabilities)
            .map_err(|_| ReceivePackLimitError::Exceeded(ReceivePackLimitKind::Capabilities))?;
        let push_options = u64::try_from(push_options)
            .map_err(|_| ReceivePackLimitError::Exceeded(ReceivePackLimitKind::PushOptions))?;
        let max_commands = u64::try_from(self.max_commands)
            .map_err(|_| ReceivePackLimitError::Exceeded(ReceivePackLimitKind::Commands))?;
        let max_wanted_objects = u64::try_from(self.max_wanted_objects)
            .map_err(|_| ReceivePackLimitError::Exceeded(ReceivePackLimitKind::WantedObjects))?;
        let max_capabilities = u64::try_from(self.max_capabilities)
            .map_err(|_| ReceivePackLimitError::Exceeded(ReceivePackLimitKind::Capabilities))?;
        let max_push_options = u64::try_from(self.max_push_options)
            .map_err(|_| ReceivePackLimitError::Exceeded(ReceivePackLimitKind::PushOptions))?;
        let observations = [
            (
                request_bytes,
                self.max_request_bytes,
                ReceivePackLimitKind::RequestBytes,
            ),
            (commands, max_commands, ReceivePackLimitKind::Commands),
            (
                wanted_objects,
                max_wanted_objects,
                ReceivePackLimitKind::WantedObjects,
            ),
            (
                capabilities,
                max_capabilities,
                ReceivePackLimitKind::Capabilities,
            ),
            (
                push_options,
                max_push_options,
                ReceivePackLimitKind::PushOptions,
            ),
            (
                push_option_bytes,
                self.max_push_option_bytes,
                ReceivePackLimitKind::PushOptionBytes,
            ),
            (
                pack_bytes,
                self.max_pack_bytes,
                ReceivePackLimitKind::PackBytes,
            ),
        ];
        if let Some((_, _, kind)) = observations
            .into_iter()
            .find(|(value, maximum, _)| value > maximum)
        {
            return Err(ReceivePackLimitError::Exceeded(kind));
        }
        Ok(())
    }

    /// Validate decoded PACK totals without allocating object payload buffers.
    ///
    /// Zero objects and zero bytes are valid for an empty or delete-only push. `objects` is the
    /// observed entry count, `largest_object_bytes` is the largest inflated direct object, and
    /// `total_inflated_bytes` includes direct objects and inflated delta instructions.
    ///
    /// # Errors
    ///
    /// Returns the first exceeded object-count or inflated-byte dimension.
    pub fn validate_pack_totals(
        self,
        objects: usize,
        largest_object_bytes: u64,
        total_inflated_bytes: u64,
    ) -> Result<(), ReceivePackLimitError> {
        self.validate()?;
        if objects > self.max_objects {
            return Err(ReceivePackLimitError::Exceeded(
                ReceivePackLimitKind::Objects,
            ));
        }
        if largest_object_bytes > self.max_object_bytes {
            return Err(ReceivePackLimitError::Exceeded(
                ReceivePackLimitKind::ObjectBytes,
            ));
        }
        if total_inflated_bytes > self.max_inflated_bytes {
            return Err(ReceivePackLimitError::Exceeded(
                ReceivePackLimitKind::InflatedBytes,
            ));
        }
        Ok(())
    }

    /// Validate one resolved delta using checked multiplication instead of division.
    ///
    /// `instruction_bytes` is the inflated delta program, `resolved_bytes` is the resulting object
    /// size, and `depth` is the complete resolved chain depth. A zero-byte instruction stream is
    /// invalid because a Git delta always contains base/result size headers.
    ///
    /// # Errors
    ///
    /// Returns a typed instruction-size, depth, or expansion-ratio failure.
    pub fn validate_delta(
        self,
        instruction_bytes: u64,
        resolved_bytes: u64,
        depth: u32,
    ) -> Result<(), ReceivePackLimitError> {
        self.validate()?;
        if instruction_bytes == 0 || instruction_bytes > self.max_delta_instruction_bytes {
            return Err(ReceivePackLimitError::Exceeded(
                ReceivePackLimitKind::DeltaInstructionBytes,
            ));
        }
        if depth == 0 || depth > self.max_delta_depth {
            return Err(ReceivePackLimitError::Exceeded(
                ReceivePackLimitKind::DeltaDepth,
            ));
        }
        if resolved_bytes > self.max_object_bytes {
            return Err(ReceivePackLimitError::Exceeded(
                ReceivePackLimitKind::ObjectBytes,
            ));
        }
        let maximum_resolved = instruction_bytes
            .checked_mul(self.max_delta_expansion_ratio)
            .ok_or(ReceivePackLimitError::Exceeded(
                ReceivePackLimitKind::DeltaExpansionRatio,
            ))?;
        if resolved_bytes > maximum_resolved {
            return Err(ReceivePackLimitError::Exceeded(
                ReceivePackLimitKind::DeltaExpansionRatio,
            ));
        }
        Ok(())
    }

    /// Validate an explicit caller-measured request duration.
    ///
    /// # Errors
    ///
    /// Returns a duration-limit failure when `elapsed` exceeds the configured request lifetime.
    pub fn validate_duration(self, elapsed: Duration) -> Result<(), ReceivePackLimitError> {
        self.validate()?;
        if elapsed > self.max_duration {
            return Err(ReceivePackLimitError::Exceeded(
                ReceivePackLimitKind::Duration,
            ));
        }
        Ok(())
    }

    /// Validate one request-memory high-water observation.
    ///
    /// # Errors
    ///
    /// Returns the first exceeded structural, prepared, delta-base, or metadata memory dimension.
    /// A reported total smaller than an individual component is inconsistent.
    pub fn validate_memory(
        self,
        observation: PushMemoryMetrics,
    ) -> Result<(), ReceivePackLimitError> {
        self.validate()?;
        for (value, maximum, kind) in [
            (
                observation.structural_bytes,
                self.max_structural_memory_bytes,
                ReceivePackLimitKind::StructuralMemory,
            ),
            (
                observation.prepared_bytes,
                self.max_prepared_memory_bytes,
                ReceivePackLimitKind::PreparedMemory,
            ),
            (
                observation.delta_base_bytes,
                self.max_delta_base_memory_bytes,
                ReceivePackLimitKind::DeltaBaseMemory,
            ),
            (
                observation.metadata_bytes,
                self.max_metadata_memory_bytes,
                ReceivePackLimitKind::MetadataMemory,
            ),
        ] {
            if value > maximum {
                return Err(ReceivePackLimitError::Exceeded(kind));
            }
        }
        if observation.total_bytes
            < observation
                .structural_bytes
                .max(observation.prepared_bytes)
                .max(observation.delta_base_bytes)
                .max(observation.metadata_bytes)
        {
            return Err(ReceivePackLimitError::Inconsistent);
        }
        Ok(())
    }

    /// Validate caller-observed request bytes concurrently in flight.
    ///
    /// # Errors
    ///
    /// Returns a bytes-in-flight limit failure when `bytes` exceeds the configured ceiling.
    pub fn validate_bytes_in_flight(self, bytes: u64) -> Result<(), ReceivePackLimitError> {
        self.validate()?;
        if bytes > self.bytes_in_flight {
            return Err(ReceivePackLimitError::Exceeded(
                ReceivePackLimitKind::BytesInFlight,
            ));
        }
        Ok(())
    }

    /// Validate aggregate backend operation counters from a point-in-time snapshot.
    ///
    /// # Errors
    ///
    /// Returns the first exceeded SQL, object, range, or external operation dimension. Object and
    /// external totals use checked addition and fail closed on overflow.
    pub fn validate_backend(
        self,
        observation: PushBackendMetrics,
    ) -> Result<(), ReceivePackLimitError> {
        self.validate()?;
        let object_operations = observation
            .object_reads
            .checked_add(observation.object_writes)
            .ok_or(ReceivePackLimitError::Exceeded(
                ReceivePackLimitKind::ObjectOperations,
            ))?;
        let external_operations = observation
            .external_gets
            .checked_add(observation.external_puts)
            .ok_or(ReceivePackLimitError::Exceeded(
                ReceivePackLimitKind::ExternalOperations,
            ))?;
        for (value, maximum, kind) in [
            (
                observation.sql_statements,
                self.max_sql_statements,
                ReceivePackLimitKind::SqlStatements,
            ),
            (
                object_operations,
                self.max_object_operations,
                ReceivePackLimitKind::ObjectOperations,
            ),
            (
                observation.range_reads,
                self.max_range_operations,
                ReceivePackLimitKind::RangeOperations,
            ),
            (
                external_operations,
                self.max_external_operations,
                ReceivePackLimitKind::ExternalOperations,
            ),
        ] {
            if value > maximum {
                return Err(ReceivePackLimitError::Exceeded(kind));
            }
        }
        Ok(())
    }
}

impl Default for ReceivePackLimits {
    fn default() -> Self {
        Self {
            max_request_bytes: (64_u64 << 30) + (16_u64 << 20),
            max_commands: 1_024,
            max_wanted_objects: 1_024,
            max_capabilities: 256,
            max_push_options: 1_024,
            max_push_option_bytes: 1_u64 << 20,
            max_pack_bytes: 64_u64 << 30,
            max_objects: 2_000_000,
            max_object_bytes: 16_u64 << 30,
            max_inflated_bytes: 256_u64 << 30,
            max_delta_instruction_bytes: 256_u64 << 20,
            max_delta_expansion_ratio: 10_000,
            max_delta_depth: 128,
            worker_count: 8,
            bytes_in_flight: 256_u64 << 20,
            max_structural_memory_bytes: 256_u64 << 20,
            max_prepared_memory_bytes: 1_u64 << 30,
            max_delta_base_memory_bytes: 512_u64 << 20,
            max_metadata_memory_bytes: 256_u64 << 20,
            max_duration: Duration::from_secs(2 * 60 * 60),
            max_sql_statements: 20_000_000,
            max_object_operations: 20_000_000,
            max_range_operations: 2_000_000,
            max_external_operations: 2_000_000,
        }
    }
}

/// Stable receive-pack processing phase.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum PushPhase {
    /// Command/capability parsing and authorization.
    Request,
    /// First input byte through durable quarantine completion.
    Quarantine,
    /// Pack structure, object identity, delta, and closure validation.
    Validation,
    /// Policy, hook, fast-forward, and ref preflight.
    Policy,
    /// Durable object/pack and prepared metadata installation.
    DurableInstall,
    /// Transactional ref and reflog publication.
    RefPublication,
    /// Quarantine cleanup after acceptance or rejection.
    Cleanup,
    /// Complete request lifetime.
    Total,
}

/// Stable limit/policy rejection classification without client-controlled text.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PushRejectionReason {
    /// A typed safety limit was exceeded.
    Limit(ReceivePackLimitKind),
    /// Authentication or repository authorization denied the request.
    Authorization,
    /// A push policy or hook rejected the request.
    Policy,
    /// Compare-and-swap or ref namespace conflict.
    RefConflict,
    /// Branch update was not a fast-forward.
    NonFastForward,
    /// Request or pack protocol validation failed.
    InvalidRequest,
    /// Admission rejected work before execution.
    Admission,
}

/// Stable cancellation classification.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PushCancellationReason {
    /// Client disconnected or cancelled its request.
    Client,
    /// Explicit request deadline elapsed.
    Deadline,
    /// Server shutdown or operator cancellation stopped work.
    Server,
}

/// Stable backend-failure classification.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PushBackendFailureReason {
    /// Repository storage operation failed.
    Storage,
    /// Audit persistence failed.
    Audit,
    /// Ref invalidation publication failed.
    Invalidation,
}

/// First terminal receive-pack outcome.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PushOutcome {
    /// Push completed and refs were published.
    Accepted,
    /// Push was rejected before publication.
    Rejected(PushRejectionReason),
    /// Push stopped due to cancellation.
    Cancelled(PushCancellationReason),
    /// Backend integration failed.
    BackendFailure(PushBackendFailureReason),
    /// Protocol response or transport failed.
    ProtocolFailure,
}

/// Input and decoded object measurements.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PushPackMetrics {
    /// Complete buffered request bytes.
    pub request_bytes: u64,
    /// Compressed PACK bytes.
    pub pack_bytes: u64,
    /// PACK object entries.
    pub objects: u64,
    /// Direct object entries.
    pub direct_objects: u64,
    /// OFS_DELTA entries.
    pub ofs_deltas: u64,
    /// REF_DELTA entries.
    pub ref_deltas: u64,
    /// Inflated direct-object and delta-instruction bytes.
    pub inflated_bytes: u64,
    /// Largest resolved delta depth.
    pub maximum_delta_depth: u32,
    /// Largest resolved/instruction byte ratio.
    pub maximum_delta_expansion_ratio: u64,
}

/// Peak request-owned memory measurements.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PushMemoryMetrics {
    /// Pack structure and command parsing memory.
    pub structural_bytes: u64,
    /// Prepared push/quarantine object memory.
    pub prepared_bytes: u64,
    /// Retained resolved delta-base memory.
    pub delta_base_bytes: u64,
    /// Prepared commit/tree/ref metadata memory.
    pub metadata_bytes: u64,
    /// Peak total request-owned memory.
    pub total_bytes: u64,
}

/// Backend operations and byte volumes.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PushBackendMetrics {
    /// SQL statements.
    pub sql_statements: u64,
    /// SQL affected/returned rows.
    pub sql_rows: u64,
    /// SQL transactions.
    pub sql_transactions: u64,
    /// SQL payload bytes.
    pub sql_bytes: u64,
    /// Object reads.
    pub object_reads: u64,
    /// Object writes.
    pub object_writes: u64,
    /// Object payload bytes read or written.
    pub object_bytes: u64,
    /// Range reads.
    pub range_reads: u64,
    /// Range bytes read.
    pub range_bytes: u64,
    /// External whole-object GET operations.
    pub external_gets: u64,
    /// External PUT operations.
    pub external_puts: u64,
    /// External payload bytes transferred.
    pub external_bytes: u64,
}

/// Quarantine lifecycle byte volumes.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PushQuarantineMetrics {
    /// Compressed bytes appended to quarantine.
    pub appended_bytes: u64,
    /// Bytes promoted into durable live storage.
    pub promoted_bytes: u64,
    /// Bytes reclaimed during cleanup.
    pub cleanup_bytes: u64,
    /// Reclaimable bytes left after failed cleanup.
    pub orphan_bytes: u64,
}

/// Explicit caller-measured phase durations.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PushPhaseMetrics {
    /// Request parsing/authorization duration.
    pub request: Option<Duration>,
    /// First-byte-to-quarantine duration.
    pub quarantine: Option<Duration>,
    /// Pack and closure validation duration.
    pub validation: Option<Duration>,
    /// Policy/preflight duration.
    pub policy: Option<Duration>,
    /// Durable installation duration.
    pub durable_install: Option<Duration>,
    /// Ref publication duration.
    pub ref_publication: Option<Duration>,
    /// Cleanup duration.
    pub cleanup: Option<Duration>,
    /// Complete request duration.
    pub total: Option<Duration>,
}

/// Point-in-time receive-pack metrics report.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PushMetricsReport {
    /// First terminal outcome, or `None` while in progress.
    pub outcome: Option<PushOutcome>,
    /// Input and decoded PACK observations.
    pub pack: PushPackMetrics,
    /// Peak request memory observations.
    pub memory: PushMemoryMetrics,
    /// Backend operation observations.
    pub backend: PushBackendMetrics,
    /// Quarantine byte lifecycle.
    pub quarantine: PushQuarantineMetrics,
    /// Explicit phase durations.
    pub phases: PushPhaseMetrics,
}

/// Metric counter whose checked addition overflowed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PushMetricCounter {
    /// Any PACK/input counter.
    Pack,
    /// Any request-memory counter.
    Memory,
    /// Any backend counter.
    Backend,
    /// Any quarantine counter.
    Quarantine,
}

/// Typed push recorder failure.
#[derive(Clone, Copy, Debug, thiserror::Error, PartialEq, Eq)]
pub enum PushMetricsError {
    /// Checked counter addition overflowed.
    #[error("push metric counter {0:?} overflowed")]
    Overflow(PushMetricCounter),
    /// A single-assignment observation was recorded more than once.
    #[error("push metric observation was already recorded")]
    AlreadyRecorded,
    /// A terminal report cannot accept later observations.
    #[error("push metric recorder is already finished")]
    Finished,
    /// A phase repeated or moved backwards.
    #[error("push metric phase transition is not monotonic")]
    InvalidPhaseTransition,
    /// A component duration exceeded the already-recorded total.
    #[error("push metric phase duration exceeds total duration")]
    DurationExceedsTotal,
    /// Component durations could not be summed without overflow.
    #[error("push metric phase durations overflowed")]
    DurationOverflow,
}

/// Request-scoped checked recorder with an optional lightweight no-op mode.
#[derive(Clone, Debug, Default)]
pub struct PushMetricsRecorder {
    enabled: bool,
    report: PushMetricsReport,
    last_phase: Option<PushPhase>,
    input_recorded: bool,
}

impl PushMetricsRecorder {
    /// Construct an enabled request recorder.
    #[must_use]
    pub fn enabled() -> Self {
        Self {
            enabled: true,
            ..Self::default()
        }
    }

    /// Construct a recorder whose methods perform no work.
    #[must_use]
    pub fn disabled() -> Self {
        Self::default()
    }

    /// Return whether observations are retained.
    #[must_use]
    pub const fn is_enabled(&self) -> bool {
        self.enabled
    }

    /// Record complete request and compressed pack bytes once.
    ///
    /// # Errors
    ///
    /// Returns overflow if an observation cannot fit its counter, already-recorded for a second
    /// input observation, or finished after a terminal outcome.
    pub fn record_input(
        &mut self,
        request_bytes: usize,
        pack_bytes: usize,
    ) -> Result<(), PushMetricsError> {
        if !self.enabled {
            return Ok(());
        }
        self.ensure_active()?;
        if self.input_recorded {
            return Err(PushMetricsError::AlreadyRecorded);
        }
        let request_bytes = u64::try_from(request_bytes)
            .map_err(|_| PushMetricsError::Overflow(PushMetricCounter::Pack))?;
        let pack_bytes = u64::try_from(pack_bytes)
            .map_err(|_| PushMetricsError::Overflow(PushMetricCounter::Pack))?;
        self.report.pack.request_bytes = request_bytes;
        self.report.pack.pack_bytes = pack_bytes;
        self.input_recorded = true;
        Ok(())
    }

    /// Add decoded PACK entry and inflated-byte observations.
    ///
    /// # Errors
    ///
    /// Returns overflow without partially changing counters, or finished after a terminal outcome.
    pub fn record_objects(
        &mut self,
        direct: u64,
        ofs_deltas: u64,
        ref_deltas: u64,
        inflated_bytes: u64,
        maximum_delta_depth: u32,
        maximum_delta_expansion_ratio: u64,
    ) -> Result<(), PushMetricsError> {
        if !self.enabled {
            return Ok(());
        }
        self.ensure_active()?;
        let objects = direct
            .checked_add(ofs_deltas)
            .and_then(|value| value.checked_add(ref_deltas))
            .ok_or(PushMetricsError::Overflow(PushMetricCounter::Pack))?;
        let current = self.report.pack;
        let next = PushPackMetrics {
            request_bytes: current.request_bytes,
            pack_bytes: current.pack_bytes,
            objects: current
                .objects
                .checked_add(objects)
                .ok_or(PushMetricsError::Overflow(PushMetricCounter::Pack))?,
            direct_objects: current
                .direct_objects
                .checked_add(direct)
                .ok_or(PushMetricsError::Overflow(PushMetricCounter::Pack))?,
            ofs_deltas: current
                .ofs_deltas
                .checked_add(ofs_deltas)
                .ok_or(PushMetricsError::Overflow(PushMetricCounter::Pack))?,
            ref_deltas: current
                .ref_deltas
                .checked_add(ref_deltas)
                .ok_or(PushMetricsError::Overflow(PushMetricCounter::Pack))?,
            inflated_bytes: current
                .inflated_bytes
                .checked_add(inflated_bytes)
                .ok_or(PushMetricsError::Overflow(PushMetricCounter::Pack))?,
            maximum_delta_depth: current.maximum_delta_depth.max(maximum_delta_depth),
            maximum_delta_expansion_ratio: current
                .maximum_delta_expansion_ratio
                .max(maximum_delta_expansion_ratio),
        };
        self.report.pack = next;
        Ok(())
    }

    /// Raise memory high-water marks without decreasing earlier observations.
    ///
    /// # Errors
    ///
    /// Returns overflow when the observation's component total cannot be represented, or finished
    /// when a terminal outcome was already recorded.
    pub fn observe_memory(
        &mut self,
        observation: PushMemoryMetrics,
    ) -> Result<(), PushMetricsError> {
        if !self.enabled {
            return Ok(());
        }
        self.ensure_active()?;
        let component_total = observation
            .structural_bytes
            .checked_add(observation.prepared_bytes)
            .and_then(|value| value.checked_add(observation.delta_base_bytes))
            .and_then(|value| value.checked_add(observation.metadata_bytes))
            .ok_or(PushMetricsError::Overflow(PushMetricCounter::Memory))?;
        self.report.memory.structural_bytes = self
            .report
            .memory
            .structural_bytes
            .max(observation.structural_bytes);
        self.report.memory.prepared_bytes = self
            .report
            .memory
            .prepared_bytes
            .max(observation.prepared_bytes);
        self.report.memory.delta_base_bytes = self
            .report
            .memory
            .delta_base_bytes
            .max(observation.delta_base_bytes);
        self.report.memory.metadata_bytes = self
            .report
            .memory
            .metadata_bytes
            .max(observation.metadata_bytes);
        self.report.memory.total_bytes = self
            .report
            .memory
            .total_bytes
            .max(observation.total_bytes)
            .max(component_total);
        Ok(())
    }

    /// Add backend observations using checked counters.
    ///
    /// # Errors
    ///
    /// Returns overflow without partially applying the observation, or finished after a terminal
    /// outcome.
    pub fn record_backend(
        &mut self,
        observation: PushBackendMetrics,
    ) -> Result<(), PushMetricsError> {
        if !self.enabled {
            return Ok(());
        }
        self.ensure_active()?;
        let current = self.report.backend;
        let next = PushBackendMetrics {
            sql_statements: checked_metric(
                current.sql_statements,
                observation.sql_statements,
                PushMetricCounter::Backend,
            )?,
            sql_rows: checked_metric(
                current.sql_rows,
                observation.sql_rows,
                PushMetricCounter::Backend,
            )?,
            sql_transactions: checked_metric(
                current.sql_transactions,
                observation.sql_transactions,
                PushMetricCounter::Backend,
            )?,
            sql_bytes: checked_metric(
                current.sql_bytes,
                observation.sql_bytes,
                PushMetricCounter::Backend,
            )?,
            object_reads: checked_metric(
                current.object_reads,
                observation.object_reads,
                PushMetricCounter::Backend,
            )?,
            object_writes: checked_metric(
                current.object_writes,
                observation.object_writes,
                PushMetricCounter::Backend,
            )?,
            object_bytes: checked_metric(
                current.object_bytes,
                observation.object_bytes,
                PushMetricCounter::Backend,
            )?,
            range_reads: checked_metric(
                current.range_reads,
                observation.range_reads,
                PushMetricCounter::Backend,
            )?,
            range_bytes: checked_metric(
                current.range_bytes,
                observation.range_bytes,
                PushMetricCounter::Backend,
            )?,
            external_gets: checked_metric(
                current.external_gets,
                observation.external_gets,
                PushMetricCounter::Backend,
            )?,
            external_puts: checked_metric(
                current.external_puts,
                observation.external_puts,
                PushMetricCounter::Backend,
            )?,
            external_bytes: checked_metric(
                current.external_bytes,
                observation.external_bytes,
                PushMetricCounter::Backend,
            )?,
        };
        self.report.backend = next;
        Ok(())
    }

    /// Add quarantine lifecycle bytes using checked counters.
    ///
    /// # Errors
    ///
    /// Returns overflow without partially applying the observation, or finished after a terminal
    /// outcome.
    pub fn record_quarantine(
        &mut self,
        observation: PushQuarantineMetrics,
    ) -> Result<(), PushMetricsError> {
        if !self.enabled {
            return Ok(());
        }
        self.ensure_active()?;
        let current = self.report.quarantine;
        let next = PushQuarantineMetrics {
            appended_bytes: checked_metric(
                current.appended_bytes,
                observation.appended_bytes,
                PushMetricCounter::Quarantine,
            )?,
            promoted_bytes: checked_metric(
                current.promoted_bytes,
                observation.promoted_bytes,
                PushMetricCounter::Quarantine,
            )?,
            cleanup_bytes: checked_metric(
                current.cleanup_bytes,
                observation.cleanup_bytes,
                PushMetricCounter::Quarantine,
            )?,
            orphan_bytes: checked_metric(
                current.orphan_bytes,
                observation.orphan_bytes,
                PushMetricCounter::Quarantine,
            )?,
        };
        self.report.quarantine = next;
        Ok(())
    }

    /// Record one explicit phase duration in monotonic phase order.
    ///
    /// # Errors
    ///
    /// Rejects repeated/backward phases, a component sum longer than total duration, duration-sum
    /// overflow, or recording after a terminal outcome.
    pub fn record_phase(
        &mut self,
        phase: PushPhase,
        duration: Duration,
    ) -> Result<(), PushMetricsError> {
        if !self.enabled {
            return Ok(());
        }
        self.ensure_active()?;
        if self.last_phase.is_some_and(|last| phase <= last) {
            return Err(PushMetricsError::InvalidPhaseTransition);
        }
        if phase != PushPhase::Total
            && self
                .report
                .phases
                .total
                .is_some_and(|total| duration > total)
        {
            return Err(PushMetricsError::DurationExceedsTotal);
        }
        if phase == PushPhase::Total {
            let components = self.report.phases;
            let component_total = [
                components.request,
                components.quarantine,
                components.validation,
                components.policy,
                components.durable_install,
                components.ref_publication,
                components.cleanup,
            ]
            .into_iter()
            .flatten()
            .try_fold(Duration::ZERO, |total, component| {
                total.checked_add(component)
            })
            .ok_or(PushMetricsError::DurationOverflow)?;
            if component_total > duration {
                return Err(PushMetricsError::DurationExceedsTotal);
            }
        }
        match phase {
            PushPhase::Request => self.report.phases.request = Some(duration),
            PushPhase::Quarantine => self.report.phases.quarantine = Some(duration),
            PushPhase::Validation => self.report.phases.validation = Some(duration),
            PushPhase::Policy => self.report.phases.policy = Some(duration),
            PushPhase::DurableInstall => self.report.phases.durable_install = Some(duration),
            PushPhase::RefPublication => self.report.phases.ref_publication = Some(duration),
            PushPhase::Cleanup => self.report.phases.cleanup = Some(duration),
            PushPhase::Total => self.report.phases.total = Some(duration),
        }
        self.last_phase = Some(phase);
        Ok(())
    }

    /// Preserve the first terminal outcome and return a stable report snapshot.
    #[must_use]
    pub fn finish(&mut self, outcome: PushOutcome) -> PushMetricsReport {
        if self.enabled && self.report.outcome.is_none() {
            self.report.outcome = Some(outcome);
        }
        self.report.clone()
    }

    /// Return a point-in-time report without changing terminal state.
    #[must_use]
    pub fn snapshot(&self) -> PushMetricsReport {
        self.report.clone()
    }

    fn ensure_active(&self) -> Result<(), PushMetricsError> {
        if self.report.outcome.is_some() {
            return Err(PushMetricsError::Finished);
        }
        Ok(())
    }
}

fn checked_metric(
    current: u64,
    additional: u64,
    counter: PushMetricCounter,
) -> Result<u64, PushMetricsError> {
    current
        .checked_add(additional)
        .ok_or(PushMetricsError::Overflow(counter))
}
