//! Runtime-agnostic weighted admission control for backend work.
//!
//! The controller is deliberately synchronous: callers provide every timestamp and decide when
//! to poll queued tickets. It performs no sleeping, retries, environment reads, or backend I/O.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};

use time::{Duration, OffsetDateTime};

use crate::ids::{RepositoryId, TenantId};

/// Workload class used for priority and capacity isolation.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ResourceClass {
    /// Ref, summary, and tree metadata reads.
    InteractiveMetadata,
    /// Clone and fetch response construction.
    Clone,
    /// Push validation and publication.
    Push,
    /// Repository import and migration work.
    ImportMigration,
    /// Repack, repair, and cleanup work.
    Maintenance,
    /// Large-object and expensive delta work.
    LargeObject,
}

impl ResourceClass {
    const COUNT: usize = 6;

    const fn index(self) -> usize {
        match self {
            Self::InteractiveMetadata => 0,
            Self::Push => 1,
            Self::Clone => 2,
            Self::LargeObject => 3,
            Self::ImportMigration => 4,
            Self::Maintenance => 5,
        }
    }
}

/// Stable request ownership used for concurrency isolation.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct AdmissionIdentity {
    /// Owning tenant.
    pub tenant: TenantId,
    /// Target repository.
    pub repository: RepositoryId,
    /// Authenticated actor identity.
    pub actor: String,
}

impl AdmissionIdentity {
    /// Construct a validated request identity.
    ///
    /// # Errors
    ///
    /// Returns [`AdmissionConfigError::Invalid`] for an empty or oversized actor.
    pub fn new(
        tenant: TenantId,
        repository: RepositoryId,
        actor: impl Into<String>,
    ) -> Result<Self, AdmissionConfigError> {
        let actor = actor.into();
        if actor.trim().is_empty() || actor.len() > 512 {
            return Err(AdmissionConfigError::Invalid("actor identity"));
        }
        Ok(Self {
            tenant,
            repository,
            actor,
        })
    }
}

/// Weighted resources charged while a request is admitted.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ResourceWeights {
    /// Concurrent execution slots.
    pub slots: u32,
    /// Peak resident memory estimate.
    pub memory_bytes: u64,
    /// Backend and network I/O byte estimate.
    pub io_bytes: u64,
    /// Relative CPU work units.
    pub cpu_units: u64,
    /// SQL connection or query units.
    pub sql_units: u64,
    /// S3 request or transfer units.
    pub s3_units: u64,
}

impl ResourceWeights {
    fn checked_add(self, other: Self) -> Option<Self> {
        Some(Self {
            slots: self.slots.checked_add(other.slots)?,
            memory_bytes: self.memory_bytes.checked_add(other.memory_bytes)?,
            io_bytes: self.io_bytes.checked_add(other.io_bytes)?,
            cpu_units: self.cpu_units.checked_add(other.cpu_units)?,
            sql_units: self.sql_units.checked_add(other.sql_units)?,
            s3_units: self.s3_units.checked_add(other.s3_units)?,
        })
    }

    fn checked_sub(self, other: Self) -> Option<Self> {
        Some(Self {
            slots: self.slots.checked_sub(other.slots)?,
            memory_bytes: self.memory_bytes.checked_sub(other.memory_bytes)?,
            io_bytes: self.io_bytes.checked_sub(other.io_bytes)?,
            cpu_units: self.cpu_units.checked_sub(other.cpu_units)?,
            sql_units: self.sql_units.checked_sub(other.sql_units)?,
            s3_units: self.s3_units.checked_sub(other.s3_units)?,
        })
    }

    fn fits(self, limit: Self) -> bool {
        self.slots <= limit.slots
            && self.memory_bytes <= limit.memory_bytes
            && self.io_bytes <= limit.io_bytes
            && self.cpu_units <= limit.cpu_units
            && self.sql_units <= limit.sql_units
            && self.s3_units <= limit.s3_units
    }

    fn scaled(self, remaining_basis_points: u16) -> Self {
        let scale = |value: u64| {
            value
                .saturating_mul(u64::from(remaining_basis_points))
                .checked_div(10_000)
                .unwrap_or_default()
        };
        Self {
            slots: u32::try_from(scale(u64::from(self.slots))).unwrap_or_default(),
            memory_bytes: scale(self.memory_bytes),
            io_bytes: scale(self.io_bytes),
            cpu_units: scale(self.cpu_units),
            sql_units: scale(self.sql_units),
            s3_units: scale(self.s3_units),
        }
    }
}

/// Per-class weighted capacities.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ClassCapacities {
    values: [ResourceWeights; ResourceClass::COUNT],
}

impl ClassCapacities {
    /// Construct capacities in enum declaration order.
    #[must_use]
    pub const fn new(
        interactive: ResourceWeights,
        clone: ResourceWeights,
        push: ResourceWeights,
        import_migration: ResourceWeights,
        maintenance: ResourceWeights,
        large_object: ResourceWeights,
    ) -> Self {
        let mut values = [ResourceWeights {
            slots: 0,
            memory_bytes: 0,
            io_bytes: 0,
            cpu_units: 0,
            sql_units: 0,
            s3_units: 0,
        }; ResourceClass::COUNT];
        values[ResourceClass::InteractiveMetadata.index()] = interactive;
        values[ResourceClass::Clone.index()] = clone;
        values[ResourceClass::Push.index()] = push;
        values[ResourceClass::ImportMigration.index()] = import_migration;
        values[ResourceClass::Maintenance.index()] = maintenance;
        values[ResourceClass::LargeObject.index()] = large_object;
        Self { values }
    }

    /// Return the capacity assigned to `class`.
    #[must_use]
    pub const fn get(self, class: ResourceClass) -> ResourceWeights {
        self.values[class.index()]
    }
}

/// Validated admission controller limits.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AdmissionLimits {
    /// Capacity shared by all classes.
    pub global: ResourceWeights,
    /// Capacity isolated per workload class.
    pub classes: ClassCapacities,
    /// Global capacity unavailable to non-interactive classes.
    pub reserved_interactive: ResourceWeights,
    /// Independent ceiling for large-object work.
    pub large_object: ResourceWeights,
    /// Weighted capacity allowed per tenant.
    pub tenant: ResourceWeights,
    /// Weighted capacity allowed per repository within a tenant.
    pub repository: ResourceWeights,
    /// Weighted capacity allowed per actor within a tenant.
    pub actor: ResourceWeights,
    /// Maximum queued requests.
    pub max_queue: usize,
}

impl AdmissionLimits {
    fn validate(self) -> Result<Self, AdmissionConfigError> {
        if self.global.slots == 0
            || self.tenant.slots == 0
            || self.repository.slots == 0
            || self.actor.slots == 0
            || self.max_queue == 0
            || !self.reserved_interactive.fits(self.global)
            || self.classes.values.iter().any(|limit| limit.slots == 0)
            || self.large_object.slots == 0
        {
            return Err(AdmissionConfigError::Invalid("admission limits"));
        }
        Ok(self)
    }
}

/// Caller-observed backend utilization, in basis points.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct BackendPressure {
    /// Database pool utilization.
    pub sql_basis_points: u16,
    /// S3 latency/saturation signal.
    pub s3_basis_points: u16,
    /// Memory pressure.
    pub memory_basis_points: u16,
    /// Worker/CPU utilization.
    pub worker_basis_points: u16,
}

impl BackendPressure {
    /// Validate backend pressure values.
    ///
    /// # Errors
    ///
    /// Returns [`AdmissionConfigError::Invalid`] when a value exceeds 10,000.
    pub fn validate(self) -> Result<Self, AdmissionConfigError> {
        if [
            self.sql_basis_points,
            self.s3_basis_points,
            self.memory_basis_points,
            self.worker_basis_points,
        ]
        .iter()
        .any(|value| *value > 10_000)
        {
            return Err(AdmissionConfigError::Invalid("backend pressure"));
        }
        Ok(self)
    }

    fn apply(self, limit: ResourceWeights) -> ResourceWeights {
        let mut effective = limit.scaled(10_000_u16.saturating_sub(self.worker_basis_points));
        effective.memory_bytes = limit.memory_bytes.saturating_mul(u64::from(
            10_000_u16.saturating_sub(self.memory_basis_points),
        )) / 10_000;
        effective.sql_units = limit
            .sql_units
            .saturating_mul(u64::from(10_000_u16.saturating_sub(self.sql_basis_points)))
            / 10_000;
        effective.s3_units = limit
            .s3_units
            .saturating_mul(u64::from(10_000_u16.saturating_sub(self.s3_basis_points)))
            / 10_000;
        effective
    }

    fn hard_overloaded(self, weights: ResourceWeights) -> bool {
        (weights.sql_units > 0 && self.sql_basis_points >= 9_500)
            || (weights.s3_units > 0 && self.s3_basis_points >= 9_500)
            || (weights.memory_bytes > 0 && self.memory_basis_points >= 9_500)
            || (weights.cpu_units > 0 && self.worker_basis_points >= 9_500)
    }
}

/// One admission request with explicit timing and service estimate.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AdmissionRequest {
    /// Workload class.
    pub class: ResourceClass,
    /// Tenant, repository, and actor ownership.
    pub identity: AdmissionIdentity,
    /// Charged resources.
    pub weights: ResourceWeights,
    /// Caller-supplied queue submission time.
    pub submitted_at: OffsetDateTime,
    /// Latest acceptable completion time.
    pub deadline: OffsetDateTime,
    /// Caller-estimated execution time.
    pub service_estimate: Duration,
}

/// Stable queue ticket.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct AdmissionTicket(u64);

impl AdmissionTicket {
    /// Return the controller-local ticket number.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

/// Invalid controller configuration or submission.
#[derive(Clone, Debug, thiserror::Error, PartialEq, Eq)]
pub enum AdmissionConfigError {
    /// A typed value violated its documented bounds.
    #[error("invalid {0}")]
    Invalid(&'static str),
    /// The request can never fit configured capacity.
    #[error("request exceeds configured capacity")]
    ExceedsCapacity,
    /// The bounded queue is full.
    #[error("admission queue is full")]
    QueueFull,
    /// Memory for a bounded controller record could not be reserved.
    #[error("admission controller allocation failed")]
    Allocation,
}

/// Typed reason a queued request was rejected.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AdmissionRejectReason {
    /// The ticket is not queued or active.
    UnknownTicket,
    /// The request deadline has passed or cannot include its service estimate.
    Deadline,
    /// A relevant backend is severely saturated.
    BackendOverloaded,
    /// A grant record could not be allocated.
    AllocationPressure,
}

/// Explicit caller retry guidance; the controller never retries automatically.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RetryGuidance {
    /// Retrying this ticket cannot succeed.
    DoNotRetry,
    /// An idempotent caller may choose a jittered time inside this bounded window.
    RetryWindow {
        /// Earliest suggested retry time.
        earliest: OffsetDateTime,
        /// Latest suggested retry time.
        latest: OffsetDateTime,
    },
}

/// Result of polling one stable ticket.
#[derive(Debug)]
pub enum AdmissionDecision {
    /// Resources were charged until the permit is dropped or cancelled.
    Granted(AdmissionPermit),
    /// Work remains queued at the reported priority/FIFO position.
    Queued {
        /// One-based queue position.
        position: usize,
        /// Sum of service estimates ahead of this request.
        estimated_wait: Duration,
    },
    /// Work was removed from the queue.
    Rejected {
        /// Stable rejection category.
        reason: AdmissionRejectReason,
        /// Explicit bounded retry advice.
        retry: RetryGuidance,
    },
}

#[derive(Clone, Debug)]
struct QueuedRequest {
    ticket: AdmissionTicket,
    request: AdmissionRequest,
}

#[derive(Debug)]
struct State {
    next_ticket: u64,
    queue: VecDeque<QueuedRequest>,
    active: HashMap<AdmissionTicket, AdmissionRequest>,
    global: ResourceWeights,
    classes: [ResourceWeights; ResourceClass::COUNT],
    large: ResourceWeights,
    tenants: HashMap<TenantId, ResourceWeights>,
    repositories: HashMap<(TenantId, RepositoryId), ResourceWeights>,
    actors: HashMap<(TenantId, String), ResourceWeights>,
    last_tenant: [Option<TenantId>; ResourceClass::COUNT],
    total_queue_time: Duration,
    grants: u64,
    rejections: u64,
    cancellations: u64,
}

impl Default for State {
    fn default() -> Self {
        Self {
            next_ticket: 1,
            queue: VecDeque::new(),
            active: HashMap::new(),
            global: ResourceWeights::default(),
            classes: [ResourceWeights::default(); ResourceClass::COUNT],
            large: ResourceWeights::default(),
            tenants: HashMap::new(),
            repositories: HashMap::new(),
            actors: HashMap::new(),
            last_tenant: std::array::from_fn(|_| None),
            total_queue_time: Duration::ZERO,
            grants: 0,
            rejections: 0,
            cancellations: 0,
        }
    }
}

/// Thread-safe, runtime-agnostic admission controller.
#[derive(Clone, Debug)]
pub struct AdmissionController {
    inner: Arc<AdmissionInner>,
}

#[derive(Debug)]
struct AdmissionInner {
    limits: AdmissionLimits,
    state: Mutex<State>,
}

impl AdmissionController {
    /// Construct a controller after validating all configured limits.
    ///
    /// # Errors
    ///
    /// Returns [`AdmissionConfigError::Invalid`] for inconsistent limits.
    pub fn new(limits: AdmissionLimits) -> Result<Self, AdmissionConfigError> {
        Ok(Self {
            inner: Arc::new(AdmissionInner {
                limits: limits.validate()?,
                state: Mutex::new(State::default()),
            }),
        })
    }

    /// Submit one request and return its stable ticket without executing work.
    ///
    /// # Errors
    ///
    /// Rejects malformed timing/weights, impossible capacity, a full queue, or allocation failure.
    pub fn submit(
        &self,
        request: AdmissionRequest,
    ) -> Result<AdmissionTicket, AdmissionConfigError> {
        validate_request(&request)?;
        if !request_fits_static(self.inner.limits, &request) {
            return Err(AdmissionConfigError::ExceedsCapacity);
        }
        let mut state = self
            .inner
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if state.queue.len() >= self.inner.limits.max_queue {
            return Err(AdmissionConfigError::QueueFull);
        }
        state
            .queue
            .try_reserve(1)
            .map_err(|_| AdmissionConfigError::Allocation)?;
        let ticket = AdmissionTicket(state.next_ticket);
        state.next_ticket = state
            .next_ticket
            .checked_add(1)
            .ok_or(AdmissionConfigError::Invalid("ticket sequence"))?;
        state.queue.push_back(QueuedRequest { ticket, request });
        Ok(ticket)
    }

    /// Poll a ticket using caller-observed time and backend pressure.
    ///
    /// Priority is deterministic by class and tenant round-robin, preserving FIFO within each
    /// tenant. Capacity-blocked tickets are skipped so they do not stall unrelated tenants.
    #[must_use]
    pub fn poll(
        &self,
        ticket: AdmissionTicket,
        observed_at: OffsetDateTime,
        pressure: BackendPressure,
    ) -> AdmissionDecision {
        let mut state = self
            .inner
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if pressure.validate().is_err() {
            if let Some(index) = state
                .queue
                .iter()
                .position(|queued| queued.ticket == ticket)
            {
                state.queue.remove(index);
                state.rejections = state.rejections.saturating_add(1);
            }
            return AdmissionDecision::Rejected {
                reason: AdmissionRejectReason::BackendOverloaded,
                retry: RetryGuidance::DoNotRetry,
            };
        }
        if state.active.contains_key(&ticket) {
            return AdmissionDecision::Rejected {
                reason: AdmissionRejectReason::UnknownTicket,
                retry: RetryGuidance::DoNotRetry,
            };
        }
        let Some(index) = state
            .queue
            .iter()
            .position(|queued| queued.ticket == ticket)
        else {
            return AdmissionDecision::Rejected {
                reason: AdmissionRejectReason::UnknownTicket,
                retry: RetryGuidance::DoNotRetry,
            };
        };
        let request = state.queue[index].request.clone();
        if observed_at >= request.deadline
            || observed_at
                .checked_add(request.service_estimate)
                .is_none_or(|completion| completion > request.deadline)
        {
            state.queue.remove(index);
            state.rejections = state.rejections.saturating_add(1);
            return AdmissionDecision::Rejected {
                reason: AdmissionRejectReason::Deadline,
                retry: RetryGuidance::DoNotRetry,
            };
        }
        if pressure.hard_overloaded(request.weights) {
            state.queue.remove(index);
            state.rejections = state.rejections.saturating_add(1);
            let earliest = observed_at.saturating_add(Duration::seconds(1));
            return AdmissionDecision::Rejected {
                reason: AdmissionRejectReason::BackendOverloaded,
                retry: RetryGuidance::RetryWindow {
                    earliest,
                    latest: observed_at.saturating_add(Duration::seconds(5)),
                },
            };
        }
        let Ok(order) = fair_queue_order(&state) else {
            state.queue.remove(index);
            state.rejections = state.rejections.saturating_add(1);
            return AdmissionDecision::Rejected {
                reason: AdmissionRejectReason::AllocationPressure,
                retry: RetryGuidance::DoNotRetry,
            };
        };
        let position = order
            .iter()
            .position(|queued_index| *queued_index == index)
            .unwrap_or_default();
        let estimated_wait = order[..position]
            .iter()
            .filter(|queued_index| {
                can_charge(
                    &state,
                    self.inner.limits,
                    &state.queue[**queued_index].request,
                    pressure,
                )
            })
            .fold(Duration::ZERO, |wait, queued_index| {
                wait.saturating_add(state.queue[*queued_index].request.service_estimate)
            });
        if observed_at
            .checked_add(estimated_wait)
            .and_then(|start| start.checked_add(request.service_estimate))
            .is_none_or(|completion| completion > request.deadline)
        {
            state.queue.remove(index);
            state.rejections = state.rejections.saturating_add(1);
            return AdmissionDecision::Rejected {
                reason: AdmissionRejectReason::Deadline,
                retry: RetryGuidance::DoNotRetry,
            };
        }
        let selected = order.iter().copied().find(|queued_index| {
            can_charge(
                &state,
                self.inner.limits,
                &state.queue[*queued_index].request,
                pressure,
            )
        });
        if selected != Some(index) {
            return AdmissionDecision::Queued {
                position: position.saturating_add(1),
                estimated_wait,
            };
        }
        if state.active.try_reserve(1).is_err() {
            state.queue.remove(index);
            state.rejections = state.rejections.saturating_add(1);
            return AdmissionDecision::Rejected {
                reason: AdmissionRejectReason::AllocationPressure,
                retry: RetryGuidance::DoNotRetry,
            };
        }
        let Some(queued) = state.queue.remove(index) else {
            return AdmissionDecision::Rejected {
                reason: AdmissionRejectReason::UnknownTicket,
                retry: RetryGuidance::DoNotRetry,
            };
        };
        if !charge(&mut state, &queued.request) {
            state.rejections = state.rejections.saturating_add(1);
            return AdmissionDecision::Rejected {
                reason: AdmissionRejectReason::AllocationPressure,
                retry: RetryGuidance::DoNotRetry,
            };
        }
        state.total_queue_time = state
            .total_queue_time
            .saturating_add(observed_at - queued.request.submitted_at);
        state.grants = state.grants.saturating_add(1);
        state.last_tenant[request.class.index()] = Some(request.identity.tenant.clone());
        state.active.insert(ticket, queued.request);
        AdmissionDecision::Granted(AdmissionPermit {
            ticket,
            inner: Arc::clone(&self.inner),
        })
    }

    /// Cancel queued or active work. Repeating cancellation is a no-op.
    #[must_use]
    pub fn cancel(&self, ticket: AdmissionTicket) -> bool {
        let mut state = self
            .inner
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if let Some(index) = state
            .queue
            .iter()
            .position(|queued| queued.ticket == ticket)
        {
            state.queue.remove(index);
            state.cancellations = state.cancellations.saturating_add(1);
            return true;
        }
        if release(&mut state, ticket) {
            state.cancellations = state.cancellations.saturating_add(1);
            return true;
        }
        false
    }

    /// Return a point-in-time usage and queue metric snapshot.
    #[must_use]
    pub fn snapshot(&self) -> AdmissionSnapshot {
        let state = self
            .inner
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        AdmissionSnapshot {
            queued: state.queue.len(),
            active: state.active.len(),
            global_usage: state.global,
            class_usage: ClassCapacities {
                values: state.classes,
            },
            large_object_usage: state.large,
            grants: state.grants,
            rejections: state.rejections,
            cancellations: state.cancellations,
            total_queue_time: state.total_queue_time,
        }
    }
}

/// RAII resource lease; dropping it releases every charged counter.
#[derive(Debug)]
pub struct AdmissionPermit {
    ticket: AdmissionTicket,
    inner: Arc<AdmissionInner>,
}

impl AdmissionPermit {
    /// Return the ticket represented by this permit.
    #[must_use]
    pub const fn ticket(&self) -> AdmissionTicket {
        self.ticket
    }
}

impl Drop for AdmissionPermit {
    fn drop(&mut self) {
        let mut state = self
            .inner
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        release(&mut state, self.ticket);
    }
}

/// Observable controller counters and queue-time metrics.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AdmissionSnapshot {
    /// Number of queued requests.
    pub queued: usize,
    /// Number of granted permits.
    pub active: usize,
    /// Current global weighted usage.
    pub global_usage: ResourceWeights,
    /// Current per-class weighted usage.
    pub class_usage: ClassCapacities,
    /// Current large-object pool usage.
    pub large_object_usage: ResourceWeights,
    /// Total successful grants.
    pub grants: u64,
    /// Total post-submission rejections.
    pub rejections: u64,
    /// Total successful cancellations.
    pub cancellations: u64,
    /// Sum of observed queue durations for granted requests.
    pub total_queue_time: Duration,
}

fn validate_request(request: &AdmissionRequest) -> Result<(), AdmissionConfigError> {
    if request.weights.slots == 0
        || request.service_estimate <= Duration::ZERO
        || request.deadline <= request.submitted_at
        || request
            .submitted_at
            .checked_add(request.service_estimate)
            .is_none_or(|completion| completion > request.deadline)
    {
        return Err(AdmissionConfigError::Invalid("admission request"));
    }
    Ok(())
}

fn request_fits_static(limits: AdmissionLimits, request: &AdmissionRequest) -> bool {
    let global = if request.class == ResourceClass::InteractiveMetadata {
        limits.global
    } else {
        limits
            .global
            .checked_sub(limits.reserved_interactive)
            .unwrap_or_default()
    };
    request.weights.fits(global)
        && request.weights.fits(limits.classes.get(request.class))
        && (request.class != ResourceClass::LargeObject
            || request.weights.fits(limits.large_object))
        && request.weights.fits(limits.tenant)
        && request.weights.fits(limits.repository)
        && request.weights.fits(limits.actor)
}

fn can_charge(
    state: &State,
    limits: AdmissionLimits,
    request: &AdmissionRequest,
    pressure: BackendPressure,
) -> bool {
    let global_limit = if request.class == ResourceClass::InteractiveMetadata {
        limits.global
    } else {
        limits
            .global
            .checked_sub(limits.reserved_interactive)
            .unwrap_or_default()
    };
    let weighted = state
        .global
        .checked_add(request.weights)
        .is_some_and(|usage| usage.fits(pressure.apply(global_limit)))
        && state.classes[request.class.index()]
            .checked_add(request.weights)
            .is_some_and(|usage| usage.fits(pressure.apply(limits.classes.get(request.class))))
        && (request.class != ResourceClass::LargeObject
            || state
                .large
                .checked_add(request.weights)
                .is_some_and(|usage| usage.fits(pressure.apply(limits.large_object))));
    weighted
        && identity_fits(
            &state.tenants,
            &request.identity.tenant,
            request.weights,
            pressure.apply(limits.tenant),
        )
        && identity_fits(
            &state.repositories,
            &(
                request.identity.tenant.clone(),
                request.identity.repository.clone(),
            ),
            request.weights,
            pressure.apply(limits.repository),
        )
        && identity_fits(
            &state.actors,
            &(
                request.identity.tenant.clone(),
                request.identity.actor.clone(),
            ),
            request.weights,
            pressure.apply(limits.actor),
        )
}

fn identity_fits<K: Eq + std::hash::Hash>(
    map: &HashMap<K, ResourceWeights>,
    key: &K,
    added: ResourceWeights,
    limit: ResourceWeights,
) -> bool {
    map.get(key)
        .copied()
        .unwrap_or_default()
        .checked_add(added)
        .is_some_and(|usage| usage.fits(limit))
}

fn fair_queue_order(state: &State) -> Result<Vec<usize>, ()> {
    let mut order = Vec::new();
    order.try_reserve(state.queue.len()).map_err(|_| ())?;
    for class_index in 0..ResourceClass::COUNT {
        let mut tenants: Vec<(TenantId, VecDeque<usize>)> = Vec::new();
        tenants.try_reserve(state.queue.len()).map_err(|_| ())?;
        for (index, queued) in state.queue.iter().enumerate() {
            if queued.request.class.index() != class_index {
                continue;
            }
            if let Some((_, requests)) = tenants
                .iter_mut()
                .find(|(tenant, _)| tenant == &queued.request.identity.tenant)
            {
                requests.try_reserve(1).map_err(|_| ())?;
                requests.push_back(index);
            } else {
                let mut requests = VecDeque::new();
                requests.try_reserve(1).map_err(|_| ())?;
                requests.push_back(index);
                tenants.push((queued.request.identity.tenant.clone(), requests));
            }
        }
        if tenants.is_empty() {
            continue;
        }
        let start = state.last_tenant[class_index]
            .as_ref()
            .and_then(|last| tenants.iter().position(|(tenant, _)| tenant == last))
            .map_or(0, |position| (position + 1) % tenants.len());
        loop {
            let mut added = false;
            for offset in 0..tenants.len() {
                let tenant_index = (start + offset) % tenants.len();
                if let Some(index) = tenants[tenant_index].1.pop_front() {
                    order.push(index);
                    added = true;
                }
            }
            if !added {
                break;
            }
        }
    }
    Ok(order)
}

fn charge(state: &mut State, request: &AdmissionRequest) -> bool {
    let Some(global) = state.global.checked_add(request.weights) else {
        return false;
    };
    let Some(class) = state.classes[request.class.index()].checked_add(request.weights) else {
        return false;
    };
    let large = if request.class == ResourceClass::LargeObject {
        let Some(large) = state.large.checked_add(request.weights) else {
            return false;
        };
        large
    } else {
        state.large
    };
    let tenant_key = request.identity.tenant.clone();
    let repository_key = (
        request.identity.tenant.clone(),
        request.identity.repository.clone(),
    );
    let actor_key = (
        request.identity.tenant.clone(),
        request.identity.actor.clone(),
    );
    let Some(tenant) = state
        .tenants
        .get(&tenant_key)
        .copied()
        .unwrap_or_default()
        .checked_add(request.weights)
    else {
        return false;
    };
    let Some(repository) = state
        .repositories
        .get(&repository_key)
        .copied()
        .unwrap_or_default()
        .checked_add(request.weights)
    else {
        return false;
    };
    let Some(actor) = state
        .actors
        .get(&actor_key)
        .copied()
        .unwrap_or_default()
        .checked_add(request.weights)
    else {
        return false;
    };
    if state.tenants.try_reserve(1).is_err()
        || state.repositories.try_reserve(1).is_err()
        || state.actors.try_reserve(1).is_err()
    {
        return false;
    }
    state.global = global;
    state.classes[request.class.index()] = class;
    state.large = large;
    state.tenants.insert(tenant_key, tenant);
    state.repositories.insert(repository_key, repository);
    state.actors.insert(actor_key, actor);
    true
}

fn release(state: &mut State, ticket: AdmissionTicket) -> bool {
    let Some(request) = state.active.get(&ticket).cloned() else {
        return false;
    };
    let Some(global) = state.global.checked_sub(request.weights) else {
        return false;
    };
    let Some(class) = state.classes[request.class.index()].checked_sub(request.weights) else {
        return false;
    };
    let large = if request.class == ResourceClass::LargeObject {
        let Some(large) = state.large.checked_sub(request.weights) else {
            return false;
        };
        large
    } else {
        state.large
    };
    let tenant_key = request.identity.tenant.clone();
    let repository_key = (
        request.identity.tenant.clone(),
        request.identity.repository.clone(),
    );
    let actor_key = (request.identity.tenant, request.identity.actor);
    let Some(tenant) = state
        .tenants
        .get(&tenant_key)
        .and_then(|usage| usage.checked_sub(request.weights))
    else {
        return false;
    };
    let Some(repository) = state
        .repositories
        .get(&repository_key)
        .and_then(|usage| usage.checked_sub(request.weights))
    else {
        return false;
    };
    let Some(actor) = state
        .actors
        .get(&actor_key)
        .and_then(|usage| usage.checked_sub(request.weights))
    else {
        return false;
    };
    state.active.remove(&ticket);
    state.global = global;
    state.classes[request.class.index()] = class;
    state.large = large;
    set_or_remove(&mut state.tenants, tenant_key, tenant);
    set_or_remove(&mut state.repositories, repository_key, repository);
    set_or_remove(&mut state.actors, actor_key, actor);
    true
}

fn set_or_remove<K: Eq + std::hash::Hash>(
    map: &mut HashMap<K, ResourceWeights>,
    key: K,
    value: ResourceWeights,
) {
    if value == ResourceWeights::default() {
        map.remove(&key);
    } else {
        map.insert(key, value);
    }
}
