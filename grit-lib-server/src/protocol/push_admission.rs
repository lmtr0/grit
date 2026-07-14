//! Fair, bounded push admission and phase orchestration.
//!
//! This module composes the shared weighted [`AdmissionController`] with push-specific queue,
//! backend-concurrency, and lifecycle accounting. It performs no I/O, sleeping, clock reads, or
//! random generation; handlers supply observations and execute granted phases.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, Mutex};

use time::{Duration, OffsetDateTime};

use crate::admission::{
    AdmissionConfigError, AdmissionController, AdmissionDecision, AdmissionIdentity,
    AdmissionPermit, AdmissionRejectReason, AdmissionRequest, AdmissionTicket, BackendPressure,
    ResourceClass, ResourceWeights, RetryGuidance,
};
use crate::ids::{RepositoryId, TenantId};
use crate::protocol::push_metrics::ReceivePackLimits;

/// Stable scheduler-local push ticket.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct PushAdmissionTicket(u64);

impl PushAdmissionTicket {
    /// Return the opaque scheduler sequence value.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

/// Conservative prediction of one push's complete bounded resource demand.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PushAdmissionCost {
    /// Incoming compressed PACK bytes.
    pub compressed_bytes: u64,
    /// Aggregate inflated object/instruction bytes.
    pub inflated_bytes: u64,
    /// Peak retained delta-base bytes.
    pub base_bytes: u64,
    /// Peak structural/prepared metadata bytes.
    pub metadata_bytes: u64,
    /// Private quarantine bytes retained until terminal cleanup/publication.
    pub quarantine_bytes: u64,
    /// Deterministic validation CPU/work units.
    pub validation_work: u64,
    /// Expected SQL statements.
    pub sql_statements: u64,
    /// Expected SQL rows processed by bounded batches.
    pub sql_rows: u64,
    /// Expected external provider requests.
    pub external_requests: u64,
    /// Expected external bytes read or written.
    pub external_bytes: u64,
    /// Validation worker concurrency reserved while active.
    pub workers: usize,
    /// Backend range-request concurrency reserved while active.
    pub range_concurrency: usize,
    /// Multipart concurrency reserved while active.
    pub multipart_concurrency: usize,
    /// Deficit-round-robin service units.
    pub fair_service_units: u64,
}

impl PushAdmissionCost {
    fn weights(self) -> Result<ResourceWeights, PushAdmissionError> {
        let memory_bytes = self
            .inflated_bytes
            .checked_add(self.base_bytes)
            .and_then(|value| value.checked_add(self.metadata_bytes))
            .and_then(|value| value.checked_add(self.quarantine_bytes))
            .ok_or(PushAdmissionError::InvalidCost)?;
        let io_bytes = self
            .compressed_bytes
            .checked_add(self.external_bytes)
            .ok_or(PushAdmissionError::InvalidCost)?;
        let sql_units = self
            .sql_statements
            .checked_add(self.sql_rows)
            .ok_or(PushAdmissionError::InvalidCost)?;
        Ok(ResourceWeights {
            slots: 1,
            memory_bytes,
            io_bytes,
            cpu_units: self.validation_work,
            sql_units,
            s3_units: self.external_requests,
        })
    }

    fn validate(self, limits: ReceivePackLimits) -> Result<Self, PushAdmissionError> {
        if self.compressed_bytes > limits.max_pack_bytes
            || self.inflated_bytes > limits.max_inflated_bytes
            || self.base_bytes > limits.max_delta_base_memory_bytes
            || self.metadata_bytes > limits.max_metadata_memory_bytes
            || self.quarantine_bytes > limits.max_request_bytes
            || self.validation_work == 0
            || self.sql_statements > limits.max_sql_statements
            || usize::try_from(self.sql_rows).map_or(true, |rows| rows > limits.max_objects)
            || self.external_requests > limits.max_external_operations
            || self.external_bytes > limits.max_request_bytes
            || u64::try_from(self.range_concurrency)
                .map_or(true, |value| value > limits.max_range_operations)
            || self.workers == 0
            || self.workers > limits.worker_count
            || self.range_concurrency == 0
            || self.multipart_concurrency == 0
            || self.fair_service_units == 0
        {
            return Err(PushAdmissionError::InvalidCost);
        }
        self.weights()?;
        Ok(self)
    }
}

/// Complete pre-input push admission request with a trusted authenticated identity.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PushAdmissionRequest {
    /// Trusted tenant, repository, and authenticated actor identity.
    pub identity: AdmissionIdentity,
    /// Predicted bounded push resource demand.
    pub cost: PushAdmissionCost,
    /// Retained scheduler record and pre-input metadata bytes.
    pub queued_bytes: u64,
    /// Explicit submission time.
    pub submitted_at: OffsetDateTime,
    /// Explicit end-to-end request deadline.
    pub deadline: OffsetDateTime,
    /// Predicted admitted service duration.
    pub service_estimate: Duration,
    /// Receive-pack safety limits already selected for this trusted scope.
    pub receive_limits: ReceivePackLimits,
}

impl PushAdmissionRequest {
    fn validate(&self) -> Result<(), PushAdmissionError> {
        let limits = self
            .receive_limits
            .validate()
            .map_err(|_| PushAdmissionError::InvalidRequest)?;
        self.cost.validate(limits)?;
        let identity_bytes = self
            .identity
            .tenant
            .as_str()
            .len()
            .checked_add(self.identity.repository.as_str().len())
            .and_then(|value| value.checked_add(self.identity.actor.len()))
            .and_then(|value| value.checked_add(std::mem::size_of::<QueuedPush>()))
            .and_then(|value| u64::try_from(value).ok())
            .ok_or(PushAdmissionError::InvalidRequest)?;
        let max_duration = time::Duration::try_from(limits.max_duration)
            .map_err(|_| PushAdmissionError::InvalidRequest)?;
        if self.identity.actor.trim().is_empty()
            || self.identity.actor.len() > 512
            || self.queued_bytes < identity_bytes
            || self.service_estimate <= Duration::ZERO
            || self.service_estimate > max_duration
            || self.deadline <= self.submitted_at
            || self.deadline - self.submitted_at > max_duration
            || self
                .submitted_at
                .checked_add(self.service_estimate)
                .is_none_or(|completion| completion > self.deadline)
        {
            return Err(PushAdmissionError::InvalidRequest);
        }
        Ok(())
    }

    fn shared_request(&self) -> Result<AdmissionRequest, PushAdmissionError> {
        Ok(AdmissionRequest {
            class: ResourceClass::Push,
            identity: self.identity.clone(),
            weights: self.cost.weights()?,
            submitted_at: self.submitted_at,
            deadline: self.deadline,
            service_estimate: self.service_estimate,
        })
    }
}

/// Push queue fairness, quota, and backend concurrency limits.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PushQueueLimits {
    /// Global queued requests.
    pub max_queued: usize,
    /// Global retained queue bytes.
    pub max_queued_bytes: u64,
    /// Per-tenant queued requests.
    pub max_tenant_queued: usize,
    /// Per-tenant retained queue bytes.
    pub max_tenant_queued_bytes: u64,
    /// Per-repository queued requests.
    pub max_repository_queued: usize,
    /// Per-repository retained queue bytes.
    pub max_repository_queued_bytes: u64,
    /// Per-actor queued requests.
    pub max_actor_queued: usize,
    /// Per-actor retained queue bytes.
    pub max_actor_queued_bytes: u64,
    /// DRR credit added once per caller-controlled logical round.
    pub fair_quantum: u64,
    /// Maximum accumulated DRR credit.
    pub max_deficit: u64,
    /// Explicit waiting interval for one aging credit.
    pub aging_interval: Duration,
    /// Service units removed per aging interval.
    pub aging_credit_units: u64,
    /// Requests at or below this service cost receive small-push preference.
    pub small_push_units: u64,
    /// Maximum consecutive small grants while an eligible large push waits.
    pub max_small_bypass: u32,
    /// Push-only active worker capacity.
    pub max_workers: usize,
    /// Push-only active range-request capacity.
    pub max_range_concurrency: usize,
    /// Push-only active multipart capacity.
    pub max_multipart_concurrency: usize,
    /// Push-only active quarantine bytes.
    pub max_quarantine_bytes: u64,
}

impl PushQueueLimits {
    /// Validate queue, fairness, and independent backend resource limits.
    ///
    /// # Errors
    ///
    /// Returns [`PushAdmissionError::InvalidQueueLimits`] for inconsistent limits.
    pub fn validate(self) -> Result<Self, PushAdmissionError> {
        if self.max_queued == 0
            || self.max_queued_bytes == 0
            || self.max_tenant_queued == 0
            || self.max_tenant_queued > self.max_queued
            || self.max_tenant_queued_bytes == 0
            || self.max_tenant_queued_bytes > self.max_queued_bytes
            || self.max_repository_queued == 0
            || self.max_repository_queued > self.max_tenant_queued
            || self.max_repository_queued_bytes == 0
            || self.max_repository_queued_bytes > self.max_tenant_queued_bytes
            || self.max_actor_queued == 0
            || self.max_actor_queued >= self.max_tenant_queued
            || self.max_actor_queued >= self.max_repository_queued
            || self.max_actor_queued_bytes == 0
            || self.max_actor_queued_bytes >= self.max_tenant_queued_bytes
            || self.max_actor_queued_bytes >= self.max_repository_queued_bytes
            || self.fair_quantum == 0
            || self.max_deficit < self.fair_quantum
            || self.aging_interval <= Duration::ZERO
            || self.small_push_units == 0
            || self.small_push_units > self.max_deficit
            || self.max_small_bypass == 0
            || self.max_workers == 0
            || self.max_range_concurrency == 0
            || self.max_multipart_concurrency == 0
            || self.max_quarantine_bytes == 0
        {
            return Err(PushAdmissionError::InvalidQueueLimits);
        }
        Ok(self)
    }
}

/// Caller-controlled deterministic queue scan allowance.
pub trait PushQueueWorkBudget {
    /// Charge queue/fairness scan units, returning `false` when exhausted.
    fn charge(&mut self, units: u64) -> bool;
}

/// Counter-backed implementation of [`PushQueueWorkBudget`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PushQueueWorkLimit {
    remaining: u64,
}

impl PushQueueWorkLimit {
    /// Construct an exact queue scan allowance.
    #[must_use]
    pub const fn new(units: u64) -> Self {
        Self { remaining: units }
    }

    /// Return remaining scan units.
    #[must_use]
    pub const fn remaining(&self) -> u64 {
        self.remaining
    }
}

impl PushQueueWorkBudget for PushQueueWorkLimit {
    fn charge(&mut self, units: u64) -> bool {
        let Some(remaining) = self.remaining.checked_sub(units) else {
            return false;
        };
        self.remaining = remaining;
        true
    }
}

/// Stable, non-sensitive load-shedding category.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PushAdmissionRejectReason {
    /// Queue count or retained-byte scope quota is full.
    QueueFull,
    /// Shared or independent backend capacity is saturated.
    LoadShed,
    /// The request cannot complete before its explicit deadline.
    Deadline,
    /// Request can never fit configured capacity.
    TooLarge,
    /// Bounded allocation failed.
    Allocation,
}

/// Retryable rejection without tenant, repository, actor, or provider labels.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PushAdmissionRejection {
    /// Stable rejection class.
    pub reason: PushAdmissionRejectReason,
    /// Explicit shared-controller retry guidance.
    pub retry: RetryGuidance,
}

/// Typed scheduler/configuration failure.
#[derive(Clone, Debug, thiserror::Error, PartialEq, Eq)]
pub enum PushAdmissionError {
    /// Request identity, timing, or retained shape is invalid.
    #[error("invalid push admission request")]
    InvalidRequest,
    /// Predicted cost is zero, overflowing, or exceeds receive-pack limits.
    #[error("invalid push admission cost")]
    InvalidCost,
    /// Queue/fairness/backend limits are inconsistent.
    #[error("invalid push queue limits")]
    InvalidQueueLimits,
    /// Queue scope quota is full.
    #[error("bounded push queue is full")]
    QueueFull,
    /// Bounded scheduler allocation failed.
    #[error("push scheduler allocation failed")]
    Allocation,
    /// Ticket sequence exhausted.
    #[error("push admission ticket sequence exhausted")]
    TicketOverflow,
    /// Shared weighted admission rejected submission.
    #[error("shared push admission failed")]
    Admission(AdmissionConfigError),
    /// Deterministic queue scan work was exhausted.
    #[error("push queue scan work exhausted")]
    WorkBudget,
    /// Explicit cancellation won a pre-publication race.
    #[error("push admission cancelled")]
    Cancelled,
    /// Caller-observed time or logical round moved backwards.
    #[error("push scheduler observation is non-monotonic")]
    NonMonotonicObservation,
    /// Execution phase transition is invalid.
    #[error("invalid push orchestration phase transition")]
    InvalidPhase,
    /// Execution deadline elapsed before durable publication.
    #[error("push execution deadline elapsed")]
    Deadline,
}

/// One scheduler poll result.
#[derive(Debug)]
pub enum PushScheduleDecision {
    /// Nothing can be granted during this logical round.
    Waiting {
        /// Current bounded queue count.
        queued: usize,
    },
    /// Request owns all weighted and push-specific resources.
    Granted {
        /// Scheduler-local ticket.
        ticket: PushAdmissionTicket,
        /// Original validated request.
        request: PushAdmissionRequest,
        /// RAII execution/orchestration permit.
        permit: PushExecutionPermit,
    },
    /// Request was removed with stable retry guidance.
    Rejected {
        /// Scheduler-local ticket.
        ticket: PushAdmissionTicket,
        /// Non-sensitive rejection.
        rejection: PushAdmissionRejection,
    },
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct Usage {
    count: usize,
    bytes: u64,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct BackendUsage {
    workers: usize,
    ranges: usize,
    multipart: usize,
    quarantine_bytes: u64,
}

impl BackendUsage {
    fn checked_add(self, cost: PushAdmissionCost) -> Option<Self> {
        Some(Self {
            workers: self.workers.checked_add(cost.workers)?,
            ranges: self.ranges.checked_add(cost.range_concurrency)?,
            multipart: self.multipart.checked_add(cost.multipart_concurrency)?,
            quarantine_bytes: self.quarantine_bytes.checked_add(cost.quarantine_bytes)?,
        })
    }

    fn checked_sub(self, cost: PushAdmissionCost) -> Option<Self> {
        Some(Self {
            workers: self.workers.checked_sub(cost.workers)?,
            ranges: self.ranges.checked_sub(cost.range_concurrency)?,
            multipart: self.multipart.checked_sub(cost.multipart_concurrency)?,
            quarantine_bytes: self.quarantine_bytes.checked_sub(cost.quarantine_bytes)?,
        })
    }

    fn fits(self, limits: PushQueueLimits) -> bool {
        self.workers <= limits.max_workers
            && self.ranges <= limits.max_range_concurrency
            && self.multipart <= limits.max_multipart_concurrency
            && self.quarantine_bytes <= limits.max_quarantine_bytes
    }
}

#[derive(Clone, Debug)]
struct QueuedPush {
    ticket: PushAdmissionTicket,
    repository_key: (TenantId, RepositoryId),
    actor_key: (TenantId, String),
    request: PushAdmissionRequest,
}

#[derive(Debug)]
struct PushQueueState {
    next_ticket: u64,
    queue: VecDeque<QueuedPush>,
    queued_bytes: u64,
    tenants: HashMap<TenantId, Usage>,
    repositories: HashMap<(TenantId, RepositoryId), Usage>,
    actors: HashMap<(TenantId, String), Usage>,
    tenant_deficit: HashMap<TenantId, u64>,
    repository_deficit: HashMap<(TenantId, RepositoryId), u64>,
    actor_deficit: HashMap<(TenantId, String), u64>,
    dispatching: HashSet<PushAdmissionTicket>,
    cursor: usize,
    last_observed_at: Option<OffsetDateTime>,
    last_round: u64,
    consecutive_small: u32,
    backend: BackendUsage,
    grants: u64,
    rejections: u64,
    cancellations: u64,
    completed: u64,
    published: u64,
    rejected_terminal: u64,
    backend_failures: u64,
    aborted: u64,
    active: usize,
}

impl Default for PushQueueState {
    fn default() -> Self {
        Self {
            next_ticket: 1,
            queue: VecDeque::new(),
            queued_bytes: 0,
            tenants: HashMap::new(),
            repositories: HashMap::new(),
            actors: HashMap::new(),
            tenant_deficit: HashMap::new(),
            repository_deficit: HashMap::new(),
            actor_deficit: HashMap::new(),
            dispatching: HashSet::new(),
            cursor: 0,
            last_observed_at: None,
            last_round: 0,
            consecutive_small: 0,
            backend: BackendUsage::default(),
            grants: 0,
            rejections: 0,
            cancellations: 0,
            completed: 0,
            published: 0,
            rejected_terminal: 0,
            backend_failures: 0,
            aborted: 0,
            active: 0,
        }
    }
}

/// Fair push queue layered over shared weighted admission.
#[derive(Clone, Debug)]
pub struct PushAdmissionScheduler {
    admission: AdmissionController,
    limits: PushQueueLimits,
    state: Arc<Mutex<PushQueueState>>,
}

impl PushAdmissionScheduler {
    /// Construct a push scheduler over the shared controller.
    ///
    /// # Errors
    ///
    /// Returns invalid queue/fairness/backend configuration.
    pub fn new(
        admission: AdmissionController,
        limits: PushQueueLimits,
    ) -> Result<Self, PushAdmissionError> {
        Ok(Self {
            admission,
            limits: limits.validate()?,
            state: Arc::new(Mutex::new(PushQueueState::default())),
        })
    }

    /// Validate and enqueue one push before reading input or creating quarantine bytes.
    ///
    /// # Errors
    ///
    /// Returns request/cost, scoped quota, allocation, or ticket failures.
    pub fn enqueue(
        &self,
        request: PushAdmissionRequest,
    ) -> Result<PushAdmissionTicket, PushAdmissionError> {
        request.validate()?;
        if request.cost.fair_service_units > self.limits.max_deficit
            || !BackendUsage::default()
                .checked_add(request.cost)
                .is_some_and(|usage| usage.fits(self.limits))
        {
            return Err(PushAdmissionError::InvalidCost);
        }
        let tenant_key = request.identity.tenant.clone();
        let repository_key = (
            request.identity.tenant.clone(),
            request.identity.repository.clone(),
        );
        let actor_key = (
            request.identity.tenant.clone(),
            request.identity.actor.clone(),
        );
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        let tenant = state.tenants.get(&tenant_key).copied().unwrap_or_default();
        let repository = state
            .repositories
            .get(&repository_key)
            .copied()
            .unwrap_or_default();
        let actor = state.actors.get(&actor_key).copied().unwrap_or_default();
        if state.queue.len() >= self.limits.max_queued
            || !usage_fits(
                tenant,
                request.queued_bytes,
                self.limits.max_tenant_queued,
                self.limits.max_tenant_queued_bytes,
            )
            || !usage_fits(
                repository,
                request.queued_bytes,
                self.limits.max_repository_queued,
                self.limits.max_repository_queued_bytes,
            )
            || !usage_fits(
                actor,
                request.queued_bytes,
                self.limits.max_actor_queued,
                self.limits.max_actor_queued_bytes,
            )
        {
            state.rejections = state.rejections.saturating_add(1);
            return Err(PushAdmissionError::QueueFull);
        }
        let Some(next_bytes) = state
            .queued_bytes
            .checked_add(request.queued_bytes)
            .filter(|bytes| *bytes <= self.limits.max_queued_bytes)
        else {
            state.rejections = state.rejections.saturating_add(1);
            return Err(PushAdmissionError::QueueFull);
        };
        let next_tenant = add_usage(tenant, request.queued_bytes)?;
        let next_repository = add_usage(repository, request.queued_bytes)?;
        let next_actor = add_usage(actor, request.queued_bytes)?;
        let next_ticket = state
            .next_ticket
            .checked_add(1)
            .ok_or(PushAdmissionError::TicketOverflow)?;
        state
            .queue
            .try_reserve(1)
            .map_err(|_| PushAdmissionError::Allocation)?;
        reserve_scope_maps(&mut state, &tenant_key, &repository_key, &actor_key)?;
        let ticket = PushAdmissionTicket(state.next_ticket);
        state.next_ticket = next_ticket;
        state.queued_bytes = next_bytes;
        state.tenants.insert(tenant_key, next_tenant);
        state
            .repositories
            .insert(repository_key.clone(), next_repository);
        state.actors.insert(actor_key.clone(), next_actor);
        state.queue.push_back(QueuedPush {
            ticket,
            repository_key,
            actor_key,
            request,
        });
        Ok(ticket)
    }

    /// Cancel queued work. A concurrent pending dispatch observes removal and unwinds its ticket.
    #[must_use]
    pub fn cancel(&self, ticket: PushAdmissionTicket) -> bool {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        let Some(index) = state
            .queue
            .iter()
            .position(|queued| queued.ticket == ticket)
        else {
            return false;
        };
        let removed = remove_queued(&mut state, index).is_some();
        if removed {
            state.cancellations = state.cancellations.saturating_add(1);
        }
        removed
    }

    /// Run one caller-controlled logical DRR/aging round.
    ///
    /// No scheduler lock is held while submitting to or polling the shared admission controller.
    ///
    /// # Errors
    ///
    /// Returns monotonicity, cancellation, scan-budget, allocation, or shared admission failures.
    pub fn poll_next<B: PushQueueWorkBudget>(
        &self,
        logical_round: u64,
        observed_at: OffsetDateTime,
        pressure: BackendPressure,
        budget: &mut B,
        cancelled: bool,
    ) -> Result<PushScheduleDecision, PushAdmissionError> {
        if cancelled {
            return Err(PushAdmissionError::Cancelled);
        }
        if logical_round == 0 {
            return Err(PushAdmissionError::NonMonotonicObservation);
        }
        pressure.validate().map_err(PushAdmissionError::Admission)?;
        let selected = {
            let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
            if state
                .last_observed_at
                .is_some_and(|prior| observed_at < prior)
                || logical_round < state.last_round
            {
                return Err(PushAdmissionError::NonMonotonicObservation);
            }
            state.last_observed_at = Some(observed_at);
            if let Some(ticket) = remove_expired(&mut state, observed_at, budget)? {
                state.rejections = state.rejections.saturating_add(1);
                return Ok(PushScheduleDecision::Rejected {
                    ticket,
                    rejection: deadline_rejection(),
                });
            }
            if state.queue.is_empty() {
                return Ok(PushScheduleDecision::Waiting { queued: 0 });
            }
            if logical_round > state.last_round {
                add_deficits(&mut state, self.limits, budget)?;
                state.last_round = logical_round;
            }
            choose_candidate(&mut state, self.limits, observed_at, budget)?
        };
        let Some((index, queued, cost)) = selected else {
            return Ok(PushScheduleDecision::Waiting {
                queued: self.snapshot().queued,
            });
        };
        let mut dispatch = PendingPushDispatch::new(self, queued, index, cost);
        let (ticket, request) = dispatch
            .queued()
            .map(|queued| (queued.ticket, queued.request.shared_request()))
            .ok_or(PushAdmissionError::Cancelled)?;
        let admission_ticket = self
            .admission
            .submit(request?)
            .map_err(PushAdmissionError::Admission)?;
        dispatch.arm(admission_ticket);
        match self.admission.poll(admission_ticket, observed_at, pressure) {
            AdmissionDecision::Granted(admission_permit) => {
                dispatch.disarm();
                match dispatch.finish_granted() {
                    GrantResult::Accepted {
                        backend,
                        observed_at: granted_at,
                    } => {
                        let request = dispatch
                            .take_queued()
                            .map(|queued| queued.request)
                            .ok_or(PushAdmissionError::Cancelled)?;
                        let deadline = request.deadline;
                        Ok(PushScheduleDecision::Granted {
                            ticket,
                            request,
                            permit: PushExecutionPermit::new(
                                admission_permit,
                                backend,
                                Arc::clone(&self.state),
                                deadline,
                                granted_at,
                            ),
                        })
                    }
                    GrantResult::Waiting => {
                        drop(admission_permit);
                        Ok(PushScheduleDecision::Waiting {
                            queued: self.snapshot().queued,
                        })
                    }
                    GrantResult::Deadline => {
                        drop(admission_permit);
                        Ok(PushScheduleDecision::Rejected {
                            ticket,
                            rejection: deadline_rejection(),
                        })
                    }
                    GrantResult::Cancelled => {
                        drop(admission_permit);
                        Err(PushAdmissionError::Cancelled)
                    }
                }
            }
            AdmissionDecision::Queued { .. } => {
                if self.admission.cancel(admission_ticket) {
                    dispatch.disarm();
                }
                if !dispatch.finish(false, true) {
                    return Err(PushAdmissionError::Cancelled);
                }
                Ok(PushScheduleDecision::Waiting {
                    queued: self.snapshot().queued,
                })
            }
            AdmissionDecision::Rejected { reason, retry } => {
                dispatch.disarm();
                if !dispatch.finish(true, true) {
                    return Err(PushAdmissionError::Cancelled);
                }
                Ok(PushScheduleDecision::Rejected {
                    ticket,
                    rejection: map_rejection(reason, retry),
                })
            }
        }
    }

    /// Return class/count-only scheduler metrics without identity labels.
    #[must_use]
    pub fn snapshot(&self) -> PushAdmissionSnapshot {
        let state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        PushAdmissionSnapshot {
            queued: state.queue.len(),
            queued_bytes: state.queued_bytes,
            active: state.active,
            active_workers: state.backend.workers,
            active_ranges: state.backend.ranges,
            active_multipart: state.backend.multipart,
            active_quarantine_bytes: state.backend.quarantine_bytes,
            grants: state.grants,
            rejections: state.rejections,
            cancellations: state.cancellations,
            completed: state.completed,
            published: state.published,
            rejected_terminal: state.rejected_terminal,
            backend_failures: state.backend_failures,
            aborted: state.aborted,
        }
    }

    fn finish_dispatch(
        &self,
        queued: &QueuedPush,
        index: usize,
        cost: u64,
        remove: bool,
        refund: bool,
    ) -> bool {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        if !state.dispatching.remove(&queued.ticket) {
            return false;
        }
        let Some(index) = locate(&state, queued.ticket, index) else {
            refund_deficits(&mut state, queued, cost, self.limits.max_deficit);
            return false;
        };
        if remove {
            if remove_queued(&mut state, index).is_none() {
                refund_deficits(&mut state, queued, cost, self.limits.max_deficit);
                return false;
            }
            if refund {
                refund_deficits(&mut state, queued, cost, self.limits.max_deficit);
            }
            state.rejections = state.rejections.saturating_add(1);
        } else if refund {
            refund_deficits(&mut state, queued, cost, self.limits.max_deficit);
        }
        state.cursor = if state.queue.is_empty() {
            0
        } else if remove {
            index % state.queue.len()
        } else {
            (index + 1) % state.queue.len()
        };
        true
    }

    fn finish_granted(&self, queued: &QueuedPush, index: usize, cost: u64) -> GrantResult {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        if !state.dispatching.remove(&queued.ticket) {
            return GrantResult::Cancelled;
        }
        let Some(index) = locate(&state, queued.ticket, index) else {
            refund_deficits(&mut state, queued, cost, self.limits.max_deficit);
            return GrantResult::Cancelled;
        };
        let observed_at = state
            .last_observed_at
            .map_or(queued.request.submitted_at, |value| value);
        if observed_at >= queued.request.deadline
            || observed_at
                .checked_add(queued.request.service_estimate)
                .is_none_or(|end| end > queued.request.deadline)
        {
            if remove_queued(&mut state, index).is_none() {
                refund_deficits(&mut state, queued, cost, self.limits.max_deficit);
                return GrantResult::Cancelled;
            }
            refund_deficits(&mut state, queued, cost, self.limits.max_deficit);
            state.rejections = state.rejections.saturating_add(1);
            return GrantResult::Deadline;
        }
        let Some(backend) = state.backend.checked_add(queued.request.cost) else {
            refund_deficits(&mut state, queued, cost, self.limits.max_deficit);
            return GrantResult::Waiting;
        };
        if !backend.fits(self.limits) {
            refund_deficits(&mut state, queued, cost, self.limits.max_deficit);
            return GrantResult::Waiting;
        }
        let Some(active) = state.active.checked_add(1) else {
            refund_deficits(&mut state, queued, cost, self.limits.max_deficit);
            return GrantResult::Waiting;
        };
        if remove_queued(&mut state, index).is_none() {
            refund_deficits(&mut state, queued, cost, self.limits.max_deficit);
            return GrantResult::Cancelled;
        }
        state.backend = backend;
        state.active = active;
        state.grants = state.grants.saturating_add(1);
        if queued.request.cost.fair_service_units <= self.limits.small_push_units {
            state.consecutive_small = state.consecutive_small.saturating_add(1);
        } else {
            state.consecutive_small = 0;
        }
        GrantResult::Accepted {
            backend: PushBackendPermit {
                state: Arc::clone(&self.state),
                cost: Some(queued.request.cost),
            },
            observed_at,
        }
    }
}

/// Class/count-only scheduler metrics snapshot.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PushAdmissionSnapshot {
    /// Queued push count.
    pub queued: usize,
    /// Retained queue bytes.
    pub queued_bytes: u64,
    /// Active push count approximation from granted worker reservations.
    pub active: usize,
    /// Active validation workers.
    pub active_workers: usize,
    /// Active range concurrency.
    pub active_ranges: usize,
    /// Active multipart concurrency.
    pub active_multipart: usize,
    /// Active quarantine bytes.
    pub active_quarantine_bytes: u64,
    /// Grants since construction.
    pub grants: u64,
    /// Rejections since construction.
    pub rejections: u64,
    /// Cancellations since construction.
    pub cancellations: u64,
    /// Explicit successful terminal outcomes.
    pub completed: u64,
    /// Successful durable publication outcomes.
    pub published: u64,
    /// Explicit pre-publication rejections.
    pub rejected_terminal: u64,
    /// Explicit pre-publication backend failures.
    pub backend_failures: u64,
    /// Dropped/aborted terminal outcomes.
    pub aborted: u64,
}

#[derive(Debug)]
struct PushBackendPermit {
    state: Arc<Mutex<PushQueueState>>,
    cost: Option<PushAdmissionCost>,
}

impl Drop for PushBackendPermit {
    fn drop(&mut self) {
        let Some(cost) = self.cost.take() else {
            return;
        };
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        if let (Some(usage), Some(active)) =
            (state.backend.checked_sub(cost), state.active.checked_sub(1))
        {
            state.backend = usage;
            state.active = active;
        }
    }
}

/// Ordered push orchestration phase.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum PushExecutionPhase {
    /// Resources granted; no request input consumed.
    Admitted,
    /// Input streaming is about to begin.
    Input,
    /// Quarantine creation/write is about to begin.
    Quarantine,
    /// Pack validation is about to begin.
    Validation,
    /// Pack promotion is about to begin.
    Promotion,
    /// Atomic publication is about to begin.
    Publication,
    /// Durable publication committed; cancellation no longer changes the result.
    Published,
    /// Wire output/status emission has started.
    OutputStarted,
}

/// Exact terminal push outcome category.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PushExecutionOutcome {
    /// Durable publication and terminal status succeeded.
    Published,
    /// Push was rejected before publication.
    Rejected,
    /// Backend failed before publication.
    BackendFailure,
}

/// Combined RAII permit for shared admission, backend resources, and terminal accounting.
#[derive(Debug)]
pub struct PushExecutionPermit {
    admission: Option<AdmissionPermit>,
    backend: Option<PushBackendPermit>,
    metrics: Arc<Mutex<PushQueueState>>,
    phase: PushExecutionPhase,
    deadline: OffsetDateTime,
    last_observed_at: OffsetDateTime,
    publication_recorded: bool,
    terminal: bool,
}

impl PushExecutionPermit {
    fn new(
        admission: AdmissionPermit,
        backend: PushBackendPermit,
        metrics: Arc<Mutex<PushQueueState>>,
        deadline: OffsetDateTime,
        observed_at: OffsetDateTime,
    ) -> Self {
        Self {
            admission: Some(admission),
            backend: Some(backend),
            metrics,
            phase: PushExecutionPhase::Admitted,
            deadline,
            last_observed_at: observed_at,
            publication_recorded: false,
            terminal: false,
        }
    }

    /// Return the most recently entered phase.
    #[must_use]
    pub const fn phase(&self) -> PushExecutionPhase {
        self.phase
    }

    /// Enter the next phase after checking explicit time and cancellation.
    ///
    /// Cancellation before [`PushExecutionPhase::Published`] aborts unambiguously. At or after
    /// publication it is ignored so the handler must finish the already-durable status response.
    ///
    /// # Errors
    ///
    /// Returns invalid transition, non-monotonic observation, pre-publication cancellation, or
    /// pre-publication deadline.
    pub fn checkpoint(
        &mut self,
        next: PushExecutionPhase,
        observed_at: OffsetDateTime,
        deadline: OffsetDateTime,
        cancelled: bool,
    ) -> Result<(), PushAdmissionError> {
        if self.terminal || next <= self.phase || !valid_phase_transition(self.phase, next) {
            return Err(PushAdmissionError::InvalidPhase);
        }
        if next < PushExecutionPhase::Published {
            if deadline != self.deadline || observed_at < self.last_observed_at {
                return Err(PushAdmissionError::NonMonotonicObservation);
            }
            if cancelled {
                return Err(PushAdmissionError::Cancelled);
            }
            if observed_at >= self.deadline {
                return Err(PushAdmissionError::Deadline);
            }
            self.last_observed_at = observed_at;
        }
        if next == PushExecutionPhase::Published && !self.publication_recorded {
            let mut state = self
                .metrics
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            state.published = state.published.saturating_add(1);
            self.publication_recorded = true;
        }
        self.phase = next;
        Ok(())
    }

    /// Record exactly one terminal outcome and release every resource reservation.
    pub fn finish(mut self, outcome: PushExecutionOutcome) -> Result<(), PushAdmissionError> {
        let valid = match outcome {
            PushExecutionOutcome::Published => {
                self.phase >= PushExecutionPhase::Published && self.publication_recorded
            }
            PushExecutionOutcome::Rejected | PushExecutionOutcome::BackendFailure => {
                self.phase < PushExecutionPhase::Published
            }
        };
        if !valid {
            return Err(PushAdmissionError::InvalidPhase);
        }
        self.terminal = true;
        let mut state = self
            .metrics
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        state.completed = state.completed.saturating_add(1);
        match outcome {
            PushExecutionOutcome::Published => {}
            PushExecutionOutcome::Rejected => {
                state.rejected_terminal = state.rejected_terminal.saturating_add(1);
            }
            PushExecutionOutcome::BackendFailure => {
                state.backend_failures = state.backend_failures.saturating_add(1);
            }
        }
        drop(state);
        let _ = self.backend.take();
        let _ = self.admission.take();
        Ok(())
    }
}

impl Drop for PushExecutionPermit {
    fn drop(&mut self) {
        if !self.terminal {
            let mut state = self
                .metrics
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            if self.publication_recorded {
                state.completed = state.completed.saturating_add(1);
            }
            state.aborted = state.aborted.saturating_add(1);
            drop(state);
        }
        let _ = self.backend.take();
        let _ = self.admission.take();
    }
}

#[derive(Debug)]
enum GrantResult {
    Accepted {
        backend: PushBackendPermit,
        observed_at: OffsetDateTime,
    },
    Waiting,
    Deadline,
    Cancelled,
}

struct PendingPushDispatch<'a> {
    scheduler: &'a PushAdmissionScheduler,
    queued: Option<QueuedPush>,
    index: usize,
    cost: u64,
    admission: Option<AdmissionTicket>,
    finished: bool,
}

impl<'a> PendingPushDispatch<'a> {
    fn new(
        scheduler: &'a PushAdmissionScheduler,
        queued: QueuedPush,
        index: usize,
        cost: u64,
    ) -> Self {
        Self {
            scheduler,
            queued: Some(queued),
            index,
            cost,
            admission: None,
            finished: false,
        }
    }
    fn queued(&self) -> Option<&QueuedPush> {
        self.queued.as_ref()
    }
    fn take_queued(&mut self) -> Option<QueuedPush> {
        self.queued.take()
    }
    fn arm(&mut self, ticket: AdmissionTicket) {
        self.admission = Some(ticket);
    }
    fn disarm(&mut self) {
        self.admission = None;
    }
    fn finish(&mut self, remove: bool, refund: bool) -> bool {
        if self.finished {
            return false;
        }
        let Some(queued) = self.queued.as_ref() else {
            return false;
        };
        self.finished = true;
        self.scheduler
            .finish_dispatch(queued, self.index, self.cost, remove, refund)
    }
    fn finish_granted(&mut self) -> GrantResult {
        if self.finished {
            return GrantResult::Cancelled;
        }
        let Some(queued) = self.queued.as_ref() else {
            return GrantResult::Cancelled;
        };
        self.finished = true;
        self.scheduler.finish_granted(queued, self.index, self.cost)
    }
}

impl Drop for PendingPushDispatch<'_> {
    fn drop(&mut self) {
        if let Some(ticket) = self.admission.take() {
            let _ = self.scheduler.admission.cancel(ticket);
        }
        if !self.finished {
            if let Some(queued) = self.queued.as_ref() {
                let _ = self
                    .scheduler
                    .finish_dispatch(queued, self.index, self.cost, false, true);
            }
        }
    }
}

fn choose_candidate<B: PushQueueWorkBudget>(
    state: &mut PushQueueState,
    limits: PushQueueLimits,
    observed_at: OffsetDateTime,
    budget: &mut B,
) -> Result<Option<(usize, QueuedPush, u64)>, PushAdmissionError> {
    let len = state.queue.len();
    let scan_units = u64::try_from(len).map_err(|_| PushAdmissionError::WorkBudget)?;
    if !budget.charge(scan_units) {
        return Err(PushAdmissionError::WorkBudget);
    }
    let eligible_large = state.queue.iter().any(|queued| {
        !state.dispatching.contains(&queued.ticket)
            && observed_at >= queued.request.submitted_at
            && can_complete(queued, observed_at)
            && backend_can_admit(state, queued, limits)
            && queued.request.cost.fair_service_units > limits.small_push_units
            && has_deficit(
                state,
                queued,
                effective_cost(&queued.request, observed_at, limits),
            )
    });
    if !eligible_large {
        state.consecutive_small = 0;
    }
    let force_large = state.consecutive_small >= limits.max_small_bypass && eligible_large;
    for offset in 0..len {
        if !budget.charge(1) {
            return Err(PushAdmissionError::WorkBudget);
        }
        let index = (state.cursor + offset) % len;
        let queued = &state.queue[index];
        if state.dispatching.contains(&queued.ticket) || observed_at < queued.request.submitted_at {
            continue;
        }
        if !can_complete(queued, observed_at) || !backend_can_admit(state, queued, limits) {
            continue;
        }
        let is_small = queued.request.cost.fair_service_units <= limits.small_push_units;
        if force_large && is_small {
            continue;
        }
        let cost = effective_cost(&queued.request, observed_at, limits);
        if has_deficit(state, queued, cost) {
            let queued = queued.clone();
            state
                .dispatching
                .try_reserve(1)
                .map_err(|_| PushAdmissionError::Allocation)?;
            state.dispatching.insert(queued.ticket);
            debit(
                &mut state.tenant_deficit,
                &queued.request.identity.tenant,
                cost,
            );
            debit(&mut state.repository_deficit, &queued.repository_key, cost);
            debit(&mut state.actor_deficit, &queued.actor_key, cost);
            return Ok(Some((index, queued, cost)));
        }
    }
    Ok(None)
}

fn can_complete(queued: &QueuedPush, observed_at: OffsetDateTime) -> bool {
    observed_at < queued.request.deadline
        && observed_at
            .checked_add(queued.request.service_estimate)
            .is_some_and(|end| end <= queued.request.deadline)
}

fn backend_can_admit(state: &PushQueueState, queued: &QueuedPush, limits: PushQueueLimits) -> bool {
    state
        .backend
        .checked_add(queued.request.cost)
        .is_some_and(|usage| usage.fits(limits))
}

fn has_deficit(state: &PushQueueState, queued: &QueuedPush, cost: u64) -> bool {
    cost <= state
        .tenant_deficit
        .get(&queued.request.identity.tenant)
        .copied()
        .unwrap_or_default()
        && cost
            <= state
                .repository_deficit
                .get(&queued.repository_key)
                .copied()
                .unwrap_or_default()
        && cost
            <= state
                .actor_deficit
                .get(&queued.actor_key)
                .copied()
                .unwrap_or_default()
}

fn add_deficits<B: PushQueueWorkBudget>(
    state: &mut PushQueueState,
    limits: PushQueueLimits,
    budget: &mut B,
) -> Result<(), PushAdmissionError> {
    let units = u64::try_from(state.queue.len()).map_err(|_| PushAdmissionError::WorkBudget)?;
    if !budget.charge(units) {
        return Err(PushAdmissionError::WorkBudget);
    }
    let mut tenants = HashSet::new();
    let mut repositories = HashSet::new();
    let mut actors = HashSet::new();
    tenants
        .try_reserve(state.queue.len())
        .map_err(|_| PushAdmissionError::Allocation)?;
    repositories
        .try_reserve(state.queue.len())
        .map_err(|_| PushAdmissionError::Allocation)?;
    actors
        .try_reserve(state.queue.len())
        .map_err(|_| PushAdmissionError::Allocation)?;
    for queued in &state.queue {
        tenants.insert(queued.request.identity.tenant.clone());
        repositories.insert(queued.repository_key.clone());
        actors.insert(queued.actor_key.clone());
    }
    let missing_tenants = tenants
        .iter()
        .filter(|key| !state.tenant_deficit.contains_key(*key))
        .count();
    let missing_repositories = repositories
        .iter()
        .filter(|key| !state.repository_deficit.contains_key(*key))
        .count();
    let missing_actors = actors
        .iter()
        .filter(|key| !state.actor_deficit.contains_key(*key))
        .count();
    state
        .tenant_deficit
        .try_reserve(missing_tenants)
        .map_err(|_| PushAdmissionError::Allocation)?;
    state
        .repository_deficit
        .try_reserve(missing_repositories)
        .map_err(|_| PushAdmissionError::Allocation)?;
    state
        .actor_deficit
        .try_reserve(missing_actors)
        .map_err(|_| PushAdmissionError::Allocation)?;
    for tenant in tenants {
        credit(&mut state.tenant_deficit, tenant, limits);
    }
    for repository in repositories {
        credit(&mut state.repository_deficit, repository, limits);
    }
    for actor in actors {
        credit(&mut state.actor_deficit, actor, limits);
    }
    Ok(())
}

fn credit<K: Eq + std::hash::Hash>(map: &mut HashMap<K, u64>, key: K, limits: PushQueueLimits) {
    let value = map.entry(key).or_default();
    *value = value
        .saturating_add(limits.fair_quantum)
        .min(limits.max_deficit);
}

fn effective_cost(
    request: &PushAdmissionRequest,
    observed_at: OffsetDateTime,
    limits: PushQueueLimits,
) -> u64 {
    let waited = if observed_at > request.submitted_at {
        observed_at - request.submitted_at
    } else {
        Duration::ZERO
    };
    let intervals = waited
        .whole_nanoseconds()
        .checked_div(limits.aging_interval.whole_nanoseconds())
        .map_or(0, |value| u64::try_from(value).unwrap_or(u64::MAX));
    request
        .cost
        .fair_service_units
        .saturating_sub(intervals.saturating_mul(limits.aging_credit_units))
        .max(1)
}

fn remove_expired<B: PushQueueWorkBudget>(
    state: &mut PushQueueState,
    observed_at: OffsetDateTime,
    budget: &mut B,
) -> Result<Option<PushAdmissionTicket>, PushAdmissionError> {
    for index in 0..state.queue.len() {
        if !budget.charge(1) {
            return Err(PushAdmissionError::WorkBudget);
        }
        let queued = &state.queue[index];
        if !state.dispatching.contains(&queued.ticket)
            && (observed_at >= queued.request.deadline
                || observed_at
                    .checked_add(queued.request.service_estimate)
                    .is_none_or(|end| end > queued.request.deadline))
        {
            let ticket = queued.ticket;
            remove_queued(state, index).ok_or(PushAdmissionError::InvalidPhase)?;
            return Ok(Some(ticket));
        }
    }
    Ok(None)
}

fn remove_queued(state: &mut PushQueueState, index: usize) -> Option<QueuedPush> {
    let (next_bytes, tenant, repository, actor) = {
        let queued = state.queue.get(index)?;
        let next_bytes = state
            .queued_bytes
            .checked_sub(queued.request.queued_bytes)?;
        let tenant = subtracted_usage(
            state
                .tenants
                .get(&queued.request.identity.tenant)
                .copied()?,
            queued.request.queued_bytes,
        )?;
        let repository = subtracted_usage(
            state.repositories.get(&queued.repository_key).copied()?,
            queued.request.queued_bytes,
        )?;
        let actor = subtracted_usage(
            state.actors.get(&queued.actor_key).copied()?,
            queued.request.queued_bytes,
        )?;
        (next_bytes, tenant, repository, actor)
    };
    let removed = state.queue.remove(index)?;
    state.queued_bytes = next_bytes;
    apply_usage(
        &mut state.tenants,
        &mut state.tenant_deficit,
        &removed.request.identity.tenant,
        tenant,
    );
    apply_usage(
        &mut state.repositories,
        &mut state.repository_deficit,
        &removed.repository_key,
        repository,
    );
    apply_usage(
        &mut state.actors,
        &mut state.actor_deficit,
        &removed.actor_key,
        actor,
    );
    Some(removed)
}

fn subtracted_usage(usage: Usage, bytes: u64) -> Option<Usage> {
    Some(Usage {
        count: usage.count.checked_sub(1)?,
        bytes: usage.bytes.checked_sub(bytes)?,
    })
}

fn apply_usage<K: Eq + std::hash::Hash>(
    map: &mut HashMap<K, Usage>,
    deficits: &mut HashMap<K, u64>,
    key: &K,
    usage: Usage,
) {
    if usage.count == 0 {
        map.remove(key);
        deficits.remove(key);
    } else {
        if let Some(current) = map.get_mut(key) {
            *current = usage;
        }
    }
}

fn reserve_scope_maps(
    state: &mut PushQueueState,
    tenant: &TenantId,
    repository: &(TenantId, RepositoryId),
    actor: &(TenantId, String),
) -> Result<(), PushAdmissionError> {
    if !state.tenants.contains_key(tenant) {
        state
            .tenants
            .try_reserve(1)
            .map_err(|_| PushAdmissionError::Allocation)?;
    }
    if !state.repositories.contains_key(repository) {
        state
            .repositories
            .try_reserve(1)
            .map_err(|_| PushAdmissionError::Allocation)?;
    }
    if !state.actors.contains_key(actor) {
        state
            .actors
            .try_reserve(1)
            .map_err(|_| PushAdmissionError::Allocation)?;
    }
    Ok(())
}

fn usage_fits(usage: Usage, bytes: u64, max_count: usize, max_bytes: u64) -> bool {
    usage.count < max_count
        && usage
            .bytes
            .checked_add(bytes)
            .is_some_and(|value| value <= max_bytes)
}

fn add_usage(usage: Usage, bytes: u64) -> Result<Usage, PushAdmissionError> {
    Ok(Usage {
        count: usage
            .count
            .checked_add(1)
            .ok_or(PushAdmissionError::QueueFull)?,
        bytes: usage
            .bytes
            .checked_add(bytes)
            .ok_or(PushAdmissionError::QueueFull)?,
    })
}

fn locate(state: &PushQueueState, ticket: PushAdmissionTicket, expected: usize) -> Option<usize> {
    state
        .queue
        .get(expected)
        .filter(|queued| queued.ticket == ticket)
        .map(|_| expected)
        .or_else(|| {
            state
                .queue
                .iter()
                .position(|queued| queued.ticket == ticket)
        })
}

fn debit<K: Eq + std::hash::Hash>(map: &mut HashMap<K, u64>, key: &K, cost: u64) {
    if let Some(value) = map.get_mut(key) {
        *value = value.saturating_sub(cost);
    }
}

fn refund_deficits(state: &mut PushQueueState, queued: &QueuedPush, cost: u64, maximum: u64) {
    refund(
        &mut state.tenant_deficit,
        &queued.request.identity.tenant,
        cost,
        maximum,
    );
    refund(
        &mut state.repository_deficit,
        &queued.repository_key,
        cost,
        maximum,
    );
    refund(&mut state.actor_deficit, &queued.actor_key, cost, maximum);
}

fn refund<K: Eq + std::hash::Hash>(map: &mut HashMap<K, u64>, key: &K, cost: u64, maximum: u64) {
    if let Some(value) = map.get_mut(key) {
        *value = value.saturating_add(cost).min(maximum);
    }
}

fn map_rejection(reason: AdmissionRejectReason, retry: RetryGuidance) -> PushAdmissionRejection {
    let reason = match reason {
        AdmissionRejectReason::Deadline => PushAdmissionRejectReason::Deadline,
        AdmissionRejectReason::BackendOverloaded => PushAdmissionRejectReason::LoadShed,
        AdmissionRejectReason::AllocationPressure => PushAdmissionRejectReason::Allocation,
        AdmissionRejectReason::UnknownTicket => PushAdmissionRejectReason::LoadShed,
    };
    PushAdmissionRejection { reason, retry }
}

fn deadline_rejection() -> PushAdmissionRejection {
    PushAdmissionRejection {
        reason: PushAdmissionRejectReason::Deadline,
        retry: RetryGuidance::DoNotRetry,
    }
}

fn valid_phase_transition(current: PushExecutionPhase, next: PushExecutionPhase) -> bool {
    matches!(
        (current, next),
        (PushExecutionPhase::Admitted, PushExecutionPhase::Input)
            | (PushExecutionPhase::Input, PushExecutionPhase::Quarantine)
            | (
                PushExecutionPhase::Quarantine,
                PushExecutionPhase::Validation
            )
            | (
                PushExecutionPhase::Validation,
                PushExecutionPhase::Promotion
            )
            | (
                PushExecutionPhase::Promotion,
                PushExecutionPhase::Publication
            )
            | (
                PushExecutionPhase::Publication,
                PushExecutionPhase::Published
            )
            | (
                PushExecutionPhase::Published,
                PushExecutionPhase::OutputStarted
            )
    )
}
