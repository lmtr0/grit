//! Fair bounded clone scheduling layered over weighted backend admission.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, Mutex};

use time::{Duration, OffsetDateTime};

use crate::admission::{
    AdmissionConfigError, AdmissionController, AdmissionDecision, AdmissionIdentity,
    AdmissionPermit, AdmissionRejectReason, AdmissionRequest, AdmissionTicket, BackendPressure,
    ResourceClass, ResourceWeights,
};
use crate::ids::{RepositoryId, TenantId};
use crate::protocol::clone_metrics::{
    CloneLimitError, CloneLimits, CloneMetricsRecorder, CloneMetricsReport, CloneOutcome,
};
use crate::protocol::pack_stream::{PackAbortReason, PackChunkSink, PackSinkState};

/// Stable scheduler-local clone ticket.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct CloneAdmissionTicket(u64);

impl CloneAdmissionTicket {
    /// Return the opaque scheduler-local sequence value.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

/// Weighted clone cost used for backend capacity and fair queue service.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CloneAdmissionCost {
    /// Peak decoded/delta/output memory charged while executing.
    pub memory_bytes: u64,
    /// Expected backend plus response output bytes.
    pub io_bytes: u64,
    /// Relative decode/compression CPU units.
    pub cpu_units: u64,
    /// Expected SQL/query units.
    pub sql_units: u64,
    /// Expected external object-store request units.
    pub s3_units: u64,
    /// Deficit-round-robin service cost; small jobs should use smaller values.
    pub fair_service_units: u64,
}

impl CloneAdmissionCost {
    fn weights(self) -> ResourceWeights {
        ResourceWeights {
            slots: 1,
            memory_bytes: self.memory_bytes,
            io_bytes: self.io_bytes,
            cpu_units: self.cpu_units,
            sql_units: self.sql_units,
            s3_units: self.s3_units,
        }
    }

    fn validate(self) -> Result<Self, CloneAdmissionError> {
        if self.memory_bytes == 0
            || self.io_bytes == 0
            || self.cpu_units == 0
            || self.fair_service_units == 0
        {
            return Err(CloneAdmissionError::InvalidCost);
        }
        Ok(self)
    }
}

/// Complete pre-output clone admission request.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CloneAdmissionRequest {
    /// Tenant, repository, and authenticated actor isolation identity.
    pub identity: AdmissionIdentity,
    /// Request want count validated before negotiation.
    pub wants: usize,
    /// Request have count validated before negotiation.
    pub haves: usize,
    /// Planned selected-object count validated before pack output.
    pub selected_objects: usize,
    /// Planned maximum raw response bytes validated before pack output.
    pub output_bytes: u64,
    /// Estimated bounded queue record/input bytes retained while waiting.
    pub queued_bytes: u64,
    /// Weighted execution/fairness cost.
    pub cost: CloneAdmissionCost,
    /// Caller-supplied submission time.
    pub submitted_at: OffsetDateTime,
    /// Explicit request deadline.
    pub deadline: OffsetDateTime,
    /// Estimated admitted service duration.
    pub service_estimate: Duration,
    /// Clone allocation and request ceilings enforced before enqueue.
    pub clone_limits: CloneLimits,
}

impl CloneAdmissionRequest {
    fn validate(&self) -> Result<(), CloneAdmissionError> {
        let limits = self
            .clone_limits
            .validate()
            .map_err(CloneAdmissionError::CloneLimits)?;
        limits
            .validate_request_shape(self.wants, self.haves)
            .map_err(CloneAdmissionError::CloneLimits)?;
        self.cost.validate()?;
        let minimum_queued_bytes = u64::try_from(std::mem::size_of::<QueuedClone>())
            .ok()
            .and_then(|base| base.checked_add(u64::try_from(self.identity.actor.len()).ok()?))
            .and_then(|base| {
                let tenant_bytes = u64::try_from(self.identity.tenant.as_str().len())
                    .ok()?
                    .checked_mul(2)?;
                base.checked_add(tenant_bytes)
            })
            .and_then(|base| {
                let repository_bytes = u64::try_from(self.identity.repository.as_str().len())
                    .ok()?
                    .checked_mul(2)?;
                base.checked_add(repository_bytes)
            })
            .ok_or(CloneAdmissionError::InvalidRequest)?;
        if self.selected_objects > limits.max_selected_objects
            || self.output_bytes == 0
            || self.output_bytes > limits.max_output_bytes
            || self.queued_bytes < minimum_queued_bytes
            || self.identity.actor.trim().is_empty()
            || self.identity.actor.len() > 512
            || self.service_estimate <= Duration::ZERO
            || self.service_estimate > limits.max_duration
            || self.deadline <= self.submitted_at
            || self.deadline - self.submitted_at > limits.max_duration
            || self
                .submitted_at
                .checked_add(self.service_estimate)
                .is_none_or(|completion| completion > self.deadline)
        {
            return Err(CloneAdmissionError::InvalidRequest);
        }
        Ok(())
    }

    fn admission_request(&self) -> AdmissionRequest {
        AdmissionRequest {
            class: ResourceClass::Clone,
            identity: self.identity.clone(),
            weights: self.cost.weights(),
            submitted_at: self.submitted_at,
            deadline: self.deadline,
            service_estimate: self.service_estimate,
        }
    }
}

/// Fair clone queue limits and explicit-time aging policy.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CloneQueueLimits {
    /// Global queued request count.
    pub max_queued: usize,
    /// Global retained queue bytes.
    pub max_queued_bytes: u64,
    /// Queued request count per tenant.
    pub max_tenant_queued: usize,
    /// Retained queue bytes per tenant.
    pub max_tenant_queued_bytes: u64,
    /// Queued request count per repository.
    pub max_repository_queued: usize,
    /// Retained queue bytes per repository.
    pub max_repository_queued_bytes: u64,
    /// Deficit units added to each queued tenant and repository per scheduling round.
    pub fair_quantum: u64,
    /// Deficit accumulation cap.
    pub max_deficit: u64,
    /// Explicit observed wait interval that earns one aging credit.
    pub aging_interval: Duration,
    /// Fair-service units discounted per aging interval.
    pub aging_credit_units: u64,
}

impl CloneQueueLimits {
    /// Validate queue, deficit, and aging bounds.
    ///
    /// # Errors
    ///
    /// Returns [`CloneAdmissionError::InvalidQueueLimits`] for zero/inconsistent limits.
    pub fn validate(self) -> Result<Self, CloneAdmissionError> {
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
            || self.fair_quantum == 0
            || self.max_deficit < self.fair_quantum
            || self.aging_interval <= Duration::ZERO
        {
            return Err(CloneAdmissionError::InvalidQueueLimits);
        }
        Ok(self)
    }
}

/// Explicit scheduling work checkpoint.
pub trait CloneQueueWorkBudget {
    /// Charge one or more deterministic queue-scan units.
    fn charge(&mut self, units: u64) -> bool;
}

/// Counter-based queue scan budget.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CloneQueueWorkLimit {
    remaining: u64,
}

impl CloneQueueWorkLimit {
    /// Construct an exact queue scan allowance.
    #[must_use]
    pub const fn new(units: u64) -> Self {
        Self { remaining: units }
    }

    /// Remaining scan units.
    #[must_use]
    pub const fn remaining(&self) -> u64 {
        self.remaining
    }
}

impl CloneQueueWorkBudget for CloneQueueWorkLimit {
    fn charge(&mut self, units: u64) -> bool {
        let Some(remaining) = self.remaining.checked_sub(units) else {
            return false;
        };
        self.remaining = remaining;
        true
    }
}

/// Typed clone scheduling/admission failure.
#[derive(Clone, Debug, thiserror::Error, PartialEq, Eq)]
pub enum CloneAdmissionError {
    /// Clone limit validation failed.
    #[error("clone limits rejected admission request")]
    CloneLimits(CloneLimitError),
    /// Request shape, output, or timing is invalid.
    #[error("invalid clone admission request")]
    InvalidRequest,
    /// Weighted cost has a zero required dimension.
    #[error("invalid clone admission cost")]
    InvalidCost,
    /// Queue/fairness policy is zero or inconsistent.
    #[error("invalid clone queue limits")]
    InvalidQueueLimits,
    /// Global, tenant, or repository queue count/byte bound is full.
    #[error("bounded clone queue is full")]
    QueueFull,
    /// Bounded scheduler allocation failed.
    #[error("clone scheduler allocation failed")]
    Allocation,
    /// Ticket sequence exhausted.
    #[error("clone ticket sequence exhausted")]
    TicketOverflow,
    /// Existing weighted admission rejected submission.
    #[error("weighted clone admission rejected submission")]
    Admission(AdmissionConfigError),
    /// Explicit queue scan work budget was exhausted.
    #[error("clone queue scan budget exhausted")]
    WorkBudget,
    /// Poll observed global cancellation.
    #[error("clone scheduling cancelled")]
    Cancelled,
    /// Caller-observed scheduler time moved backwards.
    #[error("clone scheduler time moved backwards")]
    NonMonotonicTime,
}

/// One fair scheduler poll result.
#[derive(Debug)]
pub enum CloneScheduleDecision {
    /// No queued request was eligible or backend resources remain occupied.
    Waiting {
        /// Current bounded queue depth.
        queued: usize,
    },
    /// One request owns weighted backend resources until the permit is released.
    Granted {
        /// Scheduler-local ticket.
        ticket: CloneAdmissionTicket,
        /// Original validated request.
        request: CloneAdmissionRequest,
        /// Combined RAII execution permit.
        permit: CloneExecutionPermit,
    },
    /// One expired/overloaded request was removed.
    Rejected {
        /// Scheduler-local ticket.
        ticket: CloneAdmissionTicket,
        /// Stable weighted-admission rejection reason.
        reason: AdmissionRejectReason,
    },
}

/// RAII permit that releases weighted admission exactly once.
#[derive(Debug)]
pub struct CloneExecutionPermit {
    admission: Option<AdmissionPermit>,
}

impl CloneExecutionPermit {
    /// Explicitly release execution resources before this value leaves scope.
    pub fn release(mut self) {
        let _ = self.admission.take();
    }
}

#[derive(Clone, Debug)]
struct QueuedClone {
    ticket: CloneAdmissionTicket,
    repository_key: (TenantId, RepositoryId),
    request: CloneAdmissionRequest,
}

#[derive(Clone, Copy, Debug, Default)]
struct QueueUsage {
    count: usize,
    bytes: u64,
}

#[derive(Debug)]
struct CloneQueueState {
    next_ticket: u64,
    queue: VecDeque<QueuedClone>,
    queued_bytes: u64,
    tenant_usage: HashMap<TenantId, QueueUsage>,
    repository_usage: HashMap<(TenantId, RepositoryId), QueueUsage>,
    tenant_deficit: HashMap<TenantId, u64>,
    repository_deficit: HashMap<(TenantId, RepositoryId), u64>,
    cursor: usize,
    dispatching: HashSet<CloneAdmissionTicket>,
    last_observed_at: Option<OffsetDateTime>,
    last_credit_at: Option<OffsetDateTime>,
}

impl Default for CloneQueueState {
    fn default() -> Self {
        Self {
            next_ticket: 1,
            queue: VecDeque::new(),
            queued_bytes: 0,
            tenant_usage: HashMap::new(),
            repository_usage: HashMap::new(),
            tenant_deficit: HashMap::new(),
            repository_deficit: HashMap::new(),
            cursor: 0,
            dispatching: HashSet::new(),
            last_observed_at: None,
            last_credit_at: None,
        }
    }
}

/// Thread-safe fair clone queue layered over [`AdmissionController`].
#[derive(Clone, Debug)]
pub struct CloneAdmissionScheduler {
    admission: AdmissionController,
    limits: CloneQueueLimits,
    state: Arc<Mutex<CloneQueueState>>,
}

impl CloneAdmissionScheduler {
    /// Construct a fair clone scheduler over an existing weighted controller.
    ///
    /// # Errors
    ///
    /// Returns invalid queue/fairness configuration.
    pub fn new(
        admission: AdmissionController,
        limits: CloneQueueLimits,
    ) -> Result<Self, CloneAdmissionError> {
        Ok(Self {
            admission,
            limits: limits.validate()?,
            state: Arc::new(Mutex::new(CloneQueueState::default())),
        })
    }

    /// Validate and enqueue one clone without starting negotiation or output.
    ///
    /// # Errors
    ///
    /// Returns request/clone-limit, global/tenant/repository queue, allocation, or ticket errors.
    pub fn enqueue(
        &self,
        request: CloneAdmissionRequest,
    ) -> Result<CloneAdmissionTicket, CloneAdmissionError> {
        request.validate()?;
        if request.cost.fair_service_units > self.limits.max_deficit {
            return Err(CloneAdmissionError::InvalidCost);
        }
        let tenant_key = request.identity.tenant.clone();
        let repository_key = (
            request.identity.tenant.clone(),
            request.identity.repository.clone(),
        );
        let queued_repository_key = repository_key.clone();
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        let tenant = &request.identity.tenant;
        let tenant_usage = state.tenant_usage.get(tenant).copied().unwrap_or_default();
        let repository_usage = state
            .repository_usage
            .get(&repository_key)
            .copied()
            .unwrap_or_default();
        let next_queued_bytes = state
            .queued_bytes
            .checked_add(request.queued_bytes)
            .filter(|bytes| *bytes <= self.limits.max_queued_bytes)
            .ok_or(CloneAdmissionError::QueueFull)?;
        if state.queue.len() >= self.limits.max_queued
            || tenant_usage.count >= self.limits.max_tenant_queued
            || tenant_usage
                .bytes
                .checked_add(request.queued_bytes)
                .is_none_or(|bytes| bytes > self.limits.max_tenant_queued_bytes)
            || repository_usage.count >= self.limits.max_repository_queued
            || repository_usage
                .bytes
                .checked_add(request.queued_bytes)
                .is_none_or(|bytes| bytes > self.limits.max_repository_queued_bytes)
        {
            return Err(CloneAdmissionError::QueueFull);
        }
        state
            .queue
            .try_reserve(1)
            .map_err(|_| CloneAdmissionError::Allocation)?;
        if !state.tenant_usage.contains_key(tenant) {
            state
                .tenant_usage
                .try_reserve(1)
                .map_err(|_| CloneAdmissionError::Allocation)?;
        }
        if !state.repository_usage.contains_key(&repository_key) {
            state
                .repository_usage
                .try_reserve(1)
                .map_err(|_| CloneAdmissionError::Allocation)?;
        }
        let ticket = CloneAdmissionTicket(state.next_ticket);
        state.next_ticket = state
            .next_ticket
            .checked_add(1)
            .ok_or(CloneAdmissionError::TicketOverflow)?;
        state.queued_bytes = next_queued_bytes;
        state.tenant_usage.insert(
            tenant_key,
            QueueUsage {
                count: tenant_usage
                    .count
                    .checked_add(1)
                    .ok_or(CloneAdmissionError::QueueFull)?,
                bytes: tenant_usage
                    .bytes
                    .checked_add(request.queued_bytes)
                    .ok_or(CloneAdmissionError::QueueFull)?,
            },
        );
        state.repository_usage.insert(
            repository_key,
            QueueUsage {
                count: repository_usage
                    .count
                    .checked_add(1)
                    .ok_or(CloneAdmissionError::QueueFull)?,
                bytes: repository_usage
                    .bytes
                    .checked_add(request.queued_bytes)
                    .ok_or(CloneAdmissionError::QueueFull)?,
            },
        );
        state.queue.push_back(QueuedClone {
            ticket,
            repository_key: queued_repository_key,
            request,
        });
        Ok(ticket)
    }

    /// Cancel a queued request. Dispatch races are remembered and release any newly granted permit.
    #[must_use]
    pub fn cancel(&self, ticket: CloneAdmissionTicket) -> bool {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        if let Some(index) = state
            .queue
            .iter()
            .position(|queued| queued.ticket == ticket)
        {
            return remove_queued(&mut state, index).is_some();
        }
        false
    }

    /// Run one deterministic fair scheduling round using caller-supplied time and pressure.
    ///
    /// No scheduler lock is held while consulting the weighted admission controller. Deficit
    /// credit is applied at most once per caller-supplied timestamp to every queued
    /// tenant/repository, and explicit-time aging lowers service cost without reading a hidden
    /// clock. Concurrent polls with the same timestamp therefore share one logical round.
    ///
    /// # Errors
    ///
    /// Returns cancellation, queue scan budget, allocation, or admission submission failures.
    pub fn poll_next<B: CloneQueueWorkBudget>(
        &self,
        observed_at: OffsetDateTime,
        pressure: BackendPressure,
        budget: &mut B,
        cancelled: bool,
    ) -> Result<CloneScheduleDecision, CloneAdmissionError> {
        if cancelled {
            return Err(CloneAdmissionError::Cancelled);
        }
        pressure
            .validate()
            .map_err(CloneAdmissionError::Admission)?;
        let selected = {
            let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
            if state
                .last_observed_at
                .is_some_and(|previous| observed_at < previous)
            {
                return Err(CloneAdmissionError::NonMonotonicTime);
            }
            state.last_observed_at = Some(observed_at);
            if state.queue.is_empty() {
                return Ok(CloneScheduleDecision::Waiting { queued: 0 });
            }
            if let Some(ticket) = remove_one_expired(&mut state, observed_at, budget)? {
                return Ok(CloneScheduleDecision::Rejected {
                    ticket,
                    reason: AdmissionRejectReason::Deadline,
                });
            }
            if state.last_credit_at != Some(observed_at) {
                let deficit_work = u64::try_from(state.queue.len())
                    .map_err(|_| CloneAdmissionError::WorkBudget)?;
                if !budget.charge(deficit_work) {
                    return Err(CloneAdmissionError::WorkBudget);
                }
                add_deficits(&mut state, self.limits)?;
                state.last_credit_at = Some(observed_at);
            }
            let len = state.queue.len();
            let mut chosen = None;
            for offset in 0..len {
                if !budget.charge(1) {
                    return Err(CloneAdmissionError::WorkBudget);
                }
                let index = (state.cursor + offset) % len;
                let queued = &state.queue[index];
                if state.dispatching.contains(&queued.ticket)
                    || observed_at < queued.request.submitted_at
                {
                    continue;
                }
                let cost = effective_cost(&queued.request, observed_at, self.limits);
                let tenant_credit = state
                    .tenant_deficit
                    .get(&queued.request.identity.tenant)
                    .copied()
                    .unwrap_or_default();
                let repository_credit = state
                    .repository_deficit
                    .get(&(
                        queued.request.identity.tenant.clone(),
                        queued.request.identity.repository.clone(),
                    ))
                    .copied()
                    .unwrap_or_default();
                if cost <= tenant_credit && cost <= repository_credit {
                    chosen = Some((index, queued.clone(), cost));
                    break;
                }
            }
            let Some((index, queued, cost)) = chosen else {
                return Ok(CloneScheduleDecision::Waiting { queued: len });
            };
            state
                .dispatching
                .try_reserve(1)
                .map_err(|_| CloneAdmissionError::Allocation)?;
            state.dispatching.insert(queued.ticket);
            debit(
                &mut state.tenant_deficit,
                &queued.request.identity.tenant,
                cost,
            );
            debit(&mut state.repository_deficit, &queued.repository_key, cost);
            (index, queued, cost)
        };

        let (index, queued, cost) = selected;
        let mut dispatch = PendingAdmissionDispatch::new(self, queued, index, cost);
        let (scheduler_ticket, admission_request) = dispatch
            .queued()
            .map(|queued| (queued.ticket, queued.request.admission_request()))
            .ok_or(CloneAdmissionError::Cancelled)?;
        let admission_ticket = self
            .admission
            .submit(admission_request)
            .map_err(CloneAdmissionError::Admission)?;
        dispatch.arm_admission(admission_ticket);
        match self.admission.poll(admission_ticket, observed_at, pressure) {
            AdmissionDecision::Granted(permit) => {
                dispatch.disarm_admission();
                let decision = dispatch.finish_granted();
                match decision {
                    GrantFinalization::Accepted => {
                        let request = dispatch
                            .take_queued()
                            .map(|queued| queued.request)
                            .ok_or(CloneAdmissionError::Cancelled)?;
                        Ok(CloneScheduleDecision::Granted {
                            ticket: scheduler_ticket,
                            request,
                            permit: CloneExecutionPermit {
                                admission: Some(permit),
                            },
                        })
                    }
                    GrantFinalization::Deadline => {
                        drop(permit);
                        Ok(CloneScheduleDecision::Rejected {
                            ticket: scheduler_ticket,
                            reason: AdmissionRejectReason::Deadline,
                        })
                    }
                    GrantFinalization::Cancelled => {
                        drop(permit);
                        Err(CloneAdmissionError::Cancelled)
                    }
                }
            }
            AdmissionDecision::Queued { .. } => {
                if self.admission.cancel(admission_ticket) {
                    dispatch.disarm_admission();
                }
                if !dispatch.finish(false, true) {
                    return Err(CloneAdmissionError::Cancelled);
                }
                Ok(CloneScheduleDecision::Waiting {
                    queued: self.queued(),
                })
            }
            AdmissionDecision::Rejected { reason, .. } => {
                dispatch.disarm_admission();
                if !dispatch.finish(true, true) {
                    return Err(CloneAdmissionError::Cancelled);
                }
                Ok(CloneScheduleDecision::Rejected {
                    ticket: scheduler_ticket,
                    reason,
                })
            }
        }
    }

    /// Current bounded queue depth.
    #[must_use]
    pub fn queued(&self) -> usize {
        self.state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .queue
            .len()
    }

    fn finish_dispatch(
        &self,
        queued: &QueuedClone,
        remove: bool,
        refund: bool,
        expected_index: usize,
        cost: u64,
    ) -> bool {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        if !state.dispatching.remove(&queued.ticket) {
            return false;
        }
        let current_index = state
            .queue
            .get(expected_index)
            .filter(|current| current.ticket == queued.ticket)
            .map(|_| expected_index)
            .or_else(|| {
                state
                    .queue
                    .iter()
                    .position(|current| current.ticket == queued.ticket)
            });
        let Some(index) = current_index else {
            refund_deficit(&mut state, queued, cost, self.limits.max_deficit);
            return false;
        };
        if !remove {
            if refund {
                refund_deficit(&mut state, queued, cost, self.limits.max_deficit);
            }
            state.cursor = if state.queue.is_empty() {
                0
            } else {
                (index + 1) % state.queue.len()
            };
            return true;
        }
        if remove_queued(&mut state, index).is_none() {
            refund_deficit(&mut state, queued, cost, self.limits.max_deficit);
            return false;
        }
        if refund {
            refund_deficit(&mut state, queued, cost, self.limits.max_deficit);
        }
        state.cursor = if state.queue.is_empty() {
            0
        } else {
            index % state.queue.len()
        };
        true
    }

    fn finish_granted_dispatch(
        &self,
        queued: &QueuedClone,
        expected_index: usize,
        cost: u64,
    ) -> GrantFinalization {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        if !state.dispatching.remove(&queued.ticket) {
            return GrantFinalization::Cancelled;
        }
        let current_index = state
            .queue
            .get(expected_index)
            .filter(|current| current.ticket == queued.ticket)
            .map(|_| expected_index)
            .or_else(|| {
                state
                    .queue
                    .iter()
                    .position(|current| current.ticket == queued.ticket)
            });
        let Some(index) = current_index else {
            refund_deficit(&mut state, queued, cost, self.limits.max_deficit);
            return GrantFinalization::Cancelled;
        };
        let observed_at = state
            .last_observed_at
            .unwrap_or(queued.request.submitted_at);
        let deadline = observed_at >= queued.request.deadline
            || observed_at
                .checked_add(queued.request.service_estimate)
                .is_none_or(|completion| completion > queued.request.deadline);
        if remove_queued(&mut state, index).is_none() {
            refund_deficit(&mut state, queued, cost, self.limits.max_deficit);
            return GrantFinalization::Cancelled;
        }
        state.cursor = if state.queue.is_empty() {
            0
        } else {
            index % state.queue.len()
        };
        if deadline {
            refund_deficit(&mut state, queued, cost, self.limits.max_deficit);
            GrantFinalization::Deadline
        } else {
            GrantFinalization::Accepted
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum GrantFinalization {
    Accepted,
    Deadline,
    Cancelled,
}

struct PendingAdmissionDispatch<'a> {
    scheduler: &'a CloneAdmissionScheduler,
    queued: Option<QueuedClone>,
    expected_index: usize,
    cost: u64,
    admission_ticket: Option<AdmissionTicket>,
    scheduler_finished: bool,
}

impl<'a> PendingAdmissionDispatch<'a> {
    fn new(
        scheduler: &'a CloneAdmissionScheduler,
        queued: QueuedClone,
        expected_index: usize,
        cost: u64,
    ) -> Self {
        Self {
            scheduler,
            queued: Some(queued),
            expected_index,
            cost,
            admission_ticket: None,
            scheduler_finished: false,
        }
    }

    fn arm_admission(&mut self, admission_ticket: AdmissionTicket) {
        self.admission_ticket = Some(admission_ticket);
    }

    fn queued(&self) -> Option<&QueuedClone> {
        self.queued.as_ref()
    }

    fn take_queued(&mut self) -> Option<QueuedClone> {
        self.queued.take()
    }

    fn disarm_admission(&mut self) {
        self.admission_ticket = None;
    }

    fn finish(&mut self, remove: bool, refund: bool) -> bool {
        if self.scheduler_finished {
            return false;
        }
        let Some(queued) = self.queued.as_ref() else {
            return false;
        };
        let finished =
            self.scheduler
                .finish_dispatch(queued, remove, refund, self.expected_index, self.cost);
        self.scheduler_finished = true;
        finished
    }

    fn finish_granted(&mut self) -> GrantFinalization {
        if self.scheduler_finished {
            return GrantFinalization::Cancelled;
        }
        let Some(queued) = self.queued.as_ref() else {
            return GrantFinalization::Cancelled;
        };
        let decision =
            self.scheduler
                .finish_granted_dispatch(queued, self.expected_index, self.cost);
        self.scheduler_finished = true;
        decision
    }
}

impl Drop for PendingAdmissionDispatch<'_> {
    fn drop(&mut self) {
        if let Some(ticket) = self.admission_ticket.take() {
            let _ = self.scheduler.admission.cancel(ticket);
        }
        if !self.scheduler_finished {
            if let Some(queued) = self.queued.as_ref() {
                let _ = self.scheduler.finish_dispatch(
                    queued,
                    false,
                    true,
                    self.expected_index,
                    self.cost,
                );
            }
        }
    }
}

fn add_deficits(
    state: &mut CloneQueueState,
    limits: CloneQueueLimits,
) -> Result<(), CloneAdmissionError> {
    let mut tenants = HashSet::new();
    let mut repositories = HashSet::new();
    tenants
        .try_reserve(state.queue.len())
        .map_err(|_| CloneAdmissionError::Allocation)?;
    repositories
        .try_reserve(state.queue.len())
        .map_err(|_| CloneAdmissionError::Allocation)?;
    for queued in &state.queue {
        tenants.insert(queued.request.identity.tenant.clone());
        repositories.insert((
            queued.request.identity.tenant.clone(),
            queued.request.identity.repository.clone(),
        ));
    }
    state
        .tenant_deficit
        .try_reserve(tenants.len())
        .map_err(|_| CloneAdmissionError::Allocation)?;
    state
        .repository_deficit
        .try_reserve(repositories.len())
        .map_err(|_| CloneAdmissionError::Allocation)?;
    for tenant_id in tenants {
        let tenant = state.tenant_deficit.entry(tenant_id).or_default();
        *tenant = tenant
            .saturating_add(limits.fair_quantum)
            .min(limits.max_deficit);
    }
    for repository_id in repositories {
        let repository = state.repository_deficit.entry(repository_id).or_default();
        *repository = repository
            .saturating_add(limits.fair_quantum)
            .min(limits.max_deficit);
    }
    Ok(())
}

fn effective_cost(
    request: &CloneAdmissionRequest,
    observed_at: OffsetDateTime,
    limits: CloneQueueLimits,
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
    let credit = intervals.saturating_mul(limits.aging_credit_units);
    request
        .cost
        .fair_service_units
        .saturating_sub(credit)
        .max(1)
}

fn remove_one_expired<B: CloneQueueWorkBudget>(
    state: &mut CloneQueueState,
    observed_at: OffsetDateTime,
    budget: &mut B,
) -> Result<Option<CloneAdmissionTicket>, CloneAdmissionError> {
    for index in 0..state.queue.len() {
        if !budget.charge(1) {
            return Err(CloneAdmissionError::WorkBudget);
        }
        let queued = &state.queue[index];
        let cannot_finish = observed_at
            .checked_add(queued.request.service_estimate)
            .is_none_or(|completion| completion > queued.request.deadline);
        if !state.dispatching.contains(&queued.ticket)
            && (observed_at >= queued.request.deadline || cannot_finish)
        {
            let ticket = queued.ticket;
            return Ok(remove_queued(state, index).map(|_| ticket));
        }
    }
    Ok(None)
}

fn remove_queued(state: &mut CloneQueueState, index: usize) -> Option<QueuedClone> {
    let queued = state.queue.get(index)?;
    let tenant_usage = state.tenant_usage.get(&queued.repository_key.0).copied()?;
    let repository_usage = state
        .repository_usage
        .get(&queued.repository_key)
        .copied()?;
    let next_bytes = state
        .queued_bytes
        .checked_sub(queued.request.queued_bytes)?;
    let next_tenant = QueueUsage {
        count: tenant_usage.count.checked_sub(1)?,
        bytes: tenant_usage
            .bytes
            .checked_sub(queued.request.queued_bytes)?,
    };
    let next_repository = QueueUsage {
        count: repository_usage.count.checked_sub(1)?,
        bytes: repository_usage
            .bytes
            .checked_sub(queued.request.queued_bytes)?,
    };
    let queued = state.queue.remove(index)?;
    let tenant = &queued.repository_key.0;
    let repository_key = &queued.repository_key;
    state.queued_bytes = next_bytes;
    if next_tenant.count == 0 {
        state.tenant_usage.remove(tenant);
        state.tenant_deficit.remove(tenant);
    } else if let Some(usage) = state.tenant_usage.get_mut(tenant) {
        *usage = next_tenant;
    }
    if next_repository.count == 0 {
        state.repository_usage.remove(repository_key);
        state.repository_deficit.remove(repository_key);
    } else if let Some(usage) = state.repository_usage.get_mut(repository_key) {
        *usage = next_repository;
    }
    Some(queued)
}

fn debit<K: Eq + std::hash::Hash>(deficits: &mut HashMap<K, u64>, key: &K, cost: u64) {
    if let Some(deficit) = deficits.get_mut(key) {
        *deficit = deficit.saturating_sub(cost);
    }
}

fn refund_deficit(state: &mut CloneQueueState, queued: &QueuedClone, cost: u64, max_deficit: u64) {
    if let Some(deficit) = state
        .tenant_deficit
        .get_mut(&queued.request.identity.tenant)
    {
        *deficit = deficit.saturating_add(cost).min(max_deficit);
    }
    if let Some(deficit) = state.repository_deficit.get_mut(&queued.repository_key) {
        *deficit = deficit.saturating_add(cost).min(max_deficit);
    }
}

/// Abort a started clone stream, record its terminal outcome, and release its permit.
///
/// No scheduler/admission lock is held across the sink call. Abort cleanup is best effort because
/// a transport may already be terminal; metrics preserve the caller-selected first outcome.
pub async fn abort_admitted_clone(
    permit: CloneExecutionPermit,
    sink: &mut dyn PackChunkSink,
    recorder: &mut CloneMetricsRecorder,
    reason: PackAbortReason,
    outcome: CloneOutcome,
) -> CloneMetricsReport {
    if sink.state() == PackSinkState::Open {
        let _ = sink.abort(reason).await;
    }
    let report = recorder.finish(outcome);
    drop(permit);
    report
}
