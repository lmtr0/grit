//! Deterministic bounded delta selection and incremental PACK execution.

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

use flate2::{Compress, Compression, FlushCompress, Status};
use grit_lib::objects::{ObjectId, ObjectKind};

use crate::protocol::pack_entry_reuse::{
    PackDependencySet, PackEntryPlan, PackRecompressReason, PackReuseCapabilities,
    ReusedOfsDeltaEntry, ReusedRefDeltaEntry, StoredPackEntryKind, ValidatedPackEntry,
};
use crate::protocol::pack_stream::CancellationProbe;
use crate::protocol::pack_writer::{
    IncrementalPackWriter, PackObjectWriteReport, PackWriterFailure,
};
use crate::storage::StoredObject;

/// Hard ceiling for a delta candidate window.
pub const MAX_DELTA_WINDOW: usize = 256;
/// Hard ceiling for candidate comparisons in one plan.
pub const MAX_DELTA_CANDIDATE_COMPARISONS: u64 = 10_000_000;
/// Hard ceiling for generated delta depth.
pub const MAX_DELTA_DEPTH: u16 = 128;
/// Hard ceiling for candidate bytes represented by a planning window.
pub const MAX_DELTA_RETAINED_BASE_BYTES: u64 = 512 * 1024 * 1024;
/// Hard ceiling for one inflated or compressed generated instruction stream.
pub const MAX_GENERATED_DELTA_INSTRUCTION_BYTES: usize = 64 * 1024 * 1024;

/// Explicit runtime-neutral work budget consulted at deterministic checkpoints.
pub trait DeltaWorkBudget {
    /// Charge deterministic work units, returning `false` when work must stop.
    fn charge(&mut self, units: u64) -> bool;
}

/// Simple injected work counter with no implicit clock access.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DeltaWorkLimit {
    remaining: u64,
    checkpoints: u64,
}

impl DeltaWorkLimit {
    /// Construct a work counter with an exact unit allowance.
    #[must_use]
    pub const fn new(units: u64) -> Self {
        Self {
            remaining: units,
            checkpoints: 0,
        }
    }

    /// Work units not yet charged.
    #[must_use]
    pub const fn remaining(&self) -> u64 {
        self.remaining
    }

    /// Number of deterministic charge checkpoints observed.
    #[must_use]
    pub const fn checkpoints(&self) -> u64 {
        self.checkpoints
    }
}

impl DeltaWorkBudget for DeltaWorkLimit {
    fn charge(&mut self, units: u64) -> bool {
        self.checkpoints = self.checkpoints.saturating_add(1);
        let Some(remaining) = self.remaining.checked_sub(units) else {
            return false;
        };
        self.remaining = remaining;
        true
    }
}

/// Bounded deterministic delta planner configuration.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DeltaPackOptions {
    /// Maximum compatible earlier objects considered for one target.
    pub window: usize,
    /// Maximum candidate comparisons across the complete plan.
    pub max_candidate_comparisons: u64,
    /// Maximum generated/reused delta chain depth.
    pub max_depth: u16,
    /// Maximum candidate base bytes represented by all active windows.
    pub max_retained_base_bytes: u64,
    /// Maximum generated inflated instruction bytes for one entry.
    pub max_instruction_bytes: usize,
    /// Maximum retained compressed instruction bytes for one entry.
    pub max_compressed_instruction_bytes: usize,
    /// Maximum path hint bytes used as a stable similarity signal.
    pub max_path_hint_bytes: usize,
    /// Minimum exact compressed-byte saving required over a direct zlib stream.
    pub min_savings_bytes: u64,
    /// Numerator of maximum delta/direct compressed-size ratio.
    pub max_expansion_numerator: u32,
    /// Denominator of maximum delta/direct compressed-size ratio.
    pub max_expansion_denominator: u32,
    /// Negotiated representation capabilities.
    pub capabilities: PackReuseCapabilities,
}

impl Default for DeltaPackOptions {
    fn default() -> Self {
        Self {
            window: 16,
            max_candidate_comparisons: 100_000,
            max_depth: 16,
            max_retained_base_bytes: 64 * 1024 * 1024,
            max_instruction_bytes: 8 * 1024 * 1024,
            max_compressed_instruction_bytes: 8 * 1024 * 1024,
            max_path_hint_bytes: 4 * 1024,
            min_savings_bytes: 32,
            max_expansion_numerator: 7,
            max_expansion_denominator: 8,
            capabilities: PackReuseCapabilities::default(),
        }
    }
}

/// Invalid bounded delta configuration.
#[derive(Clone, Copy, Debug, thiserror::Error, PartialEq, Eq)]
pub enum DeltaPackOptionsError {
    /// A limit is zero, exceeds a hard ceiling, or forms an invalid ratio.
    #[error("invalid bounded delta pack options")]
    Invalid,
}

impl DeltaPackOptions {
    /// Validate all limits before object planning begins.
    ///
    /// # Errors
    ///
    /// Returns [`DeltaPackOptionsError::Invalid`] for zero/inconsistent limits or hard-ceiling
    /// violations.
    pub fn validate(self) -> Result<Self, DeltaPackOptionsError> {
        if self.window == 0
            || self.window > MAX_DELTA_WINDOW
            || self.max_candidate_comparisons == 0
            || self.max_candidate_comparisons > MAX_DELTA_CANDIDATE_COMPARISONS
            || self.max_depth == 0
            || self.max_depth > MAX_DELTA_DEPTH
            || self.max_retained_base_bytes == 0
            || self.max_retained_base_bytes > MAX_DELTA_RETAINED_BASE_BYTES
            || self.max_instruction_bytes == 0
            || self.max_instruction_bytes > MAX_GENERATED_DELTA_INSTRUCTION_BYTES
            || self.max_compressed_instruction_bytes == 0
            || self.max_compressed_instruction_bytes > MAX_GENERATED_DELTA_INSTRUCTION_BYTES
            || self.max_path_hint_bytes == 0
            || self.max_path_hint_bytes > 64 * 1024
            || self.max_expansion_numerator == 0
            || self.max_expansion_denominator == 0
            || self.max_expansion_numerator > self.max_expansion_denominator
        {
            return Err(DeltaPackOptionsError::Invalid);
        }
        Ok(self)
    }
}

/// One decoded outgoing object and its optional prevalidated stored representation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeltaObjectInput {
    /// Object ID in deterministic output order.
    pub oid: ObjectId,
    /// Complete decoded object used by direct fallback and candidate generation.
    pub object: StoredObject,
    /// Optional stable repository-relative path/name similarity hint.
    pub path_hint: Option<String>,
    /// Optional stored compressed representation, always preferred when dependency-safe.
    pub stored: Option<ValidatedPackEntry>,
}

/// Normal direct-fallback reason chosen without failing pack construction.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DeltaFallbackReason {
    /// No compatible candidate met similarity and size requirements.
    NoCandidate,
    /// Candidate comparison or generation work budget was exhausted.
    WorkBudget,
    /// Global candidate comparison limit was reached.
    ComparisonBudget,
    /// Candidate base retention would exceed its byte bound.
    BaseBytes,
    /// Generated instructions exceeded their inflated or compressed byte bound.
    InstructionBytes,
    /// Candidate would exceed the configured chain depth.
    Depth,
    /// Delta did not beat the direct compressed representation by the configured threshold.
    NotSmaller,
    /// Delta generation or zlib encoding could not complete within bounded state.
    Encoding,
}

/// Generated delta payload selected before output begins.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GeneratedDeltaPlan {
    base: ObjectId,
    base_index: Option<usize>,
    result_kind: ObjectKind,
    result_size: u64,
    instruction_size: u64,
    compressed: Arc<[u8]>,
    depth: u16,
}

/// Per-object deterministic output action.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DeltaEntryAction {
    /// Attempt stored compressed reuse first; executor falls back direct if dependency proof fails.
    Reuse(ValidatedPackEntry),
    /// Emit generated, precompressed delta instructions against a proven base.
    Generated(GeneratedDeltaPlan),
    /// Use the existing direct incremental compressor.
    Direct(DeltaFallbackReason),
}

/// Planned object retaining its decoded direct fallback.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeltaPlannedEntry {
    /// Object ID emitted by this entry.
    pub oid: ObjectId,
    /// Decoded object used if reuse/generation is ineligible at execution.
    pub object: StoredObject,
    /// Bounded stable path/name hint retained for later candidate selection.
    pub path_hint: Option<String>,
    /// Deterministic selected action.
    pub action: DeltaEntryAction,
}

/// Bounded planning counters.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct DeltaPlanningReport {
    /// Compatible candidates whose similarity was inspected.
    pub candidate_comparisons: u64,
    /// Stored entries selected before new delta generation.
    pub reused_entries: u64,
    /// Newly generated bounded deltas.
    pub generated_entries: u64,
    /// Direct fallback entries.
    pub direct_entries: u64,
    /// Peak candidate base bytes represented by active windows.
    pub peak_retained_base_bytes: u64,
    /// Whether the injected work budget stopped further delta work.
    pub work_exhausted: bool,
}

/// Complete deterministic bounded delta plan.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BoundedDeltaPackPlan {
    entries: Vec<DeltaPlannedEntry>,
    report: DeltaPlanningReport,
}

impl BoundedDeltaPackPlan {
    /// Borrow entries in their fixed output order.
    #[must_use]
    pub fn entries(&self) -> &[DeltaPlannedEntry] {
        &self.entries
    }

    /// Return deterministic planning counters.
    #[must_use]
    pub const fn report(&self) -> DeltaPlanningReport {
        self.report
    }

    /// Execute every action through an already-started incremental writer.
    ///
    /// Stored reuse is rechecked against actual output offsets. Any stale reuse plan becomes the
    /// existing direct streaming compressor before entry output begins. Generated OFS distances
    /// use actual writer offsets; REF-deltas are used for earlier outgoing bases when OFS is not
    /// negotiated. Thin entries are not emitted without opaque reachability evidence.
    ///
    /// # Errors
    ///
    /// Returns only existing terminal writer failures such as output limits, cancellation, or
    /// downstream rejection. Delta selection/generation failures have already become direct plans.
    pub async fn write_to<P: CancellationProbe>(
        &self,
        writer: &mut IncrementalPackWriter<'_, P>,
        capabilities: PackReuseCapabilities,
    ) -> Result<DeltaPackWriteReport, PackWriterFailure> {
        let mut dependencies = PackDependencySet::new();
        let mut report = DeltaPackWriteReport::default();
        for entry in &self.entries {
            let output_offset = writer.next_entry_offset();
            let written = match &entry.action {
                DeltaEntryAction::Reuse(source) => {
                    let plan = PackEntryPlan::from_validated_entry(
                        source,
                        &dependencies,
                        capabilities,
                        output_offset,
                    );
                    writer.write_entry_plan(&plan, Some(&entry.object)).await?
                }
                DeltaEntryAction::Generated(delta) => {
                    let plan =
                        generated_entry_plan(delta, &dependencies, capabilities, output_offset);
                    writer.write_entry_plan(&plan, Some(&entry.object)).await?
                }
                DeltaEntryAction::Direct(_) => writer.write_object(&entry.object).await?,
            };
            dependencies.record_outgoing(entry.oid, output_offset);
            report.observe(written);
        }
        Ok(report)
    }
}

/// Aggregate bounded delta execution counters.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct DeltaPackWriteReport {
    /// Completed entries.
    pub written_entries: u64,
    /// Logical decoded result bytes.
    pub decoded_bytes: u64,
    /// Compressed entry bytes emitted.
    pub compressed_bytes: u64,
}

impl DeltaPackWriteReport {
    fn observe(&mut self, entry: PackObjectWriteReport) {
        self.written_entries = self.written_entries.saturating_add(1);
        self.decoded_bytes = self.decoded_bytes.saturating_add(entry.decoded_bytes);
        self.compressed_bytes = self.compressed_bytes.saturating_add(entry.compressed_bytes);
    }
}

/// Build a deterministic bounded plan without emitting response bytes.
///
/// Objects are considered only against compatible earlier objects in per-kind sliding windows.
/// Stored deltas are attempted before generation. Any budget, depth, allocation, quality, or
/// encoding rejection becomes a direct action and planning continues deterministically.
///
/// # Errors
///
/// Returns only invalid configuration. Per-object delta failures safely select direct entries.
pub fn plan_bounded_deltas<B: DeltaWorkBudget>(
    objects: Vec<DeltaObjectInput>,
    options: DeltaPackOptions,
    budget: &mut B,
) -> Result<BoundedDeltaPackPlan, DeltaPackOptionsError> {
    let options = options.validate()?;
    let hash_algo = objects.first().map(|input| input.oid.algo());
    if objects.iter().any(|input| {
        input.oid.is_zero()
            || Some(input.oid.algo()) != hash_algo
            || input
                .stored
                .as_ref()
                .is_some_and(|stored| !stored.matches_result(input.oid, &input.object))
    }) {
        return Err(DeltaPackOptionsError::Invalid);
    }
    let mut window = VecDeque::<(usize, u64)>::new();
    let mut window_bytes = 0_u64;
    let mut planned = Vec::new();
    let mut report = DeltaPlanningReport::default();
    let mut depths = HashMap::<ObjectId, u16>::new();

    for mut input in objects {
        let target_index = planned.len();
        let target_size = input.object.data.len();
        if input
            .path_hint
            .as_ref()
            .is_some_and(|path| path.len() > options.max_path_hint_bytes)
        {
            input.path_hint = None;
        }
        let action = if let Some(stored) = input.stored.clone() {
            if stored_dependency_depth(&stored, &depths) <= options.max_depth {
                report.reused_entries = report.reused_entries.saturating_add(1);
                DeltaEntryAction::Reuse(stored)
            } else {
                DeltaEntryAction::Direct(DeltaFallbackReason::Depth)
            }
        } else if report.work_exhausted {
            DeltaEntryAction::Direct(DeltaFallbackReason::WorkBudget)
        } else if target_size == 0 {
            DeltaEntryAction::Direct(DeltaFallbackReason::NoCandidate)
        } else {
            select_generated_delta(
                &input,
                &planned,
                &window,
                &depths,
                options,
                budget,
                &mut report,
            )
        };
        let depth = match &action {
            DeltaEntryAction::Generated(delta) => delta.depth,
            DeltaEntryAction::Reuse(stored) => stored_dependency_depth(stored, &depths),
            DeltaEntryAction::Direct(_) => 0,
        };
        depths.insert(input.oid, depth);
        if matches!(action, DeltaEntryAction::Direct(_)) {
            report.direct_entries = report.direct_entries.saturating_add(1);
        }
        planned.push(DeltaPlannedEntry {
            oid: input.oid,
            object: input.object,
            path_hint: input.path_hint,
            action,
        });
        let object_bytes = match u64::try_from(target_size) {
            Ok(bytes) => bytes,
            Err(_) => u64::MAX,
        };
        if object_bytes <= options.max_retained_base_bytes {
            while window.len() >= options.window
                || window_bytes
                    .checked_add(object_bytes)
                    .is_none_or(|total| total > options.max_retained_base_bytes)
            {
                let Some((_evicted, evicted_bytes)) = window.pop_front() else {
                    break;
                };
                window_bytes = window_bytes
                    .checked_sub(evicted_bytes)
                    .ok_or(DeltaPackOptionsError::Invalid)?;
            }
            let Some(next_window_bytes) = window_bytes.checked_add(object_bytes) else {
                return Err(DeltaPackOptionsError::Invalid);
            };
            if next_window_bytes > options.max_retained_base_bytes {
                continue;
            }
            window.push_back((target_index, object_bytes));
            window_bytes = next_window_bytes;
            report.peak_retained_base_bytes = report.peak_retained_base_bytes.max(window_bytes);
        }
    }
    Ok(BoundedDeltaPackPlan {
        entries: planned,
        report,
    })
}

fn stored_dependency_depth(stored: &ValidatedPackEntry, depths: &HashMap<ObjectId, u16>) -> u16 {
    match stored.kind() {
        StoredPackEntryKind::Direct(_) => 0,
        StoredPackEntryKind::RefDelta { base, .. } => depths
            .get(&base)
            .copied()
            .map_or(u16::MAX, |depth| depth.saturating_add(1)),
        StoredPackEntryKind::OfsDelta { base, .. } => depths
            .get(&base)
            .copied()
            .map_or(u16::MAX, |depth| depth.saturating_add(1)),
    }
}

#[allow(clippy::too_many_arguments)]
fn select_generated_delta<B: DeltaWorkBudget>(
    target: &DeltaObjectInput,
    planned: &[DeltaPlannedEntry],
    window: &VecDeque<(usize, u64)>,
    depths: &HashMap<ObjectId, u16>,
    options: DeltaPackOptions,
    budget: &mut B,
    report: &mut DeltaPlanningReport,
) -> DeltaEntryAction {
    let mut best: Option<Candidate<'_>> = None;
    for (index, _) in window.iter().rev().copied() {
        let base = &planned[index];
        if base.object.kind != target.object.kind {
            continue;
        }
        if report.candidate_comparisons >= options.max_candidate_comparisons {
            return DeltaEntryAction::Direct(DeltaFallbackReason::ComparisonBudget);
        }
        report.candidate_comparisons = report.candidate_comparisons.saturating_add(1);
        let candidate = match inspect_candidate(
            target,
            base.oid,
            &base.object,
            base.path_hint.as_deref(),
            index,
            depths.get(&base.oid).copied().unwrap_or_default(),
            options,
            budget,
        ) {
            Ok(Some(candidate)) => candidate,
            Ok(None) => continue,
            Err(()) => {
                report.work_exhausted = true;
                return DeltaEntryAction::Direct(DeltaFallbackReason::WorkBudget);
            }
        };
        if candidate.depth <= options.max_depth
            && best
                .as_ref()
                .is_none_or(|current| candidate.better_than(current))
        {
            best = Some(candidate);
        }
    }
    let Some(best) = best else {
        return DeltaEntryAction::Direct(if report.work_exhausted {
            DeltaFallbackReason::WorkBudget
        } else {
            DeltaFallbackReason::NoCandidate
        });
    };
    let Some(instructions) = encode_lcp_delta_bounded(
        best.data,
        &target.object.data,
        best.prefix,
        options.max_instruction_bytes,
    ) else {
        return DeltaEntryAction::Direct(DeltaFallbackReason::InstructionBytes);
    };
    let instruction_work = u64::try_from(instructions.len()).unwrap_or(u64::MAX);
    if !budget.charge(instruction_work) {
        report.work_exhausted = true;
        return DeltaEntryAction::Direct(DeltaFallbackReason::WorkBudget);
    }
    let Some(compressed_delta) = compress_bounded(
        &instructions,
        options.max_compressed_instruction_bytes,
        true,
    ) else {
        return DeltaEntryAction::Direct(DeltaFallbackReason::Encoding);
    };
    let target_work = u64::try_from(target.object.data.len()).unwrap_or(u64::MAX);
    if !budget.charge(target_work) {
        report.work_exhausted = true;
        return DeltaEntryAction::Direct(DeltaFallbackReason::WorkBudget);
    }
    let Some(direct_size) = compress_bounded(
        &target.object.data,
        options.max_compressed_instruction_bytes,
        false,
    )
    .and_then(|bytes| u64::try_from(bytes.len()).ok()) else {
        return DeltaEntryAction::Direct(DeltaFallbackReason::Encoding);
    };
    let delta_size = u64::try_from(compressed_delta.len()).unwrap_or(u64::MAX);
    let ratio_ok = delta_size
        .checked_mul(u64::from(options.max_expansion_denominator))
        .zip(direct_size.checked_mul(u64::from(options.max_expansion_numerator)))
        .is_some_and(|(delta, direct)| delta <= direct);
    if !ratio_ok
        || delta_size
            .checked_add(options.min_savings_bytes)
            .is_none_or(|minimum| minimum > direct_size)
    {
        return DeltaEntryAction::Direct(DeltaFallbackReason::NotSmaller);
    }
    report.generated_entries = report.generated_entries.saturating_add(1);
    DeltaEntryAction::Generated(GeneratedDeltaPlan {
        base: best.oid,
        base_index: (best.index != usize::MAX).then_some(best.index),
        result_kind: target.object.kind,
        result_size: target_work,
        instruction_size: instruction_work,
        compressed: Arc::from(compressed_delta),
        depth: best.depth,
    })
}

struct Candidate<'a> {
    oid: ObjectId,
    data: &'a [u8],
    index: usize,
    path_score: u8,
    prefix: usize,
    size_difference: usize,
    depth: u16,
}

impl Candidate<'_> {
    fn better_than(&self, other: &Self) -> bool {
        (
            self.path_score,
            self.prefix,
            std::cmp::Reverse(self.size_difference),
            std::cmp::Reverse(self.oid),
            self.index,
        ) > (
            other.path_score,
            other.prefix,
            std::cmp::Reverse(other.size_difference),
            std::cmp::Reverse(other.oid),
            other.index,
        )
    }
}

#[allow(clippy::too_many_arguments)]
fn inspect_candidate<'a, B: DeltaWorkBudget>(
    target: &DeltaObjectInput,
    oid: ObjectId,
    base: &'a StoredObject,
    base_path: Option<&str>,
    index: usize,
    base_depth: u16,
    options: DeltaPackOptions,
    budget: &mut B,
) -> Result<Option<Candidate<'a>>, ()> {
    if base.kind != target.object.kind || base.data.is_empty() || oid == target.oid {
        return Ok(None);
    }
    if u64::try_from(base.data.len()).map_or(true, |bytes| bytes > options.max_retained_base_bytes)
    {
        return Ok(None);
    }
    let scan = base.data.len().min(target.object.data.len());
    let scan = u64::try_from(scan).map_err(|_| ())?;
    if !budget.charge(scan) {
        return Err(());
    }
    let prefix = base
        .data
        .iter()
        .zip(&target.object.data)
        .take_while(|(left, right)| left == right)
        .count();
    if prefix < 16 || prefix.saturating_mul(2) < target.object.data.len() {
        return Ok(None);
    }
    Ok(Some(Candidate {
        oid,
        data: &base.data,
        index,
        path_score: path_score(
            target.path_hint.as_deref(),
            base_path,
            options.max_path_hint_bytes,
        ),
        prefix,
        size_difference: base.data.len().abs_diff(target.object.data.len()),
        depth: base_depth.saturating_add(1),
    }))
}

fn path_score(target: Option<&str>, base: Option<&str>, maximum: usize) -> u8 {
    let (Some(target), Some(base)) = (target, base) else {
        return 0;
    };
    if target.len() > maximum || base.len() > maximum {
        return 0;
    }
    if target == base {
        return 2;
    }
    u8::from(target.rsplit('/').next() == base.rsplit('/').next())
}

fn encode_lcp_delta_bounded(
    base: &[u8],
    target: &[u8],
    prefix: usize,
    maximum: usize,
) -> Option<Vec<u8>> {
    let suffix = target.get(prefix..)?;
    let copy_overhead = prefix.div_ceil(0x10000).checked_mul(7)?;
    let literal_overhead = suffix.len().div_ceil(127);
    let estimate = suffix
        .len()
        .checked_add(copy_overhead)?
        .checked_add(literal_overhead)?
        .checked_add(20)?;
    if estimate > maximum {
        return None;
    }
    let mut output = Vec::new();
    output.try_reserve_exact(estimate).ok()?;
    push_varint(&mut output, base.len(), maximum)?;
    push_varint(&mut output, target.len(), maximum)?;
    push_copy(&mut output, prefix, maximum)?;
    for chunk in suffix.chunks(127) {
        push_byte(&mut output, u8::try_from(chunk.len()).ok()?, maximum)?;
        push_slice(&mut output, chunk, maximum)?;
    }
    Some(output)
}

fn push_varint(output: &mut Vec<u8>, mut value: usize, maximum: usize) -> Option<()> {
    loop {
        let mut byte = u8::try_from(value & 0x7f).ok()?;
        value >>= 7;
        if value != 0 {
            byte |= 0x80;
        }
        push_byte(output, byte, maximum)?;
        if value == 0 {
            return Some(());
        }
    }
}

fn push_copy(output: &mut Vec<u8>, mut size: usize, maximum: usize) -> Option<()> {
    let mut offset = 0_usize;
    while size > 0 {
        if u64::try_from(offset).ok()? > u64::from(u32::MAX) {
            return None;
        }
        let chunk = size.min(0x10000);
        let mut opcode = 0x80_u8;
        if offset & 0xff != 0 {
            opcode |= 0x01;
        }
        if offset & 0xff00 != 0 {
            opcode |= 0x02;
        }
        if offset & 0xff0000 != 0 {
            opcode |= 0x04;
        }
        if offset & 0xff000000 != 0 {
            opcode |= 0x08;
        }
        if chunk != 0x10000 {
            if chunk & 0xff != 0 {
                opcode |= 0x10;
            }
            if chunk & 0xff00 != 0 {
                opcode |= 0x20;
            }
        }
        push_byte(output, opcode, maximum)?;
        for (flag, shift) in [(0x01, 0), (0x02, 8), (0x04, 16), (0x08, 24)] {
            if opcode & flag != 0 {
                push_byte(
                    output,
                    u8::try_from((offset >> shift) & 0xff).ok()?,
                    maximum,
                )?;
            }
        }
        for (flag, shift) in [(0x10, 0), (0x20, 8)] {
            if opcode & flag != 0 {
                push_byte(output, u8::try_from((chunk >> shift) & 0xff).ok()?, maximum)?;
            }
        }
        offset = offset.checked_add(chunk)?;
        size -= chunk;
    }
    Some(())
}

fn push_byte(output: &mut Vec<u8>, byte: u8, maximum: usize) -> Option<()> {
    if output.len() >= maximum {
        return None;
    }
    output.push(byte);
    Some(())
}

fn push_slice(output: &mut Vec<u8>, bytes: &[u8], maximum: usize) -> Option<()> {
    if output.len().checked_add(bytes.len())? > maximum {
        return None;
    }
    output.extend_from_slice(bytes);
    Some(())
}

fn compress_bounded(input: &[u8], maximum: usize, retain: bool) -> Option<Vec<u8>> {
    const CHUNK: usize = 32 * 1024;
    let mut compressor = Compress::new(Compression::default(), true);
    let mut scratch = [0_u8; CHUNK];
    let mut output = Vec::new();
    let mut input_offset = 0_usize;
    loop {
        let before_in = compressor.total_in();
        let before_out = compressor.total_out();
        let status = compressor
            .compress(&input[input_offset..], &mut scratch, FlushCompress::Finish)
            .ok()?;
        let consumed = usize::try_from(compressor.total_in().checked_sub(before_in)?).ok()?;
        let produced = usize::try_from(compressor.total_out().checked_sub(before_out)?).ok()?;
        input_offset = input_offset.checked_add(consumed)?;
        if output.len().checked_add(produced)? > maximum {
            return None;
        }
        if retain {
            output.try_reserve(produced).ok()?;
        }
        if retain {
            output.extend_from_slice(&scratch[..produced]);
        } else {
            output.resize(output.len().checked_add(produced)?, 0);
        }
        if status == Status::StreamEnd {
            return (input_offset == input.len()).then_some(output);
        }
        if consumed == 0 && produced == 0 {
            return None;
        }
    }
}

fn generated_entry_plan(
    delta: &GeneratedDeltaPlan,
    dependencies: &PackDependencySet,
    capabilities: PackReuseCapabilities,
    output_offset: u64,
) -> PackEntryPlan {
    if let Some(_base_index) = delta.base_index {
        let Some(base_offset) = dependencies.outgoing_offset_for(&delta.base) else {
            return PackEntryPlan::RecompressFallback(PackRecompressReason::UnprovenBase);
        };
        if base_offset >= output_offset {
            return PackEntryPlan::RecompressFallback(PackRecompressReason::InvalidOutputDistance);
        }
        if capabilities.ofs_delta {
            return PackEntryPlan::OfsDelta(ReusedOfsDeltaEntry {
                result_kind: delta.result_kind,
                result_size: delta.result_size,
                declared_size: delta.instruction_size,
                base_output_offset: base_offset,
                compressed: delta.compressed.clone(),
            });
        }
        return PackEntryPlan::RefDelta(ReusedRefDeltaEntry {
            result_kind: delta.result_kind,
            result_size: delta.result_size,
            declared_size: delta.instruction_size,
            base: delta.base,
            compressed: delta.compressed.clone(),
        });
    }
    PackEntryPlan::RecompressFallback(PackRecompressReason::UnprovenBase)
}
