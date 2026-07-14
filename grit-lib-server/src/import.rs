//! Import filesystem-backed repositories into server storage.

use std::collections::{HashMap, HashSet, VecDeque};
use std::fs;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};
use std::thread::JoinHandle;
use std::time::SystemTime;

use grit_lib::config::ConfigSet;
use grit_lib::objects::{
    parse_commit, parse_tag, parse_tree, HashAlgo, Object, ObjectId, ObjectKind,
};
use grit_lib::odb::Odb;
use grit_lib::pack::{self, PackIndex};
use grit_lib::refs;
use grit_lib::repo::Repository;
use sha1::{Digest as _, Sha1};
use sha2::Sha256;

use crate::error::{Error, Result};
use crate::storage::{
    commit_time_from_identity, ImportPublication, ImportedPack, IndexedCommit, IndexedTreeEntry,
    PackMetadata, PackObjectIndex, ServerStorage, StoredObject, StoredRef,
};

const IMPORT_OBJECT_BATCH_MAX_COUNT: usize = 2_000;
const IMPORT_OBJECT_BATCH_MAX_BYTES: usize = 32 * 1024 * 1024;
const DEFAULT_IMPORT_DECODE_WORKERS: usize = 4;
const DEFAULT_IMPORT_DECODE_BYTES: usize = 128 * 1024 * 1024;
const MAX_IMPORT_DECODE_WORKERS: usize = 32;
const MAX_IMPORT_DECODE_BYTES: usize = 1024 * 1024 * 1024;

type NativeObjectMetadata = HashMap<ObjectId, NativeObjectMetadataEntry>;

#[derive(Clone, Copy)]
struct NativeObjectMetadataEntry {
    kind: ObjectKind,
    size: u64,
    decode_working_set: u64,
    import_working_set: u64,
}

#[derive(Clone, Copy)]
struct ImportWork {
    oid: ObjectId,
    expected_kind: Option<ObjectKind>,
}

struct DecodeJob {
    work: ImportWork,
    native_metadata: Option<NativeObjectMetadataEntry>,
    skip_payload: bool,
}

struct DecodedImportObject {
    work: ImportWork,
    stored: Option<StoredObject>,
    object_kind: ObjectKind,
    object_size: u64,
    parsed: ParsedImportObject,
}

enum ParsedImportObject {
    Commit {
        tree: ObjectId,
        parents: Vec<ObjectId>,
        commit_time: i64,
    },
    Tree(Vec<ParsedTreeEntry>),
    Tag {
        target: ObjectId,
        target_kind: ObjectKind,
    },
    Blob,
}

struct ParsedTreeEntry {
    name: String,
    mode: u32,
    oid: ObjectId,
    kind: ObjectKind,
}

impl ImportWork {
    fn object(oid: ObjectId) -> Self {
        Self {
            oid,
            expected_kind: None,
        }
    }

    fn expected(oid: ObjectId, expected_kind: ObjectKind) -> Self {
        Self {
            oid,
            expected_kind: Some(expected_kind),
        }
    }
}

struct NativeSourcePack {
    index: PackIndex,
    data: Arc<Vec<u8>>,
    object_metadata: NativeObjectMetadata,
    metadata: PackMetadata,
    modified: SystemTime,
    already_stored: bool,
}

#[derive(Default)]
struct NativeSourcePacks {
    packs: Vec<Arc<NativeSourcePack>>,
    loose_oids: Vec<ObjectId>,
    full_mirror_compatible: bool,
    retention_enabled: bool,
}

struct NativeInstallInputs<'a> {
    source: &'a Repository,
    options: &'a ImportOptions,
    reachable: &'a HashSet<ObjectId>,
    newly_trusted: &'a [(ObjectId, ObjectKind)],
}

struct SourceImportInputs {
    roots: Vec<ImportWork>,
    desired_refs: Vec<(String, StoredRef)>,
    config_entries: Vec<(String, String)>,
}

impl NativeSourcePacks {
    fn object_metadata(&self, oid: &ObjectId) -> Option<NativeObjectMetadataEntry> {
        self.packs
            .iter()
            .rev()
            .find_map(|source_pack| source_pack.object_metadata.get(oid).copied())
    }

    fn read_verified_object(&self, oid: &ObjectId) -> Result<Option<Object>> {
        let Some(source_pack) = self
            .packs
            .iter()
            .rev()
            .find(|source_pack| source_pack.index.find_offset(oid).is_some())
        else {
            return Ok(None);
        };
        let object = pack::read_object_from_pack_bytes(
            &source_pack.data,
            &source_pack.index,
            oid.as_bytes(),
        )?;
        Ok(Some(object))
    }
}

/// Summary of an import run.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ImportReport {
    /// Number of refs imported.
    pub refs: usize,
    /// Number of config entries imported.
    pub config_entries: usize,
    /// Number of unique reachable objects imported.
    pub objects: usize,
    /// Number of unique direct tree entries indexed.
    pub tree_entries: usize,
    /// Number of reachable commit graph rows indexed during import.
    pub commit_graph_entries: usize,
    /// Number of refs removed because they no longer exist in the source repository.
    pub pruned_refs: usize,
    /// Progress checkpoints recorded during the import.
    pub checkpoints: Vec<ImportCheckpoint>,
    /// Compatible source packs newly retained without expanding their payloads into loose rows.
    pub native_packs: usize,
    /// Newly trusted reachable objects served directly from retained source packs.
    pub native_pack_objects: usize,
    /// Object payloads written through the loose-object fallback or full-mirror overlay.
    pub loose_objects: usize,
    /// Whether a requested full mirror included every compatible local pack and loose object.
    pub full_mirror_completed: bool,
    /// Backend-neutral work counters collected during the import.
    pub metrics: ImportMetrics,
}

/// Per-kind object counts and decoded payload bytes.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ImportObjectKindMetrics {
    /// Commit objects decoded and their canonical payload bytes.
    pub commits: ImportObjectMetrics,
    /// Tree objects decoded and their canonical payload bytes.
    pub trees: ImportObjectMetrics,
    /// Blob objects decoded and their canonical payload bytes.
    pub blobs: ImportObjectMetrics,
    /// Tag objects decoded and their canonical payload bytes.
    pub tags: ImportObjectMetrics,
}

impl ImportObjectKindMetrics {
    fn record(&mut self, kind: ObjectKind, bytes: u64) {
        let metric = match kind {
            ObjectKind::Commit => &mut self.commits,
            ObjectKind::Tree => &mut self.trees,
            ObjectKind::Blob => &mut self.blobs,
            ObjectKind::Tag => &mut self.tags,
        };
        metric.objects = metric.objects.saturating_add(1);
        metric.bytes = metric.bytes.saturating_add(bytes);
    }

    fn merge(&mut self, other: &Self) {
        self.commits.merge(&other.commits);
        self.trees.merge(&other.trees);
        self.blobs.merge(&other.blobs);
        self.tags.merge(&other.tags);
    }
}

/// Count and canonical payload bytes for one Git object kind.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ImportObjectMetrics {
    /// Number of objects decoded.
    pub objects: u64,
    /// Sum of decoded canonical object payload bytes.
    pub bytes: u64,
}

impl ImportObjectMetrics {
    fn merge(&mut self, other: &Self) {
        self.objects = self.objects.saturating_add(other.objects);
        self.bytes = self.bytes.saturating_add(other.bytes);
    }
}

/// Source-side work performed by repository discovery and object decoding.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ImportSourceMetrics {
    /// Compatible local source packs inspected by the native-pack discovery path.
    pub discovered_packs: u64,
    /// Pack bytes loaded while inspecting compatible local source packs.
    pub discovered_pack_bytes: u64,
    /// Actual object decode work, including retained-pack validation and fallback reads.
    pub decoded: ImportObjectKindMetrics,
}

/// Destination storage calls and durable row or byte volumes initiated by the importer.
///
/// These are application-level storage operations. A backend metrics adapter can compare them
/// with SQL statement or external-service request counters to identify amplification below the
/// [`crate::storage::ServerStorage`] boundary.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ImportDestinationMetrics {
    /// Calls that write one bounded loose-object batch.
    pub loose_object_write_batches: u64,
    /// Loose-object payload bytes submitted to destination storage.
    pub loose_object_write_bytes: u64,
    /// Calls that write a batch of retained packs and their indexes.
    pub pack_write_batches: u64,
    /// Retained pack bytes submitted to destination storage.
    pub retained_pack_bytes: u64,
    /// Object-to-pack index rows submitted with retained packs.
    pub pack_index_rows: u64,
    /// Calls that upsert direct tree-entry batches.
    pub tree_upsert_batches: u64,
    /// Direct tree-entry rows submitted to destination storage.
    pub tree_rows: u64,
    /// Calls that upsert commit graph batches.
    pub commit_upsert_batches: u64,
    /// Commit graph rows submitted to destination storage.
    pub commit_rows: u64,
    /// Guarded ref/config publication transactions completed by the importer.
    pub publication_transactions: u64,
    /// Trusted import completion markers successfully stored by the importer.
    pub completion_markers: u64,
}

/// Stable source and destination counters for one import run.
///
/// Counters saturate at [`u64::MAX`] rather than wrapping.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ImportMetrics {
    /// Source discovery and decoding work.
    pub source: ImportSourceMetrics,
    /// Destination storage calls and durable volumes.
    pub destination: ImportDestinationMetrics,
}

/// Stable phases emitted around the major import pipeline boundaries.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ImportPhase {
    /// Discover refs, configuration, local packs, and pack metadata.
    SourceDiscovery,
    /// Open a fenced destination import session and load its trusted manifest.
    DestinationPreparation,
    /// Traverse reachable objects and prepare semantic indexes.
    ObjectTraversal,
    /// Validate and install eligible native packs plus loose fallback objects.
    NativePackInstallation,
    /// Resolve object sizes and write the direct-tree browse index.
    BrowseIndex,
    /// Compute and write commit graph generations.
    CommitGraph,
    /// Atomically publish configuration, refs, pruning, and trusted objects.
    Publication,
    /// Store the guarded trusted import completion marker after publication.
    Completion,
}

/// Controls whether filesystem source packs may be retained directly during import.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum NativePackImportMode {
    /// Never inspect or retain source packs; use the portable reachable-object traversal.
    Disabled,
    /// Retain a pack only when every indexed object is in the traversed reachable closure.
    #[default]
    Reachable,
    /// Mirror every compatible local source pack and local loose object, including unreachable
    /// objects. Alternates and unsafe packs still fall back to reachable-object copying.
    FullMirror,
}

/// Options controlling repository import behavior.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ImportOptions {
    /// Remove destination refs that are absent from the filesystem-backed source.
    pub prune_deleted_refs: bool,
    /// Emit and store checkpoints every `checkpoint_interval` imported objects.
    pub checkpoint_interval: usize,
    /// Policy for retaining compatible source packs in backends that explicitly support it.
    pub native_pack_import: NativePackImportMode,
}

impl Default for ImportOptions {
    fn default() -> Self {
        Self {
            prune_deleted_refs: true,
            checkpoint_interval: 1_000,
            native_pack_import: NativePackImportMode::default(),
        }
    }
}

/// Resource limits for blocking repository-import execution.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ImportExecutionOptions {
    /// Requested number of reusable source decode and validation workers.
    ///
    /// Import clamps this value to the inclusive range `1..=32` before allocating the worker
    /// pool. The host-independent default is four.
    pub decode_workers: usize,
    /// Requested conservative source decode-and-preparation working-set bytes allowed in one wave.
    ///
    /// Import clamps this value to the inclusive range `1..=1 GiB`. A job whose conservative
    /// estimate exceeds the effective limit runs alone, as does a job without a conservative
    /// estimate. The host-independent default is 128 MiB.
    pub decode_in_flight_bytes: usize,
}

impl Default for ImportExecutionOptions {
    fn default() -> Self {
        Self {
            decode_workers: DEFAULT_IMPORT_DECODE_WORKERS,
            decode_in_flight_bytes: DEFAULT_IMPORT_DECODE_BYTES,
        }
    }
}

#[derive(Clone, Copy)]
struct ValidatedImportExecutionOptions {
    decode_workers: usize,
    decode_in_flight_bytes: usize,
}

impl From<&ImportExecutionOptions> for ValidatedImportExecutionOptions {
    fn from(options: &ImportExecutionOptions) -> Self {
        Self {
            decode_workers: options.decode_workers.clamp(1, MAX_IMPORT_DECODE_WORKERS),
            decode_in_flight_bytes: options
                .decode_in_flight_bytes
                .clamp(1, MAX_IMPORT_DECODE_BYTES),
        }
    }
}

struct BlockingState<T> {
    result: Option<Result<T>>,
    waker: Option<Waker>,
}

struct BlockingTask<T> {
    state: Arc<Mutex<BlockingState<T>>>,
}

impl<T> Future for BlockingTask<T> {
    type Output = Result<T>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        if let Some(result) = state.result.take() {
            Poll::Ready(result)
        } else {
            state.waker = Some(context.waker().clone());
            Poll::Pending
        }
    }
}

type PoolJob = Box<dyn FnOnce() + Send + 'static>;

enum PoolMessage {
    Run(PoolJob),
    Shutdown,
}

struct BlockingPool {
    sender: mpsc::Sender<PoolMessage>,
    workers: Vec<JoinHandle<()>>,
    cancelled: Arc<AtomicBool>,
    reaper_sender: Option<mpsc::Sender<Vec<JoinHandle<()>>>>,
    reaper: Option<JoinHandle<()>>,
}

impl BlockingPool {
    fn new(worker_count: usize) -> Result<Self> {
        let (reaper_sender, reaper_receiver) = mpsc::channel::<Vec<JoinHandle<()>>>();
        let reaper = std::thread::Builder::new()
            .name("grit-import-worker-reaper".to_owned())
            .spawn(move || {
                while let Ok(workers) = reaper_receiver.recv() {
                    for worker in workers {
                        let _ = worker.join();
                    }
                }
            })?;
        let (sender, receiver) = mpsc::channel();
        let receiver = Arc::new(Mutex::new(receiver));
        let cancelled = Arc::new(AtomicBool::new(false));
        let mut workers = Vec::with_capacity(worker_count);
        for worker_index in 0..worker_count {
            let receiver = Arc::clone(&receiver);
            let worker = std::thread::Builder::new()
                .name(format!("grit-import-worker-{worker_index}"))
                .spawn(move || loop {
                    let message = receiver
                        .lock()
                        .unwrap_or_else(|error| error.into_inner())
                        .recv();
                    match message {
                        Ok(PoolMessage::Run(job)) => job(),
                        Ok(PoolMessage::Shutdown) | Err(_) => break,
                    }
                });
            match worker {
                Ok(worker) => workers.push(worker),
                Err(error) => {
                    cancelled.store(true, Ordering::Release);
                    for _ in &workers {
                        let _ = sender.send(PoolMessage::Shutdown);
                    }
                    let _ = reaper_sender.send(workers);
                    drop(reaper_sender);
                    drop(reaper);
                    return Err(error.into());
                }
            }
        }
        Ok(Self {
            sender,
            workers,
            cancelled,
            reaper_sender: Some(reaper_sender),
            reaper: Some(reaper),
        })
    }

    fn submit<T>(
        &self,
        work: impl FnOnce() -> Result<T> + Send + 'static,
    ) -> Result<BlockingTask<T>>
    where
        T: Send + 'static,
    {
        let state = Arc::new(Mutex::new(BlockingState {
            result: None,
            waker: None,
        }));
        let worker_state = Arc::clone(&state);
        let cancelled = Arc::clone(&self.cancelled);
        self.sender
            .send(PoolMessage::Run(Box::new(move || {
                let result = if cancelled.load(Ordering::Acquire) {
                    Err(Error::Protocol(
                        "repository import was cancelled".to_owned(),
                    ))
                } else {
                    std::panic::catch_unwind(std::panic::AssertUnwindSafe(work))
                        .map_err(|_| Error::Protocol("blocking import worker panicked".to_owned()))
                        .and_then(std::convert::identity)
                };
                let waker = {
                    let mut state = worker_state
                        .lock()
                        .unwrap_or_else(|error| error.into_inner());
                    state.result = Some(result);
                    state.waker.take()
                };
                if let Some(waker) = waker {
                    waker.wake();
                }
            })))
            .map_err(|_| Error::Protocol("blocking import worker pool stopped".to_owned()))?;
        Ok(BlockingTask { state })
    }

    async fn run_jobs<J, T, F>(&self, jobs: Vec<J>, work: F) -> Result<Vec<T>>
    where
        J: Send + 'static,
        T: Send + 'static,
        F: Fn(J) -> Result<T> + Send + Sync + 'static,
    {
        let work = Arc::new(work);
        let mut tasks = Vec::with_capacity(jobs.len());
        let mut first_error = None;
        for job in jobs {
            let work = Arc::clone(&work);
            match self.submit(move || work(job)) {
                Ok(task) => tasks.push(task),
                Err(error) => {
                    first_error = Some(error);
                    break;
                }
            }
        }
        let mut results = Vec::with_capacity(tasks.len());
        for task in tasks {
            match task.await {
                Ok(result) => results.push(Some(result)),
                Err(error) => {
                    results.push(None);
                    if first_error.is_none() {
                        first_error = Some(error);
                    }
                }
            }
        }
        if let Some(error) = first_error {
            return Err(error);
        }
        results
            .into_iter()
            .map(|result| {
                result.ok_or_else(|| {
                    Error::Protocol("blocking import job lost its result".to_owned())
                })
            })
            .collect()
    }

    fn shutdown(&mut self) -> Result<()> {
        let mut first_error = None;
        for _ in &self.workers {
            if self.sender.send(PoolMessage::Shutdown).is_err() && first_error.is_none() {
                first_error = Some(Error::Protocol(
                    "blocking import worker pool stopped".to_owned(),
                ));
            }
        }
        for worker in self.workers.drain(..) {
            if worker.join().is_err() && first_error.is_none() {
                first_error = Some(Error::Protocol(
                    "blocking import worker panicked".to_owned(),
                ));
            }
        }
        drop(self.reaper_sender.take());
        if self
            .reaper
            .take()
            .is_some_and(|reaper| reaper.join().is_err())
            && first_error.is_none()
        {
            first_error = Some(Error::Protocol(
                "blocking import worker reaper panicked".to_owned(),
            ));
        }
        first_error.map_or(Ok(()), Err)
    }
}

impl Drop for BlockingPool {
    fn drop(&mut self) {
        if self.workers.is_empty() {
            return;
        }
        self.cancelled.store(true, Ordering::Release);
        for _ in &self.workers {
            let _ = self.sender.send(PoolMessage::Shutdown);
        }
        let workers = self.workers.drain(..).collect::<Vec<_>>();
        if let Some(reaper_sender) = self.reaper_sender.take() {
            let _ = reaper_sender.send(workers);
        }
        // Disconnecting the channel lets the already-running reaper self-terminate after joining
        // the active workers. Dropping its handle intentionally detaches that non-blocking cleanup.
        drop(self.reaper.take());
    }
}

/// Import progress checkpoint for large repository imports.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ImportCheckpoint {
    /// Number of unique objects imported when the checkpoint was emitted.
    pub objects: usize,
    /// Number of refs imported when the checkpoint was emitted.
    pub refs: usize,
    /// Number of tree entries indexed when the checkpoint was emitted.
    pub tree_entries: usize,
}

/// Progress event emitted while importing a repository.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ImportProgressEvent {
    /// A stable import pipeline phase is about to begin.
    PhaseStarted(ImportPhase),
    /// A stable import pipeline phase completed with cumulative metrics at its boundary.
    PhaseCompleted {
        /// Phase that completed.
        phase: ImportPhase,
        /// Cumulative import counters after the phase completed.
        metrics: ImportMetrics,
    },
    /// A config entry was copied.
    ConfigEntry {
        /// Config key that was imported.
        key: String,
    },
    /// A ref was copied from the source repository.
    Ref {
        /// Ref name that was imported.
        refname: String,
    },
    /// A destination ref was removed because it is absent from the source repository.
    PrunedRef {
        /// Ref name that was removed.
        refname: String,
    },
    /// A reachable object was copied.
    Object {
        /// Object id that was imported.
        oid: ObjectId,
        /// Git object kind.
        kind: ObjectKind,
    },
    /// Tree entries were indexed.
    TreeEntries {
        /// Tree object whose direct browse entries were indexed.
        tree_oid: ObjectId,
        /// Number of entries indexed in this batch.
        entries: usize,
    },
    /// A progress checkpoint was reached.
    Checkpoint(ImportCheckpoint),
    /// Durable repository publication completed and is ready for its trusted completion marker.
    Completed(ImportReport),
}

/// Import reachable repository data from a filesystem-backed [`Repository`].
///
/// # Errors
///
/// Returns errors from the source repository, destination storage, object parsing, or worker-pool
/// construction, submission, execution, and orderly shutdown.
pub async fn import_repository<S>(
    destination: &crate::repository::ServerRepository<S>,
    source: &Repository,
) -> Result<ImportReport>
where
    S: ServerStorage,
{
    import_repository_with_options(destination, source, ImportOptions::default(), |_| Ok(())).await
}

/// Import reachable repository data with explicit semantic options and progress reporting.
///
/// This source-compatible entry point uses [`ImportExecutionOptions::default`] for blocking work
/// limits. Use [`import_repository_with_execution_options`] to tune those resource limits.
///
/// # Errors
///
/// Returns errors from the source repository, destination storage, progress callback, object
/// parsing, or worker-pool construction, submission, execution, and orderly shutdown.
pub async fn import_repository_with_options<S, P>(
    destination: &crate::repository::ServerRepository<S>,
    source: &Repository,
    options: ImportOptions,
    progress: P,
) -> Result<ImportReport>
where
    S: ServerStorage,
    P: FnMut(ImportProgressEvent) -> Result<()>,
{
    import_repository_with_execution_options(
        destination,
        source,
        options,
        ImportExecutionOptions::default(),
        progress,
    )
    .await
}

/// Import reachable repository data with semantic and execution options plus progress reporting.
///
/// Direct entries discovered during import are upserted by immutable tree object id. Existing
/// entries for stored objects outside the current reachable closure are preserved. Use
/// [`crate::maintenance::repair_browse_index`] to migrate or remove legacy flattened rows.
/// After a completed import, subsequent runs stop at object ids recorded in the durable trusted
/// manifest. A failed run leaves the repository incomplete, so its retry traverses the source
/// closure without trusting objects written by the partial run. Ref updates and pruning are
/// published only after object and query-index writes finish. Progress callbacks for final config,
/// ref, prune, and completion events run after guarded publication but before the trusted
/// completion marker; rejecting any such event leaves the import incomplete. The final
/// `PhaseCompleted(Completion)` event is necessarily emitted after the marker is durable; if that
/// callback rejects the event, the function returns its error but the import remains complete.
///
/// [`NativePackImportMode::Reachable`] retains only self-contained local packs whose complete
/// index belongs to the current closure or a previously completed trusted closure.
/// [`NativePackImportMode::FullMirror`] additionally retains unreachable objects from compatible
/// local packs and the local loose store. Native retention requires a checksum-valid version-2
/// index bound to the validated pack trailer, matching per-entry CRCs, and valid in-pack delta
/// dependencies. Before publication, every object in an eligible pack is fully decoded and
/// canonical-hash verified for both SHA-1 and SHA-256; each transient payload is discarded instead
/// of being retained as a loose copy. Promisor, thin, corrupt, hash-incompatible, alternate, or
/// backend-unsupported packs safely use the reachable loose-object fallback. Refs remain
/// unpublished until every retained pack, fallback object, and query index has been installed.
/// Blocking ref/config/pack discovery, reads, decompression, commit/tree/tag parsing, tree-entry
/// preparation, and native-pack validation run outside the async executor. The caller task only
/// merges prepared records into traversal/index state and computes commit generations. Decode
/// worker count and conservative decode-plus-prepared-record working-set bytes are bounded
/// independently by [`ImportExecutionOptions`]. Delta estimates include inflated instruction
/// streams, recursive base resolution, the active base, and result payload. Preparation estimates
/// also cover the retained raw payload, parsed commit/tag strings and parent vectors, and worst-case
/// tree entry/name allocations while the completed wave waits to merge. One object whose estimate
/// is unknown or exceeds the byte limit may exceed that limit, but it always runs alone.
/// The process-wide 96 MiB delta-base cache and the separately bounded destination write batch are
/// additional memory. The portable pipeline applies to every storage backend; direct pack
/// retention remains conditional on [`crate::storage::PackStore::supports_native_pack_import`].
/// Worker completion order is normalized to scheduling order within each wave. Imported semantic
/// state and progress counts do not depend on worker timing, but changing execution limits may
/// change traversal, progress-event, and destination-batch order.
///
/// The `progress` callback receives events after durable writes complete. Returning an error from
/// the callback aborts the import and returns that error.
///
/// # Errors
///
/// Returns errors from the source repository, destination storage, progress callback, object
/// parsing, or worker-pool construction, submission, execution, and orderly shutdown. Worker
/// panics are converted to errors. Cancellation skips queued jobs and hands active-worker joining
/// to an off-executor reaper.
pub async fn import_repository_with_execution_options<S, P>(
    destination: &crate::repository::ServerRepository<S>,
    source: &Repository,
    options: ImportOptions,
    execution_options: ImportExecutionOptions,
    mut progress: P,
) -> Result<ImportReport>
where
    S: ServerStorage,
    P: FnMut(ImportProgressEvent) -> Result<()>,
{
    let mut report = ImportReport::default();
    start_import_phase(&mut progress, ImportPhase::SourceDiscovery)?;
    let execution = ValidatedImportExecutionOptions::from(&execution_options);
    let mut pool = BlockingPool::new(execution.decode_workers)?;
    let git_dir = source.git_dir.clone();
    let SourceImportInputs {
        roots,
        desired_refs,
        config_entries,
    } = pool
        .submit(move || discover_source_inputs(&git_dir))?
        .await?;
    let native_source_packs = discover_native_source_packs(destination, source, &options, &pool)
        .await?
        .map(Arc::new);
    let native_import_enabled = native_source_packs
        .as_ref()
        .is_some_and(|native| native.retention_enabled);
    if let Some(native) = &native_source_packs {
        report.metrics.source.discovered_packs = usize_to_u64(native.packs.len());
        report.metrics.source.discovered_pack_bytes =
            native.packs.iter().fold(0u64, |bytes, pack| {
                bytes.saturating_add(pack.metadata.size_bytes)
            });
    }
    complete_import_phase(&mut progress, ImportPhase::SourceDiscovery, &report)?;

    start_import_phase(&mut progress, ImportPhase::DestinationPreparation)?;
    let session = destination
        .storage()
        .begin_import(destination.tenant(), destination.repository())
        .await?;
    let trusted_objects = session
        .trusted_objects()
        .iter()
        .map(|(oid, _)| *oid)
        .collect::<HashSet<_>>();
    complete_import_phase(&mut progress, ImportPhase::DestinationPreparation, &report)?;

    start_import_phase(&mut progress, ImportPhase::ObjectTraversal)?;
    let mut seen = HashSet::new();
    let mut newly_trusted = Vec::new();
    let mut indexed_commits = HashMap::new();
    let mut object_sizes = HashMap::new();
    let mut pending_size_entries = HashMap::<ObjectId, Vec<usize>>::new();
    let mut tree_entries = Vec::<IndexedTreeEntry>::new();
    let mut indexed_tree_batches = Vec::new();
    let mut pending_objects = Vec::with_capacity(IMPORT_OBJECT_BATCH_MAX_COUNT);
    let mut pending_object_bytes = 0usize;
    let mut stack = roots;
    while !stack.is_empty() {
        let worker_limit = execution.decode_workers.min(stack.len().max(1));
        let byte_limit = execution.decode_in_flight_bytes;
        let mut scheduled_bytes = 0usize;
        let mut jobs = Vec::with_capacity(worker_limit);
        while jobs.len() < worker_limit {
            let Some(work) = stack.pop() else {
                break;
            };
            let oid = work.oid;
            if seen.contains(&oid) || trusted_objects.contains(&oid) {
                continue;
            }
            let native_metadata = native_source_packs
                .as_ref()
                .and_then(|native| native.object_metadata(&oid));
            if let (Some(expected), Some(metadata)) = (work.expected_kind, native_metadata) {
                if expected != metadata.kind {
                    return Err(Error::Protocol(format!(
                        "source object {oid} has kind {}, expected {expected}",
                        metadata.kind
                    )));
                }
            }
            let skip_payload = native_import_enabled
                && native_metadata.is_some_and(|metadata| metadata.kind == ObjectKind::Blob)
                && work
                    .expected_kind
                    .is_none_or(|kind| kind == ObjectKind::Blob);
            let working_set = native_metadata
                .and_then(|metadata| usize::try_from(metadata.import_working_set).ok());
            let exclusive = !skip_payload && working_set.is_none_or(|size| size > byte_limit);
            let estimated_bytes = if skip_payload {
                0
            } else {
                working_set.unwrap_or(byte_limit).min(byte_limit)
            };
            if !jobs.is_empty()
                && (exclusive || scheduled_bytes.saturating_add(estimated_bytes) > byte_limit)
            {
                stack.push(work);
                break;
            }
            seen.insert(oid);
            scheduled_bytes = scheduled_bytes.saturating_add(estimated_bytes);
            jobs.push(DecodeJob {
                work,
                native_metadata,
                skip_payload,
            });
            if exclusive {
                break;
            }
        }

        let odb = source.odb.clone();
        let native = native_source_packs.clone();
        let decoded = pool
            .run_jobs(jobs, move |job| {
                decode_import_job(&odb, native.as_deref(), job)
            })
            .await?;

        for decoded in decoded {
            let work = decoded.work;
            let oid = work.oid;
            let stored = decoded.stored;
            let object_kind = decoded.object_kind;
            let object_size = decoded.object_size;
            if stored.is_some() {
                report
                    .metrics
                    .source
                    .decoded
                    .record(object_kind, object_size);
            }
            newly_trusted.push((oid, object_kind));

            object_sizes.insert(oid, object_size);
            if let Some(entries) = pending_size_entries.remove(&oid) {
                for entry_index in entries {
                    if let Some(entry) = tree_entries.get_mut(entry_index) {
                        entry.size = Some(object_size);
                    }
                }
            }

            match decoded.parsed {
                ParsedImportObject::Commit {
                    tree,
                    parents,
                    commit_time,
                } => {
                    stack.push(ImportWork::expected(tree, ObjectKind::Tree));
                    stack.extend(
                        parents
                            .iter()
                            .copied()
                            .map(|parent| ImportWork::expected(parent, ObjectKind::Commit)),
                    );
                    indexed_commits.insert(
                        oid,
                        IndexedCommit {
                            oid,
                            tree,
                            parents,
                            commit_time,
                            generation: 1,
                        },
                    );
                }
                ParsedImportObject::Tree(entries) => {
                    let indexed = merge_parsed_tree(
                        oid,
                        entries,
                        &mut stack,
                        &mut tree_entries,
                        &object_sizes,
                        &mut pending_size_entries,
                    );
                    indexed_tree_batches.push((oid, indexed));
                }
                ParsedImportObject::Tag {
                    target,
                    target_kind,
                } => {
                    stack.push(ImportWork::expected(target, target_kind));
                }
                ParsedImportObject::Blob => {}
            }

            if !native_import_enabled {
                let stored = stored.ok_or_else(|| {
                    Error::Protocol(
                        "portable import unexpectedly omitted object payload".to_owned(),
                    )
                })?;
                let payload_bytes = stored.data.len();
                let exceeds_byte_limit = pending_object_bytes.saturating_add(payload_bytes)
                    > IMPORT_OBJECT_BATCH_MAX_BYTES;
                if !pending_objects.is_empty()
                    && (pending_objects.len() >= IMPORT_OBJECT_BATCH_MAX_COUNT
                        || exceeds_byte_limit)
                {
                    flush_imported_objects(
                        destination,
                        &mut pending_objects,
                        &mut pending_object_bytes,
                        &options,
                        &mut report,
                        &mut progress,
                    )
                    .await?;
                }
                pending_object_bytes = pending_object_bytes.saturating_add(payload_bytes);
                pending_objects.push((oid, stored));
                if pending_objects.len() >= IMPORT_OBJECT_BATCH_MAX_COUNT
                    || pending_object_bytes >= IMPORT_OBJECT_BATCH_MAX_BYTES
                {
                    flush_imported_objects(
                        destination,
                        &mut pending_objects,
                        &mut pending_object_bytes,
                        &options,
                        &mut report,
                        &mut progress,
                    )
                    .await?;
                }
            }
        }
    }
    if native_import_enabled {
        complete_import_phase(&mut progress, ImportPhase::ObjectTraversal, &report)?;
        start_import_phase(&mut progress, ImportPhase::NativePackInstallation)?;
        let native_source_packs = native_source_packs.ok_or_else(|| {
            Error::Protocol("native import was enabled without source pack metadata".to_owned())
        })?;
        let mut completed_reachable = trusted_objects.clone();
        completed_reachable.extend(seen.iter().copied());
        install_native_source_packs(
            destination,
            native_source_packs,
            NativeInstallInputs {
                source,
                options: &options,
                reachable: &completed_reachable,
                newly_trusted: &newly_trusted,
            },
            execution,
            &pool,
            &mut report,
            &mut progress,
        )
        .await?;
        complete_import_phase(&mut progress, ImportPhase::NativePackInstallation, &report)?;
    } else {
        flush_imported_objects(
            destination,
            &mut pending_objects,
            &mut pending_object_bytes,
            &options,
            &mut report,
            &mut progress,
        )
        .await?;
        complete_import_phase(&mut progress, ImportPhase::ObjectTraversal, &report)?;
        start_import_phase(&mut progress, ImportPhase::NativePackInstallation)?;
        complete_import_phase(&mut progress, ImportPhase::NativePackInstallation, &report)?;
    }
    pool.shutdown()?;

    start_import_phase(&mut progress, ImportPhase::BrowseIndex)?;
    resolve_pending_tree_sizes(destination, &mut tree_entries, pending_size_entries).await?;

    if !tree_entries.is_empty() {
        destination
            .storage()
            .upsert_tree_entries(
                destination.tenant(),
                destination.repository(),
                &tree_entries,
            )
            .await?;
        report.metrics.destination.tree_upsert_batches = report
            .metrics
            .destination
            .tree_upsert_batches
            .saturating_add(1);
        report.metrics.destination.tree_rows = report
            .metrics
            .destination
            .tree_rows
            .saturating_add(usize_to_u64(tree_entries.len()));
    }
    report.tree_entries = tree_entries.len();
    for (tree_oid, entries) in indexed_tree_batches {
        progress(ImportProgressEvent::TreeEntries { tree_oid, entries })?;
    }
    complete_import_phase(&mut progress, ImportPhase::BrowseIndex, &report)?;

    start_import_phase(&mut progress, ImportPhase::CommitGraph)?;
    let existing_commits = if indexed_commits.is_empty() {
        HashMap::new()
    } else {
        destination
            .storage()
            .list_indexed_commits(destination.tenant(), destination.repository())
            .await?
            .into_iter()
            .map(|commit| (commit.oid, commit))
            .collect()
    };
    compute_commit_generations(&mut indexed_commits, &existing_commits);
    let mut indexed_commits = indexed_commits.into_values().collect::<Vec<_>>();
    indexed_commits.sort_by_key(|commit| commit.oid);
    if !indexed_commits.is_empty() {
        destination
            .storage()
            .upsert_commits(
                destination.tenant(),
                destination.repository(),
                &indexed_commits,
            )
            .await?;
        report.metrics.destination.commit_upsert_batches = report
            .metrics
            .destination
            .commit_upsert_batches
            .saturating_add(1);
        report.metrics.destination.commit_rows = report
            .metrics
            .destination
            .commit_rows
            .saturating_add(usize_to_u64(indexed_commits.len()));
    }
    report.commit_graph_entries = indexed_commits.len();
    complete_import_phase(&mut progress, ImportPhase::CommitGraph, &report)?;

    start_import_phase(&mut progress, ImportPhase::Publication)?;
    let publication = ImportPublication {
        config_entries,
        refs: desired_refs,
        prune_deleted_refs: options.prune_deleted_refs,
        newly_trusted,
    };
    let publication_result = destination
        .storage()
        .publish_import(
            destination.tenant(),
            destination.repository(),
            &session,
            &publication,
        )
        .await?;
    report.metrics.destination.publication_transactions = report
        .metrics
        .destination
        .publication_transactions
        .saturating_add(1);
    report.config_entries = publication.config_entries.len();
    for (key, _) in &publication.config_entries {
        progress(ImportProgressEvent::ConfigEntry { key: key.clone() })?;
    }
    for (refname, _) in &publication.refs {
        report.refs += 1;
        progress(ImportProgressEvent::Ref {
            refname: refname.clone(),
        })?;
    }
    for refname in publication_result.pruned_refs {
        report.pruned_refs += 1;
        progress(ImportProgressEvent::PrunedRef { refname })?;
    }
    complete_import_phase(&mut progress, ImportPhase::Publication, &report)?;

    start_import_phase(&mut progress, ImportPhase::Completion)?;
    progress(ImportProgressEvent::Completed(report.clone()))?;
    destination
        .storage()
        .complete_import(destination.tenant(), destination.repository(), &session)
        .await?;
    report.metrics.destination.completion_markers = report
        .metrics
        .destination
        .completion_markers
        .saturating_add(1);
    complete_import_phase(&mut progress, ImportPhase::Completion, &report)?;
    Ok(report)
}

async fn discover_native_source_packs<S>(
    destination: &crate::repository::ServerRepository<S>,
    source: &Repository,
    options: &ImportOptions,
    pool: &BlockingPool,
) -> Result<Option<NativeSourcePacks>>
where
    S: ServerStorage,
{
    if options.native_pack_import == NativePackImportMode::Disabled
        || destination.hash_algo() != source.odb.hash_algo()
    {
        return Ok(None);
    }

    let retention_enabled = destination.storage().supports_native_pack_import();

    let installed = if retention_enabled {
        destination
            .storage()
            .list_packs(destination.tenant(), destination.repository())
            .await?
            .into_iter()
            .map(|metadata| metadata.pack_checksum)
            .collect::<HashSet<_>>()
    } else {
        HashSet::new()
    };
    let odb = source.odb.clone();
    let hash_algo = destination.hash_algo();
    let mode = options.native_pack_import;
    pool.submit(move || {
        discover_local_source_packs(&odb, hash_algo, mode, retention_enabled, installed)
    })?
    .await
}

fn discover_local_source_packs(
    odb: &Odb,
    hash_algo: HashAlgo,
    mode: NativePackImportMode,
    retention_enabled: bool,
    installed: HashSet<Vec<u8>>,
) -> Result<Option<NativeSourcePacks>> {
    let pack_dir = odb.objects_dir().join("pack");
    let local_pack_count = fs::read_dir(&pack_dir)
        .ok()
        .into_iter()
        .flatten()
        .filter_map(std::result::Result::ok)
        .filter(|entry| entry.path().extension().and_then(|value| value.to_str()) == Some("pack"))
        .count();
    let indexes = match read_local_pack_index_snapshots(odb.objects_dir()) {
        Ok(indexes) => indexes,
        Err(_) => {
            return Ok(Some(NativeSourcePacks {
                full_mirror_compatible: false,
                retention_enabled,
                ..NativeSourcePacks::default()
            }));
        }
    };
    let mut discovered = NativeSourcePacks {
        full_mirror_compatible: indexes.len() == local_pack_count,
        retention_enabled,
        ..NativeSourcePacks::default()
    };
    for (index, index_data) in indexes {
        if index.hash_bytes != hash_algo.len()
            || index.pack_path.with_extension("promisor").is_file()
        {
            discovered.full_mirror_compatible = false;
            continue;
        }
        let Ok(data) = pack::read_pack_bytes_cached(&index.pack_path) else {
            discovered.full_mirror_compatible = false;
            continue;
        };
        let Ok((metadata, object_metadata)) =
            source_pack_metadata(&index, &index_data, &data, hash_algo)
        else {
            discovered.full_mirror_compatible = false;
            continue;
        };
        let modified = fs::metadata(&index.pack_path)
            .and_then(|metadata| metadata.modified())
            .unwrap_or(SystemTime::UNIX_EPOCH);
        let already_stored = installed.contains(&metadata.pack_checksum);
        discovered.packs.push(Arc::new(NativeSourcePack {
            index,
            data,
            object_metadata,
            metadata,
            modified,
            already_stored,
        }));
    }
    discovered.packs.sort_by(|left, right| {
        left.modified
            .cmp(&right.modified)
            .then_with(|| left.index.pack_path.cmp(&right.index.pack_path))
    });
    if retention_enabled && mode == NativePackImportMode::FullMirror {
        match list_local_loose_object_ids(odb) {
            Ok(oids) => discovered.loose_oids = oids,
            Err(_) => discovered.full_mirror_compatible = false,
        }
    } else if discovered.packs.is_empty() {
        return Ok(None);
    }
    Ok(Some(discovered))
}

fn read_local_pack_index_snapshots(
    objects_dir: &std::path::Path,
) -> Result<Vec<(PackIndex, Vec<u8>)>> {
    let pack_dir = objects_dir.join("pack");
    let entries = match fs::read_dir(&pack_dir) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(error.into()),
    };
    let mut snapshots = Vec::new();
    for entry in entries {
        let path = entry?.path();
        if path.extension().and_then(|value| value.to_str()) != Some("idx") {
            continue;
        }
        let index_data = match fs::read(&path) {
            Ok(data) => data,
            Err(_) => continue,
        };
        let index = match pack::read_pack_index_snapshot(&path, &index_data) {
            Ok(index) if index.pack_path.is_file() => index,
            Ok(_) | Err(_) => continue,
        };
        snapshots.push((index, index_data));
    }
    Ok(snapshots)
}

fn source_pack_metadata(
    index: &PackIndex,
    index_data: &[u8],
    data: &[u8],
    hash_algo: HashAlgo,
) -> Result<(PackMetadata, NativeObjectMetadata)> {
    let hash_bytes = hash_algo.len();
    if data.len() < 12 + hash_bytes || &data[..4] != b"PACK" {
        return Err(Error::Protocol(
            "source pack has an invalid header".to_owned(),
        ));
    }
    let version = u32::from_be_bytes([data[4], data[5], data[6], data[7]]);
    if !matches!(version, 2 | 3) {
        return Err(Error::Protocol(format!(
            "source pack version {version} is unsupported"
        )));
    }
    let object_count = u32::from_be_bytes([data[8], data[9], data[10], data[11]]);
    if usize::try_from(object_count).ok() != Some(index.entries.len()) {
        return Err(Error::Protocol(
            "source pack and index object counts differ".to_owned(),
        ));
    }
    let trailer_start = data.len() - hash_bytes;
    let pack_checksum = digest(&data[..trailer_start], hash_algo);
    if pack_checksum.as_slice() != &data[trailer_start..] {
        return Err(Error::Protocol(
            "source pack trailing checksum mismatch".to_owned(),
        ));
    }
    let checksum_bytes = hash_bytes
        .checked_mul(2)
        .ok_or_else(|| Error::Protocol("source pack index checksum width overflow".to_owned()))?;
    if index_data.len() < checksum_bytes {
        return Err(Error::Protocol(
            "source pack index has incomplete trailing checksums".to_owned(),
        ));
    }
    let index_pack_checksum_start = index_data.len() - checksum_bytes;
    let index_checksum_start = index_data.len() - hash_bytes;
    if &index_data[index_pack_checksum_start..index_checksum_start] != pack_checksum.as_slice() {
        return Err(Error::Protocol(
            "source pack index is bound to a different pack checksum".to_owned(),
        ));
    }
    validate_self_contained_pack(index, data, trailer_start)?;
    validate_pack_entry_crcs(index, data, trailer_start)?;

    let mut object_metadata = HashMap::with_capacity(index.entries.len());
    for (raw_oid, metadata) in pack::read_all_object_decode_estimates_from_pack_bytes(data, index)?
    {
        let oid = ObjectId::from_bytes(&raw_oid)?;
        if object_metadata
            .insert(
                oid,
                NativeObjectMetadataEntry {
                    kind: metadata.kind,
                    size: metadata.size,
                    decode_working_set: metadata.working_set_size,
                    import_working_set: import_object_working_set(
                        metadata.kind,
                        metadata.size,
                        metadata.working_set_size,
                        hash_algo.len(),
                    )?,
                },
            )
            .is_some()
        {
            return Err(Error::Protocol(
                "source pack index contains duplicate object ids".to_owned(),
            ));
        }
    }
    Ok((
        PackMetadata {
            pack_checksum,
            index_checksum: index_data[index_checksum_start..].to_vec(),
            object_count,
            size_bytes: u64::try_from(data.len())
                .map_err(|_| Error::Protocol("source pack size exceeds u64".to_owned()))?,
            storage_order: 0,
        },
        object_metadata,
    ))
}

fn import_object_working_set(
    kind: ObjectKind,
    payload_size: u64,
    decode_working_set: u64,
    hash_bytes: usize,
) -> Result<u64> {
    let overflow = || Error::Protocol("import preparation working-set overflow".to_owned());
    let prepared_peak = match kind {
        ObjectKind::Blob => payload_size,
        ObjectKind::Tree => {
            // The generic parser tries SHA-1 first and accepts one octal mode byte, a space, an
            // empty name terminator, and an OID as the shortest structurally parseable entry.
            // Using that smaller-than-normal Git entry width deliberately overestimates both the
            // successful parse and a discarded SHA-1 attempt for a SHA-256 tree.
            let minimum_entry_width = u64::try_from(HashAlgo::Sha1.len())
                .map_err(|_| overflow())?
                .checked_add(3)
                .ok_or_else(overflow)?;
            let entry_count = payload_size / minimum_entry_width;
            let parsed_vector_bytes = conservative_vec_allocation_bytes(
                entry_count,
                std::mem::size_of::<grit_lib::objects::TreeEntry>(),
            )
            .ok_or_else(overflow)?;
            let prepared_vector_bytes = conservative_vec_allocation_bytes(
                entry_count,
                std::mem::size_of::<ParsedTreeEntry>(),
            )
            .ok_or_else(overflow)?;
            let vector_bytes = parsed_vector_bytes
                .checked_add(prepared_vector_bytes)
                .ok_or_else(overflow)?;
            payload_size
                .checked_mul(2)
                .and_then(|bytes| bytes.checked_add(vector_bytes))
                .ok_or_else(overflow)?
        }
        ObjectKind::Commit => {
            // The raw payload remains stored while parsing may retain author/committer strings and
            // raw forms, encoding, expanded lossy/encoded strings, decoded and raw messages, plus
            // the parent vector. Charging twenty-four payload copies covers those bounded
            // slices/duplicates and their allocation slack conservatively.
            let minimum_parent_header = u64::try_from(hash_bytes)
                .map_err(|_| overflow())?
                .checked_mul(2)
                .and_then(|bytes| bytes.checked_add(8))
                .ok_or_else(overflow)?;
            let parent_count = payload_size / minimum_parent_header;
            let parent_bytes = parent_count
                .checked_mul(
                    u64::try_from(std::mem::size_of::<ObjectId>()).map_err(|_| overflow())?,
                )
                .and_then(|bytes| bytes.checked_mul(2))
                .ok_or_else(overflow)?;
            payload_size
                .checked_mul(24)
                .and_then(|bytes| bytes.checked_add(parent_bytes))
                .ok_or_else(overflow)?
        }
        ObjectKind::Tag => {
            // The raw payload coexists with object type, tag name, tagger, and a geometrically
            // grown message buffer.
            payload_size.checked_mul(6).ok_or_else(overflow)?
        }
    };
    Ok(decode_working_set.max(prepared_peak))
}

fn conservative_vec_allocation_bytes(len: u64, element_size: usize) -> Option<u64> {
    if len == 0 || element_size == 0 {
        return Some(0);
    }
    // Rust's RawVec starts non-ZST allocations at 8 elements for one-byte items, 4 for items up
    // to 1 KiB, and 1 for larger items. Power-of-two rounding conservatively covers geometric
    // capacity growth for both the parser and prepared-entry vectors.
    let minimum_nonzero_capacity = if element_size == 1 {
        8u64
    } else if element_size <= 1024 {
        4u64
    } else {
        1u64
    };
    let capacity = len
        .max(minimum_nonzero_capacity)
        .checked_next_power_of_two()?;
    capacity.checked_mul(u64::try_from(element_size).ok()?)
}

fn validate_pack_entry_crcs(index: &PackIndex, data: &[u8], trailer_start: usize) -> Result<()> {
    let mut entries = index.entries.iter().collect::<Vec<_>>();
    entries.sort_by_key(|entry| entry.offset);
    for (position, entry) in entries.iter().enumerate() {
        let expected = entry.crc32.ok_or_else(|| {
            Error::Protocol("native retention requires a version-2 pack index".to_owned())
        })?;
        let start = usize::try_from(entry.offset)
            .map_err(|_| Error::Protocol("source pack offset exceeds usize".to_owned()))?;
        let end = entries
            .get(position + 1)
            .map(|next| usize::try_from(next.offset))
            .transpose()
            .map_err(|_| Error::Protocol("source pack offset exceeds usize".to_owned()))?
            .unwrap_or(trailer_start);
        let packed_entry = data
            .get(start..end)
            .ok_or_else(|| Error::Protocol("source pack entry span is out of bounds".to_owned()))?;
        if crc32fast::hash(packed_entry) != expected {
            return Err(Error::Protocol(format!(
                "source pack entry CRC mismatch at offset {}",
                entry.offset
            )));
        }
    }
    Ok(())
}

fn validate_self_contained_pack(
    index: &PackIndex,
    data: &[u8],
    trailer_start: usize,
) -> Result<()> {
    let offsets = index
        .entries
        .iter()
        .map(|entry| entry.offset)
        .collect::<HashSet<_>>();
    if offsets.len() != index.entries.len() {
        return Err(Error::Protocol(
            "source pack index contains duplicate offsets".to_owned(),
        ));
    }
    let oids = index
        .entries
        .iter()
        .map(|entry| entry.oid.as_slice())
        .collect::<HashSet<_>>();
    for entry in &index.entries {
        let mut cursor = usize::try_from(entry.offset)
            .map_err(|_| Error::Protocol("source pack offset exceeds usize".to_owned()))?;
        if cursor < 12 || cursor >= trailer_start {
            return Err(Error::Protocol(
                "source pack index offset is out of bounds".to_owned(),
            ));
        }
        let type_code = read_source_pack_type(data, &mut cursor, trailer_start)?;
        match type_code {
            1..=4 => {}
            6 => {
                let base = read_source_ofs_base(data, &mut cursor, trailer_start, entry.offset)?;
                if !offsets.contains(&base) {
                    return Err(Error::Protocol(
                        "source pack has an out-of-pack offset delta base".to_owned(),
                    ));
                }
            }
            7 => {
                let end = cursor.checked_add(index.hash_bytes).ok_or_else(|| {
                    Error::Protocol("source ref-delta base offset overflow".to_owned())
                })?;
                let base = data.get(cursor..end).ok_or_else(|| {
                    Error::Protocol("source ref-delta base is truncated".to_owned())
                })?;
                if !oids.contains(base) {
                    return Err(Error::Protocol(
                        "source pack has an out-of-pack reference delta base".to_owned(),
                    ));
                }
            }
            _ => {
                return Err(Error::Protocol(format!(
                    "source pack object type {type_code} is unsupported"
                )));
            }
        }
    }
    Ok(())
}

fn read_source_pack_type(data: &[u8], cursor: &mut usize, end: usize) -> Result<u8> {
    let first = read_source_pack_byte(data, cursor, end)?;
    let mut byte = first;
    while byte & 0x80 != 0 {
        byte = read_source_pack_byte(data, cursor, end)?;
    }
    Ok((first >> 4) & 0x07)
}

fn read_source_ofs_base(
    data: &[u8],
    cursor: &mut usize,
    end: usize,
    object_offset: u64,
) -> Result<u64> {
    let mut byte = read_source_pack_byte(data, cursor, end)?;
    let mut distance = u64::from(byte & 0x7f);
    while byte & 0x80 != 0 {
        byte = read_source_pack_byte(data, cursor, end)?;
        distance = distance
            .checked_add(1)
            .and_then(|value| value.checked_mul(1 << 7))
            .map(|value| value | u64::from(byte & 0x7f))
            .ok_or_else(|| Error::Protocol("source offset-delta base overflow".to_owned()))?;
    }
    object_offset
        .checked_sub(distance)
        .ok_or_else(|| Error::Protocol("source offset-delta base is invalid".to_owned()))
}

fn read_source_pack_byte(data: &[u8], cursor: &mut usize, end: usize) -> Result<u8> {
    if *cursor >= end {
        return Err(Error::Protocol(
            "source pack object header is truncated".to_owned(),
        ));
    }
    let byte = data[*cursor];
    *cursor += 1;
    Ok(byte)
}

fn digest(data: &[u8], hash_algo: HashAlgo) -> Vec<u8> {
    match hash_algo {
        HashAlgo::Sha1 => {
            let mut hasher = Sha1::new();
            hasher.update(data);
            hasher.finalize().to_vec()
        }
        HashAlgo::Sha256 => {
            let mut hasher = Sha256::new();
            hasher.update(data);
            hasher.finalize().to_vec()
        }
    }
}

fn read_source_object_verified_from_odb(odb: &Odb, oid: &ObjectId) -> Result<Object> {
    let object = odb.read(oid)?;
    let actual = pack::hash_object_bytes(object.kind, &object.data, odb.hash_algo().len())?;
    if actual.as_slice() != oid.as_bytes() {
        return Err(Error::Protocol(format!(
            "source object {oid} failed identity verification"
        )));
    }
    Ok(object)
}

fn decode_import_job(
    odb: &Odb,
    native: Option<&NativeSourcePacks>,
    job: DecodeJob,
) -> Result<DecodedImportObject> {
    let stored = if job.skip_payload {
        None
    } else {
        let object = if let Some(native) = native {
            native.read_verified_object(&job.work.oid)?.map_or_else(
                || read_source_object_verified_from_odb(odb, &job.work.oid),
                Ok,
            )?
        } else {
            read_source_object_verified_from_odb(odb, &job.work.oid)?
        };
        if let Some(expected) = job.work.expected_kind {
            if object.kind != expected {
                return Err(Error::Protocol(format!(
                    "source object {} has kind {}, expected {expected}",
                    job.work.oid, object.kind
                )));
            }
        }
        Some(StoredObject::new(object.kind, object.data))
    };
    let (object_kind, object_size) = match (&stored, job.native_metadata) {
        (Some(stored), Some(metadata)) => {
            let decoded_size = u64::try_from(stored.data.len())
                .map_err(|_| Error::Protocol("source object size exceeds u64".to_owned()))?;
            if stored.kind != metadata.kind || decoded_size != metadata.size {
                return Err(Error::Protocol(format!(
                    "source pack metadata does not match decoded object {}",
                    job.work.oid
                )));
            }
            (stored.kind, decoded_size)
        }
        (Some(stored), None) => (
            stored.kind,
            u64::try_from(stored.data.len())
                .map_err(|_| Error::Protocol("source object size exceeds u64".to_owned()))?,
        ),
        (None, Some(metadata)) => (metadata.kind, metadata.size),
        (None, None) => {
            return Err(Error::Protocol(format!(
                "source object {} has neither payload nor retained-pack metadata",
                job.work.oid
            )));
        }
    };
    let parsed = match object_kind {
        ObjectKind::Commit => {
            let data = stored
                .as_ref()
                .map(|stored| stored.data.as_slice())
                .ok_or_else(|| {
                    Error::Protocol("commit payload was not decoded during import".to_owned())
                })?;
            let commit = parse_commit(data)?;
            ParsedImportObject::Commit {
                tree: commit.tree,
                parents: commit.parents,
                commit_time: commit_time_from_identity(&commit.committer),
            }
        }
        ObjectKind::Tree => {
            let data = stored
                .as_ref()
                .map(|stored| stored.data.as_slice())
                .ok_or_else(|| {
                    Error::Protocol("tree payload was not decoded during import".to_owned())
                })?;
            let entries = parse_tree(data)?
                .into_iter()
                .map(|entry| {
                    let name = String::from_utf8(entry.name).map_err(|_| {
                        Error::PathNotFound("tree entry name is not UTF-8".to_owned())
                    })?;
                    Ok(ParsedTreeEntry {
                        name,
                        mode: entry.mode,
                        oid: entry.oid,
                        kind: kind_for_mode(entry.mode),
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            ParsedImportObject::Tree(entries)
        }
        ObjectKind::Tag => {
            let data = stored
                .as_ref()
                .map(|stored| stored.data.as_slice())
                .ok_or_else(|| {
                    Error::Protocol("tag payload was not decoded during import".to_owned())
                })?;
            let tag = parse_tag(data)?;
            let target_kind = ObjectKind::from_tag_type_field(tag.object_type.as_bytes())
                .ok_or_else(|| {
                    Error::Protocol("tag target has an invalid object kind".to_owned())
                })?;
            ParsedImportObject::Tag {
                target: tag.object,
                target_kind,
            }
        }
        ObjectKind::Blob => ParsedImportObject::Blob,
    };
    Ok(DecodedImportObject {
        work: job.work,
        stored,
        object_kind,
        object_size,
        parsed,
    })
}

async fn read_source_object_wave(
    odb: Odb,
    requests: &mut VecDeque<(ObjectId, Option<u64>)>,
    execution: ValidatedImportExecutionOptions,
    pool: &BlockingPool,
) -> Result<Vec<(ObjectId, Result<Object>)>> {
    let worker_limit = execution.decode_workers.min(requests.len().max(1));
    let byte_limit = execution.decode_in_flight_bytes;
    let mut scheduled_bytes = 0usize;
    let mut jobs = Vec::with_capacity(worker_limit);
    while jobs.len() < worker_limit {
        let Some((oid, size)) = requests.pop_front() else {
            break;
        };
        let working_set = size.and_then(|size| usize::try_from(size).ok());
        let exclusive = working_set.is_none_or(|size| size > byte_limit);
        let estimated_bytes = working_set.unwrap_or(byte_limit).min(byte_limit);
        if !jobs.is_empty()
            && (exclusive || scheduled_bytes.saturating_add(estimated_bytes) > byte_limit)
        {
            requests.push_front((oid, size));
            break;
        }
        scheduled_bytes = scheduled_bytes.saturating_add(estimated_bytes);
        jobs.push(oid);
        if exclusive {
            break;
        }
    }
    pool.run_jobs(jobs, move |oid| {
        Ok((oid, read_source_object_verified_from_odb(&odb, &oid)))
    })
    .await
}

fn list_local_loose_object_ids(odb: &Odb) -> Result<Vec<ObjectId>> {
    let expected_hex_len = odb.hash_algo().len().saturating_mul(2);
    let mut oids = Vec::new();
    let entries = match fs::read_dir(odb.objects_dir()) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(oids),
        Err(error) => return Err(error.into()),
    };
    for directory in entries {
        let directory = directory?;
        let prefix = directory.file_name().to_string_lossy().into_owned();
        if prefix.len() != 2 || !prefix.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            continue;
        }
        let files = match fs::read_dir(directory.path()) {
            Ok(files) => files,
            Err(_) => continue,
        };
        for file in files {
            let file = file?;
            let suffix = file.file_name().to_string_lossy().into_owned();
            if prefix.len().saturating_add(suffix.len()) != expected_hex_len
                || !suffix.bytes().all(|byte| byte.is_ascii_hexdigit())
            {
                continue;
            }
            if let Ok(oid) = ObjectId::from_hex(&format!("{prefix}{suffix}")) {
                oids.push(oid);
            }
        }
    }
    oids.sort();
    oids.dedup();
    Ok(oids)
}

async fn install_native_source_packs<S, P>(
    destination: &crate::repository::ServerRepository<S>,
    native: Arc<NativeSourcePacks>,
    inputs: NativeInstallInputs<'_>,
    execution: ValidatedImportExecutionOptions,
    pool: &BlockingPool,
    report: &mut ImportReport,
    progress: &mut P,
) -> Result<()>
where
    S: ServerStorage,
    P: FnMut(ImportProgressEvent) -> Result<()>,
{
    let mut covered = HashSet::new();
    let mut packs_to_install = Vec::new();
    let mut all_candidates_installed = true;
    let mut candidates = VecDeque::new();
    for source_pack in &native.packs {
        let pack_oids = source_pack
            .index
            .entries
            .iter()
            .filter_map(|entry| ObjectId::from_bytes(&entry.oid).ok())
            .collect::<Vec<_>>();
        let eligible = inputs.options.native_pack_import == NativePackImportMode::FullMirror
            || (pack_oids.len() == source_pack.index.entries.len()
                && pack_oids.iter().all(|oid| inputs.reachable.contains(oid)));
        if !eligible {
            if inputs.options.native_pack_import == NativePackImportMode::FullMirror {
                all_candidates_installed = false;
            }
            continue;
        }
        candidates.push_back((Arc::clone(source_pack), pack_oids));
    }

    let worker_limit = execution.decode_workers.min(candidates.len().max(1));
    let byte_limit = execution.decode_in_flight_bytes;
    while !candidates.is_empty() {
        let mut scheduled_bytes = 0usize;
        let mut jobs = Vec::with_capacity(worker_limit);
        while jobs.len() < worker_limit {
            let Some((source_pack, pack_oids)) = candidates.pop_front() else {
                break;
            };
            let working_set =
                source_pack
                    .object_metadata
                    .values()
                    .try_fold(0usize, |maximum, metadata| {
                        usize::try_from(metadata.decode_working_set)
                            .ok()
                            .map(|size| maximum.max(size))
                    });
            let exclusive = working_set.is_none_or(|size| size > byte_limit);
            let estimated_bytes = working_set.unwrap_or(byte_limit).min(byte_limit);
            if !jobs.is_empty()
                && (exclusive || scheduled_bytes.saturating_add(estimated_bytes) > byte_limit)
            {
                candidates.push_front((source_pack, pack_oids));
                break;
            }
            scheduled_bytes = scheduled_bytes.saturating_add(estimated_bytes);
            jobs.push((source_pack, pack_oids));
            if exclusive {
                break;
            }
        }
        let validated = pool
            .run_jobs(jobs, |(source_pack, pack_oids)| {
                let validation = validate_native_pack_readability(&source_pack);
                Ok((source_pack, pack_oids, validation))
            })
            .await?;
        for (source_pack, pack_oids, validation) in validated {
            let (validation_metrics, validation_result) = validation;
            report.metrics.source.decoded.merge(&validation_metrics);
            if validation_result.is_err() {
                all_candidates_installed = false;
                continue;
            }
            if source_pack.already_stored {
                covered.extend(pack_oids);
                continue;
            }
            match prepare_native_stored_pack(&source_pack) {
                Ok(pack) => {
                    covered.extend(pack.index.iter().map(|entry| entry.oid));
                    packs_to_install.push(pack);
                }
                Err(_) => all_candidates_installed = false,
            }
        }
    }

    report.native_packs = packs_to_install.len();
    if !packs_to_install.is_empty() {
        let retained_pack_bytes = packs_to_install.iter().fold(0u64, |bytes, pack| {
            bytes.saturating_add(pack.metadata.size_bytes)
        });
        let pack_index_rows = packs_to_install.iter().fold(0u64, |rows, pack| {
            rows.saturating_add(usize_to_u64(pack.index.len()))
        });
        destination
            .storage()
            .write_imported_packs(
                destination.tenant(),
                destination.repository(),
                packs_to_install,
            )
            .await?;
        report.metrics.destination.pack_write_batches = report
            .metrics
            .destination
            .pack_write_batches
            .saturating_add(1);
        report.metrics.destination.retained_pack_bytes = report
            .metrics
            .destination
            .retained_pack_bytes
            .saturating_add(retained_pack_bytes);
        report.metrics.destination.pack_index_rows = report
            .metrics
            .destination
            .pack_index_rows
            .saturating_add(pack_index_rows);
    }

    for (oid, kind) in inputs.newly_trusted {
        if !covered.contains(oid) {
            continue;
        }
        report.objects += 1;
        report.native_pack_objects += 1;
        progress(ImportProgressEvent::Object {
            oid: *oid,
            kind: *kind,
        })?;
        maybe_checkpoint(inputs.options, report, progress)?;
    }

    let mut pending = Vec::with_capacity(IMPORT_OBJECT_BATCH_MAX_COUNT);
    let mut pending_bytes = 0usize;
    let mut fallback_requests = inputs
        .newly_trusted
        .iter()
        .filter(|(oid, _)| !covered.contains(oid))
        .map(|(oid, _)| {
            let working_set = native
                .object_metadata(oid)
                .map(|metadata| metadata.decode_working_set);
            (*oid, working_set)
        })
        .collect::<VecDeque<_>>();
    while !fallback_requests.is_empty() {
        let fallback_objects = read_source_object_wave(
            inputs.source.odb.clone(),
            &mut fallback_requests,
            execution,
            pool,
        )
        .await?;
        for (oid, object) in fallback_objects {
            let object = object?;
            report
                .metrics
                .source
                .decoded
                .record(object.kind, usize_to_u64(object.data.len()));
            let stored = StoredObject::new(object.kind, object.data);
            let payload_bytes = stored.data.len();
            let exceeds_byte_limit =
                pending_bytes.saturating_add(payload_bytes) > IMPORT_OBJECT_BATCH_MAX_BYTES;
            if !pending.is_empty()
                && (pending.len() >= IMPORT_OBJECT_BATCH_MAX_COUNT || exceeds_byte_limit)
            {
                flush_imported_objects(
                    destination,
                    &mut pending,
                    &mut pending_bytes,
                    inputs.options,
                    report,
                    progress,
                )
                .await?;
            }
            pending_bytes = pending_bytes.saturating_add(payload_bytes);
            pending.push((oid, stored));
            if pending.len() >= IMPORT_OBJECT_BATCH_MAX_COUNT
                || pending_bytes >= IMPORT_OBJECT_BATCH_MAX_BYTES
            {
                flush_imported_objects(
                    destination,
                    &mut pending,
                    &mut pending_bytes,
                    inputs.options,
                    report,
                    progress,
                )
                .await?;
            }
        }
    }
    flush_imported_objects(
        destination,
        &mut pending,
        &mut pending_bytes,
        inputs.options,
        report,
        progress,
    )
    .await?;

    let mut mirror_loose_complete = true;
    if inputs.options.native_pack_import == NativePackImportMode::FullMirror {
        let newly_reachable = inputs
            .newly_trusted
            .iter()
            .map(|(oid, _)| *oid)
            .collect::<HashSet<_>>();
        let mut mirror_batch = Vec::with_capacity(IMPORT_OBJECT_BATCH_MAX_COUNT);
        let mut mirror_bytes = 0usize;
        let mut mirror_requests = native
            .loose_oids
            .iter()
            .filter(|oid| !covered.contains(oid) && !newly_reachable.contains(oid))
            .map(|oid| (*oid, None))
            .collect::<VecDeque<_>>();
        while !mirror_requests.is_empty() {
            let mirror_objects = read_source_object_wave(
                inputs.source.odb.clone(),
                &mut mirror_requests,
                execution,
                pool,
            )
            .await?;
            for (oid, object) in mirror_objects {
                let object = match object {
                    Ok(object) => object,
                    Err(_) => {
                        mirror_loose_complete = false;
                        continue;
                    }
                };
                report
                    .metrics
                    .source
                    .decoded
                    .record(object.kind, usize_to_u64(object.data.len()));
                let stored = StoredObject::new(object.kind, object.data);
                let payload_bytes = stored.data.len();
                if !mirror_batch.is_empty()
                    && (mirror_batch.len() >= IMPORT_OBJECT_BATCH_MAX_COUNT
                        || mirror_bytes.saturating_add(payload_bytes)
                            > IMPORT_OBJECT_BATCH_MAX_BYTES)
                {
                    write_mirror_loose_batch(destination, &mut mirror_batch, report).await?;
                    mirror_bytes = 0;
                }
                mirror_bytes = mirror_bytes.saturating_add(payload_bytes);
                mirror_batch.push((oid, stored));
                if mirror_batch.len() >= IMPORT_OBJECT_BATCH_MAX_COUNT
                    || mirror_bytes >= IMPORT_OBJECT_BATCH_MAX_BYTES
                {
                    write_mirror_loose_batch(destination, &mut mirror_batch, report).await?;
                    mirror_bytes = 0;
                }
            }
        }
        write_mirror_loose_batch(destination, &mut mirror_batch, report).await?;
    }
    report.full_mirror_completed = inputs.options.native_pack_import
        == NativePackImportMode::FullMirror
        && native.full_mirror_compatible
        && all_candidates_installed
        && mirror_loose_complete;
    Ok(())
}

fn validate_native_pack_readability(
    source: &NativeSourcePack,
) -> (ImportObjectKindMetrics, Result<()>) {
    let mut metrics = ImportObjectKindMetrics::default();
    for entry in &source.index.entries {
        let validation = (|| {
            let oid = ObjectId::from_bytes(&entry.oid)?;
            let expected = source.object_metadata.get(&oid).copied().ok_or_else(|| {
                Error::Protocol("native pack object has no validated metadata".to_owned())
            })?;
            let object =
                pack::read_object_from_pack_bytes(&source.data, &source.index, &entry.oid)?;
            let decoded_size = u64::try_from(object.data.len())
                .map_err(|_| Error::Protocol("native pack object size exceeds u64".to_owned()))?;
            metrics.record(object.kind, decoded_size);
            if object.kind != expected.kind || decoded_size != expected.size {
                return Err(Error::Protocol(format!(
                    "source pack metadata does not match verified object {oid}"
                )));
            }
            Ok(())
        })();
        if let Err(error) = validation {
            return (metrics, Err(error));
        }
    }
    (metrics, Ok(()))
}

fn prepare_native_stored_pack(source: &NativeSourcePack) -> Result<ImportedPack> {
    let trailer_start = source
        .data
        .len()
        .checked_sub(source.index.hash_bytes)
        .ok_or_else(|| Error::Protocol("native pack trailer is truncated".to_owned()))?;
    let mut ordered_offsets = source
        .index
        .entries
        .iter()
        .map(|entry| entry.offset)
        .collect::<Vec<_>>();
    ordered_offsets.sort_unstable();
    let trailer_start = u64::try_from(trailer_start)
        .map_err(|_| Error::Protocol("native pack trailer offset exceeds u64".to_owned()))?;
    let mut stored_spans = HashMap::with_capacity(ordered_offsets.len());
    for (position, offset) in ordered_offsets.iter().enumerate() {
        let end = ordered_offsets
            .get(position + 1)
            .copied()
            .unwrap_or(trailer_start);
        let span = end
            .checked_sub(*offset)
            .ok_or_else(|| Error::Protocol("native pack offsets are out of order".to_owned()))?;
        stored_spans.insert(*offset, span);
    }
    let mut entries = Vec::with_capacity(source.index.entries.len());
    for entry in &source.index.entries {
        let oid = ObjectId::from_bytes(&entry.oid)?;
        let metadata = source.object_metadata.get(&oid).copied().ok_or_else(|| {
            Error::Protocol("native pack object has no validated metadata".to_owned())
        })?;
        let stored_span = stored_spans.get(&entry.offset).copied().ok_or_else(|| {
            Error::Protocol("native pack object has no stored byte span".to_owned())
        })?;
        entries.push(PackObjectIndex {
            oid,
            kind: metadata.kind,
            offset: entry.offset,
            size: metadata.size,
            compressed_size: stored_span,
        });
    }
    entries.sort_by_key(|entry| entry.offset);
    Ok(ImportedPack {
        metadata: source.metadata.clone(),
        data: Arc::clone(&source.data),
        index: entries,
    })
}

async fn write_mirror_loose_batch<S>(
    destination: &crate::repository::ServerRepository<S>,
    pending: &mut Vec<(ObjectId, StoredObject)>,
    report: &mut ImportReport,
) -> Result<()>
where
    S: ServerStorage,
{
    if pending.is_empty() {
        return Ok(());
    }
    let object_bytes = pending.iter().fold(0u64, |bytes, (_, object)| {
        bytes.saturating_add(usize_to_u64(object.data.len()))
    });
    report.loose_objects = report.loose_objects.saturating_add(pending.len());
    destination
        .storage()
        .write_imported_objects(
            destination.tenant(),
            destination.repository(),
            std::mem::take(pending),
        )
        .await?;
    report.metrics.destination.loose_object_write_batches = report
        .metrics
        .destination
        .loose_object_write_batches
        .saturating_add(1);
    report.metrics.destination.loose_object_write_bytes = report
        .metrics
        .destination
        .loose_object_write_bytes
        .saturating_add(object_bytes);
    Ok(())
}

fn compute_commit_generations(
    commits: &mut HashMap<ObjectId, IndexedCommit>,
    existing: &HashMap<ObjectId, IndexedCommit>,
) {
    let mut remaining_parents = HashMap::with_capacity(commits.len());
    let mut children = HashMap::<ObjectId, Vec<ObjectId>>::new();

    let new_oids = commits.keys().copied().collect::<HashSet<_>>();
    for commit in commits.values_mut() {
        commit.generation = commit
            .parents
            .iter()
            .filter(|parent| !new_oids.contains(parent))
            .filter_map(|parent| existing.get(parent))
            .map(|parent| parent.generation.saturating_add(1))
            .max()
            .unwrap_or(1);
    }
    for commit in commits.values() {
        let mut parent_count = 0usize;
        for parent in &commit.parents {
            if commits.contains_key(parent) {
                parent_count += 1;
                children.entry(*parent).or_default().push(commit.oid);
            }
        }
        remaining_parents.insert(commit.oid, parent_count);
    }

    let mut ready = remaining_parents
        .iter()
        .filter_map(|(oid, parent_count)| (*parent_count == 0).then_some(*oid))
        .collect::<VecDeque<_>>();
    while let Some(oid) = ready.pop_front() {
        let generation = commits
            .get(&oid)
            .map(|commit| commit.generation)
            .unwrap_or(1);
        for child_oid in children.remove(&oid).unwrap_or_default() {
            if let Some(child) = commits.get_mut(&child_oid) {
                child.generation = child.generation.max(generation.saturating_add(1));
            }
            if let Some(parent_count) = remaining_parents.get_mut(&child_oid) {
                *parent_count = (*parent_count).saturating_sub(1);
                if *parent_count == 0 {
                    ready.push_back(child_oid);
                }
            }
        }
    }
}

fn discover_source_inputs(git_dir: &std::path::Path) -> Result<SourceImportInputs> {
    let mut roots = Vec::new();
    let mut desired_refs = Vec::new();
    if let Ok(Some(target)) = refs::read_symbolic_ref(git_dir, "HEAD") {
        desired_refs.push(("HEAD".to_owned(), StoredRef::Symbolic(target)));
    } else if let Ok(oid) = refs::resolve_ref(git_dir, "HEAD") {
        desired_refs.push(("HEAD".to_owned(), StoredRef::Direct(oid)));
        roots.push(ImportWork::object(oid));
    }
    for (name, oid) in refs::list_refs(git_dir, "refs/")? {
        desired_refs.push((name, StoredRef::Direct(oid)));
        roots.push(ImportWork::object(oid));
    }
    let config = ConfigSet::load_repo_local_only(git_dir)?;
    let config_entries = config
        .entries()
        .iter()
        .filter_map(|entry| {
            entry
                .value
                .as_ref()
                .map(|value| (entry.key.clone(), value.clone()))
        })
        .collect();
    Ok(SourceImportInputs {
        roots,
        desired_refs,
        config_entries,
    })
}

async fn resolve_pending_tree_sizes<S>(
    destination: &crate::repository::ServerRepository<S>,
    tree_entries: &mut [IndexedTreeEntry],
    pending: HashMap<ObjectId, Vec<usize>>,
) -> Result<()>
where
    S: ServerStorage,
{
    for (oid, entry_indexes) in pending {
        let object = destination
            .read_object(&oid)
            .await?
            .ok_or_else(|| Error::ObjectNotFound(oid.to_hex()))?;
        let size = object.data.len() as u64;
        for entry_index in entry_indexes {
            if let Some(entry) = tree_entries.get_mut(entry_index) {
                entry.size = Some(size);
            }
        }
    }
    Ok(())
}

async fn flush_imported_objects<S, P>(
    destination: &crate::repository::ServerRepository<S>,
    pending: &mut Vec<(ObjectId, StoredObject)>,
    pending_bytes: &mut usize,
    options: &ImportOptions,
    report: &mut ImportReport,
    progress: &mut P,
) -> Result<()>
where
    S: ServerStorage,
    P: FnMut(ImportProgressEvent) -> Result<()>,
{
    if pending.is_empty() {
        return Ok(());
    }

    let events = pending
        .iter()
        .map(|(oid, object)| (*oid, object.kind))
        .collect::<Vec<_>>();
    let object_bytes = pending.iter().fold(0u64, |bytes, (_, object)| {
        bytes.saturating_add(usize_to_u64(object.data.len()))
    });
    destination
        .storage()
        .write_imported_objects(
            destination.tenant(),
            destination.repository(),
            std::mem::take(pending),
        )
        .await?;
    *pending_bytes = 0;
    report.loose_objects = report.loose_objects.saturating_add(events.len());
    report.metrics.destination.loose_object_write_batches = report
        .metrics
        .destination
        .loose_object_write_batches
        .saturating_add(1);
    report.metrics.destination.loose_object_write_bytes = report
        .metrics
        .destination
        .loose_object_write_bytes
        .saturating_add(object_bytes);

    for (oid, kind) in events {
        report.objects += 1;
        progress(ImportProgressEvent::Object { oid, kind })?;
        maybe_checkpoint(options, report, progress)?;
    }
    Ok(())
}

fn maybe_checkpoint(
    options: &ImportOptions,
    report: &mut ImportReport,
    progress: &mut impl FnMut(ImportProgressEvent) -> Result<()>,
) -> Result<()> {
    if options.checkpoint_interval == 0
        || !report.objects.is_multiple_of(options.checkpoint_interval)
    {
        return Ok(());
    }
    let checkpoint = ImportCheckpoint {
        objects: report.objects,
        refs: report.refs,
        tree_entries: report.tree_entries,
    };
    report.checkpoints.push(checkpoint.clone());
    progress(ImportProgressEvent::Checkpoint(checkpoint))
}

fn start_import_phase(
    progress: &mut impl FnMut(ImportProgressEvent) -> Result<()>,
    phase: ImportPhase,
) -> Result<()> {
    progress(ImportProgressEvent::PhaseStarted(phase))
}

fn complete_import_phase(
    progress: &mut impl FnMut(ImportProgressEvent) -> Result<()>,
    phase: ImportPhase,
    report: &ImportReport,
) -> Result<()> {
    progress(ImportProgressEvent::PhaseCompleted {
        phase,
        metrics: report.metrics.clone(),
    })
}

fn usize_to_u64(value: usize) -> u64 {
    match u64::try_from(value) {
        Ok(value) => value,
        Err(_) => u64::MAX,
    }
}

fn merge_parsed_tree(
    tree_oid: ObjectId,
    entries: Vec<ParsedTreeEntry>,
    stack: &mut Vec<ImportWork>,
    indexed: &mut Vec<IndexedTreeEntry>,
    object_sizes: &HashMap<ObjectId, u64>,
    pending_size_entries: &mut HashMap<ObjectId, Vec<usize>>,
) -> usize {
    let initial_len = indexed.len();
    for entry in entries {
        let size = if entry.kind == ObjectKind::Blob {
            object_sizes.get(&entry.oid).copied()
        } else {
            None
        };
        let entry_index = indexed.len();
        indexed.push(IndexedTreeEntry {
            tree_oid,
            path: entry.name,
            mode: entry.mode,
            oid: entry.oid,
            kind: entry.kind,
            size,
        });
        if entry.kind == ObjectKind::Blob && size.is_none() {
            pending_size_entries
                .entry(entry.oid)
                .or_default()
                .push(entry_index);
        }
        // Gitlinks name commits from another repository. Their object IDs are not
        // required (or generally available) in the superproject object database.
        if entry.mode != 0o160000 {
            stack.push(ImportWork::expected(entry.oid, entry.kind));
        }
    }
    indexed.len() - initial_len
}

fn kind_for_mode(mode: u32) -> ObjectKind {
    match mode {
        0o040000 => ObjectKind::Tree,
        0o160000 => ObjectKind::Commit,
        _ => ObjectKind::Blob,
    }
}
