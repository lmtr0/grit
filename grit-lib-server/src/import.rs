//! Import filesystem-backed repositories into server storage.

use std::collections::{HashMap, HashSet, VecDeque};

use grit_lib::config::ConfigSet;
use grit_lib::objects::{parse_commit, parse_tag, parse_tree, ObjectId, ObjectKind};
use grit_lib::refs;
use grit_lib::repo::Repository;

use crate::error::{Error, Result};
use crate::storage::{
    commit_time_from_identity, ImportPublication, IndexedCommit, IndexedTreeEntry, ServerStorage,
    StoredObject, StoredRef,
};

const IMPORT_OBJECT_BATCH_MAX_COUNT: usize = 2_000;
const IMPORT_OBJECT_BATCH_MAX_BYTES: usize = 32 * 1024 * 1024;

enum ImportWork {
    Object(ObjectId),
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
}

/// Options controlling repository import behavior.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ImportOptions {
    /// Remove destination refs that are absent from the filesystem-backed source.
    pub prune_deleted_refs: bool,
    /// Emit and store checkpoints every `checkpoint_interval` imported objects.
    pub checkpoint_interval: usize,
}

impl Default for ImportOptions {
    fn default() -> Self {
        Self {
            prune_deleted_refs: true,
            checkpoint_interval: 1_000,
        }
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
/// Returns errors from the source repository, destination storage, or object parsing.
pub async fn import_repository<S>(
    destination: &crate::repository::ServerRepository<S>,
    source: &Repository,
) -> Result<ImportReport>
where
    S: ServerStorage,
{
    import_repository_with_options(destination, source, ImportOptions::default(), |_| Ok(())).await
}

/// Import reachable repository data with explicit options and progress reporting.
///
/// Direct entries discovered during import are upserted by immutable tree object id. Existing
/// entries for stored objects outside the current reachable closure are preserved. Use
/// [`crate::maintenance::repair_browse_index`] to migrate or remove legacy flattened rows.
/// After a completed import, subsequent runs stop at object ids recorded in the durable trusted
/// manifest. A failed run leaves the repository incomplete, so its retry traverses the source
/// closure without trusting objects written by the partial run. Ref updates and pruning are
/// published only after object and query-index writes finish. Progress callbacks for final config,
/// ref, prune, and completion events run after guarded publication but before the trusted
/// completion marker; rejecting any such event leaves the import incomplete.
///
/// The `progress` callback receives events after durable writes complete. Returning an error from
/// the callback aborts the import and returns that error.
///
/// # Errors
///
/// Returns errors from the source repository, destination storage, progress callback, or object
/// parsing.
pub async fn import_repository_with_options<S, P>(
    destination: &crate::repository::ServerRepository<S>,
    source: &Repository,
    options: ImportOptions,
    mut progress: P,
) -> Result<ImportReport>
where
    S: ServerStorage,
    P: FnMut(ImportProgressEvent) -> Result<()>,
{
    let mut roots = Vec::new();
    let mut desired_refs = Vec::new();

    if let Ok(Some(target)) = refs::read_symbolic_ref(&source.git_dir, "HEAD") {
        desired_refs.push(("HEAD".to_owned(), StoredRef::Symbolic(target)));
    } else if let Ok(oid) = refs::resolve_ref(&source.git_dir, "HEAD") {
        desired_refs.push(("HEAD".to_owned(), StoredRef::Direct(oid)));
        roots.push(ImportWork::Object(oid));
    }

    for (name, oid) in refs::list_refs(&source.git_dir, "refs/")? {
        desired_refs.push((name, StoredRef::Direct(oid)));
        roots.push(ImportWork::Object(oid));
    }
    let config_entries = read_config(source)?;

    let session = destination
        .storage()
        .begin_import(destination.tenant(), destination.repository())
        .await?;
    let trusted_objects = session
        .trusted_objects()
        .iter()
        .map(|(oid, _)| *oid)
        .collect::<HashSet<_>>();
    let mut report = ImportReport::default();

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
    while let Some(work) = stack.pop() {
        let ImportWork::Object(oid) = work;
        if !seen.insert(oid) {
            continue;
        }
        if trusted_objects.contains(&oid) {
            continue;
        }
        let object = source.odb.read(&oid)?;
        let stored = StoredObject::new(object.kind, object.data);
        newly_trusted.push((oid, stored.kind));

        let object_size = stored.data.len() as u64;
        object_sizes.insert(oid, object_size);
        if let Some(entries) = pending_size_entries.remove(&oid) {
            for entry_index in entries {
                if let Some(entry) = tree_entries.get_mut(entry_index) {
                    entry.size = Some(object_size);
                }
            }
        }

        match stored.kind {
            ObjectKind::Commit => {
                let commit = parse_commit(&stored.data)?;
                stack.push(ImportWork::Object(commit.tree));
                stack.extend(commit.parents.iter().copied().map(ImportWork::Object));
                indexed_commits.insert(
                    oid,
                    IndexedCommit {
                        oid,
                        tree: commit.tree,
                        parents: commit.parents,
                        commit_time: commit_time_from_identity(&commit.committer),
                        generation: 1,
                    },
                );
            }
            ObjectKind::Tree => {
                let indexed = index_tree(
                    oid,
                    &stored.data,
                    &mut stack,
                    &mut tree_entries,
                    &object_sizes,
                    &mut pending_size_entries,
                )?;
                indexed_tree_batches.push((oid, indexed));
            }
            ObjectKind::Tag => {
                let tag = parse_tag(&stored.data)?;
                stack.push(ImportWork::Object(tag.object));
            }
            ObjectKind::Blob => {}
        }

        let payload_bytes = stored.data.len();
        let exceeds_byte_limit =
            pending_object_bytes.saturating_add(payload_bytes) > IMPORT_OBJECT_BATCH_MAX_BYTES;
        if !pending_objects.is_empty()
            && (pending_objects.len() >= IMPORT_OBJECT_BATCH_MAX_COUNT || exceeds_byte_limit)
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
    flush_imported_objects(
        destination,
        &mut pending_objects,
        &mut pending_object_bytes,
        &options,
        &mut report,
        &mut progress,
    )
    .await?;
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
    }
    report.tree_entries = tree_entries.len();
    for (tree_oid, entries) in indexed_tree_batches {
        progress(ImportProgressEvent::TreeEntries { tree_oid, entries })?;
    }

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
    }
    report.commit_graph_entries = indexed_commits.len();

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
    progress(ImportProgressEvent::Completed(report.clone()))?;
    destination
        .storage()
        .complete_import(destination.tenant(), destination.repository(), &session)
        .await?;
    Ok(report)
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

fn read_config(source: &Repository) -> Result<Vec<(String, String)>> {
    let config = ConfigSet::load_repo_local_only(&source.git_dir)?;
    Ok(config
        .entries()
        .iter()
        .filter_map(|entry| {
            entry
                .value
                .as_ref()
                .map(|value| (entry.key.clone(), value.clone()))
        })
        .collect())
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
    destination
        .storage()
        .write_imported_objects(
            destination.tenant(),
            destination.repository(),
            std::mem::take(pending),
        )
        .await?;
    *pending_bytes = 0;

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

fn index_tree(
    tree_oid: ObjectId,
    data: &[u8],
    stack: &mut Vec<ImportWork>,
    indexed: &mut Vec<IndexedTreeEntry>,
    object_sizes: &HashMap<ObjectId, u64>,
    pending_size_entries: &mut HashMap<ObjectId, Vec<usize>>,
) -> Result<usize> {
    let initial_len = indexed.len();
    for entry in parse_tree(data)? {
        let name = String::from_utf8(entry.name)
            .map_err(|_| Error::PathNotFound("tree entry name is not UTF-8".to_owned()))?;
        let kind = kind_for_mode(entry.mode);
        let size = if kind == ObjectKind::Blob {
            object_sizes.get(&entry.oid).copied()
        } else {
            None
        };
        let entry_index = indexed.len();
        indexed.push(IndexedTreeEntry {
            tree_oid,
            path: name,
            mode: entry.mode,
            oid: entry.oid,
            kind,
            size,
        });
        if kind == ObjectKind::Blob && size.is_none() {
            pending_size_entries
                .entry(entry.oid)
                .or_default()
                .push(entry_index);
        }
        stack.push(ImportWork::Object(entry.oid));
    }
    Ok(indexed.len() - initial_len)
}

fn kind_for_mode(mode: u32) -> ObjectKind {
    match mode {
        0o040000 => ObjectKind::Tree,
        _ => ObjectKind::Blob,
    }
}
