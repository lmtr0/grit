//! Import filesystem-backed repositories into server storage.

use std::collections::HashSet;

use grit_lib::config::ConfigSet;
use grit_lib::objects::{parse_commit, parse_tag, parse_tree, ObjectId, ObjectKind};
use grit_lib::refs;
use grit_lib::repo::Repository;

use crate::error::{Error, Result};
use crate::storage::{IndexedTreeEntry, ServerStorage, StoredObject, StoredRef};

enum ImportWork {
    Object(ObjectId),
    Tree {
        oid: ObjectId,
        root: ObjectId,
        prefix: String,
    },
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
    /// Number of tree entries indexed.
    pub tree_entries: usize,
    /// Number of commit graph rows rebuilt from imported commits.
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
        /// Root tree whose browse entries were indexed.
        tree_oid: ObjectId,
        /// Number of entries indexed in this batch.
        entries: usize,
    },
    /// A progress checkpoint was reached.
    Checkpoint(ImportCheckpoint),
    /// Import completed.
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
    let mut report = ImportReport {
        config_entries: import_config(destination, source, &mut progress).await?,
        ..ImportReport::default()
    };
    let mut roots = Vec::new();
    let mut imported_refs = HashSet::new();

    if let Ok(Some(target)) = refs::read_symbolic_ref(&source.git_dir, "HEAD") {
        destination
            .storage()
            .write_ref(
                destination.tenant(),
                destination.repository(),
                "HEAD",
                &StoredRef::Symbolic(target),
                None,
            )
            .await?;
        imported_refs.insert("HEAD".to_owned());
        report.refs += 1;
        progress(ImportProgressEvent::Ref {
            refname: "HEAD".to_owned(),
        })?;
    } else if let Ok(oid) = refs::resolve_ref(&source.git_dir, "HEAD") {
        destination
            .storage()
            .write_ref(
                destination.tenant(),
                destination.repository(),
                "HEAD",
                &StoredRef::Direct(oid),
                None,
            )
            .await?;
        imported_refs.insert("HEAD".to_owned());
        roots.push(ImportWork::Object(oid));
        report.refs += 1;
        progress(ImportProgressEvent::Ref {
            refname: "HEAD".to_owned(),
        })?;
    }

    for (name, oid) in refs::list_refs(&source.git_dir, "refs/")? {
        destination
            .storage()
            .write_ref(
                destination.tenant(),
                destination.repository(),
                &name,
                &StoredRef::Direct(oid),
                None,
            )
            .await?;
        imported_refs.insert(name.clone());
        roots.push(ImportWork::Object(oid));
        report.refs += 1;
        progress(ImportProgressEvent::Ref { refname: name })?;
    }

    if options.prune_deleted_refs {
        for (refname, _) in destination.list_refs("").await? {
            if imported_refs.contains(&refname) {
                continue;
            }
            destination
                .storage()
                .delete_ref(
                    destination.tenant(),
                    destination.repository(),
                    &refname,
                    None,
                )
                .await?;
            report.pruned_refs += 1;
            progress(ImportProgressEvent::PrunedRef { refname })?;
        }
    }

    let mut seen = HashSet::new();
    let mut indexed_trees = HashSet::new();
    let mut stack = roots;
    while let Some(work) = stack.pop() {
        let (oid, tree_context) = match work {
            ImportWork::Object(oid) => (oid, None),
            ImportWork::Tree { oid, root, prefix } => (oid, Some((root, prefix))),
        };
        let first_object_visit = seen.insert(oid);
        if !first_object_visit && tree_context.is_none() {
            continue;
        }
        let object = source.odb.read(&oid)?;
        if first_object_visit {
            let stored = StoredObject::new(object.kind, object.data.clone());
            destination
                .storage()
                .write_object(
                    destination.tenant(),
                    destination.repository(),
                    &oid,
                    &stored,
                )
                .await?;
            report.objects += 1;
            progress(ImportProgressEvent::Object {
                oid,
                kind: object.kind,
            })?;
            maybe_checkpoint(&options, &mut report, &mut progress)?;
        }

        match object.kind {
            ObjectKind::Commit => {
                let commit = parse_commit(&object.data)?;
                stack.push(ImportWork::Tree {
                    oid: commit.tree,
                    root: commit.tree,
                    prefix: String::new(),
                });
                stack.extend(commit.parents.into_iter().map(ImportWork::Object));
            }
            ObjectKind::Tree => {
                let (root, prefix) = tree_context.unwrap_or((oid, String::new()));
                if !indexed_trees.insert((root, oid, prefix.clone())) {
                    continue;
                }
                let indexed =
                    index_tree(destination, source, root, &prefix, &object.data, &mut stack)
                        .await?;
                report.tree_entries += indexed;
                progress(ImportProgressEvent::TreeEntries {
                    tree_oid: root,
                    entries: indexed,
                })?;
            }
            ObjectKind::Tag => {
                let tag = parse_tag(&object.data)?;
                stack.push(ImportWork::Object(tag.object));
            }
            ObjectKind::Blob => {}
        }
    }

    report.commit_graph_entries = destination.repair_commit_graph().await?;
    progress(ImportProgressEvent::Completed(report.clone()))?;
    Ok(report)
}

async fn import_config<S>(
    destination: &crate::repository::ServerRepository<S>,
    source: &Repository,
    progress: &mut impl FnMut(ImportProgressEvent) -> Result<()>,
) -> Result<usize>
where
    S: ServerStorage,
{
    let config = ConfigSet::load_repo_local_only(&source.git_dir)?;
    let mut count = 0;
    for entry in config.entries() {
        let Some(value) = entry.value.as_deref() else {
            continue;
        };
        destination
            .storage()
            .set_config(
                destination.tenant(),
                destination.repository(),
                &entry.key,
                value,
            )
            .await?;
        progress(ImportProgressEvent::ConfigEntry {
            key: entry.key.clone(),
        })?;
        count += 1;
    }
    Ok(count)
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

async fn index_tree<S>(
    destination: &crate::repository::ServerRepository<S>,
    source: &Repository,
    root_tree_oid: ObjectId,
    prefix: &str,
    data: &[u8],
    stack: &mut Vec<ImportWork>,
) -> Result<usize>
where
    S: ServerStorage,
{
    let mut entries = Vec::new();
    for entry in parse_tree(data)? {
        let name = String::from_utf8(entry.name)
            .map_err(|_| Error::PathNotFound("tree entry name is not UTF-8".to_owned()))?;
        let path = if prefix.is_empty() {
            name
        } else {
            format!("{prefix}/{name}")
        };
        let kind = kind_for_mode(entry.mode);
        let size = if kind == ObjectKind::Blob {
            source
                .odb
                .read(&entry.oid)
                .ok()
                .map(|object| object.data.len() as u64)
        } else {
            None
        };
        entries.push(IndexedTreeEntry {
            tree_oid: root_tree_oid,
            path,
            mode: entry.mode,
            oid: entry.oid,
            kind,
            size,
        });
        match kind {
            ObjectKind::Tree => stack.push(ImportWork::Tree {
                oid: entry.oid,
                root: root_tree_oid,
                prefix: entries
                    .last()
                    .map(|entry| entry.path.clone())
                    .unwrap_or_default(),
            }),
            _ => stack.push(ImportWork::Object(entry.oid)),
        }
    }
    let count = entries.len();
    destination
        .storage()
        .upsert_tree_entries(destination.tenant(), destination.repository(), &entries)
        .await?;
    Ok(count)
}

fn kind_for_mode(mode: u32) -> ObjectKind {
    match mode {
        0o040000 => ObjectKind::Tree,
        _ => ObjectKind::Blob,
    }
}
