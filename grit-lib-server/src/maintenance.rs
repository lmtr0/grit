//! Maintenance, repair, and migration utilities for server-backed repositories.

use std::collections::{BTreeMap, HashSet};
use std::fs;
use std::io::Write as _;
use std::path::Path;

use flate2::write::ZlibEncoder;
use flate2::Compression;
use grit_lib::objects::{parse_commit, parse_tag, parse_tree, ObjectId, ObjectKind};

use crate::error::{Error, Result};
use crate::repository::ServerRepository;
use crate::storage::{IndexedTreeEntry, ServerStorage, StoredObject, StoredRef};

/// Result of a repository consistency check.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ConsistencyReport {
    /// Number of refs checked.
    pub checked_refs: usize,
    /// Number of objects checked.
    pub checked_objects: usize,
    /// Number of indexed commits checked.
    pub checked_commits: usize,
    /// Number of browse-index entries checked.
    pub checked_browse_entries: usize,
    /// Consistency issues found.
    pub issues: Vec<ConsistencyIssue>,
}

impl ConsistencyReport {
    /// Return whether the repository passed every consistency check.
    #[must_use]
    pub fn is_clean(&self) -> bool {
        self.issues.is_empty()
    }
}

/// One repository consistency issue.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum ConsistencyIssue {
    /// A direct ref points at an object that is not stored.
    MissingRefTarget {
        /// Ref name with the missing target.
        refname: String,
        /// Missing object id.
        target: ObjectId,
    },
    /// A symbolic ref points at another ref that is not stored.
    DanglingSymbolicRef {
        /// Symbolic ref name.
        refname: String,
        /// Missing symbolic target.
        target: String,
    },
    /// A stored object hashes to a different id than its storage key.
    ObjectHashMismatch {
        /// Object id used as the storage key.
        expected: ObjectId,
        /// Object id computed from the stored payload.
        actual: ObjectId,
    },
    /// A commit names a tree object that is not stored.
    MissingCommitTree {
        /// Commit object id.
        commit: ObjectId,
        /// Missing tree object id.
        tree: ObjectId,
    },
    /// A commit names a parent commit that is not stored.
    MissingCommitParent {
        /// Commit object id.
        commit: ObjectId,
        /// Missing parent commit id.
        parent: ObjectId,
    },
    /// A tree entry names an object that is not stored.
    MissingTreeEntryObject {
        /// Tree object id.
        tree: ObjectId,
        /// Path relative to the checked root.
        path: String,
        /// Missing object id.
        oid: ObjectId,
    },
    /// A tag names an object that is not stored.
    MissingTagTarget {
        /// Tag object id.
        tag: ObjectId,
        /// Missing target object id.
        target: ObjectId,
    },
    /// A commit-graph row names a commit object that is not stored.
    MissingIndexedCommitObject {
        /// Missing commit object id.
        commit: ObjectId,
    },
    /// A browse-index row names an object that is not stored.
    MissingBrowseIndexObject {
        /// Root tree used by the browse index.
        tree: ObjectId,
        /// Indexed path.
        path: String,
        /// Missing object id.
        oid: ObjectId,
    },
}

/// Options for exporting a server-backed repository to a filesystem Git directory.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ExportOptions {
    /// Replace files that already exist at the export target.
    pub overwrite_existing: bool,
}

/// Summary of a filesystem export.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ExportReport {
    /// Number of loose objects written or already present.
    pub objects: usize,
    /// Number of refs written.
    pub refs: usize,
    /// Number of config entries written.
    pub config_entries: usize,
}

/// Rebuild the browse index from stored commit and tree objects.
///
/// # Errors
///
/// Returns backend errors or object parsing errors.
pub async fn repair_browse_index<S>(repo: &ServerRepository<S>) -> Result<usize>
where
    S: ServerStorage,
{
    let entries = build_browse_index(repo).await?;
    let count = entries.len();
    repo.storage()
        .replace_tree_entries(repo.tenant(), repo.repository(), &entries)
        .await?;
    Ok(count)
}

/// Check refs, stored objects, and repairable indexes for missing objects.
///
/// # Errors
///
/// Returns backend errors or object parsing errors.
pub async fn check_repository_consistency<S>(
    repo: &ServerRepository<S>,
) -> Result<ConsistencyReport>
where
    S: ServerStorage,
{
    let mut report = ConsistencyReport::default();
    check_refs(repo, &mut report).await?;
    check_objects(repo, &mut report).await?;
    check_commit_graph(repo, &mut report).await?;
    check_browse_index(repo, &mut report).await?;
    Ok(report)
}

/// Export stored refs, config, and loose objects to `git_dir`.
///
/// The export writes the filesystem layout used by a bare Git directory: `HEAD`, `config`, `refs`,
/// and zlib-compressed loose objects under `objects`. Pack storage is intentionally not produced by
/// this Slice 8 migration path.
///
/// # Errors
///
/// Returns backend errors, filesystem I/O errors, compression errors, or an error if the target
/// already contains repository files and overwriting is disabled.
pub async fn export_repository<S>(
    repo: &ServerRepository<S>,
    git_dir: impl AsRef<Path>,
    options: ExportOptions,
) -> Result<ExportReport>
where
    S: ServerStorage,
{
    let git_dir = git_dir.as_ref();
    ensure_export_target(git_dir, options.overwrite_existing)?;
    fs::create_dir_all(git_dir.join("objects/info"))?;
    fs::create_dir_all(git_dir.join("objects/pack"))?;
    fs::create_dir_all(git_dir.join("refs/heads"))?;
    fs::create_dir_all(git_dir.join("refs/tags"))?;

    let mut report = ExportReport::default();
    for (oid, _) in repo
        .storage()
        .list_object_ids(repo.tenant(), repo.repository(), None)
        .await?
    {
        let object = repo
            .read_object(&oid)
            .await?
            .ok_or_else(|| Error::ObjectNotFound(oid.to_hex()))?;
        let computed = object.object_id(repo.hash_algo());
        if computed != oid {
            return Err(Error::Backend(format!(
                "stored object {} hashes to {}",
                oid.to_hex(),
                computed.to_hex()
            )));
        }
        write_loose_object(git_dir, &oid, &object, options.overwrite_existing)?;
        report.objects += 1;
    }

    for (refname, value) in repo.list_refs("").await? {
        write_exported_ref(git_dir, &refname, &value, options.overwrite_existing)?;
        report.refs += 1;
    }

    let config = repo
        .storage()
        .list_config(repo.tenant(), repo.repository(), "")
        .await?;
    write_exported_config(git_dir, &config, options.overwrite_existing)?;
    report.config_entries = config.len();
    Ok(report)
}

async fn check_refs<S>(repo: &ServerRepository<S>, report: &mut ConsistencyReport) -> Result<()>
where
    S: ServerStorage,
{
    for (refname, value) in repo.list_refs("").await? {
        report.checked_refs += 1;
        match value {
            StoredRef::Direct(target) => {
                if !repo
                    .storage()
                    .object_exists(repo.tenant(), repo.repository(), &target)
                    .await?
                {
                    report
                        .issues
                        .push(ConsistencyIssue::MissingRefTarget { refname, target });
                }
            }
            StoredRef::Symbolic(target) => {
                if repo.read_ref(&target).await?.is_none() {
                    report
                        .issues
                        .push(ConsistencyIssue::DanglingSymbolicRef { refname, target });
                }
            }
        }
    }
    Ok(())
}

async fn check_objects<S>(repo: &ServerRepository<S>, report: &mut ConsistencyReport) -> Result<()>
where
    S: ServerStorage,
{
    for (oid, _) in repo
        .storage()
        .list_object_ids(repo.tenant(), repo.repository(), None)
        .await?
    {
        let Some(object) = repo.read_object(&oid).await? else {
            continue;
        };
        report.checked_objects += 1;
        let computed = object.object_id(repo.hash_algo());
        if computed != oid {
            report.issues.push(ConsistencyIssue::ObjectHashMismatch {
                expected: oid,
                actual: computed,
            });
        }
        match object.kind {
            ObjectKind::Commit => {
                let commit = parse_commit(&object.data)?;
                if !repo
                    .storage()
                    .object_exists(repo.tenant(), repo.repository(), &commit.tree)
                    .await?
                {
                    report.issues.push(ConsistencyIssue::MissingCommitTree {
                        commit: oid,
                        tree: commit.tree,
                    });
                }
                for parent in commit.parents {
                    if !repo
                        .storage()
                        .object_exists(repo.tenant(), repo.repository(), &parent)
                        .await?
                    {
                        report.issues.push(ConsistencyIssue::MissingCommitParent {
                            commit: oid,
                            parent,
                        });
                    }
                }
            }
            ObjectKind::Tree => {
                for entry in parse_tree(&object.data)? {
                    if !repo
                        .storage()
                        .object_exists(repo.tenant(), repo.repository(), &entry.oid)
                        .await?
                    {
                        let path = String::from_utf8_lossy(&entry.name).into_owned();
                        report
                            .issues
                            .push(ConsistencyIssue::MissingTreeEntryObject {
                                tree: oid,
                                path,
                                oid: entry.oid,
                            });
                    }
                }
            }
            ObjectKind::Tag => {
                let tag = parse_tag(&object.data)?;
                if !repo
                    .storage()
                    .object_exists(repo.tenant(), repo.repository(), &tag.object)
                    .await?
                {
                    report.issues.push(ConsistencyIssue::MissingTagTarget {
                        tag: oid,
                        target: tag.object,
                    });
                }
            }
            ObjectKind::Blob => {}
        }
    }
    Ok(())
}

async fn check_commit_graph<S>(
    repo: &ServerRepository<S>,
    report: &mut ConsistencyReport,
) -> Result<()>
where
    S: ServerStorage,
{
    for commit in repo
        .storage()
        .list_indexed_commits(repo.tenant(), repo.repository())
        .await?
    {
        report.checked_commits += 1;
        if !repo
            .storage()
            .object_exists(repo.tenant(), repo.repository(), &commit.oid)
            .await?
        {
            report
                .issues
                .push(ConsistencyIssue::MissingIndexedCommitObject { commit: commit.oid });
        }
    }
    Ok(())
}

async fn check_browse_index<S>(
    repo: &ServerRepository<S>,
    report: &mut ConsistencyReport,
) -> Result<()>
where
    S: ServerStorage,
{
    for (tree_oid, _) in repo
        .storage()
        .list_object_ids(repo.tenant(), repo.repository(), Some(ObjectKind::Tree))
        .await?
    {
        for entry in repo
            .storage()
            .list_tree_entries(repo.tenant(), repo.repository(), &tree_oid, "")
            .await?
        {
            report.checked_browse_entries += 1;
            if !repo
                .storage()
                .object_exists(repo.tenant(), repo.repository(), &entry.oid)
                .await?
            {
                report
                    .issues
                    .push(ConsistencyIssue::MissingBrowseIndexObject {
                        tree: entry.tree_oid,
                        path: entry.path,
                        oid: entry.oid,
                    });
            }
        }
    }
    Ok(())
}

async fn build_browse_index<S>(repo: &ServerRepository<S>) -> Result<Vec<IndexedTreeEntry>>
where
    S: ServerStorage,
{
    let mut roots = HashSet::new();
    for (oid, _) in repo
        .storage()
        .list_object_ids(repo.tenant(), repo.repository(), Some(ObjectKind::Commit))
        .await?
    {
        let Some(object) = repo.read_object(&oid).await? else {
            continue;
        };
        let commit = parse_commit(&object.data)?;
        roots.insert(commit.tree);
    }
    for (oid, _) in repo
        .storage()
        .list_object_ids(repo.tenant(), repo.repository(), Some(ObjectKind::Tree))
        .await?
    {
        roots.insert(oid);
    }

    let mut entries = Vec::new();
    for root in roots {
        append_browse_entries(repo, root, &mut entries).await?;
    }
    entries.sort_by(|left, right| {
        left.tree_oid
            .cmp(&right.tree_oid)
            .then_with(|| left.path.cmp(&right.path))
    });
    entries.dedup_by(|left, right| left.tree_oid == right.tree_oid && left.path == right.path);
    Ok(entries)
}

async fn append_browse_entries<S>(
    repo: &ServerRepository<S>,
    root: ObjectId,
    entries: &mut Vec<IndexedTreeEntry>,
) -> Result<()>
where
    S: ServerStorage,
{
    let mut stack = vec![(root, String::new())];
    while let Some((tree_oid, prefix)) = stack.pop() {
        let object = repo
            .read_object(&tree_oid)
            .await?
            .ok_or_else(|| Error::ObjectNotFound(tree_oid.to_hex()))?;
        if object.kind != ObjectKind::Tree {
            return Err(Error::UnexpectedObjectKind {
                expected: "tree",
                actual: object_kind_name(object.kind),
            });
        }
        for entry in parse_tree(&object.data)? {
            let name = String::from_utf8_lossy(&entry.name).into_owned();
            let path = if prefix.is_empty() {
                name
            } else {
                format!("{prefix}/{name}")
            };
            let kind = kind_for_mode(entry.mode);
            let size = if kind == ObjectKind::Blob {
                repo.read_object(&entry.oid)
                    .await?
                    .map(|object| object.data.len() as u64)
            } else {
                None
            };
            entries.push(IndexedTreeEntry {
                tree_oid: root,
                path: path.clone(),
                mode: entry.mode,
                oid: entry.oid,
                kind,
                size,
            });
            if kind == ObjectKind::Tree {
                stack.push((entry.oid, path));
            }
        }
    }
    Ok(())
}

fn ensure_export_target(git_dir: &Path, overwrite_existing: bool) -> Result<()> {
    if overwrite_existing {
        return Ok(());
    }
    for path in ["HEAD", "config", "objects", "refs"] {
        if git_dir.join(path).exists() {
            return Err(Error::Backend(format!(
                "export target '{}' already contains {path}",
                git_dir.display()
            )));
        }
    }
    Ok(())
}

fn write_loose_object(
    git_dir: &Path,
    oid: &ObjectId,
    object: &StoredObject,
    overwrite_existing: bool,
) -> Result<()> {
    let path = git_dir
        .join("objects")
        .join(oid.loose_prefix())
        .join(oid.loose_suffix());
    if path.exists() && !overwrite_existing {
        return Ok(());
    }
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let mut store_bytes = Vec::new();
    store_bytes.extend_from_slice(object_kind_name(object.kind).as_bytes());
    store_bytes.push(b' ');
    store_bytes.extend_from_slice(object.data.len().to_string().as_bytes());
    store_bytes.push(0);
    store_bytes.extend_from_slice(&object.data);

    let tmp_path = path.with_extension("tmp");
    {
        let tmp_file = fs::File::create(&tmp_path)?;
        let mut encoder = ZlibEncoder::new(tmp_file, Compression::default());
        encoder
            .write_all(&store_bytes)
            .map_err(|err| Error::Backend(format!("compress loose object: {err}")))?;
        encoder
            .finish()
            .map_err(|err| Error::Backend(format!("finish loose object compression: {err}")))?;
    }
    fs::rename(tmp_path, path)?;
    Ok(())
}

fn write_exported_ref(
    git_dir: &Path,
    refname: &str,
    value: &StoredRef,
    overwrite_existing: bool,
) -> Result<()> {
    let path = git_dir.join(refname);
    if path.exists() && !overwrite_existing {
        return Err(Error::Backend(format!(
            "export ref '{refname}' already exists"
        )));
    }
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let body = match value {
        StoredRef::Direct(oid) => format!("{}\n", oid.to_hex()),
        StoredRef::Symbolic(target) => format!("ref: {target}\n"),
    };
    fs::write(path, body)?;
    Ok(())
}

fn write_exported_config(
    git_dir: &Path,
    entries: &[(String, String)],
    overwrite_existing: bool,
) -> Result<()> {
    let path = git_dir.join("config");
    if path.exists() && !overwrite_existing {
        return Err(Error::Backend("export config already exists".to_owned()));
    }
    let mut sections = BTreeMap::<String, Vec<(String, String)>>::new();
    for (key, value) in entries {
        let (section, name) = key.rsplit_once('.').map_or_else(
            || ("core".to_owned(), key.clone()),
            |(section, name)| (section.to_owned(), name.to_owned()),
        );
        sections
            .entry(section)
            .or_default()
            .push((name, value.clone()));
    }

    let mut content = String::new();
    for (section, values) in sections {
        content.push('[');
        content.push_str(&section);
        content.push_str("]\n");
        for (name, value) in values {
            content.push('\t');
            content.push_str(&name);
            content.push_str(" = ");
            content.push_str(&value);
            content.push('\n');
        }
    }
    fs::write(path, content)?;
    Ok(())
}

fn kind_for_mode(mode: u32) -> ObjectKind {
    match mode {
        0o040000 => ObjectKind::Tree,
        _ => ObjectKind::Blob,
    }
}

fn object_kind_name(kind: ObjectKind) -> &'static str {
    match kind {
        ObjectKind::Blob => "blob",
        ObjectKind::Tree => "tree",
        ObjectKind::Commit => "commit",
        ObjectKind::Tag => "tag",
    }
}
