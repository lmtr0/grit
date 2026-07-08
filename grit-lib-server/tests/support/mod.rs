use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use grit_lib::config::ConfigSet;
use grit_lib::merge_base;
use grit_lib::objects::{
    parse_commit, parse_tag, parse_tree, serialize_commit, serialize_tag, serialize_tree,
    CommitData, HashAlgo, ObjectId, ObjectKind, TagData, TreeEntry,
};
use grit_lib::refs;
use grit_lib::repo::{init_repository, Repository};
use grit_lib_server::error::{Error, Result};
use grit_lib_server::ids::{RepositoryId, TenantId};
use grit_lib_server::import::import_repository;
use grit_lib_server::memory::MemoryBackend;
use grit_lib_server::repository::ServerRepository;
use grit_lib_server::storage::{StoredObject, StoredRef};
use grit_lib_server::views::{BlobView, CommitSummary, TreeEntryView, TreeView};

pub(crate) struct ConformanceFixture {
    pub(crate) _temp: tempfile::TempDir,
    pub(crate) source: Repository,
    pub(crate) server: ServerRepository<MemoryBackend>,
    pub(crate) base: ObjectId,
    pub(crate) left: ObjectId,
    pub(crate) right: ObjectId,
    pub(crate) merge: ObjectId,
    pub(crate) merge_tree: ObjectId,
    pub(crate) readme: ObjectId,
    pub(crate) script: ObjectId,
    pub(crate) annotated_tag: ObjectId,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RefSnapshot {
    pub(crate) head: StoredRef,
    pub(crate) refs: Vec<(String, StoredRef)>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ErrorClass {
    ObjectNotFound,
    RefNotFound,
    PathNotFound,
    UnexpectedObjectKind,
    Other,
}

pub(crate) fn server_error_class(error: &Error) -> ErrorClass {
    match error {
        Error::ObjectNotFound(_) => ErrorClass::ObjectNotFound,
        Error::RefNotFound(_) => ErrorClass::RefNotFound,
        Error::PathNotFound(_) => ErrorClass::PathNotFound,
        Error::UnexpectedObjectKind { .. } => ErrorClass::UnexpectedObjectKind,
        _ => ErrorClass::Other,
    }
}

pub(crate) fn server_result_class<T>(result: Result<T>) -> std::result::Result<T, ErrorClass> {
    result.map_err(|error| server_error_class(&error))
}

pub(crate) fn filesystem_error_class(error: &grit_lib::error::Error) -> ErrorClass {
    match error {
        grit_lib::error::Error::ObjectNotFound(_) => ErrorClass::ObjectNotFound,
        grit_lib::error::Error::InvalidRef(_) => ErrorClass::RefNotFound,
        grit_lib::error::Error::PathError(_) => ErrorClass::PathNotFound,
        _ => ErrorClass::Other,
    }
}

pub(crate) async fn create_conformance_fixture() -> Result<ConformanceFixture> {
    let temp = tempfile::tempdir().map_err(grit_lib::error::Error::from)?;
    let source = init_repository(temp.path(), false, "main", None, "files")?;
    refs::write_symbolic_ref(&source.git_dir, "HEAD", "refs/heads/main")?;
    std::fs::write(
        source.git_dir.join("config"),
        "[core]\n\trepositoryformatversion = 0\n\tfilemode = true\n[grit \"server\"]\n\tparity = yes\n",
    )?;

    let readme_base = source.odb.write(ObjectKind::Blob, b"base\n")?;
    let readme_left = source.odb.write(ObjectKind::Blob, b"left\n")?;
    let readme_merge = source.odb.write(ObjectKind::Blob, b"merge\n")?;
    let docs_blob = source.odb.write(ObjectKind::Blob, b"guide\n")?;
    let right_blob = source.odb.write(ObjectKind::Blob, b"right\n")?;
    let script = source
        .odb
        .write(ObjectKind::Blob, b"#!/bin/sh\necho hi\n")?;
    let license = source.odb.write(ObjectKind::Blob, b"MIT\n")?;
    let gitmodules = source
        .odb
        .write(ObjectKind::Blob, b"[submodule \"dep\"]\n\tpath = dep\n")?;
    let symlink = source.odb.write(ObjectKind::Blob, b"docs/guide.md")?;

    let base_tree = write_tree(
        &source,
        &[TreeEntry {
            mode: 0o100644,
            name: b"README.md".to_vec(),
            oid: readme_base,
        }],
    )?;
    let left_tree = write_tree(
        &source,
        &[TreeEntry {
            mode: 0o100644,
            name: b"README.md".to_vec(),
            oid: readme_left,
        }],
    )?;
    let right_tree = write_tree(
        &source,
        &[
            TreeEntry {
                mode: 0o100644,
                name: b"README.md".to_vec(),
                oid: readme_base,
            },
            TreeEntry {
                mode: 0o100644,
                name: b"right.txt".to_vec(),
                oid: right_blob,
            },
        ],
    )?;
    let docs_tree = write_tree(
        &source,
        &[TreeEntry {
            mode: 0o100644,
            name: b"guide.md".to_vec(),
            oid: docs_blob,
        }],
    )?;
    let bin_tree = write_tree(
        &source,
        &[TreeEntry {
            mode: 0o100755,
            name: b"run.sh".to_vec(),
            oid: script,
        }],
    )?;
    let merge_tree = write_tree(
        &source,
        &[
            TreeEntry {
                mode: 0o100644,
                name: b".gitmodules".to_vec(),
                oid: gitmodules,
            },
            TreeEntry {
                mode: 0o100644,
                name: b"LICENSE".to_vec(),
                oid: license,
            },
            TreeEntry {
                mode: 0o100644,
                name: b"README.md".to_vec(),
                oid: readme_merge,
            },
            TreeEntry {
                mode: 0o040000,
                name: b"bin".to_vec(),
                oid: bin_tree,
            },
            TreeEntry {
                mode: 0o040000,
                name: b"docs".to_vec(),
                oid: docs_tree,
            },
            TreeEntry {
                mode: 0o120000,
                name: b"guide-link".to_vec(),
                oid: symlink,
            },
            TreeEntry {
                mode: 0o100644,
                name: b"right.txt".to_vec(),
                oid: right_blob,
            },
        ],
    )?;

    let base = write_commit(&source, base_tree, Vec::new(), 1_700_000_000, "base")?;
    let left = write_commit(&source, left_tree, vec![base], 1_700_000_100, "left")?;
    let right = write_commit(&source, right_tree, vec![base], 1_700_000_200, "right")?;
    let merge = write_commit(
        &source,
        merge_tree,
        vec![left, right],
        1_700_000_300,
        "merge",
    )?;
    let tag = TagData {
        object: merge,
        object_type: "commit".to_owned(),
        tag: "v-merge".to_owned(),
        tagger: Some(identity(1_700_000_400)),
        message: "merge release\n".to_owned(),
    };
    let annotated_tag = source.odb.write(ObjectKind::Tag, &serialize_tag(&tag))?;

    refs::write_ref(&source.git_dir, "refs/heads/main", &merge)?;
    refs::write_ref(&source.git_dir, "refs/heads/left", &left)?;
    refs::write_ref(&source.git_dir, "refs/heads/right", &right)?;
    refs::write_ref(&source.git_dir, "refs/tags/v-base", &base)?;
    refs::write_ref(&source.git_dir, "refs/tags/v-merge", &annotated_tag)?;

    let backend = Arc::new(MemoryBackend::new());
    let server = ServerRepository::new(
        TenantId::new("parity-tenant")?,
        RepositoryId::new("parity-repo")?,
        HashAlgo::Sha1,
        backend,
    );
    import_repository(&server, &source).await?;

    Ok(ConformanceFixture {
        _temp: temp,
        source,
        server,
        base,
        left,
        right,
        merge,
        merge_tree,
        readme: readme_merge,
        script,
        annotated_tag,
    })
}

pub(crate) fn filesystem_ref_snapshot(source: &Repository) -> Result<RefSnapshot> {
    let head = match refs::read_symbolic_ref(&source.git_dir, "HEAD")? {
        Some(target) => StoredRef::Symbolic(target),
        None => StoredRef::Direct(refs::resolve_ref(&source.git_dir, "HEAD")?),
    };
    let refs = refs::list_refs(&source.git_dir, "refs/")?
        .into_iter()
        .map(|(name, oid)| (name, StoredRef::Direct(oid)))
        .collect();
    Ok(RefSnapshot { head, refs })
}

pub(crate) async fn server_ref_snapshot(
    server: &ServerRepository<MemoryBackend>,
) -> Result<RefSnapshot> {
    let head = server
        .read_ref("HEAD")
        .await?
        .ok_or_else(|| Error::RefNotFound("HEAD".to_owned()))?;
    Ok(RefSnapshot {
        head,
        refs: server.list_refs("refs/").await?,
    })
}

pub(crate) fn filesystem_config_value(source: &Repository, key: &str) -> Result<Option<String>> {
    Ok(ConfigSet::load_repo_local_only(&source.git_dir)?.get(key))
}

pub(crate) fn filesystem_object(source: &Repository, oid: &ObjectId) -> Result<StoredObject> {
    let object = source.odb.read(oid)?;
    Ok(StoredObject::new(object.kind, object.data))
}

pub(crate) fn filesystem_commit(source: &Repository, spec: &str) -> Result<CommitSummary> {
    let oid = resolve_object_or_ref(source, spec)?;
    peel_to_commit(source, oid)
}

pub(crate) fn filesystem_tree_at(
    source: &Repository,
    revision: &str,
    path: &str,
) -> Result<TreeView> {
    let root = resolve_tree_root(source, revision)?;
    let path = normalize_tree_path(path);
    let (oid, prefix) = if path.is_empty() {
        (root, String::new())
    } else {
        let entry =
            tree_entry(source, root, &path)?.ok_or_else(|| Error::PathNotFound(path.clone()))?;
        if entry.kind != ObjectKind::Tree {
            return Err(Error::UnexpectedObjectKind {
                expected: "tree",
                actual: object_kind_name(entry.kind),
            });
        }
        (entry.oid, format!("{path}/"))
    };
    let entries = list_tree_entries(source, oid, &prefix)?;
    Ok(TreeView {
        root,
        path,
        oid,
        entries,
    })
}

pub(crate) fn filesystem_blob_at(
    source: &Repository,
    revision: &str,
    path: &str,
) -> Result<BlobView> {
    let root = resolve_tree_root(source, revision)?;
    let path = normalize_tree_path(path);
    let entry =
        tree_entry(source, root, &path)?.ok_or_else(|| Error::PathNotFound(path.clone()))?;
    if entry.kind != ObjectKind::Blob {
        return Err(Error::UnexpectedObjectKind {
            expected: "blob",
            actual: object_kind_name(entry.kind),
        });
    }
    let object = source.odb.read(&entry.oid)?;
    Ok(BlobView {
        oid: entry.oid,
        path,
        mode: entry.mode,
        data: object.data,
    })
}

pub(crate) fn filesystem_history(source: &Repository, start: &str) -> Result<Vec<CommitSummary>> {
    let start = filesystem_commit(source, start)?;
    let mut indexed = HashMap::new();
    collect_commits(source, start.oid, &mut indexed)?;
    let mut seen = HashSet::new();
    let mut frontier = vec![start.oid];
    let mut walked = Vec::new();
    while !frontier.is_empty() {
        let index = best_frontier_index(&frontier, &indexed);
        let oid = frontier.swap_remove(index);
        if !seen.insert(oid) {
            continue;
        }
        walked.push(filesystem_commit(source, &oid.to_hex())?);
        let commit = indexed
            .get(&oid)
            .ok_or_else(|| Error::ObjectNotFound(oid.to_hex()))?;
        for parent in &commit.parents {
            if !seen.contains(parent) {
                frontier.push(*parent);
            }
        }
    }
    Ok(walked)
}

pub(crate) fn filesystem_is_ancestor(
    source: &Repository,
    ancestor: ObjectId,
    descendant: ObjectId,
) -> Result<bool> {
    Ok(merge_base::is_ancestor(source, ancestor, descendant)?)
}

pub(crate) fn filesystem_merge_base(
    source: &Repository,
    left: ObjectId,
    right: ObjectId,
) -> Result<Option<ObjectId>> {
    Ok(merge_base::merge_bases_all(source, &[left, right])?
        .into_iter()
        .next())
}

fn write_tree(source: &Repository, entries: &[TreeEntry]) -> grit_lib::error::Result<ObjectId> {
    source.odb.write(ObjectKind::Tree, &serialize_tree(entries))
}

fn write_commit(
    source: &Repository,
    tree: ObjectId,
    parents: Vec<ObjectId>,
    timestamp: i64,
    subject: &str,
) -> grit_lib::error::Result<ObjectId> {
    let ident = identity(timestamp);
    source.odb.write(
        ObjectKind::Commit,
        &serialize_commit(&CommitData {
            tree,
            parents,
            author: ident.clone(),
            committer: ident,
            author_raw: Vec::new(),
            committer_raw: Vec::new(),
            encoding: None,
            message: format!("{subject}\n"),
            raw_message: None,
        }),
    )
}

fn identity(timestamp: i64) -> String {
    format!("A U Thor <a@example.com> {timestamp} +0000")
}

fn resolve_object_or_ref(source: &Repository, input: &str) -> Result<ObjectId> {
    if ObjectId::is_full_hex(input) {
        return Ok(ObjectId::from_hex(input)?);
    }
    for candidate in ref_candidates(input) {
        if let Ok(oid) = refs::resolve_ref(&source.git_dir, &candidate) {
            return Ok(oid);
        }
    }
    Err(Error::RefNotFound(input.to_owned()))
}

fn ref_candidates(input: &str) -> Vec<String> {
    if input == "HEAD" || input.starts_with("refs/") {
        vec![input.to_owned()]
    } else {
        vec![
            input.to_owned(),
            format!("refs/heads/{input}"),
            format!("refs/tags/{input}"),
        ]
    }
}

fn peel_to_commit(source: &Repository, oid: ObjectId) -> Result<CommitSummary> {
    let mut current = oid;
    for _ in 0..=10 {
        let object = source.odb.read(&current)?;
        match object.kind {
            ObjectKind::Commit => {
                let commit = parse_commit(&object.data)?;
                let subject = commit.message.lines().next().unwrap_or_default().to_owned();
                return Ok(CommitSummary {
                    oid: current,
                    tree: commit.tree,
                    parents: commit.parents,
                    author: commit.author,
                    committer: commit.committer,
                    subject,
                    message: commit.message,
                });
            }
            ObjectKind::Tag => {
                current = parse_tag(&object.data)?.object;
            }
            kind => {
                return Err(Error::UnexpectedObjectKind {
                    expected: "commit",
                    actual: object_kind_name(kind),
                });
            }
        }
    }
    Err(Error::Backend(format!(
        "tag cycle while peeling '{}'",
        oid.to_hex()
    )))
}

fn resolve_tree_root(source: &Repository, revision: &str) -> Result<ObjectId> {
    let mut current = resolve_object_or_ref(source, revision)?;
    for _ in 0..=10 {
        let object = source.odb.read(&current)?;
        match object.kind {
            ObjectKind::Commit => return Ok(parse_commit(&object.data)?.tree),
            ObjectKind::Tree => return Ok(current),
            ObjectKind::Tag => current = parse_tag(&object.data)?.object,
            ObjectKind::Blob => {
                return Err(Error::UnexpectedObjectKind {
                    expected: "commit or tree",
                    actual: "blob",
                });
            }
        }
    }
    Err(Error::Backend(format!(
        "tag cycle while resolving tree for '{}'",
        revision
    )))
}

fn tree_entry(source: &Repository, root: ObjectId, path: &str) -> Result<Option<TreeEntryView>> {
    let mut current = root;
    let mut prefix = String::new();
    let mut components = path
        .split('/')
        .filter(|component| !component.is_empty())
        .peekable();
    while let Some(component) = components.next() {
        let entries = parse_tree(&source.odb.read(&current)?.data)?;
        let Some(entry) = entries
            .into_iter()
            .find(|entry| entry.name == component.as_bytes())
        else {
            return Ok(None);
        };
        let full_path = if prefix.is_empty() {
            component.to_owned()
        } else {
            format!("{prefix}/{component}")
        };
        let kind = kind_for_mode(entry.mode);
        let view = TreeEntryView {
            path: full_path.clone(),
            mode: entry.mode,
            oid: entry.oid,
            kind,
            size: blob_size(source, kind, entry.oid),
        };
        if components.peek().is_none() {
            return Ok(Some(view));
        }
        if kind != ObjectKind::Tree {
            return Ok(None);
        }
        current = entry.oid;
        prefix = full_path;
    }
    Ok(None)
}

fn list_tree_entries(
    source: &Repository,
    tree_oid: ObjectId,
    prefix: &str,
) -> Result<Vec<TreeEntryView>> {
    let mut entries = parse_tree(&source.odb.read(&tree_oid)?.data)?
        .into_iter()
        .map(|entry| {
            let name = String::from_utf8(entry.name)
                .map_err(|_| Error::PathNotFound("tree entry name is not UTF-8".to_owned()))?;
            let path = if prefix.is_empty() {
                name
            } else {
                format!("{prefix}{name}")
            };
            let kind = kind_for_mode(entry.mode);
            Ok(TreeEntryView {
                path,
                mode: entry.mode,
                oid: entry.oid,
                kind,
                size: blob_size(source, kind, entry.oid),
            })
        })
        .collect::<Result<Vec<_>>>()?;
    entries.sort_by(|left, right| left.path.cmp(&right.path));
    Ok(entries)
}

fn blob_size(source: &Repository, kind: ObjectKind, oid: ObjectId) -> Option<u64> {
    if kind == ObjectKind::Blob {
        source
            .odb
            .read(&oid)
            .ok()
            .map(|object| object.data.len() as u64)
    } else {
        None
    }
}

fn kind_for_mode(mode: u32) -> ObjectKind {
    match mode {
        0o040000 => ObjectKind::Tree,
        _ => ObjectKind::Blob,
    }
}

fn normalize_tree_path(path: &str) -> String {
    match path {
        "" | "." => String::new(),
        other => other.trim_matches('/').to_owned(),
    }
}

fn collect_commits(
    source: &Repository,
    oid: ObjectId,
    indexed: &mut HashMap<ObjectId, CommitSummary>,
) -> Result<()> {
    if indexed.contains_key(&oid) {
        return Ok(());
    }
    let commit = filesystem_commit(source, &oid.to_hex())?;
    let parents = commit.parents.clone();
    indexed.insert(oid, commit);
    for parent in parents {
        collect_commits(source, parent, indexed)?;
    }
    Ok(())
}

fn best_frontier_index(frontier: &[ObjectId], indexed: &HashMap<ObjectId, CommitSummary>) -> usize {
    let mut best = 0;
    for index in 1..frontier.len() {
        if commit_order(frontier[index], frontier[best], indexed).is_lt() {
            best = index;
        }
    }
    best
}

fn commit_order(
    left: ObjectId,
    right: ObjectId,
    indexed: &HashMap<ObjectId, CommitSummary>,
) -> std::cmp::Ordering {
    right_commit_time(right, indexed)
        .cmp(&right_commit_time(left, indexed))
        .then_with(|| left.cmp(&right))
}

fn right_commit_time(oid: ObjectId, indexed: &HashMap<ObjectId, CommitSummary>) -> i64 {
    indexed
        .get(&oid)
        .map(|commit| commit_time_from_identity(&commit.committer))
        .unwrap_or_default()
}

fn commit_time_from_identity(identity: &str) -> i64 {
    identity
        .split_whitespace()
        .rev()
        .nth(1)
        .and_then(|timestamp| timestamp.parse().ok())
        .unwrap_or_default()
}

fn object_kind_name(kind: ObjectKind) -> &'static str {
    match kind {
        ObjectKind::Blob => "blob",
        ObjectKind::Tree => "tree",
        ObjectKind::Commit => "commit",
        ObjectKind::Tag => "tag",
    }
}
