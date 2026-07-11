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
    let mut report = ImportReport {
        config_entries: import_config(destination, source).await?,
        ..ImportReport::default()
    };
    let mut roots = Vec::new();

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
        report.refs += 1;
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
        roots.push(ImportWork::Object(oid));
        report.refs += 1;
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
        roots.push(ImportWork::Object(oid));
        report.refs += 1;
    }

    let mut seen = HashSet::new();
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
                let indexed =
                    index_tree(destination, source, root, &prefix, &object.data, &mut stack)
                        .await?;
                report.tree_entries += indexed;
            }
            ObjectKind::Tag => {
                let tag = parse_tag(&object.data)?;
                stack.push(ImportWork::Object(tag.object));
            }
            ObjectKind::Blob => {}
        }
    }

    Ok(report)
}

async fn import_config<S>(
    destination: &crate::repository::ServerRepository<S>,
    source: &Repository,
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
        count += 1;
    }
    Ok(count)
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
