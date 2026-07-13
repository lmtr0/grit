//! Import filesystem-backed repositories into server storage.

use std::collections::{HashMap, HashSet, VecDeque};
use std::fs;
use std::sync::Arc;
use std::time::SystemTime;

use grit_lib::config::ConfigSet;
use grit_lib::objects::{
    parse_commit, parse_tag, parse_tree, HashAlgo, Object, ObjectId, ObjectKind,
};
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

type NativeObjectMetadata = HashMap<ObjectId, (ObjectKind, u64)>;

struct ImportWork {
    oid: ObjectId,
    expected_kind: Option<ObjectKind>,
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
    packs: Vec<NativeSourcePack>,
    loose_oids: Vec<ObjectId>,
    full_mirror_compatible: bool,
}

struct NativeInstallInputs<'a> {
    source: &'a Repository,
    options: &'a ImportOptions,
    reachable: &'a HashSet<ObjectId>,
    newly_trusted: &'a [(ObjectId, ObjectKind)],
}

impl NativeSourcePacks {
    fn object_metadata(&self, oid: &ObjectId) -> Option<(ObjectKind, u64)> {
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
        roots.push(ImportWork::object(oid));
    }

    for (name, oid) in refs::list_refs(&source.git_dir, "refs/")? {
        desired_refs.push((name, StoredRef::Direct(oid)));
        roots.push(ImportWork::object(oid));
    }
    let config_entries = read_config(source)?;
    let native_source_packs = discover_native_source_packs(destination, source, &options).await?;
    let native_import_enabled = native_source_packs.is_some();

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
        let oid = work.oid;
        if !seen.insert(oid) {
            continue;
        }
        if trusted_objects.contains(&oid) {
            continue;
        }
        let native_metadata = native_source_packs
            .as_ref()
            .and_then(|native| native.object_metadata(&oid));
        if let (Some(expected), Some((actual, _))) = (work.expected_kind, native_metadata) {
            if expected != actual {
                return Err(Error::Protocol(format!(
                    "source object {oid} has kind {actual}, expected {expected}"
                )));
            }
        }
        let skip_retained_blob = native_metadata.is_some_and(|(kind, _)| kind == ObjectKind::Blob)
            && work
                .expected_kind
                .is_none_or(|kind| kind == ObjectKind::Blob);
        let stored = if skip_retained_blob {
            None
        } else {
            let object = if let Some(native) = native_source_packs.as_ref() {
                native
                    .read_verified_object(&oid)?
                    .map_or_else(|| read_source_object_verified(source, &oid), Ok)?
            } else {
                read_source_object_verified(source, &oid)?
            };
            if let Some(expected) = work.expected_kind {
                if object.kind != expected {
                    return Err(Error::Protocol(format!(
                        "source object {oid} has kind {}, expected {expected}",
                        object.kind
                    )));
                }
            }
            Some(StoredObject::new(object.kind, object.data))
        };
        let (object_kind, object_size) = match (&stored, native_metadata) {
            (Some(stored), Some((native_kind, native_size))) => {
                let decoded_size = u64::try_from(stored.data.len())
                    .map_err(|_| Error::Protocol("source object size exceeds u64".to_owned()))?;
                if stored.kind != native_kind || decoded_size != native_size {
                    return Err(Error::Protocol(format!(
                        "source pack metadata does not match decoded object {oid}"
                    )));
                }
                (stored.kind, decoded_size)
            }
            (Some(stored), None) => (
                stored.kind,
                u64::try_from(stored.data.len())
                    .map_err(|_| Error::Protocol("source object size exceeds u64".to_owned()))?,
            ),
            (None, Some(metadata)) => metadata,
            (None, None) => {
                return Err(Error::Protocol(format!(
                    "source object {oid} has neither payload nor retained-pack metadata"
                )));
            }
        };
        newly_trusted.push((oid, object_kind));

        object_sizes.insert(oid, object_size);
        if let Some(entries) = pending_size_entries.remove(&oid) {
            for entry_index in entries {
                if let Some(entry) = tree_entries.get_mut(entry_index) {
                    entry.size = Some(object_size);
                }
            }
        }

        match object_kind {
            ObjectKind::Commit => {
                let data = stored
                    .as_ref()
                    .map(|stored| stored.data.as_slice())
                    .ok_or_else(|| {
                        Error::Protocol("commit payload was not decoded during import".to_owned())
                    })?;
                let commit = parse_commit(data)?;
                stack.push(ImportWork::expected(commit.tree, ObjectKind::Tree));
                stack.extend(
                    commit
                        .parents
                        .iter()
                        .copied()
                        .map(|parent| ImportWork::expected(parent, ObjectKind::Commit)),
                );
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
                let data = stored
                    .as_ref()
                    .map(|stored| stored.data.as_slice())
                    .ok_or_else(|| {
                        Error::Protocol("tree payload was not decoded during import".to_owned())
                    })?;
                let indexed = index_tree(
                    oid,
                    data,
                    &mut stack,
                    &mut tree_entries,
                    &object_sizes,
                    &mut pending_size_entries,
                )?;
                indexed_tree_batches.push((oid, indexed));
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
                stack.push(ImportWork::expected(tag.object, target_kind));
            }
            ObjectKind::Blob => {}
        }

        if !native_import_enabled {
            let stored = stored.ok_or_else(|| {
                Error::Protocol("portable import unexpectedly omitted object payload".to_owned())
            })?;
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
    }
    if let Some(native_source_packs) = native_source_packs {
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
            &mut report,
            &mut progress,
        )
        .await?;
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
    }
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

async fn discover_native_source_packs<S>(
    destination: &crate::repository::ServerRepository<S>,
    source: &Repository,
    options: &ImportOptions,
) -> Result<Option<NativeSourcePacks>>
where
    S: ServerStorage,
{
    if options.native_pack_import == NativePackImportMode::Disabled
        || !destination.storage().supports_native_pack_import()
        || destination.hash_algo() != source.odb.hash_algo()
    {
        return Ok(None);
    }

    let installed = destination
        .storage()
        .list_packs(destination.tenant(), destination.repository())
        .await?
        .into_iter()
        .map(|metadata| metadata.pack_checksum)
        .collect::<HashSet<_>>();
    let pack_dir = source.odb.objects_dir().join("pack");
    let local_pack_count = fs::read_dir(&pack_dir)
        .ok()
        .into_iter()
        .flatten()
        .filter_map(std::result::Result::ok)
        .filter(|entry| entry.path().extension().and_then(|value| value.to_str()) == Some("pack"))
        .count();
    let indexes = match read_local_pack_index_snapshots(source.odb.objects_dir()) {
        Ok(indexes) => indexes,
        Err(_) => {
            return Ok(Some(NativeSourcePacks {
                full_mirror_compatible: false,
                ..NativeSourcePacks::default()
            }));
        }
    };
    let mut discovered = NativeSourcePacks {
        full_mirror_compatible: indexes.len() == local_pack_count,
        ..NativeSourcePacks::default()
    };
    for (index, index_data) in indexes {
        if index.hash_bytes != destination.hash_algo().len()
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
            source_pack_metadata(&index, &index_data, &data, destination.hash_algo())
        else {
            discovered.full_mirror_compatible = false;
            continue;
        };
        let modified = fs::metadata(&index.pack_path)
            .and_then(|metadata| metadata.modified())
            .unwrap_or(SystemTime::UNIX_EPOCH);
        let already_stored = installed.contains(&metadata.pack_checksum);
        discovered.packs.push(NativeSourcePack {
            index,
            data,
            object_metadata,
            metadata,
            modified,
            already_stored,
        });
    }
    discovered.packs.sort_by(|left, right| {
        left.modified
            .cmp(&right.modified)
            .then_with(|| left.index.pack_path.cmp(&right.index.pack_path))
    });
    if options.native_pack_import == NativePackImportMode::FullMirror {
        match list_local_loose_object_ids(source) {
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
    for (raw_oid, metadata) in pack::read_all_object_metadata_from_pack_bytes(data, index)? {
        let oid = ObjectId::from_bytes(&raw_oid)?;
        if object_metadata
            .insert(oid, (metadata.kind, metadata.size))
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

fn read_source_object_verified(source: &Repository, oid: &ObjectId) -> Result<Object> {
    let object = source.odb.read(oid)?;
    let actual = pack::hash_object_bytes(object.kind, &object.data, source.odb.hash_algo().len())?;
    if actual.as_slice() != oid.as_bytes() {
        return Err(Error::Protocol(format!(
            "source object {oid} failed identity verification"
        )));
    }
    Ok(object)
}

fn list_local_loose_object_ids(source: &Repository) -> Result<Vec<ObjectId>> {
    let expected_hex_len = source.odb.hash_algo().len().saturating_mul(2);
    let mut oids = Vec::new();
    let entries = match fs::read_dir(source.odb.objects_dir()) {
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
    native: NativeSourcePacks,
    inputs: NativeInstallInputs<'_>,
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
    for source_pack in native.packs {
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
        if validate_native_pack_readability(&source_pack).is_err() {
            all_candidates_installed = false;
            continue;
        }
        if source_pack.already_stored {
            covered.extend(pack_oids);
            continue;
        }
        match prepare_native_stored_pack(source_pack) {
            Ok(pack) => {
                covered.extend(pack.index.iter().map(|entry| entry.oid));
                packs_to_install.push(pack);
            }
            Err(_) => all_candidates_installed = false,
        }
    }

    report.native_packs = packs_to_install.len();
    if !packs_to_install.is_empty() {
        destination
            .storage()
            .write_imported_packs(
                destination.tenant(),
                destination.repository(),
                packs_to_install,
            )
            .await?;
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
    for (oid, _) in inputs.newly_trusted {
        if covered.contains(oid) {
            continue;
        }
        let object = read_source_object_verified(inputs.source, oid)?;
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
        pending.push((*oid, stored));
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
        for oid in native.loose_oids {
            if covered.contains(&oid) || newly_reachable.contains(&oid) {
                continue;
            }
            let object = match read_source_object_verified(inputs.source, &oid) {
                Ok(object) => object,
                Err(_) => {
                    mirror_loose_complete = false;
                    continue;
                }
            };
            let stored = StoredObject::new(object.kind, object.data);
            let payload_bytes = stored.data.len();
            if !mirror_batch.is_empty()
                && (mirror_batch.len() >= IMPORT_OBJECT_BATCH_MAX_COUNT
                    || mirror_bytes.saturating_add(payload_bytes) > IMPORT_OBJECT_BATCH_MAX_BYTES)
            {
                write_mirror_loose_batch(destination, &mut mirror_batch, report).await?;
                mirror_bytes = 0;
            }
            mirror_bytes = mirror_bytes.saturating_add(payload_bytes);
            mirror_batch.push((oid, stored));
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

fn validate_native_pack_readability(source: &NativeSourcePack) -> Result<()> {
    for entry in &source.index.entries {
        let oid = ObjectId::from_bytes(&entry.oid)?;
        let expected = source.object_metadata.get(&oid).copied().ok_or_else(|| {
            Error::Protocol("native pack object has no validated metadata".to_owned())
        })?;
        let object = pack::read_object_from_pack_bytes(&source.data, &source.index, &entry.oid)?;
        let decoded_size = u64::try_from(object.data.len())
            .map_err(|_| Error::Protocol("native pack object size exceeds u64".to_owned()))?;
        if (object.kind, decoded_size) != expected {
            return Err(Error::Protocol(format!(
                "source pack metadata does not match verified object {oid}"
            )));
        }
    }
    Ok(())
}

fn prepare_native_stored_pack(source: NativeSourcePack) -> Result<ImportedPack> {
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
        let (kind, size) = source.object_metadata.get(&oid).copied().ok_or_else(|| {
            Error::Protocol("native pack object has no validated metadata".to_owned())
        })?;
        let stored_span = stored_spans.get(&entry.offset).copied().ok_or_else(|| {
            Error::Protocol("native pack object has no stored byte span".to_owned())
        })?;
        entries.push(PackObjectIndex {
            oid,
            kind,
            offset: entry.offset,
            size,
            compressed_size: stored_span,
        });
    }
    entries.sort_by_key(|entry| entry.offset);
    Ok(ImportedPack {
        metadata: source.metadata,
        data: source.data,
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
    report.loose_objects = report.loose_objects.saturating_add(pending.len());
    destination
        .storage()
        .write_imported_objects(
            destination.tenant(),
            destination.repository(),
            std::mem::take(pending),
        )
        .await
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
    report.loose_objects = report.loose_objects.saturating_add(events.len());

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
        // Gitlinks name commits from another repository. Their object IDs are not
        // required (or generally available) in the superproject object database.
        if entry.mode != 0o160000 {
            stack.push(ImportWork::expected(entry.oid, kind));
        }
    }
    Ok(indexed.len() - initial_len)
}

fn kind_for_mode(mode: u32) -> ObjectKind {
    match mode {
        0o040000 => ObjectKind::Tree,
        0o160000 => ObjectKind::Commit,
        _ => ObjectKind::Blob,
    }
}
