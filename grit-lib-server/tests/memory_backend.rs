use std::sync::Arc;

use grit_lib::objects::{
    serialize_commit, serialize_tree, CommitData, HashAlgo, ObjectId, ObjectKind, TreeEntry,
};
use grit_lib::refs;
use grit_lib::repo::init_repository;
use grit_lib_server::cached::CachedStorage;
use grit_lib_server::ids::{RepositoryId, TenantId};
use grit_lib_server::import::import_repository;
use grit_lib_server::memory::MemoryBackend;
use grit_lib_server::repository::ServerRepository;
use grit_lib_server::storage::{
    BrowseIndex, IndexedTreeEntry, ObjectStore, RefStore, StoredObject, StoredRef,
};

fn ids() -> grit_lib_server::error::Result<(TenantId, RepositoryId)> {
    Ok((TenantId::new("tenant-a")?, RepositoryId::new("repo-a")?))
}

#[tokio::test]
async fn stores_objects_under_computed_oid() -> grit_lib_server::error::Result<()> {
    let (tenant, repository) = ids()?;
    let backend = Arc::new(MemoryBackend::new());
    let repo = ServerRepository::new(tenant, repository, HashAlgo::Sha1, backend.clone());
    let object = StoredObject::new(ObjectKind::Blob, b"hello\n".to_vec());

    let oid = repo.write_object(&object).await?;
    assert_eq!(oid.to_hex(), "ce013625030ba8dba906f756967f9e9ca394464a");
    assert!(
        backend
            .object_exists(repo.tenant(), repo.repository(), &oid)
            .await?
    );
    assert_eq!(repo.read_object(&oid).await?, Some(object));

    Ok(())
}

#[tokio::test]
async fn imports_repository_refs_objects_and_browse_index() -> grit_lib_server::error::Result<()> {
    let temp = tempfile::tempdir().map_err(grit_lib::error::Error::from)?;
    let source = init_repository(temp.path(), false, "main", None, "files")?;
    let readme_oid = source.odb.write(ObjectKind::Blob, b"# Demo\n")?;
    let main_oid = source.odb.write(ObjectKind::Blob, b"fn main() {}\n")?;
    let src_tree = serialize_tree(&[TreeEntry {
        mode: 0o100644,
        name: b"main.rs".to_vec(),
        oid: main_oid,
    }]);
    let src_tree_oid = source.odb.write(ObjectKind::Tree, &src_tree)?;
    let root_tree = serialize_tree(&[
        TreeEntry {
            mode: 0o100644,
            name: b"README.md".to_vec(),
            oid: readme_oid,
        },
        TreeEntry {
            mode: 0o040000,
            name: b"src".to_vec(),
            oid: src_tree_oid,
        },
    ]);
    let root_tree_oid = source.odb.write(ObjectKind::Tree, &root_tree)?;
    let ident = "A U Thor <a@example.com> 1700000000 +0000".to_owned();
    let commit = CommitData {
        tree: root_tree_oid,
        parents: Vec::new(),
        author: ident.clone(),
        committer: ident,
        author_raw: Vec::new(),
        committer_raw: Vec::new(),
        encoding: None,
        message: "initial import\n".to_owned(),
        raw_message: None,
    };
    let commit_oid = source
        .odb
        .write(ObjectKind::Commit, &serialize_commit(&commit))?;
    refs::write_ref(&source.git_dir, "refs/heads/main", &commit_oid)?;

    let (tenant, repository) = ids()?;
    let backend = Arc::new(MemoryBackend::new());
    let repo = ServerRepository::new(tenant, repository, HashAlgo::Sha1, backend);
    let report = import_repository(&repo, &source).await?;

    assert!(report.objects >= 5);
    assert_eq!(repo.resolve_ref("HEAD").await?, Some(commit_oid));
    assert_eq!(repo.resolve_ref("refs/heads/main").await?, Some(commit_oid));

    let summary = repo
        .commit_summary(&commit_oid)
        .await?
        .ok_or_else(|| grit_lib_server::error::Error::ObjectNotFound(commit_oid.to_hex()))?;
    assert_eq!(summary.tree, root_tree_oid);
    assert_eq!(summary.subject, "initial import");

    let root_entries = repo.list_tree(&root_tree_oid, "").await?;
    assert!(root_entries.iter().any(|entry| entry.path == "README.md"));
    assert!(root_entries.iter().any(|entry| entry.path == "src"));
    assert!(root_entries.iter().any(|entry| entry.path == "src/main.rs"));

    let blob = repo
        .read_blob_at_path(&root_tree_oid, "src/main.rs")
        .await?
        .ok_or_else(|| grit_lib_server::error::Error::PathNotFound("src/main.rs".to_owned()))?;
    assert_eq!(blob.oid, main_oid);
    assert_eq!(blob.data, b"fn main() {}\n");

    Ok(())
}

#[tokio::test]
async fn ref_writes_support_compare_and_swap() -> grit_lib_server::error::Result<()> {
    let (tenant, repository) = ids()?;
    let backend = MemoryBackend::new();
    let first = StoredRef::Direct(ObjectId::from_hex(
        "1111111111111111111111111111111111111111",
    )?);
    let second = StoredRef::Direct(ObjectId::from_hex(
        "2222222222222222222222222222222222222222",
    )?);

    backend
        .write_ref(&tenant, &repository, "refs/heads/main", &first, Some(None))
        .await?;
    let conflict = backend
        .write_ref(&tenant, &repository, "refs/heads/main", &second, Some(None))
        .await;
    assert!(matches!(
        conflict,
        Err(grit_lib_server::error::Error::RefConflict(_))
    ));
    backend
        .write_ref(
            &tenant,
            &repository,
            "refs/heads/main",
            &second,
            Some(Some(first)),
        )
        .await?;
    assert_eq!(
        backend
            .read_ref(&tenant, &repository, "refs/heads/main")
            .await?,
        Some(second)
    );

    Ok(())
}

#[tokio::test]
async fn repository_ref_write_publishes_invalidation() -> grit_lib_server::error::Result<()> {
    let (tenant, repository) = ids()?;
    let backend = Arc::new(MemoryBackend::new());
    let repo = ServerRepository::new(tenant, repository, HashAlgo::Sha1, backend.clone());
    let target = StoredRef::Symbolic("refs/heads/main".to_owned());

    repo.write_ref_and_publish("HEAD", &target, None, backend.as_ref())
        .await?;
    let events = backend.events()?;
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].tenant, repo.tenant().clone());
    assert_eq!(events[0].repository, repo.repository().clone());
    assert_eq!(
        backend
            .read_ref(repo.tenant(), repo.repository(), "HEAD")
            .await?,
        Some(target)
    );

    Ok(())
}

#[tokio::test]
async fn browse_index_resolves_blob_contents_by_path() -> grit_lib_server::error::Result<()> {
    let (tenant, repository) = ids()?;
    let backend = MemoryBackend::new();
    let blob = StoredObject::new(ObjectKind::Blob, b"readme".to_vec());
    let blob_oid = blob.object_id(HashAlgo::Sha1);
    let tree_oid = ObjectId::from_hex("3333333333333333333333333333333333333333")?;

    backend
        .write_object(&tenant, &repository, &blob_oid, &blob)
        .await?;
    backend
        .upsert_tree_entries(
            &tenant,
            &repository,
            &[IndexedTreeEntry {
                tree_oid,
                path: "README.md".to_owned(),
                mode: 0o100644,
                oid: blob_oid,
                kind: ObjectKind::Blob,
                size: Some(6),
            }],
        )
        .await?;

    let entries = backend
        .list_tree_entries(&tenant, &repository, &tree_oid, "")
        .await?;
    assert_eq!(entries.len(), 1);
    assert_eq!(
        backend
            .read_blob_at_path(&tenant, &repository, &tree_oid, "README.md")
            .await?,
        Some(blob)
    );

    Ok(())
}

#[tokio::test]
async fn cached_storage_reads_refs_from_cache_and_updates_on_write(
) -> grit_lib_server::error::Result<()> {
    let (tenant, repository) = ids()?;
    let durable = Arc::new(MemoryBackend::new());
    let cache = Arc::new(MemoryBackend::new());
    let cached = CachedStorage::new(durable.clone(), cache);
    let first = StoredRef::Direct(ObjectId::from_hex(
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    )?);
    let second = StoredRef::Direct(ObjectId::from_hex(
        "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
    )?);

    durable
        .write_ref(&tenant, &repository, "refs/heads/main", &first, None)
        .await?;
    assert_eq!(
        cached
            .read_ref(&tenant, &repository, "refs/heads/main")
            .await?,
        Some(first.clone())
    );

    durable
        .write_ref(&tenant, &repository, "refs/heads/main", &second, None)
        .await?;
    assert_eq!(
        cached
            .read_ref(&tenant, &repository, "refs/heads/main")
            .await?,
        Some(first)
    );

    cached
        .write_ref(&tenant, &repository, "refs/heads/main", &second, None)
        .await?;
    assert_eq!(
        cached
            .read_ref(&tenant, &repository, "refs/heads/main")
            .await?,
        Some(second)
    );

    Ok(())
}
