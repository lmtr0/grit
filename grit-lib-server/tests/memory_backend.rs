use std::sync::Arc;

use grit_lib::objects::{
    serialize_commit, serialize_tag, serialize_tree, CommitData, HashAlgo, ObjectId, ObjectKind,
    TagData, TreeEntry,
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

struct ImportedFixture {
    repo: ServerRepository<MemoryBackend>,
    report: grit_lib_server::import::ImportReport,
    base_commit: ObjectId,
    head_commit: ObjectId,
    head_tree: ObjectId,
    readme: ObjectId,
    script: ObjectId,
    symlink: ObjectId,
    annotated_tag: ObjectId,
}

async fn import_hosting_fixture() -> grit_lib_server::error::Result<ImportedFixture> {
    let temp = tempfile::tempdir().map_err(grit_lib::error::Error::from)?;
    let source = init_repository(temp.path(), false, "main", None, "files")?;
    refs::write_symbolic_ref(&source.git_dir, "HEAD", "refs/heads/main")?;

    let old_readme = source.odb.write(ObjectKind::Blob, b"# Demo\n")?;
    let base_tree = serialize_tree(&[TreeEntry {
        mode: 0o100644,
        name: b"README.md".to_vec(),
        oid: old_readme,
    }]);
    let base_tree_oid = source.odb.write(ObjectKind::Tree, &base_tree)?;
    let ident = "A U Thor <a@example.com> 1700000000 +0000".to_owned();
    let base_commit_data = CommitData {
        tree: base_tree_oid,
        parents: Vec::new(),
        author: ident.clone(),
        committer: ident.clone(),
        author_raw: Vec::new(),
        committer_raw: Vec::new(),
        encoding: None,
        message: "base\n".to_owned(),
        raw_message: None,
    };
    let base_commit = source
        .odb
        .write(ObjectKind::Commit, &serialize_commit(&base_commit_data))?;

    let readme = source.odb.write(ObjectKind::Blob, b"# Demo\n\nUpdated\n")?;
    let license = source.odb.write(ObjectKind::Blob, b"MIT\n")?;
    let gitmodules = source
        .odb
        .write(ObjectKind::Blob, b"[submodule \"dep\"]\n\tpath = dep\n")?;
    let script = source
        .odb
        .write(ObjectKind::Blob, b"#!/bin/sh\necho hi\n")?;
    let guide = source.odb.write(ObjectKind::Blob, b"guide\n")?;
    let symlink = source.odb.write(ObjectKind::Blob, b"docs/guide.md")?;

    let bin_tree = serialize_tree(&[TreeEntry {
        mode: 0o100755,
        name: b"run.sh".to_vec(),
        oid: script,
    }]);
    let bin_tree_oid = source.odb.write(ObjectKind::Tree, &bin_tree)?;
    let docs_tree = serialize_tree(&[TreeEntry {
        mode: 0o100644,
        name: b"guide.md".to_vec(),
        oid: guide,
    }]);
    let docs_tree_oid = source.odb.write(ObjectKind::Tree, &docs_tree)?;
    let head_tree = serialize_tree(&[
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
            oid: readme,
        },
        TreeEntry {
            mode: 0o040000,
            name: b"bin".to_vec(),
            oid: bin_tree_oid,
        },
        TreeEntry {
            mode: 0o040000,
            name: b"docs".to_vec(),
            oid: docs_tree_oid,
        },
        TreeEntry {
            mode: 0o120000,
            name: b"guide-link".to_vec(),
            oid: symlink,
        },
    ]);
    let head_tree = source.odb.write(ObjectKind::Tree, &head_tree)?;
    let head_commit_data = CommitData {
        tree: head_tree,
        parents: vec![base_commit],
        author: ident.clone(),
        committer: ident.clone(),
        author_raw: Vec::new(),
        committer_raw: Vec::new(),
        encoding: None,
        message: "head\n\nbody\n".to_owned(),
        raw_message: None,
    };
    let head_commit = source
        .odb
        .write(ObjectKind::Commit, &serialize_commit(&head_commit_data))?;
    let tag = TagData {
        object: head_commit,
        object_type: "commit".to_owned(),
        tag: "v2.0.0".to_owned(),
        tagger: Some(ident),
        message: "release\n".to_owned(),
    };
    let annotated_tag = source.odb.write(ObjectKind::Tag, &serialize_tag(&tag))?;

    refs::write_ref(&source.git_dir, "refs/heads/feature", &base_commit)?;
    refs::write_ref(&source.git_dir, "refs/heads/main", &head_commit)?;
    refs::write_ref(&source.git_dir, "refs/tags/v1.0.0", &base_commit)?;
    refs::write_ref(&source.git_dir, "refs/tags/v2.0.0", &annotated_tag)?;

    let (tenant, repository) = ids()?;
    let backend = Arc::new(MemoryBackend::new());
    let repo = ServerRepository::new(tenant, repository, HashAlgo::Sha1, backend);
    let report = import_repository(&repo, &source).await?;

    Ok(ImportedFixture {
        repo,
        report,
        base_commit,
        head_commit,
        head_tree,
        readme,
        script,
        symlink,
        annotated_tag,
    })
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
async fn hosting_views_work_for_imported_memory_repository() -> grit_lib_server::error::Result<()> {
    let fixture = import_hosting_fixture().await?;
    let repo = &fixture.repo;

    let default_branch = repo
        .default_branch()
        .await?
        .ok_or_else(|| grit_lib_server::error::Error::RefNotFound("HEAD".to_owned()))?;
    assert_eq!(default_branch.name, "main");
    assert_eq!(default_branch.refname, "refs/heads/main");
    assert_eq!(default_branch.target, fixture.head_commit);
    assert_eq!(
        default_branch.commit.as_ref().map(|commit| commit.oid),
        Some(fixture.head_commit)
    );

    let summary = repo.summary().await?;
    assert_eq!(
        summary
            .default_branch
            .as_ref()
            .map(|branch| branch.name.as_str()),
        Some("main")
    );
    assert_eq!(summary.refs_count, fixture.report.refs);
    assert_eq!(summary.object_count, fixture.report.objects);
    assert_eq!(
        summary.latest_commit.as_ref().map(|commit| commit.oid),
        Some(fixture.head_commit)
    );

    let branches = repo.branches().await?;
    assert_eq!(branches.len(), 2);
    assert!(branches
        .iter()
        .any(|branch| branch.name == "feature" && branch.target == fixture.base_commit));
    assert!(branches
        .iter()
        .any(|branch| branch.name == "main" && branch.target == fixture.head_commit));

    let tags = repo.tags().await?;
    let annotated = tags
        .iter()
        .find(|tag| tag.name == "v2.0.0")
        .ok_or_else(|| grit_lib_server::error::Error::RefNotFound("refs/tags/v2.0.0".to_owned()))?;
    assert_eq!(annotated.oid, fixture.annotated_tag);
    assert_eq!(annotated.target, fixture.head_commit);
    assert_eq!(annotated.target_kind, ObjectKind::Commit);
    assert_eq!(
        annotated.peeled_commit.as_ref().map(|commit| commit.oid),
        Some(fixture.head_commit)
    );
    assert_eq!(annotated.message.as_deref(), Some("release\n"));

    let lightweight = tags
        .iter()
        .find(|tag| tag.name == "v1.0.0")
        .ok_or_else(|| grit_lib_server::error::Error::RefNotFound("refs/tags/v1.0.0".to_owned()))?;
    assert_eq!(lightweight.oid, fixture.base_commit);
    assert_eq!(
        lightweight.peeled_commit.as_ref().map(|commit| commit.oid),
        Some(fixture.base_commit)
    );

    assert_eq!(
        repo.commit(&fixture.head_commit.to_hex()).await?.oid,
        fixture.head_commit
    );
    assert_eq!(
        repo.commit("refs/heads/main").await?.oid,
        fixture.head_commit
    );
    assert_eq!(repo.commit("main").await?.oid, fixture.head_commit);
    assert_eq!(repo.commit("v2.0.0").await?.oid, fixture.head_commit);

    let root = repo.tree_at("main", "").await?;
    assert_eq!(root.oid, fixture.head_tree);
    assert!(root.entries.iter().any(|entry| entry.path == "README.md"));
    assert!(root.entries.iter().any(|entry| entry.path == "bin"));
    assert!(!root.entries.iter().any(|entry| entry.path == "bin/run.sh"));

    let bin = repo.tree_at("refs/heads/main", "bin").await?;
    assert_eq!(bin.path, "bin");
    assert_eq!(bin.entries.len(), 1);
    assert_eq!(bin.entries[0].path, "bin/run.sh");
    assert_eq!(bin.entries[0].mode, 0o100755);

    let by_tree_oid = repo.tree_at(&fixture.head_tree.to_hex(), "docs").await?;
    assert_eq!(by_tree_oid.entries[0].path, "docs/guide.md");

    let readme = repo.blob_at("HEAD", "README.md").await?;
    assert_eq!(readme.oid, fixture.readme);
    assert_eq!(readme.data, b"# Demo\n\nUpdated\n");
    let script = repo
        .blob_at(&fixture.head_commit.to_hex(), "bin/run.sh")
        .await?;
    assert_eq!(script.oid, fixture.script);
    assert_eq!(script.mode, 0o100755);
    let symlink = repo.blob_at("main", "guide-link").await?;
    assert_eq!(symlink.oid, fixture.symlink);
    assert_eq!(symlink.mode, 0o120000);
    assert_eq!(symlink.data, b"docs/guide.md");

    let discovered = repo
        .discover_files("main", &["README.md", "README", "LICENSE", ".gitmodules"])
        .await?;
    let discovered_paths = discovered
        .iter()
        .map(|file| file.blob.path.as_str())
        .collect::<Vec<_>>();
    assert_eq!(
        discovered_paths,
        vec!["README.md", "LICENSE", ".gitmodules"]
    );

    let compare = repo.compare_inputs("refs/heads/feature", "main").await?;
    assert_eq!(compare.base.oid, fixture.base_commit);
    assert_eq!(compare.head.oid, fixture.head_commit);
    assert_eq!(compare.head_tree, fixture.head_tree);

    assert!(matches!(
        repo.blob_at("main", "empty/file").await,
        Err(grit_lib_server::error::Error::PathNotFound(_))
    ));
    assert!(matches!(
        repo.tree_at("main", "bin/run.sh").await,
        Err(grit_lib_server::error::Error::UnexpectedObjectKind { .. })
    ));
    assert!(matches!(
        repo.blob_at("main", "bin").await,
        Err(grit_lib_server::error::Error::UnexpectedObjectKind { .. })
    ));

    Ok(())
}

#[tokio::test]
async fn default_branch_handles_detached_head() -> grit_lib_server::error::Result<()> {
    let temp = tempfile::tempdir().map_err(grit_lib::error::Error::from)?;
    let source = init_repository(temp.path(), false, "main", None, "files")?;
    let blob = source.odb.write(ObjectKind::Blob, b"detached\n")?;
    let tree = serialize_tree(&[TreeEntry {
        mode: 0o100644,
        name: b"README.md".to_vec(),
        oid: blob,
    }]);
    let tree = source.odb.write(ObjectKind::Tree, &tree)?;
    let ident = "A U Thor <a@example.com> 1700000000 +0000".to_owned();
    let commit = CommitData {
        tree,
        parents: Vec::new(),
        author: ident.clone(),
        committer: ident,
        author_raw: Vec::new(),
        committer_raw: Vec::new(),
        encoding: None,
        message: "detached\n".to_owned(),
        raw_message: None,
    };
    let commit = source
        .odb
        .write(ObjectKind::Commit, &serialize_commit(&commit))?;
    refs::write_ref(&source.git_dir, "HEAD", &commit)?;

    let (tenant, repository) = ids()?;
    let backend = Arc::new(MemoryBackend::new());
    let repo = ServerRepository::new(tenant, repository, HashAlgo::Sha1, backend);
    import_repository(&repo, &source).await?;

    assert!(repo.default_branch().await?.is_none());
    assert_eq!(
        repo.summary()
            .await?
            .latest_commit
            .as_ref()
            .map(|summary| summary.oid),
        Some(commit)
    );
    assert_eq!(repo.blob_at("HEAD", "README.md").await?.data, b"detached\n");

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
