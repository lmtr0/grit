mod support;

use grit_lib_server::storage::{BrowseIndex, ConfigStore};
use grit_lib_server::views::CommitHistoryOptions;

use support::{
    create_conformance_fixture, filesystem_blob_at, filesystem_commit, filesystem_config_value,
    filesystem_error_class, filesystem_history, filesystem_is_ancestor, filesystem_merge_base,
    filesystem_object, filesystem_ref_snapshot, filesystem_tree_at, server_ref_snapshot,
    server_result_class, ErrorClass,
};

#[tokio::test]
async fn objects_refs_and_config_match_filesystem_repository() -> grit_lib_server::error::Result<()>
{
    let fixture = create_conformance_fixture().await?;

    assert_eq!(
        fixture.server.read_object(&fixture.readme).await?,
        Some(filesystem_object(&fixture.source, &fixture.readme)?)
    );
    assert_eq!(
        fixture.server.read_object(&fixture.script).await?,
        Some(filesystem_object(&fixture.source, &fixture.script)?)
    );
    assert_eq!(
        server_ref_snapshot(&fixture.server).await?,
        filesystem_ref_snapshot(&fixture.source)?
    );
    assert_eq!(
        fixture
            .server
            .storage()
            .get_config(
                fixture.server.tenant(),
                fixture.server.repository(),
                "grit.server.parity",
            )
            .await?,
        filesystem_config_value(&fixture.source, "grit.server.parity")?
    );

    Ok(())
}

#[tokio::test]
async fn hosting_views_match_filesystem_repository() -> grit_lib_server::error::Result<()> {
    let fixture = create_conformance_fixture().await?;

    let summary = fixture.server.summary().await?;
    assert_eq!(
        summary.latest_commit,
        Some(filesystem_commit(&fixture.source, "HEAD")?)
    );
    assert_eq!(
        summary.default_branch.as_ref().map(|branch| branch.target),
        Some(fixture.merge)
    );

    assert_eq!(
        fixture.server.commit("main").await?,
        filesystem_commit(&fixture.source, "main")?
    );
    assert_eq!(
        fixture.server.commit("v-merge").await?,
        filesystem_commit(&fixture.source, "v-merge")?
    );
    assert_eq!(fixture.server.tags().await?.len(), 2);
    assert!(fixture
        .server
        .tags()
        .await?
        .iter()
        .any(|tag| tag.oid == fixture.annotated_tag && tag.target == fixture.merge));

    assert_eq!(
        fixture.server.tree_at("main", "").await?,
        filesystem_tree_at(&fixture.source, "main", "")?
    );
    assert_eq!(
        fixture.server.tree_at("main", "bin").await?,
        filesystem_tree_at(&fixture.source, "main", "bin")?
    );
    assert_eq!(
        fixture
            .server
            .tree_at(&fixture.merge_tree.to_hex(), "docs")
            .await?,
        filesystem_tree_at(&fixture.source, &fixture.merge_tree.to_hex(), "docs")?
    );
    assert_eq!(
        fixture.server.blob_at("main", "README.md").await?,
        filesystem_blob_at(&fixture.source, "main", "README.md")?
    );
    assert_eq!(
        fixture.server.blob_at("main", "bin/run.sh").await?,
        filesystem_blob_at(&fixture.source, "main", "bin/run.sh")?
    );

    let discovered = fixture
        .server
        .discover_files("main", &["README.md", "README", "LICENSE", ".gitmodules"])
        .await?;
    assert_eq!(
        discovered
            .iter()
            .map(|file| file.blob.path.as_str())
            .collect::<Vec<_>>(),
        vec!["README.md", "LICENSE", ".gitmodules"]
    );

    let compare = fixture.server.compare_inputs("left", "main").await?;
    assert_eq!(compare.base, filesystem_commit(&fixture.source, "left")?);
    assert_eq!(compare.head, filesystem_commit(&fixture.source, "main")?);

    Ok(())
}

#[tokio::test]
async fn graph_queries_match_filesystem_repository() -> grit_lib_server::error::Result<()> {
    let fixture = create_conformance_fixture().await?;

    let server_history = fixture
        .server
        .commit_history(
            "main",
            CommitHistoryOptions {
                offset: 0,
                limit: None,
                path: None,
            },
        )
        .await?;
    assert_eq!(
        server_history.commits,
        filesystem_history(&fixture.source, "main")?
    );

    assert_eq!(
        fixture
            .server
            .commit_parents("main")
            .await?
            .into_iter()
            .map(|commit| commit.oid)
            .collect::<Vec<_>>(),
        vec![fixture.left, fixture.right]
    );
    assert_eq!(
        fixture
            .server
            .is_ancestor(&fixture.base.to_hex(), "main")
            .await?,
        filesystem_is_ancestor(&fixture.source, fixture.base, fixture.merge)?
    );
    assert_eq!(
        fixture
            .server
            .merge_base("left", "right")
            .await?
            .map(|commit| commit.oid),
        filesystem_merge_base(&fixture.source, fixture.left, fixture.right)?
    );

    let comparison = fixture.server.compare_commits("right", "main").await?;
    assert_eq!(comparison.base.oid, fixture.right);
    assert_eq!(comparison.head.oid, fixture.merge);
    assert_eq!(
        comparison.merge_base.as_ref().map(|commit| commit.oid),
        Some(fixture.right)
    );
    assert_eq!(comparison.ahead_by, 2);
    assert_eq!(comparison.behind_by, 0);

    let readme_history = fixture
        .server
        .commit_history(
            "main",
            CommitHistoryOptions {
                offset: 0,
                limit: None,
                path: Some("README.md".to_owned()),
            },
        )
        .await?;
    assert_eq!(
        readme_history
            .commits
            .iter()
            .map(|commit| commit.oid)
            .collect::<Vec<_>>(),
        vec![fixture.merge, fixture.left, fixture.base]
    );

    Ok(())
}

#[tokio::test]
async fn error_classes_match_for_hosting_operations() -> grit_lib_server::error::Result<()> {
    let fixture = create_conformance_fixture().await?;

    assert_eq!(
        server_result_class(fixture.server.commit("missing").await),
        Err(ErrorClass::RefNotFound)
    );
    assert_eq!(
        filesystem_error_class(
            &grit_lib::refs::resolve_ref(&fixture.source.git_dir, "missing")
                .err()
                .ok_or_else(|| grit_lib_server::error::Error::Backend(
                    "missing ref unexpectedly resolved".to_owned()
                ),)?
        ),
        ErrorClass::RefNotFound
    );
    assert_eq!(
        server_result_class(fixture.server.blob_at("main", "missing.txt").await),
        Err(ErrorClass::PathNotFound)
    );
    assert_eq!(
        server_result_class(fixture.server.blob_at("main", "docs").await),
        Err(ErrorClass::UnexpectedObjectKind)
    );
    assert_eq!(
        server_result_class(fixture.server.tree_at("main", "README.md").await),
        Err(ErrorClass::UnexpectedObjectKind)
    );

    Ok(())
}

#[tokio::test]
async fn browse_index_repair_restores_filesystem_tree_view() -> grit_lib_server::error::Result<()> {
    let fixture = create_conformance_fixture().await?;
    fixture
        .server
        .storage()
        .replace_tree_entries(fixture.server.tenant(), fixture.server.repository(), &[])
        .await?;
    assert!(fixture.server.tree_at("main", "").await?.entries.is_empty());

    let repaired = fixture.server.repair_browse_index().await?;
    assert!(repaired > 0);
    assert_eq!(
        fixture.server.tree_at("main", "").await?,
        filesystem_tree_at(&fixture.source, "main", "")?
    );

    Ok(())
}
