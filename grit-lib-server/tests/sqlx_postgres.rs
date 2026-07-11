#![cfg(feature = "sqlx-postgres")]

use std::env;
use std::sync::atomic::{AtomicUsize, Ordering};

use grit_lib::objects::{HashAlgo, ObjectId, ObjectKind};
use grit_lib_server::error::{Error, Result};
use grit_lib_server::ids::{RepositoryId, TenantId};
use grit_lib_server::sqlx_postgres::PgServerStorage;
use grit_lib_server::storage::{
    BrowseIndex, CommitGraphStore, ConfigStore, IndexedCommit, IndexedTreeEntry, ObjectStore,
    RefStore, ReflogEntry, ReflogStore, StoredObject, StoredRef,
};
use sqlx::postgres::PgPoolOptions;
use time::OffsetDateTime;

static NEXT_ID: AtomicUsize = AtomicUsize::new(0);

async fn postgres_storage() -> Result<Option<PgServerStorage>> {
    let url = match env::var("GRIT_LIB_SERVER_POSTGRES_URL") {
        Ok(url) => url,
        Err(env::VarError::NotPresent) => return Ok(None),
        Err(env::VarError::NotUnicode(_)) => {
            return Err(Error::Backend(
                "GRIT_LIB_SERVER_POSTGRES_URL is not valid Unicode".to_owned(),
            ));
        }
    };
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&url)
        .await?;
    let storage = PgServerStorage::new(pool);
    storage.migrate().await?;
    Ok(Some(storage))
}

fn ids(label: &str) -> Result<(TenantId, RepositoryId)> {
    let next = NEXT_ID.fetch_add(1, Ordering::Relaxed);
    let process = std::process::id();
    Ok((
        TenantId::new(format!("slice3-{label}-{process}-{next}"))?,
        RepositoryId::new("repo")?,
    ))
}

async fn cleanup(
    storage: &PgServerStorage,
    tenant: &TenantId,
    repository: &RepositoryId,
) -> Result<()> {
    match storage.delete_repository(tenant, repository).await {
        Ok(()) | Err(Error::RepositoryNotFound(_)) => Ok(()),
        Err(error) => Err(error),
    }
}

fn oid(hex: &str) -> Result<ObjectId> {
    Ok(ObjectId::from_hex(hex)?)
}

fn reflog(refname: &str, old_oid: ObjectId, new_oid: ObjectId, message: &str) -> ReflogEntry {
    ReflogEntry {
        refname: refname.to_owned(),
        old_oid,
        new_oid,
        actor: "Slice Three <slice3@example.com>".to_owned(),
        timestamp: OffsetDateTime::UNIX_EPOCH,
        message: message.to_owned(),
    }
}

#[tokio::test]
#[ignore]
async fn migration_creates_hardened_indexes() -> Result<()> {
    let Some(storage) = postgres_storage().await? else {
        return Ok(());
    };

    let count: i64 = sqlx::query_scalar(
        "select count(*) from (
            values
                ('grit_repositories_listing_idx'),
                ('grit_objects_repo_kind_idx'),
                ('grit_refs_repo_prefix_idx'),
                ('grit_tree_entries_repo_prefix_idx'),
                ('grit_commits_repo_time_idx'),
                ('grit_commit_parents_parent_idx')
        ) as expected(name)
        where to_regclass(expected.name) is not null",
    )
    .fetch_one(storage.pool())
    .await?;

    assert_eq!(count, 6);
    Ok(())
}

#[tokio::test]
#[ignore]
async fn repository_lifecycle_scopes_rows() -> Result<()> {
    let Some(storage) = postgres_storage().await? else {
        return Ok(());
    };
    let (tenant, repository) = ids("lifecycle")?;
    let renamed = RepositoryId::new("renamed")?;
    cleanup(&storage, &tenant, &repository).await?;
    cleanup(&storage, &tenant, &renamed).await?;

    let created = storage
        .create_repository(&tenant, &repository, HashAlgo::Sha1)
        .await?;
    assert_eq!(created.repository, repository);
    assert!(matches!(
        storage
            .create_repository(&tenant, &repository, HashAlgo::Sha1)
            .await,
        Err(Error::RepositoryAlreadyExists(_))
    ));

    let object = StoredObject::new(ObjectKind::Blob, b"readme".to_vec());
    let object_oid = object.object_id(HashAlgo::Sha1);
    let tree_oid = oid("1111111111111111111111111111111111111111")?;
    storage
        .write_object(&tenant, &repository, &object_oid, &object)
        .await?;
    storage
        .write_ref(
            &tenant,
            &repository,
            "refs/heads/main",
            &StoredRef::Direct(object_oid),
            Some(None),
        )
        .await?;
    storage
        .set_config(&tenant, &repository, "core.repositoryformatversion", "0")
        .await?;
    storage
        .upsert_tree_entries(
            &tenant,
            &repository,
            &[IndexedTreeEntry {
                tree_oid,
                path: "README.md".to_owned(),
                mode: 0o100644,
                oid: object_oid,
                kind: ObjectKind::Blob,
                size: Some(6),
            }],
        )
        .await?;
    let commit_oid = oid("2222222222222222222222222222222222222222")?;
    storage
        .upsert_commits(
            &tenant,
            &repository,
            &[IndexedCommit {
                oid: commit_oid,
                tree: tree_oid,
                parents: Vec::new(),
                commit_time: 1,
                generation: 1,
            }],
        )
        .await?;

    let renamed_row = storage
        .rename_repository(&tenant, &repository, &renamed)
        .await?;
    assert_eq!(renamed_row.repository, renamed);
    assert_eq!(storage.read_repository(&tenant, &repository).await?, None);
    assert!(
        storage
            .object_exists(&tenant, &renamed, &object_oid)
            .await?
    );
    assert!(
        !storage
            .object_exists(&tenant, &repository, &object_oid)
            .await?
    );
    assert_eq!(
        storage
            .read_ref(&tenant, &renamed, "refs/heads/main")
            .await?,
        Some(StoredRef::Direct(object_oid))
    );
    assert_eq!(
        storage
            .get_config(&tenant, &renamed, "core.repositoryformatversion")
            .await?,
        Some("0".to_owned())
    );
    assert!(storage
        .read_indexed_commit(&tenant, &renamed, &commit_oid)
        .await?
        .is_some());
    assert!(storage
        .read_indexed_commit(&tenant, &repository, &commit_oid)
        .await?
        .is_none());

    let archived = storage.archive_repository(&tenant, &renamed).await?;
    assert!(archived.archived_at.is_some());
    let repositories = storage.list_repositories(&tenant).await?;
    assert_eq!(repositories.len(), 1);
    assert_eq!(repositories[0].repository, renamed);

    storage.delete_repository(&tenant, &renamed).await?;
    assert_eq!(storage.read_repository(&tenant, &renamed).await?, None);
    assert!(
        !storage
            .object_exists(&tenant, &renamed, &object_oid)
            .await?
    );
    assert_eq!(
        storage
            .read_ref(&tenant, &renamed, "refs/heads/main")
            .await?,
        None
    );
    assert!(storage
        .read_indexed_commit(&tenant, &renamed, &commit_oid)
        .await?
        .is_none());
    Ok(())
}

#[tokio::test]
#[ignore]
async fn transaction_rollback_discards_partial_writes() -> Result<()> {
    let Some(storage) = postgres_storage().await? else {
        return Ok(());
    };
    let (tenant, repository) = ids("rollback")?;
    cleanup(&storage, &tenant, &repository).await?;
    storage
        .create_repository(&tenant, &repository, HashAlgo::Sha1)
        .await?;

    let object = StoredObject::new(ObjectKind::Blob, b"rollback".to_vec());
    let object_oid = object.object_id(HashAlgo::Sha1);
    let mut transaction = storage.transaction().await?;
    transaction
        .write_object(&tenant, &repository, &object_oid, &object)
        .await?;
    transaction
        .write_ref(
            &tenant,
            &repository,
            "refs/heads/main",
            &StoredRef::Direct(object_oid),
            Some(None),
        )
        .await?;
    transaction
        .set_config(&tenant, &repository, "slice3.rollback", "present")
        .await?;
    transaction.rollback().await?;

    assert!(
        !storage
            .object_exists(&tenant, &repository, &object_oid)
            .await?
    );
    assert_eq!(
        storage
            .read_ref(&tenant, &repository, "refs/heads/main")
            .await?,
        None
    );
    assert_eq!(
        storage
            .get_config(&tenant, &repository, "slice3.rollback")
            .await?,
        None
    );

    cleanup(&storage, &tenant, &repository).await
}

#[tokio::test]
#[ignore]
async fn object_insert_is_idempotent() -> Result<()> {
    let Some(storage) = postgres_storage().await? else {
        return Ok(());
    };
    let (tenant, repository) = ids("objects")?;
    cleanup(&storage, &tenant, &repository).await?;
    storage
        .create_repository(&tenant, &repository, HashAlgo::Sha1)
        .await?;

    let first = StoredObject::new(ObjectKind::Blob, b"first".to_vec());
    let second = StoredObject::new(ObjectKind::Blob, b"second".to_vec());
    let oid = first.object_id(HashAlgo::Sha1);
    storage
        .write_object(&tenant, &repository, &oid, &first)
        .await?;
    storage
        .write_object(&tenant, &repository, &oid, &second)
        .await?;

    assert_eq!(
        storage.read_object(&tenant, &repository, &oid).await?,
        Some(first)
    );
    assert_eq!(storage.count_objects(&tenant, &repository).await?, 1);

    cleanup(&storage, &tenant, &repository).await
}

#[tokio::test]
#[ignore]
async fn ref_updates_are_atomic_and_reflogged() -> Result<()> {
    let Some(storage) = postgres_storage().await? else {
        return Ok(());
    };
    let (tenant, repository) = ids("refs")?;
    cleanup(&storage, &tenant, &repository).await?;
    storage
        .create_repository(&tenant, &repository, HashAlgo::Sha1)
        .await?;

    let first = oid("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")?;
    let second = oid("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")?;
    let third = oid("cccccccccccccccccccccccccccccccccccccccc")?;
    let refname = "refs/heads/main";

    storage
        .write_ref_with_reflog(
            &tenant,
            &repository,
            refname,
            &StoredRef::Direct(first),
            Some(None),
            &reflog(refname, ObjectId::zero(), first, "create"),
        )
        .await?;
    let conflict = storage
        .write_ref_with_reflog(
            &tenant,
            &repository,
            refname,
            &StoredRef::Direct(second),
            Some(None),
            &reflog(refname, first, second, "conflict"),
        )
        .await;
    assert!(matches!(conflict, Err(Error::RefConflict(_))));
    assert_eq!(
        storage
            .read_reflog(&tenant, &repository, refname)
            .await?
            .len(),
        1
    );

    storage
        .write_ref_with_reflog(
            &tenant,
            &repository,
            refname,
            &StoredRef::Direct(second),
            Some(Some(StoredRef::Direct(first))),
            &reflog(refname, first, second, "update"),
        )
        .await?;
    assert_eq!(
        storage.read_ref(&tenant, &repository, refname).await?,
        Some(StoredRef::Direct(second))
    );
    assert_eq!(
        storage
            .read_reflog(&tenant, &repository, refname)
            .await?
            .len(),
        2
    );

    let race_ref = "refs/heads/race";
    let left_value = StoredRef::Direct(first);
    let right_value = StoredRef::Direct(third);
    let left = storage.write_ref(&tenant, &repository, race_ref, &left_value, Some(None));
    let right = storage.write_ref(&tenant, &repository, race_ref, &right_value, Some(None));
    let (left, right) = tokio::join!(left, right);
    let successes = usize::from(left.is_ok()) + usize::from(right.is_ok());
    let conflicts = usize::from(matches!(left, Err(Error::RefConflict(_))))
        + usize::from(matches!(right, Err(Error::RefConflict(_))));
    assert_eq!(successes, 1);
    assert_eq!(conflicts, 1);

    cleanup(&storage, &tenant, &repository).await
}
