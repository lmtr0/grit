use std::sync::Arc;
use std::time::Duration;

use grit_lib::objects::{ObjectId, ObjectKind};
#[cfg(feature = "nats")]
use grit_lib_server::cache::EventPublisher;
use grit_lib_server::cache::{
    apply_invalidation, Cache, CacheKey, CacheValue, CacheValueKind, InvalidationEvent,
    InvalidationEventKind,
};
use grit_lib_server::cached::CachedStorage;
use grit_lib_server::ids::{RepositoryId, TenantId};
use grit_lib_server::layered::LayeredCache;
use grit_lib_server::memory::MemoryBackend;
use grit_lib_server::storage::{
    BrowseIndex, CommitGraphStore, ConfigStore, IndexedCommit, IndexedTreeEntry, RefStore,
    StoredRef,
};

fn ids() -> grit_lib_server::error::Result<(TenantId, RepositoryId)> {
    Ok((TenantId::new("tenant-a")?, RepositoryId::new("repo-a")?))
}

fn oid(hex: &str) -> grit_lib_server::error::Result<ObjectId> {
    Ok(ObjectId::from_hex(hex)?)
}

#[tokio::test]
async fn layered_cache_reads_through_far_cache_and_fills_near() -> grit_lib_server::error::Result<()>
{
    let (tenant, repository) = ids()?;
    let near = Arc::new(MemoryBackend::new());
    let far = Arc::new(MemoryBackend::new());
    let layered = LayeredCache::new(near.clone(), far.clone());
    let key = CacheKey::Object("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned());
    let value = CacheValue::typed(
        CacheValueKind::Object,
        br#"{"kind":"blob","data":[104,105]}"#,
    );

    far.put(&tenant, &repository, &key, value.clone()).await?;
    assert_eq!(near.get(&tenant, &repository, &key).await?, None);
    assert_eq!(
        layered.get(&tenant, &repository, &key).await?,
        Some(value.clone())
    );
    assert_eq!(near.get(&tenant, &repository, &key).await?, Some(value));

    Ok(())
}

#[tokio::test]
async fn invalidation_event_round_trips_and_invalidates_ref_lists(
) -> grit_lib_server::error::Result<()> {
    let (tenant, repository) = ids()?;
    let event = InvalidationEvent::new(
        tenant.clone(),
        repository.clone(),
        InvalidationEventKind::RefWrite {
            refname: "refs/heads/main".to_owned(),
        },
    );
    let decoded = InvalidationEvent::from_json_slice(&event.to_json_bytes()?)?;
    assert_eq!(decoded, event);

    let cache = MemoryBackend::new();
    for key in [
        CacheKey::Ref("refs/heads/main".to_owned()),
        CacheKey::RefList(String::new()),
        CacheKey::RefList("refs/".to_owned()),
        CacheKey::RefList("refs/heads/".to_owned()),
    ] {
        cache
            .put(
                &tenant,
                &repository,
                &key,
                CacheValue::typed(CacheValueKind::Raw, b"stale"),
            )
            .await?;
    }

    apply_invalidation(&cache, &event).await?;
    for key in event.cache_keys() {
        assert_eq!(cache.get(&tenant, &repository, &key).await?, None);
    }

    Ok(())
}

#[tokio::test]
async fn repository_delete_event_clears_scoped_cache_entries() -> grit_lib_server::error::Result<()>
{
    let (tenant, repository) = ids()?;
    let other = RepositoryId::new("repo-b")?;
    let cache = MemoryBackend::new();
    let key = CacheKey::Config("core.defaultBranch".to_owned());

    cache
        .put(
            &tenant,
            &repository,
            &key,
            CacheValue::typed(CacheValueKind::Config, br#""main""#),
        )
        .await?;
    cache
        .put(
            &tenant,
            &other,
            &key,
            CacheValue::typed(CacheValueKind::Config, br#""main""#),
        )
        .await?;

    apply_invalidation(
        &cache,
        &InvalidationEvent::new(
            tenant.clone(),
            repository.clone(),
            InvalidationEventKind::RepositoryDelete,
        ),
    )
    .await?;

    assert_eq!(cache.get(&tenant, &repository, &key).await?, None);
    assert!(cache.get(&tenant, &other, &key).await?.is_some());

    Ok(())
}

#[tokio::test]
async fn cached_storage_caches_and_invalidates_ref_lists() -> grit_lib_server::error::Result<()> {
    let (tenant, repository) = ids()?;
    let durable = Arc::new(MemoryBackend::new());
    let cache = Arc::new(MemoryBackend::new());
    let cached = CachedStorage::new(durable.clone(), cache);
    let first = StoredRef::Direct(oid("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")?);
    let second = StoredRef::Direct(oid("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")?);

    durable
        .write_ref(&tenant, &repository, "refs/heads/main", &first, None)
        .await?;
    assert_eq!(
        cached
            .list_refs(&tenant, &repository, "refs/heads/")
            .await?,
        vec![("refs/heads/main".to_owned(), first.clone())]
    );

    durable
        .write_ref(&tenant, &repository, "refs/heads/next", &second, None)
        .await?;
    assert_eq!(
        cached
            .list_refs(&tenant, &repository, "refs/heads/")
            .await?,
        vec![("refs/heads/main".to_owned(), first.clone())]
    );

    cached
        .write_ref(&tenant, &repository, "refs/heads/main", &first, None)
        .await?;
    assert_eq!(
        cached
            .list_refs(&tenant, &repository, "refs/heads/")
            .await?,
        vec![
            ("refs/heads/main".to_owned(), first),
            ("refs/heads/next".to_owned(), second)
        ]
    );

    Ok(())
}

#[tokio::test]
async fn cached_storage_caches_tree_lists_and_rebuild_invalidates_repository(
) -> grit_lib_server::error::Result<()> {
    let (tenant, repository) = ids()?;
    let durable = Arc::new(MemoryBackend::new());
    let cache = Arc::new(MemoryBackend::new());
    let cached = CachedStorage::new(durable.clone(), cache);
    let tree = oid("1111111111111111111111111111111111111111")?;
    let first = IndexedTreeEntry {
        tree_oid: tree,
        path: "README.md".to_owned(),
        mode: 0o100644,
        oid: oid("2222222222222222222222222222222222222222")?,
        kind: ObjectKind::Blob,
        size: Some(6),
    };
    let second = IndexedTreeEntry {
        tree_oid: tree,
        path: "src/main.rs".to_owned(),
        mode: 0o100644,
        oid: oid("3333333333333333333333333333333333333333")?,
        kind: ObjectKind::Blob,
        size: Some(12),
    };

    durable
        .upsert_tree_entries(&tenant, &repository, std::slice::from_ref(&first))
        .await?;
    assert_eq!(
        cached
            .list_tree_entries(&tenant, &repository, &tree, "")
            .await?,
        vec![first.clone()]
    );
    durable
        .upsert_tree_entries(&tenant, &repository, std::slice::from_ref(&second))
        .await?;
    assert_eq!(
        cached
            .list_tree_entries(&tenant, &repository, &tree, "")
            .await?,
        vec![first.clone()]
    );

    cached
        .upsert_tree_entries(&tenant, &repository, std::slice::from_ref(&second))
        .await?;
    assert_eq!(
        cached
            .list_tree_entries(&tenant, &repository, &tree, "")
            .await?,
        vec![first, second]
    );

    Ok(())
}

#[tokio::test]
async fn cached_storage_caches_config_and_indexed_commits() -> grit_lib_server::error::Result<()> {
    let (tenant, repository) = ids()?;
    let durable = Arc::new(MemoryBackend::new());
    let cache = Arc::new(MemoryBackend::new());
    let cached = CachedStorage::new(durable.clone(), cache);
    let commit = IndexedCommit {
        oid: oid("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")?,
        tree: oid("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")?,
        parents: vec![oid("cccccccccccccccccccccccccccccccccccccccc")?],
        commit_time: 1_700_000_000,
        generation: 2,
    };

    durable
        .set_config(&tenant, &repository, "core.defaultBranch", "main")
        .await?;
    assert_eq!(
        cached
            .get_config(&tenant, &repository, "core.defaultBranch")
            .await?,
        Some("main".to_owned())
    );
    durable
        .set_config(&tenant, &repository, "core.defaultBranch", "next")
        .await?;
    assert_eq!(
        cached
            .get_config(&tenant, &repository, "core.defaultBranch")
            .await?,
        Some("main".to_owned())
    );

    durable
        .upsert_commits(&tenant, &repository, std::slice::from_ref(&commit))
        .await?;
    assert_eq!(
        cached
            .read_indexed_commit(&tenant, &repository, &commit.oid)
            .await?,
        Some(commit.clone())
    );

    Ok(())
}

#[cfg(feature = "redis")]
#[tokio::test]
#[ignore = "requires GRIT_LIB_SERVER_REDIS_URL and a live Redis service"]
async fn redis_cache_live_round_trip() -> grit_lib_server::error::Result<()> {
    let Ok(url) = std::env::var("GRIT_LIB_SERVER_REDIS_URL") else {
        return Ok(());
    };
    let (tenant, repository) = ids()?;
    let cache = grit_lib_server::redis_cache::RedisCache::connect_with_options(
        &url,
        grit_lib_server::redis_cache::RedisCacheOptions {
            connection_timeout: Duration::from_secs(1),
            response_timeout: Duration::from_secs(1),
            connection_retries: 0,
            ..grit_lib_server::redis_cache::RedisCacheOptions::default()
        },
    )
    .await?;
    let key = CacheKey::Config("core.defaultBranch".to_owned());
    let value = CacheValue::typed(CacheValueKind::Config, br#""main""#);

    cache.invalidate_repository(&tenant, &repository).await?;
    cache.put(&tenant, &repository, &key, value.clone()).await?;
    assert_eq!(cache.get(&tenant, &repository, &key).await?, Some(value));
    cache.invalidate(&tenant, &repository, &key).await?;
    assert_eq!(cache.get(&tenant, &repository, &key).await?, None);

    Ok(())
}

#[cfg(feature = "nats")]
#[tokio::test]
#[ignore = "requires GRIT_LIB_SERVER_NATS_URL and a live NATS service"]
async fn nats_publisher_live_smoke() -> grit_lib_server::error::Result<()> {
    let Ok(url) = std::env::var("GRIT_LIB_SERVER_NATS_URL") else {
        return Ok(());
    };
    let (tenant, repository) = ids()?;
    let publisher =
        grit_lib_server::nats_invalidation::NatsInvalidationPublisher::connect(&url, "grit.test")
            .await?;
    publisher
        .publish_invalidation(InvalidationEvent::new(
            tenant,
            repository,
            InvalidationEventKind::ConfigUpdate {
                key: "core.defaultBranch".to_owned(),
            },
        ))
        .await?;

    Ok(())
}
