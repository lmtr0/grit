use std::sync::Arc;

use grit_lib::objects::{HashAlgo, ObjectKind};
use grit_lib_server::external::{ExternalByteStore, MemoryByteStore};
use grit_lib_server::ids::{RepositoryId, TenantId};
use grit_lib_server::memory::MemoryBackend;
use grit_lib_server::repository::ServerRepository;
use grit_lib_server::storage::{PackStore, StoredObject};

fn ids() -> grit_lib_server::error::Result<(TenantId, RepositoryId)> {
    Ok((TenantId::new("tenant-a")?, RepositoryId::new("repo-a")?))
}

#[tokio::test]
async fn memory_byte_store_is_idempotent_and_supports_ranges() -> grit_lib_server::error::Result<()>
{
    let store = MemoryByteStore::new();

    store.put_if_absent("payload", b"abcdef").await?;
    store.put_if_absent("payload", b"ignored").await?;

    assert_eq!(store.len()?, 1);
    assert_eq!(store.get("payload").await?, Some(b"abcdef".to_vec()));
    assert_eq!(
        store.get_range("payload", 2, 3).await?,
        Some(b"cde".to_vec())
    );
    assert_eq!(store.get_range("payload", 20, 3).await?, Some(Vec::new()));

    store.delete("payload").await?;
    assert!(store.is_empty()?);
    Ok(())
}

#[tokio::test]
async fn pack_store_default_range_reads_slice_from_full_pack() -> grit_lib_server::error::Result<()>
{
    let (tenant, repository) = ids()?;
    let backend = Arc::new(MemoryBackend::new());
    let repo = ServerRepository::new(
        tenant.clone(),
        repository.clone(),
        HashAlgo::Sha1,
        backend.clone(),
    );
    let object = StoredObject::new(ObjectKind::Blob, b"packed\n".to_vec());
    let oid = repo.write_object(&object).await?;
    let pack = repo.build_pack(&[oid]).await?;
    let metadata = repo.write_pack(&pack).await?;

    let range = backend
        .read_pack_range(&tenant, &repository, &metadata.pack_checksum, 0, 4)
        .await?
        .ok_or_else(|| grit_lib_server::error::Error::ObjectNotFound("pack range".to_owned()))?;
    assert_eq!(range, b"PACK");
    Ok(())
}
