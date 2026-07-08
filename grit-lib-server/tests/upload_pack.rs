use std::sync::Arc;

use grit_lib::objects::{
    serialize_commit, serialize_tag, serialize_tree, CommitData, HashAlgo, ObjectId, ObjectKind,
    TagData, TreeEntry,
};
use grit_lib::pkt_line::{self, Packet};
use grit_lib_server::ids::{RepositoryId, TenantId};
use grit_lib_server::memory::MemoryBackend;
use grit_lib_server::protocol::upload_pack::{
    UploadPackCapability, UploadPackRequest, UploadPackService,
};
use grit_lib_server::repository::ServerRepository;
use grit_lib_server::storage::{RefStore, StoredObject, StoredRef};

fn ids() -> grit_lib_server::error::Result<(TenantId, RepositoryId)> {
    Ok((TenantId::new("tenant-a")?, RepositoryId::new("repo-a")?))
}

struct UploadFixture {
    repo: ServerRepository<MemoryBackend>,
    base_blob: ObjectId,
    base_tree: ObjectId,
    base_commit: ObjectId,
    head_blob: ObjectId,
    head_tree: ObjectId,
    head_commit: ObjectId,
    tag: ObjectId,
}

async fn upload_fixture() -> grit_lib_server::error::Result<UploadFixture> {
    let (tenant, repository) = ids()?;
    let backend = Arc::new(MemoryBackend::new());
    let repo = ServerRepository::new(tenant, repository, HashAlgo::Sha1, backend);

    let base_blob = repo
        .write_object(&StoredObject::new(ObjectKind::Blob, b"base\n"))
        .await?;
    let base_tree_data = serialize_tree(&[TreeEntry {
        mode: 0o100644,
        name: b"README.md".to_vec(),
        oid: base_blob,
    }]);
    let base_tree = repo
        .write_object(&StoredObject::new(ObjectKind::Tree, base_tree_data))
        .await?;
    let ident = "A U Thor <a@example.com> 1700000000 +0000".to_owned();
    let base_commit_data = CommitData {
        tree: base_tree,
        parents: Vec::new(),
        author: ident.clone(),
        committer: ident.clone(),
        author_raw: Vec::new(),
        committer_raw: Vec::new(),
        encoding: None,
        message: "base\n".to_owned(),
        raw_message: None,
    };
    let base_commit = repo
        .write_object(&StoredObject::new(
            ObjectKind::Commit,
            serialize_commit(&base_commit_data),
        ))
        .await?;

    let head_blob = repo
        .write_object(&StoredObject::new(ObjectKind::Blob, b"head\n"))
        .await?;
    let head_tree_data = serialize_tree(&[TreeEntry {
        mode: 0o100644,
        name: b"README.md".to_vec(),
        oid: head_blob,
    }]);
    let head_tree = repo
        .write_object(&StoredObject::new(ObjectKind::Tree, head_tree_data))
        .await?;
    let head_commit_data = CommitData {
        tree: head_tree,
        parents: vec![base_commit],
        author: ident.clone(),
        committer: ident.clone(),
        author_raw: Vec::new(),
        committer_raw: Vec::new(),
        encoding: None,
        message: "head\n".to_owned(),
        raw_message: None,
    };
    let head_commit = repo
        .write_object(&StoredObject::new(
            ObjectKind::Commit,
            serialize_commit(&head_commit_data),
        ))
        .await?;
    let tag_data = TagData {
        object: head_commit,
        object_type: "commit".to_owned(),
        tag: "v1.0.0".to_owned(),
        tagger: Some(ident),
        message: "release\n".to_owned(),
    };
    let tag = repo
        .write_object(&StoredObject::new(
            ObjectKind::Tag,
            serialize_tag(&tag_data),
        ))
        .await?;

    repo.storage()
        .write_ref(
            repo.tenant(),
            repo.repository(),
            "HEAD",
            &StoredRef::Symbolic("refs/heads/main".to_owned()),
            None,
        )
        .await?;
    repo.storage()
        .write_ref(
            repo.tenant(),
            repo.repository(),
            "refs/heads/main",
            &StoredRef::Direct(head_commit),
            None,
        )
        .await?;
    repo.storage()
        .write_ref(
            repo.tenant(),
            repo.repository(),
            "refs/tags/v1.0.0",
            &StoredRef::Direct(tag),
            None,
        )
        .await?;

    Ok(UploadFixture {
        repo,
        base_blob,
        base_tree,
        base_commit,
        head_blob,
        head_tree,
        head_commit,
        tag,
    })
}

fn pack_object_count(pack: &[u8]) -> grit_lib_server::error::Result<usize> {
    if pack.len() < 12 || &pack[..4] != b"PACK" {
        return Err(grit_lib_server::error::Error::Protocol(
            "invalid pack header".to_owned(),
        ));
    }
    let count = u32::from_be_bytes([pack[8], pack[9], pack[10], pack[11]]);
    Ok(count as usize)
}

#[tokio::test]
async fn advertises_refs_with_capabilities_and_peeled_tags() -> grit_lib_server::error::Result<()> {
    let fixture = upload_fixture().await?;
    let advertisement = fixture.repo.advertise_refs().await?;

    assert!(advertisement
        .capabilities
        .contains(&UploadPackCapability::SideBand64k));
    assert!(advertisement.refs.iter().any(|advertised| {
        advertised.name == "HEAD" && advertised.oid == fixture.head_commit && !advertised.peeled
    }));
    assert!(advertisement.refs.iter().any(|advertised| {
        advertised.name == "refs/heads/main"
            && advertised.oid == fixture.head_commit
            && !advertised.peeled
    }));
    assert!(advertisement.refs.iter().any(|advertised| {
        advertised.name == "refs/tags/v1.0.0" && advertised.oid == fixture.tag && !advertised.peeled
    }));
    assert!(advertisement.refs.iter().any(|advertised| {
        advertised.name == "refs/tags/v1.0.0^{}"
            && advertised.oid == fixture.head_commit
            && advertised.peeled
    }));

    let pkt_lines = advertisement.to_pkt_lines(HashAlgo::Sha1)?;
    assert!(pkt_lines
        .windows(b"HEAD\0multi_ack".len())
        .any(|window| { window == b"HEAD\0multi_ack" }));
    assert!(pkt_lines.ends_with(pkt_line::FLUSH.as_bytes()));

    Ok(())
}

#[tokio::test]
async fn advertises_empty_repository_capabilities() -> grit_lib_server::error::Result<()> {
    let (tenant, repository) = ids()?;
    let repo = ServerRepository::new(
        tenant,
        repository,
        HashAlgo::Sha1,
        Arc::new(MemoryBackend::new()),
    );

    let advertisement = repo.advertise_refs().await?;
    assert!(advertisement.refs.is_empty());
    let pkt_lines = advertisement.to_pkt_lines(HashAlgo::Sha1)?;
    assert!(String::from_utf8_lossy(&pkt_lines).contains("capabilities^{}"));
    assert!(String::from_utf8_lossy(&pkt_lines).contains("side-band-64k"));

    Ok(())
}

#[tokio::test]
async fn parses_v0_wants_haves_and_capabilities() -> grit_lib_server::error::Result<()> {
    let fixture = upload_fixture().await?;
    let mut input = Vec::new();
    pkt_line::write_line_to_vec(
        &mut input,
        &format!(
            "want {} multi_ack side-band-64k thin-pack",
            fixture.head_commit.to_hex()
        ),
    )?;
    pkt_line::write_line_to_vec(
        &mut input,
        &format!("have {}", fixture.base_commit.to_hex()),
    )?;
    pkt_line::write_line_to_vec(&mut input, "done")?;
    pkt_line::write_flush(&mut input)?;

    let request = UploadPackRequest::parse_v0(&input)?;
    assert_eq!(request.wants, vec![fixture.head_commit]);
    assert_eq!(request.haves, vec![fixture.base_commit]);
    assert!(request.done);
    assert!(request
        .capabilities
        .contains(&UploadPackCapability::MultiAck));
    assert!(request.wants_sideband64k());

    Ok(())
}

#[tokio::test]
async fn negotiates_object_closure_excluding_common_haves() -> grit_lib_server::error::Result<()> {
    let fixture = upload_fixture().await?;
    let request = UploadPackRequest {
        wants: vec![fixture.head_commit],
        haves: vec![fixture.base_commit],
        capabilities: vec![UploadPackCapability::SideBand64k],
        done: true,
        ..UploadPackRequest::default()
    };

    let plan = fixture.repo.negotiate_fetch(request).await?;
    assert_eq!(plan.common_haves, vec![fixture.base_commit]);
    assert!(plan.objects.contains(&fixture.head_commit));
    assert!(plan.objects.contains(&fixture.head_tree));
    assert!(plan.objects.contains(&fixture.head_blob));
    assert!(!plan.objects.contains(&fixture.base_commit));
    assert!(!plan.objects.contains(&fixture.base_tree));
    assert!(!plan.objects.contains(&fixture.base_blob));

    let response = fixture.repo.build_fetch_pack(plan.clone()).await?;
    assert_eq!(pack_object_count(&response.pack)?, plan.objects.len());
    assert!(response.sideband);
    let mut reader = response.wire_response.as_slice();
    assert_eq!(
        pkt_line::read_packet(&mut reader)?,
        Some(Packet::Data(format!(
            "ACK {}",
            fixture.base_commit.to_hex()
        )))
    );
    let primary = pkt_line::decode_sideband_primary(reader)?;
    assert_eq!(primary, response.pack);

    Ok(())
}

#[tokio::test]
async fn tag_want_includes_tag_target_closure() -> grit_lib_server::error::Result<()> {
    let fixture = upload_fixture().await?;
    let service = UploadPackService::new(fixture.repo.clone());
    let request = UploadPackRequest {
        wants: vec![fixture.tag],
        capabilities: Vec::new(),
        ..UploadPackRequest::default()
    };

    let plan = service.negotiate_fetch(request).await?;
    assert!(plan.objects.contains(&fixture.tag));
    assert!(plan.objects.contains(&fixture.head_commit));
    assert!(plan.objects.contains(&fixture.head_tree));
    assert!(plan.objects.contains(&fixture.head_blob));

    let response = service.build_fetch_pack(plan.clone()).await?;
    assert_eq!(pack_object_count(&response.pack)?, plan.objects.len());
    assert!(!response.sideband);
    assert!(response
        .wire_response
        .windows(b"PACK".len())
        .any(|window| { window == b"PACK" }));

    Ok(())
}
