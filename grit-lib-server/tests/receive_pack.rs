use std::sync::Arc;

use async_trait::async_trait;
use flate2::write::ZlibEncoder;
use grit_lib::objects::{
    serialize_commit, serialize_tag, serialize_tree, CommitData, HashAlgo, ObjectId, ObjectKind,
    TagData, TreeEntry,
};
use grit_lib::pkt_line;
use grit_lib_server::cache::InvalidationEventKind;
use grit_lib_server::error::{Error, Result};
use grit_lib_server::ids::{RepositoryId, TenantId};
use grit_lib_server::memory::MemoryBackend;
use grit_lib_server::protocol::receive_pack::{
    AllowAllPushPolicy, PushPolicy, PushPolicyContext, ReceivePackCapability, ReceivePackCommand,
    ReceivePackRequest,
};
use grit_lib_server::repository::ServerRepository;
use grit_lib_server::storage::{RefStore, ReflogStore, StoredObject, StoredRef};
use sha1::{Digest as _, Sha1};
use time::OffsetDateTime;

fn ids() -> Result<(TenantId, RepositoryId)> {
    Ok((TenantId::new("tenant-a")?, RepositoryId::new("repo-a")?))
}

struct ReceiveFixture {
    repo: ServerRepository<MemoryBackend>,
    backend: Arc<MemoryBackend>,
    base_commit: ObjectId,
}

async fn receive_fixture() -> Result<ReceiveFixture> {
    let (tenant, repository) = ids()?;
    let backend = Arc::new(MemoryBackend::new());
    let repo = ServerRepository::new(tenant, repository, HashAlgo::Sha1, backend.clone());

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
    let base_commit = commit_object(base_tree, Vec::new(), 1_700_000_000, "base");
    let base_commit_oid = repo.write_object(&base_commit).await?;

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
            &StoredRef::Direct(base_commit_oid),
            None,
        )
        .await?;

    Ok(ReceiveFixture {
        repo,
        backend,
        base_commit: base_commit_oid,
    })
}

fn commit_object(
    tree: ObjectId,
    parents: Vec<ObjectId>,
    timestamp: i64,
    message: &str,
) -> StoredObject {
    let ident = format!("A U Thor <a@example.com> {timestamp} +0000");
    StoredObject::new(
        ObjectKind::Commit,
        serialize_commit(&CommitData {
            tree,
            parents,
            author: ident.clone(),
            committer: ident,
            author_raw: Vec::new(),
            committer_raw: Vec::new(),
            encoding: None,
            message: format!("{message}\n"),
            raw_message: None,
        }),
    )
}

fn advanced_objects(
    parent: ObjectId,
    contents: &[u8],
) -> (
    StoredObject,
    ObjectId,
    StoredObject,
    ObjectId,
    StoredObject,
    ObjectId,
) {
    let blob = StoredObject::new(ObjectKind::Blob, contents);
    let blob_oid = blob.object_id(HashAlgo::Sha1);
    let tree = StoredObject::new(
        ObjectKind::Tree,
        serialize_tree(&[TreeEntry {
            mode: 0o100644,
            name: b"README.md".to_vec(),
            oid: blob_oid,
        }]),
    );
    let tree_oid = tree.object_id(HashAlgo::Sha1);
    let commit = commit_object(tree_oid, vec![parent], 1_700_000_100, "advance");
    let commit_oid = commit.object_id(HashAlgo::Sha1);
    (blob, blob_oid, tree, tree_oid, commit, commit_oid)
}

fn pack(objects: &[StoredObject]) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    out.extend_from_slice(b"PACK");
    out.extend_from_slice(&2u32.to_be_bytes());
    let count = u32::try_from(objects.len())
        .map_err(|_| Error::Protocol("too many test objects".to_owned()))?;
    out.extend_from_slice(&count.to_be_bytes());

    for object in objects {
        encode_pack_object_header(&mut out, pack_type_code(object.kind), object.data.len());
        let mut encoder = ZlibEncoder::new(Vec::new(), flate2::Compression::default());
        std::io::Write::write_all(&mut encoder, &object.data)?;
        out.extend_from_slice(&encoder.finish()?);
    }

    let mut hasher = Sha1::new();
    hasher.update(&out);
    out.extend_from_slice(&hasher.finalize());
    Ok(out)
}

fn request(commands: Vec<ReceivePackCommand>, pack: Vec<u8>) -> ReceivePackRequest {
    ReceivePackRequest {
        commands,
        capabilities: vec![ReceivePackCapability::ReportStatus],
        pack,
    }
}

fn command(old_oid: ObjectId, new_oid: ObjectId, refname: &str) -> ReceivePackCommand {
    ReceivePackCommand {
        old_oid,
        new_oid,
        refname: refname.to_owned(),
    }
}

fn timestamp() -> Result<OffsetDateTime> {
    OffsetDateTime::from_unix_timestamp(1_700_000_200)
        .map_err(|err| Error::Backend(err.to_string()))
}

#[tokio::test]
async fn parses_receive_pack_commands_capabilities_and_pack() -> Result<()> {
    let fixture = receive_fixture().await?;
    let mut input = Vec::new();
    pkt_line::write_packet_raw(
        &mut input,
        format!(
            "{} {} refs/heads/main\0report-status side-band-64k\n",
            fixture.base_commit.to_hex(),
            fixture.base_commit.to_hex()
        )
        .as_bytes(),
    )?;
    pkt_line::write_flush(&mut input)?;
    input.extend_from_slice(b"PACK");

    let parsed = ReceivePackRequest::parse_v0(&input)?;
    assert_eq!(parsed.commands.len(), 1);
    assert_eq!(parsed.commands[0].old_oid, fixture.base_commit);
    assert_eq!(parsed.commands[0].new_oid, fixture.base_commit);
    assert_eq!(parsed.commands[0].refname, "refs/heads/main");
    assert!(parsed
        .capabilities
        .contains(&ReceivePackCapability::ReportStatus));
    assert!(parsed
        .capabilities
        .contains(&ReceivePackCapability::SideBand64k));
    assert_eq!(parsed.pack, b"PACK");

    Ok(())
}

#[tokio::test]
async fn fast_forward_push_updates_ref_reflog_and_events() -> Result<()> {
    let fixture = receive_fixture().await?;
    let (blob, _, tree, _, commit, commit_oid) = advanced_objects(fixture.base_commit, b"next\n");
    let request = request(
        vec![command(fixture.base_commit, commit_oid, "refs/heads/main")],
        pack(&[blob, tree, commit])?,
    );

    let plan = fixture
        .repo
        .prepare_push(request, &AllowAllPushPolicy)
        .await?;
    assert!(fixture.repo.read_object(&commit_oid).await?.is_none());

    let report = fixture
        .repo
        .apply_push(
            plan,
            "tester <tester@example.com>",
            timestamp()?,
            fixture.backend.as_ref(),
        )
        .await?;
    assert_eq!(report.unpacked_objects, 3);
    assert_eq!(
        fixture.repo.resolve_ref("refs/heads/main").await?,
        Some(commit_oid)
    );
    assert!(fixture.repo.read_object(&commit_oid).await?.is_some());

    let reflog = fixture
        .backend
        .read_reflog(
            fixture.repo.tenant(),
            fixture.repo.repository(),
            "refs/heads/main",
        )
        .await?;
    assert_eq!(reflog.len(), 1);
    assert_eq!(reflog[0].old_oid, fixture.base_commit);
    assert_eq!(reflog[0].new_oid, commit_oid);

    let events = fixture.backend.events()?;
    assert!(events.iter().any(|event| {
        matches!(
            &event.kind,
            InvalidationEventKind::RefWrite { refname } if refname == "refs/heads/main"
        )
    }));

    Ok(())
}

#[tokio::test]
async fn non_fast_forward_branch_update_is_rejected() -> Result<()> {
    let fixture = receive_fixture().await?;
    let blob = StoredObject::new(ObjectKind::Blob, b"other\n");
    let blob_oid = blob.object_id(HashAlgo::Sha1);
    let tree = StoredObject::new(
        ObjectKind::Tree,
        serialize_tree(&[TreeEntry {
            mode: 0o100644,
            name: b"README.md".to_vec(),
            oid: blob_oid,
        }]),
    );
    let tree_oid = tree.object_id(HashAlgo::Sha1);
    let commit = commit_object(tree_oid, Vec::new(), 1_700_000_100, "side");
    let commit_oid = commit.object_id(HashAlgo::Sha1);
    let request = request(
        vec![command(fixture.base_commit, commit_oid, "refs/heads/main")],
        pack(&[blob, tree, commit])?,
    );

    let err = fixture
        .repo
        .prepare_push(request, &AllowAllPushPolicy)
        .await
        .err()
        .ok_or_else(|| Error::Backend("non-fast-forward push unexpectedly passed".to_owned()))?;
    assert!(matches!(
        err,
        Error::NonFastForward { refname, .. } if refname == "refs/heads/main"
    ));
    assert_eq!(
        fixture.repo.resolve_ref("refs/heads/main").await?,
        Some(fixture.base_commit)
    );

    Ok(())
}

#[tokio::test]
async fn creates_and_deletes_branch() -> Result<()> {
    let fixture = receive_fixture().await?;
    let (blob, _, tree, _, commit, commit_oid) = advanced_objects(fixture.base_commit, b"topic\n");
    let create = request(
        vec![command(
            ObjectId::null(HashAlgo::Sha1),
            commit_oid,
            "refs/heads/topic",
        )],
        pack(&[blob, tree, commit])?,
    );
    let create_plan = fixture
        .repo
        .prepare_push(create, &AllowAllPushPolicy)
        .await?;
    fixture
        .repo
        .apply_push(
            create_plan,
            "tester <tester@example.com>",
            timestamp()?,
            fixture.backend.as_ref(),
        )
        .await?;
    assert_eq!(
        fixture.repo.resolve_ref("refs/heads/topic").await?,
        Some(commit_oid)
    );

    let delete = request(
        vec![command(
            commit_oid,
            ObjectId::null(HashAlgo::Sha1),
            "refs/heads/topic",
        )],
        Vec::new(),
    );
    let delete_plan = fixture
        .repo
        .prepare_push(delete, &AllowAllPushPolicy)
        .await?;
    fixture
        .repo
        .apply_push(
            delete_plan,
            "tester <tester@example.com>",
            timestamp()?,
            fixture.backend.as_ref(),
        )
        .await?;
    assert_eq!(fixture.repo.read_ref("refs/heads/topic").await?, None);

    Ok(())
}

#[tokio::test]
async fn updates_tag_without_fast_forward_check() -> Result<()> {
    let fixture = receive_fixture().await?;
    let tag_data = TagData {
        object: fixture.base_commit,
        object_type: "commit".to_owned(),
        tag: "v1".to_owned(),
        tagger: Some("A U Thor <a@example.com> 1700000000 +0000".to_owned()),
        message: "v1\n".to_owned(),
    };
    let tag = StoredObject::new(ObjectKind::Tag, serialize_tag(&tag_data));
    let tag_oid = fixture.repo.write_object(&tag).await?;
    fixture
        .repo
        .storage()
        .write_ref(
            fixture.repo.tenant(),
            fixture.repo.repository(),
            "refs/tags/v1",
            &StoredRef::Direct(tag_oid),
            None,
        )
        .await?;

    let replacement = StoredObject::new(ObjectKind::Blob, b"tag payload\n");
    let replacement_oid = replacement.object_id(HashAlgo::Sha1);
    let update = request(
        vec![command(tag_oid, replacement_oid, "refs/tags/v1")],
        pack(&[replacement])?,
    );
    let plan = fixture
        .repo
        .prepare_push(update, &AllowAllPushPolicy)
        .await?;
    fixture
        .repo
        .apply_push(
            plan,
            "tester <tester@example.com>",
            timestamp()?,
            fixture.backend.as_ref(),
        )
        .await?;
    assert_eq!(
        fixture.repo.resolve_ref("refs/tags/v1").await?,
        Some(replacement_oid)
    );

    Ok(())
}

#[tokio::test]
async fn missing_object_is_rejected_before_refs_change() -> Result<()> {
    let fixture = receive_fixture().await?;
    let missing = ObjectId::from_hex("1111111111111111111111111111111111111111")?;
    let push = request(
        vec![command(
            ObjectId::null(HashAlgo::Sha1),
            missing,
            "refs/heads/missing",
        )],
        Vec::new(),
    );

    let err = fixture
        .repo
        .prepare_push(push, &AllowAllPushPolicy)
        .await
        .err()
        .ok_or_else(|| Error::Backend("missing object push unexpectedly passed".to_owned()))?;
    assert!(matches!(err, Error::ObjectNotFound(_)));
    assert_eq!(fixture.repo.read_ref("refs/heads/missing").await?, None);

    Ok(())
}

#[derive(Default)]
struct RejectAllPolicy;

#[async_trait]
impl PushPolicy for RejectAllPolicy {
    async fn check(&self, context: &PushPolicyContext) -> Result<()> {
        Err(Error::PushPolicyRejected {
            refname: context.refname.clone(),
            reason: "test policy".to_owned(),
        })
    }
}

#[tokio::test]
async fn policy_rejection_blocks_push() -> Result<()> {
    let fixture = receive_fixture().await?;
    let (blob, _, tree, _, commit, commit_oid) = advanced_objects(fixture.base_commit, b"next\n");
    let push = request(
        vec![command(fixture.base_commit, commit_oid, "refs/heads/main")],
        pack(&[blob, tree, commit])?,
    );

    let err = fixture
        .repo
        .prepare_push(push, &RejectAllPolicy)
        .await
        .err()
        .ok_or_else(|| Error::Backend("policy rejection unexpectedly passed".to_owned()))?;
    assert!(matches!(
        err,
        Error::PushPolicyRejected { refname, .. } if refname == "refs/heads/main"
    ));
    assert_eq!(
        fixture.repo.resolve_ref("refs/heads/main").await?,
        Some(fixture.base_commit)
    );

    Ok(())
}

#[tokio::test]
async fn concurrent_push_conflict_is_typed() -> Result<()> {
    let fixture = receive_fixture().await?;
    let (left_blob, _, left_tree, _, left_commit, left_oid) =
        advanced_objects(fixture.base_commit, b"left\n");
    let (right_blob, _, right_tree, _, right_commit, right_oid) =
        advanced_objects(fixture.base_commit, b"right\n");

    let left = request(
        vec![command(fixture.base_commit, left_oid, "refs/heads/main")],
        pack(&[left_blob, left_tree, left_commit])?,
    );
    let right = request(
        vec![command(fixture.base_commit, right_oid, "refs/heads/main")],
        pack(&[right_blob, right_tree, right_commit])?,
    );
    let left_plan = fixture.repo.prepare_push(left, &AllowAllPushPolicy).await?;
    let right_plan = fixture
        .repo
        .prepare_push(right, &AllowAllPushPolicy)
        .await?;

    fixture
        .repo
        .apply_push(
            left_plan,
            "tester <tester@example.com>",
            timestamp()?,
            fixture.backend.as_ref(),
        )
        .await?;
    let err = fixture
        .repo
        .apply_push(
            right_plan,
            "tester <tester@example.com>",
            timestamp()?,
            fixture.backend.as_ref(),
        )
        .await
        .err()
        .ok_or_else(|| Error::Backend("stale push unexpectedly passed".to_owned()))?;
    assert!(matches!(err, Error::RefConflict(refname) if refname == "refs/heads/main"));
    assert_eq!(
        fixture.repo.resolve_ref("refs/heads/main").await?,
        Some(left_oid)
    );

    Ok(())
}

fn pack_type_code(kind: ObjectKind) -> u8 {
    match kind {
        ObjectKind::Commit => 1,
        ObjectKind::Tree => 2,
        ObjectKind::Blob => 3,
        ObjectKind::Tag => 4,
    }
}

fn encode_pack_object_header(buf: &mut Vec<u8>, type_code: u8, payload_len: usize) {
    let mut size = payload_len;
    let first = ((type_code & 0x7) << 4) | (size & 0x0f) as u8;
    size >>= 4;
    if size > 0 {
        buf.push(first | 0x80);
        while size > 0 {
            let b = (size & 0x7f) as u8;
            size >>= 7;
            buf.push(if size > 0 { b | 0x80 } else { b });
        }
    } else {
        buf.push(first);
    }
}
