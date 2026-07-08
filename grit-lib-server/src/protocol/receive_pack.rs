//! Receive-pack protocol support for accepting pushes.

use std::collections::{HashMap, HashSet, VecDeque};
use std::io::Read;

use async_trait::async_trait;
use flate2::read::ZlibDecoder;
use grit_lib::check_ref_format::{check_refname_format, RefNameOptions};
use grit_lib::objects::{parse_commit, parse_tag, parse_tree, HashAlgo, ObjectId, ObjectKind};
use grit_lib::pkt_line;
use sha1::{Digest as Sha1Digest, Sha1};
use sha2::{Digest as Sha2Digest, Sha256};
use time::OffsetDateTime;

use crate::cache::{EventPublisher, InvalidationEvent, InvalidationEventKind};
use crate::error::{Error, Result};
use crate::repository::ServerRepository;
use crate::storage::{ReflogEntry, ServerStorage, StoredObject, StoredRef};

/// Receive-pack capability advertised or requested on the wire.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum ReceivePackCapability {
    /// Ask the server to report per-ref status.
    ReportStatus,
    /// Ask the server to report per-ref status using the v2 status shape.
    ReportStatusV2,
    /// Side-band-64k multiplexing support.
    SideBand64k,
    /// Ref deletion support.
    DeleteRefs,
    /// Atomic ref update support.
    Atomic,
    /// Offset-delta pack support.
    OfsDelta,
    /// Push option support.
    PushOptions,
    /// Agent identity.
    Agent(String),
    /// Repository object format.
    ObjectFormat(HashAlgo),
    /// Unknown capability preserved for callers that need pass-through visibility.
    Other(String),
}

impl ReceivePackCapability {
    /// Parse a receive-pack capability token.
    #[must_use]
    pub fn parse(token: &str) -> Self {
        match token {
            "report-status" => Self::ReportStatus,
            "report-status-v2" => Self::ReportStatusV2,
            "side-band-64k" => Self::SideBand64k,
            "delete-refs" => Self::DeleteRefs,
            "atomic" => Self::Atomic,
            "ofs-delta" => Self::OfsDelta,
            "push-options" => Self::PushOptions,
            other => other
                .strip_prefix("agent=")
                .map(|agent| Self::Agent(agent.to_owned()))
                .or_else(|| {
                    other
                        .strip_prefix("object-format=")
                        .and_then(HashAlgo::from_name)
                        .map(Self::ObjectFormat)
                })
                .unwrap_or_else(|| Self::Other(other.to_owned())),
        }
    }

    /// Return the wire token for this capability.
    #[must_use]
    pub fn as_wire_token(&self) -> String {
        match self {
            Self::ReportStatus => "report-status".to_owned(),
            Self::ReportStatusV2 => "report-status-v2".to_owned(),
            Self::SideBand64k => "side-band-64k".to_owned(),
            Self::DeleteRefs => "delete-refs".to_owned(),
            Self::Atomic => "atomic".to_owned(),
            Self::OfsDelta => "ofs-delta".to_owned(),
            Self::PushOptions => "push-options".to_owned(),
            Self::Agent(agent) => format!("agent={agent}"),
            Self::ObjectFormat(algo) => format!("object-format={}", algo.name()),
            Self::Other(value) => value.clone(),
        }
    }
}

/// Kind of ref update described by a receive-pack command.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PushCommandKind {
    /// Create a ref that did not previously exist.
    Create,
    /// Update an existing ref to a new object id.
    Update,
    /// Delete an existing ref.
    Delete,
}

/// One receive-pack command from the client.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReceivePackCommand {
    /// Object id the client believes the ref currently has, or the null id for creation.
    pub old_oid: ObjectId,
    /// Object id the client wants the ref to have, or the null id for deletion.
    pub new_oid: ObjectId,
    /// Full ref name to update.
    pub refname: String,
}

impl ReceivePackCommand {
    /// Return the command kind derived from the old and new object ids.
    #[must_use]
    pub fn kind(&self) -> PushCommandKind {
        match (self.old_oid.is_zero(), self.new_oid.is_zero()) {
            (true, false) => PushCommandKind::Create,
            (false, true) => PushCommandKind::Delete,
            _ => PushCommandKind::Update,
        }
    }
}

/// Parsed receive-pack request containing ref commands and optional PACK bytes.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ReceivePackRequest {
    /// Ref update commands in client order.
    pub commands: Vec<ReceivePackCommand>,
    /// Capabilities requested on the first command line.
    pub capabilities: Vec<ReceivePackCapability>,
    /// Raw PACK stream following the command flush, if any.
    pub pack: Vec<u8>,
}

impl ReceivePackRequest {
    /// Parse a protocol v0/v1 receive-pack request from pkt-line bytes.
    ///
    /// The parser consumes command pkt-lines through the flush packet and preserves the remaining
    /// bytes as the PACK stream.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Protocol`] for malformed commands and [`Error::Io`] for invalid framing.
    pub fn parse_v0(input: &[u8]) -> Result<Self> {
        let mut reader = input;
        let mut request = Self::default();
        let mut first = true;
        while let Some(packet) = pkt_line::read_packet(&mut reader)? {
            let pkt_line::Packet::Data(line) = packet else {
                break;
            };
            let (command_line, capabilities) = if first {
                first = false;
                line.split_once('\0')
                    .map_or((line.as_str(), ""), |(command, capabilities)| {
                        (command, capabilities)
                    })
            } else {
                (line.as_str(), "")
            };
            if !capabilities.is_empty() {
                request.capabilities.extend(
                    capabilities
                        .split_whitespace()
                        .map(ReceivePackCapability::parse),
                );
            }
            request.commands.push(parse_command_line(command_line)?);
        }
        request.pack = reader.to_vec();
        Ok(request)
    }
}

/// Object decoded from a received pack and held outside durable storage until validation passes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct QuarantinedObject {
    /// Computed object id.
    pub oid: ObjectId,
    /// Decoded Git object.
    pub object: StoredObject,
}

/// Validated push plan ready to apply.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PushPlan {
    /// Original request.
    pub request: ReceivePackRequest,
    /// Objects decoded into the temporary quarantine.
    pub quarantine: Vec<QuarantinedObject>,
}

/// Context passed to push policy hooks for one command.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PushPolicyContext {
    /// Tenant that owns the repository.
    pub tenant: crate::ids::TenantId,
    /// Repository being pushed to.
    pub repository: crate::ids::RepositoryId,
    /// Ref being updated.
    pub refname: String,
    /// Old object id from the client command.
    pub old_oid: ObjectId,
    /// New object id from the client command.
    pub new_oid: ObjectId,
    /// Derived command kind.
    pub kind: PushCommandKind,
}

/// Policy hook used to reject protected or otherwise unauthorized ref updates.
#[async_trait]
pub trait PushPolicy: Send + Sync {
    /// Check whether one receive-pack command may proceed.
    ///
    /// # Errors
    ///
    /// Returns [`Error::PushPolicyRejected`] or another typed error when the command is not
    /// allowed.
    async fn check(&self, context: &PushPolicyContext) -> Result<()>;
}

/// Push policy that accepts every command.
#[derive(Clone, Copy, Debug, Default)]
pub struct AllowAllPushPolicy;

#[async_trait]
impl PushPolicy for AllowAllPushPolicy {
    async fn check(&self, _context: &PushPolicyContext) -> Result<()> {
        Ok(())
    }
}

/// Simple protected-ref policy based on exact prefixes.
#[derive(Clone, Debug, Default)]
pub struct ProtectedRefPolicy {
    protected_prefixes: Vec<String>,
}

impl ProtectedRefPolicy {
    /// Create a policy that rejects updates to refs matching any prefix in `protected_prefixes`.
    #[must_use]
    pub fn new(protected_prefixes: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self {
            protected_prefixes: protected_prefixes.into_iter().map(Into::into).collect(),
        }
    }
}

#[async_trait]
impl PushPolicy for ProtectedRefPolicy {
    async fn check(&self, context: &PushPolicyContext) -> Result<()> {
        if self
            .protected_prefixes
            .iter()
            .any(|prefix| context.refname.starts_with(prefix))
        {
            return Err(Error::PushPolicyRejected {
                refname: context.refname.clone(),
                reason: "protected ref".to_owned(),
            });
        }
        Ok(())
    }
}

/// Per-ref status after applying a push.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PushCommandStatus {
    /// Ref name this status describes.
    pub refname: String,
    /// Command kind that was applied.
    pub kind: PushCommandKind,
    /// New object id for successful creates and updates.
    pub new_oid: Option<ObjectId>,
}

/// Result of a successful receive-pack apply operation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReceivePackReport {
    /// Number of objects moved from quarantine into durable storage.
    pub unpacked_objects: usize,
    /// Successful per-ref statuses.
    pub statuses: Vec<PushCommandStatus>,
}

impl ReceivePackReport {
    /// Serialize a report-status response as pkt-lines.
    ///
    /// # Errors
    ///
    /// Returns an I/O error if pkt-line framing fails.
    pub fn to_pkt_lines(&self) -> Result<Vec<u8>> {
        let mut out = Vec::new();
        pkt_line::write_line(&mut out, "unpack ok")?;
        for status in &self.statuses {
            pkt_line::write_line(&mut out, &format!("ok {}", status.refname))?;
        }
        pkt_line::write_flush(&mut out)?;
        Ok(out)
    }
}

/// Receive-pack service bound to one server-backed repository.
#[derive(Clone)]
pub struct ReceivePackService<S> {
    repo: ServerRepository<S>,
}

impl<S> ReceivePackService<S>
where
    S: ServerStorage,
{
    /// Create a receive-pack service over `repo`.
    #[must_use]
    pub fn new(repo: ServerRepository<S>) -> Self {
        Self { repo }
    }

    /// Parse, quarantine, and validate a push request before applying it.
    ///
    /// # Errors
    ///
    /// Returns protocol, object closure, fast-forward, ref conflict, policy, backend, or object
    /// parsing errors.
    pub async fn prepare_push<P>(&self, request: ReceivePackRequest, policy: &P) -> Result<PushPlan>
    where
        P: PushPolicy,
    {
        self.validate_commands(&request).await?;
        let quarantine = decode_pack(&request.pack, self.repo.hash_algo())?;
        let quarantined = quarantine_map(&quarantine);
        self.verify_closure(&request, &quarantined).await?;
        self.verify_fast_forwards(&request, &quarantined).await?;
        self.check_policy(&request, policy).await?;
        Ok(PushPlan {
            request,
            quarantine,
        })
    }

    /// Apply a prepared push using guarded ref updates, reflogs, and invalidation publishing.
    ///
    /// `actor` is written to each reflog entry, `timestamp` is supplied by the caller to keep core
    /// logic deterministic, and `publisher` receives ref invalidation events after successful
    /// durable updates.
    ///
    /// # Errors
    ///
    /// Returns backend, compare-and-swap conflict, reflog, or invalidation publishing errors.
    pub async fn apply_push<P>(
        &self,
        plan: PushPlan,
        actor: &str,
        timestamp: OffsetDateTime,
        publisher: &P,
    ) -> Result<ReceivePackReport>
    where
        P: EventPublisher,
    {
        for quarantined in &plan.quarantine {
            self.repo
                .storage()
                .write_object(
                    self.repo.tenant(),
                    self.repo.repository(),
                    &quarantined.oid,
                    &quarantined.object,
                )
                .await?;
        }

        let mut statuses = Vec::with_capacity(plan.request.commands.len());
        for command in &plan.request.commands {
            let expected = expected_ref_value(command);
            match command.kind() {
                PushCommandKind::Delete => {
                    self.repo
                        .storage()
                        .delete_ref(
                            self.repo.tenant(),
                            self.repo.repository(),
                            &command.refname,
                            expected,
                        )
                        .await?;
                    self.append_reflog(command, actor, timestamp).await?;
                    publish_ref_event(&self.repo, publisher, &command.refname, true).await?;
                    statuses.push(PushCommandStatus {
                        refname: command.refname.clone(),
                        kind: PushCommandKind::Delete,
                        new_oid: None,
                    });
                }
                PushCommandKind::Create | PushCommandKind::Update => {
                    self.repo
                        .storage()
                        .write_ref(
                            self.repo.tenant(),
                            self.repo.repository(),
                            &command.refname,
                            &StoredRef::Direct(command.new_oid),
                            Some(expected),
                        )
                        .await?;
                    self.append_reflog(command, actor, timestamp).await?;
                    publish_ref_event(&self.repo, publisher, &command.refname, false).await?;
                    statuses.push(PushCommandStatus {
                        refname: command.refname.clone(),
                        kind: command.kind(),
                        new_oid: Some(command.new_oid),
                    });
                }
            }
        }

        Ok(ReceivePackReport {
            unpacked_objects: plan.quarantine.len(),
            statuses,
        })
    }

    async fn validate_commands(&self, request: &ReceivePackRequest) -> Result<()> {
        if request.commands.is_empty() {
            return Err(Error::Protocol(
                "receive-pack request did not contain commands".to_owned(),
            ));
        }
        let mut seen = HashSet::new();
        let mut touched = Vec::new();
        for command in &request.commands {
            if command.old_oid.is_zero() && command.new_oid.is_zero() {
                return Err(Error::Protocol(format!(
                    "ref '{}' has both old and new object ids set to zero",
                    command.refname
                )));
            }
            validate_refname(&command.refname)?;
            validate_oid_algorithm(command.old_oid, self.repo.hash_algo())?;
            validate_oid_algorithm(command.new_oid, self.repo.hash_algo())?;
            if !seen.insert(command.refname.clone()) {
                return Err(Error::RefConflict(command.refname.clone()));
            }
            touched.push(command.refname.clone());
        }

        for left in 0..touched.len() {
            for right in left + 1..touched.len() {
                if ref_namespace_conflicts(&touched[left], &touched[right]) {
                    return Err(Error::RefNamespaceConflict {
                        refname: touched[left].clone(),
                        existing: touched[right].clone(),
                    });
                }
            }
        }

        let existing_refs = self.repo.list_refs("refs/").await?;
        for command in &request.commands {
            if command.kind() != PushCommandKind::Create {
                continue;
            }
            for (existing, _) in &existing_refs {
                if existing != &command.refname
                    && ref_namespace_conflicts(existing, &command.refname)
                {
                    return Err(Error::RefNamespaceConflict {
                        refname: command.refname.clone(),
                        existing: existing.clone(),
                    });
                }
            }
        }
        Ok(())
    }

    async fn verify_closure(
        &self,
        request: &ReceivePackRequest,
        quarantine: &HashMap<ObjectId, StoredObject>,
    ) -> Result<()> {
        let roots = request
            .commands
            .iter()
            .filter(|command| !command.new_oid.is_zero())
            .map(|command| command.new_oid)
            .collect::<Vec<_>>();
        let mut seen = HashSet::new();
        let mut queue = VecDeque::from(roots);
        while let Some(oid) = queue.pop_front() {
            if !seen.insert(oid) {
                continue;
            }
            let object = self.lookup_object(&oid, quarantine).await?;
            enqueue_references(&object, &mut queue)?;
        }
        Ok(())
    }

    async fn verify_fast_forwards(
        &self,
        request: &ReceivePackRequest,
        quarantine: &HashMap<ObjectId, StoredObject>,
    ) -> Result<()> {
        for command in &request.commands {
            if command.kind() != PushCommandKind::Update
                || !command.refname.starts_with("refs/heads/")
            {
                continue;
            }
            if !self
                .is_ancestor_commit(command.old_oid, command.new_oid, quarantine)
                .await?
            {
                return Err(Error::NonFastForward {
                    refname: command.refname.clone(),
                    old_oid: command.old_oid.to_hex(),
                    new_oid: command.new_oid.to_hex(),
                });
            }
        }
        Ok(())
    }

    async fn check_policy<P>(&self, request: &ReceivePackRequest, policy: &P) -> Result<()>
    where
        P: PushPolicy,
    {
        for command in &request.commands {
            policy
                .check(&PushPolicyContext {
                    tenant: self.repo.tenant().clone(),
                    repository: self.repo.repository().clone(),
                    refname: command.refname.clone(),
                    old_oid: command.old_oid,
                    new_oid: command.new_oid,
                    kind: command.kind(),
                })
                .await?;
        }
        Ok(())
    }

    async fn is_ancestor_commit(
        &self,
        ancestor: ObjectId,
        descendant: ObjectId,
        quarantine: &HashMap<ObjectId, StoredObject>,
    ) -> Result<bool> {
        if ancestor == descendant {
            return Ok(true);
        }
        let mut seen = HashSet::new();
        let mut queue = VecDeque::from([descendant]);
        while let Some(oid) = queue.pop_front() {
            if !seen.insert(oid) {
                continue;
            }
            let object = self.lookup_object(&oid, quarantine).await?;
            if object.kind != ObjectKind::Commit {
                return Err(Error::UnexpectedObjectKind {
                    expected: "commit",
                    actual: object_kind_name(object.kind),
                });
            }
            let commit = parse_commit(&object.data)?;
            if commit.parents.contains(&ancestor) {
                return Ok(true);
            }
            queue.extend(commit.parents);
        }
        Ok(false)
    }

    async fn lookup_object(
        &self,
        oid: &ObjectId,
        quarantine: &HashMap<ObjectId, StoredObject>,
    ) -> Result<StoredObject> {
        if let Some(object) = quarantine.get(oid) {
            return Ok(object.clone());
        }
        self.repo
            .read_object(oid)
            .await?
            .ok_or_else(|| Error::ObjectNotFound(oid.to_hex()))
    }

    async fn append_reflog(
        &self,
        command: &ReceivePackCommand,
        actor: &str,
        timestamp: OffsetDateTime,
    ) -> Result<()> {
        self.repo
            .storage()
            .append_reflog(
                self.repo.tenant(),
                self.repo.repository(),
                &ReflogEntry {
                    refname: command.refname.clone(),
                    old_oid: command.old_oid,
                    new_oid: command.new_oid,
                    actor: actor.to_owned(),
                    timestamp,
                    message: reflog_message(command.kind()).to_owned(),
                },
            )
            .await
    }
}

fn parse_command_line(line: &str) -> Result<ReceivePackCommand> {
    let mut parts = line.split_whitespace();
    let old_oid = parse_oid_token(parts.next(), "old object id")?;
    let new_oid = parse_oid_token(parts.next(), "new object id")?;
    let refname = parts
        .next()
        .ok_or_else(|| Error::Protocol("receive-pack command missing refname".to_owned()))?;
    if parts.next().is_some() {
        return Err(Error::Protocol(format!(
            "receive-pack command for '{refname}' has extra fields"
        )));
    }
    Ok(ReceivePackCommand {
        old_oid,
        new_oid,
        refname: refname.to_owned(),
    })
}

fn parse_oid_token(token: Option<&str>, field: &str) -> Result<ObjectId> {
    let Some(token) = token else {
        return Err(Error::Protocol(format!(
            "receive-pack command missing {field}"
        )));
    };
    ObjectId::from_hex(token).map_err(Into::into)
}

fn validate_refname(refname: &str) -> Result<()> {
    if !refname.starts_with("refs/") {
        return Err(Error::Protocol(format!(
            "receive-pack updates must target refs/, got '{refname}'"
        )));
    }
    check_refname_format(refname, &RefNameOptions::default())
        .map(|_| ())
        .map_err(|err| Error::Protocol(format!("invalid refname '{refname}': {err}")))
}

fn validate_oid_algorithm(oid: ObjectId, hash_algo: HashAlgo) -> Result<()> {
    if oid.algo() != hash_algo {
        return Err(Error::Protocol(format!(
            "object id '{}' does not use {}",
            oid.to_hex(),
            hash_algo.name()
        )));
    }
    Ok(())
}

fn decode_pack(pack: &[u8], hash_algo: HashAlgo) -> Result<Vec<QuarantinedObject>> {
    if pack.is_empty() {
        return Ok(Vec::new());
    }
    let trailer_len = hash_algo.len();
    if pack.len() < 12 + trailer_len {
        return Err(Error::Protocol("pack stream is too short".to_owned()));
    }
    let trailer_start = pack.len() - trailer_len;
    verify_pack_trailer(&pack[..trailer_start], &pack[trailer_start..], hash_algo)?;
    if &pack[..4] != b"PACK" {
        return Err(Error::Protocol(
            "pack stream has invalid signature".to_owned(),
        ));
    }
    let version = read_u32_be(pack, 4)?;
    if version != 2 && version != 3 {
        return Err(Error::Protocol(format!(
            "unsupported pack version {version}"
        )));
    }
    let count = read_u32_be(pack, 8)? as usize;
    let mut cursor = 12usize;
    let mut objects = Vec::with_capacity(count);
    for _ in 0..count {
        let (type_code, size) = read_type_and_size(pack, &mut cursor, trailer_start)?;
        let kind = object_kind_from_pack_type(type_code)?;
        let mut decoder = ZlibDecoder::new(&pack[cursor..trailer_start]);
        let mut data = Vec::with_capacity(size);
        decoder.read_to_end(&mut data)?;
        let consumed = decoder.total_in() as usize;
        if consumed == 0 {
            return Err(Error::Protocol(
                "pack object has empty zlib stream".to_owned(),
            ));
        }
        if data.len() != size {
            return Err(Error::Protocol(format!(
                "pack object size mismatch: expected {size}, got {}",
                data.len()
            )));
        }
        cursor = cursor
            .checked_add(consumed)
            .ok_or_else(|| Error::Protocol("pack cursor overflow".to_owned()))?;
        let object = StoredObject::new(kind, data);
        let oid = object.object_id(hash_algo);
        objects.push(QuarantinedObject { oid, object });
    }
    if cursor != trailer_start {
        return Err(Error::Protocol(
            "pack stream has trailing bytes before checksum".to_owned(),
        ));
    }
    Ok(objects)
}

fn verify_pack_trailer(body: &[u8], trailer: &[u8], hash_algo: HashAlgo) -> Result<()> {
    let digest = match hash_algo {
        HashAlgo::Sha1 => {
            let mut hasher = Sha1::new();
            Sha1Digest::update(&mut hasher, body);
            hasher.finalize().to_vec()
        }
        HashAlgo::Sha256 => {
            let mut hasher = Sha256::new();
            Sha2Digest::update(&mut hasher, body);
            hasher.finalize().to_vec()
        }
    };
    if digest != trailer {
        return Err(Error::Protocol(
            "pack trailing checksum mismatch".to_owned(),
        ));
    }
    Ok(())
}

fn read_u32_be(bytes: &[u8], offset: usize) -> Result<u32> {
    let slice = bytes
        .get(offset..offset + 4)
        .ok_or_else(|| Error::Protocol("pack stream truncated".to_owned()))?;
    Ok(u32::from_be_bytes([slice[0], slice[1], slice[2], slice[3]]))
}

fn read_type_and_size(pack: &[u8], cursor: &mut usize, end: usize) -> Result<(u8, usize)> {
    let first = read_pack_byte(pack, cursor, end)?;
    let type_code = (first >> 4) & 0x07;
    let mut size = (first & 0x0f) as usize;
    let mut shift = 4usize;
    let mut byte = first;
    while byte & 0x80 != 0 {
        byte = read_pack_byte(pack, cursor, end)?;
        size |= ((byte & 0x7f) as usize)
            .checked_shl(shift as u32)
            .ok_or_else(|| Error::Protocol("pack object size overflow".to_owned()))?;
        shift = shift
            .checked_add(7)
            .ok_or_else(|| Error::Protocol("pack object size overflow".to_owned()))?;
    }
    Ok((type_code, size))
}

fn read_pack_byte(pack: &[u8], cursor: &mut usize, end: usize) -> Result<u8> {
    if *cursor >= end {
        return Err(Error::Protocol("pack stream truncated".to_owned()));
    }
    let byte = pack[*cursor];
    *cursor += 1;
    Ok(byte)
}

fn object_kind_from_pack_type(type_code: u8) -> Result<ObjectKind> {
    match type_code {
        1 => Ok(ObjectKind::Commit),
        2 => Ok(ObjectKind::Tree),
        3 => Ok(ObjectKind::Blob),
        4 => Ok(ObjectKind::Tag),
        6 | 7 => Err(Error::Protocol(
            "delta objects are not supported by receive-pack yet".to_owned(),
        )),
        other => Err(Error::Protocol(format!(
            "unknown packed-object type {other}"
        ))),
    }
}

fn quarantine_map(quarantine: &[QuarantinedObject]) -> HashMap<ObjectId, StoredObject> {
    quarantine
        .iter()
        .map(|quarantined| (quarantined.oid, quarantined.object.clone()))
        .collect()
}

fn enqueue_references(object: &StoredObject, queue: &mut VecDeque<ObjectId>) -> Result<()> {
    match object.kind {
        ObjectKind::Commit => {
            let commit = parse_commit(&object.data)?;
            queue.push_back(commit.tree);
            queue.extend(commit.parents);
        }
        ObjectKind::Tree => {
            for entry in parse_tree(&object.data)? {
                if entry.mode != 0o160000 {
                    queue.push_back(entry.oid);
                }
            }
        }
        ObjectKind::Tag => {
            let tag = parse_tag(&object.data)?;
            queue.push_back(tag.object);
        }
        ObjectKind::Blob => {}
    }
    Ok(())
}

fn expected_ref_value(command: &ReceivePackCommand) -> Option<StoredRef> {
    if command.old_oid.is_zero() {
        None
    } else {
        Some(StoredRef::Direct(command.old_oid))
    }
}

async fn publish_ref_event<S, P>(
    repo: &ServerRepository<S>,
    publisher: &P,
    refname: &str,
    deleted: bool,
) -> Result<()>
where
    S: ServerStorage,
    P: EventPublisher,
{
    let event = InvalidationEvent::new(
        repo.tenant().clone(),
        repo.repository().clone(),
        if deleted {
            InvalidationEventKind::RefDelete {
                refname: refname.to_owned(),
            }
        } else {
            InvalidationEventKind::RefWrite {
                refname: refname.to_owned(),
            }
        },
    );
    publisher.publish_invalidation(event).await
}

fn reflog_message(kind: PushCommandKind) -> &'static str {
    match kind {
        PushCommandKind::Create => "receive-pack: create",
        PushCommandKind::Update => "receive-pack: update",
        PushCommandKind::Delete => "receive-pack: delete",
    }
}

fn ref_namespace_conflicts(left: &str, right: &str) -> bool {
    if left == right {
        return true;
    }
    left.strip_prefix(right)
        .is_some_and(|rest| rest.starts_with('/'))
        || right
            .strip_prefix(left)
            .is_some_and(|rest| rest.starts_with('/'))
}

fn object_kind_name(kind: ObjectKind) -> &'static str {
    match kind {
        ObjectKind::Blob => "blob",
        ObjectKind::Tree => "tree",
        ObjectKind::Commit => "commit",
        ObjectKind::Tag => "tag",
    }
}
