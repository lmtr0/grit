//! Read-only upload-pack protocol support.

use std::collections::{HashSet, VecDeque};

use grit_lib::objects::{parse_commit, parse_tag, parse_tree, HashAlgo, ObjectId, ObjectKind};
use grit_lib::pkt_line;

use crate::error::{Error, Result};
use crate::repository::ServerRepository;
use crate::storage::{ServerStorage, StoredRef};

/// Git protocol version used by an upload-pack exchange.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub enum GitProtocolVersion {
    /// Original upload-pack protocol.
    #[default]
    V0,
    /// Version 1 upload-pack protocol.
    V1,
    /// Version 2 upload-pack protocol.
    V2,
}

/// Upload-pack capability advertised or requested on the wire.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum UploadPackCapability {
    /// Multi-ack negotiation support.
    MultiAck,
    /// Detailed multi-ack negotiation support.
    MultiAckDetailed,
    /// Thin-pack support.
    ThinPack,
    /// Side-band framing support.
    SideBand,
    /// Side-band-64k framing support.
    SideBand64k,
    /// Offset-delta support.
    OfsDelta,
    /// Include tags reachable from wanted commits.
    IncludeTag,
    /// Agent identity.
    Agent(String),
    /// Repository object format.
    ObjectFormat(HashAlgo),
    /// Unknown capability preserved for callers that need pass-through visibility.
    Other(String),
}

impl UploadPackCapability {
    /// Parse a capability token from a v0/v1 request or advertisement.
    #[must_use]
    pub fn parse(token: &str) -> Self {
        match token {
            "multi_ack" => Self::MultiAck,
            "multi_ack_detailed" => Self::MultiAckDetailed,
            "thin-pack" => Self::ThinPack,
            "side-band" => Self::SideBand,
            "side-band-64k" => Self::SideBand64k,
            "ofs-delta" => Self::OfsDelta,
            "include-tag" => Self::IncludeTag,
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
            Self::MultiAck => "multi_ack".to_owned(),
            Self::MultiAckDetailed => "multi_ack_detailed".to_owned(),
            Self::ThinPack => "thin-pack".to_owned(),
            Self::SideBand => "side-band".to_owned(),
            Self::SideBand64k => "side-band-64k".to_owned(),
            Self::OfsDelta => "ofs-delta".to_owned(),
            Self::IncludeTag => "include-tag".to_owned(),
            Self::Agent(agent) => format!("agent={agent}"),
            Self::ObjectFormat(algo) => format!("object-format={}", algo.name()),
            Self::Other(value) => value.clone(),
        }
    }
}

/// One ref advertised by upload-pack.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AdvertisedRef {
    /// Full ref name, such as `refs/heads/main` or `HEAD`.
    pub name: String,
    /// Object id the ref resolves to.
    pub oid: ObjectId,
    /// Whether this row is a peeled annotated-tag advertisement.
    pub peeled: bool,
}

/// Upload-pack ref advertisement for protocol v0/v1 callers.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RefAdvertisement {
    /// Protocol version this advertisement is intended for.
    pub protocol: GitProtocolVersion,
    /// Advertised refs in wire order.
    pub refs: Vec<AdvertisedRef>,
    /// Capabilities attached to the first advertisement line.
    pub capabilities: Vec<UploadPackCapability>,
}

impl RefAdvertisement {
    /// Serialize this advertisement as pkt-lines.
    ///
    /// # Errors
    ///
    /// Returns an I/O error if pkt-line framing fails.
    pub fn to_pkt_lines(&self, hash_algo: HashAlgo) -> Result<Vec<u8>> {
        let mut out = Vec::new();
        let mut refs = self.refs.iter();
        if let Some(first) = refs.next() {
            let capabilities = self
                .capabilities
                .iter()
                .map(UploadPackCapability::as_wire_token)
                .collect::<Vec<_>>()
                .join(" ");
            pkt_line::write_packet_raw(
                &mut out,
                format!("{} {}\0{}\n", first.oid.to_hex(), first.name, capabilities).as_bytes(),
            )?;
            for advertised in refs {
                pkt_line::write_line(
                    &mut out,
                    &format!("{} {}", advertised.oid.to_hex(), advertised.name),
                )?;
            }
        } else {
            let zero = ObjectId::null(hash_algo).to_hex();
            let capabilities = self
                .capabilities
                .iter()
                .map(UploadPackCapability::as_wire_token)
                .collect::<Vec<_>>()
                .join(" ");
            pkt_line::write_packet_raw(
                &mut out,
                format!("{zero} capabilities^{{}}\0{capabilities}\n").as_bytes(),
            )?;
        }
        pkt_line::write_flush(&mut out)?;
        Ok(out)
    }
}

/// Parsed upload-pack request for protocol v0/v1 negotiation.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct UploadPackRequest {
    /// Protocol version used by this request.
    pub protocol: GitProtocolVersion,
    /// Object ids requested by the client.
    pub wants: Vec<ObjectId>,
    /// Object ids the client claims to already have.
    pub haves: Vec<ObjectId>,
    /// Capabilities requested on the first want line.
    pub capabilities: Vec<UploadPackCapability>,
    /// Whether the client sent `done`.
    pub done: bool,
}

impl UploadPackRequest {
    /// Parse a protocol v0/v1 upload-pack request from pkt-line bytes.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Protocol`] for malformed commands and [`Error::Io`] for invalid framing.
    pub fn parse_v0(input: &[u8]) -> Result<Self> {
        let mut request = Self {
            protocol: GitProtocolVersion::V0,
            ..Self::default()
        };
        let mut reader = input;
        let mut saw_first_want = false;
        while let Some(packet) = pkt_line::read_packet(&mut reader)? {
            let pkt_line::Packet::Data(line) = packet else {
                break;
            };
            if line == "done" {
                request.done = true;
                continue;
            }
            if let Some(rest) = line.strip_prefix("want ") {
                let mut tokens = rest.split_whitespace();
                let oid = parse_oid_token(tokens.next(), "want")?;
                request.wants.push(oid);
                if !saw_first_want {
                    request
                        .capabilities
                        .extend(tokens.map(UploadPackCapability::parse));
                    saw_first_want = true;
                }
                continue;
            }
            if let Some(rest) = line.strip_prefix("have ") {
                let mut tokens = rest.split_whitespace();
                request.haves.push(parse_oid_token(tokens.next(), "have")?);
                continue;
            }
            return Err(Error::Protocol(format!(
                "unsupported upload-pack command '{line}'"
            )));
        }
        Ok(request)
    }

    /// Return whether this request asked for side-band-64k pack framing.
    #[must_use]
    pub fn wants_sideband64k(&self) -> bool {
        self.capabilities
            .iter()
            .any(|capability| matches!(capability, UploadPackCapability::SideBand64k))
    }
}

/// Negotiated object plan for a fetch or clone.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FetchPackPlan {
    /// Client request used to build this plan.
    pub request: UploadPackRequest,
    /// Haves accepted as common commits.
    pub common_haves: Vec<ObjectId>,
    /// Objects that must be sent to satisfy the wants.
    pub objects: Vec<ObjectId>,
}

/// Pack bytes and framed upload-pack response for a negotiated plan.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FetchPackResponse {
    /// Negotiated plan used for the response.
    pub plan: FetchPackPlan,
    /// Raw PACK v2 bytes.
    pub pack: Vec<u8>,
    /// Full protocol v0/v1 response bytes.
    pub wire_response: Vec<u8>,
    /// Whether the pack was side-band-64k framed in [`Self::wire_response`].
    pub sideband: bool,
}

/// Upload-pack service bound to one server-backed repository.
#[derive(Clone)]
pub struct UploadPackService<S> {
    repo: ServerRepository<S>,
}

impl<S> UploadPackService<S>
where
    S: ServerStorage,
{
    /// Create an upload-pack service over `repo`.
    #[must_use]
    pub fn new(repo: ServerRepository<S>) -> Self {
        Self { repo }
    }

    /// Advertise repository refs for protocol v0/v1.
    ///
    /// # Errors
    ///
    /// Returns backend or object parsing errors while resolving refs and peeled tags.
    pub async fn advertise_refs(&self) -> Result<RefAdvertisement> {
        let mut refs = Vec::new();
        if let Some(head) = self.repo.resolve_ref("HEAD").await? {
            refs.push(AdvertisedRef {
                name: "HEAD".to_owned(),
                oid: head,
                peeled: false,
            });
        }

        let mut listed = self.repo.list_refs("refs/").await?;
        listed.sort_by(|left, right| left.0.cmp(&right.0));
        for (refname, value) in listed {
            let Some(oid) = resolve_stored_ref(&self.repo, &refname, value).await? else {
                continue;
            };
            refs.push(AdvertisedRef {
                name: refname.clone(),
                oid,
                peeled: false,
            });
            if refname.starts_with("refs/tags/") {
                if let Some(peeled) = peel_tag(&self.repo, oid).await? {
                    refs.push(AdvertisedRef {
                        name: format!("{refname}^{{}}"),
                        oid: peeled,
                        peeled: true,
                    });
                }
            }
        }

        Ok(RefAdvertisement {
            protocol: GitProtocolVersion::V0,
            refs,
            capabilities: default_capabilities(self.repo.hash_algo()),
        })
    }

    /// Negotiate a fetch and return an object plan.
    ///
    /// # Errors
    ///
    /// Returns [`Error::ObjectNotFound`] for missing wants, or backend/object parsing errors while
    /// computing reachability.
    pub async fn negotiate_fetch(&self, request: UploadPackRequest) -> Result<FetchPackPlan> {
        let mut seen_wants = HashSet::new();
        let mut wants = Vec::new();
        for want in &request.wants {
            if !seen_wants.insert(*want) {
                continue;
            }
            let object = self
                .repo
                .read_object(want)
                .await?
                .ok_or_else(|| Error::ObjectNotFound(want.to_hex()))?;
            if object.kind == ObjectKind::Tag {
                let _ = parse_tag(&object.data)?;
            }
            wants.push(*want);
        }

        let common_haves = self.common_haves(&request.haves).await?;
        let have_closure = self.reachable_set(&common_haves, true).await?;
        let objects = self
            .collect_reachable_excluding(&wants, &have_closure, false)
            .await?;

        Ok(FetchPackPlan {
            request,
            common_haves,
            objects,
        })
    }

    /// Build a raw pack and protocol response for `plan`.
    ///
    /// # Errors
    ///
    /// Returns backend, object parsing, compression, hashing, or pkt-line framing errors.
    pub async fn build_fetch_pack(&self, plan: FetchPackPlan) -> Result<FetchPackResponse> {
        let pack = self.serialize_pack(&plan.objects).await?;
        let sideband = plan.request.wants_sideband64k();
        let mut wire_response = Vec::new();
        if let Some(common) = plan.common_haves.last() {
            pkt_line::write_line(&mut wire_response, &format!("ACK {}", common.to_hex()))?;
        } else {
            pkt_line::write_line(&mut wire_response, "NAK")?;
        }
        if sideband {
            pkt_line::write_sideband_channel1_64k(&mut wire_response, &pack)?;
            pkt_line::write_flush(&mut wire_response)?;
        } else {
            wire_response.extend_from_slice(&pack);
        }

        Ok(FetchPackResponse {
            plan,
            pack,
            wire_response,
            sideband,
        })
    }

    async fn common_haves(&self, haves: &[ObjectId]) -> Result<Vec<ObjectId>> {
        let mut seen = HashSet::new();
        let mut common = Vec::new();
        for have in haves {
            if !seen.insert(*have) {
                continue;
            }
            let Some(object) = self.repo.read_object(have).await? else {
                continue;
            };
            if object.kind == ObjectKind::Commit {
                common.push(*have);
            }
        }
        Ok(common)
    }

    async fn reachable_set(
        &self,
        roots: &[ObjectId],
        skip_missing: bool,
    ) -> Result<HashSet<ObjectId>> {
        Ok(self
            .collect_reachable_excluding(roots, &HashSet::new(), skip_missing)
            .await?
            .into_iter()
            .collect())
    }

    async fn collect_reachable_excluding(
        &self,
        roots: &[ObjectId],
        exclude: &HashSet<ObjectId>,
        skip_missing: bool,
    ) -> Result<Vec<ObjectId>> {
        let mut visited = HashSet::new();
        let mut ordered = Vec::new();
        let mut queue = VecDeque::new();
        for root in roots {
            enqueue(*root, exclude, &mut visited, &mut ordered, &mut queue);
        }

        while let Some(oid) = queue.pop_front() {
            let Some(object) = self.repo.read_object(&oid).await? else {
                if skip_missing {
                    continue;
                }
                return Err(Error::ObjectNotFound(oid.to_hex()));
            };
            match object.kind {
                ObjectKind::Commit => {
                    let commit = parse_commit(&object.data)?;
                    for parent in commit.parents {
                        enqueue(parent, exclude, &mut visited, &mut ordered, &mut queue);
                    }
                    enqueue(commit.tree, exclude, &mut visited, &mut ordered, &mut queue);
                }
                ObjectKind::Tree => {
                    for entry in parse_tree(&object.data)? {
                        if entry.mode != 0o160000 {
                            enqueue(entry.oid, exclude, &mut visited, &mut ordered, &mut queue);
                        }
                    }
                }
                ObjectKind::Tag => {
                    let tag = parse_tag(&object.data)?;
                    enqueue(tag.object, exclude, &mut visited, &mut ordered, &mut queue);
                }
                ObjectKind::Blob => {}
            }
        }
        Ok(ordered)
    }

    async fn serialize_pack(&self, objects: &[ObjectId]) -> Result<Vec<u8>> {
        self.repo.build_pack(objects).await
    }
}

fn default_capabilities(hash_algo: HashAlgo) -> Vec<UploadPackCapability> {
    let mut capabilities = vec![
        UploadPackCapability::MultiAck,
        UploadPackCapability::MultiAckDetailed,
        UploadPackCapability::ThinPack,
        UploadPackCapability::SideBand,
        UploadPackCapability::SideBand64k,
        UploadPackCapability::OfsDelta,
        UploadPackCapability::Agent("grit-lib-server".to_owned()),
    ];
    if hash_algo != HashAlgo::Sha1 {
        capabilities.push(UploadPackCapability::ObjectFormat(hash_algo));
    }
    capabilities
}

async fn resolve_stored_ref<S>(
    repo: &ServerRepository<S>,
    refname: &str,
    value: StoredRef,
) -> Result<Option<ObjectId>>
where
    S: ServerStorage,
{
    match value {
        StoredRef::Direct(oid) => Ok(Some(oid)),
        StoredRef::Symbolic(_) => repo.resolve_ref(refname).await,
    }
}

async fn peel_tag<S>(repo: &ServerRepository<S>, oid: ObjectId) -> Result<Option<ObjectId>>
where
    S: ServerStorage,
{
    let Some(object) = repo.read_object(&oid).await? else {
        return Ok(None);
    };
    if object.kind != ObjectKind::Tag {
        return Ok(None);
    }
    let tag = parse_tag(&object.data)?;
    Ok(Some(tag.object))
}

fn parse_oid_token(token: Option<&str>, command: &str) -> Result<ObjectId> {
    let Some(token) = token else {
        return Err(Error::Protocol(format!(
            "{command} command missing object id"
        )));
    };
    ObjectId::from_hex(token).map_err(Into::into)
}

fn enqueue(
    oid: ObjectId,
    exclude: &HashSet<ObjectId>,
    visited: &mut HashSet<ObjectId>,
    ordered: &mut Vec<ObjectId>,
    queue: &mut VecDeque<ObjectId>,
) {
    if exclude.contains(&oid) {
        return;
    }
    if visited.insert(oid) {
        ordered.push(oid);
        queue.push_back(oid);
    }
}
