//! Read-only upload-pack protocol support.

use std::collections::{HashSet, VecDeque};

use grit_lib::objects::{parse_commit, parse_tag, parse_tree, HashAlgo, ObjectId, ObjectKind};
use grit_lib::pkt_line;

use crate::error::{Error, Result};
use crate::protocol::clone_metrics::{
    CloneLimitError, CloneLimits, CloneMemoryMetrics, CloneMetricsRecorder, CloneMetricsReport,
    CloneOutcome,
};
use crate::protocol::pack_stream::{
    CancellationProbe, PackAbortReason, PackChunkSink, PackStreamError, PackStreamLimits,
};
use crate::protocol::pack_writer::{
    IncrementalPackWriter, PackWriterError, PackWriterFailure, PackWriterReport,
};
use crate::protocol::upload_pack_wire::{
    UploadPackWireMode, UploadPackWireReport, UploadPackWireSink,
};
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
    /// Metrics the current service can measure without backend-specific instrumentation.
    pub metrics: CloneMetricsReport,
}

/// Successful raw incremental PACK response report.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UploadPackStreamReport {
    /// Writer, sink, memory, and cancellation counters.
    pub writer: PackWriterReport,
    /// Clone metrics populated by generic upload-pack and caller adapters.
    pub metrics: CloneMetricsReport,
}

/// Preflight rejection raised before the PACK header is sent.
#[derive(Clone, Copy, Debug, thiserror::Error, PartialEq, Eq)]
pub enum UploadPackStreamPreflightError {
    /// Clone execution limits are invalid.
    #[error("invalid clone streaming limits: {0}")]
    CloneLimits(CloneLimitError),
    /// Pack stream limits are invalid.
    #[error("invalid pack stream limits: {0}")]
    StreamLimits(PackStreamError),
    /// Wants or haves exceed the validated request shape.
    #[error("clone request exceeds want/have limits")]
    RequestShape,
    /// Planned closure exceeds the validated selection limit.
    #[error("clone plan exceeds selected-object limit")]
    SelectedObjects,
    /// Planned closure cannot be represented by PACK v2.
    #[error("clone plan object count exceeds u32")]
    ObjectCount,
    /// Validated inputs could not construct the writer.
    #[error("incremental pack writer configuration failed: {0}")]
    WriterConfiguration(PackWriterError),
    /// ACK/NAK, framing, or minimum wire output cannot fit validated limits.
    #[error("upload-pack wire configuration failed: {0}")]
    WireConfiguration(PackStreamError),
}

/// Raw streaming failure with post-start counters preserved.
#[derive(Debug, thiserror::Error)]
pub enum UploadPackStreamFailure {
    /// Validation failed before response bytes were sent.
    #[error(transparent)]
    Preflight(#[from] UploadPackStreamPreflightError),
    /// Incremental encoding, cancellation, limit, or sink failure.
    #[error(transparent)]
    Writer(#[from] PackWriterFailure),
    /// An object read failed after the header was emitted.
    #[error("upload-pack backend read failed")]
    Backend {
        /// Original storage error retained for internal handling.
        #[source]
        source: Error,
        /// Writer and sink state after best-effort abort.
        report: PackWriterReport,
    },
}

/// Successful negotiated streaming upload-pack response.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UploadPackResponseStreamReport {
    /// Logical PACK and clone metrics.
    pub pack: UploadPackStreamReport,
    /// Physical ACK/NAK, framing, flush, and downstream counters.
    pub wire: UploadPackWireReport,
}

/// Negotiated streaming response failure with wire counters preserved.
#[derive(Debug, thiserror::Error)]
pub enum UploadPackResponseStreamFailure {
    /// Validation failed before ACK/NAK or PACK bytes were sent.
    #[error(transparent)]
    Preflight(#[from] UploadPackStreamPreflightError),
    /// Prelude framing or its downstream write failed.
    #[error("upload-pack wire prelude failed: {source}")]
    Wire {
        /// Original stream boundary failure.
        source: PackStreamError,
        /// Physical counters after terminal propagation.
        report: UploadPackWireReport,
    },
    /// Raw PACK streaming failed after the prelude.
    #[error("upload-pack response streaming failed: {source}")]
    Pack {
        /// Existing typed raw stream failure.
        source: UploadPackStreamFailure,
        /// Physical counters after writer abort propagation.
        wire: UploadPackWireReport,
    },
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
        self.negotiate_fetch_with_limits(request, CloneLimits::default())
            .await
    }

    /// Negotiate a fetch while bounding request and reachability state before it grows.
    ///
    /// # Errors
    ///
    /// Returns limit, missing-object, backend, or object parsing errors.
    pub async fn negotiate_fetch_with_limits(
        &self,
        request: UploadPackRequest,
        limits: CloneLimits,
    ) -> Result<FetchPackPlan> {
        let limits = limits
            .validate()
            .map_err(|error| Error::Protocol(error.to_string()))?;
        limits
            .validate_request_shape(request.wants.len(), request.haves.len())
            .map_err(|error| Error::Protocol(error.to_string()))?;
        let mut seen_wants = HashSet::new();
        let mut wants = Vec::new();
        seen_wants
            .try_reserve(request.wants.len())
            .map_err(|_| Error::Backend("cannot reserve bounded clone wants".to_owned()))?;
        wants
            .try_reserve(request.wants.len())
            .map_err(|_| Error::Backend("cannot reserve bounded clone wants".to_owned()))?;
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
        let have_closure = self
            .reachable_set(&common_haves, true, limits.max_selected_objects)
            .await?;
        let objects = self
            .collect_reachable_excluding(&wants, &have_closure, false, limits.max_selected_objects)
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
        let mut recorder = CloneMetricsRecorder::default();
        self.build_fetch_pack_with_metrics(plan, CloneLimits::default(), &mut recorder)
            .await
    }

    /// Build a fetch response under validated limits and extend caller-supplied metrics.
    ///
    /// This buffered compatibility path reports selected objects, pack bytes, known non-delta
    /// shape, and retained encoded/sideband buffers. Backend adapters should populate database,
    /// external-service, cache, decoded-byte, and caller-timed phase observations. Request and
    /// selected-object limits are enforced before their corresponding repository traversal;
    /// because [`ServerRepository::build_pack`] returns one completed buffer, output bytes can only
    /// be rejected after encoding on this path. Streaming adapters must enforce the remaining
    /// worker, duration, in-flight byte, spill, coalescing, and concurrency contracts directly.
    /// The recorder is borrowed so every return path leaves its terminal outcome observable.
    ///
    /// # Errors
    ///
    /// Returns validation, object-count, output-byte, backend, pack, or framing errors.
    pub async fn build_fetch_pack_with_metrics(
        &self,
        plan: FetchPackPlan,
        limits: CloneLimits,
        recorder: &mut CloneMetricsRecorder,
    ) -> Result<FetchPackResponse> {
        let limits = match limits.validate() {
            Ok(limits) => limits,
            Err(error) => {
                let _ = recorder.finish(CloneOutcome::Rejected);
                return Err(Error::Protocol(error.to_string()));
            }
        };
        if let Err(error) =
            limits.validate_request_shape(plan.request.wants.len(), plan.request.haves.len())
        {
            let _ = recorder.finish(CloneOutcome::Rejected);
            return Err(Error::Protocol(error.to_string()));
        }
        if plan.objects.len() > limits.max_selected_objects {
            let _ = recorder.finish(CloneOutcome::Rejected);
            return Err(Error::Protocol(
                "clone selection exceeds object limit".to_owned(),
            ));
        }
        let pack = match self.serialize_pack(&plan.objects).await {
            Ok(pack) => pack,
            Err(error) => {
                finish_clone_error(recorder, &error);
                return Err(error);
            }
        };
        let pack_bytes = match u64::try_from(pack.len()) {
            Ok(size) => size,
            Err(_) => {
                let _ = recorder.finish(CloneOutcome::Rejected);
                return Err(Error::Protocol(
                    "clone output size cannot be represented".to_owned(),
                ));
            }
        };
        if pack_bytes > limits.max_output_bytes {
            let _ = recorder.finish(CloneOutcome::Rejected);
            return Err(Error::Protocol(
                "clone output exceeds byte limit".to_owned(),
            ));
        }
        let sideband = plan.request.wants_sideband64k();
        let mut wire_response = Vec::new();
        let Some(framing_overhead) = pack
            .len()
            .checked_div(60_000)
            .and_then(|chunks| chunks.checked_add(1))
            .and_then(|chunks| chunks.checked_mul(5))
            .and_then(|overhead| overhead.checked_add(1_024))
        else {
            let _ = recorder.finish(CloneOutcome::Rejected);
            return Err(Error::Protocol(
                "clone response size overflows usize".to_owned(),
            ));
        };
        let Some(response_capacity) = pack.len().checked_add(framing_overhead) else {
            let _ = recorder.finish(CloneOutcome::Rejected);
            return Err(Error::Protocol(
                "clone response size overflows usize".to_owned(),
            ));
        };
        if wire_response.try_reserve(response_capacity).is_err() {
            let _ = recorder.finish(CloneOutcome::Rejected);
            return Err(Error::Backend(
                "cannot reserve bounded clone response".to_owned(),
            ));
        }
        if let Some(common) = plan.common_haves.last() {
            if let Err(error) =
                pkt_line::write_line(&mut wire_response, &format!("ACK {}", common.to_hex()))
            {
                let error = Error::from(error);
                finish_clone_error(recorder, &error);
                return Err(error);
            }
        } else {
            if let Err(error) = pkt_line::write_line(&mut wire_response, "NAK") {
                let error = Error::from(error);
                finish_clone_error(recorder, &error);
                return Err(error);
            }
        }
        if sideband {
            if let Err(error) = pkt_line::write_sideband_channel1_64k(&mut wire_response, &pack) {
                let error = Error::from(error);
                finish_clone_error(recorder, &error);
                return Err(error);
            }
            if let Err(error) = pkt_line::write_flush(&mut wire_response) {
                let error = Error::from(error);
                finish_clone_error(recorder, &error);
                return Err(error);
            }
        } else {
            wire_response.extend_from_slice(&pack);
        }

        let selected_objects = match u64::try_from(plan.objects.len()) {
            Ok(selected) => selected,
            Err(_) => {
                let _ = recorder.finish(CloneOutcome::Rejected);
                return Err(Error::Protocol(
                    "clone object count cannot be represented".to_owned(),
                ));
            }
        };
        let response_bytes = match u64::try_from(wire_response.len()) {
            Ok(size) => size,
            Err(_) => {
                let _ = recorder.finish(CloneOutcome::Rejected);
                return Err(Error::Protocol(
                    "clone response size cannot be represented".to_owned(),
                ));
            }
        };
        recorder.record_selected_objects(selected_objects);
        let _ = recorder.record_pack(pack_bytes, 0, 0);
        recorder.observe_memory(CloneMemoryMetrics {
            encoded_bytes: pack_bytes,
            response_bytes,
            total_bytes: pack_bytes.saturating_add(response_bytes),
            ..CloneMemoryMetrics::default()
        });
        let metrics = recorder.finish(CloneOutcome::Success);
        Ok(FetchPackResponse {
            plan,
            pack,
            wire_response,
            sideband,
            metrics,
        })
    }

    /// Stream a raw non-delta PACK v2 for a validated fetch plan.
    ///
    /// Objects are emitted in the deterministic order stored by [`FetchPackPlan`]. The generic
    /// object-store boundary has no size-only preflight, so this path uses one-object waves: that
    /// guarantees the decoded budget is exceeded only by the one oversized object explicitly
    /// permitted by the contract. Backends may still use the positional batch API in callers that
    /// have size metadata. The method does not sideband-frame the raw PACK. Generic storage does not expose
    /// DB/S3/cache counters, so callers should seed `recorder` with adapter observations they can
    /// measure honestly.
    ///
    /// # Errors
    ///
    /// Returns typed preflight errors before output, or a failure retaining writer counters after
    /// best-effort abort when reads, compression, cancellation, limits, or the sink fail. The
    /// borrowed recorder is finalized on every return path, including partial-output failures.
    pub async fn stream_raw_fetch_pack<P: CancellationProbe>(
        &self,
        plan: FetchPackPlan,
        sink: &mut dyn PackChunkSink,
        clone_limits: CloneLimits,
        stream_limits: PackStreamLimits,
        cancellation: P,
        recorder: &mut CloneMetricsRecorder,
    ) -> std::result::Result<UploadPackStreamReport, UploadPackStreamFailure> {
        let clone_limits = match clone_limits.validate() {
            Ok(limits) => limits,
            Err(error) => {
                return Err(finish_stream_preflight(
                    recorder,
                    UploadPackStreamPreflightError::CloneLimits(error),
                ));
            }
        };
        let stream_limits = match stream_limits.validate() {
            Ok(limits) => limits,
            Err(error) => {
                return Err(finish_stream_preflight(
                    recorder,
                    UploadPackStreamPreflightError::StreamLimits(error),
                ));
            }
        };
        if clone_limits
            .validate_request_shape(plan.request.wants.len(), plan.request.haves.len())
            .is_err()
        {
            return Err(finish_stream_preflight(
                recorder,
                UploadPackStreamPreflightError::RequestShape,
            ));
        }
        if plan.objects.len() > clone_limits.max_selected_objects {
            return Err(finish_stream_preflight(
                recorder,
                UploadPackStreamPreflightError::SelectedObjects,
            ));
        }
        if u32::try_from(plan.objects.len()).is_err() {
            return Err(finish_stream_preflight(
                recorder,
                UploadPackStreamPreflightError::ObjectCount,
            ));
        }

        let mut writer = match IncrementalPackWriter::new(
            sink,
            plan.objects.len(),
            self.repo.hash_algo(),
            stream_limits,
            clone_limits,
            cancellation,
        ) {
            Ok(writer) => writer,
            Err(error) => {
                return Err(finish_stream_preflight(
                    recorder,
                    UploadPackStreamPreflightError::WriterConfiguration(error),
                ));
            }
        };
        if let Err(failure) = writer.start().await {
            finish_stream_writer_failure(recorder, &failure);
            return Err(UploadPackStreamFailure::Writer(failure));
        }
        let read_wave_size = 1;
        for oid_wave in plan.objects.chunks(read_wave_size) {
            if let Err(failure) = writer.checkpoint_before_read().await {
                finish_stream_writer_failure(recorder, &failure);
                return Err(UploadPackStreamFailure::Writer(failure));
            }
            let objects = match self.repo.read_objects_batch(oid_wave).await {
                Ok(objects) => objects,
                Err(source) => {
                    let report = writer.abort(PackAbortReason::BackendFailure).await;
                    finish_stream_backend_failure(recorder, &report);
                    return Err(UploadPackStreamFailure::Backend { source, report });
                }
            };
            if objects.len() != oid_wave.len() {
                let source = Error::Backend(format!(
                    "object batch returned {} rows for {} requested ids",
                    objects.len(),
                    oid_wave.len()
                ));
                let report = writer.abort(PackAbortReason::BackendFailure).await;
                finish_stream_backend_failure(recorder, &report);
                return Err(UploadPackStreamFailure::Backend { source, report });
            }
            let decoded_wave_bytes = match objects
                .first()
                .and_then(|entry| entry.object.as_ref())
                .map_or(Ok(0), |object| u64::try_from(object.data.len()))
            {
                Ok(bytes) => bytes,
                Err(_) => {
                    let source = Error::Backend(
                        "object batch decoded size cannot be represented".to_owned(),
                    );
                    let report = writer.abort(PackAbortReason::BackendFailure).await;
                    finish_stream_backend_failure(recorder, &report);
                    return Err(UploadPackStreamFailure::Backend { source, report });
                }
            };
            recorder.observe_memory(CloneMemoryMetrics {
                decoded_bytes: decoded_wave_bytes,
                total_bytes: decoded_wave_bytes,
                ..CloneMemoryMetrics::default()
            });
            for (expected_oid, entry) in oid_wave.iter().zip(objects) {
                if entry.oid != *expected_oid {
                    let source = Error::Backend(format!(
                        "object batch returned {} at the position for {}",
                        entry.oid.to_hex(),
                        expected_oid.to_hex()
                    ));
                    let report = writer.abort(PackAbortReason::BackendFailure).await;
                    finish_stream_backend_failure(recorder, &report);
                    return Err(UploadPackStreamFailure::Backend { source, report });
                }
                let Some(object) = entry.object else {
                    let report = writer.abort(PackAbortReason::BackendFailure).await;
                    finish_stream_backend_failure(recorder, &report);
                    return Err(UploadPackStreamFailure::Backend {
                        source: Error::ObjectNotFound(expected_oid.to_hex()),
                        report,
                    });
                };
                let entry = match writer.write_object(&object).await {
                    Ok(entry) => entry,
                    Err(failure) => {
                        finish_stream_writer_failure(recorder, &failure);
                        return Err(UploadPackStreamFailure::Writer(failure));
                    }
                };
                recorder.record_objects(entry.kind, 1, entry.decoded_bytes, entry.compressed_bytes);
            }
        }
        let writer = match writer.finish().await {
            Ok(report) => report,
            Err(failure) => {
                finish_stream_writer_failure(recorder, &failure);
                return Err(UploadPackStreamFailure::Writer(failure));
            }
        };
        record_stream_metrics(recorder, &writer);
        let metrics = recorder.finish(CloneOutcome::Success);
        Ok(UploadPackStreamReport { writer, metrics })
    }

    /// Stream ACK/NAK and a negotiated raw or channel-1 sideband PACK response.
    ///
    /// `side-band-64k` takes precedence over `side-band`, which takes precedence over raw mode.
    /// All request, plan, writer, fixed framing, and downstream-state checks complete before the
    /// prelude. The raw writer hashes only logical PACK bytes and the decorator awaits downstream
    /// backpressure for every physical frame without constructing a whole wire response. Variable
    /// sideband overhead from compression chunking is checked before each atomic frame write.
    ///
    /// # Errors
    ///
    /// Returns preflight failures before output, a terminal wire failure for ACK/NAK emission, or
    /// the original raw-pack failure plus physical counters after streaming began.
    pub async fn stream_fetch_response<P: CancellationProbe>(
        &self,
        plan: FetchPackPlan,
        downstream: &mut dyn PackChunkSink,
        clone_limits: CloneLimits,
        stream_limits: PackStreamLimits,
        cancellation: P,
        recorder: &mut CloneMetricsRecorder,
    ) -> std::result::Result<UploadPackResponseStreamReport, UploadPackResponseStreamFailure> {
        let clone_limits = match clone_limits.validate() {
            Ok(limits) => limits,
            Err(error) => {
                let _ = recorder.finish(CloneOutcome::Rejected);
                return Err(UploadPackStreamPreflightError::CloneLimits(error).into());
            }
        };
        let stream_limits = match stream_limits.validate() {
            Ok(limits) => limits,
            Err(error) => {
                let _ = recorder.finish(CloneOutcome::Rejected);
                return Err(UploadPackStreamPreflightError::StreamLimits(error).into());
            }
        };
        let request_shape_valid = clone_limits
            .validate_request_shape(plan.request.wants.len(), plan.request.haves.len())
            .is_ok();
        let count_valid = u32::try_from(plan.objects.len()).is_ok();
        let max_chunk = u64::try_from(stream_limits.max_chunk_bytes).ok();
        let minimum_pack = 12_u64.checked_add(self.repo.hash_algo().len() as u64);
        if !request_shape_valid {
            let _ = recorder.finish(CloneOutcome::Rejected);
            return Err(UploadPackStreamPreflightError::RequestShape.into());
        }
        if plan.objects.len() > clone_limits.max_selected_objects {
            let _ = recorder.finish(CloneOutcome::Rejected);
            return Err(UploadPackStreamPreflightError::SelectedObjects.into());
        }
        if !count_valid {
            let _ = recorder.finish(CloneOutcome::Rejected);
            return Err(UploadPackStreamPreflightError::ObjectCount.into());
        }
        if max_chunk.is_none_or(|chunk| chunk > clone_limits.encoded_bytes_in_flight)
            || minimum_pack.is_none_or(|minimum| {
                minimum
                    > stream_limits
                        .max_total_bytes
                        .min(clone_limits.max_output_bytes)
            })
        {
            let _ = recorder.finish(CloneOutcome::Rejected);
            return Err(UploadPackStreamPreflightError::WriterConfiguration(
                PackWriterError::InvalidLimits,
            )
            .into());
        }

        let mode = UploadPackWireMode::negotiate(&plan.request.capabilities);
        let common = plan.common_haves.last().copied();
        if let Err(error) = UploadPackWireSink::validate_response(
            common.as_ref(),
            mode,
            stream_limits,
            self.repo.hash_algo().len(),
            plan.objects.len(),
        ) {
            let _ = recorder.finish(CloneOutcome::Rejected);
            return Err(UploadPackStreamPreflightError::WireConfiguration(error).into());
        }
        let logical_pack_budget = match UploadPackWireSink::logical_pack_budget(
            common.as_ref(),
            mode,
            stream_limits,
            plan.objects.len(),
        ) {
            Ok(budget) => budget,
            Err(error) => {
                let _ = recorder.finish(CloneOutcome::Rejected);
                return Err(UploadPackStreamPreflightError::WireConfiguration(error).into());
            }
        };
        let mut writer_stream_limits = stream_limits;
        writer_stream_limits.max_chunk_bytes =
            mode.max_pack_chunk_bytes(stream_limits.max_chunk_bytes);
        writer_stream_limits.max_total_bytes = logical_pack_budget;
        let mut wire = match UploadPackWireSink::new(downstream, mode, stream_limits) {
            Ok(wire) => wire,
            Err(source) => {
                let _ = recorder.finish(CloneOutcome::Rejected);
                return Err(UploadPackResponseStreamFailure::Wire {
                    source,
                    report: UploadPackWireReport {
                        mode,
                        ..UploadPackWireReport::default()
                    },
                });
            }
        };
        if let Err(source) = wire.write_prelude(common.as_ref()).await {
            let _ = recorder.finish(CloneOutcome::ProtocolFailure);
            return Err(UploadPackResponseStreamFailure::Wire {
                source,
                report: wire.wire_report(),
            });
        }
        let pack = self
            .stream_raw_fetch_pack(
                plan,
                &mut wire,
                clone_limits,
                writer_stream_limits,
                cancellation,
                recorder,
            )
            .await;
        let wire_report = wire.wire_report();
        match pack {
            Ok(pack) => Ok(UploadPackResponseStreamReport {
                pack,
                wire: wire_report,
            }),
            Err(source) => Err(UploadPackResponseStreamFailure::Pack {
                source,
                wire: wire_report,
            }),
        }
    }

    async fn common_haves(&self, haves: &[ObjectId]) -> Result<Vec<ObjectId>> {
        let mut seen = HashSet::new();
        let mut common = Vec::new();
        seen.try_reserve(haves.len())
            .map_err(|_| Error::Backend("cannot reserve bounded clone haves".to_owned()))?;
        common
            .try_reserve(haves.len())
            .map_err(|_| Error::Backend("cannot reserve bounded clone haves".to_owned()))?;
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
        max_objects: usize,
    ) -> Result<HashSet<ObjectId>> {
        let objects = self
            .collect_reachable_excluding(roots, &HashSet::new(), skip_missing, max_objects)
            .await?;
        let mut reachable = HashSet::new();
        reachable
            .try_reserve(objects.len())
            .map_err(|_| Error::Backend("cannot reserve bounded clone closure".to_owned()))?;
        reachable.extend(objects);
        Ok(reachable)
    }

    async fn collect_reachable_excluding(
        &self,
        roots: &[ObjectId],
        exclude: &HashSet<ObjectId>,
        skip_missing: bool,
        max_objects: usize,
    ) -> Result<Vec<ObjectId>> {
        let mut visited = HashSet::new();
        let mut ordered = Vec::new();
        let mut queue = VecDeque::new();
        let initial_capacity = roots.len().min(max_objects);
        visited
            .try_reserve(initial_capacity)
            .map_err(|_| Error::Backend("cannot reserve bounded clone closure".to_owned()))?;
        ordered
            .try_reserve(initial_capacity)
            .map_err(|_| Error::Backend("cannot reserve bounded clone closure".to_owned()))?;
        queue
            .try_reserve(initial_capacity)
            .map_err(|_| Error::Backend("cannot reserve bounded clone closure".to_owned()))?;
        for root in roots {
            enqueue(
                *root,
                exclude,
                &mut visited,
                &mut ordered,
                &mut queue,
                max_objects,
            )?;
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
                        enqueue(
                            parent,
                            exclude,
                            &mut visited,
                            &mut ordered,
                            &mut queue,
                            max_objects,
                        )?;
                    }
                    enqueue(
                        commit.tree,
                        exclude,
                        &mut visited,
                        &mut ordered,
                        &mut queue,
                        max_objects,
                    )?;
                }
                ObjectKind::Tree => {
                    for entry in parse_tree(&object.data)? {
                        if entry.mode != 0o160000 {
                            enqueue(
                                entry.oid,
                                exclude,
                                &mut visited,
                                &mut ordered,
                                &mut queue,
                                max_objects,
                            )?;
                        }
                    }
                }
                ObjectKind::Tag => {
                    let tag = parse_tag(&object.data)?;
                    enqueue(
                        tag.object,
                        exclude,
                        &mut visited,
                        &mut ordered,
                        &mut queue,
                        max_objects,
                    )?;
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
    max_objects: usize,
) -> Result<()> {
    if exclude.contains(&oid) {
        return Ok(());
    }
    if visited.contains(&oid) {
        return Ok(());
    }
    if ordered.len() >= max_objects {
        return Err(Error::Protocol(
            "clone selection exceeds object limit".to_owned(),
        ));
    }
    visited
        .try_reserve(1)
        .map_err(|_| Error::Backend("cannot grow bounded clone closure".to_owned()))?;
    ordered
        .try_reserve(1)
        .map_err(|_| Error::Backend("cannot grow bounded clone closure".to_owned()))?;
    queue
        .try_reserve(1)
        .map_err(|_| Error::Backend("cannot grow bounded clone closure".to_owned()))?;
    visited.insert(oid);
    ordered.push(oid);
    queue.push_back(oid);
    Ok(())
}

fn finish_clone_error(recorder: &mut CloneMetricsRecorder, error: &Error) {
    let outcome = match error {
        Error::Backend(_) | Error::ObjectNotFound(_) | Error::RepositoryNotFound(_) => {
            CloneOutcome::BackendFailure
        }
        _ => CloneOutcome::ProtocolFailure,
    };
    let _ = recorder.finish(outcome);
}

fn finish_stream_preflight(
    recorder: &mut CloneMetricsRecorder,
    error: UploadPackStreamPreflightError,
) -> UploadPackStreamFailure {
    let _ = recorder.finish(CloneOutcome::Rejected);
    UploadPackStreamFailure::Preflight(error)
}

fn finish_stream_writer_failure(recorder: &mut CloneMetricsRecorder, failure: &PackWriterFailure) {
    record_stream_metrics(recorder, &failure.report);
    let outcome = match failure.error {
        PackWriterError::Cancelled => CloneOutcome::Cancelled,
        PackWriterError::ObjectCount
        | PackWriterError::InvalidLimits
        | PackWriterError::TooManyObjects
        | PackWriterError::ObjectCountMismatch
        | PackWriterError::OutputLimit => CloneOutcome::Rejected,
        PackWriterError::InvalidState(_)
        | PackWriterError::ByteOverflow
        | PackWriterError::Allocation
        | PackWriterError::Compression
        | PackWriterError::MissingFallback
        | PackWriterError::Sink(_) => CloneOutcome::ProtocolFailure,
    };
    let _ = recorder.finish(outcome);
}

fn finish_stream_backend_failure(recorder: &mut CloneMetricsRecorder, report: &PackWriterReport) {
    record_stream_metrics(recorder, report);
    let _ = recorder.finish(CloneOutcome::BackendFailure);
}

fn record_stream_metrics(recorder: &mut CloneMetricsRecorder, writer: &PackWriterReport) {
    recorder.record_selected_objects(u64::from(writer.planned_objects));
    let _ = recorder.record_pack(writer.stream.emitted_bytes, 0, 0);
    recorder.observe_memory(CloneMemoryMetrics {
        decoded_bytes: writer.peak_decoded_object_bytes,
        encoded_bytes: writer.peak_compressed_chunk_bytes,
        total_bytes: writer
            .peak_decoded_object_bytes
            .saturating_add(writer.peak_compressed_chunk_bytes),
        ..CloneMemoryMetrics::default()
    });
}
