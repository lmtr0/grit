//! Canonical full-clone identity, manifest, and pure singleflight state.

use grit_lib::objects::{HashAlgo, ObjectId};
use sha2::{Digest, Sha256};
use time::OffsetDateTime;

use crate::ids::{RepositoryId, TenantId};
use crate::protocol::upload_pack::{UploadPackCapability, UploadPackRequest};

/// Maximum distinct wants in one canonical clone identity.
pub const MAX_CANONICAL_CLONE_WANTS: usize = 65_536;

/// Tenant and repository namespace included in every canonical clone key.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct CanonicalCloneScope {
    /// Tenant that owns the repository.
    pub tenant: TenantId,
    /// Tenant-scoped repository identity.
    pub repository: RepositoryId,
}

/// Stable repository incarnation and visible generations bound to a canonical pack.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct CanonicalCloneGeneration {
    /// Nonzero repository incarnation, changed after delete/recreate.
    pub incarnation: u64,
    /// Overall durable repository generation.
    pub repository: u64,
    /// Visible ref generation.
    pub refs: u64,
}

impl CanonicalCloneGeneration {
    fn validate(self) -> Result<Self, CanonicalCloneError> {
        if self.incarnation == 0 {
            return Err(CanonicalCloneError::InvalidGeneration);
        }
        Ok(self)
    }
}

/// Receiver state supported by canonical full-clone packs.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum CanonicalCloneReceiver {
    /// Receiver declares no existing objects.
    NoHaves,
}

/// Shallow boundary shape supported by canonical packs.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum CanonicalCloneShallow {
    /// Complete, unshallowed history.
    None,
}

/// History-deepening shape supported by canonical packs.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum CanonicalCloneDeepen {
    /// Complete history without a depth or time boundary.
    None,
}

/// Object filter shape supported by canonical packs.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum CanonicalCloneFilter {
    /// No partial-clone object filter.
    None,
}

/// Pack-representation choices that affect canonical bytes.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub struct CanonicalCloneRepresentation {
    /// Allow offset-delta entries.
    pub ofs_delta: bool,
    /// Allow a thin pack relative to explicitly modelled receiver state.
    pub thin_pack: bool,
}

/// Complete logical identity of a reusable canonical clone pack.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CanonicalCloneShape {
    /// Tenant/repository namespace whose immutable bytes this shape represents.
    pub scope: CanonicalCloneScope,
    /// Repository identity and mutable generations captured by the pack.
    pub generation: CanonicalCloneGeneration,
    /// Sorted, duplicate-free wanted object IDs.
    pub wants: Vec<ObjectId>,
    /// Repository object format.
    pub hash_algo: HashAlgo,
    /// Whether annotated tags reachable from wants are included.
    pub include_tag: bool,
    /// Receiver state; canonical packs currently require no haves.
    pub receiver: CanonicalCloneReceiver,
    /// Shallow shape; currently complete history only.
    pub shallow: CanonicalCloneShallow,
    /// Deepening shape; currently complete history only.
    pub deepen: CanonicalCloneDeepen,
    /// Filter shape; currently no filter only.
    pub filter: CanonicalCloneFilter,
    /// Byte-affecting representation choices.
    pub representation: CanonicalCloneRepresentation,
}

impl CanonicalCloneShape {
    /// Build and normalize a canonical shape from an upload-pack request.
    ///
    /// Sideband and agent capabilities are deliberately excluded because they do not change raw
    /// PACK bytes. Unknown capabilities are rejected rather than silently aliasing cache keys.
    ///
    /// # Errors
    ///
    /// Returns a typed error for haves, empty/oversized wants, unknown capabilities, mixed object
    /// formats, or invalid repository identity.
    pub fn from_request(
        request: &UploadPackRequest,
        scope: CanonicalCloneScope,
        generation: CanonicalCloneGeneration,
        hash_algo: HashAlgo,
    ) -> Result<Self, CanonicalCloneError> {
        generation.validate()?;
        if !request.haves.is_empty() {
            return Err(CanonicalCloneError::HavesUnsupported);
        }
        if request.wants.is_empty() {
            return Err(CanonicalCloneError::EmptyWants);
        }
        if request.wants.len() > MAX_CANONICAL_CLONE_WANTS {
            return Err(CanonicalCloneError::TooManyWants);
        }
        if request.wants.iter().any(ObjectId::is_zero) {
            return Err(CanonicalCloneError::NullWant);
        }
        if request.wants.iter().any(|oid| oid.algo() != hash_algo) {
            return Err(CanonicalCloneError::ObjectFormatMismatch);
        }
        let mut include_tag = false;
        let mut representation = CanonicalCloneRepresentation::default();
        let mut object_format_seen = false;
        for capability in &request.capabilities {
            match capability {
                UploadPackCapability::IncludeTag => include_tag = true,
                UploadPackCapability::OfsDelta => representation.ofs_delta = true,
                // A no-haves canonical full clone has no receiver bases, so its raw pack must be
                // self-contained whether or not the peer advertises thin-pack support.
                UploadPackCapability::ThinPack => {}
                UploadPackCapability::ObjectFormat(algo) => {
                    if object_format_seen {
                        return Err(CanonicalCloneError::AmbiguousObjectFormat);
                    }
                    object_format_seen = true;
                    if *algo != hash_algo {
                        return Err(CanonicalCloneError::ObjectFormatMismatch);
                    }
                }
                UploadPackCapability::Other(_) => {
                    return Err(CanonicalCloneError::UnsupportedCapability);
                }
                UploadPackCapability::MultiAck
                | UploadPackCapability::MultiAckDetailed
                | UploadPackCapability::SideBand
                | UploadPackCapability::SideBand64k
                | UploadPackCapability::Agent(_) => {}
            }
        }
        let mut wants = Vec::new();
        wants
            .try_reserve_exact(request.wants.len())
            .map_err(|_| CanonicalCloneError::Allocation)?;
        wants.extend_from_slice(&request.wants);
        wants.sort_unstable();
        wants.dedup();
        Ok(Self {
            scope,
            generation,
            wants,
            hash_algo,
            include_tag,
            receiver: CanonicalCloneReceiver::NoHaves,
            shallow: CanonicalCloneShallow::None,
            deepen: CanonicalCloneDeepen::None,
            filter: CanonicalCloneFilter::None,
            representation,
        })
    }

    /// Compute the versioned deterministic SHA-256 identity for this shape.
    ///
    /// # Errors
    ///
    /// Returns allocation or length errors before hashing an incomplete encoding.
    pub fn key(&self) -> Result<CanonicalCloneKey, CanonicalCloneError> {
        self.generation.validate()?;
        if self.wants.is_empty() || self.wants.len() > MAX_CANONICAL_CLONE_WANTS {
            return Err(CanonicalCloneError::InvalidShape);
        }
        if self.wants.windows(2).any(|pair| pair[0] >= pair[1])
            || self.wants.iter().any(|oid| oid.algo() != self.hash_algo)
        {
            return Err(CanonicalCloneError::InvalidShape);
        }
        if self.representation.thin_pack {
            return Err(CanonicalCloneError::InvalidShape);
        }
        let mut canonical = Sha256::new();
        hash_field(&mut canonical, b"grit-canonical-clone-v1")?;
        hash_field(&mut canonical, self.scope.tenant.as_str().as_bytes())?;
        hash_field(&mut canonical, self.scope.repository.as_str().as_bytes())?;
        canonical.update(self.generation.incarnation.to_be_bytes());
        canonical.update(self.generation.repository.to_be_bytes());
        canonical.update(self.generation.refs.to_be_bytes());
        canonical.update([self.hash_algo.oid_version()]);
        canonical.update([u8::from(self.include_tag)]);
        canonical.update([0]); // receiver: no haves
        canonical.update([0]); // shallow: none
        canonical.update([0]); // deepen: none
        canonical.update([0]); // filter: none
        canonical.update([u8::from(self.representation.ofs_delta)]);
        canonical.update([0]); // thin-pack: forbidden for a self-contained full clone
        canonical.update(
            u32::try_from(self.wants.len())
                .map_err(|_| CanonicalCloneError::Length)?
                .to_be_bytes(),
        );
        for oid in &self.wants {
            hash_field(&mut canonical, oid.as_bytes())?;
        }
        let digest = canonical.finalize();
        let mut key = [0_u8; 32];
        key.copy_from_slice(&digest);
        Ok(CanonicalCloneKey(key))
    }
}

fn hash_field(output: &mut Sha256, bytes: &[u8]) -> Result<(), CanonicalCloneError> {
    let length = u32::try_from(bytes.len()).map_err(|_| CanonicalCloneError::Length)?;
    output.update(length.to_be_bytes());
    output.update(bytes);
    Ok(())
}

/// Versioned SHA-256 canonical clone key.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct CanonicalCloneKey([u8; 32]);

impl CanonicalCloneKey {
    /// Borrow the 32 raw key bytes.
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

/// Bounded opaque storage locator for canonical pack bytes.
#[derive(Clone, PartialEq, Eq)]
pub struct CanonicalCloneLocator(String);

impl std::fmt::Debug for CanonicalCloneLocator {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("CanonicalCloneLocator(<opaque>)")
    }
}

impl CanonicalCloneLocator {
    /// Validate and construct an opaque locator.
    ///
    /// # Errors
    ///
    /// Returns [`CanonicalCloneError::InvalidLocator`] for empty or overlong values.
    pub fn new(value: impl Into<String>) -> Result<Self, CanonicalCloneError> {
        let value = value.into();
        if value.trim().is_empty() || value.len() > 2_048 {
            return Err(CanonicalCloneError::InvalidLocator);
        }
        Ok(Self(value))
    }

    /// Borrow the opaque locator.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Verification state of immutable canonical PACK bytes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CanonicalCloneVerification {
    /// Stored bytes have not yet been checksum-verified.
    Unverified,
    /// Stored bytes and declared checksum were verified.
    Verified,
}

/// Lifecycle state of a canonical pack manifest.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CanonicalCloneManifestState {
    /// Manifest may be served when also verified.
    Ready,
    /// Manifest is retained until its explicit retirement time but is not eligible.
    Retired,
    /// Pack generation or verification failed and is not eligible.
    Failed,
}

/// Purpose of an immutable generated pack.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ClonePackPurpose {
    /// Reusable full-clone representation.
    Canonical,
}

/// Immutable canonical clone pack metadata.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CanonicalCloneManifest {
    /// Manifest purpose.
    pub purpose: ClonePackPurpose,
    /// Canonical request/generation identity.
    pub key: CanonicalCloneKey,
    /// Complete validated request shape represented by the raw pack.
    pub shape: CanonicalCloneShape,
    /// Repository generation captured by the pack.
    pub generation: CanonicalCloneGeneration,
    /// Raw PACK trailing checksum.
    pub pack_checksum: ObjectId,
    /// Raw PACK hash algorithm.
    pub hash_algo: HashAlgo,
    /// PACK header object count.
    pub object_count: u32,
    /// Complete raw PACK size including trailer.
    pub size_bytes: u64,
    /// Caller-supplied creation time.
    pub created_at: OffsetDateTime,
    /// Opaque durable byte location.
    pub locator: CanonicalCloneLocator,
    /// Checksum verification state.
    pub verification: CanonicalCloneVerification,
    /// Serving/retirement state.
    pub state: CanonicalCloneManifestState,
    /// Explicit retirement deadline, required only for retired manifests.
    pub retire_after: Option<OffsetDateTime>,
}

impl CanonicalCloneManifest {
    /// Validate manifest invariants and serving eligibility at `observed_at`.
    ///
    /// # Errors
    ///
    /// Returns a typed error for checksum algorithms, impossible sizes, or retirement shape.
    pub fn validate(&self) -> Result<(), CanonicalCloneError> {
        self.generation.validate()?;
        if self.shape.key()? != self.key
            || self.shape.generation != self.generation
            || self.shape.hash_algo != self.hash_algo
            || self.purpose != ClonePackPurpose::Canonical
            || self.pack_checksum.algo() != self.hash_algo
            || self.pack_checksum.is_zero()
            || self.object_count == 0
            || usize::try_from(self.object_count)
                .map_or(true, |count| count < self.shape.wants.len())
            || self.size_bytes < 12_u64.saturating_add(self.hash_algo.len() as u64)
            || (self.state == CanonicalCloneManifestState::Ready
                && self.verification != CanonicalCloneVerification::Verified)
            || (self.state == CanonicalCloneManifestState::Retired) != self.retire_after.is_some()
            || self
                .retire_after
                .is_some_and(|retire_after| retire_after <= self.created_at)
        {
            return Err(CanonicalCloneError::InvalidManifest);
        }
        Ok(())
    }

    /// Return whether this exact manifest may be served.
    #[must_use]
    pub fn is_eligible(&self) -> bool {
        self.verification == CanonicalCloneVerification::Verified
            && self.state == CanonicalCloneManifestState::Ready
            && self.retire_after.is_none()
    }
}

/// Bounded opaque singleflight ownership token.
#[derive(Clone, PartialEq, Eq)]
pub struct CanonicalCloneToken(String);

impl std::fmt::Debug for CanonicalCloneToken {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("CanonicalCloneToken(<redacted>)")
    }
}

impl CanonicalCloneToken {
    /// Validate and construct an ownership token.
    pub fn new(value: impl Into<String>) -> Result<Self, CanonicalCloneError> {
        let value = value.into();
        if value.trim().is_empty() || value.len() > 256 {
            return Err(CanonicalCloneError::InvalidToken);
        }
        Ok(Self(value))
    }

    /// Borrow the opaque token.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Active fenced singleflight lease.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CanonicalCloneLease {
    /// Opaque worker ownership token.
    pub token: CanonicalCloneToken,
    /// Strictly increasing fencing value.
    pub fencing: u64,
    /// Explicit time at which this lease was first issued.
    pub issued_at: OffsetDateTime,
    /// Latest successful owner observation, used to reject time reversal.
    pub last_observed_at: OffsetDateTime,
    /// Exclusive caller-supplied lease deadline.
    pub expires_at: OffsetDateTime,
}

/// Pure singleflight lifecycle.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CanonicalCloneFlightState {
    /// No worker currently owns generation.
    Idle { last_fencing: u64 },
    /// One worker owns generation until its lease expires.
    Building(CanonicalCloneLease),
    /// A verified eligible manifest was published.
    Published(CanonicalCloneManifest),
    /// Last builder failed; a higher fencing claim may retry.
    Failed {
        /// Fence retained against stale retries.
        last_fencing: u64,
        /// Redacted owner identity retained to make failure retries idempotent.
        token: CanonicalCloneToken,
    },
    /// Generation was invalidated and can never publish.
    Invalidated {
        /// Explicit invalidation observation time.
        invalidated_at: OffsetDateTime,
        /// Published bytes retained as retired metadata until asynchronous deletion.
        retired: Option<Box<CanonicalCloneManifest>>,
    },
}

/// Mutable pure state for one canonical key/generation singleflight.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CanonicalCloneSingleflight {
    /// Canonical clone key governed by this state.
    key: CanonicalCloneKey,
    /// Exact repository generation governed by this state.
    generation: CanonicalCloneGeneration,
    /// Current pure lifecycle state.
    state: CanonicalCloneFlightState,
    last_observed_at: Option<OffsetDateTime>,
}

/// Claim attempt outcome.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CanonicalCloneClaimResult {
    /// Claim was installed with the supplied token/fence/lease.
    Claimed,
    /// A nonexpired builder owns the flight.
    Busy,
    /// An eligible manifest is already published.
    Published,
    /// Generation was invalidated.
    Ineligible,
    /// Fence did not advance beyond the last owner.
    Stale,
}

/// Fenced mutation outcome.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CanonicalCloneMutationResult {
    /// Exact owned mutation was applied.
    Applied,
    /// Token, fence, lease, key, or generation was stale/mismatched.
    Stale,
    /// Generation was invalidated.
    Ineligible,
}

impl CanonicalCloneSingleflight {
    /// Create idle state for a validated key/generation.
    pub fn new(
        key: CanonicalCloneKey,
        generation: CanonicalCloneGeneration,
    ) -> Result<Self, CanonicalCloneError> {
        generation.validate()?;
        Ok(Self {
            key,
            generation,
            state: CanonicalCloneFlightState::Idle { last_fencing: 0 },
            last_observed_at: None,
        })
    }

    /// Return the canonical key guarded by this flight.
    #[must_use]
    pub const fn key(&self) -> CanonicalCloneKey {
        self.key
    }

    /// Return the exact repository generation guarded by this flight.
    #[must_use]
    pub const fn generation(&self) -> CanonicalCloneGeneration {
        self.generation
    }

    /// Borrow the current lifecycle state.
    #[must_use]
    pub const fn state(&self) -> &CanonicalCloneFlightState {
        &self.state
    }

    fn validate_observed_at(&self, observed_at: OffsetDateTime) -> Result<(), CanonicalCloneError> {
        if self
            .last_observed_at
            .is_some_and(|previous| observed_at < previous)
        {
            return Err(CanonicalCloneError::NonMonotonicTime);
        }
        Ok(())
    }

    /// Claim or reclaim generation using a strictly newer fence and explicit lease times.
    pub fn claim(
        &mut self,
        token: CanonicalCloneToken,
        fencing: u64,
        observed_at: OffsetDateTime,
        expires_at: OffsetDateTime,
    ) -> Result<CanonicalCloneClaimResult, CanonicalCloneError> {
        if fencing == 0 || expires_at <= observed_at {
            return Err(CanonicalCloneError::InvalidLease);
        }
        self.validate_observed_at(observed_at)?;
        let last = match &self.state {
            CanonicalCloneFlightState::Idle { last_fencing }
            | CanonicalCloneFlightState::Failed { last_fencing, .. } => *last_fencing,
            CanonicalCloneFlightState::Building(lease) if observed_at >= lease.expires_at => {
                lease.fencing
            }
            CanonicalCloneFlightState::Building(_) => return Ok(CanonicalCloneClaimResult::Busy),
            CanonicalCloneFlightState::Published(_) => {
                return Ok(CanonicalCloneClaimResult::Published);
            }
            CanonicalCloneFlightState::Invalidated { .. } => {
                return Ok(CanonicalCloneClaimResult::Ineligible);
            }
        };
        if last == u64::MAX {
            return Err(CanonicalCloneError::FencingOverflow);
        }
        if fencing <= last {
            return Ok(CanonicalCloneClaimResult::Stale);
        }
        self.state = CanonicalCloneFlightState::Building(CanonicalCloneLease {
            token,
            fencing,
            issued_at: observed_at,
            last_observed_at: observed_at,
            expires_at,
        });
        self.last_observed_at = Some(observed_at);
        Ok(CanonicalCloneClaimResult::Claimed)
    }

    /// Renew the exact current owner without changing its fence.
    pub fn renew(
        &mut self,
        token: &CanonicalCloneToken,
        fencing: u64,
        observed_at: OffsetDateTime,
        expires_at: OffsetDateTime,
    ) -> Result<CanonicalCloneMutationResult, CanonicalCloneError> {
        if expires_at <= observed_at {
            return Err(CanonicalCloneError::InvalidLease);
        }
        self.validate_observed_at(observed_at)?;
        let CanonicalCloneFlightState::Building(lease) = &mut self.state else {
            return Ok(
                if matches!(self.state, CanonicalCloneFlightState::Invalidated { .. }) {
                    CanonicalCloneMutationResult::Ineligible
                } else {
                    CanonicalCloneMutationResult::Stale
                },
            );
        };
        if lease.token != *token
            || lease.fencing != fencing
            || observed_at < lease.last_observed_at
            || observed_at >= lease.expires_at
            || expires_at <= lease.expires_at
        {
            return Ok(CanonicalCloneMutationResult::Stale);
        }
        lease.last_observed_at = observed_at;
        lease.expires_at = expires_at;
        self.last_observed_at = Some(observed_at);
        Ok(CanonicalCloneMutationResult::Applied)
    }

    /// Publish only from the exact live owner and exact key/generation.
    pub fn publish(
        &mut self,
        token: &CanonicalCloneToken,
        fencing: u64,
        observed_at: OffsetDateTime,
        manifest: CanonicalCloneManifest,
    ) -> Result<CanonicalCloneMutationResult, CanonicalCloneError> {
        manifest.validate()?;
        self.validate_observed_at(observed_at)?;
        let CanonicalCloneFlightState::Building(lease) = &self.state else {
            return Ok(
                if matches!(self.state, CanonicalCloneFlightState::Invalidated { .. }) {
                    CanonicalCloneMutationResult::Ineligible
                } else {
                    CanonicalCloneMutationResult::Stale
                },
            );
        };
        if lease.token != *token
            || lease.fencing != fencing
            || observed_at < lease.last_observed_at
            || observed_at >= lease.expires_at
            || manifest.key != self.key
            || manifest.generation != self.generation
            || manifest.created_at > observed_at
            || !manifest.is_eligible()
        {
            return Ok(CanonicalCloneMutationResult::Stale);
        }
        self.state = CanonicalCloneFlightState::Published(manifest);
        self.last_observed_at = Some(observed_at);
        Ok(CanonicalCloneMutationResult::Applied)
    }

    /// Mark the exact live owner failed, preserving its fence against stale retries.
    pub fn fail(
        &mut self,
        token: &CanonicalCloneToken,
        fencing: u64,
        observed_at: OffsetDateTime,
    ) -> Result<CanonicalCloneMutationResult, CanonicalCloneError> {
        self.validate_observed_at(observed_at)?;
        if let CanonicalCloneFlightState::Failed {
            last_fencing,
            token: failed_token,
        } = &self.state
        {
            return Ok(if *last_fencing == fencing && failed_token == token {
                CanonicalCloneMutationResult::Applied
            } else {
                CanonicalCloneMutationResult::Stale
            });
        }
        let CanonicalCloneFlightState::Building(lease) = &self.state else {
            return Ok(
                if matches!(self.state, CanonicalCloneFlightState::Invalidated { .. }) {
                    CanonicalCloneMutationResult::Ineligible
                } else {
                    CanonicalCloneMutationResult::Stale
                },
            );
        };
        if lease.token != *token
            || lease.fencing != fencing
            || observed_at < lease.last_observed_at
            || observed_at >= lease.expires_at
        {
            return Ok(CanonicalCloneMutationResult::Stale);
        }
        self.state = CanonicalCloneFlightState::Failed {
            last_fencing: fencing,
            token: token.clone(),
        };
        self.last_observed_at = Some(observed_at);
        Ok(CanonicalCloneMutationResult::Applied)
    }

    /// Immediately and permanently invalidate this exact generation.
    pub fn invalidate(
        &mut self,
        generation: CanonicalCloneGeneration,
        invalidated_at: OffsetDateTime,
        retire_after: OffsetDateTime,
    ) -> Result<CanonicalCloneMutationResult, CanonicalCloneError> {
        self.validate_observed_at(invalidated_at)?;
        if retire_after <= invalidated_at {
            return Err(CanonicalCloneError::InvalidManifest);
        }
        if generation != self.generation {
            return Ok(CanonicalCloneMutationResult::Stale);
        }
        if matches!(self.state, CanonicalCloneFlightState::Invalidated { .. }) {
            return Ok(CanonicalCloneMutationResult::Applied);
        }
        let retired = if let CanonicalCloneFlightState::Published(manifest) = &self.state {
            let mut manifest = manifest.clone();
            manifest.state = CanonicalCloneManifestState::Retired;
            manifest.retire_after = Some(retire_after);
            manifest.validate()?;
            Some(Box::new(manifest))
        } else {
            None
        };
        self.state = CanonicalCloneFlightState::Invalidated {
            invalidated_at,
            retired,
        };
        self.last_observed_at = Some(invalidated_at);
        Ok(CanonicalCloneMutationResult::Applied)
    }
}

/// Atomic persistence result for a canonical singleflight state transition.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CanonicalCloneStoreCas {
    /// Expected state matched and the replacement was stored atomically.
    Applied,
    /// Durable state changed before the replacement could be stored.
    Conflict,
}

/// Runtime-neutral persistence boundary for canonical manifests and singleflight state.
///
/// Implementations must compare the complete expected value atomically, preserve monotonic
/// fencing across deletion/recreation of a key, and bound durable result/allocation sizes. The
/// model deliberately does not prescribe an async runtime or database client.
pub trait CanonicalCloneStore: Send + Sync {
    /// Backend-specific persistence failure.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Load the exact state for `key`, if it exists.
    ///
    /// # Errors
    ///
    /// Returns the adapter's typed persistence error.
    fn load(
        &self,
        key: &CanonicalCloneKey,
    ) -> Result<Option<CanonicalCloneSingleflight>, Self::Error>;

    /// Atomically install `replacement` only when durable state equals `expected`.
    ///
    /// `expected = None` is an insert-if-absent operation. Implementations must reject a
    /// replacement whose key differs from `key`.
    ///
    /// # Errors
    ///
    /// Returns the adapter's typed persistence error.
    fn compare_and_swap(
        &self,
        key: &CanonicalCloneKey,
        expected: Option<&CanonicalCloneSingleflight>,
        replacement: &CanonicalCloneSingleflight,
    ) -> Result<CanonicalCloneStoreCas, Self::Error>;
}

/// Canonical clone shape, manifest, or singleflight validation failure.
#[derive(Clone, Copy, Debug, thiserror::Error, PartialEq, Eq)]
pub enum CanonicalCloneError {
    /// Repository incarnation must be nonzero.
    #[error("invalid canonical clone repository generation")]
    InvalidGeneration,
    /// Canonical full clones require at least one want.
    #[error("canonical clone wants must not be empty")]
    EmptyWants,
    /// Null object IDs cannot name objects in a canonical clone request.
    #[error("canonical clone wants contain a null object id")]
    NullWant,
    /// Want count exceeded the canonical bound.
    #[error("canonical clone has too many wants")]
    TooManyWants,
    /// Receiver haves make the response receiver-specific.
    #[error("canonical clone does not support receiver haves")]
    HavesUnsupported,
    /// An unmodelled capability could affect the response.
    #[error("canonical clone capability is unsupported")]
    UnsupportedCapability,
    /// Request and repository object formats differ.
    #[error("canonical clone object format mismatch")]
    ObjectFormatMismatch,
    /// More than one object-format declaration made request interpretation ambiguous.
    #[error("canonical clone object format capability is ambiguous")]
    AmbiguousObjectFormat,
    /// A manually constructed shape violated canonical ordering or bounds.
    #[error("invalid canonical clone shape")]
    InvalidShape,
    /// Canonical length arithmetic overflowed.
    #[error("canonical clone encoding length overflow")]
    Length,
    /// Bounded canonical encoding allocation failed.
    #[error("canonical clone allocation failed")]
    Allocation,
    /// Opaque storage locator was empty or too long.
    #[error("invalid canonical clone locator")]
    InvalidLocator,
    /// Manifest fields are inconsistent.
    #[error("invalid canonical clone manifest")]
    InvalidManifest,
    /// Opaque ownership token was empty or too long.
    #[error("invalid canonical clone ownership token")]
    InvalidToken,
    /// Fencing or lease timestamps were invalid.
    #[error("invalid canonical clone lease")]
    InvalidLease,
    /// A supplied observation preceded the last applied state transition.
    #[error("canonical clone observation time moved backwards")]
    NonMonotonicTime,
    /// No strictly greater fencing value can be represented.
    #[error("canonical clone fencing sequence exhausted")]
    FencingOverflow,
}
