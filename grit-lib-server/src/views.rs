//! Hosting-oriented read models.

use std::time::Duration;

use grit_lib::objects::{ObjectId, ObjectKind};
use time::OffsetDateTime;

/// Repository-level metadata for landing pages and API summaries.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RepositorySummary {
    /// Branch selected by symbolic `HEAD`, when `HEAD` points at a branch.
    pub default_branch: Option<BranchView>,
    /// Number of stored refs, including `HEAD`.
    pub refs_count: usize,
    /// Number of stored objects in the repository.
    pub object_count: usize,
    /// Commit currently resolved by `HEAD`, including detached `HEAD`.
    pub latest_commit: Option<CommitSummary>,
}

/// Branch metadata for branch listing and default-branch views.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BranchView {
    /// Short branch name without the `refs/heads/` prefix.
    pub name: String,
    /// Full ref name.
    pub refname: String,
    /// Object id stored by the branch ref after symbolic resolution.
    pub target: ObjectId,
    /// Peeled commit metadata when the branch target is available and commit-like.
    pub commit: Option<CommitSummary>,
}

/// Tag metadata for tag listing views.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TagView {
    /// Short tag name without the `refs/tags/` prefix.
    pub name: String,
    /// Full ref name.
    pub refname: String,
    /// Object id stored by the tag ref.
    pub oid: ObjectId,
    /// Object id reached after reading an annotated tag object, or `oid` for lightweight tags.
    pub target: ObjectId,
    /// Kind of the immediate target object when it is known.
    pub target_kind: ObjectKind,
    /// Commit metadata after peeling tag chains, when the tag ultimately names a commit.
    pub peeled_commit: Option<CommitSummary>,
    /// Raw tagger identity line for annotated tags.
    pub tagger: Option<String>,
    /// Annotated tag message, if the ref points at a tag object.
    pub message: Option<String>,
}

/// Commit metadata shown in repository history views.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CommitSummary {
    /// Commit object id.
    pub oid: ObjectId,
    /// Root tree object id.
    pub tree: ObjectId,
    /// Parent commit ids.
    pub parents: Vec<ObjectId>,
    /// Decoded author identity line.
    pub author: String,
    /// Decoded committer identity line.
    pub committer: String,
    /// Commit subject, the first line of the message.
    pub subject: String,
    /// Full decoded commit message.
    pub message: String,
}

/// Tree entry returned by repository browsing APIs.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TreeEntryView {
    /// Entry path relative to the browsed tree root.
    pub path: String,
    /// Entry mode.
    pub mode: u32,
    /// Entry object id.
    pub oid: ObjectId,
    /// Entry kind.
    pub kind: ObjectKind,
    /// Blob size when known.
    pub size: Option<u64>,
}

/// Tree entries returned for a revision and path.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TreeView {
    /// Root tree object id used for indexed path lookup.
    pub root: ObjectId,
    /// Browsed path relative to the root tree.
    pub path: String,
    /// Object id for the browsed tree.
    pub oid: ObjectId,
    /// Direct child entries below `path`.
    pub entries: Vec<TreeEntryView>,
}

/// Blob contents returned by path lookup APIs.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlobView {
    /// Blob object id.
    pub oid: ObjectId,
    /// Blob path relative to the tree root.
    pub path: String,
    /// Blob mode from the tree entry.
    pub mode: u32,
    /// Blob data.
    pub data: Vec<u8>,
}

/// Blob metadata returned without transferring blob contents.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlobMetadataView {
    /// Blob object id.
    pub oid: ObjectId,
    /// Blob path relative to the tree root.
    pub path: String,
    /// Blob mode from the tree entry.
    pub mode: u32,
    /// Blob size when known from the browse index.
    pub size: Option<u64>,
}

/// Preferred delivery mode for blob contents.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BlobContentDelivery {
    /// Return the blob contents through the server API.
    Inline,
    /// Materialize the raw blob bytes and return a signed direct-download URL.
    SignedUrl,
    /// Return small blobs inline and large blobs through a signed direct-download URL.
    Auto,
}

/// Options for server-mediated blob content delivery.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlobDownloadOptions {
    /// Delivery mode.
    pub delivery: BlobContentDelivery,
    /// Maximum blob size returned inline when `delivery` is [`BlobContentDelivery::Auto`].
    pub inline_threshold: usize,
    /// Signed URL validity duration.
    pub expires_in: Duration,
    /// Explicit signing start time supplied by the embedding server.
    pub issued_at: OffsetDateTime,
    /// Optional prefix prepended to materialized raw blob keys.
    pub key_prefix: String,
}

impl BlobDownloadOptions {
    /// Create default automatic blob download options with an explicit issue timestamp.
    #[must_use]
    pub fn new(issued_at: OffsetDateTime) -> Self {
        Self {
            delivery: BlobContentDelivery::Auto,
            inline_threshold: 64 * 1024,
            expires_in: Duration::from_secs(5 * 60),
            issued_at,
            key_prefix: String::new(),
        }
    }
}

/// Signed direct-download URL for materialized raw blob contents.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SignedContentUrl {
    /// HTTP method to use with `url`.
    pub method: String,
    /// Signed URL.
    pub url: String,
    /// Additional HTTP headers the client must send with the signed request.
    pub headers: Vec<(String, String)>,
    /// Timestamp after which the URL should no longer be used.
    pub expires_at: OffsetDateTime,
}

/// Direct-download blob response.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlobDownloadView {
    /// Blob object id.
    pub oid: ObjectId,
    /// Blob path relative to the tree root.
    pub path: String,
    /// Blob mode from the tree entry.
    pub mode: u32,
    /// Blob size in bytes.
    pub size: u64,
    /// Signed direct-download URL.
    pub signed_url: SignedContentUrl,
}

/// Blob content response that either carries bytes inline or delegates transfer to object storage.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BlobContentView {
    /// Inline blob contents returned by the server.
    Inline(BlobView),
    /// Signed direct-download URL for raw blob contents.
    Redirect(BlobDownloadView),
}

/// Conventional file discovered in a repository tree.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DiscoveredFile {
    /// Candidate path supplied by the caller.
    pub candidate: String,
    /// Blob view for the discovered file.
    pub blob: BlobView,
}

/// Normalized commit inputs for later compare and diff views.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CompareInputs {
    /// Base commit.
    pub base: CommitSummary,
    /// Head commit.
    pub head: CommitSummary,
    /// Root tree for the base commit.
    pub base_tree: ObjectId,
    /// Root tree for the head commit.
    pub head_tree: ObjectId,
}

/// Options for paginated commit history queries.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CommitHistoryOptions {
    /// Number of matching commits to skip before returning results.
    pub offset: usize,
    /// Maximum number of commits to return. `None` returns every matching commit after `offset`.
    pub limit: Option<usize>,
    /// Optional repository-relative path filter.
    pub path: Option<String>,
}

/// One page of commit history.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CommitHistoryPage {
    /// Commits in deterministic reverse chronological traversal order.
    pub commits: Vec<CommitSummary>,
    /// Offset to request for the next page, or `None` when this is the last page.
    pub next_offset: Option<usize>,
    /// Number of commits matching the query.
    pub total_estimate: usize,
}

/// Ahead/behind result for two commits.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CommitComparison {
    /// Base commit used for the comparison.
    pub base: CommitSummary,
    /// Head commit used for the comparison.
    pub head: CommitSummary,
    /// Best common ancestor found for `base` and `head`.
    pub merge_base: Option<CommitSummary>,
    /// Number of commits reachable from `head` that are not reachable from `base`.
    pub ahead_by: usize,
    /// Number of commits reachable from `base` that are not reachable from `head`.
    pub behind_by: usize,
}
