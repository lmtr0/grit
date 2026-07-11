//! Hosting-oriented read models.

use grit_lib::objects::{ObjectId, ObjectKind};

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
