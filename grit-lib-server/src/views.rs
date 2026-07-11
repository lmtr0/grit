//! Hosting-oriented read models.

use grit_lib::objects::{ObjectId, ObjectKind};

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
