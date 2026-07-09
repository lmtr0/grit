#![allow(dead_code)]

use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
};
use grit_lib::objects::{ObjectId, ObjectKind};
use grit_lib_server::{
    error::Error,
    views::{
        BlobContentView, BlobMetadataView, BlobView, BranchView, CommitSummary, RepositorySummary,
        SignedContentUrl, TagView, TreeEntryView, TreeView,
    },
};
use serde::Serialize;

pub struct AppError(pub Error);

impl From<Error> for AppError {
    fn from(error: Error) -> Self {
        Self(error)
    }
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let status = match &self.0 {
            Error::PathNotFound(_) | Error::RefNotFound(_) | Error::ObjectNotFound(_) => {
                StatusCode::NOT_FOUND
            }
            Error::InvalidRepositoryId(_)
            | Error::InvalidTenantId(_)
            | Error::UnexpectedObjectKind { .. }
            | Error::Protocol(_) => StatusCode::BAD_REQUEST,
            Error::AuthorizationDenied { .. }
            | Error::PushPolicyRejected { .. }
            | Error::NonFastForward { .. } => StatusCode::FORBIDDEN,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        };
        (status, self.0.to_string()).into_response()
    }
}

#[derive(Serialize)]
pub struct RepositorySummaryDto {
    default_branch: Option<BranchDto>,
    refs_count: usize,
    object_count: usize,
    latest_commit: Option<CommitDto>,
}

#[derive(Serialize)]
pub struct BranchDto {
    name: String,
    refname: String,
    target: String,
    commit: Option<CommitDto>,
}

#[derive(Serialize)]
pub struct TagDto {
    name: String,
    refname: String,
    oid: String,
    target: String,
    target_kind: String,
    peeled_commit: Option<CommitDto>,
    tagger: Option<String>,
    message: Option<String>,
}

#[derive(Serialize)]
pub struct CommitDto {
    oid: String,
    tree: String,
    parents: Vec<String>,
    author: String,
    committer: String,
    subject: String,
    message: String,
}

#[derive(Serialize)]
pub struct TreeDto {
    root: String,
    path: String,
    oid: String,
    entries: Vec<TreeEntryDto>,
}

#[derive(Serialize)]
pub struct TreeEntryDto {
    path: String,
    mode: u32,
    oid: String,
    kind: String,
    size: Option<u64>,
}

#[derive(Serialize)]
pub struct BlobMetadataDto {
    oid: String,
    path: String,
    mode: u32,
    size: Option<u64>,
}

#[derive(Serialize)]
pub struct BlobDto {
    oid: String,
    path: String,
    mode: u32,
    content_utf8_lossy: String,
    size: usize,
}

#[derive(Serialize)]
pub struct BlobContentDto {
    delivery: &'static str,
    inline: Option<BlobDto>,
    signed_url: Option<SignedContentUrlDto>,
}

#[derive(Serialize)]
pub struct SignedContentUrlDto {
    method: String,
    url: String,
    headers: Vec<(String, String)>,
    expires_at: String,
}

pub fn summary_dto(view: RepositorySummary) -> RepositorySummaryDto {
    RepositorySummaryDto {
        default_branch: view.default_branch.map(branch_dto),
        refs_count: view.refs_count,
        object_count: view.object_count,
        latest_commit: view.latest_commit.map(commit_dto),
    }
}

pub fn branch_dto(view: BranchView) -> BranchDto {
    BranchDto {
        name: view.name,
        refname: view.refname,
        target: hex(view.target),
        commit: view.commit.map(commit_dto),
    }
}

pub fn tag_dto(view: TagView) -> TagDto {
    TagDto {
        name: view.name,
        refname: view.refname,
        oid: hex(view.oid),
        target: hex(view.target),
        target_kind: kind(view.target_kind),
        peeled_commit: view.peeled_commit.map(commit_dto),
        tagger: view.tagger,
        message: view.message,
    }
}

pub fn commit_dto(view: CommitSummary) -> CommitDto {
    CommitDto {
        oid: hex(view.oid),
        tree: hex(view.tree),
        parents: view.parents.into_iter().map(hex).collect(),
        author: view.author,
        committer: view.committer,
        subject: view.subject,
        message: view.message,
    }
}

pub fn tree_dto(view: TreeView) -> TreeDto {
    TreeDto {
        root: hex(view.root),
        path: view.path,
        oid: hex(view.oid),
        entries: view.entries.into_iter().map(tree_entry_dto).collect(),
    }
}

pub fn tree_entry_dto(view: TreeEntryView) -> TreeEntryDto {
    TreeEntryDto {
        path: view.path,
        mode: view.mode,
        oid: hex(view.oid),
        kind: kind(view.kind),
        size: view.size,
    }
}

pub fn blob_metadata_dto(view: BlobMetadataView) -> BlobMetadataDto {
    BlobMetadataDto {
        oid: hex(view.oid),
        path: view.path,
        mode: view.mode,
        size: view.size,
    }
}

pub fn blob_dto(view: BlobView) -> BlobDto {
    let size = view.data.len();
    BlobDto {
        oid: hex(view.oid),
        path: view.path,
        mode: view.mode,
        content_utf8_lossy: String::from_utf8_lossy(&view.data).into_owned(),
        size,
    }
}

pub fn blob_content_dto(view: BlobContentView) -> BlobContentDto {
    match view {
        BlobContentView::Inline(blob) => BlobContentDto {
            delivery: "inline",
            inline: Some(blob_dto(blob)),
            signed_url: None,
        },
        BlobContentView::Redirect(download) => BlobContentDto {
            delivery: "signed_url",
            inline: None,
            signed_url: Some(signed_url_dto(download.signed_url)),
        },
    }
}

fn signed_url_dto(view: SignedContentUrl) -> SignedContentUrlDto {
    SignedContentUrlDto {
        method: view.method,
        url: view.url,
        headers: view.headers,
        expires_at: view.expires_at.to_string(),
    }
}

fn hex(oid: ObjectId) -> String {
    oid.to_hex()
}

fn kind(kind: ObjectKind) -> String {
    kind.to_string()
}
