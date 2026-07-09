mod support;

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Result;
use axum::{
    body::Bytes,
    extract::{Path, State},
    http::{header, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use clap::Parser;
use grit_lib::objects::HashAlgo;
use grit_lib::repo::Repository;
use grit_lib_server::{
    ids::{RepositoryId, TenantId},
    import::{import_repository_with_options, ImportOptions, ImportProgressEvent},
    memory::MemoryBackend,
    policy::{AllowAllPushPolicy, NoopAuditSink, PolicyActor},
    protocol::{
        receive_pack::{ReceivePackRequest, ReceivePackService},
        upload_pack::{UploadPackRequest, UploadPackService},
    },
    repository::ServerRepository,
};
use time::OffsetDateTime;

use support::{
    blob_dto, blob_metadata_dto, branch_dto, commit_dto, summary_dto, tag_dto, tree_dto, AppError,
};

#[derive(Parser)]
struct Args {
    /// Local Git repository to import into the in-memory backend.
    #[arg(long)]
    repo: PathBuf,

    /// Address to bind to.
    #[arg(long, default_value = "127.0.0.1:3000")]
    bind: SocketAddr,
}

#[derive(Clone)]
struct AppState {
    repo: ServerRepository<MemoryBackend>,
    storage: Arc<MemoryBackend>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let source = Repository::discover(Some(&args.repo))?;
    let storage = Arc::new(MemoryBackend::new());
    let repo = ServerRepository::new(
        TenantId::new("local")?,
        RepositoryId::new("memory-demo")?,
        HashAlgo::Sha1,
        storage.clone(),
    );
    println!("importing repository");

    let mut last_tree_entry_report = 0;
    let report =
        import_repository_with_options(&repo, &source, ImportOptions::default(), |event| {
            match event {
                ImportProgressEvent::Checkpoint(checkpoint) => {
                    println!(
                        "imported {} objects, {} refs, {} tree entries",
                        checkpoint.objects, checkpoint.refs, checkpoint.tree_entries
                    );
                }
                ImportProgressEvent::TreeEntries { entries, .. } => {
                    last_tree_entry_report += entries;
                    if last_tree_entry_report >= 25_000 {
                        println!("indexed another {last_tree_entry_report} tree entries");
                        last_tree_entry_report = 0;
                    }
                }
                ImportProgressEvent::Completed(_) => {
                    println!("finalizing import");
                }
                _ => {}
            }
            Ok(())
        })
        .await?;
    println!(
        "imported {} refs, {} objects, {} tree entries",
        report.refs, report.objects, report.tree_entries
    );

    let app = Router::new()
        .route("/summary", get(summary))
        .route("/branches", get(branches))
        .route("/tags", get(tags))
        .route("/commits/{rev}", get(commit))
        .route("/tree/{rev}", get(root_tree))
        .route("/tree/{rev}/{*path}", get(tree))
        .route("/blob/{rev}/{*path}", get(blob))
        .route("/blob-text/{rev}/{*path}", get(blob_text))
        .route("/blob-meta/{rev}/{*path}", get(blob_meta))
        .route("/git-upload-pack/info-refs", get(upload_pack_refs))
        .route("/git-upload-pack", post(upload_pack))
        .route("/git-receive-pack", post(receive_pack))
        .with_state(AppState { repo, storage });

    let listener = tokio::net::TcpListener::bind(args.bind).await?;
    println!("listening on http://{}", listener.local_addr()?);
    axum::serve(listener, app).await?;
    Ok(())
}

async fn summary(
    State(state): State<AppState>,
) -> Result<Json<support::RepositorySummaryDto>, AppError> {
    Ok(Json(summary_dto(state.repo.summary().await?)))
}

async fn branches(
    State(state): State<AppState>,
) -> Result<Json<Vec<support::BranchDto>>, AppError> {
    Ok(Json(
        state
            .repo
            .branches()
            .await?
            .into_iter()
            .map(branch_dto)
            .collect(),
    ))
}

async fn tags(State(state): State<AppState>) -> Result<Json<Vec<support::TagDto>>, AppError> {
    Ok(Json(
        state.repo.tags().await?.into_iter().map(tag_dto).collect(),
    ))
}

async fn commit(
    State(state): State<AppState>,
    Path(rev): Path<String>,
) -> Result<Json<support::CommitDto>, AppError> {
    Ok(Json(commit_dto(state.repo.commit(&rev).await?)))
}

async fn root_tree(
    State(state): State<AppState>,
    Path(rev): Path<String>,
) -> Result<Json<support::TreeDto>, AppError> {
    Ok(Json(tree_dto(state.repo.tree_at(&rev, "").await?)))
}

async fn tree(
    State(state): State<AppState>,
    Path((rev, path)): Path<(String, String)>,
) -> Result<Json<support::TreeDto>, AppError> {
    Ok(Json(tree_dto(state.repo.tree_at(&rev, &path).await?)))
}

async fn blob(
    State(state): State<AppState>,
    Path((rev, path)): Path<(String, String)>,
) -> Result<Response, AppError> {
    let blob = state.repo.blob_at(&rev, &path).await?;
    Ok((
        [(header::CONTENT_TYPE, "application/octet-stream")],
        blob.data,
    )
        .into_response())
}

async fn blob_text(
    State(state): State<AppState>,
    Path((rev, path)): Path<(String, String)>,
) -> Result<Json<support::BlobDto>, AppError> {
    Ok(Json(blob_dto(state.repo.blob_at(&rev, &path).await?)))
}

async fn blob_meta(
    State(state): State<AppState>,
    Path((rev, path)): Path<(String, String)>,
) -> Result<Json<support::BlobMetadataDto>, AppError> {
    Ok(Json(blob_metadata_dto(
        state.repo.blob_metadata_at(&rev, &path).await?,
    )))
}

async fn upload_pack_refs(State(state): State<AppState>) -> Result<Response, AppError> {
    let service = UploadPackService::new(state.repo.clone());
    let advertisement = service.advertise_refs().await?;
    let body = advertisement.to_pkt_lines(state.repo.hash_algo())?;
    Ok((
        StatusCode::OK,
        [(
            header::CONTENT_TYPE,
            "application/x-git-upload-pack-advertisement",
        )],
        body,
    )
        .into_response())
}

async fn upload_pack(State(state): State<AppState>, body: Bytes) -> Result<Response, AppError> {
    let request = UploadPackRequest::parse_v0(&body)?;
    let service = UploadPackService::new(state.repo.clone());
    let plan = service.negotiate_fetch(request).await?;
    let response = service.build_fetch_pack(plan).await?;
    Ok((
        StatusCode::OK,
        [(header::CONTENT_TYPE, "application/x-git-upload-pack-result")],
        response.wire_response,
    )
        .into_response())
}

async fn receive_pack(State(state): State<AppState>, body: Bytes) -> Result<Response, AppError> {
    let request = ReceivePackRequest::parse_v0(&body)?;
    let service = ReceivePackService::new(state.repo.clone());
    let actor = PolicyActor::new("local-example", "Local Example <local@example.com>");
    let report = service
        .receive_push(
            request,
            actor,
            OffsetDateTime::UNIX_EPOCH,
            &AllowAllPushPolicy,
            state.storage.as_ref(),
            &NoopAuditSink,
        )
        .await?;
    Ok((
        StatusCode::OK,
        [(
            header::CONTENT_TYPE,
            "application/x-git-receive-pack-result",
        )],
        report.to_pkt_lines()?,
    )
        .into_response())
}
