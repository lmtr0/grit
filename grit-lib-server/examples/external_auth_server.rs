mod support;

use std::collections::{BTreeSet, HashMap};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Result;
use axum::{
    extract::{Path, State},
    http::{header, HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::get,
    Json, Router,
};
use clap::Parser;
use grit_lib::objects::HashAlgo;
use grit_lib::repo::Repository;
use grit_lib_server::{
    ids::{RepositoryId, TenantId},
    import::import_repository,
    memory::MemoryBackend,
    policy::PolicyActor,
    repository::ServerRepository,
};

use support::{
    blob_metadata_dto, branch_dto, commit_dto, summary_dto, tag_dto, tree_dto, AppError,
};

#[derive(Parser)]
struct Args {
    /// Local Git repository to import into the in-memory backend.
    #[arg(long)]
    repo: PathBuf,

    /// Bearer token with read access.
    #[arg(long, env = "GRIT_EXAMPLE_READ_TOKEN", default_value = "read-token")]
    read_token: String,

    /// Bearer token with read and write access.
    #[arg(long, env = "GRIT_EXAMPLE_WRITE_TOKEN", default_value = "write-token")]
    write_token: String,

    /// Address to bind to.
    #[arg(long, default_value = "127.0.0.1:3000")]
    bind: SocketAddr,
}

#[derive(Clone)]
struct AppState {
    repo: ServerRepository<MemoryBackend>,
    auth: Arc<dyn ExternalAuthSource>,
}

#[derive(Clone)]
struct ExternalIdentity {
    id: String,
    display_name: String,
    permissions: BTreeSet<Permission>,
}

impl ExternalIdentity {
    fn policy_actor(&self) -> PolicyActor {
        PolicyActor::new(
            self.id.clone(),
            format!("{} <{}@example.invalid>", self.display_name, self.id),
        )
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Permission {
    Read,
    Write,
}

trait ExternalAuthSource: Send + Sync {
    fn authenticate(&self, bearer_token: &str) -> Option<ExternalIdentity>;
}

#[derive(Clone)]
struct StaticTokenDirectory {
    tokens: HashMap<String, ExternalIdentity>,
}

impl StaticTokenDirectory {
    fn new(read_token: String, write_token: String) -> Self {
        let mut tokens = HashMap::new();
        tokens.insert(
            read_token,
            ExternalIdentity {
                id: "reader".to_owned(),
                display_name: "Example Reader".to_owned(),
                permissions: BTreeSet::from([Permission::Read]),
            },
        );
        tokens.insert(
            write_token,
            ExternalIdentity {
                id: "writer".to_owned(),
                display_name: "Example Writer".to_owned(),
                permissions: BTreeSet::from([Permission::Read, Permission::Write]),
            },
        );
        Self { tokens }
    }
}

impl ExternalAuthSource for StaticTokenDirectory {
    fn authenticate(&self, bearer_token: &str) -> Option<ExternalIdentity> {
        self.tokens.get(bearer_token).cloned()
    }
}

enum ExampleError {
    Server(grit_lib_server::error::Error),
    Auth(StatusCode, &'static str),
}

impl From<grit_lib_server::error::Error> for ExampleError {
    fn from(error: grit_lib_server::error::Error) -> Self {
        Self::Server(error)
    }
}

impl IntoResponse for ExampleError {
    fn into_response(self) -> Response {
        match self {
            Self::Server(error) => AppError(error).into_response(),
            Self::Auth(status, message) => (status, message).into_response(),
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let source = Repository::discover(Some(&args.repo))?;
    let storage = Arc::new(MemoryBackend::new());
    let repo = ServerRepository::new(
        TenantId::new("local")?,
        RepositoryId::new("external-auth-demo")?,
        HashAlgo::Sha1,
        storage,
    );
    let report = import_repository(&repo, &source).await?;
    println!(
        "imported {} refs, {} objects, {} tree entries",
        report.refs, report.objects, report.tree_entries
    );

    let auth = Arc::new(StaticTokenDirectory::new(args.read_token, args.write_token));
    let app = Router::new()
        .route("/summary", get(summary))
        .route("/branches", get(branches))
        .route("/tags", get(tags))
        .route("/commits/{rev}", get(commit))
        .route("/tree/{rev}", get(root_tree))
        .route("/tree/{rev}/{*path}", get(tree))
        .route("/blob/{rev}/{*path}", get(blob))
        .route("/blob-meta/{rev}/{*path}", get(blob_meta))
        .route("/whoami", get(whoami))
        .with_state(AppState { repo, auth });

    let listener = tokio::net::TcpListener::bind(args.bind).await?;
    println!("listening on http://{}", listener.local_addr()?);
    println!(
        "try: curl -H 'Authorization: Bearer read-token' http://{}/summary",
        args.bind
    );
    axum::serve(listener, app).await?;
    Ok(())
}

async fn whoami(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<IdentityDto>, ExampleError> {
    let identity = require_permission(&state, &headers, Permission::Read)?;
    Ok(Json(identity_dto(&identity)))
}

async fn summary(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<support::RepositorySummaryDto>, ExampleError> {
    let identity = require_permission(&state, &headers, Permission::Read)?;
    let _actor = identity.policy_actor();
    Ok(Json(summary_dto(state.repo.summary().await?)))
}

async fn branches(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<Vec<support::BranchDto>>, ExampleError> {
    require_permission(&state, &headers, Permission::Read)?;
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

async fn tags(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<Vec<support::TagDto>>, ExampleError> {
    require_permission(&state, &headers, Permission::Read)?;
    Ok(Json(
        state.repo.tags().await?.into_iter().map(tag_dto).collect(),
    ))
}

async fn commit(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(rev): Path<String>,
) -> Result<Json<support::CommitDto>, ExampleError> {
    require_permission(&state, &headers, Permission::Read)?;
    Ok(Json(commit_dto(state.repo.commit(&rev).await?)))
}

async fn root_tree(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(rev): Path<String>,
) -> Result<Json<support::TreeDto>, ExampleError> {
    require_permission(&state, &headers, Permission::Read)?;
    Ok(Json(tree_dto(state.repo.tree_at(&rev, "").await?)))
}

async fn tree(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path((rev, path)): Path<(String, String)>,
) -> Result<Json<support::TreeDto>, ExampleError> {
    require_permission(&state, &headers, Permission::Read)?;
    Ok(Json(tree_dto(state.repo.tree_at(&rev, &path).await?)))
}

async fn blob(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path((rev, path)): Path<(String, String)>,
) -> Result<Response, ExampleError> {
    require_permission(&state, &headers, Permission::Read)?;
    let blob = state.repo.blob_at(&rev, &path).await?;
    Ok((
        [(header::CONTENT_TYPE, "application/octet-stream")],
        blob.data,
    )
        .into_response())
}

async fn blob_meta(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path((rev, path)): Path<(String, String)>,
) -> Result<Json<support::BlobMetadataDto>, ExampleError> {
    require_permission(&state, &headers, Permission::Read)?;
    Ok(Json(blob_metadata_dto(
        state.repo.blob_metadata_at(&rev, &path).await?,
    )))
}

fn require_permission(
    state: &AppState,
    headers: &HeaderMap,
    permission: Permission,
) -> Result<ExternalIdentity, ExampleError> {
    let token = bearer_token(headers).ok_or(ExampleError::Auth(
        StatusCode::UNAUTHORIZED,
        "missing bearer token",
    ))?;
    let identity = state.auth.authenticate(token).ok_or(ExampleError::Auth(
        StatusCode::UNAUTHORIZED,
        "invalid bearer token",
    ))?;
    if identity.permissions.contains(&permission) {
        Ok(identity)
    } else {
        Err(ExampleError::Auth(
            StatusCode::FORBIDDEN,
            "permission denied",
        ))
    }
}

fn bearer_token(headers: &HeaderMap) -> Option<&str> {
    headers
        .get(header::AUTHORIZATION)?
        .to_str()
        .ok()?
        .strip_prefix("Bearer ")
}

#[derive(serde::Serialize)]
struct IdentityDto {
    id: String,
    display_name: String,
    permissions: Vec<&'static str>,
}

fn identity_dto(identity: &ExternalIdentity) -> IdentityDto {
    IdentityDto {
        id: identity.id.clone(),
        display_name: identity.display_name.clone(),
        permissions: identity
            .permissions
            .iter()
            .map(|permission| match permission {
                Permission::Read => "read",
                Permission::Write => "write",
            })
            .collect(),
    }
}
