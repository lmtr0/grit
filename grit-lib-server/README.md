# grit-lib-server

`grit-lib-server` is the server-side storage and hosting-view crate for Grit
repositories. It lets an embedding application import repository data, store it
behind a backend, and ask read-only questions that are useful for a simple Git
browser or hosting API.

This crate provides:

- storage backends like `MemoryBackend` for tests and local prototypes
- stable hosted repository identity with `TenantId` and `RepositoryId`
- import from a filesystem-backed `grit_lib::repo::Repository`
- read-only repository views through `ServerRepository`
- lower-level storage, protocol, cache, and maintenance APIs for larger hosting
  integrations

This is different from `grit-http-server`: that crate serves Git smart HTTP for
clone, fetch, and push. This guide shows a tiny browser-style JSON API that can
show repository information, browse trees, and return file contents.

## Key APIs

The example below uses:

- `grit_lib_server::memory::MemoryBackend`
- `grit_lib_server::repository::ServerRepository`
- `grit_lib_server::ids::{TenantId, RepositoryId}`
- `grit_lib_server::import::import_repository`
- `grit_lib_server::views::{RepositorySummary, BranchView, TagView, CommitSummary, TreeView, TreeEntryView, BlobView, BlobMetadataView}`
- `grit_lib::repo::Repository`
- `grit_lib::objects::HashAlgo`

### Memory import pack modes

`ImportOptions::default()` uses `NativePackImportMode::Reachable`. With
`MemoryBackend`, a self-contained local source pack is retained as shared immutable bytes only
when every object in that pack belongs to the imported closure or the manifest from a previously
completed import. Mixed reachable/unreachable, promisor, thin, corrupt, or hash-incompatible packs
automatically use the ordinary loose-object traversal. Backends that do not advertise native pack
support use the portable traversal, but still share the bounded source-read pipeline described
below.

Retention verifies one immutable version-2 index snapshot, binds its embedded pack checksum to the
validated pack trailer, and checks every index CRC against the corresponding packed byte span.
Before any eligible pack is published, every object—including every blob—is fully decoded and
canonical-hash verified. Resolved payloads are discarded after validation rather than retained as
loose copies; the delta-base cache remains bounded, with peak transient memory additionally
depending on the largest active object/delta chain being verified. FullMirror applies this
validation to unreachable packed objects as well. Pack-native import therefore removes retained
uncompressed object storage, duplicate long-lived allocations, and later whole-pack copies.
Ref/config/pack discovery, source reads, commit/tree/tag parsing, tree-entry preparation, and
whole-pack validation run on blocking workers outside the async executor. The caller only merges
prepared traversal/index records and computes commit generations. `ImportExecutionOptions`
defaults to a fixed four-worker pool and 128 MiB of conservative decode-and-preparation working set
in flight.
The limit covers completed prepared records retained until wave merge as well as decoding. Delta
estimates include recursively retained inflated instructions,
the active base, and the result. One unknown or oversized job may exceed the configured byte limit,
but it runs alone; the process-wide 96 MiB delta cache and 32 MiB write batch are additional bounds.
Tree preparation charges worst-case geometric capacity (including minimum nonzero capacity) for
both simultaneously live parsed/prepared entry vectors, plus copied names from the shortest
structurally parseable entry; commit/tag preparation charges parent vectors, parsed strings and
messages, raw forms, and allocation slack. Overflowed estimates become unknown and run alone.
Worker results are handled in scheduling order within a wave, so completion timing cannot change
semantic state or progress counts. Changing execution limits may change traversal, progress-event,
and batch order. Existing `import_repository_with_options` callers retain these defaults; use
`import_repository_with_execution_options` to pass custom execution limits. The portable pipeline
is used by every backend; direct pack retention remains specific to backends that advertise it.
Normal completion joins the pool before publication. Cancellation skips queued jobs and transfers
active-worker joins to a reaper created before the pool workers, so dropping the import future
never waits on stuck source I/O. The reaper joins workers after active operations return and then
self-terminates; cancellation does not synchronously join the reaper itself.
These checks protect against corrupt or mismatched source files; the source checksums are integrity
checks, not signatures for accepting an adversarial repository as trusted input.

Set `native_pack_import` to `Disabled` for the portable traversal unconditionally, or to
`FullMirror` to retain all compatible local packs and copy local loose objects, including
unreachable objects. `ImportReport::full_mirror_completed` distinguishes a complete local mirror
from a run that safely fell back for an incompatible pack. Alternates are copied only when their
objects are reachable; they are not part of the local full mirror.

## Minimal Dependencies

For a standalone test project near this workspace, use:

```toml
[dependencies]
anyhow = "1"
axum = "0.8"
clap = { version = "4", features = ["derive"] }
grit-lib = { path = "../grit-lib" }
grit-lib-server = { path = "../grit-lib-server" }
serde = { version = "1", features = ["derive"] }
tokio = { version = "1", features = ["full"] }
```

Outside this workspace, replace the path dependencies with the published crate
versions once they are available.

## Tiny Repository Browser

Create `src/main.rs`:

```rust
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Result;
use axum::{
    extract::{Path, State},
    http::{header, StatusCode},
    response::{IntoResponse, Response},
    routing::get,
    Json, Router,
};
use clap::Parser;
use grit_lib::objects::{HashAlgo, ObjectId, ObjectKind};
use grit_lib::repo::Repository;
use grit_lib_server::error::Error as ServerError;
use grit_lib_server::ids::{RepositoryId, TenantId};
use grit_lib_server::import::import_repository;
use grit_lib_server::memory::MemoryBackend;
use grit_lib_server::repository::ServerRepository;
use grit_lib_server::views::{
    BlobMetadataView, BranchView, CommitSummary, RepositorySummary, TagView, TreeEntryView,
    TreeView,
};
use serde::Serialize;

#[derive(Parser)]
struct Args {
    /// Local Git repository to import.
    #[arg(long)]
    repo: PathBuf,

    /// Address to bind to.
    #[arg(long, default_value = "127.0.0.1:3000")]
    bind: SocketAddr,
}

#[derive(Clone)]
struct AppState {
    repo: ServerRepository<MemoryBackend>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let source = Repository::discover(Some(&args.repo))?;
    let storage = Arc::new(MemoryBackend::new());
    let server_repo = ServerRepository::new(
        TenantId::new("local")?,
        RepositoryId::new("demo")?,
        HashAlgo::Sha1,
        storage,
    );

    let report = import_repository(&server_repo, &source).await?;
    println!(
        "imported {} refs, {} objects, and {} tree entries",
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
        .route("/blob-meta/{rev}/{*path}", get(blob_meta))
        .with_state(AppState { repo: server_repo });

    let listener = tokio::net::TcpListener::bind(args.bind).await?;
    println!("listening on http://{}", listener.local_addr()?);
    axum::serve(listener, app).await?;
    Ok(())
}

async fn summary(State(state): State<AppState>) -> Result<Json<RepositorySummaryDto>, AppError> {
    Ok(Json(summary_dto(state.repo.summary().await?)))
}

async fn branches(State(state): State<AppState>) -> Result<Json<Vec<BranchDto>>, AppError> {
    let branches = state
        .repo
        .branches()
        .await?
        .into_iter()
        .map(branch_dto)
        .collect();
    Ok(Json(branches))
}

async fn tags(State(state): State<AppState>) -> Result<Json<Vec<TagDto>>, AppError> {
    let tags = state.repo.tags().await?.into_iter().map(tag_dto).collect();
    Ok(Json(tags))
}

async fn commit(
    State(state): State<AppState>,
    Path(rev): Path<String>,
) -> Result<Json<CommitDto>, AppError> {
    Ok(Json(commit_dto(state.repo.commit(&rev).await?)))
}

async fn root_tree(
    State(state): State<AppState>,
    Path(rev): Path<String>,
) -> Result<Json<TreeDto>, AppError> {
    Ok(Json(tree_dto(state.repo.tree_at(&rev, "").await?)))
}

async fn tree(
    State(state): State<AppState>,
    Path((rev, path)): Path<(String, String)>,
) -> Result<Json<TreeDto>, AppError> {
    Ok(Json(tree_dto(state.repo.tree_at(&rev, &path).await?)))
}

async fn blob(
    State(state): State<AppState>,
    Path((rev, path)): Path<(String, String)>,
) -> Result<Response, AppError> {
    let blob = state.repo.blob_at(&rev, &path).await?;
    Ok(([(header::CONTENT_TYPE, "application/octet-stream")], blob.data).into_response())
}

async fn blob_meta(
    State(state): State<AppState>,
    Path((rev, path)): Path<(String, String)>,
) -> Result<Json<BlobMetadataDto>, AppError> {
    Ok(Json(blob_metadata_dto(
        state.repo.blob_metadata_at(&rev, &path).await?,
    )))
}

struct AppError(ServerError);

impl From<ServerError> for AppError {
    fn from(error: ServerError) -> Self {
        Self(error)
    }
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let status = match &self.0 {
            ServerError::PathNotFound(_)
            | ServerError::RefNotFound(_)
            | ServerError::ObjectNotFound(_) => StatusCode::NOT_FOUND,
            ServerError::UnexpectedObjectKind { .. }
            | ServerError::InvalidRepositoryId(_)
            | ServerError::InvalidTenantId(_)
            | ServerError::Protocol(_) => StatusCode::BAD_REQUEST,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        };
        (status, self.0.to_string()).into_response()
    }
}

#[derive(Serialize)]
struct RepositorySummaryDto {
    default_branch: Option<BranchDto>,
    refs_count: usize,
    object_count: usize,
    latest_commit: Option<CommitDto>,
}

#[derive(Serialize)]
struct BranchDto {
    name: String,
    refname: String,
    target: String,
    commit: Option<CommitDto>,
}

#[derive(Serialize)]
struct TagDto {
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
struct CommitDto {
    oid: String,
    tree: String,
    parents: Vec<String>,
    author: String,
    committer: String,
    subject: String,
    message: String,
}

#[derive(Serialize)]
struct TreeDto {
    root: String,
    path: String,
    oid: String,
    entries: Vec<TreeEntryDto>,
}

#[derive(Serialize)]
struct TreeEntryDto {
    path: String,
    mode: u32,
    oid: String,
    kind: String,
    size: Option<u64>,
}

#[derive(Serialize)]
struct BlobMetadataDto {
    oid: String,
    path: String,
    mode: u32,
    size: Option<u64>,
}

fn summary_dto(view: RepositorySummary) -> RepositorySummaryDto {
    RepositorySummaryDto {
        default_branch: view.default_branch.map(branch_dto),
        refs_count: view.refs_count,
        object_count: view.object_count,
        latest_commit: view.latest_commit.map(commit_dto),
    }
}

fn branch_dto(view: BranchView) -> BranchDto {
    BranchDto {
        name: view.name,
        refname: view.refname,
        target: hex(view.target),
        commit: view.commit.map(commit_dto),
    }
}

fn tag_dto(view: TagView) -> TagDto {
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

fn commit_dto(view: CommitSummary) -> CommitDto {
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

fn tree_dto(view: TreeView) -> TreeDto {
    TreeDto {
        root: hex(view.root),
        path: view.path,
        oid: hex(view.oid),
        entries: view.entries.into_iter().map(tree_entry_dto).collect(),
    }
}

fn tree_entry_dto(view: TreeEntryView) -> TreeEntryDto {
    TreeEntryDto {
        path: view.path,
        mode: view.mode,
        oid: hex(view.oid),
        kind: kind(view.kind),
        size: view.size,
    }
}

fn blob_metadata_dto(view: BlobMetadataView) -> BlobMetadataDto {
    BlobMetadataDto {
        oid: hex(view.oid),
        path: view.path,
        mode: view.mode,
        size: view.size,
    }
}

fn hex(oid: ObjectId) -> String {
    oid.to_hex()
}

fn kind(kind: ObjectKind) -> String {
    kind.to_string()
}
```

The current `grit-lib-server` view structs are plain Rust models, not JSON API
types. The example defines small DTO structs and converts object IDs and object
kinds into strings before returning `Json(...)`.

## Routes

| Route | Handler | `grit-lib-server` API |
|---|---|---|
| `GET /summary` | repository summary | `repo.summary().await` |
| `GET /branches` | branch list | `repo.branches().await` |
| `GET /tags` | tag list | `repo.tags().await` |
| `GET /commits/:rev` | commit metadata | `repo.commit(&rev).await` |
| `GET /tree/:rev` | root tree | `repo.tree_at(&rev, "").await` |
| `GET /tree/:rev/*path` | nested tree | `repo.tree_at(&rev, &path).await` |
| `GET /blob/:rev/*path` | raw file body | `repo.blob_at(&rev, &path).await` |
| `GET /blob-meta/:rev/*path` | file metadata only | `repo.blob_metadata_at(&rev, &path).await` |

In the Axum `0.8` code above, those routes are written with `{rev}` captures
and `{*path}` wildcards. The table uses the shorter route notation for
readability.

`GET /blob/:rev/*path` returns `200 OK` with `application/octet-stream` and the
raw bytes from `BlobView.data`. For text files, you can open the URL directly in
a browser.

## Run It

```sh
cargo run -- --repo /path/to/repo --bind 127.0.0.1:3000
```

Try the API:

```sh
curl http://127.0.0.1:3000/summary
curl http://127.0.0.1:3000/branches
curl http://127.0.0.1:3000/tree/HEAD
curl http://127.0.0.1:3000/tree/HEAD/src
curl http://127.0.0.1:3000/blob/HEAD/README.md
curl http://127.0.0.1:3000/blob-meta/HEAD/README.md
```

## Notes And Limitations

- The example imports at startup into `MemoryBackend`; restarting loses
  server-side state.
- The API is read-only.
- This server is meant for testing and local inspection.
- It does not implement auth, pagination, caching, large-blob streaming, range
  requests, or production persistence.
- `blob_at` loads file contents into memory.
- Use `blob_metadata_at` before downloading when you only need file size or
  object metadata.
- For production-like storage, look at the optional Postgres and external
  storage features, plus the storage traits in `grit-lib-server`.

## Runnable Examples

The `examples/` directory contains small servers that exercise common embedding
patterns:

```sh
cargo run -p grit-lib-server --example in_memory_browser -- \
  --repo /path/to/repo --bind 127.0.0.1:3000
```

`in_memory_browser` imports a local repository into `MemoryBackend`, serves
summary/tree/blob JSON routes, returns raw blobs, and exposes minimal typed
`git-upload-pack` and `git-receive-pack` endpoints backed by
`UploadPackService` and `ReceivePackService`.

```sh
cargo run -p grit-lib-server --example db_only_server --features sqlx-postgres -- \
  --database-url postgres://localhost/grit \
  --source-repo /path/to/repo \
  --tenant local \
  --repository db-demo
```

`db_only_server` stores repository metadata and object bytes in PostgreSQL using
`PgServerStorage`. Omit `--source-repo` to serve a repository that is already
loaded in the database.

```sh
cargo run -p grit-lib-server --example s3_db_server --features s3 -- \
  --database-url postgres://localhost/grit \
  --bucket grit-server \
  --source-repo /path/to/repo
```

`s3_db_server` stores metadata in PostgreSQL and immutable object/pack bytes in
S3 through `PgExternalizedStorage<S3ByteStore>`. It also includes a
`/blob-url/:rev/*path` route that returns a signed URL instead of always sending
blob bytes through the application server.

```sh
cargo run -p grit-lib-server --example external_auth_server -- \
  --repo /path/to/repo \
  --read-token read-token \
  --write-token write-token
```

`external_auth_server` shows where an embedding service can connect an external
identity source. It protects read routes with bearer tokens and maps accepted
identities into `PolicyActor` values for write-policy integration.
