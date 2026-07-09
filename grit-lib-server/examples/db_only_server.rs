#[cfg(feature = "sqlx-postgres")]
mod support;

#[cfg(feature = "sqlx-postgres")]
mod app {
    use std::net::SocketAddr;
    use std::path::PathBuf;
    use std::sync::Arc;

    use anyhow::Result;
    use axum::{
        extract::{Path, State},
        http::header,
        response::{IntoResponse, Response},
        routing::get,
        Json, Router,
    };
    use clap::Parser;
    use grit_lib::objects::HashAlgo;
    use grit_lib::repo::Repository;
    use grit_lib_server::{
        error::Error,
        ids::{RepositoryId, TenantId},
        import::import_repository,
        repository::ServerRepository,
        sqlx_postgres::PgServerStorage,
    };
    use sqlx::postgres::PgPoolOptions;

    use crate::support::{
        blob_dto, blob_metadata_dto, branch_dto, commit_dto, summary_dto, tag_dto, tree_dto,
        AppError,
    };

    #[derive(Parser)]
    struct Args {
        /// PostgreSQL connection URL.
        #[arg(long, env = "GRIT_LIB_SERVER_POSTGRES_URL")]
        database_url: String,

        /// Optional local Git repository to import before serving.
        #[arg(long)]
        source_repo: Option<PathBuf>,

        /// Tenant id for the hosted repository.
        #[arg(long, default_value = "local")]
        tenant: String,

        /// Repository id for the hosted repository.
        #[arg(long, default_value = "db-demo")]
        repository: String,

        /// Address to bind to.
        #[arg(long, default_value = "127.0.0.1:3000")]
        bind: SocketAddr,
    }

    #[derive(Clone)]
    struct AppState {
        repo: ServerRepository<PgServerStorage>,
    }

    pub async fn run() -> Result<()> {
        let args = Args::parse();
        let tenant = TenantId::new(args.tenant)?;
        let repository = RepositoryId::new(args.repository)?;
        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect(&args.database_url)
            .await?;
        let storage = Arc::new(PgServerStorage::new(pool));
        storage.migrate().await?;
        ensure_repository(&storage, &tenant, &repository).await?;
        let repo = ServerRepository::new(tenant, repository, HashAlgo::Sha1, storage);

        if let Some(path) = args.source_repo {
            let source = Repository::discover(Some(&path))?;
            let report = import_repository(&repo, &source).await?;
            println!(
                "imported {} refs, {} objects, {} tree entries",
                report.refs, report.objects, report.tree_entries
            );
        }

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
            .with_state(AppState { repo });

        let listener = tokio::net::TcpListener::bind(args.bind).await?;
        println!("listening on http://{}", listener.local_addr()?);
        axum::serve(listener, app).await?;
        Ok(())
    }

    async fn ensure_repository(
        storage: &PgServerStorage,
        tenant: &TenantId,
        repository: &RepositoryId,
    ) -> grit_lib_server::error::Result<()> {
        match storage
            .create_repository(tenant, repository, HashAlgo::Sha1)
            .await
        {
            Ok(_) | Err(Error::RepositoryAlreadyExists(_)) => Ok(()),
            Err(error) => Err(error),
        }
    }

    async fn summary(
        State(state): State<AppState>,
    ) -> Result<Json<crate::support::RepositorySummaryDto>, AppError> {
        Ok(Json(summary_dto(state.repo.summary().await?)))
    }

    async fn branches(
        State(state): State<AppState>,
    ) -> Result<Json<Vec<crate::support::BranchDto>>, AppError> {
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
    ) -> Result<Json<Vec<crate::support::TagDto>>, AppError> {
        Ok(Json(
            state.repo.tags().await?.into_iter().map(tag_dto).collect(),
        ))
    }

    async fn commit(
        State(state): State<AppState>,
        Path(rev): Path<String>,
    ) -> Result<Json<crate::support::CommitDto>, AppError> {
        Ok(Json(commit_dto(state.repo.commit(&rev).await?)))
    }

    async fn root_tree(
        State(state): State<AppState>,
        Path(rev): Path<String>,
    ) -> Result<Json<crate::support::TreeDto>, AppError> {
        Ok(Json(tree_dto(state.repo.tree_at(&rev, "").await?)))
    }

    async fn tree(
        State(state): State<AppState>,
        Path((rev, path)): Path<(String, String)>,
    ) -> Result<Json<crate::support::TreeDto>, AppError> {
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
    ) -> Result<Json<crate::support::BlobDto>, AppError> {
        Ok(Json(blob_dto(state.repo.blob_at(&rev, &path).await?)))
    }

    async fn blob_meta(
        State(state): State<AppState>,
        Path((rev, path)): Path<(String, String)>,
    ) -> Result<Json<crate::support::BlobMetadataDto>, AppError> {
        Ok(Json(blob_metadata_dto(
            state.repo.blob_metadata_at(&rev, &path).await?,
        )))
    }
}

#[cfg(feature = "sqlx-postgres")]
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    app::run().await
}

#[cfg(not(feature = "sqlx-postgres"))]
fn main() {
    eprintln!(
        "db_only_server requires the sqlx-postgres feature, for example: \
         cargo run -p grit-lib-server --example db_only_server --features sqlx-postgres -- \
         --database-url postgres://..."
    );
}
