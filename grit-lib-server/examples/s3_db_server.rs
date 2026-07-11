#[cfg(feature = "s3")]
mod support;

#[cfg(feature = "s3")]
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
        externalized::{ExternalStorageOptions, PgExternalizedStorage},
        ids::{RepositoryId, TenantId},
        import::import_repository,
        repository::ServerRepository,
        s3_byte_store::S3ByteStore,
        sqlx_postgres::PgServerStorage,
        views::{BlobContentDelivery, BlobDownloadOptions},
    };
    use sqlx::postgres::PgPoolOptions;
    use time::OffsetDateTime;

    use crate::support::{blob_content_dto, blob_metadata_dto, summary_dto, tree_dto, AppError};

    type HybridStorage = PgExternalizedStorage<S3ByteStore>;

    #[derive(Parser)]
    struct Args {
        /// PostgreSQL connection URL for metadata rows.
        #[arg(long, env = "GRIT_LIB_SERVER_POSTGRES_URL")]
        database_url: String,

        /// S3 bucket for object and pack bytes.
        #[arg(long, env = "GRIT_LIB_SERVER_S3_BUCKET")]
        bucket: String,

        /// Optional local Git repository to import before serving.
        #[arg(long)]
        source_repo: Option<PathBuf>,

        /// Tenant id for the hosted repository.
        #[arg(long, default_value = "local")]
        tenant: String,

        /// Repository id for the hosted repository.
        #[arg(long, default_value = "s3-db-demo")]
        repository: String,

        /// Prefix for external object keys.
        #[arg(long, default_value = "grit-lib-server-example")]
        key_prefix: String,

        /// Address to bind to.
        #[arg(long, default_value = "127.0.0.1:3000")]
        bind: SocketAddr,
    }

    #[derive(Clone)]
    struct AppState {
        repo: ServerRepository<HybridStorage>,
        bytes: Arc<S3ByteStore>,
    }

    pub async fn run() -> Result<()> {
        let args = Args::parse();
        let tenant = TenantId::new(args.tenant)?;
        let repository = RepositoryId::new(args.repository)?;
        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect(&args.database_url)
            .await?;
        let sql = PgServerStorage::new(pool);
        sql.migrate().await?;
        ensure_repository(&sql, &tenant, &repository).await?;

        let bytes = Arc::new(S3ByteStore::from_env(args.bucket).await?);
        let storage = Arc::new(PgExternalizedStorage::new(
            sql,
            bytes.clone(),
            ExternalStorageOptions {
                key_prefix: args.key_prefix,
                ..ExternalStorageOptions::default()
            },
        ));
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
            .route("/tree/{rev}", get(root_tree))
            .route("/tree/{rev}/{*path}", get(tree))
            .route("/blob-meta/{rev}/{*path}", get(blob_meta))
            .route("/blob/{rev}/{*path}", get(blob))
            .route("/blob-url/{rev}/{*path}", get(blob_url))
            .with_state(AppState { repo, bytes });

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

    async fn blob_meta(
        State(state): State<AppState>,
        Path((rev, path)): Path<(String, String)>,
    ) -> Result<Json<crate::support::BlobMetadataDto>, AppError> {
        Ok(Json(blob_metadata_dto(
            state.repo.blob_metadata_at(&rev, &path).await?,
        )))
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

    async fn blob_url(
        State(state): State<AppState>,
        Path((rev, path)): Path<(String, String)>,
    ) -> Result<Json<crate::support::BlobContentDto>, AppError> {
        let mut options = BlobDownloadOptions::new(OffsetDateTime::now_utc());
        options.delivery = BlobContentDelivery::SignedUrl;
        options.key_prefix = "downloads".to_owned();
        let view = state
            .repo
            .blob_content_at(
                &rev,
                &path,
                state.bytes.as_ref(),
                state.bytes.as_ref(),
                options,
            )
            .await?;
        Ok(Json(blob_content_dto(view)))
    }
}

#[cfg(feature = "s3")]
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    app::run().await
}

#[cfg(not(feature = "s3"))]
fn main() {
    eprintln!(
        "s3_db_server requires the s3 feature, for example: \
         cargo run -p grit-lib-server --example s3_db_server --features s3 -- \
         --database-url postgres://... --bucket grit-server"
    );
}
