//! SQLx PostgreSQL/YugaByteDB backend.
//!
//! The schema is intentionally append-friendly and transaction-oriented: objects are content
//! addressed and immutable, while refs are the primary compare-and-swap mutation point.

use async_trait::async_trait;
use grit_lib::objects::{HashAlgo, ObjectId, ObjectKind};
use sqlx::{PgPool, Postgres, QueryBuilder, Row, Transaction};
use std::collections::{HashMap, HashSet};
use time::OffsetDateTime;

use crate::error::{Error, Result};
use crate::ids::{RepositoryId, TenantId};
use crate::storage::{
    BrowseIndex, CommitGraphStore, ConfigStore, ImportPublication, ImportPublicationResult,
    ImportSession, ImportStateStore, ImportedPack, IndexedCommit, IndexedTreeEntry, ObjectStore,
    PackMetadata, PackObjectIndex, PackStore, RefStore, ReflogEntry, ReflogStore, StoredObject,
    StoredPack, StoredRef,
};
use crate::tree_block::{
    decode_tree_block, encode_tree_block, tree_block_prefix_range, TreeBlockEntry, TreeBlockError,
    TREE_BLOCK_FORMAT_VERSION,
};

const POSTGRES_BIND_LIMIT: usize = 65_535;
const TREE_BLOCK_BINDS_PER_ROW: usize = 5;
const MAX_TREE_BLOCKS_PER_BATCH: usize = POSTGRES_BIND_LIMIT / TREE_BLOCK_BINDS_PER_ROW;

/// SQL migration statements for the initial server storage schema.
pub const MIGRATIONS: &[&str] = &[
    "create table if not exists grit_repositories (
        tenant_id text not null,
        repository_id text not null,
        hash_algo text not null default 'sha1',
        archived_at timestamptz,
        deleted_at timestamptz,
        created_at timestamptz not null default now(),
        updated_at timestamptz not null default now(),
        primary key (tenant_id, repository_id)
    )",
    "alter table grit_repositories
        add column if not exists archived_at timestamptz",
    "alter table grit_repositories
        add column if not exists deleted_at timestamptz",
    "alter table grit_repositories
        add column if not exists updated_at timestamptz not null default now()",
    "alter table grit_repositories
        add column if not exists ref_generation bigint not null default 0",
    "do $$ begin
        if not exists (
            select 1 from pg_constraint
            where conrelid = 'grit_repositories'::regclass
              and conname = 'grit_repositories_ref_generation_nonnegative'
        ) then
            alter table grit_repositories
                add constraint grit_repositories_ref_generation_nonnegative
                check (ref_generation >= 0) not valid;
        end if;
     end $$",
    "alter table grit_repositories validate constraint grit_repositories_ref_generation_nonnegative",
    "create sequence if not exists grit_repository_pk_seq",
    "alter table grit_repositories add column if not exists repository_pk bigint",
    "alter sequence grit_repository_pk_seq
        owned by grit_repositories.repository_pk",
    "select setval(
        'grit_repository_pk_seq',
        greatest(
            (select last_value from grit_repository_pk_seq),
            coalesce((select max(repository_pk) from grit_repositories), 1)
        ),
        (select is_called from grit_repository_pk_seq)
            or exists (select 1 from grit_repositories where repository_pk is not null)
    )",
    "alter table grit_repositories alter column repository_pk
        set default nextval('grit_repository_pk_seq')",
    "update grit_repositories set repository_pk = nextval('grit_repository_pk_seq')
        where repository_pk is null",
    "alter table grit_repositories alter column repository_pk set not null",
    "create unique index if not exists grit_repositories_pk_idx
        on grit_repositories (repository_pk)",
    "create table if not exists grit_objects (
        tenant_id text not null,
        repository_id text not null,
        oid text not null,
        kind text not null,
        data bytea,
        storage_backend text not null default 'database',
        storage_key text,
        created_at timestamptz not null default now(),
        primary key (tenant_id, repository_id, oid),
        check ((storage_backend = 'database' and data is not null and storage_key is null)
            or (storage_backend <> 'database' and data is null and storage_key is not null))
    )",
    "alter table grit_objects
        alter column data drop not null",
    "alter table grit_objects
        add column if not exists storage_backend text not null default 'database'",
    "alter table grit_objects
        add column if not exists storage_key text",
    "create table if not exists grit_refs (
        tenant_id text not null,
        repository_id text not null,
        refname text not null,
        target_oid text,
        symbolic_target text,
        updated_at timestamptz not null default now(),
        primary key (tenant_id, repository_id, refname),
        check ((target_oid is null) <> (symbolic_target is null))
    )",
    "create table if not exists grit_reflog (
        tenant_id text not null,
        repository_id text not null,
        refname text not null,
        sequence bigserial primary key,
        old_oid text not null,
        new_oid text not null,
        actor text not null,
        message text not null,
        written_at timestamptz not null default now()
    )",
    "create index if not exists grit_reflog_repo_ref_idx
        on grit_reflog (tenant_id, repository_id, refname, sequence)",
    "create table if not exists grit_config (
        tenant_id text not null,
        repository_id text not null,
        key text not null,
        value text not null,
        primary key (tenant_id, repository_id, key)
    )",
    "create table if not exists grit_tree_entries (
        tenant_id text not null,
        repository_id text not null,
        tree_oid text not null,
        path text not null,
        mode integer not null,
        oid text not null,
        kind text not null,
        size bigint,
        primary key (tenant_id, repository_id, tree_oid, path)
    )",
    "create table if not exists grit_tree_blocks (
        repository_pk bigint not null,
        tree_oid bytea not null,
        format_version smallint not null,
        entry_count bigint not null,
        data bytea not null,
        primary key (repository_pk, tree_oid),
        constraint grit_tree_blocks_repository_fk foreign key (repository_pk)
            references grit_repositories (repository_pk) on delete cascade,
        check (octet_length(tree_oid) in (20, 32)),
        check (format_version between 1 and 255),
        check (entry_count between 0 and 4294967295)
    )",
    "create table if not exists grit_repository_summaries (
        repository_pk bigint not null,
        ref_generation bigint not null,
        hash_algo text not null,
        repository_created_at timestamptz not null,
        default_branch_refname text,
        default_branch_target bytea,
        refs_count bigint not null,
        branch_count bigint not null,
        tag_count bigint not null,
        latest_commit_oid bytea,
        latest_commit_tree_oid bytea,
        latest_commit_time bigint,
        latest_commit_generation integer,
        primary key (repository_pk, ref_generation),
        constraint grit_repository_summaries_repository_fk foreign key (repository_pk)
            references grit_repositories (repository_pk) on delete cascade,
        check (ref_generation >= 0),
        check (refs_count >= 0 and branch_count >= 0 and tag_count >= 0),
        check ((default_branch_refname is null) = (default_branch_target is null)),
        check ((latest_commit_oid is null) = (latest_commit_tree_oid is null)
            and (latest_commit_oid is null) = (latest_commit_time is null)
            and (latest_commit_oid is null) = (latest_commit_generation is null)),
        check (default_branch_target is null or octet_length(default_branch_target) in (20, 32)),
        check (latest_commit_oid is null or octet_length(latest_commit_oid) in (20, 32)),
        check (latest_commit_tree_oid is null or octet_length(latest_commit_tree_oid) in (20, 32))
    )",
    "create table if not exists grit_commits (
        tenant_id text not null,
        repository_id text not null,
        commit_oid text not null,
        tree_oid text not null,
        commit_time bigint not null,
        generation integer not null,
        primary key (tenant_id, repository_id, commit_oid)
    )",
    "create table if not exists grit_commit_parents (
        tenant_id text not null,
        repository_id text not null,
        commit_oid text not null,
        parent_oid text not null,
        parent_order integer not null,
        primary key (tenant_id, repository_id, commit_oid, parent_order)
    )",
    "create sequence if not exists grit_import_session_token_seq",
    "create table if not exists grit_import_state (
        tenant_id text not null,
        repository_id text not null,
        generation bigint not null,
        token bigint not null,
        complete boolean not null,
        published boolean not null,
        primary key (tenant_id, repository_id)
    )",
    "alter table grit_import_state
        add column if not exists published boolean not null default false",
    "alter table grit_import_state
        add column if not exists token bigint not null default 0",
    "create table if not exists grit_import_trusted_objects (
        tenant_id text not null,
        repository_id text not null,
        oid text not null,
        kind text not null,
        primary key (tenant_id, repository_id, oid)
    )",
    "create table if not exists grit_packs (
        tenant_id text not null,
        repository_id text not null,
        pack_checksum text not null,
        index_checksum text not null,
        data bytea,
        storage_backend text not null default 'database',
        storage_key text,
        object_count integer not null,
        size_bytes bigint not null,
        storage_order bigserial not null,
        primary key (tenant_id, repository_id, pack_checksum),
        check ((storage_backend = 'database' and data is not null and storage_key is null)
            or (storage_backend <> 'database' and data is null and storage_key is not null))
    )",
    "alter table grit_packs
        alter column data drop not null",
    "alter table grit_packs
        add column if not exists storage_backend text not null default 'database'",
    "alter table grit_packs
        add column if not exists storage_key text",
    "create table if not exists grit_external_orphan_candidates (
        tenant_id text not null,
        repository_id text not null,
        storage_backend text not null,
        storage_key text not null,
        size_bytes bigint not null,
        first_observed_at timestamptz,
        primary key (tenant_id, repository_id, storage_backend, storage_key),
        check (size_bytes >= 0)
    )",
    "create table if not exists grit_pack_objects (
        tenant_id text not null,
        repository_id text not null,
        pack_checksum text not null,
        oid text not null,
        kind text not null,
        offset bigint not null,
        size bigint not null,
        compressed_size bigint not null,
        primary key (tenant_id, repository_id, pack_checksum, offset)
    )",
    "create or replace function grit_assign_repository_pk()
     returns trigger language plpgsql as $$
     begin
       select repository_pk into new.repository_pk
       from grit_repositories
       where tenant_id = new.tenant_id and repository_id = new.repository_id;
       if new.repository_pk is null then
         raise exception 'repository %/% does not exist', new.tenant_id, new.repository_id
           using errcode = 'foreign_key_violation';
       end if;
       return new;
     end $$",
    "do $$
     declare child_table text;
     begin
       foreach child_table in array array[
         'grit_objects', 'grit_refs', 'grit_reflog', 'grit_config',
         'grit_tree_entries', 'grit_commits', 'grit_commit_parents',
         'grit_import_state', 'grit_import_trusted_objects', 'grit_packs',
         'grit_pack_objects', 'grit_external_orphan_candidates'
       ] loop
         execute format('alter table %I add column if not exists repository_pk bigint', child_table);
         if not exists (
           select 1 from pg_trigger
           where tgrelid = to_regclass(child_table)
             and tgname = 'grit_repository_pk_dual_write'
             and not tgisinternal
         ) then
           execute format(
             'create trigger grit_repository_pk_dual_write
              before insert or update of tenant_id, repository_id, repository_pk on %I
              for each row execute function grit_assign_repository_pk()',
             child_table
           );
         end if;
         execute format(
           'update %I child set repository_pk = repository.repository_pk
            from grit_repositories repository
            where child.repository_pk is null
              and child.tenant_id = repository.tenant_id
              and child.repository_id = repository.repository_id',
           child_table
         );
         execute format('alter table %I alter column repository_pk set not null', child_table);
         execute format(
           'create index if not exists %I on %I (repository_pk)',
           child_table || '_repository_pk_idx', child_table
         );
       end loop;
     end $$",
    "do $$
     declare column_mapping record;
     begin
       for column_mapping in
         select * from (values
           ('grit_objects', 'oid', 'oid_bytes',
            'grit_objects_pk_oid_bytes_idx', 'grit_objects_oid_bytes_width'),
           ('grit_refs', 'target_oid', 'target_oid_bytes',
            'grit_refs_pk_target_oid_bytes_idx', 'grit_refs_target_oid_bytes_width'),
           ('grit_reflog', 'old_oid', 'old_oid_bytes',
            'grit_reflog_pk_old_oid_bytes_idx', 'grit_reflog_old_oid_bytes_width'),
           ('grit_reflog', 'new_oid', 'new_oid_bytes',
            'grit_reflog_pk_new_oid_bytes_idx', 'grit_reflog_new_oid_bytes_width'),
           ('grit_tree_entries', 'tree_oid', 'tree_oid_bytes',
            'grit_trees_pk_tree_oid_bytes_idx', 'grit_trees_tree_oid_bytes_width'),
           ('grit_tree_entries', 'oid', 'oid_bytes',
            'grit_trees_pk_oid_bytes_idx', 'grit_trees_oid_bytes_width'),
           ('grit_commits', 'commit_oid', 'commit_oid_bytes',
            'grit_commits_pk_commit_oid_bytes_idx', 'grit_commits_commit_oid_bytes_width'),
           ('grit_commits', 'tree_oid', 'tree_oid_bytes',
            'grit_commits_pk_tree_oid_bytes_idx', 'grit_commits_tree_oid_bytes_width'),
           ('grit_commit_parents', 'commit_oid', 'commit_oid_bytes',
            'grit_parents_pk_commit_oid_bytes_idx', 'grit_parents_commit_oid_bytes_width'),
           ('grit_commit_parents', 'parent_oid', 'parent_oid_bytes',
            'grit_parents_pk_parent_oid_bytes_idx', 'grit_parents_parent_oid_bytes_width'),
           ('grit_import_trusted_objects', 'oid', 'oid_bytes',
            'grit_trusted_pk_oid_bytes_idx', 'grit_trusted_oid_bytes_width'),
           ('grit_packs', 'pack_checksum', 'pack_checksum_bytes',
            'grit_packs_pk_pack_checksum_bytes_idx', 'grit_packs_pack_checksum_bytes_width'),
           ('grit_packs', 'index_checksum', 'index_checksum_bytes',
            'grit_packs_pk_index_checksum_bytes_idx', 'grit_packs_index_checksum_bytes_width'),
           ('grit_pack_objects', 'pack_checksum', 'pack_checksum_bytes',
            'grit_pack_objects_pk_pack_bytes_idx', 'grit_pack_objects_pack_bytes_width'),
           ('grit_pack_objects', 'oid', 'oid_bytes',
            'grit_pack_objects_pk_oid_bytes_idx', 'grit_pack_objects_oid_bytes_width')
         ) as mapping(
           table_name,
           source_column,
           binary_column,
           index_name,
           constraint_name
         )
       loop
         if not exists (
           select 1 from pg_attribute
           where attrelid = to_regclass(column_mapping.table_name)
             and attname = column_mapping.binary_column
             and not attisdropped
         ) then
           execute format(
             'alter table %I add column %I bytea
                generated always as (decode(%I, ''hex'')) stored',
             column_mapping.table_name,
             column_mapping.binary_column,
             column_mapping.source_column
           );
         end if;

         if not exists (
           select 1 from pg_constraint
           where conrelid = to_regclass(column_mapping.table_name)
             and conname = column_mapping.constraint_name
         ) then
           execute format(
             'alter table %I add constraint %I
                check (%I is null or octet_length(%I) in (20, 32)) not valid',
             column_mapping.table_name,
             column_mapping.constraint_name,
             column_mapping.binary_column,
             column_mapping.binary_column
           );
         end if;
         execute format(
           'alter table %I validate constraint %I',
           column_mapping.table_name,
           column_mapping.constraint_name
         );
         execute format(
           'create index if not exists %I on %I (repository_pk, %I)',
           column_mapping.index_name,
           column_mapping.table_name,
           column_mapping.binary_column
         );
       end loop;
     end $$",
    "create index if not exists grit_repositories_listing_idx
        on grit_repositories (tenant_id, archived_at, repository_id)
        where deleted_at is null",
    "create index if not exists grit_objects_repo_kind_idx
        on grit_objects (tenant_id, repository_id, kind, oid)",
    "create index if not exists grit_refs_repo_prefix_idx
        on grit_refs (tenant_id, repository_id, refname text_pattern_ops)",
    "create index if not exists grit_tree_entries_repo_prefix_idx
        on grit_tree_entries (tenant_id, repository_id, tree_oid, path text_pattern_ops)",
    "create index if not exists grit_commits_repo_time_idx
        on grit_commits (tenant_id, repository_id, commit_time desc, commit_oid)",
    "create index if not exists grit_commit_parents_parent_idx
        on grit_commit_parents (tenant_id, repository_id, parent_oid, commit_oid)",
    "create index if not exists grit_pack_objects_oid_idx
        on grit_pack_objects (tenant_id, repository_id, oid)",
    "create index if not exists grit_packs_repo_order_idx
        on grit_packs (tenant_id, repository_id, storage_order)",
];

/// Stable numeric identity for one PostgreSQL repository row.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RepositoryPk(i64);

/// Error returned when a repository primary key is zero or negative.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
#[error("repository primary key must be positive")]
pub struct InvalidRepositoryPk;

impl RepositoryPk {
    /// Return the positive database-assigned identifier.
    #[must_use]
    pub const fn get(self) -> i64 {
        self.0
    }
}

impl TryFrom<i64> for RepositoryPk {
    type Error = InvalidRepositoryPk;

    fn try_from(value: i64) -> std::result::Result<Self, Self::Error> {
        if value <= 0 {
            return Err(InvalidRepositoryPk);
        }
        Ok(Self(value))
    }
}

impl From<RepositoryPk> for i64 {
    fn from(value: RepositoryPk) -> Self {
        value.get()
    }
}

/// Repository metadata stored by the SQL backend.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PgRepositoryRow {
    /// Stable numeric repository identity used by additive child-table migrations.
    pub repository_pk: RepositoryPk,
    /// Tenant that owns the repository row.
    pub tenant: TenantId,
    /// Repository identifier within the tenant.
    pub repository: RepositoryId,
    /// Object hash algorithm used by the repository.
    pub hash_algo: HashAlgo,
    /// Durable generation advanced by visible ref mutations.
    pub ref_generation: u64,
    /// Timestamp when the repository row was created.
    pub created_at: OffsetDateTime,
    /// Timestamp when repository metadata was last changed.
    pub updated_at: OffsetDateTime,
    /// Timestamp when the repository was archived, if archived.
    pub archived_at: Option<OffsetDateTime>,
    /// Timestamp when the repository was soft-deleted, if retained.
    pub deleted_at: Option<OffsetDateTime>,
}

/// Resolved default branch stored in a generation-scoped repository summary.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PgSummaryBranch {
    /// Full branch ref name.
    pub refname: String,
    /// Object id reached by resolving the branch.
    pub target: ObjectId,
}

/// Commit-graph metadata stored for the commit resolved from `HEAD`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PgSummaryCommit {
    /// Commit object id.
    pub oid: ObjectId,
    /// Root tree object id.
    pub tree: ObjectId,
    /// Commit timestamp from the indexed commit row.
    pub commit_time: i64,
    /// Commit-graph generation number.
    pub generation: u32,
}

/// Durable, generation-scoped repository summary used by bounded landing-page reads.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PgRepositorySummary {
    /// Immutable numeric repository identity.
    pub repository_pk: RepositoryPk,
    /// Ref generation represented by this summary.
    pub ref_generation: u64,
    /// Repository object hash algorithm.
    pub hash_algo: HashAlgo,
    /// Repository creation timestamp.
    pub created_at: OffsetDateTime,
    /// Resolved default branch, or explicit absence for detached, unborn, or missing `HEAD`.
    pub default_branch: Option<PgSummaryBranch>,
    /// Count of all refs, including `HEAD`.
    pub refs_count: u64,
    /// Count of refs below `refs/heads/`.
    pub branch_count: u64,
    /// Count of refs below `refs/tags/`.
    pub tag_count: u64,
    /// Indexed commit metadata reached from `HEAD`, when available.
    pub latest_commit: Option<PgSummaryCommit>,
}

/// Result of reading the one bounded summary row nearest the current ref generation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PgRepositorySummaryRead {
    /// A summary exactly matches the repository's current ref generation.
    Hit(PgRepositorySummary),
    /// No summary has been installed for this repository.
    Miss {
        /// Current ref generation that a builder should target.
        current_generation: u64,
    },
    /// Only an older summary exists and must not be served as current.
    Stale {
        /// Current ref generation that a builder should target.
        current_generation: u64,
        /// Newest cached generation observed by the bounded read.
        cached_generation: u64,
    },
}

/// Explicit limits for deterministic repository-summary recomputation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PgRepositorySummaryOptions {
    /// Maximum number of symbolic ref links followed while resolving `HEAD`.
    pub max_symbolic_depth: u32,
}

/// Result of a recompute-and-install attempt.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PgRepositorySummaryInstall {
    /// The summary was installed while its observed generation remained current.
    Installed(PgRepositorySummary),
    /// A concurrent ref mutation advanced the generation, so no stale row was published.
    Stale {
        /// Generation used to build the rejected summary.
        observed_generation: u64,
    },
}

/// Compact-tree read behavior selected for one repository cohort.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TreeBlockReadMode {
    /// Read only the legacy row-oriented browse index.
    LegacyOnly,
    /// Prefer a valid compact block and fall back to legacy rows when it is absent or invalid.
    PreferBlock,
    /// Read both representations and report the first typed difference before serving rows.
    Compare,
}

/// Explicit compact-tree rollout policy supplied by the repository cohort selector.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TreeBlockReadPolicy {
    /// Representation selection and comparison behavior.
    pub mode: TreeBlockReadMode,
    /// Largest encoded block that may be fetched and decoded for one request.
    pub max_block_bytes: usize,
    /// Largest number of legacy rows that may be fetched for one direct tree.
    pub max_legacy_rows: usize,
}

impl TreeBlockReadPolicy {
    /// Construct a policy with explicit byte and row limits.
    ///
    /// The caller is responsible for selecting the repository cohort; storage keeps no implicit
    /// feature flag or process-global rollout state.
    #[must_use]
    pub const fn new(
        mode: TreeBlockReadMode,
        max_block_bytes: usize,
        max_legacy_rows: usize,
    ) -> Self {
        Self {
            mode,
            max_block_bytes,
            max_legacy_rows,
        }
    }
}

/// Durable representation that supplied a compact-tree read result.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TreeBlockReadSource {
    /// Result came from the compact immutable block.
    CompactBlock,
    /// Result came from legacy direct-entry rows.
    LegacyRows,
}

/// Reason a compact block could not be used and legacy rows were selected.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum TreeBlockReadIssue {
    /// No compact block has been persisted for the tree.
    Missing,
    /// Encoded bytes exceeded the caller's request limit.
    BlockTooLarge {
        /// Stored encoded byte length.
        actual: usize,
        /// Caller-supplied maximum encoded byte length.
        limit: usize,
    },
    /// Stored format metadata disagreed with the supported codec or decoded payload.
    InvalidMetadata {
        /// Format version stored beside the payload.
        format_version: i16,
        /// Entry count stored beside the payload.
        entry_count: i64,
    },
    /// Strict decoding rejected the stored payload.
    InvalidPayload(TreeBlockError),
}

/// First difference between compact and legacy direct-tree representations.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TreeBlockMismatch {
    /// Zero-based sorted entry index where the representations first differ.
    pub index: usize,
    /// Compact entry at the differing index, or `None` when compact data ended first.
    pub compact: Option<TreeBlockEntry>,
    /// Legacy entry at the differing index, or `None` when legacy data ended first.
    pub legacy: Option<TreeBlockEntry>,
}

/// Result of a policy-controlled compact-tree read.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TreeBlockReadOutcome {
    /// Sorted direct entries whose names begin with the requested raw prefix.
    pub entries: Vec<TreeBlockEntry>,
    /// Durable representation selected for the returned entries.
    pub source: TreeBlockReadSource,
    /// Compact-block fallback reason, when the block was unavailable or invalid.
    pub issue: Option<TreeBlockReadIssue>,
    /// First comparison difference in [`TreeBlockReadMode::Compare`].
    pub mismatch: Option<TreeBlockMismatch>,
}

/// Stable key used to resume a compact-tree backfill after one processed tree.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TreeBlockBackfillCursor {
    /// Numeric repository identity.
    pub repository_pk: RepositoryPk,
    /// Last processed tree object identifier.
    pub tree_oid: ObjectId,
}

/// Explicit bounds and key range for one resumable compact-tree backfill call.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TreeBlockBackfillOptions {
    /// Exclusive cursor from the previous call, or `None` to begin at the first key.
    pub start_after: Option<TreeBlockBackfillCursor>,
    /// Inclusive final key for an operator-selected repository/tree cohort.
    pub end_at: Option<TreeBlockBackfillCursor>,
    /// Maximum candidate trees examined by this call.
    pub max_trees: usize,
    /// Maximum legacy direct-entry rows transferred by this call.
    pub max_rows: usize,
    /// Maximum estimated legacy row bytes transferred by this call.
    pub max_legacy_bytes: usize,
    /// Maximum encoded bytes accepted for any one generated block.
    pub max_block_bytes: usize,
}

/// One tree skipped because its legacy rows or encoded block exceeded call bounds.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OversizedTreeBlock {
    /// Tree key that was deliberately skipped.
    pub cursor: TreeBlockBackfillCursor,
    /// Number of legacy direct-entry rows reported by PostgreSQL.
    pub rows: u64,
    /// Estimated bytes of names, object IDs, modes, and sizes in those rows.
    pub estimated_bytes: u64,
}

/// Progress returned from one bounded, idempotent compact-tree backfill call.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TreeBlockBackfillReport {
    /// Cursor to supply as [`TreeBlockBackfillOptions::start_after`] on the next call.
    pub next_cursor: Option<TreeBlockBackfillCursor>,
    /// Number of compact blocks inserted or replaced.
    pub blocks_written: usize,
    /// Number of legacy rows encoded into block candidates accepted by the per-tree bounds.
    pub rows_encoded: usize,
    /// Total encoded bytes written.
    pub bytes_written: usize,
    /// Trees skipped because one tree could not fit within the explicit per-tree limits.
    pub oversized: Vec<OversizedTreeBlock>,
    /// Whether no later candidate exists in the requested key range.
    pub exhausted: bool,
}

/// SQLx-backed repository storage using PostgreSQL-compatible SQL.
#[derive(Clone)]
pub struct PgServerStorage {
    pool: PgPool,
}

impl PgServerStorage {
    /// Create storage from a PostgreSQL pool.
    #[must_use]
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Borrow the underlying pool.
    #[must_use]
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Read at most one cached summary and validate it against the durable ref generation.
    ///
    /// This path does not list or recount refs and never returns an older generation as a hit.
    ///
    /// # Errors
    ///
    /// Returns [`Error::RepositoryNotFound`] when the repository is absent, [`Error::Backend`]
    /// for invalid cached metadata, or SQLx errors from PostgreSQL.
    pub async fn read_repository_summary(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
    ) -> Result<PgRepositorySummaryRead> {
        let row = sqlx::query(
            "select repository.repository_pk,
                    repository.ref_generation as current_generation,
                    summary.ref_generation as cached_generation,
                    summary.hash_algo, summary.repository_created_at,
                    summary.default_branch_refname, summary.default_branch_target,
                    summary.refs_count, summary.branch_count, summary.tag_count,
                    summary.latest_commit_oid,
                    summary.latest_commit_tree_oid, summary.latest_commit_time,
                    summary.latest_commit_generation
             from grit_repositories repository
             left join lateral (
                 select * from grit_repository_summaries cached
                 where cached.repository_pk = repository.repository_pk
                 order by cached.ref_generation desc
                 limit 1
             ) summary on true
             where repository.tenant_id = $1 and repository.repository_id = $2
               and repository.deleted_at is null",
        )
        .bind(tenant.as_str())
        .bind(repository.as_str())
        .fetch_optional(&self.pool)
        .await?;
        let Some(row) = row else {
            return Err(Error::RepositoryNotFound(repository_key(
                tenant, repository,
            )));
        };
        let current_generation =
            nonnegative_i64_to_u64(row.try_get("current_generation")?, "current ref generation")?;
        let Some(cached_generation) = row.try_get::<Option<i64>, _>("cached_generation")? else {
            return Ok(PgRepositorySummaryRead::Miss { current_generation });
        };
        let cached_generation = nonnegative_i64_to_u64(cached_generation, "cached ref generation")?;
        if cached_generation != current_generation {
            return Ok(PgRepositorySummaryRead::Stale {
                current_generation,
                cached_generation,
            });
        }
        Ok(PgRepositorySummaryRead::Hit(row_to_repository_summary(
            &row,
        )?))
    }

    /// Recompute a summary with fixed set-based queries and install it if its generation is current.
    ///
    /// `options.max_symbolic_depth` bounds recursive `HEAD` resolution. Ref counts are computed by
    /// a PostgreSQL aggregate and are never materialized into process memory. A final repository-row
    /// lock prevents a builder from publishing after a concurrent ref mutation.
    ///
    /// # Errors
    ///
    /// Returns [`Error::RepositoryNotFound`] when the repository is absent, [`Error::Backend`]
    /// when limits or stored metadata are invalid, or SQLx errors from PostgreSQL.
    pub async fn recompute_repository_summary(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        options: PgRepositorySummaryOptions,
    ) -> Result<PgRepositorySummaryInstall> {
        if options.max_symbolic_depth == 0 || options.max_symbolic_depth > 64 {
            return Err(Error::Backend(
                "summary symbolic-ref depth must be between 1 and 64".to_owned(),
            ));
        }
        let repository_row = self.require_repository(tenant, repository).await?;
        let depth = i32::try_from(options.max_symbolic_depth)
            .map_err(|_| Error::Backend("symbolic-ref depth exceeds i32".to_owned()))?;
        let aggregate = sqlx::query(
            "with recursive head_chain(depth, refname, target_oid, symbolic_target, seen) as (
                 select 0, ref.refname, ref.target_oid, ref.symbolic_target, array[ref.refname]
                 from grit_refs ref
                 where ref.repository_pk = $1 and ref.refname = 'HEAD'
                 union all
                 select chain.depth + 1, ref.refname, ref.target_oid, ref.symbolic_target,
                        chain.seen || ref.refname
                 from head_chain chain
                 join grit_refs ref
                   on ref.repository_pk = $1 and ref.refname = chain.symbolic_target
                 where chain.symbolic_target is not null and chain.depth < $2
                   and not ref.refname = any(chain.seen)
             ), resolution as (
                 select depth, refname, target_oid, symbolic_target, seen
                 from head_chain
                 order by depth desc
                 limit 1
             ), ref_counts as (
                 select count(*) as refs_count,
                        count(*) filter (where refname like 'refs/heads/%') as branch_count,
                        count(*) filter (where refname like 'refs/tags/%') as tag_count
                 from grit_refs where repository_pk = $1
             )
             select ref_counts.refs_count, ref_counts.branch_count, ref_counts.tag_count,
                    resolution.refname as resolved_refname,
                    resolution.target_oid as resolved_head_oid,
                    coalesce(
                        resolution.symbolic_target = any(resolution.seen), false
                    ) as cycle_detected,
                    coalesce(
                        resolution.symbolic_target is not null
                        and resolution.depth >= $2
                        and exists (
                            select 1 from grit_refs next_ref
                            where next_ref.repository_pk = $1
                              and next_ref.refname = resolution.symbolic_target
                        ), false
                    ) as depth_exhausted
             from ref_counts left join resolution on true",
        )
        .bind(repository_row.repository_pk.get())
        .bind(depth)
        .fetch_one(&self.pool)
        .await?;
        if aggregate.try_get::<bool, _>("cycle_detected")? {
            return Err(Error::Backend(
                "symbolic HEAD resolution contains a cycle".to_owned(),
            ));
        }
        if aggregate.try_get::<bool, _>("depth_exhausted")? {
            return Err(Error::Backend(format!(
                "symbolic HEAD resolution exceeds the configured depth {}",
                options.max_symbolic_depth
            )));
        }
        let resolved_head_oid = aggregate
            .try_get::<Option<String>, _>("resolved_head_oid")?
            .map(|oid| ObjectId::from_hex(&oid))
            .transpose()?;
        if resolved_head_oid
            .as_ref()
            .is_some_and(|oid| oid.algo() != repository_row.hash_algo)
        {
            return Err(Error::Backend(
                "resolved HEAD object id has the wrong hash algorithm".to_owned(),
            ));
        }
        let latest_oid = resolved_head_oid;
        let default_branch = match (
            aggregate.try_get::<Option<String>, _>("resolved_refname")?,
            resolved_head_oid,
        ) {
            (Some(refname), Some(target)) if refname.starts_with("refs/heads/") => {
                Some(PgSummaryBranch { refname, target })
            }
            _ => None,
        };
        let latest_commit = match latest_oid {
            Some(oid) => {
                self.read_summary_commit(repository_row.repository_pk, oid)
                    .await?
            }
            None => None,
        };
        let summary = PgRepositorySummary {
            repository_pk: repository_row.repository_pk,
            ref_generation: repository_row.ref_generation,
            hash_algo: repository_row.hash_algo,
            created_at: repository_row.created_at,
            default_branch,
            refs_count: nonnegative_i64_to_u64(aggregate.try_get("refs_count")?, "ref count")?,
            branch_count: nonnegative_i64_to_u64(
                aggregate.try_get("branch_count")?,
                "branch count",
            )?,
            tag_count: nonnegative_i64_to_u64(aggregate.try_get("tag_count")?, "tag count")?,
            latest_commit,
        };
        self.install_repository_summary(summary).await
    }

    /// Apply the built-in schema migrations.
    ///
    /// # Errors
    ///
    /// Returns SQLx errors from migration statements.
    pub async fn migrate(&self) -> Result<()> {
        for statement in MIGRATIONS {
            sqlx::query(statement).execute(&self.pool).await?;
        }
        Ok(())
    }

    /// Encode and idempotently persist one compact direct-tree block.
    ///
    /// This additive write does not remove or update legacy `grit_tree_entries`; callers may keep
    /// dual-writing those rows for rollback during rollout.
    ///
    /// # Errors
    ///
    /// Returns [`Error::RepositoryNotFound`] when the repository is absent, [`Error::Backend`]
    /// when the tree or entry object IDs disagree with its hash algorithm, codec errors, or SQLx
    /// errors from PostgreSQL.
    pub async fn upsert_tree_block(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        tree_oid: &ObjectId,
        entries: &[TreeBlockEntry],
    ) -> Result<()> {
        let row = self.require_repository(tenant, repository).await?;
        validate_tree_oid(row.hash_algo, tree_oid)?;
        let encoded = encode_tree_block(row.hash_algo, entries)
            .map_err(|error| Error::Backend(error.to_string()))?;
        let entry_count = i64::try_from(entries.len())
            .map_err(|_| Error::Backend("tree block entry count exceeds i64".to_owned()))?;
        let mut tx = self.pool.begin().await?;
        lock_import_repository(&mut tx, tenant, repository).await?;
        sqlx::query(
            "insert into grit_tree_blocks
                (repository_pk, tree_oid, format_version, entry_count, data)
             values ($1, $2, $3, $4, $5)
             on conflict (repository_pk, tree_oid)
             do update set format_version = excluded.format_version,
                           entry_count = excluded.entry_count,
                           data = excluded.data
             where grit_tree_blocks.format_version is distinct from excluded.format_version
                or grit_tree_blocks.entry_count is distinct from excluded.entry_count
                or grit_tree_blocks.data is distinct from excluded.data",
        )
        .bind(row.repository_pk.get())
        .bind(tree_oid.as_bytes())
        .bind(i16::from(TREE_BLOCK_FORMAT_VERSION))
        .bind(entry_count)
        .bind(encoded)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(())
    }

    /// Read direct-tree entries using an explicit repository rollout policy.
    ///
    /// `prefix` is matched against raw direct-child name bytes. Compact reads perform one block
    /// query and binary-search the decoded sorted block. Missing, oversized, or invalid blocks
    /// fall back to bounded legacy rows. Compare mode reads both complete representations,
    /// reports the first typed mismatch, and serves the legacy result when they diverge.
    ///
    /// # Errors
    ///
    /// Returns [`Error::RepositoryNotFound`] when the repository is absent, [`Error::Backend`]
    /// for an invalid policy, hash-algorithm mismatch, or legacy tree exceeding its row bound, and
    /// SQLx errors from PostgreSQL.
    pub async fn read_tree_block_entries(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        tree_oid: &ObjectId,
        prefix: &[u8],
        policy: TreeBlockReadPolicy,
    ) -> Result<TreeBlockReadOutcome> {
        validate_tree_block_read_policy(policy)?;
        let repository_row = self.require_repository(tenant, repository).await?;
        validate_tree_oid(repository_row.hash_algo, tree_oid)?;

        if policy.mode == TreeBlockReadMode::LegacyOnly {
            let legacy = self
                .load_legacy_tree_entries(
                    repository_row.repository_pk,
                    tree_oid,
                    repository_row.hash_algo,
                    policy.max_legacy_rows,
                )
                .await?;
            return Ok(TreeBlockReadOutcome {
                entries: select_tree_block_prefix(&legacy, prefix),
                source: TreeBlockReadSource::LegacyRows,
                issue: None,
                mismatch: None,
            });
        }

        let block = self
            .load_tree_block(
                repository_row.repository_pk,
                tree_oid,
                repository_row.hash_algo,
                policy.max_block_bytes,
            )
            .await?;
        let compact = match block {
            Ok(entries) => entries,
            Err(issue) => {
                let legacy = self
                    .load_legacy_tree_entries(
                        repository_row.repository_pk,
                        tree_oid,
                        repository_row.hash_algo,
                        policy.max_legacy_rows,
                    )
                    .await?;
                return Ok(TreeBlockReadOutcome {
                    entries: select_tree_block_prefix(&legacy, prefix),
                    source: TreeBlockReadSource::LegacyRows,
                    issue: Some(issue),
                    mismatch: None,
                });
            }
        };

        if policy.mode == TreeBlockReadMode::Compare {
            let legacy = self
                .load_legacy_tree_entries(
                    repository_row.repository_pk,
                    tree_oid,
                    repository_row.hash_algo,
                    policy.max_legacy_rows,
                )
                .await?;
            let mismatch = first_tree_block_mismatch(&compact, &legacy);
            if mismatch.is_some() {
                return Ok(TreeBlockReadOutcome {
                    entries: select_tree_block_prefix(&legacy, prefix),
                    source: TreeBlockReadSource::LegacyRows,
                    issue: None,
                    mismatch,
                });
            }
        }

        Ok(TreeBlockReadOutcome {
            entries: select_tree_block_prefix(&compact, prefix),
            source: TreeBlockReadSource::CompactBlock,
            issue: None,
            mismatch: None,
        })
    }

    /// Backfill a bounded key range of compact tree blocks from legacy direct-entry rows.
    ///
    /// Candidate keys are ordered by `(repository_pk, tree_oid)` and fetched with keyset
    /// pagination. Legacy entries for all accepted keys are fetched in one bounded query, then
    /// blocks are installed in one idempotent set-based upsert. A tree larger than the explicit
    /// bounds is reported and its cursor advances so an operator can handle it separately.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Backend`] for zero/overflowing limits, invalid stored repository/hash/OID
    /// metadata, or encoded blocks exceeding internal format limits, and SQLx errors from
    /// PostgreSQL. No implicit clock, environment, or global rollout state is consulted.
    pub async fn backfill_tree_blocks(
        &self,
        options: &TreeBlockBackfillOptions,
    ) -> Result<TreeBlockBackfillReport> {
        validate_tree_block_backfill_options(options)?;
        let candidate_limit = options
            .max_trees
            .checked_add(1)
            .and_then(|value| i64::try_from(value).ok())
            .ok_or_else(|| Error::Backend("tree-block candidate limit exceeds i64".to_owned()))?;
        let start_pk = options
            .start_after
            .as_ref()
            .map(|cursor| cursor.repository_pk.get());
        let start_oid = options
            .start_after
            .as_ref()
            .map(|cursor| cursor.tree_oid.as_bytes().to_vec());
        let end_pk = options
            .end_at
            .as_ref()
            .map(|cursor| cursor.repository_pk.get());
        let end_oid = options
            .end_at
            .as_ref()
            .map(|cursor| cursor.tree_oid.as_bytes().to_vec());
        let rows = sqlx::query(
            "select repository.repository_pk, repository.hash_algo, entry.tree_oid_bytes,
                    count(*) as entry_count,
                    coalesce(sum(octet_length(entry.path) + octet_length(entry.oid_bytes)
                                 + octet_length(entry.tree_oid_bytes) + 20), 0)
                        as estimated_bytes
             from grit_tree_entries entry
             join grit_repositories repository
               on repository.repository_pk = entry.repository_pk
             where repository.deleted_at is null
               and ($1::bigint is null
                    or (repository.repository_pk, entry.tree_oid_bytes) > ($1, $2::bytea))
               and ($3::bigint is null
                    or (repository.repository_pk, entry.tree_oid_bytes) <= ($3, $4::bytea))
             group by repository.repository_pk, repository.hash_algo, entry.tree_oid_bytes
             order by repository.repository_pk, entry.tree_oid_bytes
             limit $5",
        )
        .bind(start_pk)
        .bind(start_oid)
        .bind(end_pk)
        .bind(end_oid)
        .bind(candidate_limit)
        .fetch_all(&self.pool)
        .await?;

        let has_more_candidates = rows.len() > options.max_trees;
        let candidates = rows
            .iter()
            .take(options.max_trees)
            .map(tree_block_candidate_from_row)
            .collect::<Result<Vec<_>>>()?;
        let mut selected = Vec::new();
        let mut oversized = Vec::new();
        let mut selected_rows = 0usize;
        let mut selected_bytes = 0usize;
        let mut next_cursor = options.start_after.clone();
        let mut stopped_for_budget = false;
        for candidate in candidates {
            let cursor = candidate.cursor();
            if candidate.entry_count > options.max_rows
                || candidate.estimated_bytes > options.max_legacy_bytes
            {
                oversized.push(oversized_tree_block(&candidate)?);
                next_cursor = Some(cursor);
                continue;
            }
            let Some(next_rows) = selected_rows.checked_add(candidate.entry_count) else {
                return Err(Error::Backend("tree-block row budget overflow".to_owned()));
            };
            let Some(next_bytes) = selected_bytes.checked_add(candidate.estimated_bytes) else {
                return Err(Error::Backend("tree-block byte budget overflow".to_owned()));
            };
            if next_rows > options.max_rows || next_bytes > options.max_legacy_bytes {
                stopped_for_budget = true;
                break;
            }
            selected_rows = next_rows;
            selected_bytes = next_bytes;
            next_cursor = Some(cursor);
            selected.push(candidate);
        }

        let legacy_rows = self.load_backfill_tree_entries(&selected).await?;
        let mut encoded_blocks = Vec::with_capacity(selected.len());
        let mut rows_encoded = 0usize;
        for candidate in &selected {
            let entries = legacy_rows
                .get(&(candidate.repository_pk, candidate.tree_oid))
                .cloned()
                .unwrap_or_default();
            if entries.len() != candidate.entry_count {
                return Err(Error::Backend(
                    "legacy tree changed while compact block was being backfilled".to_owned(),
                ));
            }
            let encoded = encode_tree_block(candidate.hash_algo, &entries)
                .map_err(|error| Error::Backend(error.to_string()))?;
            if encoded.len() > options.max_block_bytes {
                oversized.push(oversized_tree_block(candidate)?);
                continue;
            }
            rows_encoded = rows_encoded.checked_add(entries.len()).ok_or_else(|| {
                Error::Backend("tree-block encoded row count overflow".to_owned())
            })?;
            encoded_blocks.push((candidate, encoded));
        }
        let write_stats = self
            .verify_and_upsert_tree_blocks(&selected, &legacy_rows, &encoded_blocks)
            .await?;
        Ok(TreeBlockBackfillReport {
            next_cursor,
            blocks_written: write_stats.blocks,
            rows_encoded,
            bytes_written: write_stats.bytes,
            oversized,
            exhausted: !has_more_candidates && !stopped_for_budget,
        })
    }

    async fn read_summary_commit(
        &self,
        repository_pk: RepositoryPk,
        oid: ObjectId,
    ) -> Result<Option<PgSummaryCommit>> {
        let row = sqlx::query(
            "select tree_oid, commit_time, generation
             from grit_commits
             where repository_pk = $1 and commit_oid = $2",
        )
        .bind(repository_pk.get())
        .bind(oid.to_hex())
        .fetch_optional(&self.pool)
        .await?;
        row.map(|row| {
            let tree: String = row.try_get("tree_oid")?;
            let tree = ObjectId::from_hex(&tree)?;
            let generation: i32 = row.try_get("generation")?;
            Ok(PgSummaryCommit {
                oid,
                tree,
                commit_time: row.try_get("commit_time")?,
                generation: u32::try_from(generation).map_err(|_| {
                    Error::Backend("summary commit generation is negative".to_owned())
                })?,
            })
        })
        .transpose()
    }

    async fn install_repository_summary(
        &self,
        summary: PgRepositorySummary,
    ) -> Result<PgRepositorySummaryInstall> {
        let observed_generation = i64::try_from(summary.ref_generation)
            .map_err(|_| Error::Backend("ref generation exceeds i64".to_owned()))?;
        let mut tx = self.pool.begin().await?;
        let current: Option<i64> = sqlx::query_scalar(
            "select ref_generation from grit_repositories
             where repository_pk = $1 and deleted_at is null
             for update",
        )
        .bind(summary.repository_pk.get())
        .fetch_optional(&mut *tx)
        .await?;
        let Some(current) = current else {
            return Err(Error::RepositoryNotFound(format!(
                "repository pk {}",
                summary.repository_pk.get()
            )));
        };
        if current != observed_generation {
            return Ok(PgRepositorySummaryInstall::Stale {
                observed_generation: summary.ref_generation,
            });
        }
        let (default_branch_refname, default_branch_target) = summary
            .default_branch
            .as_ref()
            .map(|branch| {
                (
                    Some(branch.refname.as_str()),
                    Some(branch.target.as_bytes()),
                )
            })
            .unwrap_or((None, None));
        let (latest_commit_oid, latest_commit_tree_oid, latest_commit_time, latest_generation) =
            summary
                .latest_commit
                .as_ref()
                .map(|commit| {
                    (
                        Some(commit.oid.as_bytes()),
                        Some(commit.tree.as_bytes()),
                        Some(commit.commit_time),
                        i32::try_from(commit.generation).ok(),
                    )
                })
                .unwrap_or((None, None, None, None));
        if summary.latest_commit.is_some() && latest_generation.is_none() {
            return Err(Error::Backend(
                "summary commit generation exceeds i32".to_owned(),
            ));
        }
        sqlx::query(
            "insert into grit_repository_summaries
                (repository_pk, ref_generation, hash_algo, repository_created_at,
                 default_branch_refname, default_branch_target, refs_count, branch_count, tag_count,
                 latest_commit_oid, latest_commit_tree_oid, latest_commit_time,
                 latest_commit_generation)
             values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
             on conflict (repository_pk, ref_generation) do update
             set hash_algo = excluded.hash_algo,
                 repository_created_at = excluded.repository_created_at,
                 default_branch_refname = excluded.default_branch_refname,
                 default_branch_target = excluded.default_branch_target,
                 refs_count = excluded.refs_count,
                 branch_count = excluded.branch_count,
                 tag_count = excluded.tag_count,
                 latest_commit_oid = excluded.latest_commit_oid,
                 latest_commit_tree_oid = excluded.latest_commit_tree_oid,
                 latest_commit_time = excluded.latest_commit_time,
                 latest_commit_generation = excluded.latest_commit_generation",
        )
        .bind(summary.repository_pk.get())
        .bind(observed_generation)
        .bind(summary.hash_algo.name())
        .bind(summary.created_at)
        .bind(default_branch_refname)
        .bind(default_branch_target)
        .bind(
            i64::try_from(summary.refs_count)
                .map_err(|_| Error::Backend("ref count exceeds i64".to_owned()))?,
        )
        .bind(
            i64::try_from(summary.branch_count)
                .map_err(|_| Error::Backend("branch count exceeds i64".to_owned()))?,
        )
        .bind(
            i64::try_from(summary.tag_count)
                .map_err(|_| Error::Backend("tag count exceeds i64".to_owned()))?,
        )
        .bind(latest_commit_oid)
        .bind(latest_commit_tree_oid)
        .bind(latest_commit_time)
        .bind(latest_generation)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(PgRepositorySummaryInstall::Installed(summary))
    }

    async fn require_repository(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
    ) -> Result<PgRepositoryRow> {
        self.read_repository(tenant, repository)
            .await?
            .ok_or_else(|| Error::RepositoryNotFound(format!("{tenant}/{repository}")))
    }

    async fn load_tree_block(
        &self,
        repository_pk: RepositoryPk,
        tree_oid: &ObjectId,
        hash_algo: HashAlgo,
        max_block_bytes: usize,
    ) -> Result<std::result::Result<Vec<TreeBlockEntry>, TreeBlockReadIssue>> {
        let max_bytes = i64::try_from(max_block_bytes)
            .map_err(|_| Error::Backend("tree-block byte limit exceeds i64".to_owned()))?;
        let row = sqlx::query(
            "select format_version, entry_count, octet_length(data) as data_len,
                    case when octet_length(data) <= $3 then data else null end as bounded_data
             from grit_tree_blocks
             where repository_pk = $1 and tree_oid = $2",
        )
        .bind(repository_pk.get())
        .bind(tree_oid.as_bytes())
        .bind(max_bytes)
        .fetch_optional(&self.pool)
        .await?;
        let Some(row) = row else {
            return Ok(Err(TreeBlockReadIssue::Missing));
        };
        let format_version: i16 = row.try_get("format_version")?;
        let entry_count: i64 = row.try_get("entry_count")?;
        let data_len: i32 = row.try_get("data_len")?;
        if data_len < 0 {
            return Ok(Err(TreeBlockReadIssue::InvalidMetadata {
                format_version,
                entry_count,
            }));
        }
        let data_len = usize::try_from(data_len)
            .map_err(|_| Error::Backend("tree-block byte length is negative".to_owned()))?;
        if data_len > max_block_bytes {
            return Ok(Err(TreeBlockReadIssue::BlockTooLarge {
                actual: data_len,
                limit: max_block_bytes,
            }));
        }
        let data: Option<Vec<u8>> = row.try_get("bounded_data")?;
        if format_version != i16::from(TREE_BLOCK_FORMAT_VERSION) || entry_count < 0 {
            return Ok(Err(TreeBlockReadIssue::InvalidMetadata {
                format_version,
                entry_count,
            }));
        }
        let Some(data) = data else {
            return Ok(Err(TreeBlockReadIssue::InvalidMetadata {
                format_version,
                entry_count,
            }));
        };
        let entries = match decode_tree_block(hash_algo, &data) {
            Ok(entries) => entries,
            Err(error) => return Ok(Err(TreeBlockReadIssue::InvalidPayload(error))),
        };
        if i64::try_from(entries.len()).ok() != Some(entry_count) {
            return Ok(Err(TreeBlockReadIssue::InvalidMetadata {
                format_version,
                entry_count,
            }));
        }
        Ok(Ok(entries))
    }

    async fn load_legacy_tree_entries(
        &self,
        repository_pk: RepositoryPk,
        tree_oid: &ObjectId,
        hash_algo: HashAlgo,
        max_rows: usize,
    ) -> Result<Vec<TreeBlockEntry>> {
        let limit = max_rows
            .checked_add(1)
            .and_then(|value| i64::try_from(value).ok())
            .ok_or_else(|| Error::Backend("legacy tree row limit exceeds i64".to_owned()))?;
        let rows = sqlx::query(
            "select path, mode, oid_bytes, size
             from grit_tree_entries
             where repository_pk = $1 and tree_oid_bytes = $2
             order by path collate \"C\"
             limit $3",
        )
        .bind(repository_pk.get())
        .bind(tree_oid.as_bytes())
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        if rows.len() > max_rows {
            return Err(Error::Backend(format!(
                "legacy tree {} exceeds the configured {max_rows}-row read bound",
                tree_oid.to_hex()
            )));
        }
        rows.iter()
            .map(|row| legacy_tree_entry_from_row(row, hash_algo))
            .collect()
    }

    async fn load_backfill_tree_entries(
        &self,
        candidates: &[PgTreeBlockCandidate],
    ) -> Result<HashMap<(RepositoryPk, ObjectId), Vec<TreeBlockEntry>>> {
        if candidates.is_empty() {
            return Ok(HashMap::new());
        }
        let mut query =
            QueryBuilder::<Postgres>::new("with selected(repository_pk, tree_oid) as (");
        query.push_values(candidates, |mut row, candidate| {
            row.push_bind(candidate.repository_pk.get())
                .push_bind(candidate.tree_oid.as_bytes());
        });
        query.push(
            ") select entry.repository_pk, entry.tree_oid_bytes, entry.path, entry.mode,
                      entry.oid_bytes, entry.size
               from grit_tree_entries entry
               join selected on selected.repository_pk = entry.repository_pk
                            and selected.tree_oid = entry.tree_oid_bytes
               order by entry.repository_pk, entry.tree_oid_bytes, entry.path collate \"C\"",
        );
        let rows = query
            .build()
            .persistent(false)
            .fetch_all(&self.pool)
            .await?;
        collect_backfill_tree_entries(&rows, candidates)
    }

    async fn verify_and_upsert_tree_blocks(
        &self,
        selected: &[PgTreeBlockCandidate],
        source_entries: &HashMap<(RepositoryPk, ObjectId), Vec<TreeBlockEntry>>,
        encoded_blocks: &[(&PgTreeBlockCandidate, Vec<u8>)],
    ) -> Result<PgTreeBlockWriteStats> {
        if encoded_blocks.is_empty() {
            return Ok(PgTreeBlockWriteStats::default());
        }
        let mut tx = self.pool.begin().await?;
        lock_tree_block_backfill_repositories(&mut tx, selected).await?;
        let current_entries = load_backfill_tree_entries_in_transaction(&mut tx, selected).await?;
        if &current_entries != source_entries {
            return Err(Error::Backend(
                "legacy trees changed while compact blocks were being encoded".to_owned(),
            ));
        }
        let write_stats =
            upsert_encoded_tree_blocks_in_transaction(&mut tx, encoded_blocks).await?;
        tx.commit().await?;
        Ok(write_stats)
    }

    /// Create a repository metadata row for a tenant-scoped repository.
    ///
    /// # Errors
    ///
    /// Returns [`Error::RepositoryAlreadyExists`] if the row already exists, or SQLx errors from
    /// the backend.
    pub async fn create_repository(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        hash_algo: HashAlgo,
    ) -> Result<PgRepositoryRow> {
        let mut tx = self.pool.begin().await?;
        lock_repository_name(&mut tx, tenant, repository).await?;
        let row = sqlx::query(
            "insert into grit_repositories
                (tenant_id, repository_id, hash_algo, created_at, updated_at)
             values ($1, $2, $3, now(), now())
             on conflict (tenant_id, repository_id) do nothing
             returning repository_pk, tenant_id, repository_id, hash_algo, ref_generation, created_at, updated_at,
                       archived_at, deleted_at",
        )
        .bind(tenant.as_str())
        .bind(repository.as_str())
        .bind(hash_algo.name())
        .fetch_optional(&mut *tx)
        .await?;

        let Some(row) = row else {
            return Err(Error::RepositoryAlreadyExists(format!(
                "{tenant}/{repository}"
            )));
        };
        let repository = row_to_repository(&row)?;
        tx.commit().await?;
        Ok(repository)
    }

    /// Read a repository metadata row when it exists and has not been deleted.
    ///
    /// # Errors
    ///
    /// Returns SQLx errors from the backend, invalid stored identifiers, or invalid hash
    /// algorithm metadata.
    pub async fn read_repository(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
    ) -> Result<Option<PgRepositoryRow>> {
        let row = sqlx::query(
            "select repository_pk, tenant_id, repository_id, hash_algo, ref_generation, created_at, updated_at,
                    archived_at, deleted_at
             from grit_repositories
             where tenant_id = $1 and repository_id = $2 and deleted_at is null",
        )
        .bind(tenant.as_str())
        .bind(repository.as_str())
        .fetch_optional(&self.pool)
        .await?;
        row.as_ref().map(row_to_repository).transpose()
    }

    /// List repository metadata rows for a tenant.
    ///
    /// # Errors
    ///
    /// Returns SQLx errors from the backend, invalid stored identifiers, or invalid hash
    /// algorithm metadata.
    pub async fn list_repositories(&self, tenant: &TenantId) -> Result<Vec<PgRepositoryRow>> {
        let rows = sqlx::query(
            "select repository_pk, tenant_id, repository_id, hash_algo, ref_generation, created_at, updated_at,
                    archived_at, deleted_at
             from grit_repositories
             where tenant_id = $1 and deleted_at is null
             order by repository_id",
        )
        .bind(tenant.as_str())
        .fetch_all(&self.pool)
        .await?;

        rows.iter().map(row_to_repository).collect()
    }

    /// Rename a repository and every scoped SQL row owned by it.
    ///
    /// # Errors
    ///
    /// Returns [`Error::RepositoryNotFound`] if the source repository does not exist,
    /// [`Error::RepositoryAlreadyExists`] if the destination exists, or SQLx errors from the
    /// backend.
    pub async fn rename_repository(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        new_repository: &RepositoryId,
    ) -> Result<PgRepositoryRow> {
        let mut tx = self.pool.begin().await?;
        let row =
            rename_repository_in_transaction(&mut tx, tenant, repository, new_repository).await?;
        tx.commit().await?;
        Ok(row)
    }

    /// Mark a repository as archived.
    ///
    /// # Errors
    ///
    /// Returns [`Error::RepositoryNotFound`] if the repository does not exist, or SQLx errors
    /// from the backend.
    pub async fn archive_repository(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
    ) -> Result<PgRepositoryRow> {
        let row = sqlx::query(
            "update grit_repositories
             set archived_at = coalesce(archived_at, now()), updated_at = now()
             where tenant_id = $1 and repository_id = $2 and deleted_at is null
             returning repository_pk, tenant_id, repository_id, hash_algo, ref_generation, created_at, updated_at,
                       archived_at, deleted_at",
        )
        .bind(tenant.as_str())
        .bind(repository.as_str())
        .fetch_optional(&self.pool)
        .await?;

        let Some(row) = row else {
            return Err(Error::RepositoryNotFound(format!("{tenant}/{repository}")));
        };
        row_to_repository(&row)
    }

    /// Hard-delete a repository and every scoped SQL row owned by it.
    ///
    /// # Errors
    ///
    /// Returns [`Error::RepositoryNotFound`] if the repository does not exist, or SQLx errors
    /// from the backend.
    pub async fn delete_repository(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
    ) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        delete_repository_in_transaction(&mut tx, tenant, repository).await?;
        tx.commit().await?;
        Ok(())
    }

    /// Start a SQL transaction for multi-table repository writes.
    ///
    /// # Errors
    ///
    /// Returns SQLx errors when the backend cannot acquire a transaction.
    pub async fn transaction(&self) -> Result<PgServerStorageTransaction> {
        Ok(PgServerStorageTransaction {
            tx: self.pool.begin().await?,
            repository_scope: PgTransactionScope::Unscoped,
        })
    }

    /// Write a ref and append its reflog entry in one SQL transaction.
    ///
    /// # Errors
    ///
    /// Returns [`Error::RefConflict`] when the compare-and-swap guard fails, backend validation
    /// errors when the reflog does not match the ref, or SQLx errors from the backend.
    pub async fn write_ref_with_reflog(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        refname: &str,
        value: &StoredRef,
        expected: Option<Option<StoredRef>>,
        entry: &ReflogEntry,
    ) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        lock_import_repository(&mut tx, tenant, repository).await?;
        let changed = write_ref_with_reflog_in_transaction(
            &mut tx, tenant, repository, refname, value, expected, entry,
        )
        .await?;
        if changed {
            bump_ref_generation_in_transaction(&mut tx, tenant, repository).await?;
        }
        tx.commit().await?;
        Ok(())
    }
}

/// SQL transaction wrapper for multi-table writes.
///
/// Child-table mutations are restricted to one tenant/repository identity. This keeps advisory
/// name locks and repository row locks in canonical order instead of allowing callers to acquire
/// arbitrary multi-repository lock sequences. Rename must be the first scoped operation; after a
/// successful rename the transaction is scoped to the destination identity. A failed rename
/// poisons the lifecycle scope so only [`Self::commit`] or [`Self::rollback`] may follow.
pub struct PgServerStorageTransaction {
    tx: Transaction<'static, Postgres>,
    repository_scope: PgTransactionScope,
}

enum PgTransactionScope {
    Unscoped,
    Repository(TenantId, RepositoryId),
    Lifecycle,
}

impl PgServerStorageTransaction {
    /// Commit all writes performed through this transaction.
    ///
    /// # Errors
    ///
    /// Returns SQLx errors from the backend if commit fails.
    pub async fn commit(self) -> Result<()> {
        self.tx.commit().await?;
        Ok(())
    }

    /// Roll back all writes performed through this transaction.
    ///
    /// # Errors
    ///
    /// Returns SQLx errors from the backend if rollback fails.
    pub async fn rollback(self) -> Result<()> {
        self.tx.rollback().await?;
        Ok(())
    }

    /// Rename a repository and every scoped SQL row owned by it.
    ///
    /// # Errors
    ///
    /// Returns repository lifecycle errors or SQLx errors from the backend.
    pub async fn rename_repository(
        &mut self,
        tenant: &TenantId,
        repository: &RepositoryId,
        new_repository: &RepositoryId,
    ) -> Result<PgRepositoryRow> {
        let requested = format!(
            "{} -> {}",
            repository_key(tenant, repository),
            repository_key(tenant, new_repository)
        );
        match &self.repository_scope {
            PgTransactionScope::Unscoped => {
                self.repository_scope = PgTransactionScope::Lifecycle;
            }
            PgTransactionScope::Repository(locked_tenant, locked_repository) => {
                return Err(Error::TransactionRepositoryScope {
                    locked: repository_key(locked_tenant, locked_repository),
                    requested,
                });
            }
            PgTransactionScope::Lifecycle => {
                return Err(Error::TransactionLifecyclePoisoned { requested });
            }
        }
        let row =
            rename_repository_in_transaction(&mut self.tx, tenant, repository, new_repository)
                .await?;
        self.repository_scope =
            PgTransactionScope::Repository(tenant.clone(), new_repository.clone());
        Ok(row)
    }

    /// Hard-delete a repository and every scoped SQL row owned by it.
    ///
    /// # Errors
    ///
    /// Returns repository lifecycle errors or SQLx errors from the backend.
    pub async fn delete_repository(
        &mut self,
        tenant: &TenantId,
        repository: &RepositoryId,
    ) -> Result<()> {
        self.bind_repository_scope(tenant, repository)?;
        delete_repository_in_transaction(&mut self.tx, tenant, repository).await
    }

    /// Insert an object idempotently within the transaction.
    ///
    /// # Errors
    ///
    /// Returns SQLx errors from the backend.
    pub async fn write_object(
        &mut self,
        tenant: &TenantId,
        repository: &RepositoryId,
        oid: &ObjectId,
        object: &StoredObject,
    ) -> Result<()> {
        self.bind_repository_scope(tenant, repository)?;
        write_object_in_transaction(&mut self.tx, tenant, repository, oid, object).await
    }

    /// Write a ref within the transaction using compare-and-swap semantics.
    ///
    /// # Errors
    ///
    /// Returns [`Error::RefConflict`] when the compare-and-swap guard fails, or SQLx errors from
    /// the backend.
    pub async fn write_ref(
        &mut self,
        tenant: &TenantId,
        repository: &RepositoryId,
        refname: &str,
        value: &StoredRef,
        expected: Option<Option<StoredRef>>,
    ) -> Result<()> {
        self.bind_repository_scope(tenant, repository)?;
        lock_import_repository(&mut self.tx, tenant, repository).await?;
        let changed =
            write_ref_in_transaction(&mut self.tx, tenant, repository, refname, value, expected)
                .await?;
        if changed {
            bump_ref_generation_in_transaction(&mut self.tx, tenant, repository).await?;
        }
        Ok(())
    }

    /// Delete a ref within the transaction using compare-and-swap semantics.
    ///
    /// # Errors
    ///
    /// Returns [`Error::RefConflict`] when the compare-and-swap guard fails, or SQLx errors from
    /// the backend.
    pub async fn delete_ref(
        &mut self,
        tenant: &TenantId,
        repository: &RepositoryId,
        refname: &str,
        expected: Option<StoredRef>,
    ) -> Result<()> {
        self.bind_repository_scope(tenant, repository)?;
        lock_import_repository(&mut self.tx, tenant, repository).await?;
        let changed =
            delete_ref_in_transaction(&mut self.tx, tenant, repository, refname, expected).await?;
        if changed {
            bump_ref_generation_in_transaction(&mut self.tx, tenant, repository).await?;
        }
        Ok(())
    }

    /// Append a reflog entry within the transaction.
    ///
    /// # Errors
    ///
    /// Returns SQLx errors from the backend.
    pub async fn append_reflog(
        &mut self,
        tenant: &TenantId,
        repository: &RepositoryId,
        entry: &ReflogEntry,
    ) -> Result<()> {
        self.bind_repository_scope(tenant, repository)?;
        append_reflog_in_transaction(&mut self.tx, tenant, repository, entry).await
    }

    /// Write a ref and append its reflog entry in this transaction.
    ///
    /// # Errors
    ///
    /// Returns [`Error::RefConflict`] when the compare-and-swap guard fails, backend validation
    /// errors when the reflog does not match the ref, or SQLx errors from the backend.
    pub async fn write_ref_with_reflog(
        &mut self,
        tenant: &TenantId,
        repository: &RepositoryId,
        refname: &str,
        value: &StoredRef,
        expected: Option<Option<StoredRef>>,
        entry: &ReflogEntry,
    ) -> Result<()> {
        self.bind_repository_scope(tenant, repository)?;
        lock_import_repository(&mut self.tx, tenant, repository).await?;
        let changed = write_ref_with_reflog_in_transaction(
            &mut self.tx,
            tenant,
            repository,
            refname,
            value,
            expected,
            entry,
        )
        .await?;
        if changed {
            bump_ref_generation_in_transaction(&mut self.tx, tenant, repository).await?;
        }
        Ok(())
    }

    /// Set or replace a config key within the transaction.
    ///
    /// # Errors
    ///
    /// Returns SQLx errors from the backend.
    pub async fn set_config(
        &mut self,
        tenant: &TenantId,
        repository: &RepositoryId,
        key: &str,
        value: &str,
    ) -> Result<()> {
        self.bind_repository_scope(tenant, repository)?;
        lock_import_repository(&mut self.tx, tenant, repository).await?;
        set_config_in_transaction(&mut self.tx, tenant, repository, key, value).await
    }

    /// Upsert tree entries within the transaction.
    ///
    /// # Errors
    ///
    /// Returns backend validation errors when entry sizes exceed SQL limits, or SQLx errors from
    /// the backend.
    pub async fn upsert_tree_entries(
        &mut self,
        tenant: &TenantId,
        repository: &RepositoryId,
        entries: &[IndexedTreeEntry],
    ) -> Result<()> {
        self.bind_repository_scope(tenant, repository)?;
        upsert_tree_entries_in_transaction(&mut self.tx, tenant, repository, entries).await
    }

    /// Store a pack and its object index rows within the transaction.
    ///
    /// # Errors
    ///
    /// Returns repository lifecycle or backend validation errors, or SQLx errors from the
    /// backend.
    pub async fn write_pack(
        &mut self,
        tenant: &TenantId,
        repository: &RepositoryId,
        pack: &StoredPack,
    ) -> Result<PackMetadata> {
        self.bind_repository_scope(tenant, repository)?;
        write_pack_in_transaction(&mut self.tx, tenant, repository, pack).await
    }

    fn bind_repository_scope(
        &mut self,
        tenant: &TenantId,
        repository: &RepositoryId,
    ) -> Result<()> {
        let requested = (tenant.clone(), repository.clone());
        match &self.repository_scope {
            PgTransactionScope::Unscoped => {
                self.repository_scope = PgTransactionScope::Repository(requested.0, requested.1);
                Ok(())
            }
            PgTransactionScope::Repository(locked_tenant, locked_repository)
                if locked_tenant == &requested.0 && locked_repository == &requested.1 =>
            {
                Ok(())
            }
            PgTransactionScope::Repository(locked_tenant, locked_repository) => {
                Err(Error::TransactionRepositoryScope {
                    locked: repository_key(locked_tenant, locked_repository),
                    requested: repository_key(tenant, repository),
                })
            }
            PgTransactionScope::Lifecycle => Err(Error::TransactionLifecyclePoisoned {
                requested: repository_key(tenant, repository),
            }),
        }
    }
}

fn kind_to_name(kind: ObjectKind) -> &'static str {
    match kind {
        ObjectKind::Blob => "blob",
        ObjectKind::Tree => "tree",
        ObjectKind::Commit => "commit",
        ObjectKind::Tag => "tag",
    }
}

fn name_to_kind(name: &str) -> Result<ObjectKind> {
    match name {
        "blob" => Ok(ObjectKind::Blob),
        "tree" => Ok(ObjectKind::Tree),
        "commit" => Ok(ObjectKind::Commit),
        "tag" => Ok(ObjectKind::Tag),
        other => Err(Error::Backend(format!("unknown object kind '{other}'"))),
    }
}

fn row_to_ref(row: &sqlx::postgres::PgRow, refname: &str) -> Result<StoredRef> {
    let target_oid: Option<String> = row.try_get("target_oid")?;
    let symbolic_target: Option<String> = row.try_get("symbolic_target")?;
    match (target_oid, symbolic_target) {
        (Some(oid), None) => Ok(StoredRef::Direct(ObjectId::from_hex(&oid)?)),
        (None, Some(target)) => Ok(StoredRef::Symbolic(target)),
        _ => Err(Error::Backend(format!("invalid ref row for '{refname}'"))),
    }
}

fn row_to_repository(row: &sqlx::postgres::PgRow) -> Result<PgRepositoryRow> {
    let repository_pk: i64 = row.try_get("repository_pk")?;
    let repository_pk =
        RepositoryPk::try_from(repository_pk).map_err(|error| Error::Backend(error.to_string()))?;
    let tenant: String = row.try_get("tenant_id")?;
    let repository: String = row.try_get("repository_id")?;
    let hash_algo: String = row.try_get("hash_algo")?;
    let hash_algo = HashAlgo::from_name(&hash_algo)
        .ok_or_else(|| Error::Backend(format!("unknown hash algorithm '{hash_algo}'")))?;
    let ref_generation: i64 = row.try_get("ref_generation")?;
    Ok(PgRepositoryRow {
        repository_pk,
        tenant: TenantId::new(tenant)?,
        repository: RepositoryId::new(repository)?,
        hash_algo,
        ref_generation: u64::try_from(ref_generation)
            .map_err(|_| Error::Backend("ref generation is negative".to_owned()))?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
        archived_at: row.try_get("archived_at")?,
        deleted_at: row.try_get("deleted_at")?,
    })
}

fn nonnegative_i64_to_u64(value: i64, field: &str) -> Result<u64> {
    u64::try_from(value).map_err(|_| Error::Backend(format!("{field} is negative")))
}

fn row_to_repository_summary(row: &sqlx::postgres::PgRow) -> Result<PgRepositorySummary> {
    let repository_pk = RepositoryPk::try_from(row.try_get::<i64, _>("repository_pk")?)
        .map_err(|error| Error::Backend(error.to_string()))?;
    let hash_algo_name: String = row.try_get("hash_algo")?;
    let hash_algo = HashAlgo::from_name(&hash_algo_name).ok_or_else(|| {
        Error::Backend(format!("unknown summary hash algorithm '{hash_algo_name}'"))
    })?;
    let default_branch = match (
        row.try_get::<Option<String>, _>("default_branch_refname")?,
        row.try_get::<Option<Vec<u8>>, _>("default_branch_target")?,
    ) {
        (Some(refname), Some(target)) => Some(PgSummaryBranch {
            refname,
            target: ObjectId::from_bytes(&target)?,
        }),
        (None, None) => None,
        _ => {
            return Err(Error::Backend(
                "summary default branch columns disagree".to_owned(),
            ));
        }
    };
    let latest_commit = match (
        row.try_get::<Option<Vec<u8>>, _>("latest_commit_oid")?,
        row.try_get::<Option<Vec<u8>>, _>("latest_commit_tree_oid")?,
        row.try_get::<Option<i64>, _>("latest_commit_time")?,
        row.try_get::<Option<i32>, _>("latest_commit_generation")?,
    ) {
        (Some(oid), Some(tree), Some(commit_time), Some(generation)) => Some(PgSummaryCommit {
            oid: ObjectId::from_bytes(&oid)?,
            tree: ObjectId::from_bytes(&tree)?,
            commit_time,
            generation: u32::try_from(generation)
                .map_err(|_| Error::Backend("summary commit generation is negative".to_owned()))?,
        }),
        (None, None, None, None) => None,
        _ => {
            return Err(Error::Backend(
                "summary latest commit columns disagree".to_owned(),
            ));
        }
    };
    if default_branch
        .as_ref()
        .is_some_and(|branch| branch.target.algo() != hash_algo)
        || latest_commit
            .as_ref()
            .is_some_and(|commit| commit.oid.algo() != hash_algo || commit.tree.algo() != hash_algo)
    {
        return Err(Error::Backend(
            "summary object id has the wrong hash algorithm".to_owned(),
        ));
    }
    Ok(PgRepositorySummary {
        repository_pk,
        ref_generation: nonnegative_i64_to_u64(
            row.try_get("cached_generation")?,
            "cached ref generation",
        )?,
        hash_algo,
        created_at: row.try_get("repository_created_at")?,
        default_branch,
        refs_count: nonnegative_i64_to_u64(row.try_get("refs_count")?, "ref count")?,
        branch_count: nonnegative_i64_to_u64(row.try_get("branch_count")?, "branch count")?,
        tag_count: nonnegative_i64_to_u64(row.try_get("tag_count")?, "tag count")?,
        latest_commit,
    })
}

fn row_to_pack_metadata(row: &sqlx::postgres::PgRow) -> Result<PackMetadata> {
    let pack_checksum: String = row.try_get("pack_checksum")?;
    let index_checksum: String = row.try_get("index_checksum")?;
    let object_count: i32 = row.try_get("object_count")?;
    let size_bytes: i64 = row.try_get("size_bytes")?;
    let storage_order: i64 = row.try_get("storage_order")?;
    if object_count < 0 || size_bytes < 0 || storage_order < 0 {
        return Err(Error::Backend(
            "pack metadata contains negative numeric fields".to_owned(),
        ));
    }
    Ok(PackMetadata {
        pack_checksum: hex_to_bytes(&pack_checksum)?,
        index_checksum: hex_to_bytes(&index_checksum)?,
        object_count: object_count as u32,
        size_bytes: size_bytes as u64,
        storage_order: storage_order as u64,
    })
}

fn row_to_pack_object_index(row: &sqlx::postgres::PgRow) -> Result<PackObjectIndex> {
    let oid: String = row.try_get("oid")?;
    let kind: String = row.try_get("kind")?;
    let offset: i64 = row.try_get("offset")?;
    let size: i64 = row.try_get("size")?;
    let compressed_size: i64 = row.try_get("compressed_size")?;
    if offset < 0 || size < 0 || compressed_size < 0 {
        return Err(Error::Backend(
            "pack object index contains negative numeric fields".to_owned(),
        ));
    }
    Ok(PackObjectIndex {
        oid: ObjectId::from_hex(&oid)?,
        kind: name_to_kind(&kind)?,
        offset: offset as u64,
        size: size as u64,
        compressed_size: compressed_size as u64,
    })
}

fn bytes_to_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len().saturating_mul(2));
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

fn hex_to_bytes(value: &str) -> Result<Vec<u8>> {
    if !value.len().is_multiple_of(2) {
        return Err(Error::Backend("hex value has odd length".to_owned()));
    }
    value
        .as_bytes()
        .chunks(2)
        .map(|chunk| {
            let text = std::str::from_utf8(chunk)
                .map_err(|err| Error::Backend(format!("invalid hex bytes: {err}")))?;
            u8::from_str_radix(text, 16)
                .map_err(|err| Error::Backend(format!("invalid hex byte '{text}': {err}")))
        })
        .collect()
}

fn repository_key(tenant: &TenantId, repository: &RepositoryId) -> String {
    format!("{tenant}/{repository}")
}

fn ref_columns(value: &StoredRef) -> (Option<String>, Option<String>) {
    match value {
        StoredRef::Direct(oid) => (Some(oid.to_hex()), None),
        StoredRef::Symbolic(target) => (None, Some(target.clone())),
    }
}

async fn rename_repository_in_transaction(
    tx: &mut Transaction<'static, Postgres>,
    tenant: &TenantId,
    repository: &RepositoryId,
    new_repository: &RepositoryId,
) -> Result<PgRepositoryRow> {
    let (first, second) = if repository.as_str() <= new_repository.as_str() {
        (repository, new_repository)
    } else {
        (new_repository, repository)
    };
    lock_repository_name(tx, tenant, first).await?;
    if first != second {
        lock_repository_name(tx, tenant, second).await?;
    }
    let mut source_is_live = false;
    let mut destination_exists = false;
    for candidate in [first, second] {
        let is_live: Option<bool> = sqlx::query_scalar(
            "select deleted_at is null from grit_repositories
             where tenant_id = $1 and repository_id = $2
             for update",
        )
        .bind(tenant.as_str())
        .bind(candidate.as_str())
        .fetch_optional(&mut **tx)
        .await?;
        if candidate == repository {
            source_is_live = is_live.unwrap_or(false);
        }
        if candidate == new_repository {
            destination_exists = is_live.is_some();
        }
    }
    if !source_is_live {
        return Err(Error::RepositoryNotFound(repository_key(
            tenant, repository,
        )));
    }
    if destination_exists {
        return Err(Error::RepositoryAlreadyExists(repository_key(
            tenant,
            new_repository,
        )));
    }

    let row = sqlx::query(
        "update grit_repositories
         set repository_id = $3, updated_at = now()
         where tenant_id = $1 and repository_id = $2 and deleted_at is null
         returning repository_pk, tenant_id, repository_id, hash_algo, ref_generation, created_at, updated_at,
                   archived_at, deleted_at",
    )
    .bind(tenant.as_str())
    .bind(repository.as_str())
    .bind(new_repository.as_str())
    .fetch_one(&mut **tx)
    .await
    .map_err(|error| {
        if is_unique_violation(&error) {
            Error::RepositoryAlreadyExists(repository_key(tenant, new_repository))
        } else {
            error.into()
        }
    })?;

    for table in [
        "grit_objects",
        "grit_refs",
        "grit_reflog",
        "grit_config",
        "grit_tree_entries",
        "grit_commits",
        "grit_commit_parents",
        "grit_import_state",
        "grit_import_trusted_objects",
        "grit_packs",
        "grit_pack_objects",
        "grit_external_orphan_candidates",
    ] {
        let sql = format!(
            "update {table}
             set repository_id = $3
             where tenant_id = $1 and repository_id = $2"
        );
        sqlx::query(&sql)
            .bind(tenant.as_str())
            .bind(repository.as_str())
            .bind(new_repository.as_str())
            .execute(&mut **tx)
            .await?;
    }
    row_to_repository(&row)
}

async fn delete_repository_in_transaction(
    tx: &mut Transaction<'static, Postgres>,
    tenant: &TenantId,
    repository: &RepositoryId,
) -> Result<()> {
    lock_repository_name(tx, tenant, repository).await?;
    let row: Option<i32> = sqlx::query_scalar(
        "select 1 from grit_repositories
         where tenant_id = $1 and repository_id = $2 and deleted_at is null
         for update",
    )
    .bind(tenant.as_str())
    .bind(repository.as_str())
    .fetch_optional(&mut **tx)
    .await?;
    if row.is_none() {
        return Err(Error::RepositoryNotFound(repository_key(
            tenant, repository,
        )));
    }

    for table in [
        "grit_objects",
        "grit_refs",
        "grit_reflog",
        "grit_config",
        "grit_tree_entries",
        "grit_commits",
        "grit_commit_parents",
        "grit_import_trusted_objects",
        "grit_import_state",
        "grit_packs",
        "grit_pack_objects",
        "grit_external_orphan_candidates",
    ] {
        let sql = format!("delete from {table} where tenant_id = $1 and repository_id = $2");
        sqlx::query(&sql)
            .bind(tenant.as_str())
            .bind(repository.as_str())
            .execute(&mut **tx)
            .await?;
    }
    sqlx::query(
        "delete from grit_repositories
         where tenant_id = $1 and repository_id = $2",
    )
    .bind(tenant.as_str())
    .bind(repository.as_str())
    .execute(&mut **tx)
    .await?;
    Ok(())
}

/// Lock one tenant-scoped repository name until the current transaction ends.
///
/// PostgreSQL cannot row-lock an absent name, so lifecycle operations additionally serialize on
/// a 64-bit advisory key derived with `hashtextextended`. The length-prefixed, domain-separated
/// input prevents ambiguous tenant/repository concatenations. A theoretical hash collision only
/// serializes unrelated names and cannot allow conflicting lifecycle operations to overlap.
async fn lock_repository_name(
    tx: &mut Transaction<'static, Postgres>,
    tenant: &TenantId,
    repository: &RepositoryId,
) -> Result<()> {
    const REPOSITORY_NAME_LOCK_SEED: i64 = 0x4752_4954_5245_504f;
    let tenant = tenant.as_str();
    let repository = repository.as_str();
    let identity = format!(
        "grit:repository-name:v1:{}:{tenant}:{}:{repository}",
        tenant.len(),
        repository.len()
    );
    sqlx::query("select pg_advisory_xact_lock(hashtextextended($1, $2))")
        .bind(identity)
        .bind(REPOSITORY_NAME_LOCK_SEED)
        .execute(&mut **tx)
        .await?;
    Ok(())
}

fn is_unique_violation(error: &sqlx::Error) -> bool {
    matches!(
        error,
        sqlx::Error::Database(database) if database.code().as_deref() == Some("23505")
    )
}

async fn write_object_in_transaction(
    tx: &mut Transaction<'static, Postgres>,
    tenant: &TenantId,
    repository: &RepositoryId,
    oid: &ObjectId,
    object: &StoredObject,
) -> Result<()> {
    lock_import_repository(tx, tenant, repository).await?;
    sqlx::query(
        "insert into grit_objects (tenant_id, repository_id, oid, kind, data)
         values ($1, $2, $3, $4, $5)
         on conflict (tenant_id, repository_id, oid) do nothing",
    )
    .bind(tenant.as_str())
    .bind(repository.as_str())
    .bind(oid.to_hex())
    .bind(kind_to_name(object.kind))
    .bind(&object.data)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

/// One loose-object row prepared for an importer bulk insert.
pub(crate) struct ImportedObjectRow {
    oid: ObjectId,
    kind: ObjectKind,
    payload: ImportedObjectPayload,
}

enum ImportedObjectPayload {
    Database(Vec<u8>),
    External { backend: String, key: String },
}

impl ImportedObjectRow {
    /// Prepare one object whose payload remains in PostgreSQL.
    pub(crate) fn database(oid: ObjectId, object: StoredObject) -> Self {
        Self {
            oid,
            kind: object.kind,
            payload: ImportedObjectPayload::Database(object.data),
        }
    }

    /// Prepare one object whose payload has been placed in an external byte store.
    pub(crate) fn external(oid: ObjectId, kind: ObjectKind, backend: String, key: String) -> Self {
        Self {
            oid,
            kind,
            payload: ImportedObjectPayload::External { backend, key },
        }
    }
}

/// Insert imported loose-object rows in bind-limit-safe, set-based statements.
///
/// All chunks execute through `tx`, so the caller retains one transaction boundary for the
/// complete importer batch. An empty `rows` slice is a successful no-op.
///
/// # Errors
///
/// Returns SQLx errors from executing any object-row chunk.
pub(crate) async fn write_imported_object_rows_in_transaction(
    tx: &mut Transaction<'static, Postgres>,
    tenant: &TenantId,
    repository: &RepositoryId,
    rows: &[ImportedObjectRow],
) -> Result<()> {
    lock_import_repository(tx, tenant, repository).await?;
    const POSTGRES_BIND_LIMIT: usize = 65_535;
    const BINDS_PER_ROW: usize = 7;

    for chunk in rows.chunks(POSTGRES_BIND_LIMIT / BINDS_PER_ROW) {
        let mut query = QueryBuilder::<Postgres>::new(
            "insert into grit_objects
                (tenant_id, repository_id, oid, kind, data, storage_backend, storage_key) ",
        );
        query.push_values(chunk, |mut row, object| {
            row.push_bind(tenant.as_str())
                .push_bind(repository.as_str())
                .push_bind(object.oid.to_hex())
                .push_bind(kind_to_name(object.kind));
            match &object.payload {
                ImportedObjectPayload::Database(data) => {
                    row.push_bind(Some(data.as_slice()))
                        .push_bind("database")
                        .push_bind(None::<&str>);
                }
                ImportedObjectPayload::External { backend, key } => {
                    row.push_bind(None::<&[u8]>)
                        .push_bind(backend.as_str())
                        .push_bind(Some(key.as_str()));
                }
            }
        });
        query.push(" on conflict (tenant_id, repository_id, oid) do nothing");
        query.build().persistent(false).execute(&mut **tx).await?;
    }
    Ok(())
}

async fn begin_import_in_transaction(
    tx: &mut Transaction<'static, Postgres>,
    tenant: &TenantId,
    repository: &RepositoryId,
) -> Result<ImportSession> {
    lock_import_repository(tx, tenant, repository).await?;
    sqlx::query(
        "insert into grit_import_state
            (tenant_id, repository_id, generation, token, complete, published)
         values ($1, $2, 0, 0, false, false)
         on conflict (tenant_id, repository_id) do nothing",
    )
    .bind(tenant.as_str())
    .bind(repository.as_str())
    .execute(&mut **tx)
    .await?;
    let state = sqlx::query(
        "select generation, complete from grit_import_state
         where tenant_id = $1 and repository_id = $2
         for update",
    )
    .bind(tenant.as_str())
    .bind(repository.as_str())
    .fetch_optional(&mut **tx)
    .await?;

    let row = state.ok_or_else(|| Error::Backend("import state row disappeared".to_owned()))?;
    let current: i64 = row.try_get("generation")?;
    let generation = current
        .checked_add(1)
        .ok_or_else(|| Error::Backend("import generation overflow".to_owned()))?;
    let token: i64 = sqlx::query_scalar("select nextval('grit_import_session_token_seq')")
        .fetch_one(&mut **tx)
        .await?;
    let was_complete: bool = row.try_get("complete")?;
    sqlx::query(
        "update grit_import_state
         set generation = $3, token = $4, complete = false, published = false
         where tenant_id = $1 and repository_id = $2",
    )
    .bind(tenant.as_str())
    .bind(repository.as_str())
    .bind(generation)
    .bind(token)
    .execute(&mut **tx)
    .await?;

    let trusted_objects = if was_complete {
        sqlx::query(
            "select oid, kind from grit_import_trusted_objects
             where tenant_id = $1 and repository_id = $2
             order by oid",
        )
        .bind(tenant.as_str())
        .bind(repository.as_str())
        .fetch_all(&mut **tx)
        .await?
        .into_iter()
        .map(|row| {
            let oid: String = row.try_get("oid")?;
            let kind: String = row.try_get("kind")?;
            Ok((ObjectId::from_hex(&oid)?, name_to_kind(&kind)?))
        })
        .collect::<Result<Vec<_>>>()?
    } else {
        Vec::new()
    };

    Ok(ImportSession::new(
        tenant.clone(),
        repository.clone(),
        u64::try_from(generation)
            .map_err(|_| Error::Backend("import generation is negative".to_owned()))?,
        u64::try_from(token)
            .map_err(|_| Error::Backend("import session token is negative".to_owned()))?,
        trusted_objects,
    ))
}

async fn publish_import_in_transaction(
    tx: &mut Transaction<'static, Postgres>,
    tenant: &TenantId,
    repository: &RepositoryId,
    session: &ImportSession,
    publication: &ImportPublication,
) -> Result<ImportPublicationResult> {
    lock_import_repository(tx, tenant, repository).await?;
    if session.tenant() != tenant || session.repository() != repository {
        return Err(stale_import_session(tenant, repository, session));
    }
    let state = sqlx::query(
        "select generation, token, complete, published from grit_import_state
         where tenant_id = $1 and repository_id = $2
         for update",
    )
    .bind(tenant.as_str())
    .bind(repository.as_str())
    .fetch_optional(&mut **tx)
    .await?;
    let session_generation = i64::try_from(session.generation())
        .map_err(|_| Error::Backend("import generation exceeds i64".to_owned()))?;
    let session_token = i64::try_from(session.token())
        .map_err(|_| Error::Backend("import session token exceeds i64".to_owned()))?;
    let valid = state
        .map(|row| {
            Ok::<_, sqlx::Error>(
                row.try_get::<i64, _>("generation")? == session_generation
                    && row.try_get::<i64, _>("token")? == session_token
                    && !row.try_get::<bool, _>("complete")?
                    && !row.try_get::<bool, _>("published")?,
            )
        })
        .transpose()?
        .unwrap_or(false);
    if !valid {
        return Err(stale_import_session(tenant, repository, session));
    }

    for (key, value) in &publication.config_entries {
        set_config_in_transaction(tx, tenant, repository, key, value).await?;
    }
    let desired_refs = publication
        .refs
        .iter()
        .map(|(refname, _)| refname.as_str())
        .collect::<std::collections::HashSet<_>>();
    let pruned_refs = if publication.prune_deleted_refs {
        let existing = sqlx::query_scalar::<_, String>(
            "select refname from grit_refs
             where tenant_id = $1 and repository_id = $2
             order by refname",
        )
        .bind(tenant.as_str())
        .bind(repository.as_str())
        .fetch_all(&mut **tx)
        .await?;
        existing
            .into_iter()
            .filter(|refname| !desired_refs.contains(refname.as_str()))
            .collect::<Vec<_>>()
    } else {
        Vec::new()
    };
    let mut refs_changed = false;
    for refname in &pruned_refs {
        refs_changed |= delete_ref_in_transaction(tx, tenant, repository, refname, None).await?;
    }
    for (refname, value) in &publication.refs {
        refs_changed |=
            write_ref_in_transaction(tx, tenant, repository, refname, value, None).await?;
    }
    if refs_changed {
        bump_ref_generation_in_transaction(tx, tenant, repository).await?;
    }

    const POSTGRES_BIND_LIMIT: usize = 65_535;
    const BINDS_PER_ROW: usize = 4;
    for chunk in publication
        .newly_trusted
        .chunks(POSTGRES_BIND_LIMIT / BINDS_PER_ROW)
    {
        let mut query = QueryBuilder::<Postgres>::new(
            "insert into grit_import_trusted_objects
                (tenant_id, repository_id, oid, kind) ",
        );
        query.push_values(chunk, |mut row, (oid, kind)| {
            row.push_bind(tenant.as_str())
                .push_bind(repository.as_str())
                .push_bind(oid.to_hex())
                .push_bind(kind_to_name(*kind));
        });
        query.push(" on conflict (tenant_id, repository_id, oid) do nothing");
        query.build().persistent(false).execute(&mut **tx).await?;
    }

    sqlx::query(
        "update grit_import_state
         set published = true
         where tenant_id = $1 and repository_id = $2 and generation = $3",
    )
    .bind(tenant.as_str())
    .bind(repository.as_str())
    .bind(session_generation)
    .execute(&mut **tx)
    .await?;
    Ok(ImportPublicationResult { pruned_refs })
}

async fn complete_import_in_transaction(
    tx: &mut Transaction<'static, Postgres>,
    tenant: &TenantId,
    repository: &RepositoryId,
    session: &ImportSession,
) -> Result<()> {
    lock_import_repository(tx, tenant, repository).await?;
    if session.tenant() != tenant || session.repository() != repository {
        return Err(stale_import_session(tenant, repository, session));
    }
    let state = sqlx::query(
        "select generation, token, complete, published from grit_import_state
         where tenant_id = $1 and repository_id = $2
         for update",
    )
    .bind(tenant.as_str())
    .bind(repository.as_str())
    .fetch_optional(&mut **tx)
    .await?;
    let session_generation = i64::try_from(session.generation())
        .map_err(|_| Error::Backend("import generation exceeds i64".to_owned()))?;
    let session_token = i64::try_from(session.token())
        .map_err(|_| Error::Backend("import session token exceeds i64".to_owned()))?;
    let valid = state
        .map(|row| {
            Ok::<_, sqlx::Error>(
                row.try_get::<i64, _>("generation")? == session_generation
                    && row.try_get::<i64, _>("token")? == session_token
                    && !row.try_get::<bool, _>("complete")?
                    && row.try_get::<bool, _>("published")?,
            )
        })
        .transpose()?
        .unwrap_or(false);
    if !valid {
        return Err(stale_import_session(tenant, repository, session));
    }
    sqlx::query(
        "update grit_import_state
         set complete = true
         where tenant_id = $1 and repository_id = $2 and generation = $3",
    )
    .bind(tenant.as_str())
    .bind(repository.as_str())
    .bind(session_generation)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

/// Acquire the canonical advisory-name then live-row lock pair for a repository mutation.
///
/// Repeated calls for the same identity in one transaction are safe. Callers that need multiple
/// repository identities must acquire them in deterministic identity order; the public
/// transaction wrapper prevents arbitrary multi-repository child mutations.
pub(crate) async fn lock_import_repository(
    tx: &mut Transaction<'static, Postgres>,
    tenant: &TenantId,
    repository: &RepositoryId,
) -> Result<()> {
    lock_repository_name(tx, tenant, repository).await?;
    let exists: Option<i32> = sqlx::query_scalar(
        "select 1 from grit_repositories
         where tenant_id = $1 and repository_id = $2 and deleted_at is null
         for update",
    )
    .bind(tenant.as_str())
    .bind(repository.as_str())
    .fetch_optional(&mut **tx)
    .await?;
    if exists.is_none() {
        return Err(Error::RepositoryNotFound(repository_key(
            tenant, repository,
        )));
    }
    Ok(())
}

fn stale_import_session(
    tenant: &TenantId,
    repository: &RepositoryId,
    session: &ImportSession,
) -> Error {
    Error::StaleImportSession {
        tenant: tenant.as_str().to_owned(),
        repository: repository.as_str().to_owned(),
        generation: session.generation(),
    }
}

async fn write_ref_in_transaction(
    tx: &mut Transaction<'static, Postgres>,
    tenant: &TenantId,
    repository: &RepositoryId,
    refname: &str,
    value: &StoredRef,
    expected: Option<Option<StoredRef>>,
) -> Result<bool> {
    lock_import_repository(tx, tenant, repository).await?;
    let (target_oid, symbolic_target) = ref_columns(value);
    match expected {
        None => {
            let rows = sqlx::query(
                "insert into grit_refs
                    (tenant_id, repository_id, refname, target_oid, symbolic_target, updated_at)
                 values ($1, $2, $3, $4, $5, now())
                 on conflict (tenant_id, repository_id, refname)
                 do update set target_oid = excluded.target_oid,
                               symbolic_target = excluded.symbolic_target,
                               updated_at = now()
                 where grit_refs.target_oid is distinct from excluded.target_oid
                    or grit_refs.symbolic_target is distinct from excluded.symbolic_target",
            )
            .bind(tenant.as_str())
            .bind(repository.as_str())
            .bind(refname)
            .bind(target_oid)
            .bind(symbolic_target)
            .execute(&mut **tx)
            .await?;
            Ok(rows.rows_affected() == 1)
        }
        Some(None) => {
            let rows = sqlx::query(
                "insert into grit_refs
                    (tenant_id, repository_id, refname, target_oid, symbolic_target, updated_at)
                 values ($1, $2, $3, $4, $5, now())
                 on conflict (tenant_id, repository_id, refname) do nothing",
            )
            .bind(tenant.as_str())
            .bind(repository.as_str())
            .bind(refname)
            .bind(target_oid)
            .bind(symbolic_target)
            .execute(&mut **tx)
            .await?;
            if rows.rows_affected() == 1 {
                Ok(true)
            } else {
                Err(Error::RefConflict(refname.to_owned()))
            }
        }
        Some(Some(expected)) => {
            let changed = expected != *value;
            let rows = match expected {
                StoredRef::Direct(expected_oid) => {
                    sqlx::query(
                        "update grit_refs
                         set target_oid = $4, symbolic_target = $5, updated_at = now()
                         where tenant_id = $1 and repository_id = $2 and refname = $3
                           and target_oid = $6 and symbolic_target is null",
                    )
                    .bind(tenant.as_str())
                    .bind(repository.as_str())
                    .bind(refname)
                    .bind(target_oid)
                    .bind(symbolic_target)
                    .bind(expected_oid.to_hex())
                    .execute(&mut **tx)
                    .await?
                }
                StoredRef::Symbolic(expected_target) => {
                    sqlx::query(
                        "update grit_refs
                         set target_oid = $4, symbolic_target = $5, updated_at = now()
                         where tenant_id = $1 and repository_id = $2 and refname = $3
                           and target_oid is null and symbolic_target = $6",
                    )
                    .bind(tenant.as_str())
                    .bind(repository.as_str())
                    .bind(refname)
                    .bind(target_oid)
                    .bind(symbolic_target)
                    .bind(expected_target)
                    .execute(&mut **tx)
                    .await?
                }
            };
            if rows.rows_affected() == 1 {
                Ok(changed)
            } else {
                Err(Error::RefConflict(refname.to_owned()))
            }
        }
    }
}

async fn delete_ref_in_transaction(
    tx: &mut Transaction<'static, Postgres>,
    tenant: &TenantId,
    repository: &RepositoryId,
    refname: &str,
    expected: Option<StoredRef>,
) -> Result<bool> {
    lock_import_repository(tx, tenant, repository).await?;
    let guarded = expected.is_some();
    let rows = match expected {
        None => {
            sqlx::query(
                "delete from grit_refs
                 where tenant_id = $1 and repository_id = $2 and refname = $3",
            )
            .bind(tenant.as_str())
            .bind(repository.as_str())
            .bind(refname)
            .execute(&mut **tx)
            .await?
        }
        Some(StoredRef::Direct(expected_oid)) => {
            sqlx::query(
                "delete from grit_refs
                 where tenant_id = $1 and repository_id = $2 and refname = $3
                   and target_oid = $4 and symbolic_target is null",
            )
            .bind(tenant.as_str())
            .bind(repository.as_str())
            .bind(refname)
            .bind(expected_oid.to_hex())
            .execute(&mut **tx)
            .await?
        }
        Some(StoredRef::Symbolic(expected_target)) => {
            sqlx::query(
                "delete from grit_refs
                 where tenant_id = $1 and repository_id = $2 and refname = $3
                   and target_oid is null and symbolic_target = $4",
            )
            .bind(tenant.as_str())
            .bind(repository.as_str())
            .bind(refname)
            .bind(expected_target)
            .execute(&mut **tx)
            .await?
        }
    };
    if guarded && rows.rows_affected() != 1 {
        return Err(Error::RefConflict(refname.to_owned()));
    }
    Ok(rows.rows_affected() == 1)
}

async fn bump_ref_generation_in_transaction(
    tx: &mut Transaction<'static, Postgres>,
    tenant: &TenantId,
    repository: &RepositoryId,
) -> Result<()> {
    let rows = sqlx::query(
        "update grit_repositories
         set ref_generation = ref_generation + 1, updated_at = now()
         where tenant_id = $1 and repository_id = $2 and deleted_at is null",
    )
    .bind(tenant.as_str())
    .bind(repository.as_str())
    .execute(&mut **tx)
    .await?;
    if rows.rows_affected() != 1 {
        return Err(Error::RepositoryNotFound(repository_key(
            tenant, repository,
        )));
    }
    Ok(())
}

async fn append_reflog_in_transaction(
    tx: &mut Transaction<'static, Postgres>,
    tenant: &TenantId,
    repository: &RepositoryId,
    entry: &ReflogEntry,
) -> Result<()> {
    lock_import_repository(tx, tenant, repository).await?;
    sqlx::query(
        "insert into grit_reflog
            (tenant_id, repository_id, refname, old_oid, new_oid, actor, message, written_at)
         values ($1, $2, $3, $4, $5, $6, $7, $8)",
    )
    .bind(tenant.as_str())
    .bind(repository.as_str())
    .bind(&entry.refname)
    .bind(entry.old_oid.to_hex())
    .bind(entry.new_oid.to_hex())
    .bind(&entry.actor)
    .bind(&entry.message)
    .bind(entry.timestamp)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

async fn write_ref_with_reflog_in_transaction(
    tx: &mut Transaction<'static, Postgres>,
    tenant: &TenantId,
    repository: &RepositoryId,
    refname: &str,
    value: &StoredRef,
    expected: Option<Option<StoredRef>>,
    entry: &ReflogEntry,
) -> Result<bool> {
    if entry.refname != refname {
        return Err(Error::Backend(format!(
            "reflog entry ref '{}' does not match update ref '{refname}'",
            entry.refname
        )));
    }
    if let StoredRef::Direct(oid) = value {
        if entry.new_oid != *oid {
            return Err(Error::Backend(format!(
                "reflog new oid does not match direct ref '{refname}'"
            )));
        }
    }
    let changed =
        write_ref_in_transaction(tx, tenant, repository, refname, value, expected).await?;
    append_reflog_in_transaction(tx, tenant, repository, entry).await?;
    Ok(changed)
}

async fn set_config_in_transaction(
    tx: &mut Transaction<'static, Postgres>,
    tenant: &TenantId,
    repository: &RepositoryId,
    key: &str,
    value: &str,
) -> Result<()> {
    lock_import_repository(tx, tenant, repository).await?;
    sqlx::query(
        "insert into grit_config (tenant_id, repository_id, key, value)
         values ($1, $2, $3, $4)
         on conflict (tenant_id, repository_id, key)
         do update set value = excluded.value",
    )
    .bind(tenant.as_str())
    .bind(repository.as_str())
    .bind(key)
    .bind(value)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

async fn upsert_tree_entries_in_transaction(
    tx: &mut Transaction<'static, Postgres>,
    tenant: &TenantId,
    repository: &RepositoryId,
    entries: &[IndexedTreeEntry],
) -> Result<()> {
    let values = prepare_tree_entry_values(entries)?;
    lock_import_repository(tx, tenant, repository).await?;
    insert_tree_entries_in_transaction(tx, tenant, repository, entries, &values).await
}

#[derive(Clone, Copy)]
struct PgTreeEntryValues {
    mode: i32,
    size: Option<i64>,
}

fn prepare_tree_entry_values(entries: &[IndexedTreeEntry]) -> Result<Vec<PgTreeEntryValues>> {
    entries
        .iter()
        .map(|entry| {
            let mode = i32::try_from(entry.mode)
                .map_err(|_| Error::Backend("tree entry mode exceeds i32".to_owned()))?;
            let size = entry
                .size
                .map(i64::try_from)
                .transpose()
                .map_err(|_| Error::Backend("tree entry size exceeds i64".to_owned()))?;
            Ok(PgTreeEntryValues { mode, size })
        })
        .collect()
}

async fn insert_tree_entries_in_transaction(
    tx: &mut Transaction<'static, Postgres>,
    tenant: &TenantId,
    repository: &RepositoryId,
    entries: &[IndexedTreeEntry],
    values: &[PgTreeEntryValues],
) -> Result<()> {
    const POSTGRES_BIND_LIMIT: usize = 65_535;
    const BINDS_PER_ROW: usize = 8;
    let rows_per_chunk = POSTGRES_BIND_LIMIT / BINDS_PER_ROW;
    let chunk_capacity = rows_per_chunk.min(entries.len());
    let mut final_rows = HashMap::with_capacity(chunk_capacity);
    let mut final_row_indexes = Vec::with_capacity(chunk_capacity);
    for (entry_chunk, value_chunk) in entries
        .chunks(rows_per_chunk)
        .zip(values.chunks(rows_per_chunk))
    {
        // PostgreSQL cannot update the same conflict target twice in one statement. Keep the
        // final occurrence in each chunk to preserve the previous row-at-a-time semantics.
        final_rows.clear();
        for (index, entry) in entry_chunk.iter().enumerate() {
            final_rows.insert((entry.tree_oid, entry.path.as_str()), index);
        }
        final_row_indexes.clear();
        final_row_indexes.extend(final_rows.values().copied());
        final_row_indexes.sort_unstable();

        let mut query = QueryBuilder::<Postgres>::new(
            "insert into grit_tree_entries
                (tenant_id, repository_id, tree_oid, path, mode, oid, kind, size) ",
        );
        query.push_values(final_row_indexes.iter().copied(), |mut row, index| {
            let entry = &entry_chunk[index];
            let value = value_chunk[index];
            row.push_bind(tenant.as_str())
                .push_bind(repository.as_str())
                .push_bind(entry.tree_oid.to_hex())
                .push_bind(entry.path.as_str())
                .push_bind(value.mode)
                .push_bind(entry.oid.to_hex())
                .push_bind(kind_to_name(entry.kind))
                .push_bind(value.size);
        });
        query.push(
            " on conflict (tenant_id, repository_id, tree_oid, path)
              do update set mode = excluded.mode,
                            oid = excluded.oid,
                            kind = excluded.kind,
                            size = excluded.size",
        );
        query.build().persistent(false).execute(&mut **tx).await?;
    }
    Ok(())
}

async fn write_pack_in_transaction(
    tx: &mut Transaction<'static, Postgres>,
    tenant: &TenantId,
    repository: &RepositoryId,
    pack: &StoredPack,
) -> Result<PackMetadata> {
    let values = prepare_pack_values(&pack.metadata, pack.data.len(), &pack.index)?;
    lock_import_repository(tx, tenant, repository).await?;
    install_database_pack_in_transaction(
        tx,
        tenant,
        repository,
        &pack.metadata,
        pack.data.as_slice(),
        &pack.index,
        &values,
    )
    .await
}

async fn install_database_pack_in_transaction(
    tx: &mut Transaction<'static, Postgres>,
    tenant: &TenantId,
    repository: &RepositoryId,
    metadata: &PackMetadata,
    data: &[u8],
    index: &[PackObjectIndex],
    values: &PgPackValues,
) -> Result<PackMetadata> {
    let pack_checksum = bytes_to_hex(&metadata.pack_checksum);
    let index_checksum = bytes_to_hex(&metadata.index_checksum);
    let row = sqlx::query(
        "insert into grit_packs
            (tenant_id, repository_id, pack_checksum, index_checksum, data,
             object_count, size_bytes)
         values ($1, $2, $3, $4, $5, $6, $7)
         on conflict (tenant_id, repository_id, pack_checksum)
         do update set pack_checksum = excluded.pack_checksum
         where grit_packs.storage_backend = 'database'
           and grit_packs.storage_key is null
           and grit_packs.data = excluded.data
           and grit_packs.object_count = excluded.object_count
           and grit_packs.size_bytes = excluded.size_bytes
         returning pack_checksum, index_checksum, object_count, size_bytes, storage_order",
    )
    .bind(tenant.as_str())
    .bind(repository.as_str())
    .bind(&pack_checksum)
    .bind(&index_checksum)
    .bind(data)
    .bind(values.object_count)
    .bind(values.size_bytes)
    .fetch_optional(&mut **tx)
    .await?;
    let row = row.ok_or_else(|| {
        Error::Protocol(format!(
            "stored pack {pack_checksum} conflicts with imported bytes or metadata"
        ))
    })?;

    replace_pack_index_in_transaction(tx, tenant, repository, &pack_checksum, index, values)
        .await?;

    row_to_pack_metadata(&row)
}

/// Install metadata and index rows for pack bytes held by an external backend.
pub(crate) async fn install_external_pack_in_transaction(
    tx: &mut Transaction<'static, Postgres>,
    tenant: &TenantId,
    repository: &RepositoryId,
    metadata: &PackMetadata,
    index: &[PackObjectIndex],
    values: &PgPackValues,
    backend_name: &str,
    storage_key: &str,
) -> Result<PackMetadata> {
    let pack_checksum = bytes_to_hex(&metadata.pack_checksum);
    let index_checksum = bytes_to_hex(&metadata.index_checksum);
    let row = sqlx::query(
        "insert into grit_packs
            (tenant_id, repository_id, pack_checksum, index_checksum, data,
             storage_backend, storage_key, object_count, size_bytes)
         values ($1, $2, $3, $4, null, $5, $6, $7, $8)
         on conflict (tenant_id, repository_id, pack_checksum)
         do update set pack_checksum = excluded.pack_checksum
         where grit_packs.object_count = excluded.object_count
           and grit_packs.size_bytes = excluded.size_bytes
         returning pack_checksum, index_checksum, object_count, size_bytes, storage_order",
    )
    .bind(tenant.as_str())
    .bind(repository.as_str())
    .bind(&pack_checksum)
    .bind(&index_checksum)
    .bind(backend_name)
    .bind(storage_key)
    .bind(values.object_count)
    .bind(values.size_bytes)
    .fetch_optional(&mut **tx)
    .await?;
    let row = row.ok_or_else(|| {
        Error::Protocol(format!(
            "stored pack {pack_checksum} conflicts with imported external metadata"
        ))
    })?;

    replace_pack_index_in_transaction(tx, tenant, repository, &pack_checksum, index, values)
        .await?;
    row_to_pack_metadata(&row)
}

async fn replace_pack_index_in_transaction(
    tx: &mut Transaction<'static, Postgres>,
    tenant: &TenantId,
    repository: &RepositoryId,
    pack_checksum: &str,
    index: &[PackObjectIndex],
    values: &PgPackValues,
) -> Result<()> {
    sqlx::query(
        "delete from grit_pack_objects
         where tenant_id = $1 and repository_id = $2 and pack_checksum = $3",
    )
    .bind(tenant.as_str())
    .bind(repository.as_str())
    .bind(pack_checksum)
    .execute(&mut **tx)
    .await?;

    const POSTGRES_BIND_LIMIT: usize = 65_535;
    const BINDS_PER_ROW: usize = 8;
    for (entry_chunk, value_chunk) in index
        .chunks(POSTGRES_BIND_LIMIT / BINDS_PER_ROW)
        .zip(values.index.chunks(POSTGRES_BIND_LIMIT / BINDS_PER_ROW))
    {
        let mut query = QueryBuilder::<Postgres>::new(
            "insert into grit_pack_objects
                (tenant_id, repository_id, pack_checksum, oid, kind, offset, size,
                 compressed_size) ",
        );
        query.push_values(
            entry_chunk.iter().zip(value_chunk),
            |mut row, (entry, value)| {
                row.push_bind(tenant.as_str())
                    .push_bind(repository.as_str())
                    .push_bind(pack_checksum)
                    .push_bind(entry.oid.to_hex())
                    .push_bind(kind_to_name(entry.kind))
                    .push_bind(value.offset)
                    .push_bind(value.size)
                    .push_bind(value.compressed_size);
            },
        );
        query.build().persistent(false).execute(&mut **tx).await?;
    }

    Ok(())
}

pub(crate) struct PgPackValues {
    object_count: i32,
    size_bytes: i64,
    index: Vec<PgPackIndexValues>,
}

struct PgPackIndexValues {
    offset: i64,
    size: i64,
    compressed_size: i64,
}

pub(crate) fn prepare_pack_values(
    metadata: &PackMetadata,
    data_len: usize,
    index_rows: &[PackObjectIndex],
) -> Result<PgPackValues> {
    if usize::try_from(metadata.object_count).ok() != Some(index_rows.len()) {
        return Err(Error::Protocol(
            "pack metadata object count does not match its index".to_owned(),
        ));
    }
    if u64::try_from(data_len).ok() != Some(metadata.size_bytes) {
        return Err(Error::Protocol(
            "pack metadata size does not match its bytes".to_owned(),
        ));
    }
    let hash_bytes = metadata.pack_checksum.len();
    if !matches!(hash_bytes, 20 | 32)
        || metadata.index_checksum.len() != hash_bytes
        || index_rows
            .iter()
            .any(|entry| entry.oid.as_bytes().len() != hash_bytes)
    {
        return Err(Error::Protocol(
            "pack metadata and index use incompatible object hash widths".to_owned(),
        ));
    }

    let object_count = i32::try_from(metadata.object_count)
        .map_err(|_| Error::Backend("pack object count exceeds i32".to_owned()))?;
    let size_bytes = i64::try_from(metadata.size_bytes)
        .map_err(|_| Error::Backend("pack size exceeds i64".to_owned()))?;

    let mut seen_oids = HashSet::with_capacity(index_rows.len());
    let mut seen_offsets = HashSet::with_capacity(index_rows.len());
    let mut index = Vec::with_capacity(index_rows.len());
    for entry in index_rows {
        if !seen_oids.insert(entry.oid) || !seen_offsets.insert(entry.offset) {
            return Err(Error::Protocol(
                "pack index contains duplicate object ids or offsets".to_owned(),
            ));
        }
        let offset = i64::try_from(entry.offset)
            .map_err(|_| Error::Backend("pack object offset exceeds i64".to_owned()))?;
        let size = i64::try_from(entry.size)
            .map_err(|_| Error::Backend("pack object size exceeds i64".to_owned()))?;
        let compressed_size = i64::try_from(entry.compressed_size)
            .map_err(|_| Error::Backend("pack object compressed size exceeds i64".to_owned()))?;
        index.push(PgPackIndexValues {
            offset,
            size,
            compressed_size,
        });
    }

    Ok(PgPackValues {
        object_count,
        size_bytes,
        index,
    })
}

pub(crate) struct PreparedImportedPack {
    pub(crate) pack: ImportedPack,
    pub(crate) values: PgPackValues,
}

pub(crate) fn prepare_imported_pack_batch(
    packs: Vec<ImportedPack>,
) -> Result<(Vec<PreparedImportedPack>, Vec<usize>)> {
    let mut checksum_indexes = HashMap::<Vec<u8>, usize>::with_capacity(packs.len());
    let mut prepared = Vec::<PreparedImportedPack>::with_capacity(packs.len());
    let mut result_indexes = Vec::with_capacity(packs.len());
    for pack in packs {
        let values = prepare_pack_values(&pack.metadata, pack.data.len(), &pack.index)?;
        if let Some(&existing_index) = checksum_indexes.get(&pack.metadata.pack_checksum) {
            if !imported_pack_contents_match(&prepared[existing_index].pack, &pack) {
                return Err(Error::Protocol(
                    "imported pack batch contains conflicting duplicate checksums".to_owned(),
                ));
            }
            result_indexes.push(existing_index);
            continue;
        }

        let index = prepared.len();
        checksum_indexes.insert(pack.metadata.pack_checksum.clone(), index);
        prepared.push(PreparedImportedPack { pack, values });
        result_indexes.push(index);
    }
    Ok((prepared, result_indexes))
}

fn imported_pack_contents_match(left: &ImportedPack, right: &ImportedPack) -> bool {
    left.metadata.object_count == right.metadata.object_count
        && left.metadata.size_bytes == right.metadata.size_bytes
        && left.data == right.data
        && left.index == right.index
}

#[async_trait]
impl ImportStateStore for PgServerStorage {
    async fn begin_import(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
    ) -> Result<ImportSession> {
        let mut tx = self.pool.begin().await?;
        let session = begin_import_in_transaction(&mut tx, tenant, repository).await?;
        tx.commit().await?;
        Ok(session)
    }

    async fn publish_import(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        session: &ImportSession,
        publication: &ImportPublication,
    ) -> Result<ImportPublicationResult> {
        let mut tx = self.pool.begin().await?;
        let result =
            publish_import_in_transaction(&mut tx, tenant, repository, session, publication)
                .await?;
        tx.commit().await?;
        Ok(result)
    }

    async fn complete_import(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        session: &ImportSession,
    ) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        complete_import_in_transaction(&mut tx, tenant, repository, session).await?;
        tx.commit().await?;
        Ok(())
    }
}

#[async_trait]
impl ObjectStore for PgServerStorage {
    async fn read_object(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        oid: &ObjectId,
    ) -> Result<Option<StoredObject>> {
        let row = sqlx::query(
            "select kind, data, storage_backend, storage_key from grit_objects
             where tenant_id = $1 and repository_id = $2 and oid = $3",
        )
        .bind(tenant.as_str())
        .bind(repository.as_str())
        .bind(oid.to_hex())
        .fetch_optional(&self.pool)
        .await?;

        if let Some(object) = row
            .map(|row| {
                let kind: String = row.try_get("kind")?;
                let data: Option<Vec<u8>> = row.try_get("data")?;
                let storage_backend: String = row.try_get("storage_backend")?;
                let storage_key: Option<String> = row.try_get("storage_key")?;
                let Some(data) = data else {
                    return Err(Error::Backend(format!(
                        "object {} is stored in external backend {} at {}; use externalized storage",
                        oid.to_hex(),
                        storage_backend,
                        storage_key.unwrap_or_else(|| "<missing key>".to_owned())
                    )));
                };
                Ok::<StoredObject, Error>(StoredObject {
                    kind: name_to_kind(&kind)?,
                    data,
                })
            })
            .transpose()?
        {
            return Ok(Some(object));
        }
        let Some((metadata, index)) = self.find_packed_object(tenant, repository, oid).await?
        else {
            return Ok(None);
        };
        const PACK_OBJECT_HEADER_SLACK: u64 = 64;
        let range_len = index
            .compressed_size
            .checked_add(PACK_OBJECT_HEADER_SLACK)
            .ok_or_else(|| Error::Backend("pack object range length overflow".to_owned()))?;
        let range = self
            .read_pack_range(
                tenant,
                repository,
                &metadata.pack_checksum,
                index.offset,
                range_len,
            )
            .await?
            .ok_or_else(|| Error::Backend("pack index points at missing pack data".to_owned()))?;
        match crate::packfile::read_object_from_pack_range(&range, oid, index.offset, oid.algo()) {
            Ok(object) => return Ok(Some(object)),
            Err(Error::Protocol(_)) => {}
            Err(error) => return Err(error),
        }
        let data = self
            .read_pack_data(tenant, repository, &metadata.pack_checksum)
            .await?
            .ok_or_else(|| Error::Backend("pack index points at missing pack data".to_owned()))?;
        let object = crate::packfile::read_object_at_offset(&data, index.offset, oid.algo())?;
        if object.object_id(oid.algo()) != *oid {
            return Err(Error::Protocol(format!(
                "pack object at offset {} does not match expected object {}",
                index.offset,
                oid.to_hex()
            )));
        }
        Ok(Some(object))
    }

    async fn write_object(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        oid: &ObjectId,
        object: &StoredObject,
    ) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        write_object_in_transaction(&mut tx, tenant, repository, oid, object).await?;
        tx.commit().await?;
        Ok(())
    }

    async fn write_imported_object(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        oid: &ObjectId,
        object: &StoredObject,
    ) -> Result<()> {
        let row = ImportedObjectRow::database(*oid, object.clone());
        let mut tx = self.pool.begin().await?;
        write_imported_object_rows_in_transaction(&mut tx, tenant, repository, &[row]).await?;
        tx.commit().await?;
        Ok(())
    }

    async fn write_imported_objects(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        objects: Vec<(ObjectId, StoredObject)>,
    ) -> Result<()> {
        if objects.is_empty() {
            return Ok(());
        }
        let rows = objects
            .into_iter()
            .map(|(oid, object)| ImportedObjectRow::database(oid, object))
            .collect::<Vec<_>>();
        let mut tx = self.pool.begin().await?;
        write_imported_object_rows_in_transaction(&mut tx, tenant, repository, &rows).await?;
        tx.commit().await?;
        Ok(())
    }

    async fn object_exists(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        oid: &ObjectId,
    ) -> Result<bool> {
        let exists: bool = sqlx::query_scalar(
            "select exists(
                select 1 from grit_objects
                where tenant_id = $1 and repository_id = $2 and oid = $3
                union all
                select 1 from grit_pack_objects
                where tenant_id = $1 and repository_id = $2 and oid = $3
             )",
        )
        .bind(tenant.as_str())
        .bind(repository.as_str())
        .bind(oid.to_hex())
        .fetch_one(&self.pool)
        .await?;
        Ok(exists)
    }

    async fn count_objects(&self, tenant: &TenantId, repository: &RepositoryId) -> Result<usize> {
        let count: i64 = sqlx::query_scalar(
            "select count(*) from (
                select oid from grit_objects
                where tenant_id = $1 and repository_id = $2
                union
                select oid from grit_pack_objects
                where tenant_id = $1 and repository_id = $2
             ) objects",
        )
        .bind(tenant.as_str())
        .bind(repository.as_str())
        .fetch_one(&self.pool)
        .await?;
        usize::try_from(count).map_err(|_| Error::Backend("object count exceeds usize".to_owned()))
    }

    async fn list_object_ids(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        kind: Option<ObjectKind>,
    ) -> Result<Vec<(ObjectId, ObjectKind)>> {
        let rows = if let Some(kind) = kind {
            sqlx::query(
                "select oid, min(kind) as kind from (
                    select oid, kind from grit_objects
                    where tenant_id = $1 and repository_id = $2 and kind = $3
                    union
                    select oid, kind from grit_pack_objects
                    where tenant_id = $1 and repository_id = $2 and kind = $3
                 ) objects
                 group by oid
                 order by oid",
            )
            .bind(tenant.as_str())
            .bind(repository.as_str())
            .bind(kind_to_name(kind))
            .fetch_all(&self.pool)
            .await?
        } else {
            sqlx::query(
                "select oid, min(kind) as kind from (
                    select oid, kind from grit_objects
                    where tenant_id = $1 and repository_id = $2
                    union
                    select oid, kind from grit_pack_objects
                    where tenant_id = $1 and repository_id = $2
                 ) objects
                 group by oid
                 order by oid",
            )
            .bind(tenant.as_str())
            .bind(repository.as_str())
            .fetch_all(&self.pool)
            .await?
        };

        rows.into_iter()
            .map(|row| {
                let oid: String = row.try_get("oid")?;
                let kind: String = row.try_get("kind")?;
                Ok((ObjectId::from_hex(&oid)?, name_to_kind(&kind)?))
            })
            .collect()
    }
}

#[async_trait]
impl RefStore for PgServerStorage {
    async fn read_ref(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        refname: &str,
    ) -> Result<Option<StoredRef>> {
        let row = sqlx::query(
            "select target_oid, symbolic_target from grit_refs
             where tenant_id = $1 and repository_id = $2 and refname = $3",
        )
        .bind(tenant.as_str())
        .bind(repository.as_str())
        .bind(refname)
        .fetch_optional(&self.pool)
        .await?;

        row.as_ref().map(|row| row_to_ref(row, refname)).transpose()
    }

    async fn write_ref(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        refname: &str,
        value: &StoredRef,
        expected: Option<Option<StoredRef>>,
    ) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        lock_import_repository(&mut tx, tenant, repository).await?;
        let changed =
            write_ref_in_transaction(&mut tx, tenant, repository, refname, value, expected).await?;
        if changed {
            bump_ref_generation_in_transaction(&mut tx, tenant, repository).await?;
        }
        tx.commit().await?;
        Ok(())
    }

    async fn delete_ref(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        refname: &str,
        expected: Option<StoredRef>,
    ) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        lock_import_repository(&mut tx, tenant, repository).await?;
        let changed =
            delete_ref_in_transaction(&mut tx, tenant, repository, refname, expected).await?;
        if changed {
            bump_ref_generation_in_transaction(&mut tx, tenant, repository).await?;
        }
        tx.commit().await?;
        Ok(())
    }

    async fn list_refs(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        prefix: &str,
    ) -> Result<Vec<(String, StoredRef)>> {
        let rows = sqlx::query(
            "select refname, target_oid, symbolic_target from grit_refs
             where tenant_id = $1 and repository_id = $2 and refname like $3
             order by refname",
        )
        .bind(tenant.as_str())
        .bind(repository.as_str())
        .bind(format!("{prefix}%"))
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter()
            .map(|row| {
                let refname: String = row.try_get("refname")?;
                let value = row_to_ref(&row, &refname)?;
                Ok((refname, value))
            })
            .collect()
    }
}

#[async_trait]
impl ReflogStore for PgServerStorage {
    async fn append_reflog(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        entry: &ReflogEntry,
    ) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        append_reflog_in_transaction(&mut tx, tenant, repository, entry).await?;
        tx.commit().await?;
        Ok(())
    }

    async fn read_reflog(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        refname: &str,
    ) -> Result<Vec<ReflogEntry>> {
        let rows = sqlx::query(
            "select old_oid, new_oid, actor, message, written_at from grit_reflog
             where tenant_id = $1 and repository_id = $2 and refname = $3
             order by sequence",
        )
        .bind(tenant.as_str())
        .bind(repository.as_str())
        .bind(refname)
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter()
            .map(|row| {
                let old_oid: String = row.try_get("old_oid")?;
                let new_oid: String = row.try_get("new_oid")?;
                Ok(ReflogEntry {
                    refname: refname.to_owned(),
                    old_oid: ObjectId::from_hex(&old_oid)?,
                    new_oid: ObjectId::from_hex(&new_oid)?,
                    actor: row.try_get("actor")?,
                    timestamp: row.try_get("written_at")?,
                    message: row.try_get("message")?,
                })
            })
            .collect()
    }
}

#[async_trait]
impl ConfigStore for PgServerStorage {
    async fn get_config(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        key: &str,
    ) -> Result<Option<String>> {
        sqlx::query_scalar(
            "select value from grit_config
             where tenant_id = $1 and repository_id = $2 and key = $3",
        )
        .bind(tenant.as_str())
        .bind(repository.as_str())
        .bind(key)
        .fetch_optional(&self.pool)
        .await
        .map_err(Into::into)
    }

    async fn set_config(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        key: &str,
        value: &str,
    ) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        lock_import_repository(&mut tx, tenant, repository).await?;
        set_config_in_transaction(&mut tx, tenant, repository, key, value).await?;
        tx.commit().await?;
        Ok(())
    }

    async fn list_config(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        prefix: &str,
    ) -> Result<Vec<(String, String)>> {
        let rows = sqlx::query(
            "select key, value from grit_config
             where tenant_id = $1 and repository_id = $2 and key like $3
             order by key",
        )
        .bind(tenant.as_str())
        .bind(repository.as_str())
        .bind(format!("{prefix}%"))
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter()
            .map(|row| Ok((row.try_get("key")?, row.try_get("value")?)))
            .collect()
    }
}

#[derive(Clone, Debug)]
struct PgTreeBlockCandidate {
    repository_pk: RepositoryPk,
    tree_oid: ObjectId,
    hash_algo: HashAlgo,
    entry_count: usize,
    estimated_bytes: usize,
}

#[derive(Clone, Copy, Debug, Default)]
struct PgTreeBlockWriteStats {
    blocks: usize,
    bytes: usize,
}

impl PgTreeBlockCandidate {
    fn cursor(&self) -> TreeBlockBackfillCursor {
        TreeBlockBackfillCursor {
            repository_pk: self.repository_pk,
            tree_oid: self.tree_oid,
        }
    }
}

fn oversized_tree_block(candidate: &PgTreeBlockCandidate) -> Result<OversizedTreeBlock> {
    Ok(OversizedTreeBlock {
        cursor: candidate.cursor(),
        rows: u64::try_from(candidate.entry_count)
            .map_err(|_| Error::Backend("oversized tree row count exceeds u64".to_owned()))?,
        estimated_bytes: u64::try_from(candidate.estimated_bytes).map_err(|_| {
            Error::Backend("oversized tree estimated byte count exceeds u64".to_owned())
        })?,
    })
}

fn validate_tree_oid(hash_algo: HashAlgo, tree_oid: &ObjectId) -> Result<()> {
    if tree_oid.algo() == hash_algo {
        return Ok(());
    }
    Err(Error::Backend(format!(
        "tree object ID does not match repository hash algorithm {}",
        hash_algo.name()
    )))
}

fn validate_tree_block_read_policy(policy: TreeBlockReadPolicy) -> Result<()> {
    if policy.max_block_bytes == 0 || policy.max_legacy_rows == 0 {
        return Err(Error::Backend(
            "tree-block read byte and row limits must be positive".to_owned(),
        ));
    }
    Ok(())
}

fn validate_tree_block_backfill_options(options: &TreeBlockBackfillOptions) -> Result<()> {
    if options.max_trees == 0
        || options.max_rows == 0
        || options.max_legacy_bytes == 0
        || options.max_block_bytes == 0
    {
        return Err(Error::Backend(
            "tree-block backfill tree, row, and byte limits must be positive".to_owned(),
        ));
    }
    if options.max_trees > MAX_TREE_BLOCKS_PER_BATCH {
        return Err(Error::Backend(format!(
            "tree-block backfill tree limit exceeds the PostgreSQL batch maximum of {MAX_TREE_BLOCKS_PER_BATCH}"
        )));
    }
    if options
        .start_after
        .as_ref()
        .zip(options.end_at.as_ref())
        .is_some_and(|(start, end)| compare_tree_block_cursors(start, end).is_ge())
    {
        return Err(Error::Backend(
            "tree-block backfill end cursor must follow its start cursor".to_owned(),
        ));
    }
    Ok(())
}

fn compare_tree_block_cursors(
    left: &TreeBlockBackfillCursor,
    right: &TreeBlockBackfillCursor,
) -> std::cmp::Ordering {
    left.repository_pk
        .cmp(&right.repository_pk)
        .then_with(|| left.tree_oid.as_bytes().cmp(right.tree_oid.as_bytes()))
}

fn select_tree_block_prefix(entries: &[TreeBlockEntry], prefix: &[u8]) -> Vec<TreeBlockEntry> {
    entries[tree_block_prefix_range(entries, prefix)].to_vec()
}

fn first_tree_block_mismatch(
    compact: &[TreeBlockEntry],
    legacy: &[TreeBlockEntry],
) -> Option<TreeBlockMismatch> {
    let shared = compact.len().min(legacy.len());
    let index = (0..shared)
        .find(|&index| compact.get(index) != legacy.get(index))
        .or_else(|| (compact.len() != legacy.len()).then_some(shared))?;
    Some(TreeBlockMismatch {
        index,
        compact: compact.get(index).cloned(),
        legacy: legacy.get(index).cloned(),
    })
}

fn tree_block_candidate_from_row(row: &sqlx::postgres::PgRow) -> Result<PgTreeBlockCandidate> {
    let repository_pk: i64 = row.try_get("repository_pk")?;
    let repository_pk =
        RepositoryPk::try_from(repository_pk).map_err(|error| Error::Backend(error.to_string()))?;
    let hash_algo_name: String = row.try_get("hash_algo")?;
    let hash_algo = HashAlgo::from_name(&hash_algo_name).ok_or_else(|| {
        Error::Backend(format!(
            "unknown repository hash algorithm '{hash_algo_name}'"
        ))
    })?;
    let tree_oid_bytes: Vec<u8> = row.try_get("tree_oid_bytes")?;
    let tree_oid = ObjectId::from_bytes(&tree_oid_bytes)?;
    validate_tree_oid(hash_algo, &tree_oid)?;
    let entry_count: i64 = row.try_get("entry_count")?;
    let estimated_bytes: i64 = row.try_get("estimated_bytes")?;
    if entry_count < 0 || estimated_bytes < 0 {
        return Err(Error::Backend(
            "tree-block candidate has negative aggregate metadata".to_owned(),
        ));
    }
    Ok(PgTreeBlockCandidate {
        repository_pk,
        tree_oid,
        hash_algo,
        entry_count: usize::try_from(entry_count)
            .map_err(|_| Error::Backend("tree entry count exceeds usize".to_owned()))?,
        estimated_bytes: usize::try_from(estimated_bytes)
            .map_err(|_| Error::Backend("tree entry bytes exceed usize".to_owned()))?,
    })
}

fn legacy_tree_entry_from_row(
    row: &sqlx::postgres::PgRow,
    hash_algo: HashAlgo,
) -> Result<TreeBlockEntry> {
    let path: String = row.try_get("path")?;
    let mode: i32 = row.try_get("mode")?;
    let oid_bytes: Vec<u8> = row.try_get("oid_bytes")?;
    let size: Option<i64> = row.try_get("size")?;
    if mode < 0 || size.is_some_and(|value| value < 0) {
        return Err(Error::Backend(
            "legacy tree entry has negative mode or size".to_owned(),
        ));
    }
    let mode = u32::try_from(mode)
        .map_err(|_| Error::Backend("legacy tree entry mode exceeds u32".to_owned()))?;
    let size = size
        .map(u64::try_from)
        .transpose()
        .map_err(|_| Error::Backend("legacy tree entry size exceeds u64".to_owned()))?;
    let oid = ObjectId::from_bytes(&oid_bytes)?;
    if oid.algo() != hash_algo {
        return Err(Error::Backend(format!(
            "tree entry object ID does not match repository hash algorithm {}",
            hash_algo.name()
        )));
    }
    Ok(TreeBlockEntry {
        name: path.into_bytes(),
        mode,
        oid,
        size,
    })
}

fn collect_backfill_tree_entries(
    rows: &[sqlx::postgres::PgRow],
    candidates: &[PgTreeBlockCandidate],
) -> Result<HashMap<(RepositoryPk, ObjectId), Vec<TreeBlockEntry>>> {
    let algorithms = candidates
        .iter()
        .map(|candidate| {
            (
                (candidate.repository_pk, candidate.tree_oid),
                candidate.hash_algo,
            )
        })
        .collect::<HashMap<_, _>>();
    let mut grouped = HashMap::<(RepositoryPk, ObjectId), Vec<TreeBlockEntry>>::new();
    for row in rows {
        let repository_pk: i64 = row.try_get("repository_pk")?;
        let repository_pk = RepositoryPk::try_from(repository_pk)
            .map_err(|error| Error::Backend(error.to_string()))?;
        let tree_oid_bytes: Vec<u8> = row.try_get("tree_oid_bytes")?;
        let tree_oid = ObjectId::from_bytes(&tree_oid_bytes)?;
        let key = (repository_pk, tree_oid);
        let hash_algo = algorithms.get(&key).copied().ok_or_else(|| {
            Error::Backend("PostgreSQL returned an unselected legacy tree".to_owned())
        })?;
        grouped
            .entry(key)
            .or_default()
            .push(legacy_tree_entry_from_row(row, hash_algo)?);
    }
    Ok(grouped)
}

async fn lock_tree_block_backfill_repositories(
    tx: &mut Transaction<'static, Postgres>,
    candidates: &[PgTreeBlockCandidate],
) -> Result<()> {
    let mut repository_pks = candidates
        .iter()
        .map(|candidate| candidate.repository_pk)
        .collect::<Vec<_>>();
    repository_pks.sort_unstable();
    repository_pks.dedup();
    if repository_pks.is_empty() {
        return Ok(());
    }
    let mut query = QueryBuilder::<Postgres>::new(
        "select repository_pk from grit_repositories where deleted_at is null and repository_pk in (",
    );
    let mut separated = query.separated(", ");
    for repository_pk in &repository_pks {
        separated.push_bind(repository_pk.get());
    }
    separated.push_unseparated(") order by repository_pk for share");
    let rows = query.build().persistent(false).fetch_all(&mut **tx).await?;
    if rows.len() != repository_pks.len() {
        return Err(Error::Backend(
            "repository changed while compact trees were being backfilled".to_owned(),
        ));
    }
    Ok(())
}

async fn load_backfill_tree_entries_in_transaction(
    tx: &mut Transaction<'static, Postgres>,
    candidates: &[PgTreeBlockCandidate],
) -> Result<HashMap<(RepositoryPk, ObjectId), Vec<TreeBlockEntry>>> {
    if candidates.is_empty() {
        return Ok(HashMap::new());
    }
    let mut query = QueryBuilder::<Postgres>::new("with selected(repository_pk, tree_oid) as (");
    query.push_values(candidates, |mut row, candidate| {
        row.push_bind(candidate.repository_pk.get())
            .push_bind(candidate.tree_oid.as_bytes());
    });
    query.push(
        ") select entry.repository_pk, entry.tree_oid_bytes, entry.path, entry.mode,
                  entry.oid_bytes, entry.size
           from grit_tree_entries entry
           join selected on selected.repository_pk = entry.repository_pk
                        and selected.tree_oid = entry.tree_oid_bytes
           order by entry.repository_pk, entry.tree_oid_bytes, entry.path collate \"C\"",
    );
    let rows = query.build().persistent(false).fetch_all(&mut **tx).await?;
    collect_backfill_tree_entries(&rows, candidates)
}

async fn upsert_encoded_tree_blocks_in_transaction(
    tx: &mut Transaction<'static, Postgres>,
    encoded_blocks: &[(&PgTreeBlockCandidate, Vec<u8>)],
) -> Result<PgTreeBlockWriteStats> {
    if encoded_blocks.is_empty() {
        return Ok(PgTreeBlockWriteStats::default());
    }
    let values = encoded_blocks
        .iter()
        .map(|(candidate, encoded)| {
            i64::try_from(candidate.entry_count)
                .map(|entry_count| (*candidate, encoded, entry_count))
                .map_err(|_| Error::Backend("tree-block entry count exceeds i64".to_owned()))
        })
        .collect::<Result<Vec<_>>>()?;
    let mut query = QueryBuilder::<Postgres>::new(
        "insert into grit_tree_blocks
            (repository_pk, tree_oid, format_version, entry_count, data) ",
    );
    query.push_values(values, |mut row, (candidate, encoded, entry_count)| {
        row.push_bind(candidate.repository_pk.get())
            .push_bind(candidate.tree_oid.as_bytes())
            .push_bind(i16::from(TREE_BLOCK_FORMAT_VERSION))
            .push_bind(entry_count)
            .push_bind(encoded.as_slice());
    });
    query.push(
        " on conflict (repository_pk, tree_oid)
          do update set format_version = excluded.format_version,
                        entry_count = excluded.entry_count,
                        data = excluded.data
          where grit_tree_blocks.format_version is distinct from excluded.format_version
             or grit_tree_blocks.entry_count is distinct from excluded.entry_count
             or grit_tree_blocks.data is distinct from excluded.data
          returning octet_length(data) as data_len",
    );
    let rows = query.build().persistent(false).fetch_all(&mut **tx).await?;
    rows.iter()
        .try_fold(PgTreeBlockWriteStats::default(), |mut stats, row| {
            let data_len: i32 = row.try_get("data_len")?;
            let data_len = usize::try_from(data_len).map_err(|_| {
                Error::Backend("written tree-block byte count is negative".to_owned())
            })?;
            stats.blocks = stats
                .blocks
                .checked_add(1)
                .ok_or_else(|| Error::Backend("written tree-block count overflow".to_owned()))?;
            stats.bytes = stats.bytes.checked_add(data_len).ok_or_else(|| {
                Error::Backend("written tree-block byte count overflow".to_owned())
            })?;
            Ok(stats)
        })
}

#[async_trait]
impl BrowseIndex for PgServerStorage {
    async fn upsert_tree_entries(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        entries: &[IndexedTreeEntry],
    ) -> Result<()> {
        let values = prepare_tree_entry_values(entries)?;
        let mut tx = self.pool.begin().await?;
        lock_import_repository(&mut tx, tenant, repository).await?;
        insert_tree_entries_in_transaction(&mut tx, tenant, repository, entries, &values).await?;
        tx.commit().await?;
        Ok(())
    }

    async fn replace_tree_entries(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        entries: &[IndexedTreeEntry],
    ) -> Result<()> {
        let values = prepare_tree_entry_values(entries)?;
        let mut tx = self.pool.begin().await?;
        lock_import_repository(&mut tx, tenant, repository).await?;
        sqlx::query(
            "delete from grit_tree_entries
             where tenant_id = $1 and repository_id = $2",
        )
        .bind(tenant.as_str())
        .bind(repository.as_str())
        .execute(&mut *tx)
        .await?;
        insert_tree_entries_in_transaction(&mut tx, tenant, repository, entries, &values).await?;
        tx.commit().await?;
        Ok(())
    }

    async fn list_tree_entries(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        tree_oid: &ObjectId,
        prefix: &str,
    ) -> Result<Vec<IndexedTreeEntry>> {
        let rows = sqlx::query(
            "select tree_oid, path, mode, oid, kind, size from grit_tree_entries
             where tenant_id = $1 and repository_id = $2 and tree_oid = $3 and path like $4
             order by path",
        )
        .bind(tenant.as_str())
        .bind(repository.as_str())
        .bind(tree_oid.to_hex())
        .bind(format!("{prefix}%"))
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter()
            .map(|row| {
                let tree_oid: String = row.try_get("tree_oid")?;
                let oid: String = row.try_get("oid")?;
                let kind: String = row.try_get("kind")?;
                let mode: i32 = row.try_get("mode")?;
                let size: Option<i64> = row.try_get("size")?;
                if mode < 0 {
                    return Err(Error::Backend("tree entry mode is negative".to_owned()));
                }
                Ok(IndexedTreeEntry {
                    tree_oid: ObjectId::from_hex(&tree_oid)?,
                    path: row.try_get("path")?,
                    mode: mode as u32,
                    oid: ObjectId::from_hex(&oid)?,
                    kind: name_to_kind(&kind)?,
                    size: size.map(|value| value as u64),
                })
            })
            .collect()
    }

    async fn read_blob_at_path(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        tree_oid: &ObjectId,
        path: &str,
    ) -> Result<Option<StoredObject>> {
        let oid: Option<String> = sqlx::query_scalar(
            "select oid from grit_tree_entries
             where tenant_id = $1 and repository_id = $2 and tree_oid = $3 and path = $4",
        )
        .bind(tenant.as_str())
        .bind(repository.as_str())
        .bind(tree_oid.to_hex())
        .bind(path)
        .fetch_optional(&self.pool)
        .await?;

        match oid {
            Some(oid) => {
                self.read_object(tenant, repository, &ObjectId::from_hex(&oid)?)
                    .await
            }
            None => Ok(None),
        }
    }
}

struct PgCommitRow<'a> {
    commit: &'a IndexedCommit,
    generation: i32,
}

fn prepare_commit_rows(commits: &[IndexedCommit]) -> Result<Vec<PgCommitRow<'_>>> {
    commits
        .iter()
        .map(|commit| {
            let generation = i32::try_from(commit.generation)
                .map_err(|_| Error::Backend("commit generation exceeds i32".to_owned()))?;
            if let Some(last_parent_order) = commit.parents.len().checked_sub(1) {
                i32::try_from(last_parent_order)
                    .map_err(|_| Error::Backend("parent order exceeds i32".to_owned()))?;
            }
            Ok(PgCommitRow { commit, generation })
        })
        .collect()
}

async fn insert_commit_parent_rows_in_transaction(
    tx: &mut Transaction<'static, Postgres>,
    tenant: &TenantId,
    repository: &RepositoryId,
    prepared: &[PgCommitRow<'_>],
    parent_edges: &[(usize, usize, i32)],
) -> Result<()> {
    let mut query = QueryBuilder::<Postgres>::new(
        "insert into grit_commit_parents
            (tenant_id, repository_id, commit_oid, parent_oid, parent_order) ",
    );
    query.push_values(
        parent_edges,
        |mut values, &(commit_index, parent_index, parent_order)| {
            let commit = prepared[commit_index].commit;
            values
                .push_bind(tenant.as_str())
                .push_bind(repository.as_str())
                .push_bind(commit.oid.to_hex())
                .push_bind(commit.parents[parent_index].to_hex())
                .push_bind(parent_order);
        },
    );
    query.build().persistent(false).execute(&mut **tx).await?;
    Ok(())
}

#[async_trait]
impl CommitGraphStore for PgServerStorage {
    async fn upsert_commits(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        commits: &[IndexedCommit],
    ) -> Result<()> {
        const POSTGRES_BIND_LIMIT: usize = 65_535;
        const COMMIT_BINDS_PER_ROW: usize = 6;
        const PARENT_BINDS_PER_ROW: usize = 5;
        const DELETE_FIXED_BINDS: usize = 2;

        let prepared = prepare_commit_rows(commits)?;
        let mut final_rows = HashMap::with_capacity(prepared.len());
        for (index, row) in prepared.iter().enumerate() {
            final_rows.insert(row.commit.oid, index);
        }
        let mut final_row_indexes = final_rows.into_values().collect::<Vec<_>>();
        final_row_indexes.sort_unstable();

        let mut tx = self.pool.begin().await?;
        lock_import_repository(&mut tx, tenant, repository).await?;
        for indexes in final_row_indexes.chunks(POSTGRES_BIND_LIMIT / COMMIT_BINDS_PER_ROW) {
            let mut query = QueryBuilder::<Postgres>::new(
                "insert into grit_commits
                    (tenant_id, repository_id, commit_oid, tree_oid, commit_time, generation) ",
            );
            query.push_values(indexes, |mut values, &index| {
                let row = &prepared[index];
                values
                    .push_bind(tenant.as_str())
                    .push_bind(repository.as_str())
                    .push_bind(row.commit.oid.to_hex())
                    .push_bind(row.commit.tree.to_hex())
                    .push_bind(row.commit.commit_time)
                    .push_bind(row.generation);
            });
            query.push(
                " on conflict (tenant_id, repository_id, commit_oid)
                  do update set tree_oid = excluded.tree_oid,
                                commit_time = excluded.commit_time,
                                generation = excluded.generation",
            );
            query.build().persistent(false).execute(&mut *tx).await?;
        }

        for indexes in final_row_indexes.chunks(POSTGRES_BIND_LIMIT - DELETE_FIXED_BINDS) {
            let mut query = QueryBuilder::<Postgres>::new(
                "delete from grit_commit_parents
                 where tenant_id = ",
            );
            query
                .push_bind(tenant.as_str())
                .push(" and repository_id = ")
                .push_bind(repository.as_str())
                .push(" and commit_oid in (");
            {
                let mut separated = query.separated(", ");
                for &index in indexes {
                    separated.push_bind(prepared[index].commit.oid.to_hex());
                }
            }
            query.push(")");
            query.build().persistent(false).execute(&mut *tx).await?;
        }

        let parent_rows_per_chunk = POSTGRES_BIND_LIMIT / PARENT_BINDS_PER_ROW;
        let estimated_parent_edges = final_row_indexes.len().saturating_mul(2);
        let mut parent_edges =
            Vec::with_capacity(parent_rows_per_chunk.min(estimated_parent_edges));
        for &commit_index in &final_row_indexes {
            for (parent_index, _) in prepared[commit_index].commit.parents.iter().enumerate() {
                let parent_order = i32::try_from(parent_index)
                    .map_err(|_| Error::Backend("parent order exceeds i32".to_owned()))?;
                parent_edges.push((commit_index, parent_index, parent_order));
                if parent_edges.len() == parent_rows_per_chunk {
                    insert_commit_parent_rows_in_transaction(
                        &mut tx,
                        tenant,
                        repository,
                        &prepared,
                        &parent_edges,
                    )
                    .await?;
                    parent_edges.clear();
                }
            }
        }
        if !parent_edges.is_empty() {
            insert_commit_parent_rows_in_transaction(
                &mut tx,
                tenant,
                repository,
                &prepared,
                &parent_edges,
            )
            .await?;
        }
        tx.commit().await?;
        Ok(())
    }

    async fn read_indexed_commit(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        oid: &ObjectId,
    ) -> Result<Option<IndexedCommit>> {
        let row = sqlx::query(
            "select commit_oid, tree_oid, commit_time, generation from grit_commits
             where tenant_id = $1 and repository_id = $2 and commit_oid = $3",
        )
        .bind(tenant.as_str())
        .bind(repository.as_str())
        .bind(oid.to_hex())
        .fetch_optional(&self.pool)
        .await?;

        let Some(row) = row else {
            return Ok(None);
        };
        let commit_oid: String = row.try_get("commit_oid")?;
        let tree_oid: String = row.try_get("tree_oid")?;
        let generation: i32 = row.try_get("generation")?;
        if generation < 0 {
            return Err(Error::Backend("commit generation is negative".to_owned()));
        }
        Ok(Some(IndexedCommit {
            oid: ObjectId::from_hex(&commit_oid)?,
            tree: ObjectId::from_hex(&tree_oid)?,
            parents: self.commit_parents(tenant, repository, oid).await?,
            commit_time: row.try_get("commit_time")?,
            generation: generation as u32,
        }))
    }

    async fn commit_parents(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        oid: &ObjectId,
    ) -> Result<Vec<ObjectId>> {
        let rows = sqlx::query(
            "select parent_oid from grit_commit_parents
             where tenant_id = $1 and repository_id = $2 and commit_oid = $3
             order by parent_order",
        )
        .bind(tenant.as_str())
        .bind(repository.as_str())
        .bind(oid.to_hex())
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter()
            .map(|row| {
                let parent_oid: String = row.try_get("parent_oid")?;
                Ok(ObjectId::from_hex(&parent_oid)?)
            })
            .collect()
    }

    async fn commit_children(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        oid: &ObjectId,
    ) -> Result<Vec<ObjectId>> {
        let rows = sqlx::query(
            "select commit_oid from grit_commit_parents
             where tenant_id = $1 and repository_id = $2 and parent_oid = $3
             order by commit_oid",
        )
        .bind(tenant.as_str())
        .bind(repository.as_str())
        .bind(oid.to_hex())
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter()
            .map(|row| {
                let commit_oid: String = row.try_get("commit_oid")?;
                Ok(ObjectId::from_hex(&commit_oid)?)
            })
            .collect()
    }

    async fn list_indexed_commits(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
    ) -> Result<Vec<IndexedCommit>> {
        let rows = sqlx::query(
            "select commit_oid, tree_oid, commit_time, generation from grit_commits
             where tenant_id = $1 and repository_id = $2
             order by commit_time desc, commit_oid",
        )
        .bind(tenant.as_str())
        .bind(repository.as_str())
        .fetch_all(&self.pool)
        .await?;

        let mut commits = Vec::with_capacity(rows.len());
        for row in rows {
            let commit_oid: String = row.try_get("commit_oid")?;
            let tree_oid: String = row.try_get("tree_oid")?;
            let oid = ObjectId::from_hex(&commit_oid)?;
            let generation: i32 = row.try_get("generation")?;
            if generation < 0 {
                return Err(Error::Backend("commit generation is negative".to_owned()));
            }
            commits.push(IndexedCommit {
                oid,
                tree: ObjectId::from_hex(&tree_oid)?,
                parents: self.commit_parents(tenant, repository, &oid).await?,
                commit_time: row.try_get("commit_time")?,
                generation: generation as u32,
            });
        }
        Ok(commits)
    }
}

#[async_trait]
impl PackStore for PgServerStorage {
    fn supports_native_pack_import(&self) -> bool {
        true
    }

    async fn write_imported_packs(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        packs: Vec<ImportedPack>,
    ) -> Result<Vec<PackMetadata>> {
        let (prepared, result_indexes) = prepare_imported_pack_batch(packs)?;
        let mut tx = self.pool.begin().await?;
        lock_import_repository(&mut tx, tenant, repository).await?;

        let mut unique_metadata = Vec::with_capacity(prepared.len());
        for prepared_pack in &prepared {
            unique_metadata.push(
                install_database_pack_in_transaction(
                    &mut tx,
                    tenant,
                    repository,
                    &prepared_pack.pack.metadata,
                    prepared_pack.pack.data.as_slice(),
                    &prepared_pack.pack.index,
                    &prepared_pack.values,
                )
                .await?,
            );
        }
        tx.commit().await?;

        Ok(result_indexes
            .into_iter()
            .map(|index| unique_metadata[index].clone())
            .collect())
    }

    async fn write_pack(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        pack: &StoredPack,
    ) -> Result<PackMetadata> {
        let mut tx = self.pool.begin().await?;
        let metadata = write_pack_in_transaction(&mut tx, tenant, repository, pack).await?;
        tx.commit().await?;
        Ok(metadata)
    }

    async fn read_pack_metadata(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        pack_checksum: &[u8],
    ) -> Result<Option<PackMetadata>> {
        let row = sqlx::query(
            "select pack_checksum, index_checksum, object_count, size_bytes, storage_order
             from grit_packs
             where tenant_id = $1 and repository_id = $2 and pack_checksum = $3",
        )
        .bind(tenant.as_str())
        .bind(repository.as_str())
        .bind(bytes_to_hex(pack_checksum))
        .fetch_optional(&self.pool)
        .await?;
        row.as_ref().map(row_to_pack_metadata).transpose()
    }

    async fn read_pack_data(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        pack_checksum: &[u8],
    ) -> Result<Option<Vec<u8>>> {
        let row = sqlx::query(
            "select data, storage_backend, storage_key from grit_packs
             where tenant_id = $1 and repository_id = $2 and pack_checksum = $3",
        )
        .bind(tenant.as_str())
        .bind(repository.as_str())
        .bind(bytes_to_hex(pack_checksum))
        .fetch_optional(&self.pool)
        .await?;

        let Some(row) = row else {
            return Ok(None);
        };
        let data: Option<Vec<u8>> = row.try_get("data")?;
        if let Some(data) = data {
            return Ok(Some(data));
        }
        let storage_backend: String = row.try_get("storage_backend")?;
        let storage_key: Option<String> = row.try_get("storage_key")?;
        Err(Error::Backend(format!(
            "pack {} is stored in external backend {} at {}; use externalized storage",
            bytes_to_hex(pack_checksum),
            storage_backend,
            storage_key.unwrap_or_else(|| "<missing key>".to_owned())
        )))
    }

    async fn read_pack_range(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        pack_checksum: &[u8],
        start: u64,
        len: u64,
    ) -> Result<Option<Vec<u8>>> {
        start
            .checked_add(len)
            .ok_or_else(|| Error::Backend("pack range overflow".to_owned()))?;
        // PostgreSQL's bytea substring arguments are int4, while bytea values are bounded below
        // that range. Capping larger validated requests preserves beyond-end/through-end slicing.
        let position = start
            .checked_add(1)
            .and_then(|position| i32::try_from(position).ok())
            .unwrap_or(i32::MAX);
        let len = i32::try_from(len).unwrap_or(i32::MAX);

        let row = sqlx::query(
            "select substring(data from $4 for $5) as data, storage_backend, storage_key
             from grit_packs
             where tenant_id = $1 and repository_id = $2 and pack_checksum = $3",
        )
        .bind(tenant.as_str())
        .bind(repository.as_str())
        .bind(bytes_to_hex(pack_checksum))
        .bind(position)
        .bind(len)
        .fetch_optional(&self.pool)
        .await?;

        let Some(row) = row else {
            return Ok(None);
        };
        let data: Option<Vec<u8>> = row.try_get("data")?;
        if let Some(data) = data {
            return Ok(Some(data));
        }
        let storage_backend: String = row.try_get("storage_backend")?;
        let storage_key: Option<String> = row.try_get("storage_key")?;
        Err(Error::Backend(format!(
            "pack {} is stored in external backend {} at {}; use externalized storage",
            bytes_to_hex(pack_checksum),
            storage_backend,
            storage_key.unwrap_or_else(|| "<missing key>".to_owned())
        )))
    }

    async fn find_packed_object(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        oid: &ObjectId,
    ) -> Result<Option<(PackMetadata, PackObjectIndex)>> {
        let row = sqlx::query(
            "select p.pack_checksum, p.index_checksum, p.object_count, p.size_bytes,
                    p.storage_order, o.oid, o.kind, o.offset, o.size, o.compressed_size
             from grit_pack_objects o
             join grit_packs p
               on p.tenant_id = o.tenant_id
              and p.repository_id = o.repository_id
              and p.pack_checksum = o.pack_checksum
             where o.tenant_id = $1 and o.repository_id = $2 and o.oid = $3
             order by p.storage_order desc
             limit 1",
        )
        .bind(tenant.as_str())
        .bind(repository.as_str())
        .bind(oid.to_hex())
        .fetch_optional(&self.pool)
        .await?;
        row.as_ref()
            .map(|row| Ok((row_to_pack_metadata(row)?, row_to_pack_object_index(row)?)))
            .transpose()
    }

    async fn read_pack_index_at_offset(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        pack_checksum: &[u8],
        offset: u64,
    ) -> Result<Option<PackObjectIndex>> {
        let offset = i64::try_from(offset)
            .map_err(|_| Error::Backend("pack object offset exceeds i64".to_owned()))?;
        let row = sqlx::query(
            "select oid, kind, offset, size, compressed_size
             from grit_pack_objects
             where tenant_id = $1 and repository_id = $2 and pack_checksum = $3 and offset = $4",
        )
        .bind(tenant.as_str())
        .bind(repository.as_str())
        .bind(bytes_to_hex(pack_checksum))
        .bind(offset)
        .fetch_optional(&self.pool)
        .await?;
        row.as_ref().map(row_to_pack_object_index).transpose()
    }

    async fn list_packs(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
    ) -> Result<Vec<PackMetadata>> {
        let rows = sqlx::query(
            "select pack_checksum, index_checksum, object_count, size_bytes, storage_order
             from grit_packs
             where tenant_id = $1 and repository_id = $2
             order by storage_order",
        )
        .bind(tenant.as_str())
        .bind(repository.as_str())
        .fetch_all(&self.pool)
        .await?;
        rows.iter().map(row_to_pack_metadata).collect()
    }

    async fn list_pack_objects(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        pack_checksum: Option<&[u8]>,
    ) -> Result<Vec<(PackMetadata, PackObjectIndex)>> {
        let rows = if let Some(pack_checksum) = pack_checksum {
            sqlx::query(
                "select p.pack_checksum, p.index_checksum, p.object_count, p.size_bytes,
                        p.storage_order, o.oid, o.kind, o.offset, o.size, o.compressed_size
                 from grit_pack_objects o
                 join grit_packs p
                   on p.tenant_id = o.tenant_id
                  and p.repository_id = o.repository_id
                  and p.pack_checksum = o.pack_checksum
                 where o.tenant_id = $1 and o.repository_id = $2 and o.pack_checksum = $3
                 order by p.storage_order, o.offset",
            )
            .bind(tenant.as_str())
            .bind(repository.as_str())
            .bind(bytes_to_hex(pack_checksum))
            .fetch_all(&self.pool)
            .await?
        } else {
            sqlx::query(
                "select p.pack_checksum, p.index_checksum, p.object_count, p.size_bytes,
                        p.storage_order, o.oid, o.kind, o.offset, o.size, o.compressed_size
                 from grit_pack_objects o
                 join grit_packs p
                   on p.tenant_id = o.tenant_id
                  and p.repository_id = o.repository_id
                  and p.pack_checksum = o.pack_checksum
                 where o.tenant_id = $1 and o.repository_id = $2
                 order by p.storage_order, o.offset",
            )
            .bind(tenant.as_str())
            .bind(repository.as_str())
            .fetch_all(&self.pool)
            .await?
        };

        rows.iter()
            .map(|row| Ok((row_to_pack_metadata(row)?, row_to_pack_object_index(row)?)))
            .collect()
    }
}
