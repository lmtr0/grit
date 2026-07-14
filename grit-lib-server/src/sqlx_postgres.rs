//! SQLx PostgreSQL/YugaByteDB backend.
//!
//! The schema is intentionally append-friendly and transaction-oriented: objects are content
//! addressed and immutable, while refs are the primary compare-and-swap mutation point.

use async_trait::async_trait;
use grit_lib::objects::{HashAlgo, ObjectId, ObjectKind};
use sqlx::{PgPool, Postgres, QueryBuilder, Row, Transaction};
use std::collections::{HashMap, HashSet};
use time::{Duration, OffsetDateTime};

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
    "alter table grit_repositories
        add column if not exists history_generation bigint not null default 0",
    "alter table grit_repositories
        add column if not exists config_generation bigint not null default 0",
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
    "do $$ begin
        if not exists (
            select 1 from pg_constraint
            where conrelid = 'grit_repositories'::regclass
              and conname = 'grit_repositories_history_generation_nonnegative'
        ) then
            alter table grit_repositories
                add constraint grit_repositories_history_generation_nonnegative
                check (history_generation >= 0) not valid;
        end if;
     end $$",
    "alter table grit_repositories validate constraint grit_repositories_history_generation_nonnegative",
    "do $$ begin
        if not exists (
            select 1 from pg_constraint
            where conrelid = 'grit_repositories'::regclass
              and conname = 'grit_repositories_config_generation_nonnegative'
        ) then
            alter table grit_repositories
                add constraint grit_repositories_config_generation_nonnegative
                check (config_generation >= 0) not valid;
        end if;
     end $$",
    "alter table grit_repositories validate constraint grit_repositories_config_generation_nonnegative",
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
    "create sequence if not exists grit_cache_outbox_event_id_seq",
    "create sequence if not exists grit_cache_outbox_claim_token_seq",
    "create table if not exists grit_cache_invalidation_outbox (
        event_id bigint not null default nextval('grit_cache_outbox_event_id_seq'),
        repository_pk bigint not null,
        tenant_id text not null,
        repository_id text not null,
        event_kind smallint not null,
        ref_generation bigint,
        config_generation bigint,
        history_generation bigint,
        claimed_by text,
        claim_token bigint,
        lease_expires_at timestamptz,
        not_before timestamptz,
        attempts integer not null default 0,
        primary key (event_id),
        check (event_id > 0),
        check (repository_pk > 0),
        check (
            (event_kind = 1 and ref_generation is not null
                and config_generation is null and history_generation is null)
            or (event_kind = 2 and ref_generation is null
                and config_generation is not null and history_generation is null)
            or (event_kind = 3 and ref_generation is null
                and config_generation is null and history_generation is not null)
            or (event_kind = 4 and ref_generation is null
                and config_generation is null and history_generation is null)
        ),
        check (ref_generation is null or ref_generation >= 0),
        check (config_generation is null or config_generation >= 0),
        check (history_generation is null or history_generation >= 0),
        check (attempts >= 0),
        check (claim_token is null or claim_token > 0),
        check ((claimed_by is null) = (claim_token is null)),
        check ((claimed_by is null) = (lease_expires_at is null))
    )",
    "alter sequence grit_cache_outbox_event_id_seq
        owned by grit_cache_invalidation_outbox.event_id",
    "do $$ begin
        if not exists (
            select 1 from pg_constraint
            where conrelid = 'grit_cache_invalidation_outbox'::regclass
              and conname = 'grit_cache_outbox_payload_shape'
        ) then
            alter table grit_cache_invalidation_outbox
                add constraint grit_cache_outbox_payload_shape check (
                    (event_kind = 1 and ref_generation is not null
                        and config_generation is null and history_generation is null)
                    or (event_kind = 2 and ref_generation is null
                        and config_generation is not null and history_generation is null)
                    or (event_kind = 3 and ref_generation is null
                        and config_generation is null and history_generation is not null)
                    or (event_kind = 4 and ref_generation is null
                        and config_generation is null and history_generation is null)
                ) not valid;
        end if;
     end $$",
    "alter table grit_cache_invalidation_outbox
        validate constraint grit_cache_outbox_payload_shape",
    "create index if not exists grit_cache_outbox_claim_idx
        on grit_cache_invalidation_outbox
            (not_before, lease_expires_at, event_id)
        where claimed_by is null or lease_expires_at is not null",
    "create index if not exists grit_cache_outbox_repository_idx
        on grit_cache_invalidation_outbox (repository_pk, event_id)",
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
    "create sequence if not exists grit_migration_session_id_seq",
    "create sequence if not exists grit_migration_fencing_token_seq",
    "create table if not exists grit_migration_sessions (
        migration_id bigint not null default nextval('grit_migration_session_id_seq'),
        idempotency_key text not null,
        source_repository_pk bigint,
        source_external_id text,
        destination_repository_pk bigint not null,
        hash_algo text not null,
        phase smallint not null,
        state smallint not null,
        fencing_token bigint not null default 0,
        claimed_by text,
        lease_expires_at timestamptz,
        attempt_count integer not null default 0,
        last_object_oid bytea,
        last_ref_name text,
        last_tree_oid bytea,
        last_pack_checksum bytea,
        completed_objects bigint not null default 0,
        completed_refs bigint not null default 0,
        completed_trees bigint not null default 0,
        completed_packs bigint not null default 0,
        completed_bytes bigint not null default 0,
        terminal_reason text,
        created_at timestamptz not null,
        updated_at timestamptz not null,
        primary key (migration_id),
        unique (idempotency_key),
        constraint grit_migration_destination_fk foreign key (destination_repository_pk)
            references grit_repositories (repository_pk) on delete cascade,
        check (migration_id > 0),
        check ((source_repository_pk is null) <> (source_external_id is null)),
        check (source_repository_pk is null or source_repository_pk > 0),
        check (source_repository_pk is null
            or source_repository_pk <> destination_repository_pk),
        check (length(idempotency_key) between 1 and 256),
        check (source_external_id is null or length(source_external_id) between 1 and 1024),
        check (hash_algo in ('sha1', 'sha256')),
        constraint grit_migration_phase_range check (phase between 1 and 9),
        check (state between 1 and 3),
        check (fencing_token >= 0),
        check (attempt_count >= 0),
        check ((claimed_by is null) = (lease_expires_at is null)),
        check (claimed_by is null or length(claimed_by) between 1 and 256),
        check (claimed_by is null or lease_expires_at > updated_at),
        check (state = 1 or claimed_by is null),
        check (claimed_by is null or fencing_token > 0),
        check ((state = 1 and terminal_reason is null)
            or (state in (2, 3) and terminal_reason is not null
                and length(terminal_reason) between 1 and 4096)),
        check (last_object_oid is null or octet_length(last_object_oid)
            = case hash_algo when 'sha1' then 20 else 32 end),
        check (last_tree_oid is null or octet_length(last_tree_oid)
            = case hash_algo when 'sha1' then 20 else 32 end),
        check (last_pack_checksum is null or octet_length(last_pack_checksum)
            = case hash_algo when 'sha1' then 20 else 32 end),
        check (last_ref_name is null or length(last_ref_name) between 1 and 1024),
        check (completed_objects >= 0 and completed_refs >= 0 and completed_trees >= 0
            and completed_packs >= 0 and completed_bytes >= 0),
        check (updated_at >= created_at)
    )",
    "alter table grit_migration_sessions
        add column if not exists source_snapshot_ref_generation bigint",
    "alter table grit_migration_sessions
        add column if not exists source_snapshot_history_generation bigint",
    "alter table grit_migration_sessions
        add column if not exists source_snapshot_config_generation bigint",
    "alter table grit_migration_sessions
        add column if not exists source_snapshot_external_token text",
    "alter table grit_migration_sessions
        add column if not exists applied_ref_generation bigint",
    "alter table grit_migration_sessions
        add column if not exists applied_ref_name text",
    "alter table grit_migration_sessions
        add column if not exists applied_history_generation bigint",
    "alter table grit_migration_sessions
        add column if not exists applied_config_generation bigint",
    "alter table grit_migration_sessions
        add column if not exists applied_config_key text",
    "alter table grit_migration_sessions
        add column if not exists applied_external_token text",
    "do $$ begin
        if not exists (
            select 1 from pg_constraint
            where conrelid = 'grit_migration_sessions'::regclass
              and conname = 'grit_migration_journal_cursor_shape'
        ) then
            alter table grit_migration_sessions
                add constraint grit_migration_journal_cursor_shape check (
                    (applied_ref_name is null or
                        (applied_ref_generation is not null
                         and length(applied_ref_name) between 1 and 1024))
                    and (applied_config_key is null or
                        (applied_config_generation is not null
                         and length(applied_config_key) between 1 and 1024))
                ) not valid;
        end if;
     end $$",
    "alter table grit_migration_sessions
        validate constraint grit_migration_journal_cursor_shape",
    "alter table grit_migration_sessions drop constraint if exists grit_migration_sessions_phase_check",
    "alter table grit_migration_sessions drop constraint if exists grit_migration_phase_range",
    "alter table grit_migration_sessions add constraint grit_migration_phase_range
        check (phase between 1 and 9)",
    "do $$ declare constraint_row record; begin
        for constraint_row in
            select conname from pg_constraint
            where conrelid = 'grit_migration_sessions'::regclass
              and pg_get_constraintdef(oid) like '%phase < 4%claimed_by%'
        loop
            execute format('alter table grit_migration_sessions drop constraint %I',
                constraint_row.conname);
        end loop;
     end $$",
    "create table if not exists grit_ref_change_journal (
        repository_pk bigint not null,
        generation bigint not null,
        refname text not null,
        old_target_oid text,
        old_symbolic_target text,
        new_target_oid text,
        new_symbolic_target text,
        primary key (repository_pk, generation, refname),
        constraint grit_ref_change_journal_repository_fk foreign key (repository_pk)
            references grit_repositories (repository_pk) on delete cascade,
        check (generation > 0),
        check ((old_target_oid is null) or (old_symbolic_target is null)),
        check ((new_target_oid is null) or (new_symbolic_target is null))
    )",
    "create table if not exists grit_config_change_journal (
        repository_pk bigint not null,
        generation bigint not null,
        key text not null,
        old_value text,
        new_value text,
        primary key (repository_pk, generation, key),
        constraint grit_config_change_journal_repository_fk foreign key (repository_pk)
            references grit_repositories (repository_pk) on delete cascade,
        check (generation > 0),
        check (old_value is not null or new_value is not null)
    )",
    "do $$ begin
        if not exists (
            select 1 from pg_constraint
            where conrelid = 'grit_ref_change_journal'::regclass
              and conname = 'grit_ref_change_journal_repository_fk'
        ) then
            alter table grit_ref_change_journal
                add constraint grit_ref_change_journal_repository_fk
                foreign key (repository_pk) references grit_repositories (repository_pk)
                on delete cascade not valid;
        end if;
     end $$",
    "alter table grit_ref_change_journal
        validate constraint grit_ref_change_journal_repository_fk",
    "do $$ begin
        if not exists (
            select 1 from pg_constraint
            where conrelid = 'grit_config_change_journal'::regclass
              and conname = 'grit_config_change_journal_repository_fk'
        ) then
            alter table grit_config_change_journal
                add constraint grit_config_change_journal_repository_fk
                foreign key (repository_pk) references grit_repositories (repository_pk)
                on delete cascade not valid;
        end if;
     end $$",
    "alter table grit_config_change_journal
        validate constraint grit_config_change_journal_repository_fk",
    "create or replace function grit_journal_ref_change() returns trigger language plpgsql as $$
        declare repo_pk bigint; next_generation bigint; changed_tenant text; changed_repository text;
            changed_ref text; old_oid text; old_symbolic text; new_oid text; new_symbolic text;
        begin
            if tg_op = 'UPDATE'
                and old.refname is not distinct from new.refname
                and old.target_oid is not distinct from new.target_oid
                and old.symbolic_target is not distinct from new.symbolic_target then
                return new;
            end if;
            if tg_op <> 'INSERT' then
                changed_tenant := old.tenant_id; changed_repository := old.repository_id;
                changed_ref := old.refname; old_oid := old.target_oid;
                old_symbolic := old.symbolic_target;
            end if;
            if tg_op <> 'DELETE' then
                changed_tenant := new.tenant_id; changed_repository := new.repository_id;
                changed_ref := new.refname; new_oid := new.target_oid;
                new_symbolic := new.symbolic_target;
            end if;
            select repository_pk, ref_generation + 1 into strict repo_pk, next_generation
            from grit_repositories
            where tenant_id = changed_tenant and repository_id = changed_repository
              and deleted_at is null;
            insert into grit_ref_change_journal
                (repository_pk, generation, refname, old_target_oid, old_symbolic_target,
                 new_target_oid, new_symbolic_target)
            values (repo_pk, next_generation, changed_ref,
                old_oid, old_symbolic, new_oid, new_symbolic)
            on conflict (repository_pk, generation, refname) do update
            set new_target_oid = excluded.new_target_oid,
                new_symbolic_target = excluded.new_symbolic_target;
            if tg_op = 'DELETE' then return old; else return new; end if;
        end $$",
    "drop trigger if exists grit_ref_change_journal_trigger on grit_refs",
    "create trigger grit_ref_change_journal_trigger after insert or update or delete on grit_refs
        for each row execute function grit_journal_ref_change()",
    "create or replace function grit_journal_config_change() returns trigger language plpgsql as $$
        declare repo_pk bigint; next_generation bigint; changed_tenant text; changed_repository text;
            changed_key text; previous_value text; replacement_value text;
        begin
            if tg_op = 'UPDATE' and old.key is not distinct from new.key
                and old.value is not distinct from new.value then
                return new;
            end if;
            if tg_op <> 'INSERT' then
                changed_tenant := old.tenant_id; changed_repository := old.repository_id;
                changed_key := old.key; previous_value := old.value;
            end if;
            if tg_op <> 'DELETE' then
                changed_tenant := new.tenant_id; changed_repository := new.repository_id;
                changed_key := new.key; replacement_value := new.value;
            end if;
            select repository_pk, config_generation + 1 into strict repo_pk, next_generation
            from grit_repositories
            where tenant_id = changed_tenant and repository_id = changed_repository
              and deleted_at is null;
            insert into grit_config_change_journal
                (repository_pk, generation, key, old_value, new_value)
            values (repo_pk, next_generation, changed_key, previous_value, replacement_value)
            on conflict (repository_pk, generation, key) do update
            set new_value = excluded.new_value;
            if tg_op = 'DELETE' then return old; else return new; end if;
        end $$",
    "drop trigger if exists grit_config_change_journal_trigger on grit_config",
    "create trigger grit_config_change_journal_trigger after insert or update or delete on grit_config
        for each row execute function grit_journal_config_change()",
    "alter sequence grit_migration_session_id_seq
        owned by grit_migration_sessions.migration_id",
    "create index if not exists grit_migration_sessions_claim_idx
        on grit_migration_sessions (state, phase, lease_expires_at, migration_id)",
    "create index if not exists grit_migration_sessions_destination_idx
        on grit_migration_sessions (destination_repository_pk, migration_id)",
    "create sequence if not exists grit_migration_verification_report_id_seq",
    "create table if not exists grit_migration_verification_reports (
        report_id bigint primary key default nextval('grit_migration_verification_report_id_seq'),
        migration_id bigint not null,
        report_generation bigint not null,
        mode smallint not null,
        source_ref_generation bigint not null,
        source_history_generation bigint not null,
        source_config_generation bigint not null,
        destination_ref_generation bigint not null,
        destination_history_generation bigint not null,
        destination_config_generation bigint not null,
        passed boolean not null,
        created_at timestamptz not null,
        constraint grit_migration_verification_report_session_fk foreign key (migration_id)
            references grit_migration_sessions (migration_id) on delete cascade,
        check (report_id > 0),
        check (report_generation > 0),
        check (mode in (1, 2)),
        check (source_ref_generation >= 0 and source_history_generation >= 0
            and source_config_generation >= 0),
        check (destination_ref_generation >= 0 and destination_history_generation >= 0
            and destination_config_generation >= 0),
        unique (migration_id, report_generation)
    )",
    "alter table grit_migration_verification_reports
        add column if not exists report_generation bigint",
    "alter table grit_migration_verification_reports
        add column if not exists destination_ref_generation bigint",
    "alter table grit_migration_verification_reports
        add column if not exists destination_history_generation bigint",
    "alter table grit_migration_verification_reports
        add column if not exists destination_config_generation bigint",
    "create unique index if not exists grit_migration_verification_report_generation_idx
        on grit_migration_verification_reports (migration_id, report_generation)
        where report_generation is not null",
    "create table if not exists grit_migration_verification_checks (
        report_id bigint not null,
        check_kind smallint not null,
        result smallint not null,
        source_count bigint,
        destination_count bigint,
        mismatch_count bigint,
        detail text,
        primary key (report_id, check_kind),
        constraint grit_migration_verification_check_report_fk foreign key (report_id)
            references grit_migration_verification_reports (report_id) on delete cascade,
        check (check_kind between 1 and 9),
        check (result between 1 and 3),
        check (source_count is null or source_count >= 0),
        check (destination_count is null or destination_count >= 0),
        check (mismatch_count is null or mismatch_count >= 0)
    )",
    "create table if not exists grit_repository_routes (
        source_repository_pk bigint primary key,
        active_repository_pk bigint not null,
        destination_repository_pk bigint not null,
        migration_id bigint not null unique,
        report_id bigint not null unique,
        destination_ref_generation bigint not null,
        destination_history_generation bigint not null,
        destination_config_generation bigint not null,
        cutover_at timestamptz not null,
        rollback_deadline timestamptz not null,
        rolled_back_at timestamptz,
        route_generation bigint not null default 1,
        constraint grit_repository_routes_source_fk foreign key (source_repository_pk)
            references grit_repositories (repository_pk) on delete cascade,
        constraint grit_repository_routes_active_fk foreign key (active_repository_pk)
            references grit_repositories (repository_pk),
        constraint grit_repository_routes_destination_fk foreign key (destination_repository_pk)
            references grit_repositories (repository_pk),
        constraint grit_repository_routes_migration_fk foreign key (migration_id)
            references grit_migration_sessions (migration_id) on delete cascade,
        constraint grit_repository_routes_report_fk foreign key (report_id)
            references grit_migration_verification_reports (report_id),
        check (source_repository_pk <> destination_repository_pk),
        check (active_repository_pk in (source_repository_pk, destination_repository_pk)),
        check (destination_ref_generation >= 0 and destination_history_generation >= 0
            and destination_config_generation >= 0),
        check (rollback_deadline > cutover_at),
        check (route_generation > 0),
        check ((active_repository_pk = source_repository_pk) = (rolled_back_at is not null))
    )",
    "alter table grit_repository_routes
        add column if not exists route_generation bigint not null default 1",
    "alter sequence grit_migration_verification_report_id_seq
        owned by grit_migration_verification_reports.report_id",
    "create index if not exists grit_migration_verification_reports_migration_idx
        on grit_migration_verification_reports (migration_id, report_id desc)",
    "create sequence if not exists grit_pack_maintenance_job_id_seq",
    "create sequence if not exists grit_pack_maintenance_fencing_token_seq",
    "create table if not exists grit_pack_maintenance_jobs (
        job_id bigint primary key default nextval('grit_pack_maintenance_job_id_seq'),
        repository_pk bigint not null,
        ref_generation bigint not null,
        history_generation bigint not null,
        phase smallint not null,
        min_pack_count integer not null,
        max_loose_count bigint not null,
        min_fragmented_bytes bigint not null,
        grace_seconds bigint not null,
        replacement_pack_checksum bytea,
        prune_after timestamptz,
        claimed_by text,
        fencing_token bigint not null default 0,
        lease_expires_at timestamptz,
        attempt_count integer not null default 0,
        failure_reason text,
        created_at timestamptz not null,
        updated_at timestamptz not null,
        constraint grit_pack_maintenance_repository_fk foreign key (repository_pk)
            references grit_repositories (repository_pk) on delete cascade,
        check (job_id > 0 and ref_generation >= 0 and history_generation >= 0),
        check (phase between 1 and 6),
        check (min_pack_count > 0 and max_loose_count >= 0 and min_fragmented_bytes >= 0),
        check (grace_seconds >= 0 and fencing_token >= 0 and attempt_count >= 0),
        check ((claimed_by is null) = (lease_expires_at is null)),
        check (claimed_by is null or fencing_token > 0),
        check ((phase = 6 and failure_reason is not null) or (phase <> 6 and failure_reason is null)),
        check ((phase = 1 and replacement_pack_checksum is null and prune_after is null)
            or phase = 6
            or (phase between 2 and 5 and replacement_pack_checksum is not null
                and prune_after is not null)),
        check (updated_at >= created_at)
    )",
    "create table if not exists grit_pack_maintenance_superseded (
        job_id bigint not null,
        pack_checksum bytea not null,
        index_checksum bytea not null,
        storage_backend text not null,
        storage_key text,
        object_count integer not null,
        size_bytes bigint not null,
        primary key (job_id, pack_checksum),
        constraint grit_pack_maintenance_superseded_job_fk foreign key (job_id)
            references grit_pack_maintenance_jobs (job_id) on delete cascade,
        check (object_count >= 0 and size_bytes >= 0),
        check ((storage_backend = 'database' and storage_key is null)
            or (storage_backend <> 'database' and storage_key is not null))
    )",
    "alter table grit_pack_maintenance_superseded
        add column if not exists index_checksum bytea",
    "alter table grit_pack_maintenance_superseded
        add column if not exists object_count integer",
    "create table if not exists grit_pack_maintenance_objects (
        job_id bigint not null,
        oid bytea not null,
        primary key (job_id, oid),
        constraint grit_pack_maintenance_object_job_fk foreign key (job_id)
            references grit_pack_maintenance_jobs (job_id) on delete cascade,
        check (octet_length(oid) in (20, 32))
    )",
    "create table if not exists grit_repository_retention_holds (
        repository_pk bigint not null,
        hold_key text not null,
        reason text not null,
        expires_at timestamptz not null,
        created_at timestamptz not null,
        primary key (repository_pk, hold_key),
        constraint grit_repository_retention_hold_repository_fk foreign key (repository_pk)
            references grit_repositories (repository_pk) on delete cascade,
        check (length(hold_key) between 1 and 256),
        check (length(reason) between 1 and 2048),
        check (expires_at > created_at)
    )",
    "alter sequence grit_pack_maintenance_job_id_seq
        owned by grit_pack_maintenance_jobs.job_id",
    "create index if not exists grit_pack_maintenance_claim_idx
        on grit_pack_maintenance_jobs (phase, lease_expires_at, job_id)",
    "create index if not exists grit_pack_maintenance_repository_idx
        on grit_pack_maintenance_jobs (repository_pk, job_id desc)",
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
    "create index if not exists grit_commits_repo_history_idx
        on grit_commits (repository_pk, commit_time desc, commit_oid_bytes asc)",
    "create index if not exists grit_commit_parents_repo_page_idx
        on grit_commit_parents
            (repository_pk, commit_oid_bytes, parent_order, parent_oid_bytes)",
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
    /// Durable generation advanced by commit-index or parent-graph mutations.
    pub history_generation: u64,
    /// Durable generation advanced by repository-local config mutations.
    pub config_generation: u64,
    /// Timestamp when the repository row was created.
    pub created_at: OffsetDateTime,
    /// Timestamp when repository metadata was last changed.
    pub updated_at: OffsetDateTime,
    /// Timestamp when the repository was archived, if archived.
    pub archived_at: Option<OffsetDateTime>,
    /// Timestamp when the repository was soft-deleted, if retained.
    pub deleted_at: Option<OffsetDateTime>,
}

/// Maximum number of invalidation events claimed in one PostgreSQL call.
pub const MAX_CACHE_OUTBOX_CLAIM_BATCH: usize = 1_024;

/// Typed durable mutation represented by one cache invalidation outbox event.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PgCacheInvalidationKind {
    /// The repository's visible ref generation advanced.
    RefGenerationAdvanced {
        /// New durable ref generation.
        generation: u64,
    },
    /// The repository's config generation advanced.
    ConfigGenerationAdvanced {
        /// New durable config generation.
        generation: u64,
    },
    /// The repository's indexed-history generation advanced.
    HistoryGenerationAdvanced {
        /// New durable history generation.
        generation: u64,
    },
    /// A repository and all of its durable repository-scoped rows were deleted.
    RepositoryDeleted,
}

/// One durable cache invalidation outbox event.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PgCacheInvalidationEvent {
    /// Monotonic database-assigned event identifier.
    pub event_id: u64,
    /// Immutable numeric repository identity, retained after repository deletion.
    pub repository_pk: RepositoryPk,
    /// Tenant snapshot needed by legacy name-scoped cache consumers.
    pub tenant: TenantId,
    /// Repository-name snapshot at the time of the mutation.
    pub repository: RepositoryId,
    /// Typed durable mutation and its affected generation.
    pub kind: PgCacheInvalidationKind,
}

/// Explicit limits and lease timestamps for one outbox claim call.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PgCacheOutboxClaimOptions {
    /// Stable identity of the worker claiming events.
    pub worker_id: String,
    /// Caller-observed time used to decide whether prior leases and retry delays expired.
    pub observed_at: OffsetDateTime,
    /// Caller-selected exclusive end of the new lease.
    pub lease_expires_at: OffsetDateTime,
    /// Maximum number of events returned, in `1..=`[`MAX_CACHE_OUTBOX_CLAIM_BATCH`].
    pub max_events: usize,
    /// Maximum attempts permitted for any selected event; must be positive.
    pub max_attempts: u32,
}

/// One outbox event leased to a specific worker and claim token.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PgCacheOutboxClaim {
    /// Claimed durable invalidation event.
    pub event: PgCacheInvalidationEvent,
    /// Worker identity that owns this lease.
    pub worker_id: String,
    /// Fresh monotonic token fencing every preceding claim of the event.
    pub claim_token: u64,
    /// Exclusive end of this claim's lease.
    pub lease_expires_at: OffsetDateTime,
    /// Number of times this event has been claimed, including this claim.
    pub attempts: u32,
}

/// Stable phase of a resumable repository migration.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PgMigrationPhase {
    /// Validate the source, destination, and migration inputs.
    Preflight,
    /// Capture the source state that bounds the initial copy.
    Snapshot,
    /// Copy immutable payloads and their repository metadata.
    BulkTransfer,
    /// The resumable initial migration finished successfully.
    InitialCopyComplete,
    /// Apply bounded changes observed after the immutable initial snapshot.
    CatchUp,
    /// Compare the caught-up source and destination durable state.
    Verification,
    /// A persisted verification report authorizes a coordinated cutover.
    ReadyForCutover,
    /// The source logical route points at the destination repository.
    CutOver,
    /// The source logical route was restored during the rollback window.
    RolledBack,
}

impl PgMigrationPhase {
    const fn code(self) -> i16 {
        match self {
            Self::Preflight => 1,
            Self::Snapshot => 2,
            Self::BulkTransfer => 3,
            Self::InitialCopyComplete => 4,
            Self::CatchUp => 5,
            Self::Verification => 6,
            Self::ReadyForCutover => 7,
            Self::CutOver => 8,
            Self::RolledBack => 9,
        }
    }

    fn from_code(code: i16) -> Result<Self> {
        match code {
            1 => Ok(Self::Preflight),
            2 => Ok(Self::Snapshot),
            3 => Ok(Self::BulkTransfer),
            4 => Ok(Self::InitialCopyComplete),
            5 => Ok(Self::CatchUp),
            6 => Ok(Self::Verification),
            7 => Ok(Self::ReadyForCutover),
            8 => Ok(Self::CutOver),
            9 => Ok(Self::RolledBack),
            _ => Err(Error::Backend(format!(
                "invalid migration phase code {code}"
            ))),
        }
    }
}

/// Durable lifecycle state of a resumable migration session.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PgMigrationState {
    /// The session can be claimed and resumed.
    Active,
    /// The session stopped because migration work failed.
    Failed,
    /// An operator or caller cancelled the session.
    Cancelled,
}

impl PgMigrationState {
    const fn code(self) -> i16 {
        match self {
            Self::Active => 1,
            Self::Failed => 2,
            Self::Cancelled => 3,
        }
    }

    fn from_code(code: i16) -> Result<Self> {
        match code {
            1 => Ok(Self::Active),
            2 => Ok(Self::Failed),
            3 => Ok(Self::Cancelled),
            _ => Err(Error::Backend(format!(
                "invalid migration state code {code}"
            ))),
        }
    }
}

/// Typed source identity for a resumable migration.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PgMigrationSource {
    /// Another repository stored in the same PostgreSQL control plane.
    Repository(RepositoryPk),
    /// A caller-defined external source descriptor.
    External(String),
}

/// Immutable or applied source position for incremental catch-up.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PgMigrationSourceToken {
    /// Generation tuple read from a PostgreSQL source repository.
    Postgres {
        /// Ref generation at this source position.
        ref_generation: u64,
        /// Commit-history generation at this source position.
        history_generation: u64,
        /// Config generation at this source position.
        config_generation: u64,
    },
    /// Opaque stable position supplied by an external source adapter.
    External(String),
}

/// Position within one generation-keyed migration journal.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PgMigrationJournalCursor {
    /// Generation containing the last applied journal row.
    pub generation: u64,
    /// Bytewise-ordered ref name or config key of the last applied row.
    pub key: String,
}

/// One metadata or immutable-payload prerequisite in a bounded catch-up batch.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PgMigrationCatchUpOperation {
    /// Copy the object identified by `oid` through an existing object-transfer API.
    Object { oid: ObjectId },
    /// Copy the pack identified by `checksum` through an existing pack-transfer API.
    Pack { checksum: ObjectId },
    /// Apply a source ref transition with an expected-old compare-and-swap guard.
    Ref {
        /// Source ref generation containing the transition.
        generation: u64,
        /// Full ref name.
        name: String,
        /// Value expected at the destination before applying the transition.
        expected: Option<StoredRef>,
        /// Source value after the transition, or `None` for deletion.
        value: Option<StoredRef>,
    },
    /// Apply a config transition with an expected-old compare-and-swap guard.
    Config {
        /// Source config generation containing the transition.
        generation: u64,
        /// Repository-local config key.
        key: String,
        /// Value expected at the destination before applying the transition.
        expected: Option<String>,
        /// Source value after the transition, or `None` for deletion.
        value: Option<String>,
    },
}

/// Typed source lag remaining after a catch-up plan.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PgMigrationCatchUpLag {
    /// Remaining ref generations.
    pub ref_generations: u64,
    /// Remaining commit-history generations.
    pub history_generations: u64,
    /// Remaining config generations.
    pub config_generations: u64,
    /// Whether more bounded operations are already known to remain.
    pub more_operations: bool,
}

/// Caller policy for deciding whether another catch-up pass is required.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PgMigrationCatchUpPolicy {
    /// Largest acceptable remaining ref-generation lag.
    pub max_ref_generations: u64,
    /// Largest acceptable remaining commit-history-generation lag.
    pub max_history_generations: u64,
    /// Largest acceptable remaining config-generation lag.
    pub max_config_generations: u64,
}

impl PgMigrationCatchUpLag {
    /// Return whether this lag is within `policy` and no known operations remain.
    #[must_use]
    pub const fn is_within(self, policy: PgMigrationCatchUpPolicy) -> bool {
        !self.more_operations
            && self.ref_generations <= policy.max_ref_generations
            && self.history_generations <= policy.max_history_generations
            && self.config_generations <= policy.max_config_generations
    }
}

/// Explicit bounds and observation time for one catch-up planning call.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PgMigrationCatchUpOptions {
    /// Maximum operations returned, in `1..=`[`MAX_MIGRATION_CATCH_UP_BATCH`].
    pub max_operations: usize,
    /// Caller-observed time used to validate the migration lease.
    pub observed_at: OffsetDateTime,
}

/// Bounded read-only plan for one incremental catch-up pass.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PgMigrationCatchUpBatch {
    /// Migration session that produced this batch.
    pub migration_id: u64,
    /// Fencing token that must still own the session when applying.
    pub fencing_token: u64,
    /// Durable source position before this batch.
    pub from: PgMigrationSourceToken,
    /// Ref-journal position paired with `from`, when its generation is only partly applied.
    pub from_ref_cursor: Option<PgMigrationJournalCursor>,
    /// Config-journal position paired with `from`, when its generation is only partly applied.
    pub from_config_cursor: Option<PgMigrationJournalCursor>,
    /// Source position safely reached after all operations are applied.
    pub apply_through: PgMigrationSourceToken,
    /// Ref-journal position after applying this batch, if a generation remains partial.
    pub apply_through_ref_cursor: Option<PgMigrationJournalCursor>,
    /// Config-journal position after applying this batch, if a generation remains partial.
    pub apply_through_config_cursor: Option<PgMigrationJournalCursor>,
    /// Source position observed after planning.
    pub source_observed: PgMigrationSourceToken,
    /// Bounded operations without whole object or pack payloads.
    pub operations: Vec<PgMigrationCatchUpOperation>,
    /// Typed lag remaining after `apply_through`.
    pub remaining_lag: PgMigrationCatchUpLag,
}

/// Outcome of atomically applying and checkpointing a catch-up batch.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PgMigrationCatchUpApplyResult {
    /// Metadata changes and the batch checkpoint were committed.
    Applied,
    /// The read-only plan contained no durable operations, so nothing was written.
    NoChanges,
    /// A newer worker owns the session or its lease expired.
    ClaimLost,
    /// Destination state disagreed with an expected-old ref or config value.
    Diverged {
        /// Ref name or config key that diverged.
        key: String,
    },
    /// An object or pack prerequisite has not yet been copied to the destination.
    MissingPrerequisite {
        /// Missing object ID or pack checksum.
        oid: ObjectId,
    },
}

/// Verification depth for an internal PostgreSQL migration.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PgMigrationVerificationMode {
    /// Compare every supported manifest and graph projection.
    Full,
    /// Compare durable manifests while marking graph and sample checks unsupported.
    Manifest,
}

impl PgMigrationVerificationMode {
    const fn code(self) -> i16 {
        match self {
            Self::Full => 1,
            Self::Manifest => 2,
        }
    }
}

/// Stable verification check recorded in a migration report.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PgMigrationVerificationCheckKind {
    /// Repository hash algorithms match.
    HashAlgorithm,
    /// Ref names and direct or symbolic targets match exactly.
    Refs,
    /// The direct or symbolic `HEAD` value matches exactly.
    DefaultBranch,
    /// Persisted peeled-tag values match when the storage model exposes them.
    PeeledTags,
    /// Object identifiers match exactly.
    ObjectManifest,
    /// Pack checksums match exactly.
    PackManifest,
    /// Commits and ordered parent edges match exactly.
    CommitGraph,
    /// Repository configuration, including default-branch settings, matches exactly.
    Config,
    /// Caller-selected object identifiers exist with matching kinds.
    SampleObjects,
}

impl PgMigrationVerificationCheckKind {
    const fn code(self) -> i16 {
        match self {
            Self::HashAlgorithm => 1,
            Self::Refs => 2,
            Self::DefaultBranch => 3,
            Self::PeeledTags => 4,
            Self::ObjectManifest => 5,
            Self::PackManifest => 6,
            Self::CommitGraph => 7,
            Self::Config => 8,
            Self::SampleObjects => 9,
        }
    }
}

/// Typed result of one persisted verification check.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PgMigrationVerificationResult {
    /// The compared projections match.
    Passed,
    /// The compared projections differ.
    Failed,
    /// The selected verification mode does not perform this check.
    Unsupported,
}

impl PgMigrationVerificationResult {
    const fn code(self) -> i16 {
        match self {
            Self::Passed => 1,
            Self::Failed => 2,
            Self::Unsupported => 3,
        }
    }
}

/// One immutable check row in a migration verification report.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PgMigrationVerificationCheck {
    /// Stable check identity.
    pub kind: PgMigrationVerificationCheckKind,
    /// Typed check outcome.
    pub result: PgMigrationVerificationResult,
    /// Source row count, when meaningful.
    pub source_count: Option<u64>,
    /// Destination row count, when meaningful.
    pub destination_count: Option<u64>,
    /// Number of bounded mismatches found.
    pub mismatch_count: Option<u64>,
    /// Short durable explanation for a failure or unsupported check.
    pub detail: Option<String>,
}

/// Immutable verification report authorizing cutover when `passed` is true.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PgMigrationVerificationReport {
    /// Database-assigned immutable report identifier.
    pub report_id: u64,
    /// Migration owning this report.
    pub migration_id: u64,
    /// Monotonic report generation within the migration.
    pub report_generation: u64,
    /// Selected verification depth.
    pub mode: PgMigrationVerificationMode,
    /// Exact source generation token observed by the report.
    pub source_token: PgMigrationSourceToken,
    /// Exact destination generation tuple observed by the same snapshot.
    pub destination_token: PgMigrationSourceToken,
    /// Whether every required check passed.
    pub passed: bool,
    /// Explicit caller-supplied report timestamp.
    pub created_at: OffsetDateTime,
    /// Persisted typed checks.
    pub checks: Vec<PgMigrationVerificationCheck>,
}

/// Inputs for one bounded internal PostgreSQL verification.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PgMigrationVerificationOptions {
    /// Verification depth.
    pub mode: PgMigrationVerificationMode,
    /// Explicit snapshot and report timestamp.
    pub observed_at: OffsetDateTime,
    /// Caller-selected object identifiers, bounded to 128 entries.
    pub sample_oids: Vec<ObjectId>,
}

/// Options for atomically publishing a verified destination route.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PgMigrationCutoverOptions {
    /// Explicit cutover timestamp within the current claim lease.
    pub observed_at: OffsetDateTime,
    /// Exclusive rollback deadline after `observed_at`.
    pub rollback_deadline: OffsetDateTime,
}

/// Options for restoring the source route during the rollback window.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PgMigrationRollbackOptions {
    /// Migration whose route must be restored.
    pub migration_id: u64,
    /// Explicit rollback timestamp no later than the stored deadline.
    pub observed_at: OffsetDateTime,
}

/// Idempotent cutover or rollback outcome.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PgMigrationRouteOutcome {
    /// The route and migration phase changed atomically.
    Applied,
    /// The requested route state was already installed.
    AlreadyApplied,
    /// The claim, verification report, source position, or rollback window is no longer valid.
    PreconditionsChanged,
}

/// Durable phase of a PostgreSQL pack-maintenance job.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PgPackMaintenancePhase {
    /// Candidate packs were selected; no replacement is published.
    Planned,
    /// A durable replacement covers every superseded indexed object.
    ReplacementPublished,
    /// Superseded SQL metadata was removed after its grace period.
    MetadataSwept,
    /// External bytes remain registered for the orphan sweeper.
    ExternalSweepPending,
    /// All job-owned maintenance work is complete.
    Complete,
    /// The job stopped with a durable failure reason.
    Failed,
}

impl PgPackMaintenancePhase {
    fn from_code(code: i16) -> Result<Self> {
        match code {
            1 => Ok(Self::Planned),
            2 => Ok(Self::ReplacementPublished),
            3 => Ok(Self::MetadataSwept),
            4 => Ok(Self::ExternalSweepPending),
            5 => Ok(Self::Complete),
            6 => Ok(Self::Failed),
            _ => Err(Error::Backend(format!(
                "invalid pack-maintenance phase {code}"
            ))),
        }
    }
}

/// Explicit fragmentation policy captured by a maintenance job.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PgPackMaintenancePolicy {
    /// Minimum pack count needed for selection.
    pub min_pack_count: u32,
    /// Largest acceptable loose-object count.
    pub max_loose_count: u64,
    /// Minimum total fragmented pack bytes.
    pub min_fragmented_bytes: u64,
}

/// Bounded repository-level fragmentation summary.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PgPackMaintenanceCandidate {
    /// Candidate repository identity.
    pub repository: RepositoryPk,
    /// Number of packs.
    pub pack_count: u64,
    /// Number of loose object rows.
    pub loose_count: u64,
    /// Total pack bytes.
    pub pack_bytes: u64,
}

/// Durable pack-maintenance job state.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PgPackMaintenanceJob {
    /// Database-assigned job identity.
    pub job_id: u64,
    /// Repository owned by the job.
    pub repository: RepositoryPk,
    /// Ref generation captured at planning.
    pub ref_generation: u64,
    /// History generation captured at planning.
    pub history_generation: u64,
    /// Current durable phase.
    pub phase: PgPackMaintenancePhase,
    /// Captured selection policy.
    pub policy: PgPackMaintenancePolicy,
    /// Grace interval applied when replacement is published.
    pub grace: Duration,
    /// Durable replacement checksum, when published.
    pub replacement: Option<ObjectId>,
    /// Earliest metadata prune time.
    pub prune_after: Option<OffsetDateTime>,
    /// Current worker, if leased.
    pub claimed_by: Option<String>,
    /// Current fencing token.
    pub fencing_token: u64,
    /// Lease expiry.
    pub lease_expires_at: Option<OffsetDateTime>,
    /// Claim attempts.
    pub attempt_count: u32,
    /// Last durable state-transition timestamp.
    pub updated_at: OffsetDateTime,
}

/// Explicit inputs for creating one generation-bound maintenance job.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PgPackMaintenanceCreateOptions {
    /// Repository to maintain.
    pub repository: RepositoryPk,
    /// Captured fragmentation policy.
    pub policy: PgPackMaintenancePolicy,
    /// Nonnegative superseded-pack grace interval.
    pub grace: Duration,
    /// Caller-supplied creation timestamp.
    pub created_at: OffsetDateTime,
}

/// Explicit worker lease inputs.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PgPackMaintenanceClaimOptions {
    /// Stable worker identity.
    pub worker_id: String,
    /// Caller-observed reclaim time.
    pub observed_at: OffsetDateTime,
    /// Exclusive new lease expiry.
    pub lease_expires_at: OffsetDateTime,
    /// Maximum jobs, bounded to [`MAX_PACK_MAINTENANCE_BATCH`].
    pub max_jobs: usize,
}

/// Fenced maintenance job lease.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PgPackMaintenanceClaim {
    /// Claimed job snapshot.
    pub job: PgPackMaintenanceJob,
    /// Owning worker identity.
    pub worker_id: String,
    /// Fresh fencing token.
    pub fencing_token: u64,
    /// Exclusive lease expiry.
    pub lease_expires_at: OffsetDateTime,
}

/// Typed publication or prune result.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PgPackMaintenanceOutcome {
    /// State and metadata changed atomically.
    Applied,
    /// The requested phase was already reached.
    AlreadyApplied,
    /// A newer worker owns the job.
    ClaimLost,
    /// Retention or rollback policy currently blocks pruning.
    Blocked { reason: String },
    /// Pack/index metadata is inconsistent or replacement coverage is incomplete.
    Corrupt { detail: String },
}

/// Metadata-only consistency probe for one pack.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PgPackConsistencyProbe {
    /// Pack checksum.
    pub checksum: ObjectId,
    /// Whether object-count and index binding agree.
    pub index_binding_valid: bool,
    /// Whether external storage metadata has a non-empty key.
    pub external_key_metadata_valid: bool,
}

/// Maximum candidates, claims, or superseded packs accepted by one call.
pub const MAX_PACK_MAINTENANCE_BATCH: usize = 512;

/// Maximum operations returned by one catch-up planning call.
pub const MAX_MIGRATION_CATCH_UP_BATCH: usize = 512;

/// Cursor after the last fully durable object in bytewise object-ID order.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PgMigrationObjectCursor {
    /// Object identifier at the trusted boundary.
    pub oid: ObjectId,
}

/// Cursor after the last fully durable ref in bytewise name order.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PgMigrationRefCursor {
    /// Full source ref name at the trusted boundary.
    pub name: String,
}

/// Cursor after the last fully indexed tree in bytewise object-ID order.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PgMigrationTreeCursor {
    /// Tree identifier at the trusted boundary.
    pub oid: ObjectId,
}

/// Cursor after the last fully durable pack in bytewise checksum order.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PgMigrationPackCursor {
    /// Pack checksum at the trusted boundary.
    pub checksum: ObjectId,
}

/// Durable, monotonic progress recorded at a trusted migration boundary.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PgMigrationCheckpoint {
    /// Last fully durable object, if the phase has an object cursor.
    pub object: Option<PgMigrationObjectCursor>,
    /// Last fully durable ref name, if the phase has a ref cursor.
    pub reference: Option<PgMigrationRefCursor>,
    /// Last fully indexed tree, if the phase has a tree cursor.
    pub tree: Option<PgMigrationTreeCursor>,
    /// Last fully durable pack checksum, if the phase has a pack cursor.
    pub pack: Option<PgMigrationPackCursor>,
    /// Number of fully durable objects.
    pub completed_objects: u64,
    /// Number of fully durable refs.
    pub completed_refs: u64,
    /// Number of fully indexed trees.
    pub completed_trees: u64,
    /// Number of fully durable packs.
    pub completed_packs: u64,
    /// Number of fully durable payload bytes.
    pub completed_bytes: u64,
}

/// One durable resumable migration session.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PgMigrationSession {
    /// Database-assigned migration identifier.
    pub migration_id: u64,
    /// Caller-provided key used to make creation idempotent.
    pub idempotency_key: String,
    /// Stable source identity.
    pub source: PgMigrationSource,
    /// Destination repository receiving migrated data.
    pub destination_repository: RepositoryPk,
    /// Object hash algorithm shared by the source and destination.
    pub hash_algo: HashAlgo,
    /// Current resumable phase.
    pub phase: PgMigrationPhase,
    /// Current durable lifecycle state.
    pub state: PgMigrationState,
    /// Most recently issued fencing token, or zero before the first claim.
    pub fencing_token: u64,
    /// Current worker identity, if leased.
    pub claimed_by: Option<String>,
    /// Exclusive end of the current lease, if leased.
    pub lease_expires_at: Option<OffsetDateTime>,
    /// Number of claims issued for this session.
    pub attempt_count: u32,
    /// Latest trusted checkpoint.
    pub checkpoint: PgMigrationCheckpoint,
    /// Immutable source position captured before incremental catch-up.
    pub source_snapshot: Option<PgMigrationSourceToken>,
    /// Last source position atomically applied to the destination.
    pub last_applied: Option<PgMigrationSourceToken>,
    /// Partial ref-journal position associated with `last_applied`.
    pub last_applied_ref_cursor: Option<PgMigrationJournalCursor>,
    /// Partial config-journal position associated with `last_applied`.
    pub last_applied_config_cursor: Option<PgMigrationJournalCursor>,
    /// Failure or cancellation reason for a terminal session.
    pub terminal_reason: Option<String>,
    /// Caller-supplied creation timestamp.
    pub created_at: OffsetDateTime,
    /// Caller-supplied timestamp of the latest durable mutation.
    pub updated_at: OffsetDateTime,
}

/// Inputs used to create an idempotent resumable migration session.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PgMigrationCreateOptions {
    /// Stable non-empty idempotency key.
    pub idempotency_key: String,
    /// Stable source identity.
    pub source: PgMigrationSource,
    /// Destination repository receiving migrated data.
    pub destination_repository: RepositoryPk,
    /// Object hash algorithm shared by the source and destination.
    pub hash_algo: HashAlgo,
    /// Explicit creation and initial update timestamp.
    pub created_at: OffsetDateTime,
}

/// Explicit worker and lease timestamps for claiming one migration session.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PgMigrationClaimOptions {
    /// Stable non-empty worker identity.
    pub worker_id: String,
    /// Caller-observed time used to expire an earlier lease.
    pub observed_at: OffsetDateTime,
    /// Caller-selected exclusive end of the new lease.
    pub lease_expires_at: OffsetDateTime,
    /// Maximum sessions returned, in `1..=`[`MAX_MIGRATION_CLAIM_BATCH`].
    pub max_sessions: usize,
}

/// A resumable migration lease fenced from every earlier claim.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PgMigrationClaim {
    /// Latest durable session state at claim time.
    pub session: PgMigrationSession,
    /// Worker identity owning this lease.
    pub worker_id: String,
    /// Fresh monotonic fencing token.
    pub fencing_token: u64,
    /// Exclusive end of this lease.
    pub lease_expires_at: OffsetDateTime,
}

/// Outcome of an owned migration-session mutation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PgMigrationClaimResult {
    /// The requested durable mutation was applied.
    Applied,
    /// The session is no longer owned by this claim or its lease expired.
    ClaimLost,
    /// The session already failed, was cancelled, or completed its initial copy.
    Terminal,
}

/// Maximum migration sessions leased by one PostgreSQL claim call.
pub const MAX_MIGRATION_CLAIM_BATCH: usize = 256;

/// Idempotent outcome of acknowledging, releasing, or retrying an outbox claim.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PgCacheOutboxClaimResult {
    /// The requested state transition was applied.
    Applied,
    /// An acknowledgement was already durably applied and removed the event.
    AlreadyAcknowledged,
    /// The event is already available without a lease.
    AlreadyReleased,
    /// A newer or different worker claim owns the event, so no mutation was made.
    ClaimLost,
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

/// Maximum number of commits returned by one PostgreSQL history-page request.
pub const MAX_COMMIT_HISTORY_PAGE_SIZE: usize = 256;

/// Maximum number of parent edges returned by one PostgreSQL history-page request.
pub const MAX_COMMIT_HISTORY_PARENT_EDGES: usize = 8_192;

/// Stable continuation key for repository-wide indexed-commit time pagination.
///
/// The cursor is bound to both an immutable repository row and the commit-history generation
/// observed for the preceding page. It cannot be reused after repository deletion/recreation,
/// against another repository, or after a commit-index or parent-graph mutation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PgCommitHistoryCursor {
    /// Immutable numeric repository identity observed by the page query.
    pub repository_pk: RepositoryPk,
    /// Durable commit-history generation observed by the page query.
    pub history_generation: u64,
    /// Commit time of the last item returned by the preceding page.
    pub commit_time: i64,
    /// Object ID of the last item returned by the preceding page.
    pub oid: ObjectId,
}

/// Explicit bounds and continuation key for one PostgreSQL commit-history page.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PgCommitHistoryOptions {
    /// Maximum commits returned, in `1..=`[`MAX_COMMIT_HISTORY_PAGE_SIZE`].
    pub page_size: usize,
    /// Maximum parent edges transferred, in `1..=`[`MAX_COMMIT_HISTORY_PARENT_EDGES`].
    pub max_parent_edges: usize,
    /// Exclusive continuation key returned by a preceding page.
    pub cursor: Option<PgCommitHistoryCursor>,
}

/// One bounded page of repository-wide indexed commits ordered by committer time.
///
/// This is a time-keyset listing of every commit present in the repository index. It does not
/// claim reachability from a ref or topological traversal semantics.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PgCommitHistoryPage {
    /// Indexed commits ordered by `(commit_time DESC, raw_oid ASC)`.
    pub commits: Vec<IndexedCommit>,
    /// Exclusive key for the next page, or `None` when this page is exhausted.
    pub next_cursor: Option<PgCommitHistoryCursor>,
    /// Whether no later row exists after the commits in this page.
    pub exhausted: bool,
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

    /// Create or read the migration session identified by an idempotency key.
    ///
    /// Reusing a key with a different source, destination, or hash algorithm is rejected. The
    /// destination and an internal source must name live repository rows, and the destination's
    /// stored hash algorithm must match `options.hash_algo`.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Backend`] for invalid or conflicting options, or a SQLx error when the
    /// durable session cannot be read or created.
    pub async fn create_migration_session(
        &self,
        options: PgMigrationCreateOptions,
    ) -> Result<PgMigrationSession> {
        validate_migration_create_options(&options)?;
        let mut tx = self.pool.begin().await?;
        let destination_hash = sqlx::query_scalar::<_, String>(
            "select hash_algo from grit_repositories
             where repository_pk = $1 and deleted_at is null
             for key share",
        )
        .bind(options.destination_repository.get())
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| {
            Error::Backend("migration destination repository does not exist".to_owned())
        })?;
        if destination_hash != options.hash_algo.name() {
            return Err(Error::Backend(
                "migration destination hash algorithm does not match".to_owned(),
            ));
        }
        let (source_repository_pk, source_external_id) = match &options.source {
            PgMigrationSource::Repository(repository_pk) => {
                if *repository_pk == options.destination_repository {
                    return Err(Error::Backend(
                        "migration source and destination repositories must differ".to_owned(),
                    ));
                }
                let source_hash = sqlx::query_scalar::<_, String>(
                    "select hash_algo from grit_repositories
                     where repository_pk = $1 and deleted_at is null
                     for key share",
                )
                .bind(repository_pk.get())
                .fetch_optional(&mut *tx)
                .await?
                .ok_or_else(|| {
                    Error::Backend("migration source repository does not exist".to_owned())
                })?;
                if source_hash != options.hash_algo.name() {
                    return Err(Error::Backend(
                        "migration source hash algorithm does not match".to_owned(),
                    ));
                }
                (Some(repository_pk.get()), None)
            }
            PgMigrationSource::External(identity) => (None, Some(identity.as_str())),
        };
        let inserted = sqlx::query(
            "insert into grit_migration_sessions
                (idempotency_key, source_repository_pk, source_external_id,
                 destination_repository_pk, hash_algo, phase, state, created_at, updated_at)
             values ($1, $2, $3, $4, $5, $6, $7, $8, $8)
             on conflict (idempotency_key) do nothing
             returning *",
        )
        .bind(&options.idempotency_key)
        .bind(source_repository_pk)
        .bind(source_external_id)
        .bind(options.destination_repository.get())
        .bind(options.hash_algo.name())
        .bind(PgMigrationPhase::Preflight.code())
        .bind(PgMigrationState::Active.code())
        .bind(options.created_at)
        .fetch_optional(&mut *tx)
        .await?;
        let row = match inserted {
            Some(row) => row,
            None => {
                sqlx::query("select * from grit_migration_sessions where idempotency_key = $1")
                    .bind(&options.idempotency_key)
                    .fetch_one(&mut *tx)
                    .await?
            }
        };
        let session = row_to_migration_session(&row)?;
        if session.source != options.source
            || session.destination_repository != options.destination_repository
            || session.hash_algo != options.hash_algo
        {
            return Err(Error::Backend(
                "migration idempotency key belongs to different immutable inputs".to_owned(),
            ));
        }
        tx.commit().await?;
        Ok(session)
    }

    /// Read one durable migration session by its database-assigned identifier.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Backend`] when `migration_id` exceeds PostgreSQL's integer range or a
    /// stored row is invalid, and propagates SQLx read failures.
    pub async fn read_migration_session(
        &self,
        migration_id: u64,
    ) -> Result<Option<PgMigrationSession>> {
        let migration_id = positive_u64_to_i64(migration_id, "migration id")?;
        let row = sqlx::query("select * from grit_migration_sessions where migration_id = $1")
            .bind(migration_id)
            .fetch_optional(&self.pool)
            .await?;
        row.as_ref().map(row_to_migration_session).transpose()
    }

    /// Claim a bounded oldest-first batch of active migration sessions.
    ///
    /// PostgreSQL skips rows locked by other workers. Expired leases may be reclaimed, and every
    /// claim receives a fresh database fencing token. Failed and cancelled sessions are never
    /// selected; phase four is deliberately reclaimable for the explicit catch-up transition.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Backend`] for invalid options or stored rows, and propagates SQLx failures.
    pub async fn claim_migration_sessions(
        &self,
        options: PgMigrationClaimOptions,
    ) -> Result<Vec<PgMigrationClaim>> {
        validate_migration_claim_options(&options)?;
        let max_sessions = i64::try_from(options.max_sessions)
            .map_err(|_| Error::Backend("migration claim bound exceeds i64".to_owned()))?;
        let rows = sqlx::query(
            "with candidates as (
                 select migration_id from grit_migration_sessions
                 where state = 1 and phase <= 7
                   and updated_at <= $2
                   and (claimed_by is null or lease_expires_at <= $2)
                   and attempt_count < 2147483647
                 order by phase, migration_id
                 limit $4 for update skip locked
             )
             update grit_migration_sessions session
             set claimed_by = $1,
                 fencing_token = nextval('grit_migration_fencing_token_seq'),
                 lease_expires_at = $3,
                 attempt_count = session.attempt_count + 1,
                 updated_at = $2
             from candidates
             where session.migration_id = candidates.migration_id
             returning session.*",
        )
        .bind(&options.worker_id)
        .bind(options.observed_at)
        .bind(options.lease_expires_at)
        .bind(max_sessions)
        .fetch_all(&self.pool)
        .await?;
        let mut claims = rows
            .iter()
            .map(|row| {
                let session = row_to_migration_session(row)?;
                migration_claim_from_session(session)
            })
            .collect::<Result<Vec<_>>>()?;
        claims.sort_unstable_by_key(|claim| claim.session.migration_id);
        Ok(claims)
    }

    /// Persist a monotonic checkpoint while retaining the current lease.
    ///
    /// `phase` must equal the stored phase or its immediate successor. Entering
    /// [`PgMigrationPhase::InitialCopyComplete`] releases the lease and makes the session
    /// terminal within this initial-copy control plane.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Backend`] for invalid timestamps, cursors, counters, or claims, and
    /// propagates SQLx failures.
    pub async fn checkpoint_migration_session(
        &self,
        claim: &PgMigrationClaim,
        phase: PgMigrationPhase,
        checkpoint: &PgMigrationCheckpoint,
        observed_at: OffsetDateTime,
    ) -> Result<PgMigrationClaimResult> {
        validate_migration_claim(claim, observed_at)?;
        validate_migration_checkpoint(checkpoint, claim.session.hash_algo)?;
        let migration_id = positive_u64_to_i64(claim.session.migration_id, "migration id")?;
        let fencing_token = positive_u64_to_i64(claim.fencing_token, "migration fencing token")?;
        let object = checkpoint
            .object
            .as_ref()
            .map(|cursor| cursor.oid.as_bytes());
        let reference = checkpoint
            .reference
            .as_ref()
            .map(|cursor| cursor.name.as_str());
        let tree = checkpoint.tree.as_ref().map(|cursor| cursor.oid.as_bytes());
        let pack = checkpoint
            .pack
            .as_ref()
            .map(|cursor| cursor.checksum.as_bytes());
        let counts = checkpoint_counts(checkpoint)?;
        let rows = sqlx::query(
            "update grit_migration_sessions
             set phase = $5, last_object_oid = $6, last_ref_name = $7,
                 last_tree_oid = $8, last_pack_checksum = $9,
                 completed_objects = $10, completed_refs = $11,
                 completed_trees = $12, completed_packs = $13, completed_bytes = $14,
                 claimed_by = case when $5 = 4 then null else claimed_by end,
                 lease_expires_at = case when $5 = 4 then null else lease_expires_at end,
                 updated_at = $4
             where migration_id = $1 and claimed_by = $2 and fencing_token = $3
               and state = 1 and phase < 4 and lease_expires_at > $4 and updated_at <= $4
               and ($5 = phase or $5 = phase + 1)
               and (($6::bytea is null and last_object_oid is null)
                    or ($6 is not null and (last_object_oid is null or last_object_oid <= $6)))
               and (($7::text is null and last_ref_name is null)
                    or ($7 is not null and (last_ref_name is null
                        or last_ref_name collate \"C\" <= $7 collate \"C\")))
               and (($8::bytea is null and last_tree_oid is null)
                    or ($8 is not null and (last_tree_oid is null or last_tree_oid <= $8)))
               and (($9::bytea is null and last_pack_checksum is null)
                    or ($9 is not null and (last_pack_checksum is null or last_pack_checksum <= $9)))
               and completed_objects <= $10 and completed_refs <= $11
               and completed_trees <= $12 and completed_packs <= $13
               and completed_bytes <= $14",
        )
        .bind(migration_id)
        .bind(&claim.worker_id)
        .bind(fencing_token)
        .bind(observed_at)
        .bind(phase.code())
        .bind(object)
        .bind(reference)
        .bind(tree)
        .bind(pack)
        .bind(counts[0])
        .bind(counts[1])
        .bind(counts[2])
        .bind(counts[3])
        .bind(counts[4])
        .execute(&self.pool)
        .await?;
        self.migration_mutation_result(migration_id, rows.rows_affected())
            .await
    }

    /// Release an owned migration session for immediate reclaim.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Backend`] for an invalid claim or timestamp, and propagates SQLx failures.
    pub async fn release_migration_session(
        &self,
        claim: &PgMigrationClaim,
        observed_at: OffsetDateTime,
    ) -> Result<PgMigrationClaimResult> {
        self.finish_migration_session(claim, observed_at, PgMigrationFinish::Release)
            .await
    }

    /// Mark an owned migration session as failed with an actionable reason.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Backend`] for an invalid claim, timestamp, or reason, and propagates SQLx
    /// failures.
    pub async fn fail_migration_session(
        &self,
        claim: &PgMigrationClaim,
        observed_at: OffsetDateTime,
        reason: &str,
    ) -> Result<PgMigrationClaimResult> {
        self.finish_migration_session(claim, observed_at, PgMigrationFinish::Fail(reason))
            .await
    }

    /// Mark an owned migration session as cancelled with an operator reason.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Backend`] for an invalid claim, timestamp, or reason, and propagates SQLx
    /// failures.
    pub async fn cancel_migration_session(
        &self,
        claim: &PgMigrationClaim,
        observed_at: OffsetDateTime,
        reason: &str,
    ) -> Result<PgMigrationClaimResult> {
        self.finish_migration_session(claim, observed_at, PgMigrationFinish::Cancel(reason))
            .await
    }

    async fn finish_migration_session(
        &self,
        claim: &PgMigrationClaim,
        observed_at: OffsetDateTime,
        finish: PgMigrationFinish<'_>,
    ) -> Result<PgMigrationClaimResult> {
        validate_migration_claim(claim, observed_at)?;
        let migration_id = positive_u64_to_i64(claim.session.migration_id, "migration id")?;
        let fencing_token = positive_u64_to_i64(claim.fencing_token, "migration fencing token")?;
        let (state, reason) = match finish {
            PgMigrationFinish::Release => (PgMigrationState::Active, None),
            PgMigrationFinish::Fail(reason) => {
                validate_migration_terminal_reason(reason)?;
                (PgMigrationState::Failed, Some(reason))
            }
            PgMigrationFinish::Cancel(reason) => {
                validate_migration_terminal_reason(reason)?;
                (PgMigrationState::Cancelled, Some(reason))
            }
        };
        let rows = sqlx::query(
            "update grit_migration_sessions
             set state = $5, terminal_reason = $6, claimed_by = null,
                 lease_expires_at = null, updated_at = $4
             where migration_id = $1 and claimed_by = $2 and fencing_token = $3
               and state = 1 and lease_expires_at > $4 and updated_at <= $4",
        )
        .bind(migration_id)
        .bind(&claim.worker_id)
        .bind(fencing_token)
        .bind(observed_at)
        .bind(state.code())
        .bind(reason)
        .execute(&self.pool)
        .await?;
        self.migration_mutation_result(migration_id, rows.rows_affected())
            .await
    }

    async fn migration_mutation_result(
        &self,
        migration_id: i64,
        rows_affected: u64,
    ) -> Result<PgMigrationClaimResult> {
        if rows_affected == 1 {
            return Ok(PgMigrationClaimResult::Applied);
        }
        let row =
            sqlx::query("select phase, state from grit_migration_sessions where migration_id = $1")
                .bind(migration_id)
                .fetch_optional(&self.pool)
                .await?;
        let Some(row) = row else {
            return Ok(PgMigrationClaimResult::ClaimLost);
        };
        let state = PgMigrationState::from_code(row.try_get("state")?)?;
        if state != PgMigrationState::Active {
            return Ok(PgMigrationClaimResult::Terminal);
        }
        Ok(PgMigrationClaimResult::ClaimLost)
    }

    /// Advance an initial-copy-complete session into incremental catch-up exactly once.
    ///
    /// `snapshot` is immutable after this transition and is also installed as the initial
    /// last-applied source position. Phase four must be reclaimed before making this call.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Backend`] for a mismatched token, phase, claim, or timestamp, and
    /// propagates SQLx failures.
    pub async fn begin_migration_catch_up(
        &self,
        claim: &PgMigrationClaim,
        snapshot: &PgMigrationSourceToken,
        observed_at: OffsetDateTime,
    ) -> Result<PgMigrationClaimResult> {
        validate_migration_claim(claim, observed_at)?;
        if claim.session.phase != PgMigrationPhase::InitialCopyComplete {
            return Err(Error::Backend(
                "catch-up can begin only after the initial copy completes".to_owned(),
            ));
        }
        validate_migration_source_token(&claim.session.source, snapshot)?;
        let migration_id = positive_u64_to_i64(claim.session.migration_id, "migration id")?;
        let fencing_token = positive_u64_to_i64(claim.fencing_token, "migration fencing token")?;
        let token = migration_token_columns(snapshot)?;
        let rows = sqlx::query(
            "update grit_migration_sessions
             set phase = 5,
                 source_snapshot_ref_generation = $5,
                 source_snapshot_history_generation = $6,
                 source_snapshot_config_generation = $7,
                 source_snapshot_external_token = $8,
                 applied_ref_generation = $5, applied_history_generation = $6,
                 applied_config_generation = $7, applied_external_token = $8,
                 applied_ref_name = null, applied_config_key = null,
                 updated_at = $4
             where migration_id = $1 and claimed_by = $2 and fencing_token = $3
               and state = 1 and phase = 4 and lease_expires_at > $4 and updated_at <= $4
               and source_snapshot_ref_generation is null
               and source_snapshot_history_generation is null
               and source_snapshot_config_generation is null
               and source_snapshot_external_token is null
               and applied_ref_generation is null
               and applied_history_generation is null
               and applied_config_generation is null
               and applied_external_token is null",
        )
        .bind(migration_id)
        .bind(&claim.worker_id)
        .bind(fencing_token)
        .bind(observed_at)
        .bind(token.ref_generation)
        .bind(token.history_generation)
        .bind(token.config_generation)
        .bind(token.external)
        .execute(&self.pool)
        .await?;
        self.migration_mutation_result(migration_id, rows.rows_affected())
            .await
    }

    /// Plan a bounded read-only incremental catch-up batch for a PostgreSQL source.
    ///
    /// Object and pack operations contain identifiers only. Ref and config operations come from
    /// generation-keyed journals and include expected-old values for divergence detection. An
    /// empty result performs no durable write.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Backend`] for invalid bounds, stale claims, external sources, or corrupt
    /// journal rows, and propagates SQLx failures.
    pub async fn plan_migration_catch_up(
        &self,
        claim: &PgMigrationClaim,
        options: PgMigrationCatchUpOptions,
    ) -> Result<PgMigrationCatchUpBatch> {
        validate_migration_claim(claim, options.observed_at)?;
        if options.max_operations == 0 || options.max_operations > MAX_MIGRATION_CATCH_UP_BATCH {
            return Err(Error::Backend(format!(
                "catch-up batch size must be between 1 and {MAX_MIGRATION_CATCH_UP_BATCH}"
            )));
        }
        let migration_id = positive_u64_to_i64(claim.session.migration_id, "migration id")?;
        let mut tx = self.pool.begin().await?;
        sqlx::query("set transaction isolation level repeatable read, read only")
            .execute(&mut *tx)
            .await?;
        let row = sqlx::query(
            "select * from grit_migration_sessions
             where migration_id = $1 and claimed_by = $2 and fencing_token = $3
               and state = 1 and phase = 5 and lease_expires_at > $4 and updated_at <= $4",
        )
        .bind(migration_id)
        .bind(&claim.worker_id)
        .bind(positive_u64_to_i64(
            claim.fencing_token,
            "migration fencing token",
        )?)
        .bind(options.observed_at)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| Error::Backend("migration catch-up claim was lost".to_owned()))?;
        let session = row_to_migration_session(&row)?;
        let PgMigrationSource::Repository(source_repository) = session.source else {
            return Err(Error::Backend(
                "external catch-up requires an external source adapter".to_owned(),
            ));
        };
        let from = session.last_applied.clone().ok_or_else(|| {
            Error::Backend("migration catch-up has no applied source token".to_owned())
        })?;
        let (from_refs, from_history, from_config) = postgres_token_generations(&from)?;
        let source_row = sqlx::query(
            "select ref_generation, history_generation, config_generation
             from grit_repositories where repository_pk = $1 and deleted_at is null",
        )
        .bind(source_repository.get())
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| Error::Backend("migration source repository no longer exists".to_owned()))?;
        let observed_refs = nonnegative_i64_to_u64(
            source_row.try_get("ref_generation")?,
            "source ref generation",
        )?;
        let observed_history = nonnegative_i64_to_u64(
            source_row.try_get("history_generation")?,
            "source history generation",
        )?;
        let observed_config = nonnegative_i64_to_u64(
            source_row.try_get("config_generation")?,
            "source config generation",
        )?;
        if observed_refs < from_refs
            || observed_history < from_history
            || observed_config < from_config
        {
            return Err(Error::Backend(
                "migration source generations regressed".to_owned(),
            ));
        }
        let source_observed = PgMigrationSourceToken::Postgres {
            ref_generation: observed_refs,
            history_generation: observed_history,
            config_generation: observed_config,
        };
        let mut operations = Vec::new();
        let mut more = false;
        let mut through = (from_refs, from_history, from_config);
        let from_ref_cursor = session.last_applied_ref_cursor.clone();
        let from_config_cursor = session.last_applied_config_cursor.clone();
        let mut through_ref_cursor = from_ref_cursor.clone();
        let mut through_config_cursor = from_config_cursor.clone();

        let mut remaining = options.max_operations;
        let object_rows = sqlx::query(
            "select source.oid_bytes from grit_objects source
             left join grit_objects destination
               on destination.repository_pk = $2 and destination.oid_bytes = source.oid_bytes
             where source.repository_pk = $1 and destination.repository_pk is null
             order by source.oid_bytes limit $3",
        )
        .bind(source_repository.get())
        .bind(session.destination_repository.get())
        .bind(
            i64::try_from(remaining.saturating_add(1))
                .map_err(|_| Error::Backend("catch-up bound exceeds i64".to_owned()))?,
        )
        .fetch_all(&mut *tx)
        .await?;
        more |= object_rows.len() > remaining;
        for row in object_rows.iter().take(remaining) {
            operations.push(PgMigrationCatchUpOperation::Object {
                oid: ObjectId::from_bytes(&row.try_get::<Vec<u8>, _>("oid_bytes")?)?,
            });
        }
        remaining = options.max_operations - operations.len();
        if remaining > 0 {
            let pack_rows = sqlx::query(
                "select source.pack_checksum_bytes from grit_packs source
                 left join grit_packs destination on destination.repository_pk = $2
                   and destination.pack_checksum_bytes = source.pack_checksum_bytes
                 where source.repository_pk = $1 and destination.repository_pk is null
                 order by source.pack_checksum_bytes limit $3",
            )
            .bind(source_repository.get())
            .bind(session.destination_repository.get())
            .bind(
                i64::try_from(remaining.saturating_add(1))
                    .map_err(|_| Error::Backend("catch-up bound exceeds i64".to_owned()))?,
            )
            .fetch_all(&mut *tx)
            .await?;
            more |= pack_rows.len() > remaining;
            for row in pack_rows.iter().take(remaining) {
                operations.push(PgMigrationCatchUpOperation::Pack {
                    checksum: ObjectId::from_bytes(
                        &row.try_get::<Vec<u8>, _>("pack_checksum_bytes")?,
                    )?,
                });
            }
            remaining = options.max_operations - operations.len();
        }

        if remaining > 0 {
            let ref_limit = remaining;
            let ref_rows = sqlx::query(
                "select generation, refname, old_target_oid, old_symbolic_target,
                        new_target_oid, new_symbolic_target
                 from grit_ref_change_journal
                 where repository_pk = $1 and generation <= $3
                   and (generation > $2 or (generation = $2 and $4::text is not null
                       and refname collate \"C\" > $4 collate \"C\"))
                 order by generation, refname collate \"C\" limit $5",
            )
            .bind(source_repository.get())
            .bind(
                i64::try_from(from_refs)
                    .map_err(|_| Error::Backend("source ref token exceeds i64".to_owned()))?,
            )
            .bind(
                i64::try_from(observed_refs)
                    .map_err(|_| Error::Backend("source ref generation exceeds i64".to_owned()))?,
            )
            .bind(from_ref_cursor.as_ref().map(|cursor| cursor.key.as_str()))
            .bind(
                i64::try_from(remaining.saturating_add(1))
                    .map_err(|_| Error::Backend("catch-up bound exceeds i64".to_owned()))?,
            )
            .fetch_all(&mut *tx)
            .await?;
            more |= ref_rows.len() > ref_limit;
            for row in ref_rows.iter().take(ref_limit) {
                let generation =
                    nonnegative_i64_to_u64(row.try_get("generation")?, "journal ref generation")?;
                through.0 = generation;
                operations.push(PgMigrationCatchUpOperation::Ref {
                    generation,
                    name: row.try_get("refname")?,
                    expected: journal_ref_value(row, "old_target_oid", "old_symbolic_target")?,
                    value: journal_ref_value(row, "new_target_oid", "new_symbolic_target")?,
                });
            }
            remaining = options.max_operations - operations.len();
            if ref_rows.len() <= ref_limit {
                through.0 = observed_refs;
                through_ref_cursor = None;
            } else if let Some(PgMigrationCatchUpOperation::Ref {
                generation, name, ..
            }) = operations.last()
            {
                through_ref_cursor = Some(PgMigrationJournalCursor {
                    generation: *generation,
                    key: name.clone(),
                });
            }
        }

        if remaining > 0 {
            let config_limit = remaining;
            let config_rows = sqlx::query(
                "select generation, key, old_value, new_value from grit_config_change_journal
                 where repository_pk = $1 and generation <= $3
                   and (generation > $2 or (generation = $2 and $4::text is not null
                       and key collate \"C\" > $4 collate \"C\"))
                 order by generation, key collate \"C\" limit $5",
            )
            .bind(source_repository.get())
            .bind(
                i64::try_from(from_config)
                    .map_err(|_| Error::Backend("source config token exceeds i64".to_owned()))?,
            )
            .bind(
                i64::try_from(observed_config).map_err(|_| {
                    Error::Backend("source config generation exceeds i64".to_owned())
                })?,
            )
            .bind(
                from_config_cursor
                    .as_ref()
                    .map(|cursor| cursor.key.as_str()),
            )
            .bind(
                i64::try_from(remaining.saturating_add(1))
                    .map_err(|_| Error::Backend("catch-up bound exceeds i64".to_owned()))?,
            )
            .fetch_all(&mut *tx)
            .await?;
            more |= config_rows.len() > config_limit;
            for row in config_rows.iter().take(config_limit) {
                let generation = nonnegative_i64_to_u64(
                    row.try_get("generation")?,
                    "journal config generation",
                )?;
                through.2 = generation;
                operations.push(PgMigrationCatchUpOperation::Config {
                    generation,
                    key: row.try_get("key")?,
                    expected: row.try_get("old_value")?,
                    value: row.try_get("new_value")?,
                });
            }
            if config_rows.len() <= config_limit {
                through.2 = observed_config;
                through_config_cursor = None;
            } else if let Some(PgMigrationCatchUpOperation::Config {
                generation, key, ..
            }) = operations.last()
            {
                through_config_cursor = Some(PgMigrationJournalCursor {
                    generation: *generation,
                    key: key.clone(),
                });
            }
        }
        let apply_through = PgMigrationSourceToken::Postgres {
            ref_generation: through.0,
            history_generation: through.1,
            config_generation: through.2,
        };
        more |= operations.len() == options.max_operations;
        let batch = PgMigrationCatchUpBatch {
            migration_id: session.migration_id,
            fencing_token: session.fencing_token,
            from,
            from_ref_cursor,
            from_config_cursor,
            apply_through,
            apply_through_ref_cursor: through_ref_cursor,
            apply_through_config_cursor: through_config_cursor,
            source_observed,
            operations,
            remaining_lag: PgMigrationCatchUpLag {
                ref_generations: observed_refs.saturating_sub(through.0),
                history_generations: observed_history.saturating_sub(through.1),
                config_generations: observed_config.saturating_sub(through.2),
                more_operations: more,
            },
        };
        tx.commit().await?;
        Ok(batch)
    }

    /// Atomically apply metadata deltas and checkpoint a previously planned catch-up batch.
    ///
    /// Object and pack payloads must already exist at the destination. Ref and config changes use
    /// expected-old compare-and-swap checks. Session ownership, fencing token, phase, lease, and
    /// prior source token are locked and validated in the same transaction as the checkpoint.
    /// Empty batches return [`PgMigrationCatchUpApplyResult::NoChanges`] without writing.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Backend`] for malformed batches or timestamps, and propagates SQLx
    /// failures. Destination divergence and missing payloads are typed outcomes.
    pub async fn apply_migration_catch_up(
        &self,
        claim: &PgMigrationClaim,
        batch: &PgMigrationCatchUpBatch,
        observed_at: OffsetDateTime,
    ) -> Result<PgMigrationCatchUpApplyResult> {
        validate_migration_claim(claim, observed_at)?;
        if batch.migration_id != claim.session.migration_id
            || batch.fencing_token != claim.fencing_token
        {
            return Err(Error::Backend(
                "catch-up batch does not belong to the migration claim".to_owned(),
            ));
        }
        if batch.operations.is_empty() {
            return Ok(PgMigrationCatchUpApplyResult::NoChanges);
        }
        let migration_id = positive_u64_to_i64(batch.migration_id, "migration id")?;
        let fencing_token = positive_u64_to_i64(batch.fencing_token, "migration fencing token")?;
        let destination_repository_pk: Option<i64> = sqlx::query_scalar(
            "select destination_repository_pk from grit_migration_sessions
             where migration_id = $1",
        )
        .bind(migration_id)
        .fetch_optional(&self.pool)
        .await?;
        let Some(destination_repository_pk) = destination_repository_pk else {
            return Ok(PgMigrationCatchUpApplyResult::ClaimLost);
        };
        let destination = sqlx::query(
            "select tenant_id, repository_id from grit_repositories
             where repository_pk = $1 and deleted_at is null",
        )
        .bind(destination_repository_pk)
        .fetch_optional(&self.pool)
        .await?;
        let Some(destination) = destination else {
            return Ok(PgMigrationCatchUpApplyResult::ClaimLost);
        };
        let tenant = TenantId::new(destination.try_get::<String, _>("tenant_id")?)?;
        let repository = RepositoryId::new(destination.try_get::<String, _>("repository_id")?)?;
        let mut tx = self.pool.begin().await?;
        match lock_import_repository(&mut tx, &tenant, &repository).await {
            Ok(()) => {}
            Err(Error::RepositoryNotFound(_)) => {
                tx.rollback().await?;
                return Ok(PgMigrationCatchUpApplyResult::ClaimLost);
            }
            Err(error) => return Err(error),
        }
        let locked_repository_pk: Option<i64> = sqlx::query_scalar(
            "select repository_pk from grit_repositories
             where tenant_id = $1 and repository_id = $2 and deleted_at is null",
        )
        .bind(tenant.as_str())
        .bind(repository.as_str())
        .fetch_optional(&mut *tx)
        .await?;
        if locked_repository_pk != Some(destination_repository_pk) {
            tx.rollback().await?;
            return Ok(PgMigrationCatchUpApplyResult::ClaimLost);
        }
        let session_row = sqlx::query(
            "select * from grit_migration_sessions
             where migration_id = $1 and claimed_by = $2 and fencing_token = $3
               and state = 1 and phase = 5 and lease_expires_at > $4 and updated_at <= $4
             for update",
        )
        .bind(migration_id)
        .bind(&claim.worker_id)
        .bind(fencing_token)
        .bind(observed_at)
        .fetch_optional(&mut *tx)
        .await?;
        let Some(session_row) = session_row else {
            tx.rollback().await?;
            return Ok(PgMigrationCatchUpApplyResult::ClaimLost);
        };
        let session = row_to_migration_session(&session_row)?;
        if session.last_applied.as_ref() != Some(&batch.from)
            || session.last_applied_ref_cursor != batch.from_ref_cursor
            || session.last_applied_config_cursor != batch.from_config_cursor
        {
            tx.rollback().await?;
            return Ok(PgMigrationCatchUpApplyResult::ClaimLost);
        }
        validate_migration_catch_up_batch(batch, &session)?;
        if session.destination_repository.get() != destination_repository_pk {
            tx.rollback().await?;
            return Ok(PgMigrationCatchUpApplyResult::ClaimLost);
        }
        let mut refs_changed = false;
        let mut config_changed = false;
        for operation in &batch.operations {
            match operation {
                PgMigrationCatchUpOperation::Object { oid } => {
                    let exists = sqlx::query_scalar::<_, bool>(
                        "select exists(select 1 from grit_objects
                         where repository_pk = $1 and oid_bytes = $2)",
                    )
                    .bind(session.destination_repository.get())
                    .bind(oid.as_bytes())
                    .fetch_one(&mut *tx)
                    .await?;
                    if !exists {
                        tx.rollback().await?;
                        return Ok(PgMigrationCatchUpApplyResult::MissingPrerequisite {
                            oid: *oid,
                        });
                    }
                }
                PgMigrationCatchUpOperation::Pack { checksum } => {
                    let exists = sqlx::query_scalar::<_, bool>(
                        "select exists(select 1 from grit_packs
                         where repository_pk = $1 and pack_checksum_bytes = $2)",
                    )
                    .bind(session.destination_repository.get())
                    .bind(checksum.as_bytes())
                    .fetch_one(&mut *tx)
                    .await?;
                    if !exists {
                        tx.rollback().await?;
                        return Ok(PgMigrationCatchUpApplyResult::MissingPrerequisite {
                            oid: *checksum,
                        });
                    }
                }
                PgMigrationCatchUpOperation::Ref {
                    name,
                    expected,
                    value,
                    ..
                } => {
                    let current = read_ref_for_update(&mut tx, &tenant, &repository, name).await?;
                    if current != *expected {
                        tx.rollback().await?;
                        return Ok(PgMigrationCatchUpApplyResult::Diverged { key: name.clone() });
                    }
                    write_catch_up_ref(
                        &mut tx,
                        &tenant,
                        &repository,
                        name,
                        value.as_ref(),
                        observed_at,
                    )
                    .await?;
                    refs_changed |= current != *value;
                }
                PgMigrationCatchUpOperation::Config {
                    key,
                    expected,
                    value,
                    ..
                } => {
                    let current = sqlx::query_scalar::<_, String>(
                        "select value from grit_config
                         where tenant_id = $1 and repository_id = $2 and key = $3 for update",
                    )
                    .bind(tenant.as_str())
                    .bind(repository.as_str())
                    .bind(key)
                    .fetch_optional(&mut *tx)
                    .await?;
                    if current != *expected {
                        tx.rollback().await?;
                        return Ok(PgMigrationCatchUpApplyResult::Diverged { key: key.clone() });
                    }
                    write_catch_up_config(&mut tx, &tenant, &repository, key, value.as_deref())
                        .await?;
                    config_changed |= current != *value;
                }
            }
        }
        if refs_changed {
            bump_repository_generation_at(
                &mut tx,
                &tenant,
                &repository,
                PgRepositoryGeneration::Refs,
                observed_at,
            )
            .await?;
        }
        if config_changed {
            bump_repository_generation_at(
                &mut tx,
                &tenant,
                &repository,
                PgRepositoryGeneration::Config,
                observed_at,
            )
            .await?;
        }
        let token = migration_token_columns(&batch.apply_through)?;
        let rows = sqlx::query(
            "update grit_migration_sessions
             set applied_ref_generation = $5, applied_history_generation = $6,
                 applied_config_generation = $7, applied_external_token = $8,
                 applied_ref_name = $9, applied_config_key = $10, updated_at = $4
             where migration_id = $1 and claimed_by = $2 and fencing_token = $3
               and state = 1 and phase = 5 and lease_expires_at > $4 and updated_at <= $4",
        )
        .bind(migration_id)
        .bind(&claim.worker_id)
        .bind(fencing_token)
        .bind(observed_at)
        .bind(token.ref_generation)
        .bind(token.history_generation)
        .bind(token.config_generation)
        .bind(token.external)
        .bind(
            batch
                .apply_through_ref_cursor
                .as_ref()
                .map(|cursor| cursor.key.as_str()),
        )
        .bind(
            batch
                .apply_through_config_cursor
                .as_ref()
                .map(|cursor| cursor.key.as_str()),
        )
        .execute(&mut *tx)
        .await?;
        if rows.rows_affected() != 1 {
            tx.rollback().await?;
            return Ok(PgMigrationCatchUpApplyResult::ClaimLost);
        }
        tx.commit().await?;
        Ok(PgMigrationCatchUpApplyResult::Applied)
    }

    /// Verify a fully caught-up internal PostgreSQL migration and persist an immutable report.
    ///
    /// All probes share one repeatable-read snapshot. The source ref, history, and config
    /// generations must exactly equal the last applied token, with no partial journal cursors.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Backend`] for external sources, stale claims, invalid samples, incomplete
    /// catch-up, or corrupt rows, and propagates SQLx failures.
    pub async fn verify_migration(
        &self,
        claim: &PgMigrationClaim,
        options: PgMigrationVerificationOptions,
    ) -> Result<PgMigrationVerificationReport> {
        validate_migration_claim(claim, options.observed_at)?;
        if claim.session.phase != PgMigrationPhase::CatchUp || options.sample_oids.len() > 128 {
            return Err(Error::Backend(
                "verification requires a catch-up claim and at most 128 sample objects".to_owned(),
            ));
        }
        if options
            .sample_oids
            .iter()
            .any(|oid| oid.algo() != claim.session.hash_algo)
        {
            return Err(Error::Backend(
                "verification sample has the wrong hash algorithm".to_owned(),
            ));
        }
        let migration_id = positive_u64_to_i64(claim.session.migration_id, "migration id")?;
        let fencing_token = positive_u64_to_i64(claim.fencing_token, "migration fencing token")?;
        let mut tx = self.pool.begin().await?;
        sqlx::query("set transaction isolation level repeatable read")
            .execute(&mut *tx)
            .await?;
        let session_row = sqlx::query(
            "select * from grit_migration_sessions
             where migration_id = $1 and claimed_by = $2 and fencing_token = $3
               and state = 1 and phase = 5 and lease_expires_at > $4 and updated_at <= $4
             for update",
        )
        .bind(migration_id)
        .bind(&claim.worker_id)
        .bind(fencing_token)
        .bind(options.observed_at)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| Error::Backend("migration verification claim was lost".to_owned()))?;
        let session = row_to_migration_session(&session_row)?;
        let PgMigrationSource::Repository(source_pk) = session.source else {
            return Err(Error::Backend(
                "external migration verification is unsupported".to_owned(),
            ));
        };
        let repositories = sqlx::query(
            "select repository_pk, tenant_id, repository_id, hash_algo, ref_generation,
                    history_generation, config_generation
             from grit_repositories where repository_pk in ($1, $2) and deleted_at is null",
        )
        .bind(source_pk.get())
        .bind(session.destination_repository.get())
        .fetch_all(&mut *tx)
        .await?;
        if repositories.len() != 2 {
            return Err(Error::Backend(
                "migration source or destination no longer exists".to_owned(),
            ));
        }
        let source_row = repositories
            .iter()
            .find(|row| row.get::<i64, _>("repository_pk") == source_pk.get())
            .ok_or_else(|| Error::Backend("migration source row is missing".to_owned()))?;
        let destination_row = repositories
            .iter()
            .find(|row| row.get::<i64, _>("repository_pk") == session.destination_repository.get())
            .ok_or_else(|| Error::Backend("migration destination row is missing".to_owned()))?;
        let source_token = PgMigrationSourceToken::Postgres {
            ref_generation: nonnegative_i64_to_u64(
                source_row.try_get("ref_generation")?,
                "verification source ref generation",
            )?,
            history_generation: nonnegative_i64_to_u64(
                source_row.try_get("history_generation")?,
                "verification source history generation",
            )?,
            config_generation: nonnegative_i64_to_u64(
                source_row.try_get("config_generation")?,
                "verification source config generation",
            )?,
        };
        let destination_token = PgMigrationSourceToken::Postgres {
            ref_generation: nonnegative_i64_to_u64(
                destination_row.try_get("ref_generation")?,
                "verification destination ref generation",
            )?,
            history_generation: nonnegative_i64_to_u64(
                destination_row.try_get("history_generation")?,
                "verification destination history generation",
            )?,
            config_generation: nonnegative_i64_to_u64(
                destination_row.try_get("config_generation")?,
                "verification destination config generation",
            )?,
        };
        if session.last_applied.as_ref() != Some(&source_token)
            || session.last_applied_ref_cursor.is_some()
            || session.last_applied_config_cursor.is_some()
        {
            return Err(Error::Backend(
                "migration must have zero ref, history, and config lag before verification"
                    .to_owned(),
            ));
        }
        let source_tenant: String = source_row.try_get("tenant_id")?;
        let source_repository: String = source_row.try_get("repository_id")?;
        let destination_tenant: String = destination_row.try_get("tenant_id")?;
        let destination_repository: String = destination_row.try_get("repository_id")?;
        let source_hash: String = source_row.try_get("hash_algo")?;
        let destination_hash: String = destination_row.try_get("hash_algo")?;
        let entered_verification = sqlx::query(
            "update grit_migration_sessions set phase = 6, updated_at = $4
             where migration_id = $1 and claimed_by = $2 and fencing_token = $3
               and state = 1 and phase = 5 and lease_expires_at > $4",
        )
        .bind(migration_id)
        .bind(&claim.worker_id)
        .bind(fencing_token)
        .bind(options.observed_at)
        .execute(&mut *tx)
        .await?;
        if entered_verification.rows_affected() != 1 {
            return Err(Error::Backend(
                "migration verification claim was lost".to_owned(),
            ));
        }
        let mut checks = Vec::with_capacity(9);
        checks.push(verification_check(
            PgMigrationVerificationCheckKind::HashAlgorithm,
            source_hash == destination_hash && source_hash == session.hash_algo.name(),
            None,
            None,
            None,
            None,
        ));
        let projections = [
            (
                PgMigrationVerificationCheckKind::Refs,
                "grit_refs",
                "refname",
                "s.target_oid is distinct from d.target_oid or s.symbolic_target is distinct from d.symbolic_target",
            ),
            (
                PgMigrationVerificationCheckKind::PackManifest,
                "grit_packs",
                "pack_checksum",
                "s.index_checksum is distinct from d.index_checksum or s.object_count is distinct from d.object_count or s.size_bytes is distinct from d.size_bytes",
            ),
            (
                PgMigrationVerificationCheckKind::Config,
                "grit_config",
                "key",
                "s.value is distinct from d.value",
            ),
        ];
        for (kind, table, key, unequal) in projections {
            checks.push(
                compare_repository_projection(
                    &mut tx,
                    kind,
                    table,
                    key,
                    unequal,
                    &source_tenant,
                    &source_repository,
                    &destination_tenant,
                    &destination_repository,
                )
                .await?,
            );
        }
        checks.push(
            compare_default_branch(
                &mut tx,
                &source_tenant,
                &source_repository,
                &destination_tenant,
                &destination_repository,
            )
            .await?,
        );
        checks.push(PgMigrationVerificationCheck {
            kind: PgMigrationVerificationCheckKind::PeeledTags,
            result: PgMigrationVerificationResult::Unsupported,
            source_count: None,
            destination_count: None,
            mismatch_count: None,
            detail: Some(
                "the current storage model has no durable peeled-tag projection; exact tag refs and object manifests are checked"
                    .to_owned(),
            ),
        });
        checks.push(
            compare_object_manifest(
                &mut tx,
                &source_tenant,
                &source_repository,
                &destination_tenant,
                &destination_repository,
            )
            .await?,
        );
        if options.mode == PgMigrationVerificationMode::Full {
            checks.push(
                compare_commit_graph(
                    &mut tx,
                    &source_tenant,
                    &source_repository,
                    &destination_tenant,
                    &destination_repository,
                )
                .await?,
            );
            checks.push(unsupported_canonical_samples(&options.sample_oids));
        } else {
            for kind in [
                PgMigrationVerificationCheckKind::CommitGraph,
                PgMigrationVerificationCheckKind::SampleObjects,
            ] {
                checks.push(PgMigrationVerificationCheck {
                    kind,
                    result: PgMigrationVerificationResult::Unsupported,
                    source_count: None,
                    destination_count: None,
                    mismatch_count: None,
                    detail: Some("not performed in manifest verification mode".to_owned()),
                });
            }
        }
        checks.sort_unstable_by_key(|check| check.kind.code());
        let passed =
            verification_checks_pass(options.mode, !options.sample_oids.is_empty(), &checks);
        let (source_ref_generation, source_history_generation, source_config_generation) =
            postgres_token_generations(&source_token)?;
        let (
            destination_ref_generation,
            destination_history_generation,
            destination_config_generation,
        ) = postgres_token_generations(&destination_token)?;
        let report_generation: i64 = sqlx::query_scalar(
            "select coalesce(max(report_generation), 0) + 1
             from grit_migration_verification_reports where migration_id = $1",
        )
        .bind(migration_id)
        .fetch_one(&mut *tx)
        .await?;
        let report_id: i64 =
            sqlx::query_scalar(
                "insert into grit_migration_verification_reports
                (migration_id, report_generation, mode, source_ref_generation,
                 source_history_generation, source_config_generation, destination_ref_generation,
                 destination_history_generation, destination_config_generation, passed, created_at)
             values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11) returning report_id",
            )
            .bind(migration_id)
            .bind(report_generation)
            .bind(options.mode.code())
            .bind(i64::try_from(source_ref_generation).map_err(|_| {
                Error::Backend("verification ref generation exceeds i64".to_owned())
            })?)
            .bind(i64::try_from(source_history_generation).map_err(|_| {
                Error::Backend("verification history generation exceeds i64".to_owned())
            })?)
            .bind(i64::try_from(source_config_generation).map_err(|_| {
                Error::Backend("verification config generation exceeds i64".to_owned())
            })?)
            .bind(i64::try_from(destination_ref_generation).map_err(|_| {
                Error::Backend("verification destination ref generation exceeds i64".to_owned())
            })?)
            .bind(i64::try_from(destination_history_generation).map_err(|_| {
                Error::Backend("verification destination history generation exceeds i64".to_owned())
            })?)
            .bind(i64::try_from(destination_config_generation).map_err(|_| {
                Error::Backend("verification destination config generation exceeds i64".to_owned())
            })?)
            .bind(passed)
            .bind(options.observed_at)
            .fetch_one(&mut *tx)
            .await?;
        insert_verification_checks(&mut tx, report_id, &checks).await?;
        let next_phase = if passed { 7_i16 } else { 5_i16 };
        let changed = sqlx::query(
            "update grit_migration_sessions set phase = $5, updated_at = $4
             where migration_id = $1 and claimed_by = $2 and fencing_token = $3
               and state = 1 and phase = 6 and lease_expires_at > $4",
        )
        .bind(migration_id)
        .bind(&claim.worker_id)
        .bind(fencing_token)
        .bind(options.observed_at)
        .bind(next_phase)
        .execute(&mut *tx)
        .await?;
        if changed.rows_affected() != 1 {
            return Err(Error::Backend(
                "migration verification claim was lost".to_owned(),
            ));
        }
        tx.commit().await?;
        Ok(PgMigrationVerificationReport {
            report_id: nonnegative_i64_to_u64(report_id, "verification report id")?,
            migration_id: claim.session.migration_id,
            report_generation: nonnegative_i64_to_u64(
                report_generation,
                "verification report generation",
            )?,
            mode: options.mode,
            source_token,
            destination_token,
            passed,
            created_at: options.observed_at,
            checks,
        })
    }

    /// Atomically route a verified source identity to its destination repository.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Backend`] for invalid timestamps or external sources, and propagates SQLx
    /// failures. Changed verification, source, or lease state is a typed outcome.
    pub async fn cut_over_migration(
        &self,
        claim: &PgMigrationClaim,
        options: PgMigrationCutoverOptions,
    ) -> Result<PgMigrationRouteOutcome> {
        validate_migration_claim(claim, options.observed_at)?;
        if options.rollback_deadline <= options.observed_at {
            return Err(Error::Backend(
                "migration rollback deadline must be after cutover".to_owned(),
            ));
        }
        let PgMigrationSource::Repository(source_pk) = claim.session.source else {
            return Err(Error::Backend("external cutover is unsupported".to_owned()));
        };
        let destination_pk = claim.session.destination_repository;
        let source_identity = repository_identity_by_pk(&self.pool, source_pk).await?;
        let destination_identity = repository_identity_by_pk(&self.pool, destination_pk).await?;
        let migration_id = positive_u64_to_i64(claim.session.migration_id, "migration id")?;
        let fencing_token = positive_u64_to_i64(claim.fencing_token, "migration fencing token")?;
        let mut tx = self.pool.begin().await?;
        if !lock_migration_repository_pair(
            &mut tx,
            source_pk,
            &source_identity,
            destination_pk,
            &destination_identity,
        )
        .await?
        {
            tx.rollback().await?;
            return Ok(PgMigrationRouteOutcome::PreconditionsChanged);
        }
        let session_row = sqlx::query(
            "select * from grit_migration_sessions
             where migration_id = $1 and claimed_by = $2 and fencing_token = $3
               and state = 1 and phase = 7 and lease_expires_at > $4 and updated_at <= $4
             for update",
        )
        .bind(migration_id)
        .bind(&claim.worker_id)
        .bind(fencing_token)
        .bind(options.observed_at)
        .fetch_optional(&mut *tx)
        .await?;
        let Some(session_row) = session_row else {
            tx.rollback().await?;
            return Ok(PgMigrationRouteOutcome::PreconditionsChanged);
        };
        let session = row_to_migration_session(&session_row)?;
        if session.source != PgMigrationSource::Repository(source_pk)
            || session.destination_repository != destination_pk
            || session.last_applied_ref_cursor.is_some()
            || session.last_applied_config_cursor.is_some()
        {
            tx.rollback().await?;
            return Ok(PgMigrationRouteOutcome::PreconditionsChanged);
        }
        let source_generations = repository_generations_for_update(&mut tx, source_pk).await?;
        let destination_generations =
            repository_generations_for_update(&mut tx, destination_pk).await?;
        let report = sqlx::query(
            "select report_id, report_generation, source_ref_generation,
                    source_history_generation, source_config_generation,
                    destination_ref_generation, destination_history_generation,
                    destination_config_generation, created_at
             from grit_migration_verification_reports
             where migration_id = $1 and passed = true and report_generation is not null
             order by report_generation desc limit 1 for share",
        )
        .bind(migration_id)
        .fetch_optional(&mut *tx)
        .await?;
        let Some(report) = report else {
            tx.rollback().await?;
            return Ok(PgMigrationRouteOutcome::PreconditionsChanged);
        };
        let report_generations = (
            report.try_get::<i64, _>("source_ref_generation")?,
            report.try_get::<i64, _>("source_history_generation")?,
            report.try_get::<i64, _>("source_config_generation")?,
        );
        let report_destination_generations = (
            report.try_get::<i64, _>("destination_ref_generation")?,
            report.try_get::<i64, _>("destination_history_generation")?,
            report.try_get::<i64, _>("destination_config_generation")?,
        );
        let applied = session
            .last_applied
            .as_ref()
            .map(postgres_token_generations)
            .transpose()?;
        let applied = applied
            .map(|values| {
                Ok::<_, Error>((
                    i64::try_from(values.0).map_err(|_| {
                        Error::Backend("applied ref generation exceeds i64".to_owned())
                    })?,
                    i64::try_from(values.1).map_err(|_| {
                        Error::Backend("applied history generation exceeds i64".to_owned())
                    })?,
                    i64::try_from(values.2).map_err(|_| {
                        Error::Backend("applied config generation exceeds i64".to_owned())
                    })?,
                ))
            })
            .transpose()?;
        if report_generations != source_generations
            || report_destination_generations != destination_generations
            || applied != Some(source_generations)
        {
            tx.rollback().await?;
            return Ok(PgMigrationRouteOutcome::PreconditionsChanged);
        }
        let existing = sqlx::query(
            "select migration_id, active_repository_pk, route_generation
             from grit_repository_routes
             where source_repository_pk = $1 for update",
        )
        .bind(source_pk.get())
        .fetch_optional(&mut *tx)
        .await?;
        if let Some(existing) = existing {
            let same = existing.try_get::<i64, _>("migration_id")? == migration_id
                && existing.try_get::<i64, _>("active_repository_pk")? == destination_pk.get();
            tx.rollback().await?;
            return Ok(if same {
                PgMigrationRouteOutcome::AlreadyApplied
            } else {
                PgMigrationRouteOutcome::PreconditionsChanged
            });
        }
        let report_id: i64 = report.try_get("report_id")?;
        let inserted = sqlx::query(
            "insert into grit_repository_routes
                (source_repository_pk, active_repository_pk, destination_repository_pk,
                 migration_id, report_id, destination_ref_generation,
                 destination_history_generation, destination_config_generation,
                 cutover_at, rollback_deadline, route_generation)
             values ($1, $2, $2, $3, $4, $5, $6, $7, $8, $9, 1)",
        )
        .bind(source_pk.get())
        .bind(destination_pk.get())
        .bind(migration_id)
        .bind(report_id)
        .bind(destination_generations.0)
        .bind(destination_generations.1)
        .bind(destination_generations.2)
        .bind(options.observed_at)
        .bind(options.rollback_deadline)
        .execute(&mut *tx)
        .await?;
        if inserted.rows_affected() != 1 {
            tx.rollback().await?;
            return Ok(PgMigrationRouteOutcome::PreconditionsChanged);
        }
        let changed = sqlx::query(
            "update grit_migration_sessions
             set phase = 8, claimed_by = null, lease_expires_at = null, updated_at = $4
             where migration_id = $1 and claimed_by = $2 and fencing_token = $3 and phase = 7",
        )
        .bind(migration_id)
        .bind(&claim.worker_id)
        .bind(fencing_token)
        .bind(options.observed_at)
        .execute(&mut *tx)
        .await?;
        if changed.rows_affected() != 1 {
            tx.rollback().await?;
            return Ok(PgMigrationRouteOutcome::PreconditionsChanged);
        }
        tx.commit().await?;
        Ok(PgMigrationRouteOutcome::Applied)
    }

    /// Restore a cut-over source route before its explicit rollback deadline.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Backend`] for invalid identifiers and propagates SQLx failures.
    pub async fn rollback_migration(
        &self,
        options: PgMigrationRollbackOptions,
    ) -> Result<PgMigrationRouteOutcome> {
        let migration_id = positive_u64_to_i64(options.migration_id, "migration id")?;
        let endpoints = sqlx::query(
            "select source_repository_pk, destination_repository_pk
             from grit_migration_sessions where migration_id = $1",
        )
        .bind(migration_id)
        .fetch_optional(&self.pool)
        .await?;
        let Some(endpoints) = endpoints else {
            return Ok(PgMigrationRouteOutcome::PreconditionsChanged);
        };
        let source_pk =
            RepositoryPk::try_from(endpoints.try_get::<i64, _>("source_repository_pk")?)
                .map_err(|error| Error::Backend(error.to_string()))?;
        let destination_pk =
            RepositoryPk::try_from(endpoints.try_get::<i64, _>("destination_repository_pk")?)
                .map_err(|error| Error::Backend(error.to_string()))?;
        let source_identity = repository_identity_by_pk(&self.pool, source_pk).await?;
        let destination_identity = repository_identity_by_pk(&self.pool, destination_pk).await?;
        let mut tx = self.pool.begin().await?;
        if !lock_migration_repository_pair(
            &mut tx,
            source_pk,
            &source_identity,
            destination_pk,
            &destination_identity,
        )
        .await?
        {
            tx.rollback().await?;
            return Ok(PgMigrationRouteOutcome::PreconditionsChanged);
        }
        let phase: Option<i16> = sqlx::query_scalar(
            "select phase from grit_migration_sessions where migration_id = $1 for update",
        )
        .bind(migration_id)
        .fetch_optional(&mut *tx)
        .await?;
        if phase == Some(PgMigrationPhase::RolledBack.code()) {
            tx.rollback().await?;
            return Ok(PgMigrationRouteOutcome::AlreadyApplied);
        }
        if phase != Some(PgMigrationPhase::CutOver.code()) {
            tx.rollback().await?;
            return Ok(PgMigrationRouteOutcome::PreconditionsChanged);
        }
        let route = sqlx::query(
            "select active_repository_pk, rollback_deadline, rolled_back_at, route_generation
             from grit_repository_routes where source_repository_pk = $1
               and migration_id = $2 for update",
        )
        .bind(source_pk.get())
        .bind(migration_id)
        .fetch_optional(&mut *tx)
        .await?;
        let Some(route) = route else {
            tx.rollback().await?;
            return Ok(PgMigrationRouteOutcome::PreconditionsChanged);
        };
        let deadline: OffsetDateTime = route.try_get("rollback_deadline")?;
        if options.observed_at >= deadline
            || route
                .try_get::<Option<OffsetDateTime>, _>("rolled_back_at")?
                .is_some()
            || route.try_get::<i64, _>("active_repository_pk")? != destination_pk.get()
        {
            tx.rollback().await?;
            return Ok(PgMigrationRouteOutcome::PreconditionsChanged);
        }
        let route_generation: i64 = route.try_get("route_generation")?;
        let route_changed = sqlx::query(
            "update grit_repository_routes
             set active_repository_pk = source_repository_pk, rolled_back_at = $2,
                 route_generation = route_generation + 1
             where source_repository_pk = $1 and migration_id = $3
               and active_repository_pk = destination_repository_pk
               and rolled_back_at is null and route_generation = $4",
        )
        .bind(source_pk.get())
        .bind(options.observed_at)
        .bind(migration_id)
        .bind(route_generation)
        .execute(&mut *tx)
        .await?;
        if route_changed.rows_affected() != 1 {
            tx.rollback().await?;
            return Ok(PgMigrationRouteOutcome::PreconditionsChanged);
        }
        let session_changed = sqlx::query(
            "update grit_migration_sessions set phase = 9, updated_at = $2
             where migration_id = $1 and phase = 8",
        )
        .bind(migration_id)
        .bind(options.observed_at)
        .execute(&mut *tx)
        .await?;
        if session_changed.rows_affected() != 1 {
            tx.rollback().await?;
            return Ok(PgMigrationRouteOutcome::PreconditionsChanged);
        }
        tx.commit().await?;
        Ok(PgMigrationRouteOutcome::Applied)
    }

    /// Select bounded repository fragmentation candidates without loading object rows.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Backend`] for invalid policy or bounds and propagates SQLx failures.
    pub async fn pack_maintenance_candidates(
        &self,
        policy: PgPackMaintenancePolicy,
        max_candidates: usize,
    ) -> Result<Vec<PgPackMaintenanceCandidate>> {
        validate_pack_maintenance_policy(policy)?;
        if max_candidates == 0 || max_candidates > MAX_PACK_MAINTENANCE_BATCH {
            return Err(Error::Backend(
                "invalid maintenance candidate bound".to_owned(),
            ));
        }
        let rows = sqlx::query(
            "select repository.repository_pk,
                    coalesce(packs.pack_count, 0) as pack_count,
                    coalesce(loose.loose_count, 0) as loose_count,
                    coalesce(packs.pack_bytes, 0) as pack_bytes
             from grit_repositories repository
             left join lateral (
                 select count(*) as pack_count, coalesce(sum(size_bytes), 0) as pack_bytes
                 from grit_packs where repository_pk = repository.repository_pk
             ) packs on true
             left join lateral (
                 select count(*) as loose_count from grit_objects
                 where repository_pk = repository.repository_pk
             ) loose on true
             where repository.deleted_at is null
               and (coalesce(packs.pack_count, 0) >= $1
                    or coalesce(loose.loose_count, 0) > $2)
               and coalesce(packs.pack_bytes, 0) >= $3
             order by coalesce(packs.pack_count, 0) desc,
                      coalesce(loose.loose_count, 0) desc, repository.repository_pk
             limit $4",
        )
        .bind(i64::from(policy.min_pack_count))
        .bind(i64::try_from(policy.max_loose_count).map_err(|_| {
            Error::Backend("maintenance loose-object threshold exceeds i64".to_owned())
        })?)
        .bind(
            i64::try_from(policy.min_fragmented_bytes)
                .map_err(|_| Error::Backend("maintenance byte threshold exceeds i64".to_owned()))?,
        )
        .bind(
            i64::try_from(max_candidates).map_err(|_| {
                Error::Backend("maintenance candidate bound exceeds i64".to_owned())
            })?,
        )
        .fetch_all(&self.pool)
        .await?;
        rows.iter()
            .map(|row| {
                Ok(PgPackMaintenanceCandidate {
                    repository: RepositoryPk::try_from(row.try_get::<i64, _>("repository_pk")?)
                        .map_err(|error| Error::Backend(error.to_string()))?,
                    pack_count: nonnegative_i64_to_u64(
                        row.try_get("pack_count")?,
                        "maintenance pack count",
                    )?,
                    loose_count: nonnegative_i64_to_u64(
                        row.try_get("loose_count")?,
                        "maintenance loose count",
                    )?,
                    pack_bytes: nonnegative_i64_to_u64(
                        row.try_get("pack_bytes")?,
                        "maintenance pack bytes",
                    )?,
                })
            })
            .collect()
    }

    /// Create a generation-bound pack-maintenance job.
    ///
    /// # Errors
    ///
    /// Returns validation errors or SQLx failures.
    pub async fn create_pack_maintenance_job(
        &self,
        options: PgPackMaintenanceCreateOptions,
    ) -> Result<PgPackMaintenanceJob> {
        validate_pack_maintenance_policy(options.policy)?;
        let grace_seconds = options.grace.whole_seconds();
        if options.grace.is_negative() {
            return Err(Error::Backend(
                "maintenance grace must be nonnegative".to_owned(),
            ));
        }
        let row = sqlx::query(
            "insert into grit_pack_maintenance_jobs
                (repository_pk, ref_generation, history_generation, phase, min_pack_count,
                 max_loose_count, min_fragmented_bytes, grace_seconds, created_at, updated_at)
             select repository_pk, ref_generation, history_generation, 1, $2, $3, $4, $5, $6, $6
             from grit_repositories where repository_pk = $1 and deleted_at is null
             returning *",
        )
        .bind(options.repository.get())
        .bind(
            i32::try_from(options.policy.min_pack_count)
                .map_err(|_| Error::Backend("maintenance pack threshold exceeds i32".to_owned()))?,
        )
        .bind(
            i64::try_from(options.policy.max_loose_count).map_err(|_| {
                Error::Backend("maintenance loose threshold exceeds i64".to_owned())
            })?,
        )
        .bind(
            i64::try_from(options.policy.min_fragmented_bytes)
                .map_err(|_| Error::Backend("maintenance byte threshold exceeds i64".to_owned()))?,
        )
        .bind(grace_seconds)
        .bind(options.created_at)
        .fetch_optional(&self.pool)
        .await?
        .ok_or_else(|| Error::Backend("maintenance repository does not exist".to_owned()))?;
        row_to_pack_maintenance_job(&row)
    }

    /// Claim a bounded batch of resumable maintenance jobs with fresh fencing tokens.
    ///
    /// # Errors
    ///
    /// Returns validation errors or SQLx failures.
    pub async fn claim_pack_maintenance_jobs(
        &self,
        options: PgPackMaintenanceClaimOptions,
    ) -> Result<Vec<PgPackMaintenanceClaim>> {
        if options.worker_id.trim().is_empty()
            || options.worker_id.len() > 256
            || options.max_jobs == 0
            || options.max_jobs > MAX_PACK_MAINTENANCE_BATCH
            || options.lease_expires_at <= options.observed_at
        {
            return Err(Error::Backend(
                "invalid maintenance claim options".to_owned(),
            ));
        }
        let rows = sqlx::query(
            "with candidates as (
                 select job_id from grit_pack_maintenance_jobs
                 where phase between 1 and 4 and updated_at <= $2
                   and (claimed_by is null or lease_expires_at <= $2)
                   and attempt_count < 2147483647
                 order by phase, job_id limit $4 for update skip locked
             ) update grit_pack_maintenance_jobs job
               set claimed_by = $1,
                   fencing_token = nextval('grit_pack_maintenance_fencing_token_seq'),
                   lease_expires_at = $3, attempt_count = job.attempt_count + 1,
                   updated_at = $2
             from candidates where job.job_id = candidates.job_id returning job.*",
        )
        .bind(&options.worker_id)
        .bind(options.observed_at)
        .bind(options.lease_expires_at)
        .bind(
            i64::try_from(options.max_jobs)
                .map_err(|_| Error::Backend("maintenance claim bound exceeds i64".to_owned()))?,
        )
        .fetch_all(&self.pool)
        .await?;
        rows.iter()
            .map(|row| {
                let job = row_to_pack_maintenance_job(row)?;
                Ok(PgPackMaintenanceClaim {
                    worker_id: job.claimed_by.clone().ok_or_else(|| {
                        Error::Backend("claimed maintenance job has no worker".to_owned())
                    })?,
                    fencing_token: job.fencing_token,
                    lease_expires_at: job.lease_expires_at.ok_or_else(|| {
                        Error::Backend("claimed maintenance job has no lease".to_owned())
                    })?,
                    job,
                })
            })
            .collect()
    }

    /// Create or replace a time-bounded repository retention hold.
    ///
    /// # Errors
    ///
    /// Returns validation errors or propagates SQLx failures.
    pub async fn set_pack_retention_hold(
        &self,
        repository: RepositoryPk,
        hold_key: &str,
        reason: &str,
        created_at: OffsetDateTime,
        expires_at: OffsetDateTime,
    ) -> Result<()> {
        if hold_key.is_empty()
            || hold_key.len() > 256
            || reason.is_empty()
            || reason.len() > 2048
            || expires_at <= created_at
        {
            return Err(Error::Backend("invalid pack retention hold".to_owned()));
        }
        let (tenant, repository_id) = repository_identity_by_pk(&self.pool, repository).await?;
        let mut tx = self.pool.begin().await?;
        if !lock_expected_repository(&mut tx, repository, &tenant, &repository_id).await? {
            tx.rollback().await?;
            return Err(Error::Backend(
                "retention-hold repository identity changed".to_owned(),
            ));
        }
        sqlx::query(
            "insert into grit_repository_retention_holds
                (repository_pk, hold_key, reason, expires_at, created_at)
             values ($1, $2, $3, $4, $5)
             on conflict (repository_pk, hold_key) do update
             set reason = excluded.reason, expires_at = excluded.expires_at,
                 created_at = excluded.created_at",
        )
        .bind(repository.get())
        .bind(hold_key)
        .bind(reason)
        .bind(expires_at)
        .bind(created_at)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(())
    }

    /// Release one repository retention hold, returning whether it existed.
    ///
    /// # Errors
    ///
    /// Propagates SQLx failures.
    pub async fn release_pack_retention_hold(
        &self,
        repository: RepositoryPk,
        hold_key: &str,
    ) -> Result<bool> {
        if hold_key.is_empty() || hold_key.len() > 256 {
            return Err(Error::Backend("invalid pack retention hold key".to_owned()));
        }
        let (tenant, repository_id) = repository_identity_by_pk(&self.pool, repository).await?;
        let mut tx = self.pool.begin().await?;
        if !lock_expected_repository(&mut tx, repository, &tenant, &repository_id).await? {
            tx.rollback().await?;
            return Err(Error::Backend(
                "retention-hold repository identity changed".to_owned(),
            ));
        }
        let deleted = sqlx::query(
            "delete from grit_repository_retention_holds
             where repository_pk = $1 and hold_key = $2",
        )
        .bind(repository.get())
        .bind(hold_key)
        .execute(&mut *tx)
        .await?
        .rows_affected()
            == 1;
        tx.commit().await?;
        Ok(deleted)
    }

    /// Publish an already-durable replacement and atomically snapshot superseded packs.
    ///
    /// This method never constructs a pack or deletes bytes. The replacement and all superseded
    /// packs must already be durable in the same repository, and the replacement index must cover
    /// every object indexed by the superseded set.
    ///
    /// # Errors
    ///
    /// Returns validation errors or propagates SQLx failures.
    pub async fn publish_pack_maintenance_replacement(
        &self,
        claim: &PgPackMaintenanceClaim,
        replacement: &ObjectId,
        superseded: &[ObjectId],
        observed_at: OffsetDateTime,
    ) -> Result<PgPackMaintenanceOutcome> {
        validate_pack_maintenance_claim(claim, observed_at)?;
        let superseded_bytes = validate_superseded_packs(replacement, superseded)?;
        let (tenant, repository) =
            repository_identity_by_pk(&self.pool, claim.job.repository).await?;
        let mut tx = self.pool.begin().await?;
        if !lock_expected_repository(&mut tx, claim.job.repository, &tenant, &repository).await? {
            tx.rollback().await?;
            return Ok(PgPackMaintenanceOutcome::ClaimLost);
        }
        let row =
            sqlx::query("select * from grit_pack_maintenance_jobs where job_id = $1 for update")
                .bind(
                    i64::try_from(claim.job.job_id)
                        .map_err(|_| Error::Backend("maintenance job id exceeds i64".to_owned()))?,
                )
                .fetch_optional(&mut *tx)
                .await?;
        let Some(row) = row else {
            tx.rollback().await?;
            return Ok(PgPackMaintenanceOutcome::ClaimLost);
        };
        let job = row_to_pack_maintenance_job(&row)?;
        if job.phase != PgPackMaintenancePhase::Planned {
            tx.rollback().await?;
            return Ok(if job.replacement.as_ref() == Some(replacement) {
                PgPackMaintenanceOutcome::AlreadyApplied
            } else {
                PgPackMaintenanceOutcome::Corrupt {
                    detail: "maintenance job has a different replacement".to_owned(),
                }
            });
        }
        if !maintenance_claim_matches(&job, claim, observed_at) {
            tx.rollback().await?;
            return Ok(PgPackMaintenanceOutcome::ClaimLost);
        }
        let repository_row = sqlx::query(
            "select hash_algo, ref_generation, history_generation
             from grit_repositories where repository_pk = $1",
        )
        .bind(claim.job.repository.get())
        .fetch_one(&mut *tx)
        .await?;
        let hash_algo_name: String = repository_row.try_get("hash_algo")?;
        let hash_algo = HashAlgo::from_name(&hash_algo_name)
            .ok_or_else(|| Error::Backend("invalid repository hash algorithm".to_owned()))?;
        if replacement.algo() != hash_algo || superseded.iter().any(|oid| oid.algo() != hash_algo) {
            tx.rollback().await?;
            return Ok(PgPackMaintenanceOutcome::Corrupt {
                detail: "pack checksum algorithm does not match repository".to_owned(),
            });
        }
        let ref_generation = nonnegative_i64_to_u64(
            repository_row.try_get("ref_generation")?,
            "maintenance ref generation",
        )?;
        let history_generation = nonnegative_i64_to_u64(
            repository_row.try_get("history_generation")?,
            "maintenance history generation",
        )?;
        if ref_generation != job.ref_generation || history_generation != job.history_generation {
            tx.rollback().await?;
            return Ok(PgPackMaintenanceOutcome::Blocked {
                reason: "repository generations changed after maintenance planning".to_owned(),
            });
        }
        let replacement_exists: bool = sqlx::query_scalar(
            "select exists(select 1 from grit_packs
             where repository_pk = $1 and pack_checksum_bytes = $2)",
        )
        .bind(claim.job.repository.get())
        .bind(replacement.as_bytes())
        .fetch_one(&mut *tx)
        .await?;
        let superseded_count: i64 = sqlx::query_scalar(
            "select count(*) from grit_packs
             where repository_pk = $1 and pack_checksum_bytes = any($2::bytea[])",
        )
        .bind(claim.job.repository.get())
        .bind(&superseded_bytes)
        .fetch_one(&mut *tx)
        .await?;
        if !replacement_exists || superseded_count != superseded_bytes.len() as i64 {
            tx.rollback().await?;
            return Ok(PgPackMaintenanceOutcome::Corrupt {
                detail: "replacement or superseded pack metadata is missing".to_owned(),
            });
        }
        let mut validated_packs = superseded_bytes.clone();
        validated_packs.push(replacement.as_bytes().to_vec());
        if !pack_indexes_match_metadata(&mut tx, job.repository, &validated_packs).await? {
            tx.rollback().await?;
            return Ok(PgPackMaintenanceOutcome::Corrupt {
                detail: "pack object counts disagree with their indexes".to_owned(),
            });
        }
        if !pack_replacement_covers(
            &mut tx,
            claim.job.repository,
            replacement,
            &superseded_bytes,
        )
        .await?
        {
            tx.rollback().await?;
            return Ok(PgPackMaintenanceOutcome::Corrupt {
                detail: "replacement index does not cover the superseded pack set".to_owned(),
            });
        }
        let prune_after = observed_at
            .checked_add(job.grace)
            .ok_or_else(|| Error::Backend("maintenance prune timestamp overflow".to_owned()))?;
        let inserted = sqlx::query(
            "insert into grit_pack_maintenance_superseded
                (job_id, pack_checksum, index_checksum, storage_backend, storage_key,
                 object_count, size_bytes)
             select $1, pack_checksum_bytes, index_checksum_bytes, storage_backend, storage_key,
                    object_count, size_bytes
             from grit_packs where repository_pk = $2
               and pack_checksum_bytes = any($3::bytea[])
             on conflict (job_id, pack_checksum) do nothing",
        )
        .bind(
            i64::try_from(job.job_id)
                .map_err(|_| Error::Backend("maintenance job id exceeds i64".to_owned()))?,
        )
        .bind(job.repository.get())
        .bind(&superseded_bytes)
        .execute(&mut *tx)
        .await?;
        if inserted.rows_affected() != superseded_bytes.len() as u64 {
            tx.rollback().await?;
            return Ok(PgPackMaintenanceOutcome::Corrupt {
                detail: "superseded pack snapshot was incomplete".to_owned(),
            });
        }
        sqlx::query(
            "insert into grit_pack_maintenance_objects (job_id, oid)
             select distinct $1, oid_bytes from grit_pack_objects
             where repository_pk = $2 and pack_checksum_bytes = any($3::bytea[])
             on conflict (job_id, oid) do nothing",
        )
        .bind(
            i64::try_from(job.job_id)
                .map_err(|_| Error::Backend("maintenance job id exceeds i64".to_owned()))?,
        )
        .bind(job.repository.get())
        .bind(&superseded_bytes)
        .execute(&mut *tx)
        .await?;
        if !pack_replacement_covers_snapshot(&mut tx, job.job_id, job.repository, replacement)
            .await?
        {
            tx.rollback().await?;
            return Ok(PgPackMaintenanceOutcome::Corrupt {
                detail: "replacement does not cover the persisted object snapshot".to_owned(),
            });
        }
        let changed = sqlx::query(
            "update grit_pack_maintenance_jobs
             set phase = 2, replacement_pack_checksum = $2, prune_after = $3, updated_at = $4
             where job_id = $1 and phase = 1 and claimed_by = $5 and fencing_token = $6
               and lease_expires_at > $4",
        )
        .bind(
            i64::try_from(job.job_id)
                .map_err(|_| Error::Backend("maintenance job id exceeds i64".to_owned()))?,
        )
        .bind(replacement.as_bytes())
        .bind(prune_after)
        .bind(observed_at)
        .bind(&claim.worker_id)
        .bind(
            i64::try_from(claim.fencing_token)
                .map_err(|_| Error::Backend("maintenance fencing token exceeds i64".to_owned()))?,
        )
        .execute(&mut *tx)
        .await?;
        if changed.rows_affected() != 1 {
            tx.rollback().await?;
            return Ok(PgPackMaintenanceOutcome::ClaimLost);
        }
        tx.commit().await?;
        Ok(PgPackMaintenanceOutcome::Applied)
    }

    /// Prune superseded SQL metadata after grace and revalidation.
    ///
    /// External bytes are only registered as orphan candidates; this method performs no external
    /// I/O and never runs database-wide maintenance such as `VACUUM`.
    ///
    /// # Errors
    ///
    /// Returns validation errors or propagates SQLx failures.
    pub async fn prune_pack_maintenance_job(
        &self,
        claim: &PgPackMaintenanceClaim,
        observed_at: OffsetDateTime,
    ) -> Result<PgPackMaintenanceOutcome> {
        validate_pack_maintenance_claim(claim, observed_at)?;
        let (tenant, repository) =
            repository_identity_by_pk(&self.pool, claim.job.repository).await?;
        let mut tx = self.pool.begin().await?;
        if !lock_expected_repository(&mut tx, claim.job.repository, &tenant, &repository).await? {
            tx.rollback().await?;
            return Ok(PgPackMaintenanceOutcome::ClaimLost);
        }
        let row =
            sqlx::query("select * from grit_pack_maintenance_jobs where job_id = $1 for update")
                .bind(
                    i64::try_from(claim.job.job_id)
                        .map_err(|_| Error::Backend("maintenance job id exceeds i64".to_owned()))?,
                )
                .fetch_optional(&mut *tx)
                .await?;
        let Some(row) = row else {
            tx.rollback().await?;
            return Ok(PgPackMaintenanceOutcome::ClaimLost);
        };
        let job = row_to_pack_maintenance_job(&row)?;
        if matches!(
            job.phase,
            PgPackMaintenancePhase::MetadataSwept
                | PgPackMaintenancePhase::ExternalSweepPending
                | PgPackMaintenancePhase::Complete
        ) {
            tx.rollback().await?;
            return Ok(PgPackMaintenanceOutcome::AlreadyApplied);
        }
        if job.phase != PgPackMaintenancePhase::ReplacementPublished
            || !maintenance_claim_matches(&job, claim, observed_at)
        {
            tx.rollback().await?;
            return Ok(PgPackMaintenanceOutcome::ClaimLost);
        }
        let prune_after = job.prune_after.ok_or_else(|| {
            Error::Backend("published maintenance job has no prune time".to_owned())
        })?;
        if observed_at < prune_after {
            tx.rollback().await?;
            return Ok(PgPackMaintenanceOutcome::Blocked {
                reason: "superseded pack grace period is active".to_owned(),
            });
        }
        let generations: Option<(i64, i64)> = sqlx::query_as(
            "select ref_generation, history_generation from grit_repositories
             where repository_pk = $1 and deleted_at is null",
        )
        .bind(job.repository.get())
        .fetch_optional(&mut *tx)
        .await?;
        let expected_generations = (
            i64::try_from(job.ref_generation)
                .map_err(|_| Error::Backend("maintenance ref generation exceeds i64".to_owned()))?,
            i64::try_from(job.history_generation).map_err(|_| {
                Error::Backend("maintenance history generation exceeds i64".to_owned())
            })?,
        );
        if generations != Some(expected_generations) {
            tx.rollback().await?;
            return Ok(PgPackMaintenanceOutcome::Blocked {
                reason: "repository generations changed after maintenance planning".to_owned(),
            });
        }
        let active_hold: bool = sqlx::query_scalar(
            "select exists(select 1 from grit_repository_retention_holds
             where repository_pk = $1 and expires_at > $2)",
        )
        .bind(job.repository.get())
        .bind(observed_at)
        .fetch_one(&mut *tx)
        .await?;
        let active_rollback: bool = sqlx::query_scalar(
            "select exists(select 1 from grit_repository_routes
             where (source_repository_pk = $1 or destination_repository_pk = $1)
               and active_repository_pk = destination_repository_pk
               and rolled_back_at is null and rollback_deadline > $2)",
        )
        .bind(job.repository.get())
        .bind(observed_at)
        .fetch_one(&mut *tx)
        .await?;
        if active_hold || active_rollback {
            tx.rollback().await?;
            return Ok(PgPackMaintenanceOutcome::Blocked {
                reason: if active_hold {
                    "an unexpired repository retention hold blocks pruning"
                } else {
                    "an active migration rollback window blocks pruning"
                }
                .to_owned(),
            });
        }
        let superseded_rows = sqlx::query(
            "select snapshot.pack_checksum, snapshot.storage_backend, snapshot.storage_key,
                    snapshot.size_bytes,
                    (pack.pack_checksum_bytes is not null) as pack_exists,
                    (pack.index_checksum_bytes is not distinct from snapshot.index_checksum
                     and pack.storage_backend is not distinct from snapshot.storage_backend
                     and pack.storage_key is not distinct from snapshot.storage_key
                     and pack.object_count is not distinct from snapshot.object_count
                     and pack.size_bytes is not distinct from snapshot.size_bytes)
                        as metadata_matches
             from grit_pack_maintenance_superseded snapshot
             left join grit_packs pack on pack.repository_pk = $2
               and pack.pack_checksum_bytes = snapshot.pack_checksum
             where snapshot.job_id = $1 order by snapshot.pack_checksum",
        )
        .bind(
            i64::try_from(job.job_id)
                .map_err(|_| Error::Backend("maintenance job id exceeds i64".to_owned()))?,
        )
        .bind(job.repository.get())
        .fetch_all(&mut *tx)
        .await?;
        let snapshots_match = superseded_rows.iter().try_fold(true, |matches, row| {
            Ok::<_, sqlx::Error>(
                matches
                    && row.try_get::<bool, _>("pack_exists")?
                    && row.try_get::<bool, _>("metadata_matches")?,
            )
        })?;
        if superseded_rows.is_empty() || !snapshots_match {
            tx.rollback().await?;
            return Ok(PgPackMaintenanceOutcome::Corrupt {
                detail: "superseded pack metadata changed before pruning".to_owned(),
            });
        }
        let superseded_bytes = superseded_rows
            .iter()
            .map(|row| row.try_get::<Vec<u8>, _>("pack_checksum"))
            .collect::<std::result::Result<Vec<_>, _>>()?;
        let replacement = job.replacement.ok_or_else(|| {
            Error::Backend("published maintenance job has no replacement".to_owned())
        })?;
        if !pack_indexes_match_metadata(&mut tx, job.repository, &superseded_bytes).await?
            || !pack_snapshot_matches_live_superseded(
                &mut tx,
                job.job_id,
                job.repository,
                &superseded_bytes,
            )
            .await?
            || !pack_replacement_covers_snapshot(&mut tx, job.job_id, job.repository, &replacement)
                .await?
        {
            tx.rollback().await?;
            return Ok(PgPackMaintenanceOutcome::Corrupt {
                detail: "replacement coverage changed before pruning".to_owned(),
            });
        }
        sqlx::query(
            "insert into grit_external_orphan_candidates
                (tenant_id, repository_id, storage_backend, storage_key,
                 size_bytes, first_observed_at)
             select $2, $3, storage_backend, storage_key, size_bytes, $4
             from grit_pack_maintenance_superseded
             where job_id = $1 and storage_backend <> 'database'
             on conflict (tenant_id, repository_id, storage_backend, storage_key) do update
             set size_bytes = excluded.size_bytes,
                 first_observed_at = coalesce(
                    grit_external_orphan_candidates.first_observed_at,
                    excluded.first_observed_at)",
        )
        .bind(
            i64::try_from(job.job_id)
                .map_err(|_| Error::Backend("maintenance job id exceeds i64".to_owned()))?,
        )
        .bind(tenant.as_str())
        .bind(repository.as_str())
        .bind(observed_at)
        .execute(&mut *tx)
        .await?;
        let checksums = superseded_bytes
            .iter()
            .map(|checksum| bytes_to_hex(checksum))
            .collect::<Vec<_>>();
        let expected_index_rows: i64 = sqlx::query_scalar(
            "select count(*) from grit_pack_objects
             where repository_pk = $1 and pack_checksum_bytes = any($2::bytea[])",
        )
        .bind(job.repository.get())
        .bind(&superseded_bytes)
        .fetch_one(&mut *tx)
        .await?;
        let deleted_indexes = sqlx::query(
            "delete from grit_pack_objects
             where repository_pk = $1 and pack_checksum = any($2::text[])",
        )
        .bind(job.repository.get())
        .bind(&checksums)
        .execute(&mut *tx)
        .await?;
        if i64::try_from(deleted_indexes.rows_affected()).ok() != Some(expected_index_rows) {
            tx.rollback().await?;
            return Ok(PgPackMaintenanceOutcome::Corrupt {
                detail: "superseded pack index deletion was incomplete".to_owned(),
            });
        }
        let deleted_packs = sqlx::query(
            "delete from grit_packs
             where repository_pk = $1 and pack_checksum = any($2::text[])",
        )
        .bind(job.repository.get())
        .bind(&checksums)
        .execute(&mut *tx)
        .await?;
        if deleted_packs.rows_affected() != superseded_rows.len() as u64 {
            tx.rollback().await?;
            return Ok(PgPackMaintenanceOutcome::Corrupt {
                detail: "superseded pack deletion was incomplete".to_owned(),
            });
        }
        let has_external = superseded_rows
            .iter()
            .try_fold(false, |has_external, row| {
                Ok::<_, sqlx::Error>(
                    has_external || row.try_get::<String, _>("storage_backend")? != "database",
                )
            })?;
        let changed = sqlx::query(
            "update grit_pack_maintenance_jobs set phase = $2, updated_at = $3,
                    claimed_by = null, lease_expires_at = null
             where job_id = $1 and phase = 2 and claimed_by = $4 and fencing_token = $5
               and lease_expires_at > $3",
        )
        .bind(
            i64::try_from(job.job_id)
                .map_err(|_| Error::Backend("maintenance job id exceeds i64".to_owned()))?,
        )
        .bind(if has_external { 4_i16 } else { 5_i16 })
        .bind(observed_at)
        .bind(&claim.worker_id)
        .bind(
            i64::try_from(claim.fencing_token)
                .map_err(|_| Error::Backend("maintenance fencing token exceeds i64".to_owned()))?,
        )
        .execute(&mut *tx)
        .await?;
        if changed.rows_affected() != 1 {
            tx.rollback().await?;
            return Ok(PgPackMaintenanceOutcome::ClaimLost);
        }
        tx.commit().await?;
        Ok(PgPackMaintenanceOutcome::Applied)
    }

    /// Complete a metadata-swept job after any external orphan candidates are removed.
    ///
    /// # Errors
    ///
    /// Returns validation errors or propagates SQLx failures.
    pub async fn complete_pack_maintenance_job(
        &self,
        claim: &PgPackMaintenanceClaim,
        observed_at: OffsetDateTime,
    ) -> Result<PgPackMaintenanceOutcome> {
        validate_pack_maintenance_claim(claim, observed_at)?;
        let (tenant, repository) =
            repository_identity_by_pk(&self.pool, claim.job.repository).await?;
        let mut tx = self.pool.begin().await?;
        if !lock_expected_repository(&mut tx, claim.job.repository, &tenant, &repository).await? {
            tx.rollback().await?;
            return Ok(PgPackMaintenanceOutcome::ClaimLost);
        }
        let job_id = i64::try_from(claim.job.job_id)
            .map_err(|_| Error::Backend("maintenance job id exceeds i64".to_owned()))?;
        let row =
            sqlx::query("select * from grit_pack_maintenance_jobs where job_id = $1 for update")
                .bind(job_id)
                .fetch_optional(&mut *tx)
                .await?;
        let Some(row) = row else {
            tx.rollback().await?;
            return Ok(PgPackMaintenanceOutcome::ClaimLost);
        };
        let job = row_to_pack_maintenance_job(&row)?;
        if job.phase == PgPackMaintenancePhase::Complete {
            tx.rollback().await?;
            return Ok(PgPackMaintenanceOutcome::AlreadyApplied);
        }
        if !matches!(
            job.phase,
            PgPackMaintenancePhase::MetadataSwept | PgPackMaintenancePhase::ExternalSweepPending
        ) || !maintenance_claim_matches(&job, claim, observed_at)
        {
            tx.rollback().await?;
            return Ok(PgPackMaintenanceOutcome::ClaimLost);
        }
        if job.phase == PgPackMaintenancePhase::ExternalSweepPending {
            let pending: bool = sqlx::query_scalar(
                "select exists(
                     select 1 from grit_pack_maintenance_superseded snapshot
                     join grit_external_orphan_candidates candidate
                       on candidate.tenant_id = $2 and candidate.repository_id = $3
                      and candidate.storage_backend = snapshot.storage_backend
                      and candidate.storage_key = snapshot.storage_key
                     where snapshot.job_id = $1 and snapshot.storage_backend <> 'database')",
            )
            .bind(job_id)
            .bind(tenant.as_str())
            .bind(repository.as_str())
            .fetch_one(&mut *tx)
            .await?;
            if pending {
                tx.rollback().await?;
                return Ok(PgPackMaintenanceOutcome::Blocked {
                    reason: "external orphan candidates are still pending".to_owned(),
                });
            }
        }
        let changed = sqlx::query(
            "update grit_pack_maintenance_jobs
             set phase = 5, claimed_by = null, lease_expires_at = null, updated_at = $4
             where job_id = $1 and phase in (3, 4) and claimed_by = $2
               and fencing_token = $3 and lease_expires_at > $4",
        )
        .bind(job_id)
        .bind(&claim.worker_id)
        .bind(
            i64::try_from(claim.fencing_token)
                .map_err(|_| Error::Backend("maintenance fencing token exceeds i64".to_owned()))?,
        )
        .bind(observed_at)
        .execute(&mut *tx)
        .await?;
        if changed.rows_affected() != 1 {
            tx.rollback().await?;
            return Ok(PgPackMaintenanceOutcome::ClaimLost);
        }
        tx.commit().await?;
        Ok(PgPackMaintenanceOutcome::Applied)
    }

    /// Release a live maintenance claim without changing its phase.
    ///
    /// # Errors
    ///
    /// Returns validation errors or propagates SQLx failures.
    pub async fn release_pack_maintenance_claim(
        &self,
        claim: &PgPackMaintenanceClaim,
        observed_at: OffsetDateTime,
    ) -> Result<PgPackMaintenanceOutcome> {
        validate_pack_maintenance_claim(claim, observed_at)?;
        let changed = sqlx::query(
            "update grit_pack_maintenance_jobs
             set claimed_by = null, lease_expires_at = null, updated_at = $4
             where job_id = $1 and phase between 1 and 4 and claimed_by = $2
               and fencing_token = $3 and lease_expires_at > $4",
        )
        .bind(
            i64::try_from(claim.job.job_id)
                .map_err(|_| Error::Backend("maintenance job id exceeds i64".to_owned()))?,
        )
        .bind(&claim.worker_id)
        .bind(
            i64::try_from(claim.fencing_token)
                .map_err(|_| Error::Backend("maintenance fencing token exceeds i64".to_owned()))?,
        )
        .bind(observed_at)
        .execute(&self.pool)
        .await?;
        Ok(if changed.rows_affected() == 1 {
            PgPackMaintenanceOutcome::Applied
        } else {
            PgPackMaintenanceOutcome::ClaimLost
        })
    }

    /// Stop a live maintenance job with a durable failure reason.
    ///
    /// # Errors
    ///
    /// Returns validation errors or propagates SQLx failures.
    pub async fn fail_pack_maintenance_job(
        &self,
        claim: &PgPackMaintenanceClaim,
        reason: &str,
        observed_at: OffsetDateTime,
    ) -> Result<PgPackMaintenanceOutcome> {
        validate_pack_maintenance_claim(claim, observed_at)?;
        if reason.trim().is_empty() || reason.len() > 2048 {
            return Err(Error::Backend(
                "invalid maintenance failure reason".to_owned(),
            ));
        }
        let changed = sqlx::query(
            "update grit_pack_maintenance_jobs
             set phase = 6, failure_reason = $4, claimed_by = null,
                 lease_expires_at = null, updated_at = $5
             where job_id = $1 and phase between 1 and 4 and claimed_by = $2
               and fencing_token = $3 and lease_expires_at > $5",
        )
        .bind(
            i64::try_from(claim.job.job_id)
                .map_err(|_| Error::Backend("maintenance job id exceeds i64".to_owned()))?,
        )
        .bind(&claim.worker_id)
        .bind(
            i64::try_from(claim.fencing_token)
                .map_err(|_| Error::Backend("maintenance fencing token exceeds i64".to_owned()))?,
        )
        .bind(reason)
        .bind(observed_at)
        .execute(&self.pool)
        .await?;
        Ok(if changed.rows_affected() == 1 {
            PgPackMaintenanceOutcome::Applied
        } else {
            PgPackMaintenanceOutcome::ClaimLost
        })
    }

    /// Probe bounded pack/index and external-key metadata without fetching pack bytes.
    ///
    /// # Errors
    ///
    /// Returns validation errors, corrupt stored identifiers, or SQLx failures.
    pub async fn probe_pack_consistency(
        &self,
        repository: RepositoryPk,
        checksums: &[ObjectId],
    ) -> Result<Vec<PgPackConsistencyProbe>> {
        if checksums.is_empty() || checksums.len() > MAX_PACK_MAINTENANCE_BATCH {
            return Err(Error::Backend(
                "invalid pack consistency probe bound".to_owned(),
            ));
        }
        let mut unique = HashSet::with_capacity(checksums.len());
        if checksums
            .iter()
            .any(|checksum| !unique.insert(checksum.as_bytes().to_vec()))
        {
            return Err(Error::Backend(
                "duplicate pack consistency checksum".to_owned(),
            ));
        }
        let values = checksums
            .iter()
            .map(|checksum| checksum.as_bytes().to_vec())
            .collect::<Vec<_>>();
        let rows = sqlx::query(
            "select pack.pack_checksum_bytes, pack.object_count,
                    count(indexed.oid_bytes) as indexed_count,
                    pack.storage_backend, pack.storage_key
             from grit_packs pack
             left join grit_pack_objects indexed on indexed.repository_pk = pack.repository_pk
               and indexed.pack_checksum_bytes = pack.pack_checksum_bytes
             where pack.repository_pk = $1 and pack.pack_checksum_bytes = any($2::bytea[])
             group by pack.pack_checksum_bytes, pack.object_count,
                      pack.storage_backend, pack.storage_key
             order by pack.pack_checksum_bytes",
        )
        .bind(repository.get())
        .bind(&values)
        .fetch_all(&self.pool)
        .await?;
        rows.iter()
            .map(|row| {
                let object_count: i32 = row.try_get("object_count")?;
                let indexed_count: i64 = row.try_get("indexed_count")?;
                let backend: String = row.try_get("storage_backend")?;
                let storage_key: Option<String> = row.try_get("storage_key")?;
                Ok(PgPackConsistencyProbe {
                    checksum: ObjectId::from_bytes(
                        &row.try_get::<Vec<u8>, _>("pack_checksum_bytes")?,
                    )?,
                    index_binding_valid: i64::from(object_count) == indexed_count,
                    external_key_metadata_valid: if backend == "database" {
                        storage_key.is_none()
                    } else {
                        storage_key.is_some_and(|key| !key.is_empty())
                    },
                })
            })
            .collect()
    }

    /// Claim a bounded oldest-first batch of durable cache invalidation events.
    ///
    /// The claim and fresh fencing-token assignment happen in one PostgreSQL statement. Locked
    /// rows are skipped, and a crashed worker's events become eligible when the caller's
    /// `observed_at` reaches their lease expiry. Events at `max_attempts` remain durable for
    /// operator inspection but are not claimed again by this call.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Backend`] when a bound, worker identity, timestamp relationship, or stored
    /// event is invalid, or SQLx errors from PostgreSQL.
    pub async fn claim_cache_invalidations(
        &self,
        options: PgCacheOutboxClaimOptions,
    ) -> Result<Vec<PgCacheOutboxClaim>> {
        validate_cache_outbox_claim_options(&options)?;
        let max_events = i64::try_from(options.max_events)
            .map_err(|_| Error::Backend("cache outbox batch exceeds i64".to_owned()))?;
        let max_attempts = i32::try_from(options.max_attempts)
            .map_err(|_| Error::Backend("cache outbox attempt limit exceeds i32".to_owned()))?;
        let rows = sqlx::query(
            "with candidates as (
                 select event_id
                 from grit_cache_invalidation_outbox
                 where (claimed_by is null or lease_expires_at <= $2)
                   and (not_before is null or not_before <= $2)
                   and attempts < $5
                 order by event_id
                 limit $4
                 for update skip locked
             )
             update grit_cache_invalidation_outbox event
             set claimed_by = $1,
                 claim_token = nextval('grit_cache_outbox_claim_token_seq'),
                 lease_expires_at = $3,
                 attempts = event.attempts + 1
             from candidates
             where event.event_id = candidates.event_id
             returning event.event_id, event.repository_pk, event.tenant_id,
                       event.repository_id, event.event_kind, event.ref_generation,
                       event.config_generation, event.history_generation,
                       event.claimed_by, event.claim_token,
                       event.lease_expires_at, event.attempts",
        )
        .bind(&options.worker_id)
        .bind(options.observed_at)
        .bind(options.lease_expires_at)
        .bind(max_events)
        .bind(max_attempts)
        .fetch_all(&self.pool)
        .await?;
        let mut claims = rows
            .iter()
            .map(row_to_cache_outbox_claim)
            .collect::<Result<Vec<_>>>()?;
        claims.sort_unstable_by_key(|claim| claim.event.event_id);
        Ok(claims)
    }

    /// Acknowledge and remove one owned cache invalidation event.
    ///
    /// Worker identity and claim token fence stale workers. Repeating a successful acknowledgement
    /// returns [`PgCacheOutboxClaimResult::AlreadyAcknowledged`]. `observed_at` is supplied by the
    /// caller so an expired worker cannot finish work without consulting an implicit clock.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Backend`] for an invalid claim or SQLx errors from PostgreSQL.
    pub async fn acknowledge_cache_invalidation(
        &self,
        claim: &PgCacheOutboxClaim,
        observed_at: OffsetDateTime,
    ) -> Result<PgCacheOutboxClaimResult> {
        self.finish_cache_outbox_claim(claim, observed_at, PgCacheOutboxFinish::Acknowledge)
            .await
    }

    /// Release one owned event for immediate redelivery.
    ///
    /// The event remains durable and its attempt count is preserved. Worker identity and claim
    /// token fence stale workers; repeating a successful release is idempotent. `observed_at` is
    /// supplied by the caller and must still fall within the claim's lease.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Backend`] for an invalid claim or SQLx errors from PostgreSQL.
    pub async fn release_cache_invalidation(
        &self,
        claim: &PgCacheOutboxClaim,
        observed_at: OffsetDateTime,
    ) -> Result<PgCacheOutboxClaimResult> {
        self.finish_cache_outbox_claim(claim, observed_at, PgCacheOutboxFinish::Release)
            .await
    }

    /// Release one owned event for retry no earlier than a caller-supplied timestamp.
    ///
    /// The event remains durable and its attempt count is preserved. The next claim still uses
    /// its own caller-supplied observation time; PostgreSQL's wall clock is not consulted.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Backend`] for an invalid claim or SQLx errors from PostgreSQL.
    pub async fn retry_cache_invalidation(
        &self,
        claim: &PgCacheOutboxClaim,
        observed_at: OffsetDateTime,
        not_before: OffsetDateTime,
    ) -> Result<PgCacheOutboxClaimResult> {
        if not_before <= observed_at {
            return Err(Error::Backend(
                "cache outbox retry time must be after the observed time".to_owned(),
            ));
        }
        self.finish_cache_outbox_claim(claim, observed_at, PgCacheOutboxFinish::Retry(not_before))
            .await
    }

    async fn finish_cache_outbox_claim(
        &self,
        claim: &PgCacheOutboxClaim,
        observed_at: OffsetDateTime,
        finish: PgCacheOutboxFinish,
    ) -> Result<PgCacheOutboxClaimResult> {
        validate_cache_outbox_claim(claim)?;
        if observed_at >= claim.lease_expires_at {
            return Err(Error::Backend(
                "cache outbox claim lease has expired".to_owned(),
            ));
        }
        let event_id = i64::try_from(claim.event.event_id)
            .map_err(|_| Error::Backend("cache outbox event id exceeds i64".to_owned()))?;
        let claim_token = i64::try_from(claim.claim_token)
            .map_err(|_| Error::Backend("cache outbox claim token exceeds i64".to_owned()))?;
        let mut tx = self.pool.begin().await?;
        let ownership = sqlx::query(
            "select claimed_by, claim_token, lease_expires_at
             from grit_cache_invalidation_outbox
             where event_id = $1
             for update",
        )
        .bind(event_id)
        .fetch_optional(&mut *tx)
        .await?;
        let Some(ownership) = ownership else {
            tx.commit().await?;
            return Ok(PgCacheOutboxClaimResult::AlreadyAcknowledged);
        };
        let claimed_by = ownership.try_get::<Option<String>, _>("claimed_by")?;
        let stored_token = ownership.try_get::<Option<i64>, _>("claim_token")?;
        let stored_lease = ownership.try_get::<Option<OffsetDateTime>, _>("lease_expires_at")?;
        if claimed_by.is_none() && stored_token.is_none() {
            tx.commit().await?;
            return Ok(PgCacheOutboxClaimResult::AlreadyReleased);
        }
        if claimed_by.as_deref() != Some(claim.worker_id.as_str())
            || stored_token != Some(claim_token)
            || stored_lease != Some(claim.lease_expires_at)
            || stored_lease.is_some_and(|lease_expires_at| observed_at >= lease_expires_at)
        {
            tx.commit().await?;
            return Ok(PgCacheOutboxClaimResult::ClaimLost);
        }

        match finish {
            PgCacheOutboxFinish::Acknowledge => {
                sqlx::query(
                    "delete from grit_cache_invalidation_outbox
                     where event_id = $1 and claimed_by = $2 and claim_token = $3",
                )
                .bind(event_id)
                .bind(&claim.worker_id)
                .bind(claim_token)
                .execute(&mut *tx)
                .await?;
            }
            PgCacheOutboxFinish::Release => {
                sqlx::query(
                    "update grit_cache_invalidation_outbox
                     set claimed_by = null, claim_token = null, lease_expires_at = null,
                         not_before = null
                     where event_id = $1 and claimed_by = $2 and claim_token = $3",
                )
                .bind(event_id)
                .bind(&claim.worker_id)
                .bind(claim_token)
                .execute(&mut *tx)
                .await?;
            }
            PgCacheOutboxFinish::Retry(not_before) => {
                sqlx::query(
                    "update grit_cache_invalidation_outbox
                     set claimed_by = null, claim_token = null, lease_expires_at = null,
                         not_before = $4
                     where event_id = $1 and claimed_by = $2 and claim_token = $3",
                )
                .bind(event_id)
                .bind(&claim.worker_id)
                .bind(claim_token)
                .bind(not_before)
                .execute(&mut *tx)
                .await?;
            }
        }
        tx.commit().await?;
        Ok(PgCacheOutboxClaimResult::Applied)
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

    /// List a bounded page of indexed commits with stable time-keyset pagination.
    ///
    /// Results contain every commit currently present in the repository commit index, ordered by
    /// `(commit_time DESC, raw_oid ASC)`. This method does not perform a reachability walk and does
    /// not promise topological ordering. It executes at most three queries: repository identity and
    /// generation, bounded commit rows, then bounded parent edges for the selected object IDs. All
    /// three execute in one read-only repeatable-read transaction, and the query never uses
    /// `OFFSET`.
    ///
    /// The cursor is exclusive and bound to the repository's immutable numeric identity and
    /// current commit-history generation. Callers must restart pagination when a commit-index or
    /// parent-graph mutation makes a cursor stale. `page_size` and `max_parent_edges` are both
    /// mandatory backend-work bounds.
    ///
    /// # Errors
    ///
    /// Returns [`Error::RepositoryNotFound`] when the repository is absent,
    /// [`Error::HistoryCursorRepositoryMismatch`] or [`Error::StaleHistoryCursor`] when a cursor
    /// cannot continue this history, [`Error::Backend`] for invalid limits or stored graph data,
    /// or SQLx errors from PostgreSQL.
    pub async fn read_commit_history_page(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        options: PgCommitHistoryOptions,
    ) -> Result<PgCommitHistoryPage> {
        if options.page_size == 0 || options.page_size > MAX_COMMIT_HISTORY_PAGE_SIZE {
            return Err(Error::Backend(format!(
                "commit history page size must be between 1 and {MAX_COMMIT_HISTORY_PAGE_SIZE}"
            )));
        }
        if options.max_parent_edges == 0
            || options.max_parent_edges > MAX_COMMIT_HISTORY_PARENT_EDGES
        {
            return Err(Error::Backend(format!(
                "commit history parent-edge limit must be between 1 and {MAX_COMMIT_HISTORY_PARENT_EDGES}"
            )));
        }

        let mut tx = self
            .pool
            .begin_with("begin transaction isolation level repeatable read read only")
            .await?;
        let repository_row = sqlx::query(
            "select repository_pk, hash_algo, history_generation
             from grit_repositories
             where tenant_id = $1 and repository_id = $2 and deleted_at is null",
        )
        .bind(tenant.as_str())
        .bind(repository.as_str())
        .fetch_optional(&mut *tx)
        .await?;
        let Some(repository_row) = repository_row else {
            return Err(Error::RepositoryNotFound(repository_key(
                tenant, repository,
            )));
        };
        let repository_pk =
            RepositoryPk::try_from(repository_row.try_get::<i64, _>("repository_pk")?)
                .map_err(|error| Error::Backend(error.to_string()))?;
        let hash_algo_name = repository_row.try_get::<String, _>("hash_algo")?;
        let hash_algo = HashAlgo::from_name(&hash_algo_name)
            .ok_or_else(|| Error::Backend(format!("unknown hash algorithm '{hash_algo_name}'")))?;
        let history_generation = nonnegative_i64_to_u64(
            repository_row.try_get("history_generation")?,
            "history generation",
        )?;
        if let Some(cursor) = &options.cursor {
            if cursor.repository_pk != repository_pk {
                return Err(Error::HistoryCursorRepositoryMismatch {
                    cursor_repository_pk: cursor.repository_pk.get(),
                    repository_pk: repository_pk.get(),
                });
            }
            if cursor.history_generation != history_generation {
                return Err(Error::StaleHistoryCursor {
                    cursor_generation: cursor.history_generation,
                    current_generation: history_generation,
                });
            }
            if cursor.oid.algo() != hash_algo {
                return Err(Error::Backend(
                    "commit history cursor object ID has the wrong hash algorithm".to_owned(),
                ));
            }
        }

        let row_limit = options
            .page_size
            .checked_add(1)
            .and_then(|limit| i64::try_from(limit).ok())
            .ok_or_else(|| Error::Backend("commit history row limit exceeds i64".to_owned()))?;
        let mut rows = if let Some(cursor) = &options.cursor {
            sqlx::query(
                "select commit_oid_bytes, tree_oid_bytes, commit_time, generation
                 from grit_commits
                 where repository_pk = $1
                   and (commit_time < $2
                        or (commit_time = $2 and commit_oid_bytes > $3))
                 order by commit_time desc, commit_oid_bytes asc
                 limit $4",
            )
            .bind(repository_pk.get())
            .bind(cursor.commit_time)
            .bind(cursor.oid.as_bytes())
            .bind(row_limit)
            .fetch_all(&mut *tx)
            .await?
        } else {
            sqlx::query(
                "select commit_oid_bytes, tree_oid_bytes, commit_time, generation
                 from grit_commits
                 where repository_pk = $1
                 order by commit_time desc, commit_oid_bytes asc
                 limit $2",
            )
            .bind(repository_pk.get())
            .bind(row_limit)
            .fetch_all(&mut *tx)
            .await?
        };
        let exhausted = rows.len() <= options.page_size;
        rows.truncate(options.page_size);

        let mut commits = rows
            .iter()
            .map(|row| row_to_history_commit(row, hash_algo))
            .collect::<Result<Vec<_>>>()?;
        if !commits.is_empty() {
            let commit_indexes = commits
                .iter()
                .enumerate()
                .map(|(index, commit)| (commit.oid, index))
                .collect::<HashMap<_, _>>();
            let commit_oids = commits
                .iter()
                .map(|commit| commit.oid.as_bytes().to_vec())
                .collect::<Vec<_>>();
            let edge_limit = options
                .max_parent_edges
                .checked_add(1)
                .and_then(|limit| i64::try_from(limit).ok())
                .ok_or_else(|| {
                    Error::Backend("commit history parent-edge limit exceeds i64".to_owned())
                })?;
            let parent_rows = sqlx::query(
                "select commit_oid_bytes, parent_oid_bytes, parent_order
                 from grit_commit_parents
                 where repository_pk = $1 and commit_oid_bytes = any($2::bytea[])
                 order by commit_oid_bytes asc, parent_order asc
                 limit $3",
            )
            .bind(repository_pk.get())
            .bind(&commit_oids)
            .bind(edge_limit)
            .fetch_all(&mut *tx)
            .await?;
            if parent_rows.len() > options.max_parent_edges {
                return Err(Error::Backend(format!(
                    "commit history page exceeds the configured parent-edge limit {}",
                    options.max_parent_edges
                )));
            }
            for row in parent_rows {
                let commit_oid =
                    ObjectId::from_bytes(&row.try_get::<Vec<u8>, _>("commit_oid_bytes")?)?;
                let parent_oid =
                    ObjectId::from_bytes(&row.try_get::<Vec<u8>, _>("parent_oid_bytes")?)?;
                if commit_oid.algo() != hash_algo || parent_oid.algo() != hash_algo {
                    return Err(Error::Backend(
                        "commit history parent edge has the wrong hash algorithm".to_owned(),
                    ));
                }
                let commit_index = commit_indexes.get(&commit_oid).copied().ok_or_else(|| {
                    Error::Backend(
                        "commit history parent edge does not belong to the selected page"
                            .to_owned(),
                    )
                })?;
                let parent_order = usize::try_from(row.try_get::<i32, _>("parent_order")?)
                    .map_err(|_| {
                        Error::Backend("commit history parent order is negative".to_owned())
                    })?;
                if parent_order != commits[commit_index].parents.len() {
                    return Err(Error::Backend(
                        "commit history parent order is not contiguous".to_owned(),
                    ));
                }
                commits[commit_index].parents.push(parent_oid);
            }
        }

        let next_cursor = if exhausted {
            None
        } else {
            let last = commits.last().ok_or_else(|| {
                Error::Backend("non-exhausted commit history page is empty".to_owned())
            })?;
            Some(PgCommitHistoryCursor {
                repository_pk,
                history_generation,
                commit_time: last.commit_time,
                oid: last.oid,
            })
        };
        let page = PgCommitHistoryPage {
            commits,
            next_cursor,
            exhausted,
        };
        tx.commit().await?;
        Ok(page)
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
             returning repository_pk, tenant_id, repository_id, hash_algo, ref_generation,
                       history_generation, config_generation, created_at, updated_at,
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
            "select repository_pk, tenant_id, repository_id, hash_algo, ref_generation,
                    history_generation, config_generation, created_at, updated_at,
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
            "select repository_pk, tenant_id, repository_id, hash_algo, ref_generation,
                    history_generation, config_generation, created_at, updated_at,
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
             returning repository_pk, tenant_id, repository_id, hash_algo, ref_generation,
                       history_generation, config_generation, created_at, updated_at,
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
        let changed =
            set_config_in_transaction(&mut self.tx, tenant, repository, key, value).await?;
        if changed {
            bump_config_generation_in_transaction(&mut self.tx, tenant, repository).await?;
        }
        Ok(())
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
    let history_generation: i64 = row.try_get("history_generation")?;
    let config_generation: i64 = row.try_get("config_generation")?;
    Ok(PgRepositoryRow {
        repository_pk,
        tenant: TenantId::new(tenant)?,
        repository: RepositoryId::new(repository)?,
        hash_algo,
        ref_generation: u64::try_from(ref_generation)
            .map_err(|_| Error::Backend("ref generation is negative".to_owned()))?,
        history_generation: u64::try_from(history_generation)
            .map_err(|_| Error::Backend("history generation is negative".to_owned()))?,
        config_generation: u64::try_from(config_generation)
            .map_err(|_| Error::Backend("config generation is negative".to_owned()))?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
        archived_at: row.try_get("archived_at")?,
        deleted_at: row.try_get("deleted_at")?,
    })
}

fn nonnegative_i64_to_u64(value: i64, field: &str) -> Result<u64> {
    u64::try_from(value).map_err(|_| Error::Backend(format!("{field} is negative")))
}

fn row_to_history_commit(
    row: &sqlx::postgres::PgRow,
    hash_algo: HashAlgo,
) -> Result<IndexedCommit> {
    let oid = ObjectId::from_bytes(&row.try_get::<Vec<u8>, _>("commit_oid_bytes")?)?;
    let tree = ObjectId::from_bytes(&row.try_get::<Vec<u8>, _>("tree_oid_bytes")?)?;
    if oid.algo() != hash_algo || tree.algo() != hash_algo {
        return Err(Error::Backend(
            "commit history row has the wrong hash algorithm".to_owned(),
        ));
    }
    let generation = u32::try_from(row.try_get::<i32, _>("generation")?)
        .map_err(|_| Error::Backend("commit history generation is negative".to_owned()))?;
    Ok(IndexedCommit {
        oid,
        tree,
        parents: Vec::new(),
        commit_time: row.try_get("commit_time")?,
        generation,
    })
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

enum PgCacheOutboxFinish {
    Acknowledge,
    Release,
    Retry(OffsetDateTime),
}

enum PgMigrationFinish<'a> {
    Release,
    Fail(&'a str),
    Cancel(&'a str),
}

struct PgMigrationTokenColumns {
    ref_generation: Option<i64>,
    history_generation: Option<i64>,
    config_generation: Option<i64>,
    external: Option<String>,
}

fn migration_token_columns(token: &PgMigrationSourceToken) -> Result<PgMigrationTokenColumns> {
    match token {
        PgMigrationSourceToken::Postgres {
            ref_generation,
            history_generation,
            config_generation,
        } => {
            Ok(PgMigrationTokenColumns {
                ref_generation: Some(i64::try_from(*ref_generation).map_err(|_| {
                    Error::Backend("migration ref generation exceeds i64".to_owned())
                })?),
                history_generation: Some(i64::try_from(*history_generation).map_err(|_| {
                    Error::Backend("migration history generation exceeds i64".to_owned())
                })?),
                config_generation: Some(i64::try_from(*config_generation).map_err(|_| {
                    Error::Backend("migration config generation exceeds i64".to_owned())
                })?),
                external: None,
            })
        }
        PgMigrationSourceToken::External(token) => {
            if token.trim().is_empty() || token.len() > 1_024 {
                return Err(Error::Backend(
                    "external migration token must contain 1 to 1024 bytes".to_owned(),
                ));
            }
            Ok(PgMigrationTokenColumns {
                ref_generation: None,
                history_generation: None,
                config_generation: None,
                external: Some(token.clone()),
            })
        }
    }
}

fn validate_migration_source_token(
    source: &PgMigrationSource,
    token: &PgMigrationSourceToken,
) -> Result<()> {
    match (source, token) {
        (PgMigrationSource::Repository(_), PgMigrationSourceToken::Postgres { .. }) => {
            let _ = migration_token_columns(token)?;
            Ok(())
        }
        (PgMigrationSource::External(_), PgMigrationSourceToken::External(_)) => {
            let _ = migration_token_columns(token)?;
            Ok(())
        }
        _ => Err(Error::Backend(
            "migration source token has the wrong source kind".to_owned(),
        )),
    }
}

fn postgres_token_generations(token: &PgMigrationSourceToken) -> Result<(u64, u64, u64)> {
    match token {
        PgMigrationSourceToken::Postgres {
            ref_generation,
            history_generation,
            config_generation,
        } => Ok((*ref_generation, *history_generation, *config_generation)),
        PgMigrationSourceToken::External(_) => Err(Error::Backend(
            "PostgreSQL catch-up received an external source token".to_owned(),
        )),
    }
}

fn migration_journal_cursor(generation: u64, key: String) -> Result<PgMigrationJournalCursor> {
    if key.trim().is_empty() || key.len() > 1_024 {
        return Err(Error::Backend(
            "migration journal cursor key must contain 1 to 1024 bytes".to_owned(),
        ));
    }
    Ok(PgMigrationJournalCursor { generation, key })
}

fn validate_migration_catch_up_batch(
    batch: &PgMigrationCatchUpBatch,
    session: &PgMigrationSession,
) -> Result<()> {
    if batch.operations.len() > MAX_MIGRATION_CATCH_UP_BATCH {
        return Err(Error::Backend(
            "migration catch-up batch exceeds the operation limit".to_owned(),
        ));
    }
    validate_migration_source_token(&session.source, &batch.from)?;
    validate_migration_source_token(&session.source, &batch.apply_through)?;
    validate_migration_source_token(&session.source, &batch.source_observed)?;
    let from = postgres_token_generations(&batch.from)?;
    let through = postgres_token_generations(&batch.apply_through)?;
    let observed = postgres_token_generations(&batch.source_observed)?;
    let valid_cursor = |cursor: &Option<PgMigrationJournalCursor>, generation| {
        cursor.as_ref().is_none_or(|cursor| {
            cursor.generation == generation
                && !cursor.key.trim().is_empty()
                && cursor.key.len() <= 1_024
        })
    };
    if from.0 > through.0
        || from.1 > through.1
        || from.2 > through.2
        || through.0 > observed.0
        || through.1 > observed.1
        || through.2 > observed.2
        || !valid_cursor(&batch.from_ref_cursor, from.0)
        || !valid_cursor(&batch.from_config_cursor, from.2)
        || !valid_cursor(&batch.apply_through_ref_cursor, through.0)
        || !valid_cursor(&batch.apply_through_config_cursor, through.2)
    {
        return Err(Error::Backend(
            "migration catch-up token order is invalid".to_owned(),
        ));
    }
    let valid_ref = |value: &Option<StoredRef>| match value {
        Some(StoredRef::Direct(oid)) => oid.algo() == session.hash_algo,
        Some(StoredRef::Symbolic(target)) => !target.trim().is_empty(),
        None => true,
    };
    for operation in &batch.operations {
        match operation {
            PgMigrationCatchUpOperation::Object { oid }
            | PgMigrationCatchUpOperation::Pack { checksum: oid } => {
                if oid.algo() != session.hash_algo {
                    return Err(Error::Backend(
                        "migration catch-up identifier has the wrong hash algorithm".to_owned(),
                    ));
                }
            }
            PgMigrationCatchUpOperation::Ref {
                generation,
                name,
                expected,
                value,
            } => {
                if *generation < from.0
                    || *generation > through.0
                    || (*generation == from.0
                        && batch
                            .from_ref_cursor
                            .as_ref()
                            .is_none_or(|cursor| name.as_bytes() <= cursor.key.as_bytes()))
                    || (*generation == through.0
                        && batch
                            .apply_through_ref_cursor
                            .as_ref()
                            .is_some_and(|cursor| name.as_bytes() > cursor.key.as_bytes()))
                    || name.trim().is_empty()
                    || !valid_ref(expected)
                    || !valid_ref(value)
                {
                    return Err(Error::Backend(
                        "migration catch-up ref operation is invalid".to_owned(),
                    ));
                }
            }
            PgMigrationCatchUpOperation::Config {
                generation, key, ..
            } => {
                if *generation < from.2
                    || *generation > through.2
                    || (*generation == from.2
                        && batch
                            .from_config_cursor
                            .as_ref()
                            .is_none_or(|cursor| key.as_bytes() <= cursor.key.as_bytes()))
                    || (*generation == through.2
                        && batch
                            .apply_through_config_cursor
                            .as_ref()
                            .is_some_and(|cursor| key.as_bytes() > cursor.key.as_bytes()))
                    || key.trim().is_empty()
                {
                    return Err(Error::Backend(
                        "migration catch-up config operation is invalid".to_owned(),
                    ));
                }
            }
        }
    }
    Ok(())
}

fn journal_ref_value(
    row: &sqlx::postgres::PgRow,
    oid_column: &str,
    symbolic_column: &str,
) -> Result<Option<StoredRef>> {
    match (
        row.try_get::<Option<String>, _>(oid_column)?,
        row.try_get::<Option<String>, _>(symbolic_column)?,
    ) {
        (Some(oid), None) => Ok(Some(StoredRef::Direct(ObjectId::from_hex(&oid)?))),
        (None, Some(target)) => Ok(Some(StoredRef::Symbolic(target))),
        (None, None) => Ok(None),
        _ => Err(Error::Backend(
            "migration ref journal value has conflicting targets".to_owned(),
        )),
    }
}

async fn read_ref_for_update(
    tx: &mut Transaction<'static, Postgres>,
    tenant: &TenantId,
    repository: &RepositoryId,
    refname: &str,
) -> Result<Option<StoredRef>> {
    let row = sqlx::query(
        "select target_oid, symbolic_target from grit_refs
         where tenant_id = $1 and repository_id = $2 and refname = $3 for update",
    )
    .bind(tenant.as_str())
    .bind(repository.as_str())
    .bind(refname)
    .fetch_optional(&mut **tx)
    .await?;
    row.as_ref().map(|row| row_to_ref(row, refname)).transpose()
}

async fn write_catch_up_ref(
    tx: &mut Transaction<'static, Postgres>,
    tenant: &TenantId,
    repository: &RepositoryId,
    refname: &str,
    value: Option<&StoredRef>,
    observed_at: OffsetDateTime,
) -> Result<()> {
    match value {
        Some(value) => {
            let (target_oid, symbolic_target) = ref_columns(value);
            sqlx::query(
                "insert into grit_refs
                    (tenant_id, repository_id, refname, target_oid, symbolic_target, updated_at)
                 values ($1, $2, $3, $4, $5, $6)
                 on conflict (tenant_id, repository_id, refname) do update
                 set target_oid = excluded.target_oid,
                     symbolic_target = excluded.symbolic_target,
                     updated_at = excluded.updated_at",
            )
            .bind(tenant.as_str())
            .bind(repository.as_str())
            .bind(refname)
            .bind(target_oid)
            .bind(symbolic_target)
            .bind(observed_at)
            .execute(&mut **tx)
            .await?;
        }
        None => {
            sqlx::query(
                "delete from grit_refs
                 where tenant_id = $1 and repository_id = $2 and refname = $3",
            )
            .bind(tenant.as_str())
            .bind(repository.as_str())
            .bind(refname)
            .execute(&mut **tx)
            .await?;
        }
    }
    Ok(())
}

async fn write_catch_up_config(
    tx: &mut Transaction<'static, Postgres>,
    tenant: &TenantId,
    repository: &RepositoryId,
    key: &str,
    value: Option<&str>,
) -> Result<()> {
    match value {
        Some(value) => {
            sqlx::query(
                "insert into grit_config (tenant_id, repository_id, key, value)
                 values ($1, $2, $3, $4)
                 on conflict (tenant_id, repository_id, key) do update
                 set value = excluded.value",
            )
            .bind(tenant.as_str())
            .bind(repository.as_str())
            .bind(key)
            .bind(value)
            .execute(&mut **tx)
            .await?;
        }
        None => {
            sqlx::query(
                "delete from grit_config
                 where tenant_id = $1 and repository_id = $2 and key = $3",
            )
            .bind(tenant.as_str())
            .bind(repository.as_str())
            .bind(key)
            .execute(&mut **tx)
            .await?;
        }
    }
    Ok(())
}

enum PgRepositoryGeneration {
    Refs,
    Config,
}

fn verification_check(
    kind: PgMigrationVerificationCheckKind,
    passed: bool,
    source_count: Option<u64>,
    destination_count: Option<u64>,
    mismatch_count: Option<u64>,
    detail: Option<String>,
) -> PgMigrationVerificationCheck {
    PgMigrationVerificationCheck {
        kind,
        result: if passed {
            PgMigrationVerificationResult::Passed
        } else {
            PgMigrationVerificationResult::Failed
        },
        source_count,
        destination_count,
        mismatch_count,
        detail,
    }
}

async fn compare_repository_projection(
    tx: &mut Transaction<'static, Postgres>,
    kind: PgMigrationVerificationCheckKind,
    table: &str,
    key: &str,
    unequal: &str,
    source_tenant: &str,
    source_repository: &str,
    destination_tenant: &str,
    destination_repository: &str,
) -> Result<PgMigrationVerificationCheck> {
    let sql = format!(
        "select
            (select count(*) from {table} where tenant_id = $1 and repository_id = $2)
                as source_count,
            (select count(*) from {table} where tenant_id = $3 and repository_id = $4)
                as destination_count,
            (select count(*) from
                (select * from {table} where tenant_id = $1 and repository_id = $2) s
                full join
                (select * from {table} where tenant_id = $3 and repository_id = $4) d
                on s.{key} = d.{key}
             where s.{key} is null or d.{key} is null or {unequal}) as mismatch_count"
    );
    let row = sqlx::query(&sql)
        .bind(source_tenant)
        .bind(source_repository)
        .bind(destination_tenant)
        .bind(destination_repository)
        .fetch_one(&mut **tx)
        .await?;
    let source_count = nonnegative_i64_to_u64(row.try_get("source_count")?, "source count")?;
    let destination_count =
        nonnegative_i64_to_u64(row.try_get("destination_count")?, "destination count")?;
    let mismatch_count = nonnegative_i64_to_u64(row.try_get("mismatch_count")?, "mismatch count")?;
    Ok(verification_check(
        kind,
        mismatch_count == 0,
        Some(source_count),
        Some(destination_count),
        Some(mismatch_count),
        (mismatch_count > 0).then(|| "repository projections differ".to_owned()),
    ))
}

async fn compare_commit_graph(
    tx: &mut Transaction<'static, Postgres>,
    source_tenant: &str,
    source_repository: &str,
    destination_tenant: &str,
    destination_repository: &str,
) -> Result<PgMigrationVerificationCheck> {
    let commits = compare_repository_projection(
        tx,
        PgMigrationVerificationCheckKind::CommitGraph,
        "grit_commits",
        "commit_oid",
        "s.tree_oid is distinct from d.tree_oid or s.commit_time is distinct from d.commit_time or s.generation is distinct from d.generation",
        source_tenant,
        source_repository,
        destination_tenant,
        destination_repository,
    )
    .await?;
    let row = sqlx::query(
        "select
            (select count(*) from grit_commit_parents
             where tenant_id = $1 and repository_id = $2) as source_count,
            (select count(*) from grit_commit_parents
             where tenant_id = $3 and repository_id = $4) as destination_count,
            (select count(*) from
                (select commit_oid, parent_order, parent_oid from grit_commit_parents
                 where tenant_id = $1 and repository_id = $2) s
                full join
                (select commit_oid, parent_order, parent_oid from grit_commit_parents
                 where tenant_id = $3 and repository_id = $4) d
                on s.commit_oid = d.commit_oid and s.parent_order = d.parent_order
             where s.commit_oid is null or d.commit_oid is null
                or s.parent_oid is distinct from d.parent_oid) as mismatch_count",
    )
    .bind(source_tenant)
    .bind(source_repository)
    .bind(destination_tenant)
    .bind(destination_repository)
    .fetch_one(&mut **tx)
    .await?;
    let parents = verification_check(
        PgMigrationVerificationCheckKind::CommitGraph,
        row.try_get::<i64, _>("mismatch_count")? == 0,
        Some(nonnegative_i64_to_u64(
            row.try_get("source_count")?,
            "commit parent source count",
        )?),
        Some(nonnegative_i64_to_u64(
            row.try_get("destination_count")?,
            "commit parent destination count",
        )?),
        Some(nonnegative_i64_to_u64(
            row.try_get("mismatch_count")?,
            "commit parent mismatch count",
        )?),
        None,
    );
    let source_count = commits
        .source_count
        .unwrap_or_default()
        .checked_add(parents.source_count.unwrap_or_default())
        .ok_or_else(|| Error::Backend("commit graph source count overflow".to_owned()))?;
    let destination_count = commits
        .destination_count
        .unwrap_or_default()
        .checked_add(parents.destination_count.unwrap_or_default())
        .ok_or_else(|| Error::Backend("commit graph destination count overflow".to_owned()))?;
    let mismatch_count = commits
        .mismatch_count
        .unwrap_or_default()
        .checked_add(parents.mismatch_count.unwrap_or_default())
        .ok_or_else(|| Error::Backend("commit graph mismatch count overflow".to_owned()))?;
    Ok(verification_check(
        PgMigrationVerificationCheckKind::CommitGraph,
        mismatch_count == 0,
        Some(source_count),
        Some(destination_count),
        Some(mismatch_count),
        (mismatch_count > 0).then(|| "commit or ordered parent projections differ".to_owned()),
    ))
}

async fn compare_default_branch(
    tx: &mut Transaction<'static, Postgres>,
    source_tenant: &str,
    source_repository: &str,
    destination_tenant: &str,
    destination_repository: &str,
) -> Result<PgMigrationVerificationCheck> {
    let row = sqlx::query(
        "select count(*) filter (where side = 1) as source_count,
                count(*) filter (where side = 2) as destination_count,
                case when
                    (select row(target_oid, symbolic_target) from grit_refs
                     where tenant_id = $1 and repository_id = $2 and refname = 'HEAD')
                    is not distinct from
                    (select row(target_oid, symbolic_target) from grit_refs
                     where tenant_id = $3 and repository_id = $4 and refname = 'HEAD')
                then 0 else 1 end as mismatch_count
         from (values (1), (2)) sides(side)",
    )
    .bind(source_tenant)
    .bind(source_repository)
    .bind(destination_tenant)
    .bind(destination_repository)
    .fetch_one(&mut **tx)
    .await?;
    let mismatch_count = nonnegative_i64_to_u64(
        row.try_get("mismatch_count")?,
        "default branch mismatch count",
    )?;
    Ok(verification_check(
        PgMigrationVerificationCheckKind::DefaultBranch,
        mismatch_count == 0,
        None,
        None,
        Some(mismatch_count),
        (mismatch_count > 0).then(|| "HEAD values differ".to_owned()),
    ))
}

async fn compare_object_manifest(
    tx: &mut Transaction<'static, Postgres>,
    source_tenant: &str,
    source_repository: &str,
    destination_tenant: &str,
    destination_repository: &str,
) -> Result<PgMigrationVerificationCheck> {
    let row = sqlx::query(
        "with source as (
            select oid, min(kind) as kind, count(distinct kind) as kind_count from (
                select oid, kind from grit_objects where tenant_id = $1 and repository_id = $2
                union all
                select oid, kind from grit_pack_objects where tenant_id = $1 and repository_id = $2
            ) rows group by oid
         ), destination as (
            select oid, min(kind) as kind, count(distinct kind) as kind_count from (
                select oid, kind from grit_objects where tenant_id = $3 and repository_id = $4
                union all
                select oid, kind from grit_pack_objects where tenant_id = $3 and repository_id = $4
            ) rows group by oid
         ) select
            (select count(*) from source) as source_count,
            (select count(*) from destination) as destination_count,
            (select count(*) from source full join destination using (oid)
             where source.oid is null or destination.oid is null
                or source.kind_count <> 1 or destination.kind_count <> 1
                or source.kind is distinct from destination.kind) as mismatch_count",
    )
    .bind(source_tenant)
    .bind(source_repository)
    .bind(destination_tenant)
    .bind(destination_repository)
    .fetch_one(&mut **tx)
    .await?;
    let source_count = nonnegative_i64_to_u64(row.try_get("source_count")?, "object source count")?;
    let destination_count = nonnegative_i64_to_u64(
        row.try_get("destination_count")?,
        "object destination count",
    )?;
    let mismatch_count =
        nonnegative_i64_to_u64(row.try_get("mismatch_count")?, "object mismatch count")?;
    Ok(verification_check(
        PgMigrationVerificationCheckKind::ObjectManifest,
        mismatch_count == 0,
        Some(source_count),
        Some(destination_count),
        Some(mismatch_count),
        (mismatch_count > 0).then(|| "loose/packed object manifests differ".to_owned()),
    ))
}

fn unsupported_canonical_samples(samples: &[ObjectId]) -> PgMigrationVerificationCheck {
    if samples.is_empty() {
        return verification_check(
            PgMigrationVerificationCheckKind::SampleObjects,
            true,
            Some(0),
            Some(0),
            Some(0),
            None,
        );
    }
    PgMigrationVerificationCheck {
        kind: PgMigrationVerificationCheckKind::SampleObjects,
        result: PgMigrationVerificationResult::Unsupported,
        source_count: None,
        destination_count: None,
        mismatch_count: None,
        detail: Some(
            "canonical decoded object bytes are not exposed by this transactional projection; row presence and kind alone would not prove sample equality"
                .to_owned(),
        ),
    }
}

fn verification_checks_pass(
    mode: PgMigrationVerificationMode,
    has_samples: bool,
    checks: &[PgMigrationVerificationCheck],
) -> bool {
    checks.iter().all(|check| {
        let required = match check.kind {
            PgMigrationVerificationCheckKind::PeeledTags => false,
            PgMigrationVerificationCheckKind::CommitGraph => {
                mode == PgMigrationVerificationMode::Full
            }
            PgMigrationVerificationCheckKind::SampleObjects => {
                mode == PgMigrationVerificationMode::Full && has_samples
            }
            _ => true,
        };
        if required {
            check.result == PgMigrationVerificationResult::Passed
        } else {
            check.result != PgMigrationVerificationResult::Failed
        }
    })
}

async fn insert_verification_checks(
    tx: &mut Transaction<'static, Postgres>,
    report_id: i64,
    checks: &[PgMigrationVerificationCheck],
) -> Result<()> {
    let mut query = QueryBuilder::<Postgres>::new(
        "insert into grit_migration_verification_checks
            (report_id, check_kind, result, source_count, destination_count, mismatch_count, detail) ",
    );
    query.push_values(checks, |mut row, check| {
        row.push_bind(report_id)
            .push_bind(check.kind.code())
            .push_bind(check.result.code())
            .push_bind(
                check
                    .source_count
                    .and_then(|value| i64::try_from(value).ok()),
            )
            .push_bind(
                check
                    .destination_count
                    .and_then(|value| i64::try_from(value).ok()),
            )
            .push_bind(
                check
                    .mismatch_count
                    .and_then(|value| i64::try_from(value).ok()),
            )
            .push_bind(check.detail.as_deref());
    });
    query.build().persistent(false).execute(&mut **tx).await?;
    Ok(())
}

async fn bump_repository_generation_at(
    tx: &mut Transaction<'static, Postgres>,
    tenant: &TenantId,
    repository: &RepositoryId,
    kind: PgRepositoryGeneration,
    observed_at: OffsetDateTime,
) -> Result<()> {
    let row = match kind {
        PgRepositoryGeneration::Refs => sqlx::query(
            "update grit_repositories set ref_generation = ref_generation + 1, updated_at = $3
             where tenant_id = $1 and repository_id = $2 and deleted_at is null
             returning repository_pk, ref_generation as generation",
        ),
        PgRepositoryGeneration::Config => sqlx::query(
            "update grit_repositories set config_generation = config_generation + 1, updated_at = $3
             where tenant_id = $1 and repository_id = $2 and deleted_at is null
             returning repository_pk, config_generation as generation",
        ),
    }
    .bind(tenant.as_str())
    .bind(repository.as_str())
    .bind(observed_at)
    .fetch_optional(&mut **tx)
    .await?
    .ok_or_else(|| Error::RepositoryNotFound(repository_key(tenant, repository)))?;
    let repository_pk = RepositoryPk::try_from(row.try_get::<i64, _>("repository_pk")?)
        .map_err(|error| Error::Backend(error.to_string()))?;
    let generation = nonnegative_i64_to_u64(row.try_get("generation")?, "repository generation")?;
    let event = match kind {
        PgRepositoryGeneration::Refs => {
            PgCacheInvalidationKind::RefGenerationAdvanced { generation }
        }
        PgRepositoryGeneration::Config => {
            PgCacheInvalidationKind::ConfigGenerationAdvanced { generation }
        }
    };
    enqueue_cache_invalidation_in_transaction(tx, repository_pk, tenant, repository, &event).await
}

fn validate_pack_maintenance_policy(policy: PgPackMaintenancePolicy) -> Result<()> {
    if policy.min_pack_count == 0
        || i32::try_from(policy.min_pack_count).is_err()
        || i64::try_from(policy.max_loose_count).is_err()
        || i64::try_from(policy.min_fragmented_bytes).is_err()
    {
        return Err(Error::Backend("invalid pack maintenance policy".to_owned()));
    }
    Ok(())
}

fn validate_pack_maintenance_claim(
    claim: &PgPackMaintenanceClaim,
    observed_at: OffsetDateTime,
) -> Result<()> {
    if claim.job.job_id == 0
        || claim.worker_id.trim().is_empty()
        || claim.worker_id.len() > 256
        || claim.fencing_token == 0
        || claim.job.fencing_token != claim.fencing_token
        || claim.job.claimed_by.as_deref() != Some(claim.worker_id.as_str())
        || claim.job.lease_expires_at != Some(claim.lease_expires_at)
        || observed_at < claim.job.updated_at
        || observed_at >= claim.lease_expires_at
    {
        return Err(Error::Backend("invalid pack maintenance claim".to_owned()));
    }
    Ok(())
}

fn maintenance_claim_matches(
    job: &PgPackMaintenanceJob,
    claim: &PgPackMaintenanceClaim,
    observed_at: OffsetDateTime,
) -> bool {
    job.repository == claim.job.repository
        && job.claimed_by.as_deref() == Some(claim.worker_id.as_str())
        && job.fencing_token == claim.fencing_token
        && job.lease_expires_at == Some(claim.lease_expires_at)
        && observed_at < claim.lease_expires_at
}

fn validate_superseded_packs(
    replacement: &ObjectId,
    superseded: &[ObjectId],
) -> Result<Vec<Vec<u8>>> {
    if superseded.is_empty() || superseded.len() > MAX_PACK_MAINTENANCE_BATCH {
        return Err(Error::Backend("invalid superseded pack bound".to_owned()));
    }
    let mut unique = HashSet::with_capacity(superseded.len());
    let values = superseded
        .iter()
        .map(|checksum| checksum.as_bytes().to_vec())
        .collect::<Vec<_>>();
    if values.iter().any(|checksum| {
        checksum.as_slice() == replacement.as_bytes() || !unique.insert(checksum.clone())
    }) {
        return Err(Error::Backend("invalid superseded pack set".to_owned()));
    }
    Ok(values)
}

async fn pack_replacement_covers(
    tx: &mut Transaction<'static, Postgres>,
    repository: RepositoryPk,
    replacement: &ObjectId,
    superseded: &[Vec<u8>],
) -> Result<bool> {
    let missing: i64 = sqlx::query_scalar(
        "select count(*) from (
             select distinct oid_bytes from grit_pack_objects
             where repository_pk = $1 and pack_checksum_bytes = any($2::bytea[])
             except select oid_bytes from grit_pack_objects
             where repository_pk = $1 and pack_checksum_bytes = $3
         ) missing",
    )
    .bind(repository.get())
    .bind(superseded)
    .bind(replacement.as_bytes())
    .fetch_one(&mut **tx)
    .await?;
    Ok(missing == 0)
}

async fn pack_indexes_match_metadata(
    tx: &mut Transaction<'static, Postgres>,
    repository: RepositoryPk,
    checksums: &[Vec<u8>],
) -> Result<bool> {
    let mismatches: i64 = sqlx::query_scalar(
        "select count(*) from grit_packs pack
         where pack.repository_pk = $1 and pack.pack_checksum_bytes = any($2::bytea[])
           and pack.object_count <> (
               select count(*) from grit_pack_objects indexed
               where indexed.repository_pk = pack.repository_pk
                 and indexed.pack_checksum_bytes = pack.pack_checksum_bytes)",
    )
    .bind(repository.get())
    .bind(checksums)
    .fetch_one(&mut **tx)
    .await?;
    Ok(mismatches == 0)
}

async fn pack_replacement_covers_snapshot(
    tx: &mut Transaction<'static, Postgres>,
    job_id: u64,
    repository: RepositoryPk,
    replacement: &ObjectId,
) -> Result<bool> {
    let missing: i64 = sqlx::query_scalar(
        "select count(*) from (
             select oid from grit_pack_maintenance_objects where job_id = $1
             except select oid_bytes from grit_pack_objects
             where repository_pk = $2 and pack_checksum_bytes = $3
         ) missing",
    )
    .bind(
        i64::try_from(job_id)
            .map_err(|_| Error::Backend("maintenance job id exceeds i64".to_owned()))?,
    )
    .bind(repository.get())
    .bind(replacement.as_bytes())
    .fetch_one(&mut **tx)
    .await?;
    Ok(missing == 0)
}

async fn pack_snapshot_matches_live_superseded(
    tx: &mut Transaction<'static, Postgres>,
    job_id: u64,
    repository: RepositoryPk,
    superseded: &[Vec<u8>],
) -> Result<bool> {
    let job_id = i64::try_from(job_id)
        .map_err(|_| Error::Backend("maintenance job id exceeds i64".to_owned()))?;
    let mismatches: i64 = sqlx::query_scalar(
        "select count(*) from (
             (select oid from grit_pack_maintenance_objects where job_id = $1
              except select distinct oid_bytes from grit_pack_objects
              where repository_pk = $2 and pack_checksum_bytes = any($3::bytea[]))
             union all
             (select distinct oid_bytes from grit_pack_objects
              where repository_pk = $2 and pack_checksum_bytes = any($3::bytea[])
              except select oid from grit_pack_maintenance_objects where job_id = $1)
         ) mismatches",
    )
    .bind(job_id)
    .bind(repository.get())
    .bind(superseded)
    .fetch_one(&mut **tx)
    .await?;
    Ok(mismatches == 0)
}

fn row_to_pack_maintenance_job(row: &sqlx::postgres::PgRow) -> Result<PgPackMaintenanceJob> {
    let replacement = row
        .try_get::<Option<Vec<u8>>, _>("replacement_pack_checksum")?
        .map(|bytes| ObjectId::from_bytes(&bytes))
        .transpose()?;
    let grace_seconds: i64 = row.try_get("grace_seconds")?;
    if grace_seconds < 0 {
        return Err(Error::Backend("negative maintenance grace".to_owned()));
    }
    Ok(PgPackMaintenanceJob {
        job_id: nonnegative_i64_to_u64(row.try_get("job_id")?, "maintenance job id")?,
        repository: RepositoryPk::try_from(row.try_get::<i64, _>("repository_pk")?)
            .map_err(|error| Error::Backend(error.to_string()))?,
        ref_generation: nonnegative_i64_to_u64(
            row.try_get("ref_generation")?,
            "maintenance ref generation",
        )?,
        history_generation: nonnegative_i64_to_u64(
            row.try_get("history_generation")?,
            "maintenance history generation",
        )?,
        phase: PgPackMaintenancePhase::from_code(row.try_get("phase")?)?,
        policy: PgPackMaintenancePolicy {
            min_pack_count: u32::try_from(row.try_get::<i32, _>("min_pack_count")?)
                .map_err(|_| Error::Backend("invalid maintenance pack threshold".to_owned()))?,
            max_loose_count: nonnegative_i64_to_u64(
                row.try_get("max_loose_count")?,
                "maintenance loose threshold",
            )?,
            min_fragmented_bytes: nonnegative_i64_to_u64(
                row.try_get("min_fragmented_bytes")?,
                "maintenance byte threshold",
            )?,
        },
        grace: Duration::seconds(grace_seconds),
        replacement,
        prune_after: row.try_get("prune_after")?,
        claimed_by: row.try_get("claimed_by")?,
        fencing_token: nonnegative_i64_to_u64(
            row.try_get("fencing_token")?,
            "maintenance fencing token",
        )?,
        lease_expires_at: row.try_get("lease_expires_at")?,
        attempt_count: u32::try_from(row.try_get::<i32, _>("attempt_count")?)
            .map_err(|_| Error::Backend("invalid maintenance attempt count".to_owned()))?,
        updated_at: row.try_get("updated_at")?,
    })
}

fn validate_migration_create_options(options: &PgMigrationCreateOptions) -> Result<()> {
    if options.idempotency_key.trim().is_empty() || options.idempotency_key.len() > 256 {
        return Err(Error::Backend(
            "migration idempotency key must contain 1 to 256 bytes".to_owned(),
        ));
    }
    if let PgMigrationSource::External(identity) = &options.source {
        if identity.trim().is_empty() || identity.len() > 1_024 {
            return Err(Error::Backend(
                "external migration source identity must contain 1 to 1024 bytes".to_owned(),
            ));
        }
    }
    Ok(())
}

fn validate_migration_claim_options(options: &PgMigrationClaimOptions) -> Result<()> {
    if options.worker_id.trim().is_empty() || options.worker_id.len() > 256 {
        return Err(Error::Backend(
            "migration worker identity must contain 1 to 256 bytes".to_owned(),
        ));
    }
    if options.max_sessions == 0 || options.max_sessions > MAX_MIGRATION_CLAIM_BATCH {
        return Err(Error::Backend(format!(
            "migration claim batch must be between 1 and {MAX_MIGRATION_CLAIM_BATCH}"
        )));
    }
    if options.lease_expires_at <= options.observed_at {
        return Err(Error::Backend(
            "migration lease expiry must be after the observed time".to_owned(),
        ));
    }
    Ok(())
}

fn validate_migration_claim(claim: &PgMigrationClaim, observed_at: OffsetDateTime) -> Result<()> {
    if claim.session.migration_id == 0
        || claim.fencing_token == 0
        || claim.session.attempt_count == 0
        || claim.worker_id.trim().is_empty()
        || claim.worker_id.len() > 256
    {
        return Err(Error::Backend(
            "migration claim identifiers, attempt count, and worker must be valid".to_owned(),
        ));
    }
    if claim.session.state != PgMigrationState::Active
        || claim.session.fencing_token != claim.fencing_token
        || claim.session.claimed_by.as_deref() != Some(claim.worker_id.as_str())
        || claim.session.lease_expires_at != Some(claim.lease_expires_at)
    {
        return Err(Error::Backend(
            "migration claim does not match its session lease".to_owned(),
        ));
    }
    if observed_at < claim.session.updated_at || observed_at >= claim.lease_expires_at {
        return Err(Error::Backend(
            "migration mutation time must be within the current lease".to_owned(),
        ));
    }
    Ok(())
}

fn validate_migration_checkpoint(
    checkpoint: &PgMigrationCheckpoint,
    hash_algo: HashAlgo,
) -> Result<()> {
    if checkpoint
        .object
        .as_ref()
        .is_some_and(|cursor| cursor.oid.algo() != hash_algo)
        || checkpoint
            .tree
            .as_ref()
            .is_some_and(|cursor| cursor.oid.algo() != hash_algo)
        || checkpoint
            .pack
            .as_ref()
            .is_some_and(|cursor| cursor.checksum.algo() != hash_algo)
    {
        return Err(Error::Backend(
            "migration checkpoint digest has the wrong hash algorithm".to_owned(),
        ));
    }
    if checkpoint
        .reference
        .as_ref()
        .is_some_and(|cursor| cursor.name.trim().is_empty() || cursor.name.len() > 1_024)
    {
        return Err(Error::Backend(
            "migration ref cursor must contain 1 to 1024 bytes".to_owned(),
        ));
    }
    let _ = checkpoint_counts(checkpoint)?;
    Ok(())
}

fn validate_migration_terminal_reason(reason: &str) -> Result<()> {
    if reason.trim().is_empty() || reason.len() > 4_096 {
        return Err(Error::Backend(
            "migration terminal reason must contain 1 to 4096 bytes".to_owned(),
        ));
    }
    Ok(())
}

fn checkpoint_counts(checkpoint: &PgMigrationCheckpoint) -> Result<[i64; 5]> {
    [
        checkpoint.completed_objects,
        checkpoint.completed_refs,
        checkpoint.completed_trees,
        checkpoint.completed_packs,
        checkpoint.completed_bytes,
    ]
    .map(|value| {
        i64::try_from(value)
            .map_err(|_| Error::Backend("migration checkpoint counter exceeds i64".to_owned()))
    })
    .into_iter()
    .collect::<Result<Vec<_>>>()?
    .try_into()
    .map_err(|_| Error::Backend("migration checkpoint counter shape is invalid".to_owned()))
}

fn positive_u64_to_i64(value: u64, field: &str) -> Result<i64> {
    if value == 0 {
        return Err(Error::Backend(format!("{field} must be positive")));
    }
    i64::try_from(value).map_err(|_| Error::Backend(format!("{field} exceeds i64")))
}

fn migration_claim_from_session(session: PgMigrationSession) -> Result<PgMigrationClaim> {
    let worker_id = session
        .claimed_by
        .clone()
        .ok_or_else(|| Error::Backend("claimed migration session has no worker".to_owned()))?;
    let lease_expires_at = session.lease_expires_at.ok_or_else(|| {
        Error::Backend("claimed migration session has no lease expiry".to_owned())
    })?;
    if session.fencing_token == 0 || session.attempt_count == 0 {
        return Err(Error::Backend(
            "claimed migration session has invalid fencing metadata".to_owned(),
        ));
    }
    Ok(PgMigrationClaim {
        worker_id,
        fencing_token: session.fencing_token,
        lease_expires_at,
        session,
    })
}

fn migration_tokens_from_row(
    row: &sqlx::postgres::PgRow,
    source: &PgMigrationSource,
) -> Result<(
    Option<PgMigrationSourceToken>,
    Option<PgMigrationSourceToken>,
)> {
    let snapshot_generations = (
        row.try_get::<Option<i64>, _>("source_snapshot_ref_generation")?,
        row.try_get::<Option<i64>, _>("source_snapshot_history_generation")?,
        row.try_get::<Option<i64>, _>("source_snapshot_config_generation")?,
    );
    let applied_generations = (
        row.try_get::<Option<i64>, _>("applied_ref_generation")?,
        row.try_get::<Option<i64>, _>("applied_history_generation")?,
        row.try_get::<Option<i64>, _>("applied_config_generation")?,
    );
    let snapshot_external: Option<String> = row.try_get("source_snapshot_external_token")?;
    let applied_external: Option<String> = row.try_get("applied_external_token")?;
    let postgres_token = |values: (Option<i64>, Option<i64>, Option<i64>)| match values {
        (None, None, None) => Ok(None),
        (Some(refs), Some(history), Some(config)) => Ok(Some(PgMigrationSourceToken::Postgres {
            ref_generation: nonnegative_i64_to_u64(refs, "migration token ref generation")?,
            history_generation: nonnegative_i64_to_u64(
                history,
                "migration token history generation",
            )?,
            config_generation: nonnegative_i64_to_u64(config, "migration token config generation")?,
        })),
        _ => Err(Error::Backend(
            "stored migration PostgreSQL token is incomplete".to_owned(),
        )),
    };
    match source {
        PgMigrationSource::Repository(_)
            if snapshot_external.is_none() && applied_external.is_none() =>
        {
            Ok((
                postgres_token(snapshot_generations)?,
                postgres_token(applied_generations)?,
            ))
        }
        PgMigrationSource::External(_)
            if snapshot_generations == (None, None, None)
                && applied_generations == (None, None, None) =>
        {
            let snapshot = snapshot_external.map(PgMigrationSourceToken::External);
            let applied = applied_external.map(PgMigrationSourceToken::External);
            Ok((snapshot, applied))
        }
        _ => Err(Error::Backend(
            "stored migration token does not match its source identity".to_owned(),
        )),
    }
}

fn row_to_migration_session(row: &sqlx::postgres::PgRow) -> Result<PgMigrationSession> {
    let migration_id = nonnegative_i64_to_u64(row.try_get("migration_id")?, "migration id")?;
    if migration_id == 0 {
        return Err(Error::Backend("migration id must be positive".to_owned()));
    }
    let source = match (
        row.try_get::<Option<i64>, _>("source_repository_pk")?,
        row.try_get::<Option<String>, _>("source_external_id")?,
    ) {
        (Some(repository_pk), None) => PgMigrationSource::Repository(
            RepositoryPk::try_from(repository_pk)
                .map_err(|error| Error::Backend(error.to_string()))?,
        ),
        (None, Some(identity)) if !identity.trim().is_empty() && identity.len() <= 1_024 => {
            PgMigrationSource::External(identity)
        }
        _ => {
            return Err(Error::Backend(
                "invalid migration source identity".to_owned(),
            ))
        }
    };
    let (source_snapshot, last_applied) = migration_tokens_from_row(row, &source)?;
    let applied_ref_name: Option<String> = row.try_get("applied_ref_name")?;
    let applied_config_key: Option<String> = row.try_get("applied_config_key")?;
    let (last_applied_ref_cursor, last_applied_config_cursor) = match &last_applied {
        Some(PgMigrationSourceToken::Postgres {
            ref_generation,
            config_generation,
            ..
        }) => (
            applied_ref_name
                .map(|key| migration_journal_cursor(*ref_generation, key))
                .transpose()?,
            applied_config_key
                .map(|key| migration_journal_cursor(*config_generation, key))
                .transpose()?,
        ),
        _ if applied_ref_name.is_none() && applied_config_key.is_none() => (None, None),
        _ => {
            return Err(Error::Backend(
                "stored migration journal cursor has no PostgreSQL token".to_owned(),
            ));
        }
    };
    let hash_algo_name: String = row.try_get("hash_algo")?;
    let hash_algo = HashAlgo::from_name(&hash_algo_name).ok_or_else(|| {
        Error::Backend(format!(
            "unknown migration hash algorithm '{hash_algo_name}'"
        ))
    })?;
    let object = row
        .try_get::<Option<Vec<u8>>, _>("last_object_oid")?
        .map(|bytes| ObjectId::from_bytes(&bytes).map(|oid| PgMigrationObjectCursor { oid }))
        .transpose()?;
    let tree = row
        .try_get::<Option<Vec<u8>>, _>("last_tree_oid")?
        .map(|bytes| ObjectId::from_bytes(&bytes).map(|oid| PgMigrationTreeCursor { oid }))
        .transpose()?;
    let pack = row
        .try_get::<Option<Vec<u8>>, _>("last_pack_checksum")?
        .map(|bytes| {
            ObjectId::from_bytes(&bytes).map(|checksum| PgMigrationPackCursor { checksum })
        })
        .transpose()?;
    if object
        .as_ref()
        .is_some_and(|cursor| cursor.oid.algo() != hash_algo)
        || tree
            .as_ref()
            .is_some_and(|cursor| cursor.oid.algo() != hash_algo)
        || pack
            .as_ref()
            .is_some_and(|cursor| cursor.checksum.algo() != hash_algo)
    {
        return Err(Error::Backend(
            "stored migration cursor has the wrong hash algorithm".to_owned(),
        ));
    }
    let claimed_by = row.try_get::<Option<String>, _>("claimed_by")?;
    let lease_expires_at = row.try_get::<Option<OffsetDateTime>, _>("lease_expires_at")?;
    if claimed_by.is_some() != lease_expires_at.is_some() {
        return Err(Error::Backend(
            "stored migration lease columns disagree".to_owned(),
        ));
    }
    let idempotency_key: String = row.try_get("idempotency_key")?;
    if idempotency_key.trim().is_empty() || idempotency_key.len() > 256 {
        return Err(Error::Backend(
            "stored migration idempotency key is invalid".to_owned(),
        ));
    }
    let reference = row
        .try_get::<Option<String>, _>("last_ref_name")?
        .map(|name| {
            if name.trim().is_empty() || name.len() > 1_024 {
                return Err(Error::Backend(
                    "stored migration ref cursor is invalid".to_owned(),
                ));
            }
            Ok(PgMigrationRefCursor { name })
        })
        .transpose()?;
    let terminal_reason: Option<String> = row.try_get("terminal_reason")?;
    let state = PgMigrationState::from_code(row.try_get("state")?)?;
    match (state, terminal_reason.as_deref()) {
        (PgMigrationState::Active, None) => {}
        (PgMigrationState::Failed | PgMigrationState::Cancelled, Some(reason)) => {
            validate_migration_terminal_reason(reason)?;
        }
        _ => {
            return Err(Error::Backend(
                "stored migration terminal state is invalid".to_owned(),
            ));
        }
    }
    let created_at: OffsetDateTime = row.try_get("created_at")?;
    let updated_at: OffsetDateTime = row.try_get("updated_at")?;
    if updated_at < created_at {
        return Err(Error::Backend(
            "stored migration timestamps are not monotonic".to_owned(),
        ));
    }
    let session = PgMigrationSession {
        migration_id,
        idempotency_key,
        source,
        destination_repository: RepositoryPk::try_from(
            row.try_get::<i64, _>("destination_repository_pk")?,
        )
        .map_err(|error| Error::Backend(error.to_string()))?,
        hash_algo,
        phase: PgMigrationPhase::from_code(row.try_get("phase")?)?,
        state,
        fencing_token: nonnegative_i64_to_u64(
            row.try_get("fencing_token")?,
            "migration fencing token",
        )?,
        claimed_by,
        lease_expires_at,
        attempt_count: u32::try_from(row.try_get::<i32, _>("attempt_count")?)
            .map_err(|_| Error::Backend("migration attempt count is negative".to_owned()))?,
        checkpoint: PgMigrationCheckpoint {
            object,
            reference,
            tree,
            pack,
            completed_objects: nonnegative_i64_to_u64(
                row.try_get("completed_objects")?,
                "completed migration objects",
            )?,
            completed_refs: nonnegative_i64_to_u64(
                row.try_get("completed_refs")?,
                "completed migration refs",
            )?,
            completed_trees: nonnegative_i64_to_u64(
                row.try_get("completed_trees")?,
                "completed migration trees",
            )?,
            completed_packs: nonnegative_i64_to_u64(
                row.try_get("completed_packs")?,
                "completed migration packs",
            )?,
            completed_bytes: nonnegative_i64_to_u64(
                row.try_get("completed_bytes")?,
                "completed migration bytes",
            )?,
        },
        source_snapshot,
        last_applied,
        last_applied_ref_cursor,
        last_applied_config_cursor,
        terminal_reason,
        created_at,
        updated_at,
    };
    if session.claimed_by.is_some()
        && (session.state != PgMigrationState::Active
            || session.fencing_token == 0
            || session.attempt_count == 0
            || session
                .lease_expires_at
                .is_some_and(|lease_expires_at| lease_expires_at <= session.updated_at))
    {
        return Err(Error::Backend(
            "stored migration lease is invalid".to_owned(),
        ));
    }
    Ok(session)
}

fn validate_cache_outbox_claim_options(options: &PgCacheOutboxClaimOptions) -> Result<()> {
    if options.worker_id.trim().is_empty() {
        return Err(Error::Backend(
            "cache outbox worker identity must not be empty".to_owned(),
        ));
    }
    if options.max_events == 0 || options.max_events > MAX_CACHE_OUTBOX_CLAIM_BATCH {
        return Err(Error::Backend(format!(
            "cache outbox batch size must be between 1 and {MAX_CACHE_OUTBOX_CLAIM_BATCH}"
        )));
    }
    if options.max_attempts == 0 || options.max_attempts > i32::MAX as u32 {
        return Err(Error::Backend(
            "cache outbox maximum attempts must be between 1 and i32::MAX".to_owned(),
        ));
    }
    if options.lease_expires_at <= options.observed_at {
        return Err(Error::Backend(
            "cache outbox lease expiry must be after the observed time".to_owned(),
        ));
    }
    Ok(())
}

fn validate_cache_outbox_claim(claim: &PgCacheOutboxClaim) -> Result<()> {
    if claim.worker_id.trim().is_empty() {
        return Err(Error::Backend(
            "cache outbox worker identity must not be empty".to_owned(),
        ));
    }
    if claim.event.event_id == 0 || claim.claim_token == 0 || claim.attempts == 0 {
        return Err(Error::Backend(
            "cache outbox claim identifiers and attempt count must be positive".to_owned(),
        ));
    }
    Ok(())
}

fn row_to_cache_outbox_claim(row: &sqlx::postgres::PgRow) -> Result<PgCacheOutboxClaim> {
    let event_id = nonnegative_i64_to_u64(row.try_get("event_id")?, "cache outbox event id")?;
    if event_id == 0 {
        return Err(Error::Backend(
            "cache outbox event id must be positive".to_owned(),
        ));
    }
    let repository_pk = RepositoryPk::try_from(row.try_get::<i64, _>("repository_pk")?)
        .map_err(|error| Error::Backend(error.to_string()))?;
    let tenant = TenantId::new(row.try_get::<String, _>("tenant_id")?)?;
    let repository = RepositoryId::new(row.try_get::<String, _>("repository_id")?)?;
    let kind_code = row.try_get::<i16, _>("event_kind")?;
    let ref_generation = row.try_get::<Option<i64>, _>("ref_generation")?;
    let config_generation = row.try_get::<Option<i64>, _>("config_generation")?;
    let history_generation = row.try_get::<Option<i64>, _>("history_generation")?;
    let kind = match (
        kind_code,
        ref_generation,
        config_generation,
        history_generation,
    ) {
        (1, Some(generation), None, None) => PgCacheInvalidationKind::RefGenerationAdvanced {
            generation: nonnegative_i64_to_u64(generation, "outbox ref generation")?,
        },
        (2, None, Some(generation), None) => PgCacheInvalidationKind::ConfigGenerationAdvanced {
            generation: nonnegative_i64_to_u64(generation, "outbox config generation")?,
        },
        (3, None, None, Some(generation)) => PgCacheInvalidationKind::HistoryGenerationAdvanced {
            generation: nonnegative_i64_to_u64(generation, "outbox history generation")?,
        },
        (4, None, None, None) => PgCacheInvalidationKind::RepositoryDeleted,
        _ => {
            return Err(Error::Backend(format!(
                "invalid cache outbox payload for event kind {kind_code}"
            )));
        }
    };
    let worker_id = row
        .try_get::<Option<String>, _>("claimed_by")?
        .ok_or_else(|| Error::Backend("claimed cache outbox row has no worker".to_owned()))?;
    let claim_token = nonnegative_i64_to_u64(
        row.try_get::<Option<i64>, _>("claim_token")?
            .ok_or_else(|| Error::Backend("claimed cache outbox row has no token".to_owned()))?,
        "cache outbox claim token",
    )?;
    let lease_expires_at = row
        .try_get::<Option<OffsetDateTime>, _>("lease_expires_at")?
        .ok_or_else(|| Error::Backend("claimed cache outbox row has no lease expiry".to_owned()))?;
    let attempts = u32::try_from(row.try_get::<i32, _>("attempts")?)
        .map_err(|_| Error::Backend("cache outbox attempts are negative".to_owned()))?;
    let claim = PgCacheOutboxClaim {
        event: PgCacheInvalidationEvent {
            event_id,
            repository_pk,
            tenant,
            repository,
            kind,
        },
        worker_id,
        claim_token,
        lease_expires_at,
        attempts,
    };
    validate_cache_outbox_claim(&claim)?;
    Ok(claim)
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
         returning repository_pk, tenant_id, repository_id, hash_algo, ref_generation,
                   history_generation, config_generation, created_at, updated_at,
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
    let row: Option<i64> = sqlx::query_scalar(
        "select repository_pk from grit_repositories
         where tenant_id = $1 and repository_id = $2 and deleted_at is null
         for update",
    )
    .bind(tenant.as_str())
    .bind(repository.as_str())
    .fetch_optional(&mut **tx)
    .await?;
    let Some(repository_pk) = row else {
        return Err(Error::RepositoryNotFound(repository_key(
            tenant, repository,
        )));
    };
    let repository_pk =
        RepositoryPk::try_from(repository_pk).map_err(|error| Error::Backend(error.to_string()))?;
    enqueue_cache_invalidation_in_transaction(
        tx,
        repository_pk,
        tenant,
        repository,
        &PgCacheInvalidationKind::RepositoryDeleted,
    )
    .await?;

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

async fn repository_identity_by_pk(
    pool: &PgPool,
    repository_pk: RepositoryPk,
) -> Result<(TenantId, RepositoryId)> {
    let row = sqlx::query(
        "select tenant_id, repository_id from grit_repositories
         where repository_pk = $1 and deleted_at is null",
    )
    .bind(repository_pk.get())
    .fetch_optional(pool)
    .await?
    .ok_or_else(|| Error::Backend("migration repository no longer exists".to_owned()))?;
    Ok((
        TenantId::new(row.try_get::<String, _>("tenant_id")?)?,
        RepositoryId::new(row.try_get::<String, _>("repository_id")?)?,
    ))
}

async fn lock_expected_repository(
    tx: &mut Transaction<'static, Postgres>,
    expected_pk: RepositoryPk,
    tenant: &TenantId,
    repository: &RepositoryId,
) -> Result<bool> {
    lock_import_repository(tx, tenant, repository).await?;
    let actual_pk: Option<i64> = sqlx::query_scalar(
        "select repository_pk from grit_repositories
         where tenant_id = $1 and repository_id = $2 and deleted_at is null",
    )
    .bind(tenant.as_str())
    .bind(repository.as_str())
    .fetch_optional(&mut **tx)
    .await?;
    Ok(actual_pk == Some(expected_pk.get()))
}

async fn lock_migration_repository_pair(
    tx: &mut Transaction<'static, Postgres>,
    source_pk: RepositoryPk,
    source: &(TenantId, RepositoryId),
    destination_pk: RepositoryPk,
    destination: &(TenantId, RepositoryId),
) -> Result<bool> {
    let mut repositories = [
        (source_pk, &source.0, &source.1),
        (destination_pk, &destination.0, &destination.1),
    ];
    repositories.sort_unstable_by(|left, right| {
        (left.1.as_str(), left.2.as_str()).cmp(&(right.1.as_str(), right.2.as_str()))
    });
    for (_, tenant, repository) in &repositories {
        lock_repository_name(tx, tenant, repository).await?;
    }
    for (expected_pk, tenant, repository) in repositories {
        let actual_pk: Option<i64> = sqlx::query_scalar(
            "select repository_pk from grit_repositories
             where tenant_id = $1 and repository_id = $2 and deleted_at is null for update",
        )
        .bind(tenant.as_str())
        .bind(repository.as_str())
        .fetch_optional(&mut **tx)
        .await?;
        if actual_pk != Some(expected_pk.get()) {
            return Ok(false);
        }
    }
    Ok(true)
}

async fn repository_generations_for_update(
    tx: &mut Transaction<'static, Postgres>,
    repository_pk: RepositoryPk,
) -> Result<(i64, i64, i64)> {
    let row = sqlx::query(
        "select ref_generation, history_generation, config_generation
         from grit_repositories where repository_pk = $1 and deleted_at is null for update",
    )
    .bind(repository_pk.get())
    .fetch_optional(&mut **tx)
    .await?
    .ok_or_else(|| Error::Backend("migration repository no longer exists".to_owned()))?;
    Ok((
        row.try_get("ref_generation")?,
        row.try_get("history_generation")?,
        row.try_get("config_generation")?,
    ))
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

    let mut config_changed = false;
    for (key, value) in &publication.config_entries {
        config_changed |= set_config_in_transaction(tx, tenant, repository, key, value).await?;
    }
    if config_changed {
        bump_config_generation_in_transaction(tx, tenant, repository).await?;
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
    let row = sqlx::query(
        "update grit_repositories
         set ref_generation = ref_generation + 1, updated_at = now()
         where tenant_id = $1 and repository_id = $2 and deleted_at is null
         returning repository_pk, ref_generation",
    )
    .bind(tenant.as_str())
    .bind(repository.as_str())
    .fetch_optional(&mut **tx)
    .await?;
    let Some(row) = row else {
        return Err(Error::RepositoryNotFound(repository_key(
            tenant, repository,
        )));
    };
    let repository_pk = RepositoryPk::try_from(row.try_get::<i64, _>("repository_pk")?)
        .map_err(|error| Error::Backend(error.to_string()))?;
    let generation = nonnegative_i64_to_u64(row.try_get("ref_generation")?, "ref generation")?;
    enqueue_cache_invalidation_in_transaction(
        tx,
        repository_pk,
        tenant,
        repository,
        &PgCacheInvalidationKind::RefGenerationAdvanced { generation },
    )
    .await?;
    Ok(())
}

async fn bump_history_generation_in_transaction(
    tx: &mut Transaction<'static, Postgres>,
    tenant: &TenantId,
    repository: &RepositoryId,
) -> Result<()> {
    let row = sqlx::query(
        "update grit_repositories
         set history_generation = history_generation + 1, updated_at = now()
         where tenant_id = $1 and repository_id = $2 and deleted_at is null
         returning repository_pk, history_generation",
    )
    .bind(tenant.as_str())
    .bind(repository.as_str())
    .fetch_optional(&mut **tx)
    .await?;
    let Some(row) = row else {
        return Err(Error::RepositoryNotFound(repository_key(
            tenant, repository,
        )));
    };
    let repository_pk = RepositoryPk::try_from(row.try_get::<i64, _>("repository_pk")?)
        .map_err(|error| Error::Backend(error.to_string()))?;
    let generation =
        nonnegative_i64_to_u64(row.try_get("history_generation")?, "history generation")?;
    enqueue_cache_invalidation_in_transaction(
        tx,
        repository_pk,
        tenant,
        repository,
        &PgCacheInvalidationKind::HistoryGenerationAdvanced { generation },
    )
    .await?;
    Ok(())
}

async fn bump_config_generation_in_transaction(
    tx: &mut Transaction<'static, Postgres>,
    tenant: &TenantId,
    repository: &RepositoryId,
) -> Result<()> {
    let row = sqlx::query(
        "update grit_repositories
         set config_generation = config_generation + 1, updated_at = now()
         where tenant_id = $1 and repository_id = $2 and deleted_at is null
         returning repository_pk, config_generation",
    )
    .bind(tenant.as_str())
    .bind(repository.as_str())
    .fetch_optional(&mut **tx)
    .await?;
    let Some(row) = row else {
        return Err(Error::RepositoryNotFound(repository_key(
            tenant, repository,
        )));
    };
    let repository_pk = RepositoryPk::try_from(row.try_get::<i64, _>("repository_pk")?)
        .map_err(|error| Error::Backend(error.to_string()))?;
    let generation =
        nonnegative_i64_to_u64(row.try_get("config_generation")?, "config generation")?;
    enqueue_cache_invalidation_in_transaction(
        tx,
        repository_pk,
        tenant,
        repository,
        &PgCacheInvalidationKind::ConfigGenerationAdvanced { generation },
    )
    .await?;
    Ok(())
}

async fn enqueue_cache_invalidation_in_transaction(
    tx: &mut Transaction<'static, Postgres>,
    repository_pk: RepositoryPk,
    tenant: &TenantId,
    repository: &RepositoryId,
    kind: &PgCacheInvalidationKind,
) -> Result<()> {
    let (kind_code, ref_generation, config_generation, history_generation) = match kind {
        PgCacheInvalidationKind::RefGenerationAdvanced { generation } => (
            1_i16,
            Some(i64::try_from(*generation).map_err(|_| {
                Error::Backend("cache outbox ref generation exceeds i64".to_owned())
            })?),
            None,
            None,
        ),
        PgCacheInvalidationKind::ConfigGenerationAdvanced { generation } => (
            2_i16,
            None,
            Some(i64::try_from(*generation).map_err(|_| {
                Error::Backend("cache outbox config generation exceeds i64".to_owned())
            })?),
            None,
        ),
        PgCacheInvalidationKind::HistoryGenerationAdvanced { generation } => (
            3_i16,
            None,
            None,
            Some(i64::try_from(*generation).map_err(|_| {
                Error::Backend("cache outbox history generation exceeds i64".to_owned())
            })?),
        ),
        PgCacheInvalidationKind::RepositoryDeleted => (4_i16, None, None, None),
    };
    sqlx::query(
        "insert into grit_cache_invalidation_outbox
            (repository_pk, tenant_id, repository_id, event_kind,
             ref_generation, config_generation, history_generation)
         values ($1, $2, $3, $4, $5, $6, $7)",
    )
    .bind(repository_pk.get())
    .bind(tenant.as_str())
    .bind(repository.as_str())
    .bind(kind_code)
    .bind(ref_generation)
    .bind(config_generation)
    .bind(history_generation)
    .execute(&mut **tx)
    .await?;
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
) -> Result<bool> {
    lock_import_repository(tx, tenant, repository).await?;
    let rows = sqlx::query(
        "insert into grit_config (tenant_id, repository_id, key, value)
         values ($1, $2, $3, $4)
         on conflict (tenant_id, repository_id, key)
         do update set value = excluded.value
         where grit_config.value is distinct from excluded.value",
    )
    .bind(tenant.as_str())
    .bind(repository.as_str())
    .bind(key)
    .bind(value)
    .execute(&mut **tx)
    .await?;
    Ok(rows.rows_affected() == 1)
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
        let changed = set_config_in_transaction(&mut tx, tenant, repository, key, value).await?;
        if changed {
            bump_config_generation_in_transaction(&mut tx, tenant, repository).await?;
        }
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

async fn changed_commit_parent_indexes(
    tx: &mut Transaction<'static, Postgres>,
    tenant: &TenantId,
    repository: &RepositoryId,
    prepared: &[PgCommitRow<'_>],
    final_row_indexes: &[usize],
) -> Result<Vec<usize>> {
    const POSTGRES_BIND_LIMIT: usize = 65_535;
    const BINDS_PER_ROW: usize = 2;
    let indexes_by_oid = final_row_indexes
        .iter()
        .map(|&index| (prepared[index].commit.oid.to_hex(), index))
        .collect::<HashMap<_, _>>();
    let mut changed = Vec::new();

    for indexes in final_row_indexes.chunks((POSTGRES_BIND_LIMIT - BINDS_PER_ROW) / BINDS_PER_ROW) {
        let mut query = QueryBuilder::<Postgres>::new("with desired(commit_oid, parent_oids) as (");
        query.push_values(indexes, |mut values, &index| {
            values
                .push_bind(prepared[index].commit.oid.to_hex())
                .push_bind(
                    prepared[index]
                        .commit
                        .parents
                        .iter()
                        .map(ObjectId::to_hex)
                        .collect::<Vec<_>>(),
                );
        });
        query.push(
            ") select desired.commit_oid
               from desired
               where desired.parent_oids is distinct from coalesce(
                   (select array_agg(parent.parent_oid order by parent.parent_order)
                    from grit_commit_parents parent
                    where parent.tenant_id = ",
        );
        query
            .push_bind(tenant.as_str())
            .push(" and parent.repository_id = ")
            .push_bind(repository.as_str())
            .push(
                " and parent.commit_oid = desired.commit_oid),
                   array[]::text[])",
            );
        let rows = query.build().persistent(false).fetch_all(&mut **tx).await?;
        for row in rows {
            let oid: String = row.try_get("commit_oid")?;
            let index = indexes_by_oid.get(&oid).copied().ok_or_else(|| {
                Error::Backend("PostgreSQL returned an unrequested commit parent row".to_owned())
            })?;
            changed.push(index);
        }
    }
    changed.sort_unstable();
    Ok(changed)
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
        let mut history_changed = false;
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
                                generation = excluded.generation
                  where grit_commits.tree_oid is distinct from excluded.tree_oid
                     or grit_commits.commit_time is distinct from excluded.commit_time
                     or grit_commits.generation is distinct from excluded.generation",
            );
            let rows = query.build().persistent(false).execute(&mut *tx).await?;
            history_changed |= rows.rows_affected() > 0;
        }

        let changed_parent_indexes = changed_commit_parent_indexes(
            &mut tx,
            tenant,
            repository,
            &prepared,
            &final_row_indexes,
        )
        .await?;
        history_changed |= !changed_parent_indexes.is_empty();
        for indexes in changed_parent_indexes.chunks(POSTGRES_BIND_LIMIT - DELETE_FIXED_BINDS) {
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
        let estimated_parent_edges = changed_parent_indexes.len().saturating_mul(2);
        let mut parent_edges =
            Vec::with_capacity(parent_rows_per_chunk.min(estimated_parent_edges));
        for &commit_index in &changed_parent_indexes {
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
        if history_changed {
            bump_history_generation_in_transaction(&mut tx, tenant, repository).await?;
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
