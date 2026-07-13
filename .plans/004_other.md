# Database, S3, migration, and serving performance plan

## Status

Planned work only.

## Objective

Deliver a PostgreSQL/S3-backed hosting system whose migration speed, interactive latency,
operational predictability, and storage efficiency are compelling enough for users to move large
repositories to it.

This plan contains the remaining high-value work outside the dedicated transport plans:

- large clone/fetch: `002_coloning_optimizations.md`;
- large push: `003_pushing_optimizations.md`;
- shared non-memory architecture: `non_memory_optimizations.md`.

The focus here is fast migration/import, compact PostgreSQL metadata, efficient S3 access, browse
and history latency, caching, maintenance, multi-tenant fairness, observability, and safe rollout.

## Product performance principles

1. **Migrations must scale with compressed pack bytes, not Git object count.** A packed repository
   should require a small number of large transfers and bulk metadata operations.
2. **Interactive requests must have bounded backend operations.** A page or API request must not
   accidentally walk complete history or issue one query/GET per object.
3. **The database is metadata, not an unbounded object decompression engine.** Store searchable
   metadata in PostgreSQL and immutable bulk bytes in packs.
4. **S3 is best at large immutable objects and ranges.** Avoid millions of small uncompressed
   object keys when a validated pack is available.
5. **Caches accelerate known-good durable state; they do not define correctness.** Every cache is
   byte-bounded, generation-scoped, and safely bypassable.
6. **Background work must never steal all resources from user traffic.** Imports, backfills,
   repacks, and cleanup use explicit budgets and tenant fairness.
7. **Performance claims require repeatable measurements.** Every optimization ships with before and
   after phase, request, byte, and resource data.

## Scope boundaries

This document plans the following:

- migration orchestration and resumability;
- PostgreSQL schema/key/index compaction;
- bulk metadata loading;
- native pack storage in PostgreSQL and S3;
- compact/lazy tree browse metadata;
- ref, commit, tree, blob, and history read paths;
- layered caches and invalidation;
- S3 upload/range/download efficiency;
- maintenance, garbage collection, repair, and storage tiering;
- performance isolation, SLOs, dashboards, and load testing.

It references but does not duplicate implementation details for streaming upload-pack and
receive-pack.

## Current risks that discourage migration

- PostgreSQL and externalized storage do not yet advertise native source-pack import, so packed
  repositories fall back to decoded loose-object installation.
- Tree and commit metadata use row-oriented insert loops; one representative import created more
  than 5.7 million direct-tree entries.
- Child tables repeat textual tenant/repository identities and hexadecimal OIDs in every row.
- Externalized import can issue one sequential S3 PUT per loose object and stores decoded payloads
  rather than one compressed source pack.
- The row-per-tree-entry browse model creates large PostgreSQL tables and indexes even after SQL
  round trips are batched.
- Packed reads can fall back to loading complete pack bytes when a bounded backend-native range is
  unavailable.
- Cache wrappers can duplicate the complete imported object set during loose fallback.
- Import progress reports logical counts but does not yet provide stable phase timing, backend
  request counts, throughput, ETA, or actionable bottleneck information.
- There is no complete zero-downtime migration workflow for initial copy, incremental catch-up,
  verification, and cutover.

## Service-level objectives

Milestone 0 must establish hardware, dataset, region, and concurrency baselines before these become
release gates. Initial aspirational targets for a healthy regional deployment are:

| Operation | Warm target | Cold target |
| --- | ---: | ---: |
| Ref/summary API p95 | 25 ms | 75 ms |
| Commit metadata/history page p95 | 50 ms | 150 ms |
| Direct tree listing p95 | 50 ms | 200 ms |
| Small blob time-to-first-byte p95 | 100 ms | 300 ms |
| Large S3 blob time-to-first-byte p95 | 250 ms | 750 ms |
| No-change re-import | ref/manifest comparison with near-zero durable writes | same |

Clone and push SLOs belong to plans `002` and `003`. Import throughput should be expressed as
compressed MiB/s, metadata rows/s, SQL statements, and S3 operations rather than one universal
wall-time target.

Availability and isolation goals:

- one large import cannot exhaust the interactive database pool;
- one tenant cannot occupy every import, decode, S3, or maintenance worker;
- cache loss changes latency, not correctness;
- an S3 or database fault returns a typed retryable failure and cannot expose partial refs;
- p99 and queue time are visible separately from execution time.

## Target data architecture

```text
PostgreSQL
  numeric repository identity
  refs/config/import generations
  commit graph and compact searchable metadata
  pack manifests and object->pack locations
  compact tree blocks or lazy-tree metadata
  cache/outbox/maintenance state

S3-compatible store
  immutable source/durable packs
  canonical clone packs (plan 002)
  temporary migration/push quarantine
  rare loose overlay objects
  optional large-blob direct-delivery objects

L1 process cache
  tiny hot refs, commit rows, tree blocks, pack indexes

L2 Redis-compatible cache
  generation-scoped metadata and bounded hot decoded objects
```

SQL metadata is the visibility boundary. External bytes written without committed metadata are
invisible orphans and are reclaimed after a grace period.

## Priority summary

| Priority | Work | Primary outcome |
| --- | --- | --- |
| P0 | Instrument migration and serving | Know the real bottleneck and prevent regressions |
| P0 | Bulk PostgreSQL tree/commit/pack-index loads | Remove millions of SQL executions |
| P0 | Native pack import for PostgreSQL and S3 | Operations scale with packs, not objects |
| P0 | Resumable migration and cutover workflow | Large migrations become reliable and predictable |
| P1 | Compact/lazy tree blocks | Remove millions of rows and reduce browse storage |
| P1 | Numeric repository IDs and binary OIDs | Reduce table/index size and comparison cost |
| P1 | Layered generation-safe caching | Low warm latency without stale correctness bugs |
| P1 | Backend-native range/batch reads | Avoid full-pack and N+1 reads |
| P1 | Query pagination and precomputed summaries | Predictable API latency |
| P2 | Pack consolidation, GC, repair, and tiering | Stable long-term cost and performance |
| P2 | Replicas/CDN/direct blob delivery | Scale read-heavy deployments |
| P2 | Admission control and workload isolation | Protect small interactive operations |

## Milestone 0: measurement, tracing, and migration UX baseline

### Metrics

Instrument every import/migration phase:

- source discovery and ref snapshot;
- pack/index read and validation;
- reachability and structural parsing;
- source bytes read and decoded bytes by kind;
- pack uploads, loose-overlay uploads, and retries;
- SQL copy/upsert/commit time and rows by table;
- browse-index/tree-block construction;
- ref publication, prune, completion marker, and verification;
- queue time, execution time, CPU, RSS, worker utilization, and cancellation.

Instrument interactive reads:

- endpoint/query shape and page size;
- SQL statements, rows, bytes, pool wait, execution time;
- S3 GET/range-GET count, requested/returned bytes, first-byte latency;
- L1/L2 hit, miss, negative hit, singleflight wait, and eviction bytes;
- decoded pack objects, delta-base hits, and full-pack fallback count;
- response time-to-first-byte and total bytes.

### Tracing

- Give migration, import session, repository generation, HTTP request, SQL transaction, and S3
  request correlated trace identifiers.
- Keep high-cardinality repository/tenant identifiers out of metric labels; include them only in
  access-controlled traces/logs.
- Record phase transitions and terminal typed errors.

### Migration UX

- Return phase, completed/total bytes, object/pack counts, throughput, elapsed time, and ETA range.
- Explain whether the destination retained packs or used loose fallback and why.
- Surface throttling, retry, and catch-up status rather than appearing stalled.
- Provide a final verification report and a machine-readable cutover readiness state.

### Acceptance gate

- Baselines exist for small, medium, large, delta-heavy, many-ref, and many-small-file repositories.
- Results cover memory, PostgreSQL, PostgreSQL+S3, and cached wrappers.
- Performance regressions can be assigned to source CPU, SQL, S3, cache, or queueing.

## Milestone 1: bulk PostgreSQL ingestion

### Tree metadata

- Replace one-statement-per-entry insertion with `COPY` into a temporary/staging table followed by
  set-based `INSERT ... ON CONFLICT`.
- Use `QueryBuilder` chunks as a portable fallback when binary COPY is unavailable.
- Acquire repository/import locks once per transaction.
- Bound each transaction by rows and encoded bytes to avoid excessive WAL, lock duration, and
  replication lag.

### Commit graph

- Bulk stage commits and parent edges.
- Replace parent rows set-wise for only affected commits.
- Preserve parent order, generation, and commit time.
- Add missing access indexes only from measured query shapes; avoid speculative write-heavy
  indexes.

### Pack and manifest metadata

- Bulk insert pack-object index rows, imported trusted objects, and blob-size metadata.
- Validate duplicate OIDs/offsets and numeric ranges before opening the transaction.
- Reuse prepared statements and avoid hexadecimal conversions inside row loops where possible.

### Connection and transaction isolation

- Use separate database pools or pool quotas for interactive traffic and bulk/background work.
- Set bulk statement/lock timeouts and retry only idempotent phases.
- Limit concurrent bulk transactions by database health and replication/WAL pressure.

### Acceptance gate

- SQL execution count scales with bounded chunks, not rows.
- An import cannot consume every interactive connection.
- Ref publication remains a short guarded transaction after payload/metadata staging.
- Retry does not duplicate rows or corrupt parent order.

## Milestone 2: native pack import for PostgreSQL

- Implement `supports_native_pack_import` and `write_imported_packs` for `PgServerStorage`.
- Store each compatible source pack once and bulk-install its object index.
- Keep only objects outside eligible packs in the loose overlay.
- Do not duplicate pack-contained payloads in `grit_objects`.
- Make pack checksum, object count, size, hash algorithm, storage order, and validation version
  explicit metadata.
- Read one packed object through bounded `bytea` slicing or another measured range mechanism; never
  load a large complete pack per object.
- Keep full-pack read as an observable guarded fallback, not the normal path.
- Extend consistency, export, repair, object listing, manifests, and garbage collection to treat
  loose and packed forms uniformly.

### Acceptance gate

- Packed imports write O(packs + metadata chunks + loose overlay), not O(objects) payload rows.
- Database storage has no duplicate decoded payload for pack-contained objects.
- Object reads transfer bounded indexed ranges.
- Unsupported/corrupt/thin/promisor/mixed packs safely fall back before refs publish.

## Milestone 3: native pack import for externalized/S3 storage

### Upload path

- Stream or multipart-upload immutable source packs with byte/count-bounded concurrency.
- Use content-addressed final keys and idempotent `put_if_absent` semantics.
- Verify checksum and size before SQL metadata publication.
- Bulk-install pack/object-index metadata only after the final external object exists.
- Publish refs after metadata commits.

### Loose overlay

- Upload only objects not retained in eligible packs.
- Use bounded parallel PUTs, not a sequential loop.
- Preserve idempotency by OID/content-addressed key.
- Do not automatically fill Redis with every uploaded payload.

### Read path

- Require efficient range GET from production external byte stores.
- Validate and coalesce adjacent ranges without crossing pack bounds.
- Cache immutable pack indexes and hot delta bases by bytes.
- Use HEAD sparingly; trust committed SQL size/checksum metadata and sample-verify asynchronously.

### Atomicity and cleanup

- Treat SQL as the live visibility boundary.
- Track migration/session ownership of temporary keys.
- Reclaim uploaded-but-unreferenced objects only after a grace period and a live SQL-reference
  check.
- Make cleanup idempotent and safe across retries, repository deletion, and rename.

### Acceptance gate

- S3 PUT count scales with packs plus a small loose overlay.
- Reading one packed object does not download the whole pack.
- A failed import exposes no refs and leaves at most reclaimable immutable bytes.
- Retry reuses existing uploaded content.

## Milestone 4: compact PostgreSQL identity and OID storage

Current child tables repeat tenant text, repository text, and hexadecimal OIDs. Introduce compact
internal identities after bulk ingestion is stable.

### Repository identity

- Add an immutable numeric `repository_pk` to the repository row.
- Reference it from object, pack, ref, config, commit, parent, tree, import, reflog, and cache/outbox
  tables.
- Keep external tenant/repository names unique and mutable only in the repository table.
- Repository rename then updates one logical row instead of every child key.

### Object IDs

- Store OIDs as fixed-width validated `bytea` or a typed domain instead of 40/64-character hex
  text.
- Keep the repository hash algorithm on the parent repository row and enforce matching OID width.
- Convert to hex only at protocol/display boundaries.
- Use small integer codes for object kind where it reduces row/index size without sacrificing typed
  Rust APIs.

### Partitioning

- Benchmark unpartitioned compact tables first.
- If needed, hash-partition the largest append/index tables by `repository_pk` into a fixed,
  operationally manageable partition count.
- Do not create one PostgreSQL partition per repository.

### Migration

- Add compact columns/tables and dual-write.
- Backfill in small repository-key ranges with WAL/replica-lag throttling.
- Compare counts/checksums and query results.
- Switch reads by feature flag, stop dual-write, then remove old columns after a rollback window.

### Acceptance gate

- Table and index bytes per object/tree/commit fall materially from baseline.
- Rename cost is independent of child-row count.
- Query latency and write throughput do not regress.
- SHA-1/SHA-256 widths remain validated.

## Milestone 5: compact or lazy direct-tree browse index

Bulk insertion fixes round trips but not millions of tree-entry rows. Replace or supplement them
with one immutable block per unique tree.

### Tree block format

- Primary key: repository plus tree OID.
- Versioned payload containing sorted direct children.
- One contiguous name byte arena with offset/length pairs.
- Store mode and OID; derive kind from mode.
- Store blob size once by blob OID or resolve lazily.
- Use checked lengths/offsets and reject unknown format versions.

### Eager versus lazy

Measure two modes:

1. eager compact block creation during import for predictable first browse;
2. lazy parse from retained tree objects with a byte-bounded decoded block cache.

Default based on measured import and cold/warm browse tradeoffs. A hybrid may eagerly prepare only
trees reachable from current refs and lazily cache historical trees.

### Query path

- Fetch one block by primary key.
- Exact lookup uses binary search.
- Prefix range uses two partition points.
- Path traversal caches each resolved tree block and avoids N+1 duplicate reads within a request.

### Rollout

- Dual-read/compare against row results.
- Backfill blocks incrementally.
- Enable block reads by repository cohort.
- Stop row writes, preserve rollback window, then delete old rows/indexes asynchronously.

### Acceptance gate

- Browse metadata scales with unique tree payload bytes rather than individually indexed children.
- Exact/prefix results remain identical and sorted.
- Warm tree p95 meets the SLO and cold requests have bounded queries/bytes.
- Import no longer installs millions of tree rows for repositories better served by blocks.

## Milestone 6: low-latency repository read APIs

### Refs and summaries

- Cache the resolved default branch, ref generation, branch/tag counts, latest commit, and repository
  summary as one generation-scoped record.
- Update or invalidate it transactionally through an outbox after import/push.
- Avoid recounting or listing all refs for every summary request.

### Commit history

- Use keyset pagination by `(commit_time, oid)` or topological cursor, never deep `OFFSET`.
- Batch parent lookups or return parents with one measured query shape.
- Use commit-generation indexes for ancestry/history operations.
- Cap page size and traversal work.

### Tree and blob

- Resolve tree path components with per-request memoization and cached tree blocks.
- Return metadata without fetching blob payload.
- For large external blobs, return an authorized short-lived presigned URL or stream directly from
  S3; application workers should not buffer the whole blob.
- Support HTTP range/conditional requests where exposed by the server surface.

### Avoid N+1 behavior

- Add repository-layer batch APIs for OID metadata, commits, pack locations, and tree blocks.
- Instrument query count per endpoint and fail performance tests when a fixed-size page regresses to
  O(rows) backend calls.

### Acceptance gate

- Common pages use a small fixed number of SQL/cache operations.
- Pagination latency remains stable at deep history positions.
- Blob metadata never downloads blob content.
- Large blob delivery has bounded application memory and fast regional time-to-first-byte.

## Milestone 7: layered cache architecture

### Cache contents

Use separate policies for:

- refs and repository summary;
- indexed commits and history pages;
- decoded direct-tree blocks;
- pack metadata/indexes and delta bases;
- small hot decoded objects;
- negative ref/object lookups.

Do not cache large blobs or the complete imported loose-object set in Redis by default.

### Correctness model

- Namespace mutable cache keys by repository generation.
- Immutable OID/pack-checksum keys need no mutable invalidation, but still need size/TTL limits.
- Use transactional outbox events for ref/config/generation invalidation when delivery must survive
  process failure.
- Mutable ref/config reads use write-through or generation validation.
- Cache loss, lag, or eviction always falls back to durable storage.

### Performance controls

- Byte budgets at global, tenant, repository, and item levels.
- Admission policy that rejects one-hit large values.
- Singleflight concurrent misses for the same object/tree/range.
- Short negative-cache TTLs.
- Separate L1 and L2 metrics and eviction reasons.
- Prevent imports/backfills from warming every entry and evicting interactive hot data.

### Acceptance gate

- Warm summary/tree/commit requests meet SLOs.
- Cache memory is byte-bounded and tenant-fair.
- A generation change cannot serve stale refs or tree/history derived from mutable tips.
- Import traffic does not duplicate the repository payload set in cache.

## Milestone 8: resumable and low-downtime migration workflow

### Migration phases

```text
source capability/preflight
        |
        v
initial pack/ref/config snapshot
        |
        v
validated resumable bulk transfer
        |
        v
incremental ref/object catch-up
        |
        v
source/destination verification
        |
        v
short write freeze or coordinated final sync
        |
        v
cutover + monitored rollback window
```

### Resumability

- Persist migration ID, source identity, repository hash algorithm, source ref snapshot, completed
  pack checksums, loose overlay, metadata phases, and destination import session.
- Resume at validated pack/chunk boundaries without re-uploading immutable content.
- A checkpoint is trusted only after its payload checksum and required metadata commit.
- Separate resumable transfer progress from final ref-publication authority.

### Incremental catch-up

- Compare source ref snapshots and transfer only newly reachable closure/packs.
- Reuse completed destination manifests as traversal boundaries.
- Repeat until ref lag is within cutover policy.
- Detect force pushes/deletions and preserve configured retention/audit policy.

### Verification

- Compare hash algorithm, refs, peeled tags, reachable object manifests, selected canonical object
  samples, commit graph, default branch, and configured repository metadata.
- Provide a full verification mode and a faster checksum/manifest mode with explicit guarantees.
- Emit a signed or durable migration report suitable for operator/user review.

### Cutover and rollback

- Support a short source write freeze where available; otherwise coordinate a final ref CAS sync.
- Publish destination routing only after verification and final refs commit.
- Keep source read-only or mirrored during a rollback window.
- Record the exact destination generation activated at cutover.

### Acceptance gate

- Interrupted multi-hour migration resumes without restarting transferred packs.
- A no-change catch-up performs near-zero durable writes.
- Cutover cannot silently lose a source ref update.
- Users see phase, throughput, ETA, verification, and actionable failure details.

## Milestone 9: S3 delivery, cost, and locality

- Keep active packs in a storage class with predictable range-read latency.
- Move only old unreachable/backup artifacts to colder tiers; do not tier live packs without a
  measured restore/read strategy.
- Use multipart upload for large packs with bounded part concurrency and checksum verification.
- Choose part/range sizes from regional benchmarks, not constants copied from another provider.
- Coalesce adjacent pack ranges and avoid pathological tiny range requests.
- Consider CDN/edge caching for immutable canonical clone packs and authorized public assets.
- Use short-lived presigned URLs for large blob delivery when policy permits.
- Keep encryption, tenant authorization, and content disposition at the delivery boundary.
- Track storage bytes by purpose: live packs, clone cache, quarantine, orphan, loose overlay, and
  backup.
- Avoid cross-tenant content deduplication unless privacy, deletion, billing, and side-channel
  implications are explicitly solved.

### Acceptance gate

- External request count/bytes are visible per operation.
- Live pack and blob reads meet regional SLOs.
- Lifecycle policies cannot archive/delete bytes referenced by live SQL metadata.
- Cost reports attribute duplicated/orphaned/cache bytes.

## Milestone 10: background maintenance and repair

### Pack maintenance

- Consolidate many small packs and loose overlays in bounded background repacks.
- Prioritize repositories whose pack fragmentation measurably hurts reads.
- Preserve object reachability and hash algorithm; publish replacement pack metadata atomically.
- Delete superseded packs only after a grace period and live-reference check.

### Garbage collection

- Compute unreachable objects/packs from durable refs and retention policy.
- Respect reflog, quarantine, migration, legal/retention, and rollback windows.
- Sweep SQL metadata before or with external bytes using an idempotent state machine.

### Database health

- Monitor table/index bloat, WAL, autovacuum lag, replica lag, dead tuples, and long transactions.
- Schedule analyze/vacuum after large backfills/import cohorts rather than after every repository.
- Reindex or repartition only from measured need with online/rolling procedures.

### Consistency and repair

- Sample and scheduled full checks for pack checksum, index binding, object readability, refs,
  commit graph, tree blocks, external keys, and cache generation.
- Repair derived metadata from canonical objects/packs.
- Never fabricate missing canonical payload bytes; return a typed corruption report and restore from
  replica/backup.

### Acceptance gate

- Maintenance is resumable, rate-limited, and lower priority than interactive work.
- Crash/retry does not delete live bytes or expose partial replacement packs.
- Operators can identify and repair derived-index drift without reimporting the repository.

## Milestone 11: admission control, fairness, and capacity management

- Separate resource pools for interactive reads, clone, push, import/migration, and maintenance.
- Enforce global, tenant, repository, and actor concurrency limits.
- Use weighted byte/CPU semaphores, not request count alone.
- Queue with deadlines and expose queue time; reject early when work cannot meet its deadline.
- Reserve capacity for ref/summary/tree metadata requests.
- Limit concurrent large-object/delta operations independently.
- Feed database pool saturation, S3 latency, memory pressure, and worker utilization into admission
  decisions.
- Avoid automatic retries that multiply overload; use bounded jittered retry for idempotent phases.

### Acceptance gate

- Large migrations and maintenance cannot starve interactive SLOs.
- One tenant cannot monopolize workers, SQL connections, cache, or S3 concurrency.
- Overload produces explicit throttling/retry guidance rather than process OOM or cascading timeout.

## Milestone 12: replicas, regional deployment, backup, and disaster recovery

- Route immutable metadata/history reads to replicas only when generation/lag requirements permit.
- Keep ref publication, import completion, push, and read-after-write requests on the primary.
- Include repository generation in routing decisions so a replica cannot serve an older mutable
  view as current.
- Keep S3 pack keys region-aware and replicate immutable bytes according to recovery objectives.
- Back up PostgreSQL metadata and external pack manifests consistently enough to restore references
  to existing immutable bytes.
- Regularly restore representative repositories into scratch environments and verify refs/object
  closure/browse behavior.
- Define RPO/RTO separately for metadata, immutable packs, cache, clone-cache packs, and quarantine.

### Acceptance gate

- Replica lag cannot violate read-after-import/push guarantees.
- Restores produce a verified repository without depending on cache state.
- Regional failure behavior and user-visible retry/cutover semantics are documented.

## Benchmark and validation workloads

Use repeatable scratch repositories and complete applicable upstream test files.

### Repository shapes

1. long linear history with a reused tree;
2. wide tree changing one file per commit;
3. many small files and repeated child names;
4. delta-heavy binaries;
5. a few very large blobs;
6. many refs/tags;
7. SHA-1 and SHA-256;
8. source with multiple packs and loose overlay;
9. forced updates/deleted refs between migration snapshots;
10. the Grit repository as a realistic reference.

### Backend matrix

- memory;
- PostgreSQL with local pack bytes;
- PostgreSQL plus S3-compatible external bytes;
- each durable backend behind `CachedStorage`;
- cold cache, warm cache, and cache unavailable;
- low-latency local and realistic regional database/S3 latency.

### Fault matrix

- SQL deadlock/timeout/connection loss;
- S3 timeout, truncated range, checksum mismatch, and multipart interruption;
- process cancellation/crash after each migration phase;
- cache loss/stale invalidation event;
- concurrent import/push/delete/rename;
- disk/memory pressure and admission rejection;
- stale migration/import session.

## Recommended stacked branches

1. `backend-performance-metrics`
2. `migration-progress-and-eta`
3. `pg-bulk-tree-copy`
4. `pg-bulk-commit-parent-copy`
5. `pg-bulk-pack-index-copy`
6. `pg-native-source-pack-import`
7. `pg-packed-object-range-read`
8. `external-multipart-pack-import`
9. `external-native-pack-publication`
10. `external-orphan-sweeper`
11. `repository-numeric-primary-key`
12. `binary-object-id-schema`
13. `compact-tree-block-format`
14. `tree-block-dual-read-backfill`
15. `repository-summary-cache`
16. `history-keyset-pagination`
17. `tree-block-cache-and-path-resolution`
18. `cache-generation-outbox`
19. `resumable-migration-sessions`
20. `incremental-migration-catchup`
21. `migration-verification-cutover`
22. `pack-maintenance-and-gc`
23. `backend-admission-and-fairness`
24. `backend-replica-and-dr-hardening`

Branches 1-10 are the minimum compelling migration path and should precede clone/push scale work
where their storage capabilities are dependencies. Schema compaction and tree-block rollout should
remain separate from pack import so each can be measured and rolled back independently.

## Release gates

### Performance

- Native pack migration uses pack-scale uploads and chunk-scale SQL operations.
- No common interactive endpoint has object/row-count-dependent backend request amplification.
- Warm/cold p95 targets are met or exceptions are documented from repeatable measurements.
- Import, read, cache, and maintenance memory are byte-bounded.
- Database/S3/cache queueing is visible and governed by admission control.

### Correctness

- SHA-1/SHA-256 refs and reachable objects match the source.
- Partial/stale migration sessions cannot publish or prune refs.
- Tree-block and row representations compare identically during rollout.
- SQL visibility never points at missing external bytes.
- Cache loss/staleness cannot change repository truth.

### Operability

- Migration is resumable, observable, verifiable, and supports a controlled cutover/rollback.
- Orphan/quarantine/old-pack cleanup is safe and measured.
- Repair can rebuild every derived index from canonical packs/objects.
- Load shedding protects interactive requests.
- Backup restoration and regional failure procedures are exercised.

## Definition of done

- A packed source repository migrates primarily as validated compressed packs, not decoded objects.
- PostgreSQL metadata is installed in bulk and uses compact internal identities.
- S3 operations scale with packs/ranges and have bounded multipart/range concurrency.
- Browse and history APIs use compact indexes, keyset pagination, bounded queries, and layered
  generation-safe caches.
- Large blobs bypass application buffering when policy permits.
- Multi-hour migrations resume, catch up incrementally, verify, and cut over with no silent ref
  loss.
- Background maintenance keeps packs, SQL, caches, and external bytes healthy without starving
  user traffic.
- SLOs, capacity, cost, faults, and regression data are visible per backend.
- Clone and push paths can build on these storage/read primitives without reintroducing per-object
  database or S3 amplification.
