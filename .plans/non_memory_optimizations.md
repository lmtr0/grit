# Non-memory backend import and transport optimization plan

## Status

Planned work only. This document does not authorize a schema migration or production rollout.

## Objective

Make PostgreSQL and PostgreSQL-plus-external-byte-store repositories scale with compressed pack
bytes and unique Git metadata rather than with one database or object-store operation per Git
object or tree entry. After import, browsing should remain indexed and predictable, while clone and
push paths should stream bounded data instead of materializing and recompressing complete object
closures for every request.

This plan covers:

- `PgServerStorage` in `grit-lib-server/src/sqlx_postgres.rs`;
- `PgExternalizedStorage` in `grit-lib-server/src/externalized.rs`, including S3-compatible byte
  stores;
- `CachedStorage` behavior over those backends;
- import, browse-index, upload-pack, and receive-pack integration.

## Current findings

The following costs are backend-neutral and remain present for every destination:

- source ref/config/pack discovery;
- reachability traversal and structural object parsing;
- retained-pack integrity and canonical object validation;
- direct-tree browse-index preparation;
- commit generation calculation.

The following costs are specific to non-memory destinations:

1. Native source-pack import is currently advertised only by `MemoryBackend`. PostgreSQL and
   externalized storage use the portable loose-object fallback even though both already implement
   `PackStore`.
2. PostgreSQL tree entries are inserted with one SQL execution per row. The representative Grit
   import produced more than 5.7 million direct-tree rows.
3. PostgreSQL commit and parent installation is also row-oriented.
4. Externalized imported objects are uploaded sequentially as individual uncompressed payloads.
   Request count therefore scales with object count rather than pack count.
5. `upload-pack` reads every selected object, retains the complete decoded closure, and constructs
   a new non-delta pack in memory for each clone/fetch.
6. `receive-pack` expands the complete incoming pack into quarantine objects and writes accepted
   objects one by one.
7. Cached storage can populate one cache entry per imported loose object, duplicating the imported
   payload set and increasing import traffic.

The PostgreSQL browse table has an appropriate repository/tree/path index, so individual warm
queries are not the primary import problem. The dominant issue is write and storage amplification.

## Constraints

- Do not use `gix`, `git2`, or shell out to Git.
- Preserve SHA-1 and SHA-256 behavior.
- Preserve repository import-session fencing and publish refs only after their object/index state
  is durable.
- Preserve external byte immutability and content-addressed keys.
- Keep memory bounded by configured batches/caches plus the largest active object or delta chain,
  never by the complete clone, push, or import closure.
- Do not expose partially installed packs through SQL metadata.
- A database transaction cannot atomically commit S3 bytes. The design must tolerate orphaned
  immutable blobs and provide deterministic cleanup.
- Follow the repository rule that new functional tests come from the upstream Git test suite.
  Backend validation should use existing conformance facilities, upstream clone/fetch/push tests,
  examples, and repeatable benchmark drivers rather than inventing incompatible test semantics.

## Target architecture

### Durable object representation

For medium and large repositories, the durable representation should be:

```text
PostgreSQL:
  repository/ref/config/import state
  pack metadata
  object -> pack offset/index rows
  commit metadata and parents
  browse metadata or compact tree blocks

S3-compatible byte store:
  immutable complete pack bytes
  small loose-object overlay only when an object is not retained in a pack
```

PostgreSQL-only installations may store pack bytes in `bytea`, but must use the same pack metadata
and index model. Externalized installations store the exact same logical pack in the byte store and
keep only its content-addressed key in SQL.

### Import publication order

```text
discover and validate source
        |
        v
write immutable pack bytes (DB or S3)
        |
        v
bulk-install pack/object/tree/commit metadata
        |
        v
publish refs and prune stale refs in guarded SQL transaction
        |
        v
mark import complete
```

For S3, a failed SQL transaction may leave an unreachable pack object. That object is safe because
its key is immutable and no SQL row exposes it. A lifecycle sweeper removes unreferenced objects
after a grace period.

## Milestone 0: establish measurements and invariants

### Work

- Extend import phase reporting with destination-specific counters:
  - SQL statements and transactions by operation;
  - rows copied/upserted by table;
  - external PUT/GET/range-GET counts and bytes;
  - retained pack count/bytes and loose-overlay count/bytes;
  - clone time-to-first-byte, output bytes, decoded bytes, and peak buffered bytes;
  - push quarantine bytes, decoded objects, durable writes, and ref-publication time.
- Record wall time, CPU time, and peak RSS for memory, PostgreSQL, and externalized storage.
- Use the same repository shapes described in `OPTIMIZATIONS.md`, plus:
  - a full clone with no haves;
  - a fetch with one new commit;
  - a small push;
  - a delta-heavy large push.
- Record the current Grit repository as a non-synthetic reference workload.

### Acceptance gate

- Every later milestone has a before/after result using the same input and configuration.
- Counters distinguish application work from database and external-service latency.
- Measurements can run against scratch repositories without mutating the main worktree.

## Milestone 1: bulk PostgreSQL metadata installation

### Tree entries

- Replace the per-entry loop in `upsert_tree_entries_in_transaction` with chunked set-based
  insertion.
- First implementation: `QueryBuilder::push_values` chunks under PostgreSQL's bind limit.
- Scale implementation: `COPY` into a transaction-local staging table, then one
  `INSERT ... ON CONFLICT DO UPDATE` into `grit_tree_entries`.
- Acquire the repository import lock once per transaction, not once per row.
- Preserve `(tenant_id, repository_id, tree_oid, path)` uniqueness and the
  `text_pattern_ops` prefix index.

### Commits and parents

- Stage commit rows and parent rows in bulk.
- Delete/replace parent rows set-wise for affected commits.
- Preserve parent order and generation values.
- Avoid one delete plus N inserts per commit.

### Pack object indexes and trusted manifests

- Use set-based insertion for `grit_pack_objects` and trusted object rows.
- Chunk by both bind count and encoded byte size.
- Reject duplicate OIDs/offsets before starting the SQL transaction.

### Acceptance gate

- SQL executions scale with chunks, not tree entries, commits, parents, or pack objects.
- Importing 5.7 million tree entries does not execute millions of statements.
- Ref publication and import completion remain guarded by the current session token/generation.
- Existing browse queries return the same sorted direct entries.

## Milestone 2: native pack import for PostgreSQL

### Storage capability

- Implement `PackStore::supports_native_pack_import` for `PgServerStorage`.
- Implement `write_imported_packs` as one guarded transaction:
  - reserve storage order deterministically;
  - insert pack metadata and `bytea` once;
  - bulk-insert object index rows;
  - treat an existing checksum as idempotent;
  - expose no metadata when the transaction fails.
- Accept `ImportedPack` without creating an additional application-level full-pack copy where SQLx
  permits borrowing the shared bytes.

### Loose overlay

- Keep portable object batches only for reachable objects not covered by an eligible retained pack.
- Do not duplicate pack-contained objects in `grit_objects`.
- Ensure `object_exists`, object listing, consistency checking, export, repack planning, and import
  manifests treat pack and loose representations uniformly.

### Packed-object reads

- Prefer indexed bounded reads rather than loading a complete `bytea` pack for one object.
- If PostgreSQL cannot efficiently slice the stored `bytea`, add a backend method/query using
  `substring(data from ... for ...)` with validated offsets.
- Resolve cross-entry delta bases with bounded range reads and the existing byte-bounded cache.
- Never fetch the complete pack once per object during browse or clone.

### Acceptance gate

- A compatible repository imports with pack count plus loose-overlay writes, not one object row per
  packed object.
- Database storage does not contain both a retained pack and duplicate loose payloads for the same
  import.
- Reading one non-delta packed object transfers approximately its indexed range, not the entire
  pack.
- Corrupt, thin, promisor, mixed-reachability, or incompatible packs safely use the portable
  fallback and cannot publish invalid refs.

## Milestone 3: native pack import for externalized/S3 storage

### External byte API

- Extend `ExternalByteStore` only as needed for:
  - bounded streaming or multipart `put_if_absent`;
  - efficient range GET;
  - optional object-size/head checks;
  - deletion used by orphan cleanup.
- Keep content-addressed keys based on repository identity, hash algorithm, and pack checksum.

### Installation

- Implement native-pack capability for `PgExternalizedStorage`.
- Upload each immutable pack once, with bounded concurrency and byte backpressure.
- After upload succeeds, bulk-install pack metadata and object indexes in one guarded SQL
  transaction.
- Publish refs only after SQL metadata commits.
- On retry, reuse the existing content-addressed object rather than upload it again.

### Failure and cleanup

- Record or derive candidate orphan keys when an upload succeeds but SQL installation fails.
- Add a grace-period sweeper that deletes external pack/loose keys not referenced by live SQL rows.
- The sweeper must be tenant-scoped, retryable, and safe under concurrent imports.
- Never delete an external object solely because one import session failed; verify absence from all
  live pack/object metadata first.

### Loose objects

- Upload the loose overlay with bounded parallelism and byte limits.
- Do not populate the external store with one object per member of a retained pack.
- Consider retaining source loose zlib bytes in a future format version; do not recompress during
  this milestone unless profiling proves it helps.

### Acceptance gate

- External PUT count is proportional to retained packs plus the small loose overlay.
- A repository dominated by one source pack performs approximately one large external upload, not
  tens of thousands of object uploads.
- Object reads use range GET where the indexed representation allows it.
- Failed imports expose no refs and leave at most reclaimable immutable orphan bytes.

## Milestone 4: compact or lazy browse metadata

Bulk SQL removes round trips but does not remove 5.7 million rows and their indexes. Introduce a
second representation after bulk import is stable.

### Proposed representation

Store one immutable direct-entry block per unique tree:

```text
grit_tree_blocks
  tenant_id
  repository_id
  tree_oid
  format_version
  entry_count
  data bytea
  primary key (tenant_id, repository_id, tree_oid)
```

The block should contain sorted direct children with:

- one contiguous name byte arena;
- offsets/lengths instead of one SQL `text` allocation per name;
- mode and OID;
- no stored `kind` when it can be derived from mode;
- no duplicated tree OID;
- blob size referenced through an OID-level metadata table or omitted for lazy resolution.

An alternative is no durable browse block: decode the retained tree object on first access and
cache the prepared direct entries. Choose between eager compact blocks and lazy parsing using the
Milestone 0 browse-latency data.

### Migration

- Add a format/version capability and dual-read code.
- Write new imports to tree blocks while optionally dual-writing rows during rollout.
- Backfill existing repositories incrementally, ordered by repository and tree OID.
- Compare block results with row results before switching reads.
- Stop dual writes, then remove old rows only after a rollback window.

### Query behavior

- Fetch one tree block by primary key.
- Resolve exact children with binary search.
- Resolve prefix ranges with two partition points.
- Add a bounded cache keyed by repository and tree OID.
- Cache decoded blocks by bytes, not entry count.

### Acceptance gate

- Durable browse metadata scales with unique tree payload bytes, not one indexed SQL row per child.
- Exact and prefix browse results remain byte-for-byte compatible.
- Cold browse adds at most one tree-block query plus object-content reads.
- Warm browse latency does not regress materially from indexed tree rows.

## Milestone 5: fast and bounded clone/fetch

### Remove closure materialization

- Replace `ServerRepository::build_pack`'s `Vec<(ObjectId, StoredObject)>` with a streaming pack
  writer.
- Stream object selection, encoding, sideband framing, and response output with backpressure.
- Bound decoded object, delta-base, and output buffers independently.
- Start sending only after request validation is complete, but before the entire result pack is
  materialized.

### Reuse and cache packs

- Create a canonical full-clone pack for a repository ref snapshot, either during post-import
  maintenance or on first request.
- Cache by repository generation, hash algorithm, advertised ref snapshot, filter/shallow options,
  and relevant protocol capabilities.
- For the common no-haves/full-ref clone, stream the canonical pack directly from PostgreSQL or S3.
- Invalidate by repository generation rather than deleting cache entries synchronously on every
  ref update.
- Do not assume that arbitrary source packs can be concatenated into one protocol pack.

### Negotiated fetches

- Build only the wants-minus-haves closure.
- Reuse stored compressed entries or deltas only when their bases are guaranteed in the outgoing
  pack or on the receiver.
- Add a delta-capable streaming writer; the current independently zlib-compressed non-delta pack
  remains the correctness fallback.
- Coalesce adjacent S3 ranges and cache hot pack index/base data.

### Acceptance gate

- Clone peak memory is bounded independently of repository size.
- Time-to-first-byte does not wait for a complete output pack allocation.
- A repeated full clone of an unchanged repository reuses a canonical pack and performs O(pack)
  streaming rather than O(objects) backend reads plus recompression.
- S3 GET count for the canonical path is O(packs/ranges), not O(objects).
- Incremental fetch remains protocol-compatible when no cached result applies.

## Milestone 6: pack-native push quarantine and publication

### Streaming quarantine

- Stream the incoming pack into a temporary database/external object while computing and validating
  its trailer.
- Build the object index and validate object closure with bounded delta/cache memory.
- Avoid retaining all resolved object payloads simultaneously.
- Keep quarantine bytes invisible to ordinary object lookup.

### Thin packs

- Pushes may contain thin packs whose bases exist in the repository.
- Either thicken/rewrite the pack before durable publication or explicitly model safe cross-pack
  base dependencies. Prefer a self-contained thickened pack for simpler storage and garbage
  collection.
- Reject unresolved or corrupt delta bases before ref publication.

### Durable apply

- Move or copy the validated self-contained pack to its content-addressed final key.
- Bulk-insert pack index, commit, tree, and blob metadata.
- Update refs and reflogs with compare-and-swap guards in a repository-scoped transaction.
- Publish cache/event invalidations only after durable success.
- Remove or sweep rejected quarantine data.

### Acceptance gate

- Durable writes scale with pack count plus metadata chunks, not pushed object count.
- Large pushes do not execute one object transaction or S3 PUT per object.
- Peak memory is bounded by configured quarantine/decode/cache limits.
- Ref conflicts or policy rejection expose none of the quarantined objects through refs.
- Upstream receive-pack, thin-pack, atomic push, hook/policy, and SHA-256 behaviors remain intact.

## Milestone 7: cache policy and serving hardening

- Do not populate decoded-object cache entries for every object during pack-native import.
- Cache refs, commit metadata, decoded tree blocks, pack indexes, and hot objects separately.
- Give every cache a byte budget and repository-generation namespace.
- Prevent a single repository or large blob from evicting all small hot metadata.
- Add negative-cache entries for missing objects/refs with short TTLs.
- Coalesce concurrent reads for the same pack range or decoded object.
- Ensure mutable refs/config bypass stale caches or use write-through plus generation invalidation.
- Preserve direct/presigned large-blob delivery so application workers do not proxy large S3 blobs
  unnecessarily.

### Acceptance gate

- Import does not duplicate the repository's complete object payload set in Redis or process cache.
- Warm browse requests avoid S3 for metadata and previously decoded tree blocks.
- Cache invalidation after push/import is generation-safe and observable.
- Cache memory is bounded by bytes.

## Schema and compatibility strategy

- Additive migrations first: new indexes, staging helpers, tree-block table, and pack metadata fields.
- Keep old readers functional until new data has been backfilled and verified.
- Version binary tree-block formats and reject unknown versions with a typed error.
- Do not destructively remove loose objects merely because a pack row exists; use consistency checks
  and a grace-period cleanup job.
- Repository export and repair must understand both loose and packed representations throughout the
  rollout.
- Rollback disables new writes/reads by capability flag while preserving already written immutable
  packs and blocks.

## Recommended stacked implementation order

1. `non-memory-import-metrics`
2. `pg-bulk-tree-import`
3. `pg-bulk-commit-pack-index`
4. `pg-native-pack-import`
5. `external-native-pack-import`
6. `external-pack-orphan-sweeper`
7. `compact-tree-block-storage`
8. `tree-block-dual-read-migration`
9. `streaming-upload-pack`
10. `canonical-clone-pack-cache`
11. `pack-native-receive-quarantine`
12. `pack-native-receive-publication`
13. `backend-cache-budgeting`

Each branch should be independently reviewable, keep old behavior as a fallback, and include its
own measurements before the next branch begins.

## Validation matrix

Run every applicable scenario against memory, PostgreSQL, PostgreSQL plus external storage, and
cached wrappers:

| Scenario | Required properties |
| --- | --- |
| First import | complete refs, objects, graph, browse data; bounded memory |
| No-change import | near-zero durable writes; no duplicate packs/uploads |
| One-commit import | only new closure/metadata and ref changes |
| Interrupted import | no invalid refs; retry succeeds |
| Concurrent stale import | stale session cannot publish or prune |
| Corrupt/thin/promisor source | safe fallback or pre-publication failure |
| SHA-1/SHA-256 | matching object IDs, indexes, and pack trailers |
| Browse exact/prefix | identical sorted direct entries |
| Full clone | valid repository; bounded streaming memory |
| Incremental fetch | wants-minus-haves correctness |
| Small/large/thin push | closure, policy, CAS refs, reflogs, cleanup |
| Backend outage | retryable typed error; no partial publication |
| Cache cold/warm/stale | correct results and bounded cache behavior |

Use existing upstream Git tests through `scripts/run-tests.sh` for protocol behavior. Use scratch
repositories and the existing server examples/conformance paths for backend validation. Required
Rust checks remain `cargo fmt`, relevant `cargo check`/Clippy targets, and the repository-mandated
library test command, with pre-existing failures reported separately.

## Definition of done

- PostgreSQL and externalized storage advertise and safely implement native source-pack import.
- Tree/commit/pack-index installation is set-based and measured in chunks, not rows.
- S3 request count scales with packs and loose overlay, not all reachable objects.
- Browse metadata no longer requires millions of individually indexed rows for large histories, or
  an explicit benchmark shows that the row representation is preferable.
- Full clone streams with bounded memory and reuses a cached canonical pack when eligible.
- Push retains a validated self-contained pack and publishes metadata/refs transactionally.
- Import, clone, and push have phase/request/byte metrics for every backend.
- Functional behavior remains compatible with upstream Git tests for SHA-1 and SHA-256.
- Migration, rollback, orphan cleanup, consistency repair, and cache invalidation are documented and
  exercised before production enablement.
