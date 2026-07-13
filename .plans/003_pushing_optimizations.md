# Large push optimization plan

## Status

Planned work only.

## Objective

Accept large pushes with bounded memory and request counts by streaming the incoming pack into an
isolated quarantine, validating/indexing it incrementally, retaining a self-contained compressed
pack, bulk-installing metadata, and publishing refs transactionally.

This plan expands the push portion of `non_memory_optimizations.md`. It applies to memory,
PostgreSQL, PostgreSQL-plus-S3/external storage, cached wrappers, policy hooks, audit, and reflogs.

## Current behavior and bottlenecks

The current receive-pack path:

1. holds the complete request pack in `ReceivePackRequest::pack: Vec<u8>`;
2. fully inflates direct objects and delta instructions;
3. retains resolved payloads in hash maps and then in `Vec<QuarantinedObject>`;
4. clones object payloads while resolving deltas and building quarantine maps;
5. traverses closure and history through cloned `StoredObject` values;
6. can decode the pack a second time when recording a rejected audit event;
7. writes every accepted object individually;
8. updates refs/reflogs sequentially.

Consequences:

- peak memory scales with compressed pack plus expanded objects, deltas, maps, and clones;
- large delta chains amplify CPU and memory;
- PostgreSQL executes one durable object transaction per pushed object;
- external storage can perform one PUT per object;
- accepted incoming compression/deltas are discarded and later clone/fetch must recompress;
- failure after some writes can leave unreachable loose objects and expensive retry work;
- request cancellation cannot promptly stop work already performed on the complete in-memory pack.

## Functional and security invariants

- Preserve command parsing, refname validation, object-format negotiation, report-status v1/v2,
  sideband, delete-refs, atomic, ofs-delta, push-options, and agent capabilities.
- Preserve compare-and-swap ref semantics, non-fast-forward checks, protected-ref policy, hooks,
  audit records, reflog order, and post-receive invalidations.
- Do not expose quarantined objects through ordinary repository reads before the push is accepted.
- Do not publish a ref until its complete object closure and required metadata are durable.
- Atomic pushes update all requested refs or none. Non-atomic pushes retain correct per-command
  statuses and must not accidentally broaden partial-success semantics.
- Support SHA-1 and SHA-256.
- Bound compressed input, inflated bytes, object count, delta depth, delta expansion, CPU time,
  backend requests, and total request duration.
- Treat all incoming pack bytes, declared sizes, delta instructions, paths, identities, and push
  options as untrusted.
- Cancellation must stop queued work and leave only reclaimable quarantine bytes.

## Performance targets

Milestone 0 establishes numeric baselines. Structural requirements are:

- request and quarantine memory do not scale with all expanded pushed objects;
- durable writes scale with packs plus metadata chunks, not objects;
- S3 uploads scale with quarantine/final packs plus a small loose overlay;
- incoming compressed representations are retained when safe;
- ref publication is a short metadata transaction, not the phase that writes all payload bytes;
- rejected pushes do not decode the same pack twice;
- small pushes remain low latency while large pushes are bounded/fair.

Measure:

- request bytes, objects, direct/delta counts, maximum delta depth and expansion;
- first-byte-to-quarantine, validation, policy, durable-install, ref-publication, and total time;
- peak compressed, inflated, delta-base, metadata, and total memory;
- SQL statements/rows/transactions;
- external PUT/GET/range-GET count and bytes;
- quarantine cleanup time and orphan bytes;
- accepted/rejected/cancelled outcomes and retry behavior.

## Target push pipeline

```text
parse commands/capabilities + authorize
                 |
                 v
stream pack into isolated quarantine while hashing/indexing
                 |
                 v
resolve deltas + verify canonical object IDs and closure
                 |
                 v
prepare commit/tree/blob metadata and policy context
                 |
                 v
policy/hooks/fast-forward/ref CAS preflight
                 |
                 v
promote self-contained pack + bulk metadata
                 |
                 v
transactional refs/reflogs/import generation update
                 |
                 v
events, audit success, asynchronous cleanup/maintenance
```

No ordinary repository lookup may see the quarantine before promotion.

## API design

### Streaming request

Replace the required complete `Vec<u8>` pack with a bounded source abstraction:

```text
trait PackChunkSource {
    async read_chunk()
}

ReceivePackCommands {
    commands
    capabilities
    push_options
}

ReceivePackLimits {
    max_pack_bytes
    max_objects
    max_inflated_bytes
    max_delta_depth
    max_delta_expansion_ratio
    max_duration
    worker_count
    bytes_in_flight
}
```

Keep a buffered compatibility parser for existing callers, implemented on top of the streaming
source with an explicit size limit.

### Quarantine abstraction

```text
trait PackQuarantine {
    async append(bytes)
    async read_range(offset, len)
    async finish(expected_checksum)
    async promote(final_key_or_repository)
    async discard()
}
```

Implementations:

- memory: bounded memory followed by scratch-file spill, or an immutable temporary pack buffer;
- PostgreSQL: temporary/unpublished pack row or scratch spool, avoiding one giant transaction while
  bytes arrive;
- external/S3: multipart temporary object with a non-live quarantine key;
- tests/examples: scratch file under `/tmp`.

Quarantine identity must include an unguessable server-issued token and tenant/repository scope.

### Prepared push metadata

Replace payload-heavy `PushPlan` with a metadata plan:

```text
PreparedPush {
    commands
    pack_manifest
    pack_object_index
    structural_objects
    pushed_commits
    ref_preconditions
    policy_context
    quarantine_handle
}
```

Retain decoded commit/tree/tag data needed for checks and indexing, but do not retain every blob
payload. Blob canonical identity is verified during pack validation and content remains in the
quarantine pack.

## Milestone 0: metrics, limits, and behavior inventory

- Add the measurements listed above without changing push behavior.
- Establish explicit default/hard limits and typed errors for each limit.
- Benchmark:
  - one small commit;
  - many small files;
  - one multi-gigabyte blob represented by a manageable fixture shape;
  - deep linear history;
  - delta-heavy binary changes;
  - OFS and REF deltas;
  - thin pack with existing bases;
  - multiple ref commands with and without `atomic`;
  - policy rejection, CAS conflict, disconnect, backend outage;
  - SHA-1 and SHA-256;
  - memory, PostgreSQL, and externalized storage.
- Inventory applicable upstream receive-pack/push tests and copy only complete upstream files when
  expanding scope.

### Acceptance gate

- Baselines include peak RSS, backend operations, pack/object counts, and phase timings.
- Limits reject malicious inputs without panics or attacker-sized preallocation.

## Milestone 1: stream into quarantine

- Separate command/capability parsing from pack-body consumption.
- Adapt HTTP receive-pack endpoints to feed bounded chunks rather than one body `Vec<u8>`.
- Hash the pack incrementally and validate signature/version/object count/trailer.
- Enforce compressed-byte, duration, idle-time, and object-count limits while receiving.
- Spool bytes to the backend quarantine implementation with backpressure.
- Make disconnect cancel reads and queued validation work.
- Preserve the existing full decoder behind a bounded compatibility path until incremental
  validation is complete.

### Acceptance gate

- The complete incoming pack is not required in application memory.
- Quarantine receives bytes no faster than its backend can durably accept them.
- Disconnect leaves no live metadata and cleanup is retryable.

## Milestone 2: incremental indexing and validation

- Parse pack entry headers and record exact entry spans while streaming/spooling.
- Validate zlib streams, declared sizes, CRCs where available, and trailer checksum.
- Build OID, offset, kind, compressed-size, and resolved-size index rows.
- Resolve OFS deltas by validated offset.
- Resolve REF deltas within the quarantine or against explicitly allowed existing repository bases.
- Bound delta depth, base cache, instruction bytes, output bytes, and expansion ratio.
- Compute canonical object IDs for SHA-1/SHA-256.
- Decode/retain structural commit/tree/tag metadata needed for closure, fast-forward, policies, and
  browse/commit indexes; discard blob payloads after validation.
- Produce the audit/policy summary during this pass so rejection never decodes the pack again.

### Acceptance gate

- Every accepted indexed object is readable and canonical-hash verified.
- Blob payload memory is released after validation.
- Rejected audit records use the prepared summary without a second pack decode.
- Malformed size/delta/zlib input returns a typed pre-publication error.

## Milestone 3: make thin packs self-contained

Incoming pushes commonly depend on bases already present on the server. Durable representation
should not silently create an unreadable thin pack.

- Detect every external REF-delta base.
- Validate that the base is reachable/available under repository policy.
- Implement a bounded `fix-thin` equivalent that produces one self-contained pack:
  - append/re-encode required bases and update the pack header count/trailer; or
  - rewrite affected deltas/direct entries into a new streaming pack.
- Prefer reusing compressed delta instruction streams when safe.
- Deduplicate bases and cap the number/bytes added while thickening.
- If thickening cannot satisfy configured limits, reject the push rather than publish a thin pack.

### Acceptance gate

- Promoted packs have no unresolved external base dependency.
- Deleting/repacking older repository packs cannot make a newly accepted push unreadable.
- Thin-pack upstream push cases remain compatible.

## Milestone 4: bulk durable publication

### Memory

- Promote the immutable validated pack into `MemoryBackend` pack storage.
- Install its object index in one lock acquisition.
- Avoid duplicate loose `StoredObject` rows for pack-contained objects.

### PostgreSQL

- Insert pack metadata and pack-object index rows with set-based/chunked operations.
- Bulk-install commit, parent, tree, and blob-size metadata.
- Do not execute one transaction per object.
- Keep the pack/index and ref/reflog updates in one repository-scoped transaction when pack bytes
  are stored in PostgreSQL.

### External/S3

- Promote/copy the validated quarantine object to a content-addressed immutable final key.
- Bulk-install SQL pack/index/structural metadata only after final bytes exist.
- Publish refs in the guarded SQL transaction.
- Treat a promoted external pack without committed SQL metadata as an orphan eligible for delayed
  cleanup.

### Loose fallback

- Use a loose overlay only for representations that cannot safely enter the promoted pack.
- Write overlay objects in byte/count-bounded backend batches.

### Acceptance gate

- Durable operations scale with packs and metadata chunks.
- Pack-contained payloads are not duplicated as loose rows/objects.
- A failed metadata/ref transaction exposes no new refs or ordinary object lookup entries.

## Milestone 5: transactional ref, reflog, policy, and event semantics

- Lock all affected ref names in deterministic order.
- Re-check compare-and-swap preconditions immediately before publication.
- Re-check fast-forward ancestry against promoted/prepared objects plus existing repository state.
- For `atomic`, write all refs and reflogs or roll back all.
- For non-atomic mode, define and preserve exact per-command failure/application semantics before
  optimizing; do not accidentally make a previously independent command depend on another failed
  command.
- Run pre-receive/update policies against `PreparedPush` metadata.
- Run post-receive only after durable success.
- Record one rejection audit event from the already prepared context.
- Publish cache/ref invalidation events only after commit; retry or outbox them if event delivery
  must be durable.

### Acceptance gate

- CAS races and policy failures cannot publish unintended refs.
- Reflogs exactly match successful ref updates.
- Atomic and non-atomic upstream tests retain their expected statuses.
- No pack re-decode occurs solely for audit or hooks.

## Milestone 6: bounded parallelism and backend throughput

- Use one reusable worker pool per push for independent zlib/delta/hash validation where dependency
  order permits it.
- Build a delta dependency DAG; validate ready nodes in bounded waves.
- Charge recursive base/instruction/result/prepared-metadata working sets conservatively.
- Unknown/oversized work runs alone.
- Bound S3 multipart/range concurrency and PostgreSQL metadata chunks separately.
- Apply per-tenant fairness and global push CPU/memory semaphores.
- Keep publication ordering deterministic for fixed commands/options.

### Acceptance gate

- A large pack uses available CPU without exceeding byte budgets.
- Several large pushes cannot starve small pushes or browse/clone work.
- Cancellation skips queued work and reaps active workers off the async executor.

## Milestone 7: quarantine and orphan lifecycle

- Give every quarantine an owner token, creation time, repository identity, and state:
  `receiving`, `validated`, `promoting`, `committed`, `rejected`, or `expired`.
- Add idempotent cleanup for rejected, cancelled, and expired quarantine data.
- Add an S3 orphan sweeper that confirms no live SQL reference before deletion.
- Handle process crashes between final external upload, SQL transaction, and event publication.
- Add maintenance reporting for quarantine count/bytes/age and orphan cleanup outcomes.
- Never delete a content-addressed pack referenced by another retry/session/repository row.

### Acceptance gate

- Crash injection at each phase leaves either a committed push or reclaimable invisible bytes.
- Cleanup is safe under concurrent retries and repository deletion/rename.

## Failure handling and abuse resistance

- Use checked arithmetic and fallible reservations for all declared sizes and delta operations.
- Reject overlong varints, invalid offsets, delta cycles, truncated zlib, trailer mismatch, duplicate
  offsets/OIDs, mixed hash widths, and object-kind mismatch.
- Enforce limits before allocating or scheduling work.
- Rate-limit by actor, tenant, repository, bytes, CPU, and concurrent quarantines.
- Avoid logging push options, identities, or paths without escaping/redaction.
- Ensure rejected packs cannot be queried by guessed quarantine keys.
- Bound policy/hook inputs and execution time independently of pack validation.

## Rollout

1. Add metrics and hard safety limits.
2. Introduce streaming request/quarantine behind a feature flag while retaining buffered apply.
3. Enable incremental validation for memory with result comparison against the old decoder.
4. Enable pack promotion for memory.
5. Enable PostgreSQL bulk pack publication.
6. Enable external/S3 quarantine and promotion for an allowlist.
7. Enable thin-pack thickening.
8. Enable bounded parallel validation and tenant fairness.
9. Remove per-object durable writes after rollback and repair tooling are proven.

Rollback stops new pack-native pushes and uses the bounded compatibility decoder/writer. Already
committed packs remain ordinary repository storage. Quarantines are cleaned asynchronously.

## Recommended stacked branches

1. `push-pack-metrics-and-limits`
2. `streaming-receive-pack-source`
3. `push-quarantine-abstraction`
4. `incremental-pack-index-validation`
5. `push-prepared-metadata-plan`
6. `push-fix-thin-pack`
7. `memory-pack-promotion`
8. `pg-pack-promotion`
9. `external-pack-quarantine-promotion`
10. `push-atomic-publication`
11. `push-parallel-validation`
12. `push-quarantine-orphan-sweeper`
13. `push-fairness-and-load-hardening`

## Validation matrix

Use complete applicable upstream Git tests through `scripts/run-tests.sh`, scratch repositories,
and existing server adapters.

| Scenario | Required result |
| --- | --- |
| Empty/delete-only push | no pack required; correct ref/reflog behavior |
| Small create/update | accepted closure and status |
| Non-fast-forward | rejected without publication |
| Atomic multi-ref | all or none |
| Non-atomic multi-ref | exact per-command statuses |
| OFS/REF delta | canonical objects and readable promoted pack |
| Thin pack | safely thickened before publication |
| Large blob | bounded exclusive processing |
| Delta bomb/malformed sizes | early typed rejection, no panic/OOM |
| Policy/hook rejection | one validation pass, correct audit |
| CAS race | guarded failure, no wrong ref |
| Disconnect/backend outage | invisible reclaimable quarantine |
| SHA-1/SHA-256 | correct IDs, trailer, ref widths |
| Memory/PostgreSQL/S3 | equivalent accepted repository state |
| Retry | idempotent pack/object metadata and no duplicate upload |

## Definition of done

- Receive-pack no longer requires the complete pack and all expanded objects in application memory.
- Incoming bytes are isolated until validation, policy, and ref preconditions succeed.
- Accepted pushes retain a validated self-contained pack and bulk metadata.
- PostgreSQL/S3 operations scale with packs and chunks rather than objects.
- Thin packs are made self-contained or rejected before publication.
- Refs, reflogs, policies, audits, and invalidations preserve their required ordering/atomicity.
- Cancellation, rejection, crashes, and backend failures leave only reclaimable invisible data.
- Large-push CPU, memory, storage requests, and tenant concurrency are measured and bounded.
- Upstream push/receive-pack behavior remains compatible for SHA-1 and SHA-256.
