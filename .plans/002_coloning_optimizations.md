# Large clone and fetch optimization plan

## Status

Planned work only. The filename preserves the requested `coloning` spelling.

## Objective

Make large clone and fetch responses stream with bounded payload memory, reuse durable compressed
data when safe, and avoid reading, retaining, and recompressing every selected Git object for every
request.

This plan expands the clone portion of `non_memory_optimizations.md`. It applies to memory,
PostgreSQL, PostgreSQL-plus-S3/external storage, and cached wrappers.

## Current behavior and bottlenecks

The current upload-pack path:

1. parses all wants and haves into an `UploadPackRequest`;
2. walks the wants-minus-haves closure into an ordered `Vec<ObjectId>`;
3. calls `ServerRepository::build_pack`;
4. reads every selected object into `Vec<(ObjectId, StoredObject)>`;
5. creates one complete output `Vec<u8>`;
6. zlib-compresses every object independently without delta selection;
7. creates a second complete `wire_response` buffer when sideband framing is requested.

Consequences:

- peak payload memory scales with the complete decoded closure plus output pack and wire response;
- time-to-first-byte waits for all reads and compression;
- retained source packs are decoded and recompressed rather than reused;
- PostgreSQL can execute one read per object;
- external storage can perform one GET per loose object or many small range GETs;
- repeated identical full clones repeat the same CPU and backend work;
- output packs are larger than a delta-compressed Git pack.

Object-ID negotiation state also scales with closure size, but it is much smaller than retained
payloads. Optimize payload streaming first, then add spillable negotiation state only if measured.

## Functional invariants

- Preserve protocol v0/v1 behavior and do not regress protocol-v2-capable entry points.
- Preserve wants-minus-haves reachability, annotated tag inclusion, shallow/filter behavior when
  implemented, and SHA-1/SHA-256 object formats.
- Honor negotiated `side-band`, `side-band-64k`, `ofs-delta`, `thin-pack`, `include-tag`, ACK, and
  agent/object-format capabilities.
- A response pack must be one valid pack stream; stored packs cannot simply be concatenated.
- Never reuse a delta unless its base is guaranteed to be available in the outgoing pack or on the
  client under negotiated thin-pack semantics.
- Do not send response bytes until request validation and authorization have succeeded.
- After streaming begins, failures terminate the response; they cannot be converted into a normal
  Git error packet. Perform every fallible preflight possible before the first pack byte.
- Cancellation and client disconnect must stop queued reads/compression and release buffers.
- Enforce tenant/repository concurrency, CPU, output-byte, and wall-time limits.

## Performance targets

Milestone 0 records concrete baselines before setting production thresholds. Structural acceptance
requirements are:

- decoded payload memory is bounded independently of total clone size;
- output is emitted before the complete pack is built;
- backend reads are batched/coalesced and bounded;
- repeated common full clones reuse a canonical pack;
- cached/full-clone paths stream compressed bytes without object-by-object recompression;
- incremental fetch remains correct when no reuse path is eligible.

Collect at least:

- negotiation, reachability, pack-plan, first-byte, compression, and total time;
- selected objects and decoded/compressed bytes by object kind;
- cache hit/miss/singleflight wait;
- database statements/bytes and external GET/range-GET count/bytes;
- output pack size and delta count/depth;
- peak decoded, delta-base, encoded, sideband, and total process memory;
- cancellation latency and wasted backend/CPU work.

## Target request pipeline

```text
parse + authorize request
          |
          v
negotiate wants/haves and build object plan
          |
          +---- eligible canonical pack? ---- yes ---> stream cached pack
          |
          no
          v
plan object order and reusable representations
          |
          v
bounded read/decode/delta/encode workers
          |
          v
ordered pack writer + incremental trailer hash
          |
          v
sideband chunker / HTTP response sink
```

Workers may complete out of order, but the pack writer consumes planned entries deterministically.

## API design

### Streaming response

Introduce a runtime-neutral output boundary instead of returning complete vectors:

```text
trait PackChunkSink {
    async write_chunk(bytes)
    async finish()
    async abort(error)
}

UploadPackStreamReport {
    selected_objects
    reused_entries
    decoded_bytes
    output_bytes
    cache_status
}
```

The HTTP adapter should implement the sink with a bounded channel or native streaming response.
Core protocol code must not depend on a particular async runtime. Keep the existing buffered API as
a compatibility wrapper that writes into a `Vec<u8>` with an explicit maximum response size.

### Pack writer

Add an incremental pack writer that:

- receives the planned object count before writing the pack header;
- writes object headers and compressed streams incrementally;
- updates the SHA-1/SHA-256 pack trailer digest as bytes are emitted;
- frames sideband channel 1 without making a second response copy;
- exposes byte/cancellation checkpoints;
- can copy validated compressed entry bytes when the entry plan permits it;
- supports a correctness-first non-delta path before delta reuse is introduced.

### Resource options

Use a separate non-breaking execution options type with conservative defaults:

- read/decode worker count;
- compression worker count;
- decoded bytes in flight;
- encoded bytes queued to the sink;
- delta-base cache bytes;
- external range-read concurrency and coalescing window;
- maximum objects/output bytes/request duration;
- optional negotiation-state spill threshold.

Clamp pathological values before allocation. Unknown/oversized objects run exclusively.

## Milestone 0: benchmark and protocol inventory

- Add phase and resource metrics listed above.
- Record current behavior for:
  - a full clone with all advertised refs and no haves;
  - a single-branch clone;
  - a repeated identical clone;
  - a fetch with one new commit;
  - a client with many haves;
  - a repository with large blobs;
  - a delta-heavy repository;
  - SHA-1 and SHA-256;
  - sideband enabled and disabled;
  - memory, PostgreSQL, and externalized storage.
- Inventory upstream upload-pack/clone/fetch tests already in scope and identify missing upstream
  files that must be copied whole before behavior changes.

### Acceptance gate

- Baselines include time-to-first-byte, total time, output size, backend requests, and peak RSS.
- Pack validity is checked by cloning into a scratch repository and reading the resulting object
  closure through the existing harness.

## Milestone 1: bounded streaming non-delta pack

This milestone preserves current pack contents while removing whole-response materialization.

- Replace `build_pack -> Vec<u8>` in upload-pack with the streaming pack writer.
- Read one bounded wave of objects, preserve planned order, compress, and drain to the sink before
  scheduling more.
- Do not retain the complete `Vec<(ObjectId, StoredObject)>`.
- Write sideband pkt-lines as pack chunks arrive.
- Compute the trailer incrementally.
- Propagate sink backpressure to read/decode workers.
- Cancel queued work on disconnect and reap active blocking work off the async executor.
- Keep `ServerRepository::build_pack` as a bounded compatibility wrapper or deprecate it after all
  internal callers migrate.

### Acceptance gate

- Peak payload memory is bounded by configured waves/queues plus one oversized object.
- Time-to-first-byte occurs before the final object is read.
- The produced pack contains the same object set as the old serializer.
- Sideband mode does not allocate a second complete response.

## Milestone 2: canonical full-clone pack cache

### Eligibility

A canonical pack can be reused only when the request shape exactly matches its manifest, including:

- repository generation/ref snapshot;
- normalized wants;
- no haves, or another explicitly modeled receiver state;
- hash algorithm;
- include-tag behavior;
- shallow/filter parameters;
- capabilities that affect pack representation.

Do not infer eligibility merely because a source pack exists. Multiple source packs and loose
objects must be represented by one valid canonical response pack.

### Generation and storage

- Generate a canonical full-clone pack after import/push maintenance or on first eligible request.
- Use singleflight per cache key so concurrent first clones do not generate duplicates.
- Store pack bytes through `PackStore`/external storage with a typed purpose and manifest, separate
  from ordinary object-storage packs.
- Store object count, checksum, size, generation, creation time, and request-shape manifest.
- Stream PostgreSQL/S3 bytes directly to the response sink.
- Invalidate by repository generation; delete old cache objects asynchronously after a grace
  period.

### Acceptance gate

- The second identical full clone performs no object-by-object reads or recompression.
- Cached clone memory is bounded by streaming buffers.
- Ref updates make stale canonical packs ineligible immediately through generation mismatch.
- Concurrent cache misses produce one pack.

## Milestone 3: backend-efficient object and range reads

### Memory

- Copy validated compressed entry slices directly from retained immutable pack bytes when safe.
- Keep decoded delta bases byte-bounded.

### PostgreSQL

- Fetch bounded pack ranges with a server-side `bytea` slice or another measured mechanism.
- Batch loose-object reads by OID instead of one query per object.
- Batch pack-index lookups and group planned entries by pack and adjacent offsets.

### External/S3

- Sort planned reads by pack/offset and coalesce nearby ranges.
- Bound range size and concurrent requests.
- Cache immutable pack indexes and frequently used delta bases.
- Prefer one canonical-pack GET/stream for eligible full clones.
- Avoid downloading a complete source pack repeatedly to resolve individual objects.

### Acceptance gate

- Backend request count scales with batches/coalesced ranges, not objects.
- Range coalescing never crosses validated pack bounds.
- Cache and request memory are byte-bounded.

## Milestone 4: compressed entry reuse

Reuse source compressed bytes before implementing new delta selection.

- Direct blob/tree/commit/tag entries can reuse their validated zlib stream with a newly emitted
  pack entry header.
- REF-delta payload streams can be reused only when the referenced base is valid for the outgoing
  pack/client.
- OFS-delta headers encode distance from the new output offset and therefore must be recomputed; the
  compressed delta instruction stream may still be reusable.
- Build an explicit `PackEntryPlan` describing direct, REF-delta, OFS-delta, or recompress fallback.
- Verify the final emitted pack in debug/validation modes and retain the old streaming encoder as a
  fallback.

### Acceptance gate

- Reused entries are not decompressed/recompressed solely for transport.
- Every reused delta has a proven base dependency.
- SHA-1/SHA-256 trailer and object closure remain valid.

## Milestone 5: delta-capable streaming pack construction

- Add bounded delta candidate selection grouped by compatible object kind and useful similarity
  signals such as path/name and size.
- Use a bounded sliding window; never compare every object with every other object.
- Reuse existing stored deltas when valid before computing new deltas.
- Cap delta depth, base bytes, instruction bytes, CPU time, and expansion ratio.
- Honor `ofs-delta` and thin-pack negotiation.
- Emit a direct compressed object when delta selection or generation exceeds its budget.
- Keep object ordering deterministic for a fixed plan/options configuration.

### Acceptance gate

- Delta-heavy benchmark output is materially smaller than the non-delta fallback.
- CPU and memory remain within configured bounds.
- A timeout/budget exhaustion produces a valid less-compressed pack, not a failed clone.

## Milestone 6: negotiation and large-repository scaling

- Use indexed commit generations to reduce commit negotiation work.
- Separate commit negotiation from tree/blob closure expansion.
- Avoid reading blob payloads during reachability selection.
- Represent object membership with compact hash sets or repository-local ordinal bitmaps where a
  stable object ordinal index exists.
- Spill very large negotiation/selection state to a scratch file or backend-neutral temporary
  store after a configured threshold.
- Add per-tenant fairness so several large clones cannot occupy every decode/compression worker.

### Acceptance gate

- Many-haves fetch avoids walking excluded history repeatedly.
- Negotiation memory is measured and bounded/spillable independently of payload memory.
- Small clones remain low latency under concurrent large-clone load.

## Failure handling and security

- Validate wants and authorization before streaming.
- Limit wants, haves, selected objects, output bytes, request duration, and concurrent clone jobs.
- Treat backend corruption or missing delta bases as a preflight failure where possible.
- Once output begins, abort the stream and emit metrics with the last completed phase/byte count.
- Never place credentials, tenant data, or unvalidated client strings in cache keys or logs.
- Cache only immutable content-addressed pack bytes; manifests remain tenant/repository scoped.
- Verify cached checksum/size before first use and periodically through maintenance sampling.

## Rollout

1. Add metrics with no behavior change.
2. Introduce streaming behind a per-repository feature flag, retaining buffered fallback.
3. Enable streaming non-delta output for memory, then PostgreSQL, then external storage.
4. Enable canonical cache generation but not serving; compare generated packs offline.
5. Serve cached full clones for an allowlist.
6. Enable compressed-entry reuse, then delta construction, independently.
7. Remove the unbounded buffered path only after protocol and load validation.

Rollback disables the new request path and cache eligibility. Immutable generated packs may remain
until asynchronous cleanup; they must never affect repository reachability.

## Recommended stacked branches

1. `clone-pack-metrics`
2. `streaming-pack-sink`
3. `streaming-nondelta-pack`
4. `streaming-sideband-response`
5. `clone-backend-batched-reads`
6. `canonical-clone-pack-model`
7. `canonical-clone-pack-serving`
8. `stored-compressed-entry-reuse`
9. `bounded-delta-pack-writer`
10. `clone-negotiation-scaling`
11. `clone-fairness-and-limits`

## Validation matrix

Use complete applicable upstream Git tests through `scripts/run-tests.sh`, scratch clones, and the
existing server adapters.

| Scenario | Required result |
| --- | --- |
| Empty repository | valid advertisement and empty/no-op pack behavior |
| Full all-ref clone | exact reachable closure including tags |
| Single branch | no unrelated unreachable objects required |
| Incremental fetch | wants-minus-haves closure is correct |
| Sideband 64k | valid channel framing across chunk boundaries |
| Disconnect | bounded cancellation latency and no leaked workers |
| Large blob | one exclusive object; bounded queues |
| Delta-heavy history | valid bounded-depth delta pack |
| Cached clone | generation-safe hit and byte-identical valid pack |
| SHA-1/SHA-256 | valid object IDs and trailer width/hash |
| Memory/PostgreSQL/S3 | equivalent object closure and protocol response |
| Backend fault | no panic; typed preflight error or stream abort |

## Definition of done

- Upload-pack no longer requires complete decoded-object and response vectors.
- Large responses stream with bounded memory and backpressure.
- Common repeated full clones stream a generation-safe canonical pack.
- Backend reads are batched/coalesced and S3 uses range or canonical-pack streaming.
- Stored compressed entries and deltas are reused only with proven dependency safety.
- Delta fallback always produces a valid pack under resource pressure.
- Clone/fetch behavior remains compatible with upstream Git for both hash algorithms.
- Metrics expose time-to-first-byte, throughput, CPU, memory, backend requests, and cache behavior.
