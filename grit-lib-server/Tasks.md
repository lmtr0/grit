# grit-lib-server Tasks

This document tracks the remaining implementation slices for `grit-lib-server` in the order they should be built. The current baseline provides storage traits, an in-memory backend, a SQLx/Postgres backend scaffold, repository import, read-only object/ref/tree APIs, and a cache wrapper.

The goal is feature parity with `grit-lib` for hosting-platform use cases, while allowing repository state to live in SQL and be accelerated by in-memory, Redis, and NATS-backed cache layers.

## Implementation Order

1. Read-only hosting APIs
2. Commit graph and history queries
3. SQL transaction hardening
4. Redis and NATS cache adapters
5. Upload-pack read-only protocol
6. Receive-pack push protocol
7. Authorization and hosting policy hooks
8. Import, mirror, and migration tools
9. Packfile storage and serving
10. Feature parity matrix and conformance suite

## Slice 1: Read-only Hosting APIs

Purpose: expose the repository data a hosting platform needs for web UI, API, and read-only repository browsing.

Required capabilities:

- Resolve default branch from `HEAD`.
- Return repository summary: default branch, refs count, object count, latest commit.
- List branches and tags with peeled commit metadata when available.
- Return commit detail by object id or ref name.
- Return tree entries for a commit/tree and path.
- Return blob contents and metadata for a commit/ref plus path.
- Discover conventional files such as `README`, `LICENSE`, and `.gitmodules`.
- Return compare inputs needed for later diff views.

API candidates:

- `ServerRepository::summary()`
- `ServerRepository::default_branch()`
- `ServerRepository::branches()`
- `ServerRepository::tags()`
- `ServerRepository::commit(object_or_ref)`
- `ServerRepository::tree_at(revision, path)`
- `ServerRepository::blob_at(revision, path)`
- `ServerRepository::discover_files(revision, candidates)`

Storage work:

- Extend `BrowseIndex` if current root-tree keyed lookup is not enough for commit/ref keyed browsing.
- Add typed view structs instead of returning raw storage rows.
- Keep path handling byte-compatible with Git paths where possible.

Tests:

- Build a repository through `grit-lib`, import it, and assert all hosting views work through `MemoryServerStorage`.
- Include nested paths, empty directories behavior, executable entries, symlinks if supported by current `grit-lib`, and missing path errors.
- Assert APIs work when using both raw object ids and ref names.
- Assert default branch resolution handles symbolic and detached `HEAD`.

Done when:

- A hosting caller can render a repository landing page, branches page, tags page, commit page, tree browser, and blob viewer without touching filesystem-backed `grit-lib`.

## Slice 2: Commit Graph and History Queries

Purpose: provide efficient history traversal and graph operations used by hosting features, protocol negotiation, and branch policy.

Required capabilities:

- Paginated commit history from a ref, commit, or path.
- Parent traversal.
- Ancestor checks.
- Merge-base calculation.
- Reachability from refs.
- Children lookup if stored or indexed.
- Commit count estimates for repository summary and branch comparison.

API candidates:

- `ServerRepository::commit_history(start, options)`
- `ServerRepository::is_ancestor(ancestor, descendant)`
- `ServerRepository::merge_base(left, right)`
- `ServerRepository::reachable_from(refs)`
- `ServerRepository::compare_commits(base, head)`

Storage work:

- Add `CommitGraphStore` trait.
- Add commit parent index rows during import and future writes.
- Consider generation numbers or commit dates for traversal ordering.
- Keep graph indexes repairable from stored objects.

Tests:

- Linear history, branch history, merge commit history, and criss-cross-style cases where current `grit-lib` supports them.
- Pagination boundaries and deterministic ordering.
- Path-limited history once tree diff support exists.
- Cross-check graph results against filesystem-backed `grit-lib` behavior where equivalent APIs exist.

Done when:

- Branch pages, commit history pages, compare pages, and protocol reachability can be implemented without scanning every object.

## Slice 3: SQL Transaction Hardening

Purpose: make the SQLx backend safe for production write paths and compatible with YugabyteDB/Postgres deployment.

Required capabilities:

- Repository lifecycle rows: create, rename, archive, delete.
- Transaction wrappers for multi-table writes.
- Atomic compare-and-swap ref updates.
- Reflog writes in the same transaction as ref changes.
- Object insert idempotency.
- Schema indexes for object lookup, ref lookup, browse index lookup, graph traversal, and repository listing.

API candidates:

- `PgServerStorage::create_repository(...)`
- `PgServerStorage::delete_repository(...)`
- `PgServerStorage::transaction(...)`
- `ServerStorageTransaction` trait or concrete SQL transaction wrapper.

Storage work:

- Revisit `MIGRATIONS` for repository metadata, timestamps, uniqueness, and indexes.
- Ensure all queries include tenant and repository scope.
- Decide whether large object bytes are stored inline, chunked, or externalized later.
- Audit isolation assumptions for YugabyteDB.

Tests:

- Live SQL integration tests behind an ignored test or feature flag.
- Atomic ref update race tests.
- Idempotent object insertion tests.
- Transaction rollback tests for partial import/write failure.
- Schema migration smoke test against empty database.

Done when:

- SQL can be trusted as the source of truth for repository state and ref updates are safe under concurrent hosting traffic.

## Slice 4: Redis and NATS Cache Adapters

Purpose: add deployable caching and invalidation without changing repository APIs.

Required capabilities:

- Redis cache implementation for objects, refs, ref lists, commit summaries, and tree listings.
- NATS publisher for invalidation events.
- Optional NATS subscriber/consumer that invalidates local memory cache.
- Layered cache composition: in-memory near cache over Redis, invalidated through NATS.
- Clear cache key versioning.

API candidates:

- `RedisCache`
- `NatsInvalidationPublisher`
- `NatsInvalidationSubscriber`
- `LayeredCache`
- `CacheKey::version()`

Storage work:

- Keep cache values typed and versioned.
- Define invalidation events for object write, ref write, ref delete, repository delete, tree index rebuild, and config update.
- Avoid relying on cache for correctness; durable storage remains authoritative.

Tests:

- Cache hit/miss behavior using in-memory test doubles first.
- Ref update invalidates stale ref and ref-list entries.
- Repository delete invalidates all scoped keys.
- Event serialization round trips.
- Redis/NATS integration tests behind ignored feature flags or environment variables.

Done when:

- A horizontally scaled hosting service can run with local memory caches and shared Redis/NATS invalidation without serving stale refs after successful writes.

## Slice 5: Upload-pack Read-only Protocol

Purpose: allow clone and fetch from SQL-backed repositories.

Required capabilities:

- Advertise refs.
- Parse wants and haves.
- Negotiate reachable objects.
- Generate pack response.
- Support protocol v0/v1 first, then protocol v2.
- Use commit graph indexes for negotiation where possible.

API candidates:

- `ServerRepository::advertise_refs()`
- `ServerRepository::negotiate_fetch(request)`
- `ServerRepository::build_fetch_pack(plan)`
- `UploadPackService`

Storage work:

- Efficient object enumeration by reachability.
- Pack generation from stored loose objects.
- Later integration with stored pack indexes from Slice 9.

Tests:

- Unit tests for request parsing and response framing.
- Clone/fetch fixture tests using `grit-lib-server` APIs, not shelling out from implementation.
- Compare produced object closure against expected reachable object ids.
- Protocol tests for empty repo, single branch, tags, and shallow negotiation if supported.

Done when:

- A caller can clone/fetch from a SQL-backed repository using the server library as the data source.

## Slice 6: Receive-pack Push Protocol

Purpose: accept pushes into SQL-backed repositories safely.

Required capabilities:

- Parse receive-pack commands.
- Accept and validate packfiles.
- Verify object closure.
- Enforce fast-forward and protected ref rules through policy hooks.
- Atomically update refs.
- Write reflogs.
- Publish invalidation events.

API candidates:

- `ReceivePackService`
- `ServerRepository::prepare_push(request)`
- `ServerRepository::apply_push(plan)`
- `PushPolicy` trait

Storage work:

- Transactional object insertion plus ref updates.
- Temporary object quarantine namespace or equivalent.
- Ref conflict handling with typed errors.

Tests:

- Fast-forward push.
- Non-fast-forward rejection.
- Create branch, delete branch, update tag.
- Missing object rejection.
- Policy rejection.
- Concurrent push conflict.

Done when:

- A hosting platform can accept pushes while preserving Git ref consistency and cache correctness.

## Slice 7: Authorization and Hosting Policy Hooks

Purpose: keep platform-specific authorization and policy outside core storage while making every write enforceable.

Required capabilities:

- Tenant and repository permission checks.
- Branch protection.
- Tag protection.
- Pre-receive/update/post-receive style hooks.
- Audit event emission.
- Policy context with actor, repository, old/new refs, and pushed commits.

API candidates:

- `AuthorizationProvider`
- `RepositoryPolicy`
- `PushPolicy`
- `AuditSink`
- `PolicyDecision`

Storage work:

- Avoid embedding platform user models in core storage.
- Store audit records only through explicit optional integrations.

Tests:

- Policy allow/deny paths.
- Protected branch rejection.
- Audit event emission on accepted and rejected writes.
- Ensure read-only APIs remain usable without an auth provider when embedded callers choose to authorize externally.

Done when:

- Hosting platforms can plug in their own auth and branch policy without forking `grit-lib-server`.

## Slice 8: Import, Mirror, and Migration Tools

Purpose: make onboarding and repairing repositories practical.

Required capabilities:

- Full import from filesystem-backed `grit-lib`.
- Incremental re-import.
- Import progress reporting.
- Consistency checker.
- Browse index repair.
- Commit graph repair.
- SQL-to-filesystem export for backup or debugging.

API candidates:

- `import_repository(...)` extensions with options.
- `repair_browse_index(...)`
- `repair_commit_graph(...)`
- `check_repository_consistency(...)`
- `export_repository(...)`

Storage work:

- Import checkpoints for large repositories.
- Idempotent import steps.
- Repair operations that can run online when possible.

Tests:

- Re-import after new commit.
- Re-import after deleted branch.
- Detect missing object referenced by ref.
- Rebuild browse index from object storage.
- Rebuild commit graph from commit objects.

Done when:

- Existing repositories can be migrated into SQL and later checked or repaired without manual database surgery.

## Slice 9: Packfile Storage and Serving

Purpose: improve storage and serving efficiency for large repositories.

Required capabilities:

- Store pack metadata.
- Store object-to-pack index rows.
- Read objects by pack offset.
- Generate packs for fetch.
- Accept packs from push.
- Support repack and garbage collection planning.

API candidates:

- `PackStore` trait.
- `ServerRepository::write_pack(...)`
- `ServerRepository::read_packed_object(...)`
- `ServerRepository::plan_repack(...)`

Storage work:

- Decide whether pack bytes live in SQL, object storage, or both.
- Track pack checksums and index checksums.
- Keep loose object and packed object lookup behavior equivalent.

Tests:

- Store and retrieve packed objects.
- Prefer newest object representation where duplicate objects exist.
- Pack index lookup.
- Import packed repositories once `grit-lib` exposes enough pack reading support.
- Repack preserves reachable objects.

Done when:

- Large repositories can be served without requiring every object to be stored and streamed as independent loose-object rows.

## Slice 10: Feature Parity Matrix and Conformance Suite

Purpose: make parity with `grit-lib` explicit and prevent regressions.

Required capabilities:

- Inventory `grit-lib` operations that matter for hosting.
- Classify each operation as unsupported, read-only, read-write, or optimized in `grit-lib-server`.
- Add tests that run equivalent operations against filesystem-backed `grit-lib` and server-backed storage.
- Track gaps for objects, refs, config, trees, commits, diffs, merges, protocol, and repository maintenance.

Artifacts:

- `grit-lib-server/PARITY.md`
- Conformance test helpers under `grit-lib-server/tests/support`.
- CI-friendly feature flags for tests that need SQL, Redis, or NATS.

Tests:

- Shared fixtures imported into both backends.
- Same inputs produce same typed outputs.
- Error classes match expected behavior even when message strings differ.
- Conformance tests avoid depending on the Git CLI.

Done when:

- The team can see exactly which `grit-lib` capabilities are available through `grit-lib-server`, and new server work is protected by parity tests.

## Cross-cutting Rules

- Keep durable storage authoritative; caches are performance layers only.
- Keep all public APIs typed. Do not expose argv-like strings except at protocol or CLI boundaries.
- Every public type and method needs doc comments.
- Do not shell out to Git from `grit-lib-server`.
- Prefer backend-agnostic tests against `MemoryServerStorage`, then add opt-in integration tests for SQL, Redis, and NATS.
- Ref updates must use compare-and-swap semantics wherever old value matters.
- Cache invalidation must happen after durable writes succeed.
- Repository, tenant, object, and ref identifiers must be scoped in every backend operation.
