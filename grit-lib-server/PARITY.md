# grit-lib-server Parity Matrix

This matrix tracks the `grit-lib` capabilities that matter to a hosting platform and the current
status of equivalent `grit-lib-server` APIs. The server crate is intentionally narrower than the
filesystem library: it exposes typed repository operations for hosted browsing, protocol serving,
storage repair, policy, and cache invalidation without making a filesystem checkout authoritative.

Status values:

- `unsupported`: no server API yet.
- `read-only`: server can answer queries but does not mutate this area.
- `read-write`: server can query and mutate authoritative storage.
- `optimized`: server has hosting-oriented indexes, cache layers, pack storage, or protocol paths
  beyond a direct filesystem walk.

## Matrix

| Area | grit-lib capability used for hosting | grit-lib-server status | Server API or boundary | Notes and gaps |
| --- | --- | --- | --- | --- |
| Repository identity | Open/init filesystem repositories and address a git-dir/worktree | read-write | `ServerRepository`, `TenantId`, `RepositoryId`, SQL lifecycle APIs | Server repositories are tenant scoped and backend-owned. Filesystem discovery remains in `grit-lib`. |
| Object database | Read/write loose object payloads by typed object id and kind | read-write | `ObjectStore`, `ServerRepository::read_object`, `write_object` | Object ids are computed with `grit-lib` object hashing. |
| Packed objects | Decode/store/read packfiles and object index rows | optimized | `PackStore`, `write_pack`, `read_packed_object`, `build_pack`, `plan_repack` | Delta objects are still rejected in server pack ingestion. |
| Refs | Read/list direct and symbolic refs, resolve `HEAD`, branches, and tags | read-write | `RefStore`, `resolve_ref`, `default_branch`, `branches`, `tags` | Ref writes use compare-and-swap guards where callers provide expected state. |
| Reflogs | Append/read reflog entries for writes | read-write | `ReflogStore`, receive-pack apply path | Filesystem reflog parsing parity is not covered by default conformance tests yet. |
| Config | Read/list/set repository-local config values | read-write | `ConfigStore`, import config copy | Server config is key/value storage, not a full config parser. Parsing stays in `grit-lib`. |
| Commit objects | Parse commits into hosting summaries | read-only | `commit`, `commit_summary`, `RepositorySummary` | Summary fields intentionally mirror parsed commit data, not CLI formatting. |
| Commit graph | Parents, children, ancestor checks, merge base, reachability, ahead/behind | optimized | `CommitGraphStore`, `commit_history`, `is_ancestor`, `merge_base`, `compare_commits` | Indexed graph is repairable from stored commit objects. |
| Trees | Parse tree objects and browse paths from revisions | optimized | `BrowseIndex`, `tree_at`, `list_tree`, `repair_browse_index` | Import/repair materializes tree path rows for hosting views. |
| Blobs | Resolve revision/path to blob content, mode, and size | optimized | `blob_at`, `read_blob_at_path`, `discover_files` | Symlinks are represented as blob entries with mode `120000`. |
| Tags | Lightweight and annotated tag listing and peeling | read-only | `tags`, `commit`, `compare_inputs` | Tag mutation and signing policy are not separate APIs yet. |
| Compare inputs | Normalize two commit-ish values for later compare/diff views | read-only | `compare_inputs`, `compare_commits` | Tree diff generation is still delegated to future diff work. |
| Diffs | Tree/index/worktree diff algorithms and patch formatting | unsupported | None | Hosting diff APIs are a gap after commit/tree parity. |
| Merges | Merge-base is available; content merge and merge-tree operations are not | read-only | `merge_base`, `is_ancestor` | Server does not run merge strategies or update working trees. |
| Upload-pack | Advertise refs, negotiate wants/haves, build fetch pack responses | optimized | `UploadPackService`, `advertise_refs`, `negotiate_fetch`, `build_fetch_pack` | Protocol v0/v1 path exists; protocol v2/shallow behavior remains future work. |
| Receive-pack | Parse push requests, validate closure, enforce policy, update refs | read-write | `ReceivePackService`, `prepare_push`, `receive_push`, `apply_push` | Object quarantine is typed but still in-process storage. |
| Authorization | Host-provided auth and branch/tag policy hooks | read-write | `AuthorizationProvider`, `RepositoryPolicy`, `PushPolicy`, `AuditSink` | Platform user models remain outside this crate. |
| Import/export | Import from filesystem `grit-lib`, repair indexes, export for backup/debugging | optimized | `import_repository`, `NativePackImportMode`, `repair_*`, `check_repository_consistency`, `export_repository` | Reachable import is the default. `MemoryBackend` can retain checksum-bound, CRC-validated, self-contained v2 source packs after every retained object passes full decode/hash verification; payloads are discarded rather than stored loose. Full-mirror mode also validates and includes unreachable local packed and loose objects. Unsafe packs and other backends use reachable loose-object fallback. |
| Caching | Cache objects, refs, config, commit summaries, and tree listings | optimized | `CachedStorage`, `LayeredCache`, `RedisCache`, NATS invalidation | Durable storage remains authoritative. |
| Repository maintenance | Consistency checks, browse/graph repair, repack planning | read-write | `maintenance`, `plan_repack` | Garbage collection execution is not implemented. |

## Conformance Coverage

The default conformance suite in `tests/conformance.rs` builds a filesystem-backed fixture with
`grit-lib`, imports it into `MemoryBackend`, and compares normalized typed results for:

- object payloads and repository-local config;
- refs, symbolic `HEAD`, branches, tags, and summary counts;
- commit summaries, tree views, blob views, conventional-file discovery, and compare inputs;
- commit parents, ancestor checks, merge base, reachable commits, ahead/behind, and paginated
  history;
- representative error classes for missing refs, missing paths, and wrong object kinds;
- browse-index repair against the filesystem tree oracle.

The tests avoid the Git CLI. They create objects, trees, commits, refs, and tags through `grit-lib`
APIs and compare server results against parsed filesystem repository data.

## Feature Gates for CI

Default CI can run without external services:

```bash
cargo test -p grit-lib-server
```

Live backend tests are opt-in, feature-gated, and ignored by default:

```bash
GRIT_LIB_SERVER_POSTGRES_URL=postgres://... cargo test -p grit-lib-server --features live-sqlx-postgres-tests -- --ignored
GRIT_LIB_SERVER_REDIS_URL=redis://... cargo test -p grit-lib-server --features live-redis-tests -- --ignored
GRIT_LIB_SERVER_NATS_URL=nats://... cargo test -p grit-lib-server --features live-nats-tests -- --ignored
```

The aliases map to the implementation features (`sqlx-postgres`, `redis`, and `nats`) so CI jobs
can select only the external service they provision.
