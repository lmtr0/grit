# In-memory repository import optimizations

## Scope and conclusion

This review covers `grit_lib_server::import::import_repository_with_options`
when the destination is `MemoryBackend`. It is based on the current import,
storage, repository-index, object-database, and pack-cache implementations. No
runtime profiler was used because the dominant costs are visible from the data
structures and asymptotic behavior.

The import is not primarily slow because of async overhead or progress output.
It has two scaling problems that should be fixed first:

1. The browse index materializes a full, flattened path snapshot for every
   distinct commit root tree. This discards Git tree structural sharing and can
   turn tens of thousands of Git objects into tens of millions of in-memory
   rows.
2. Every commit insertion scans all commits already in `MemoryBackend`, making
   initial commit import quadratic. The importer then reads and parses all
   commits again to repair the graph.

For scale, this repository currently has 7,367 reachable commits, 77,192
reachable object IDs, 7,253 distinct commit root trees, and 9,906 files at
`HEAD`. A systematic sample of 73 hash-sorted historical root trees averaged 8,129
leaf paths. Applying that sample average to all roots gives approximately 59
million flattened leaf rows. The importer also stores directory rows, so this
is a lower-bound estimate. It is roughly 760 browse rows per reachable object.
Even an unrealistically compact 100 bytes per row would consume about 5.9 GB;
the current representation stores paths and repository identity more than once
and uses `BTreeMap` nodes, so its actual overhead is higher.

## Priority summary

| Priority | Optimization | Main effect | Expected complexity change |
| --- | --- | --- | --- |
| P0 | Store native direct tree entries or browse trees lazily | Removes snapshot explosion | approximately `sum(files per root)` to `sum(entries in unique trees)` |
| P0 | Build commit metadata once and install it in a batch | Removes commit scan and repair duplication | `O(commits²)` to `O(commits + parent edges)` |
| P1 | Deduplicate reads and integrate size discovery with object copy | Avoids repeated pack inflation and blob allocation | Multiple reads per object toward one read per object/tree |
| P1 | Add owned, byte-bounded bulk storage methods | Reduces copying, locks, futures, and allocator traffic | Per-object overhead to per-batch overhead |
| P1 | Make re-import genuinely incremental | Avoids recopying the complete reachable closure | Full closure to new closure on normal updates |
| P2 | Shard memory state by repository and store keys once | Reduces row size and comparison/allocation cost | Lower constants and faster lookup |
| P2 | Add pack-native import after fixing packed lookup | Avoids retaining every decoded blob separately | Retained memory tracks pack/index bytes; validation still decodes every retained object |
| P3 | Add bounded parallel decode/read stages | Uses available CPU without unbounded memory growth | Wall-time improvement only after P0/P1 fixes |

## P0: replace flattened browse snapshots with Git-native tree indexing

### Current behavior

`grit-lib-server/src/import.rs:252-275` schedules the root tree of every commit.
`index_tree` then stores every descendant under a key containing the commit's
root tree OID and the descendant's full path (`import.rs:339-394`). The
`indexed_trees` key includes `(root, subtree_oid, prefix)`, so the same unchanged
subtree is expanded again for every changed root and every path at which it
appears.

This is the opposite of Git's tree model. A commit that changes one file
usually creates only the trees on that file's path; all other subtrees are
shared by OID. Flattening each root recreates the complete repository snapshot
for that commit and loses that sharing.

`MemoryBackend::upsert_tree_entries` compounds the cost
(`grit-lib-server/src/memory.rs:420-437`):

- every row is inserted into a `BTreeMap`;
- tenant and repository `String`s are cloned into every key;
- the full path is cloned into the key and retained again in the value;
- tree entries are cloned once more at insertion;
- later prefix queries scan the entire tree map (`memory.rs:461-482`), so the
  `BTreeMap` ordering is not currently being used to bound the scan.

### Recommended model

Store each unique Git tree object once as direct children:

```text
(repository, tree_oid, child_name) -> { mode, child_oid, kind, optional_size }
```

Resolve `src/commands/add.rs` by reading the direct `src` entry from the root
tree, then `commands`, then `add.rs`. `tree_at` can return a directory by one
direct-child query after resolving the requested path. This makes unchanged
subtrees reusable across every commit and branch.

Two reasonable variants are:

1. Materialize direct entries once per unique tree during import. This gives
   predictable browse latency and should be the default if a browse index is
   still desired.
2. Store only objects during import, parse tree objects on first browse, and
   retain a bounded `tree_oid -> parsed direct entries` cache. This gives the
   fastest startup and naturally avoids indexing history that is never viewed.

Blob size should be stored by blob OID or obtained from object metadata, not
duplicated in every root/path row. The public `BrowseIndex` contract currently
says it lists direct entries (`grit-lib-server/src/storage.rs:323-364`), so the
native-tree model also matches the stated API better than the current flattened
implementation.

### Transitional option

If the schema/API cannot change immediately, limit eager flattened indexing to
the trees currently named by refs, and lazily build older roots on demand. This
does not fix the representation, but it bounds startup work for repositories
with long history. It should be treated as a bridge rather than the final
design.

## P0: remove quadratic and duplicate commit-graph work

### Current behavior

`MemoryBackend::indexed_commit` scans the complete commit map for every commit
write (`grit-lib-server/src/memory.rs:65-93`). It is looking for a small list of
known parents, so those parents could be fetched directly by key. With `C`
commits, first import performs approximately `C * (C - 1) / 2` candidate scans.
For this repository that is about 27 million scans; for 100,000 commits it is
about five billion.

The traversal normally sees a child before its parents, so most of this scan
cannot even compute the final generation. After the traversal, the importer
calls `repair_commit_graph` (`import.rs:284`), which lists, reads, and parses
every commit again and replaces the graph (`repository.rs:746-786`).

Re-import is worse: the run-local `seen` set is empty at the start, so every
reachable commit is submitted again. If `C` commits are already in memory,
approximately `C²` existing rows are scanned even when nothing changed.

### Recommended implementation

- While an object is already in hand during traversal, parse each commit once
  and collect an `IndexedCommit` with generation temporarily unset or set to
  one.
- After discovery, compute generations from the collected parent adjacency
  data with the existing memoized method, then call `replace_commit_graph` or a
  bulk upsert once.
- Add a bulk-import path that stores an object without implicitly rebuilding a
  commit row. Automatic indexing can remain for isolated writes if callers rely
  on it.
- Independently fix isolated `MemoryBackend::write_object` calls by looking up
  each parent directly as `(repo, parent_oid)`. That changes the eager path from
  a scan of all commits to a handful of map lookups.

This also makes generation calculation deterministic regardless of traversal
order and eliminates the final full commit reread.

## P1: read, inflate, and copy object data fewer times

Several avoidable duplicate operations occur in the hot traversal:

- Tree-context deduplication happens after `source.odb.read` at
  `import.rs:232-265`. Repeated commits with exactly the same root tree do not
  create duplicate rows, but their root object is still read before the
  duplicate context is rejected. Check `(root, oid, prefix)` before reading.
- `index_tree` reads and fully materializes every blob just to obtain its size
  (`import.rs:359-365`). The later `ImportWork::Object` reads the blob again to
  import it. With flattened roots, the size read repeats for every occurrence
  of that path across history.
- The source object data is cloned to create `StoredObject`
  (`import.rs:232-243`), then `MemoryBackend::write_object` clones the complete
  object again (`memory.rs:149-163`). Large blobs therefore incur extra
  allocation and memory-bandwidth costs.
- Commit and tree payloads are parsed during traversal and commits are parsed
  again during graph repair.

Recommended changes:

- Maintain a small metadata table `ObjectId -> {kind, size}` populated by the
  same read that copies the object. Do not inflate blobs solely for browse-row
  sizing.
- Parse a unique tree OID once and reuse its direct entries. Avoid caching blob
  payloads beyond the time needed to hand them to storage; a full object cache
  would increase peak memory.
- Introduce an owned insertion path so the importer can move `Vec<u8>` into the
  backend. If a generic borrowed method must remain, add a memory-specific or
  trait-level bulk method taking owned `(ObjectId, StoredObject)` values.
- Treat blob-size read errors as real import errors or explicitly defer size.
  The current `.ok()` hides an ODB failure and then often repeats the failing
  read later.

## P1: add bulk storage operations and byte-bounded buffering

The storage contract only exposes one-object writes
(`grit-lib-server/src/storage.rs:159-177`). The importer awaits one boxed async
trait call and `MemoryBackend` takes/releases its object lock for every object.
Tree entries are batched only at one directory-tree parse at a time. Config and
refs are also written individually, although they are usually too small to
dominate.

Add bulk methods with safe defaults, for example:

```text
write_objects_owned(repository, Vec<(ObjectId, StoredObject)>)
upsert_direct_tree_entries(repository, Vec<DirectTreeEntry>)
replace_or_upsert_commits(repository, Vec<IndexedCommit>)
```

The memory implementation should reserve capacity and merge a batch while
holding each lock once. Persistent implementations can use a transaction and
set-based inserts. Bound batches by both object count and total payload bytes;
an object-count-only batch can hold several very large blobs and cause a peak
memory spike. A starting policy such as 1,000-4,000 objects with a 16-64 MiB
byte ceiling can be tuned from measurements rather than made part of the API.

Batching is more valuable after flattened browse rows are removed. Batching 59
million unnecessary rows would only make the wrong representation fail later.

## P1: make subsequent imports incremental

The importer deduplicates only within one invocation (`import.rs:220-230`). On
every re-import it walks, reads, copies, reports, and upserts the complete
reachable closure. `MemoryBackend`'s object map ignores an existing object, but
that check occurs only after the source read and payload clones. The report's
`objects` count consequently means "visited this run," not "newly stored."

For a completed prior import:

- load destination object IDs into one set (or expose a bulk membership API)
  rather than issuing one async existence query per object;
- stop walking at a known commit/tag boundary whose transitive closure was
  completed by an earlier successful import;
- with direct-tree indexing, stop at known tree OIDs as well;
- copy and index only newly reachable objects, then update refs;
- persist an import-complete manifest/ref snapshot so a partially failed import
  is not mistaken for a safe traversal boundary.

The existing checkpoints contain counters and live only in `ImportReport`.
They are progress notifications, not resumable checkpoints. A resumable design
needs at least phase, source ref snapshot, completed batch/frontier identity,
and a final completion marker.

Writing refs after the object/index batches are ready would also prevent a
failed import from exposing refs whose targets have not been copied yet.

## P2: reduce `MemoryBackend` key and row overhead

The current maps repeat `RepoKey = (TenantId, RepositoryId)` in every object,
tree, commit, ref, and config key. Both IDs own `String`s. This is especially
costly for browse rows, where the full path is also held in both key and value.

Prefer a two-level layout:

```text
repositories: HashMap<RepoKey, Arc<RepoState>>
RepoState {
    objects: HashMap<ObjectId, StoredObject>,
    trees: map keyed only by (tree_oid, child_name),
    commits: HashMap<ObjectId, IndexedCommit>,
    ...
}
```

This stores tenant/repository strings once per hosted repository and permits
per-repository capacity planning. Values should omit fields already present in
their map key, or share immutable strings with `Arc<str>` when both forms are
required. Use a hash map for exact access; use bounded `BTreeMap::range` calls
only where ordered prefix access is actually needed. The current
`list_tree_entries` full-map filter should be removed regardless of map choice.

## P2: consider pack-native import, with prerequisites

Most medium and large repositories already store almost all data in packs. The
current importer resolves and inflates each reachable object, then stores every
payload independently and uncompressed in `MemoryBackend`. A pack-native path
could instead retain pack bytes plus their indexes, dramatically reducing
startup allocations, memory usage, and copied bytes.

This is not safe as a quick switch. It needs decisions about whether import is
a full mirror (including unreachable packed objects) or a reachable-only
repack, and it needs delta-capable server pack ingestion. It also requires
fixing the current memory packed-object lookup first:

- `MemoryBackend::newest_packed_object` linearly scans pack indexes, scans a
  matching index twice, and clones the complete pack byte vector for each
  object read (`memory.rs:104-127`).
- Store pack data behind `Arc<[u8]>` and maintain an
  `ObjectId -> newest PackLocation` index so lookup is direct and does not copy
  the pack.
- Decode from a borrowed pack slice/range and retain a bounded delta-base cache.

After those prerequisites, direct ingestion of compatible source packs plus a
small loose-object overlay is likely the best long-term representation for a
large in-memory hosted repository.

Implemented: `MemoryBackend` now retains pack bytes behind shared immutable
storage, uses a repository-local `ObjectId -> newest PackLocation` index, and
decodes directly from the shared pack with the bounded delta-base cache in
`grit-lib`. Import options expose reachable-only and full-local-mirror modes.
Only self-contained, hash-compatible local packs with a checksum-valid
version-2 index cryptographically bound to the pack trailer and matching entry
CRCs are retained. Before publication, every retained object is fully decoded,
its delta program is applied, and its kind, size, and canonical SHA-1 or SHA-256
object ID are verified. Each resolved payload is discarded after validation;
the 96 MiB bounded delta-base cache plus the active object's delta-chain buffers
remain transient, rather than one uncompressed allocation per imported object.
FullMirror performs the same validation for unreachable packed objects.
Consequently, pack-native import primarily removes retained uncompressed
storage, duplicate long-lived allocation, and whole-pack copies—it does not
remove decode CPU. The bounded parallel validation described in P3 is the next
step for reducing wall time. Promisor, thin, corrupt, mixed-reachability,
alternate, or backend-unsupported cases fall back to the portable reachable
object traversal. Reachable eligibility includes the durable manifest from a
previously completed import, so an incremental import does not reject a pack
merely because its older reachable members were intentionally traversal stops.

## P3: use bounded parallelism only after reducing the work

`source.odb.read` is blocking filesystem/decompression work performed inside an
async function. A bounded worker pipeline could read/decode independent objects
in parallel and keep the async executor responsive. However, parallelizing the
current flattened browse expansion would increase allocation pressure and lock
contention without addressing the root cause.

After P0/P1:

- separate discovery/metadata, object decode, and batch-install stages;
- use a bounded queue sized by bytes, not only item count;
- keep writes as batch merges instead of having workers contend on the same
  `RwLock`;
- cap decompression concurrency independently for large blobs;
- preserve deterministic progress counts and error cancellation.

Per-object progress events and async-trait future allocation can then be
replaced with optional batch events. This is a lower-order optimization unless
the callback performs expensive work.

## Suggested implementation order

1. Add phase timings and counters described below.
2. Apply safe tactical fixes: direct parent lookups, reject duplicate tree
   contexts before ODB reads, collect commit rows during traversal, and install
   the commit graph once.
3. Replace flattened browse rows with direct-tree entries or lazy parsed-tree
   caching. Update `tree_at`, `blob_at`, repair, consistency checks, and both
   memory and SQL storage implementations together.
4. Add owned byte-bounded object batches and restructure memory state per
   repository.
5. Add completed-import manifests and incremental frontier pruning.
6. Optimize packed lookup and implement delta-capable pack-native import.
7. Add bounded parallel decoding if profiles still show available CPU as the
   limiting resource.

## Measurement and acceptance plan

Add import phase measurements for:

- ref/config copy;
- reachability discovery;
- source ODB reads, split by kind;
- source bytes inflated and destination bytes copied;
- duplicate ODB reads per OID;
- object batch installation;
- unique trees parsed and browse rows produced;
- commit generation/index installation;
- wall time, CPU time, and peak resident memory.

Use four repeatable repository shapes:

1. A long linear history reusing the same tree, to expose commit-graph scaling.
2. A long history changing one file in a wide tree, to measure structural
   sharing versus flattened snapshots.
3. A packed, delta-heavy repository with a few large blobs, to measure ODB and
   copy costs.
4. A completed import followed by one new commit, to measure incremental work.

The critical acceptance properties are complexity-based:

- doubling commit count with a reused tree should approximately double, not
  quadruple, import work;
- changing one file should add only new commit/tree/object metadata, not a full
  snapshot's browse rows;
- no blob should be inflated once for size and again for storage in the same
  import;
- a no-change re-import should perform ref comparison and near-zero object/tree
  writes;
- peak memory should be bounded by retained repository data plus configured
  batch/cache byte limits, not by the full import frontier.
