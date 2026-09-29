# Streaming bulk-import into Neo4j — billions of nodes/rels, no CSV

Goal: load **billions of nodes/relationships** into Neo4j **without ever materialising a CSV file**,
by streaming rows straight from a source system (here **DuckDB**) into Neo4j's offline bulk-import engine.

This document explains why the obvious "streaming" approaches (a FIFO / a `python gen.py | neo4j-admin` pipe)
**cannot** work with `neo4j-admin database import`, and gives a working, benchmarked alternative.

Tested with `neo4j:5` (5.26.31), JDK 21, DuckDB JDBC 1.4.4.0, on Docker Desktop / Apple Silicon.

---

## TL;DR

| Approach | CSV / files? | Billions? | Verdict |
|---|---|---|---|
| **FIFO / named pipe / stdin** as a CSV source | — | ❌ | **Impossible by design** (see below) |
| **`.csv.gz` on disk** + `neo4j-admin import` | yes (compressed) | ✅ | Supported fallback if temp files are acceptable |
| **Programmatic `BatchImporter` + a streamed `Input`** | **none** | ✅ | **Recommended** — this repo |
| Driver `UNWIND` in batched transactions | none | ❌ | Online/incremental loads only, far too slow at bulk scale |

Measured throughput of the streamed `Input` (this repo): **~4.5M entities/s**, peak heap **~1.5 GiB**,
flat across scales → linear scaling. Extrapolation: **1B nodes + 2B rels ≈ ~11 min** of import CPU.

---

## Why a FIFO (or `python … | neo4j-admin …`) cannot work

`neo4j-admin database import full` is only a **CSV front-end** over the engine. The engine consumes an
abstract `Input` (`org.neo4j.batchimport.api.input.Input`). The CSV implementation, `CsvInput`, **opens each
source more than once, in separate phases**:

1. **Estimate pass** — `Input.validateAndEstimate(...)` opens the source, extracts the header and
   **samples up to 1 MiB** to estimate node/relationship counts (for memory sizing). It also calls
   `source.length()` to extrapolate — meaningless for a pipe (`0`).
2. **Import pass** — `Input.nodes()` creates a **new** `data.stream()` = the source is **re-opened**.
3. (Optional) a third open in `referencedNodeSchema()` when there are constraints/indexes.

A FIFO is **non-seekable and single-shot**: once the producer has written its data and closed, the **second
`open()` blocks forever** (no writer left). That is exactly the observed symptom:

```
timeout 120 docker run --rm -v $PWD:/import -v data:/data -w /import neo4j:5 \
  neo4j-admin database import full --nodes=Person=person-header.csv,nodes.fifo --overwrite-destination neo4j
# ...prints "Available resources / High parallel IO: true" then HANGS. exit 137 (SIGKILL by timeout). No IMPORT DONE.
```

Two independent tests (header in a separate file; header inside the stream) both hang.
On Docker Desktop the block can happen even on the *first* open, because a host FIFO bind-mounted through
the VirtioFS layer does not connect its two ends across the VM boundary — but the source code shows it would
block at the estimate→import re-open **anyway**.

> **Consequence:** you cannot "handle" the multi-pass at the pipe level — no `tee`/buffering restores
> re-openability + a length. The engine requires a **re-openable, replayable source with a known length**.

A tmpfs/ramdisk file *is* such a source (real file semantics, no disk), but it must fit the whole dataset in
RAM — not an option for billions. The right fix is to stop pretending the source is a file.

---

## The solution: feed the `BatchImporter` a streamed `Input`

Bypass the CSV front-end. Implement `Input` so that:

- **`validateAndEstimate` returns known estimates directly** via `Input.knownEstimates(...)` — this removes
  both FIFO blockers (no sampling open, no `source.length()`).
- nodes/relationships are produced through the **chunk + visitor** protocol
  (`InputIterator` → `InputChunk` → `InputEntityVisitor`), pushing native values
  (`id(long)`, `labels(...)`, `property(k, v)`, `startId/endId/type`, `endOfEntity()`), **no CSV text**.

Then drive the engine exactly as `neo4j-admin`'s `ImportCommand$Full` does:

```java
StorageEngineFactory sef = StorageEngineFactory.selectStorageEngine(dbConfig);
BatchImporter importer = sef.batchImporter(
        databaseLayout, fileSystem, PageCacheTracer.NULL,
        org.neo4j.batchimport.api.Configuration.DEFAULT, logService,
        System.out /*progress*/, false /*verbose*/, DefaultAdditionalIds.EMPTY, dbConfig,
        new PrintingImportLogicMonitor(System.out, System.err), jobScheduler, Collector.EMPTY,
        TransactionLogInitializer.getLogFilesInitializer(), new IndexImporterFactoryImpl(),
        EmptyMemoryTracker.INSTANCE, CursorContextFactory.NULL_CONTEXT_FACTORY);
importer.doImport(input);   // input = our DuckDB-backed streamed Input
```

See [`DuckImport.java`](./DuckImport.java) for the full ~250-line implementation.

### Two hard rules the engine imposes on any custom `Input`

1. **`InputIterator` must be thread-safe.** The engine runs several reader threads calling `next(chunk)`
   concurrently. We partition the id space with an `AtomicLong` and give each thread its **own** DuckDB
   connection via `DuckDBConnection.duplicate()` (same instance, safe concurrent reads).
2. **`InputIterable` must be replayable.** `iterator()` may be called more than once (nodes stage, and
   relationships can be read in multiple rounds). Re-querying DuckDB satisfies this for free — this is the
   *same* "re-openable" requirement as files, but now trivially met by a query engine.

### Why DuckDB is a good fit as the source

- Embedded (single JDBC jar, in-process) — no extra service to run.
- **Re-queryable** → replayable `InputIterable` with zero extra work.
- Range-partitionable on a contiguous id column → clean thread-safe chunking
  (`WHERE id >= ? AND id < ?`).
- Using `IdType.INTEGER` (node ids are the actual record ids) keeps id-mapping memory low → flat ~1.5 GiB heap.

Swap DuckDB for anything re-readable (Parquet via DuckDB, Postgres, object storage, a deterministic generator);
only `DuckChunk.next(...)` and the two SQL strings change.

---

## Running it

Requires Docker only (the `neo4j:5` image provides JDK 21 + all Neo4j jars; the script fetches the DuckDB jar).

```bash
./run.sh                        # smoke test: 1000 / 1000, -Xmx4g
./run.sh 5000000 10000000       # 5M nodes / 10M rels
./run.sh 50000000 100000000 8g  # 50M nodes / 100M rels, -Xmx8g
```

Expected tail:

```
IMPORT DONE in 30s 861ms.
Imported:
  50000000 nodes
  100000000 relationships
  50000000 properties
Peak memory usage: 1.462GiB

RESULT: 50000000 nodes + 100000000 rels imported in 33.5s  (~4482155 entities/s)  NO CSV
```

The store is built offline under `/tmp/duckwork/neo4j-home` inside the container (ephemeral, `--rm`).
To keep/inspect it, mount a volume at that path and point a Neo4j server at the resulting database.

### Measured results

| Scale | Import time | Throughput | Peak heap |
|---|---|---|---|
| 1K nodes / 1K rels | 0.46 s | — | ~1 GiB |
| 5M nodes / 10M rels | 2.4 s | ~4.3M entities/s | 1.04 GiB |
| 50M nodes / 100M rels | 30.9 s | ~4.5M entities/s | 1.46 GiB |

---

## Caveats & notes

- **Offline / single machine.** Like `neo4j-admin import`, this builds store files directly; the target
  database must not be running. The batch importer is single-node, multi-threaded.
- **Semi-internal API.** `org.neo4j.batchimport.api.*` is the intended extension surface, but the concrete
  factory wiring (`StorageEngineFactory.batchImporter(...)`, `DefaultAdditionalIds`, `IndexImporterFactoryImpl`,
  `TransactionLogInitializer`) lives in `internal`/impl packages and **can change between major/minor versions**.
  This example is pinned to **5.26.31**; re-verify signatures when upgrading (the `--add-opens` flags mirror
  what `neo4j-admin` passes: `java.base/{java.io,java.nio,sun.nio.ch}`).
- **Estimates are advisory.** `knownEstimates(...)` only sizes caches; approximate values are fine and never
  affect correctness.
- **`IdType.INTEGER`** assumes node ids are dense longs you control. For arbitrary string keys use
  `IdType.STRING` with id groups (extra id-mapping memory).

## Files

- [`DuckImport.java`](./DuckImport.java) — the streamed `Input` + `BatchImporter` driver.
- [`run.sh`](./run.sh) — build & run inside `neo4j:5` (no local JDK needed).
