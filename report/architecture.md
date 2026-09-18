# Wavelength — Architecture Design

> Companion document to `README.md`. The README shows *what* the system looks
> like; this document explains *why* it looks that way.
>
> **Primary syllabus target:** INT315 Unit I and **CO1** — *analyse the
> similarities and differences between Spark and Hadoop.* Section 5 is the
> CO1 artifact. Section 6 carries CO3 (in-memory RDD + shared variables) and
> Sections 2, 3 and 8 carry CO5 (workflow analysis). Section 10 maps every
> syllabus item to its section.

---

## 1. Purpose and scope

Wavelength is a music discovery engine that polls two public APIs on a
schedule, streams the results through Kafka, processes them with Apache
Spark, and serves precomputed recommendations over a read-only REST API.

Design goals, in priority order:

1. **The data must be real.** Every row in `listening-events` originates from
   an actual Last.fm response. No fabricated metrics anywhere in the report.
2. **The pipeline must be live, not replayed.** A file replayed on a timer
   looks identical in a demo but teaches nothing about watermarks, late data,
   checkpoints or consumer lag.
3. **One engine, four workloads.** Streaming, SQL, graph and machine learning
   all run on Spark so the project reads as one coherent system rather than
   five scripts glued together.
4. **Syllabus fidelity.** Every CO, Unit and Practical in INT315 must be
   demonstrable from this repository.

Out of scope: a frontend UI, multi-tenant security, and production-grade
observability. The demo surface is FastAPI's auto-generated `/docs`.

---

## 2. System layers

Each layer is listed with the problem it solves and the cost of removing it.

### 2.1 Sources — Last.fm and MusicBrainz

Two independent APIs with different cadences. Last.fm gives per-user
listening history (the behavioural signal); MusicBrainz gives newly released
catalogue entries (the freshness signal).

*Why a separate layer:* they have different rate limits, different response
shapes and different poll intervals (45 s vs 300 s). Collapsing them into one
poller would couple their failure modes.

### 2.2 Ingestion — `src/pollers/`

APScheduler jobs that call each API and publish to Kafka. The critical
component here is the **since-last-time cursor store** (`data/state/`): one
`username → last_seen_ts` entry per seed user.

*Why it matters:* without the cursor every poll replays each user's entire
recent history, flooding Kafka with duplicates. This is the single most
important correctness detail in the live design, and it is why the cursor
lives on disk rather than in process memory — a restart must not lose it.

### 2.3 Transport — Apache Kafka

Two topics: `listening-events` and `catalog-releases`, both in **KRaft mode**
(no ZooKeeper), 2 partitions, replication factor 1.

*Why Kafka and not a direct call from poller to Spark:* decoupling. The
poller never waits for a processor; a slow or crashed Spark job never blocks
API polling. Kafka also gives us a replayable log, so the streaming layer can
be restarted without re-polling the source.

*Why two topics and not one:* they carry different schemas, different
retention needs and different consumers. Mixing them forces every consumer to
discriminate on message shape.

### 2.4 Stream processing — Spark Structured Streaming

Two queries: one appends validated events to `data/events_store/`, one
upserts catalog rows into `data/catalog_store/`.

Responsibilities:

- schema validation, dropping malformed rows
- `withWatermark` on event time to bound state growth
- deduplication on `(userId, songId, timestamp)`
- **broadcast join** of the catalog onto the event stream

*Why streaming here and not batch:* a batch pass every N minutes would be
simpler, but it makes "recent plays" artificially stale and it forfeits the
checkpointing/recovery behaviour that Practical P4 requires us to demonstrate.

### 2.5 Storage — partitioned Parquet

`data/events_store/` is append-only Parquet partitioned by date. The catalog
store is a slowly-changing dimension and is upserted with Delta `MERGE INTO`.

*Why Parquet and not a database:* the consumers are analytical (Spark jobs,
pandas, FastAPI serving reads). Columnar files give predicate pushdown,
compression and zero operational overhead on a laptop.

*Why Delta for the catalog only:* upserts on a plain-Parquet directory are
awkward and lossy. Delta gives ACID `MERGE` on exactly one table that needs
it, without imposing Delta on the high-volume append-only event store.

### 2.6 Batch compute — `src/sql_analytics/`, `graph_analysis/`, `ml/`, `scala-modules/`

Periodic jobs over the stored data: SQL analytics, an RDD-vs-DataFrame
benchmark, GraphFrames/GraphX community detection, and the MLlib models.

*Why this is a separate layer from streaming:* these jobs are iterative and
expensive. Running ALS or PageRank inside a streaming query would stall
microbatches. Separating them means model retraining cadence is an explicit,
independent choice.

### 2.7 Serving — `src/serving/`

FastAPI, **read-only**, stateless, serving precomputed Parquet/JSON.

*Hard rule:* the API process never constructs a `SparkSession`. Starting
Spark costs seconds-to-minutes and hundreds of megabytes to gigabytes of heap
per request; every response must be answerable from a file written by an
earlier batch job. Statelessness also means the API can be killed and
restarted with zero loss.

### 2.8 Detailed data flow

Expanded from the README overview: seven stages, both topics, the state and
checkpoint directories, the CO3 shared variables, and the deliberately
separate load-test lane.

```text
+------------------------------------------------------------------------------+
| STAGE 1 - SOURCES                                                            |
+----------------------------------+-------------------------------------------+
| Last.fm  ws.audioscrobbler.com   |  MusicBrainz  musicbrainz.org             |
| method: user.getRecentTracks     |  path: /ws/2/release                      |
| scope : 80 validated accounts    |  etiquette: 1 req/s + descriptive UA      |
+-----------------+----------------+--------------------+----------------------+
                  |                 |                    |
                  | JSON            |                    | JSON
                  v                 |                    v
+------------------------------------------------------------------------------+
| STAGE 2 - POLLERS   src/pollers/   (APScheduler)                             |
+----------------------------------+-------------------------------------------+
| events_poller.py   every 45 s     |  catalog_poller.py   every 300 s         |
| + state.py cursor store           |  release feed is already "since last",   |
|   data/state/poll_state.json      |  so no cursor is required                |
|   username -> last_seen_ts        |                                          |
|   on disk: a restart must not     |                                          |
|   lose it or history replays      |                                          |
+-----------------+----------------+--------------------+----------------------+
                  |                 |                    |
                  v                 |                    v
+------------------------------------------------------------------------------+
| STAGE 3 - KAFKA   docker-compose   KRaft mode (no ZooKeeper)                 |
+----------------------------------+-------------------------------------------+
| topic listening-events            |  topic catalog-releases                  |
| 2 partitions - replication factor |  2 partitions - replication factor 1     |
|           1                       |                                          |
+-----------------+----------------+--------------------+----------------------+
                  |                 |                    |
                  v                 |                    v
+------------------------------------------------------------------------------+
| STAGE 4 - SPARK STRUCTURED STREAMING   src/ingestion/                        |
+----------------------------------+-------------------------------------------+
| events_stream.py                 |  catalog_stream.py                        |
| 1 schema validation               | 1 schema validation                      |
| 2 withWatermark(eventTime, 30m)   | 2 foreachBatch -> Delta MERGE INTO       |
| 3 dedup (userId,songId,timestamp) |    on songId                             |
| 4 broadcast join <- catalog [CO3] | 3 upsert -> data/catalog_store/          |
| 5 accumulator: malformed    [CO3] |                                          |
| 6 append -> data/events_store/    |                                          |
|    Parquet partitioned by date    |                                          |
| 7 checkpoint data/checkpoints/    |                                          |
|    events                         |                                          |
+-----------------+----------------+--------------------+----------------------+
                  |                 |                    |
                  v                 |                    v
+------------------------------------------------------------------------------+
| STAGE 5 - BATCH JOBS   periodic, over the Parquet / Delta tables             |
+-----------------+----------------+------------------+------------------------+
| sql_analytics/  | graph_analysis/|  ml/             |  scala-modules/        |
| temp views      | GraphFrames    |  ALS             |  RDD vs DataFrame      |
| window functions| PageRank       |  4 classifiers   |  native GraphX         |
| joins/wildcards | Label Prop.    |  K-Means         |  benchmark             |
| [P2 - Unit IV]  | bridge songs   |  regression      |  [P3 - CO2]            |
|                 | [CO6]          |  [P1 - Unit VI]  |                        |
+-----------------+----------------+------------------+------------------------+
                  |                 |                    |
                  v                 v                    v
+------------------------------------------------------------------------------+
| results/ (metrics, plots, benchmarks)   ->   report/ (narrative + appendix)  |
+----------------------------------+-------------------------------------------+
                  ^                |
                  |                v
+------------------------------------------------------------------------------+
| STAGE 6 - SERVING   src/serving/   FastAPI, read-only, stateless             |
+------------------------------------------------------------------------------+
| HARD RULE: never constructs a SparkSession - reads only precomputed files    |
| /recommendations/{userId}    /trending           /communities/{userId}       |
| /search?q=                   /song/{id}/explain/{userId}   /system/status    |
| the Stage 4 accumulator counter is surfaced at /system/status                |
+------------------------------------------------------------------------------+

+------------------------------------------------------------------------------+
| SEPARATE LANE - LOAD TEST   src/loadtest/   never touches live data          |
+------------------------------------------------------------------------------+
| loadtest_producer.py  ->  topics listening-events-loadtest                   |
|                           catalog-releases-loadtest                          |
| output is labelled "scalability stress test" and reported separately         |
+------------------------------------------------------------------------------+
```

Notes on the diagram:

- **Two independent lanes at Stage 3.** Live traffic and load-test traffic
  occupy different topics; no metric can ever be computed across both.
- **State lives on disk at Stage 2** (`data/state/poll_state.json`) and
  **checkpoints on disk at Stage 4** (`data/checkpoints/`). Both are
  configuration, so relocating them to HDFS or S3 is a config edit (§8.3).
- **CO3 shared variables are marked inline** — broadcast at Stage 4 step 4,
  accumulator at Stage 4 step 5.
- **Stage 5 and Stage 6 are strictly one-way.** Batch jobs write files;
  FastAPI reads files. The API never reaches back into Spark (§2.7).
- **Every arrow in Stages 1-5 carries real data.** Only Stage 7 is synthetic.

---

## 3. Batch versus real-time (Unit I)

Unit I asks us to distinguish batch from stream processing. Wavelength uses
**both**, deliberately:

| | Streaming path | Batch path |
|---|---|---|
| Trigger | continuous, event-driven | scheduled / on demand |
| Latency goal | seconds | minutes to hours |
| Workload | validation, dedup, enrichment, append | ALS, PageRank, K-Means, SQL aggregates |
| State | watermark + checkpoint | full recomputation from Parquet |

This is *not* a full Lambda architecture — there is no separate speed layer
and batch layer reconciling into a serving layer. There is one storage layer
and two classes of job over it. That is a deliberate simplification: the
streaming layer does the cheap, latency-sensitive work; the batch layer does
the expensive, throughput-sensitive work. The two never disagree because they
read the same Parquet.

---

## 4. Why not "just batch"

The alternative considered was a single scheduled job that replays API
responses every N minutes. It was rejected because:

1. it cannot demonstrate Practical P4 (Spark Streaming) or Unit V
   (Kafka producer/consumer, ingestion patterns);
2. it hides every hard part of the problem — watermarks, late data,
   checkpoint recovery, consumer offsets;
3. the project's stated goal is a genuinely live pipeline.

The cost of the live design is real: we now own cursor state, dedup logic and
recovery tests. Section 8 states what that buys and what it still does not
guarantee.

---

## 5. Spark versus Hadoop (CO1)

This section is the CO1 artifact. It is organised as: similarities,
differences, then the justification for choosing Spark.

### 5.1 Similarities

| Aspect | Both frameworks |
|---|---|
| Origin and licence | Apache Software Foundation, open source |
| Deployment model | Horizontal scale-out across commodity machines |
| Fault tolerance | Failed work is re-derived rather than lost |
| Storage compatibility | Both read/write HDFS and object stores |
| Cluster management | Spark commonly runs *on* YARN, i.e. inside the Hadoop stack |
| Data model | Partitioned data-parallel processing over collections |
| Ecosystem role | Both are batch analytics engines at their core |

The most important similarity is often overlooked: **Spark is not a
replacement for Hadoop as a whole.** Hadoop is a stack — HDFS (storage),
YARN (resource management), MapReduce (the original compute engine). Spark
replaces the *compute engine* while routinely continuing to use HDFS and
YARN underneath. Calling them mutually exclusive is incorrect.

### 5.2 Differences

| Dimension | Hadoop MapReduce | Apache Spark |
|---|---|---|
| Processing model | Fixed two-phase map → reduce, with a materialised shuffle between them | Arbitrary DAG of transformations; the engine plans and optimises it |
| Intermediate data | Written to local/replicated disk after every stage | Held in memory; written to disk only on eviction, spill or explicit `persist` |
| Iterative workloads | Each iteration re-reads and re-writes HDFS — quadratic I/O cost | RDDs cached between iterations; subsequent passes read memory |
| Latency class | Batch only; typically minutes to hours | Batch, streaming (ms–s latency) and interactive SQL in one engine |
| Abstraction level | Only two primitives exposed: map and reduce | RDD, DataFrame, Dataset, plus SQL, streaming, ML and graph APIs |
| Language support | Java-centric (other bindings exist) | Scala, Python, Java, R, SQL all first-class |
| Fault tolerance | Re-execute the failed task from its on-disk input | Rebuild partitions from RDD **lineage**; optionally checkpoint to disk |
| Composition | Multiple MapReduce jobs chained via HDFS files | Chained transformations planned as a single optimised job |
| Ecosystem | HDFS + YARN + MapReduce + Hive/Pig/HBase | Spark Core + SQL + Streaming + MLlib + GraphX |
| Interactive use | Not practical — job submission overhead dominates | Supported (shell, notebooks, ad-hoc SQL) |

### 5.3 Why Spark for this project

1. **The workload is iterative.** ALS alternates between user/item updates
   for many iterations; K-Means reassigns points repeatedly; PageRank and
   Label Propagation iterate until convergence. Under MapReduce every one of
   those iterations pays a full HDFS round-trip. This is precisely the
   workload Spark was designed for.

2. **Four engines, one stack.** Wavelength needs streaming (Unit V), SQL
   (Unit IV), graph (Unit III / CO6) and ML (Unit VI / CO4). Assembling
   those on Hadoop would mean Kafka/Storm + Hive + Giraph/TinkerPop +
   Mahout/Spark-ML — four integration surfaces. Spark exposes all four as
   libraries in one process.

3. **Two languages, both required.** The project is written in Python for
   velocity; the syllabus requires Scala (Unit II, CO2). Spark treats both as
   first-class, so `scala-modules/` shares the same cluster, the same data
   and the same APIs as the Python code.

4. **Local mode is genuinely useful.** `local[*]` runs the entire stack on one
   laptop with no cluster to provision, while the same job submits unchanged
   to YARN or Kubernetes later.

5. **Streaming and batch share a codebase.** The Structured Streaming queries
   and the batch jobs use the same DataFrame API, so enrichment logic is
   written once.

### 5.4 What we give up by choosing Spark

- **Lineage can be expensive.** Rebuilding a partition from lineage on a long
  chain costs recomputation. Mitigated by checkpointing the streaming queries
  (which we do) and by short batch jobs.
- **Spark does not provide storage.** Hadoop's HDFS is still the durable,
  replicated store. We use local Parquet on a laptop, which has no
  replication — see Section 8.
- **Spill and shuffle are real.** Wide transformations shuffle across the
  network; at laptop scale this is invisible, at cluster scale it dominates
  cost. Our benchmark (Phase 2.9) exists to measure this rather than assert it.

### 5.5 Note on published performance figures

Apache Spark's documentation and the original RDD paper — Zaharia et al.,
*Resilient Distributed Datasets: A Fault-Tolerant Abstraction for In-Memory
Cluster Computing*, NSDI 2012 — report speedups on the order of **10× for
general workloads and up to ~100× for iterative, in-memory workloads**
relative to Hadoop MapReduce. Those are **third-party published claims, not
our measurements**, and they depend entirely on fitting the workload into
memory.

This project does not repeat them as results. Phase 2.9 instead times the
*same* aggregation implemented as an RDD and as a DataFrame at three data
sizes, and the report will show **our** numbers, whatever they turn out to
be. If the result is underwhelming it gets reported as-is.

---

## 6. In-memory computation and shared variables (CO3)

CO3 requires RDD in-memory computation and the shared-variable mechanisms.
Both appear concretely in this design:

| Mechanism | Where it is used | Why |
|---|---|---|
| RDD in-memory | `scala-modules` and `sql_analytics` benchmarks, cached DataFrames in graph/ML jobs | Avoid re-reading Parquet on every transformation of a multi-step job |
| **Broadcast variable** | Catalog table broadcast onto the event stream (Phase 2.8) | The catalog is small, read-only and needed on every executor — broadcasting avoids a per-record network shuffle |
| **Accumulator** | Malformed/dropped event counter across the streaming job (Phase 2.5) | Executors can only add to it, never read it — the correct pattern for distributed counters, later surfaced at `/system/status` |
| Lineage | Automatic RDD/DAG lineage | Fault tolerance without materialising every intermediate to disk |

---

## 7. Technology choice defence

| Choice | Why | Rejected alternative | Why not |
|---|---|---|---|
| Spark 4.2.0 | Matches the installed toolchain exactly; one version across pip, `SPARK_HOME`, `build.sbt` and Maven artifacts | Spark 3.x | Would force version reconciliation across four places for no benefit |
| Kafka (KRaft) | Required Practical P5; durable replayable log | Direct poller → Spark call | No replay, no decoupling, cannot demonstrate consumer patterns |
| KRaft, no ZooKeeper | ZooKeeper removed from Kafka 4.x; one less moving part | ZooKeeper mode | Extra container, extra failure mode, deprecated |
| Structured Streaming | Stateful, checkpointed, exactly the API Unit V asks for | Spark Streaming (DStreams) | Legacy micro-batch API, superseded |
| Parquet | Columnar, compressed, predicate pushdown, Spark-native | CSV / JSON files | No schema, no pushdown, far larger on disk |
| Delta (catalog only) | ACID `MERGE` for one slowly-changing dimension | Delta everywhere | Imposes log files on a high-volume append path for no gain |
| FastAPI | Pydantic models give free OpenAPI at `/docs` — the demo surface | Flask | No automatic schema/docs |
| Read-only serving | API reads files written by batch jobs | API calling Spark per request | Seconds of latency and a heavyweight process per request |
| APScheduler | In-process cron for the pollers | Airflow / Prefect | Orchestration overhead far beyond two periodic jobs |
| 80 validated seed accounts | Last.fm removed `group.getMembers`/`user.getFriends`, so discovery is by probing a curated pool | Friend-graph crawl | The API methods no longer exist |
| Separate load-test topics | Keeps "the system is live" unfalsifiable | Load-testing the live topic | Synthetic volume would poison every metric derived from it |

---

## 8. Scaling story

### 8.1 Where we are

| Dimension | Current value |
|---|---|
| Spark master | `local[*]` on one laptop |
| Kafka | single broker, KRaft, 2 partitions, RF 1 |
| Seed users | 80 active accounts, bounded |
| Storage | local Parquet, no replication |
| Batch cadence | manual / on demand |

### 8.2 Growth axes, in the order they bind

1. **Seed list size** — drives poll volume, event volume and graph size.
   Adding accounts is the cheapest scaling lever and the only one that also
   improves data quality.
2. **Kafka partitions** — currently 2. Throughput scales with partition count
   until the single broker saturates; beyond that the broker itself must be
   replicated.
3. **Spark parallelism** — `local[*]` already uses all cores. Moving to a
   cluster changes `master` and nothing else in the code.
4. **Retrain cadence** — ALS/K-Means cost scales with data size and feature
   width; this is an operational knob, not a code change.

### 8.3 Path to a cluster

The job code is deliberately free of deployment specifics:

- `SparkSession.builder.master(...)` is read from config, not hard-coded.
- No executor uses absolute local paths — everything is relative to the
  configured data root.
- Checkpoint locations are configuration, so moving them to HDFS/S3 is a
  config edit.

Two realistic targets:

- **YARN** — if a Hadoop cluster is available, Spark submits with
  `--master yarn` and reads/writes HDFS. This is the classic "Spark on
  Hadoop" arrangement described in Section 5.1.
- **Kubernetes** — Spark's own scheduler, no Hadoop dependency required.

Periodic batch jobs (graph, ML, benchmark) are the natural candidates for a
cluster; the streaming queries and FastAPI service can stay small.

Concretely, a heavier periodic job moves by changing `spark.master` in
`config/settings.json` (or the matching `spark-submit` flag) — the code does
not change:

```bash
# what runs today — laptop, every core
spark-submit --master 'local[*]' <job>.py

# target A — classic Spark on Hadoop (Section 5.1)
spark-submit --master yarn --deploy-mode cluster <job>.py

# target B — Spark's own scheduler, no Hadoop dependency
spark-submit --master k8s://https://<api-server>:6443 \
  --deploy-mode cluster \
  --conf spark.kubernetes.container.image=<spark-image> <job>.py
```

What deliberately does *not* move: the pollers (lightweight HTTP clients),
the two streaming queries (low volume, long-lived, checkpoint-recovery is
already proven locally) and the FastAPI service (stateless reads of files).
Those stay on one machine until the seed list grows by orders of magnitude,
which is growth axis 1 in Section 8.2 — not something a cluster fixes.

### 8.4 What breaks first

| Failure | Consequence | Mitigation |
|---|---|---|
| Single Kafka broker dies | Ingestion stops; nothing is lost (disk-backed log) | Restart; documented as a known limitation, not hidden |
| Streaming query killed | Possible in-flight loss | Checkpoint recovery test (Phase 6.7) proves no loss/duplication |
| Disk fills | Parquet writes fail | Partition pruning + retention policy |
| Too few seed accounts | Thin graph, unstable communities | Honest population-size note in the report |
| Shuffle spill at scale | Performance collapse | The Phase 2.9 benchmark exists to surface this early |

---

## 9. Honest limitations

Stated plainly so the report is not oversold:

1. **Not exactly-once end to end.** Deduplication is idempotent and
   checkpointed, but there is no distributed transaction spanning Kafka,
   Spark and Parquet. A crash in a narrow window can duplicate a message;
   dedup on `(userId, songId, timestamp)` is what makes that harmless.
2. **No replicated storage.** Local Parquet on a laptop has no redundancy.
3. **Bounded graph population.** 80 accounts produce a small graph. Community
   counts are indicative, not statistically powerful, and the report says so.
4. **Bootstrap dataset is undecided.** If historical data is unavailable the
   project starts at zero volume and the report will show growth from zero
   rather than a populated history.
5. **Laptop numbers do not extrapolate.** Timings from `local[*]` on one
   machine are useful for *relative* comparison (RDD vs DataFrame, Scala vs
   Python) and useless as absolute cluster projections.

---

## 10. Traceability

| Syllabus item | Section |
|---|---|
| CO1 — Spark vs Hadoop similarities/differences | 5.1, 5.2, 5.3 |
| CO3 — in-memory RDD, shared variables | 6 |
| CO5 — analyse workflows / processing / analysis | 2, 3, 8 |
| Unit I — MapReduce limits, batch vs real-time, Spark features | 3, 4, 5 |
| Unit I — standalone install, Spark vs Hadoop ecosystem | 5.1, 7 |
| Unit V — Kafka architecture, integration, ingestion patterns | 2.2, 2.3, 2.4 |
| Unit VI — MLlib techniques | 2.6, 7 |
| Practical P4 — Spark Streaming | 2.4, 8.3 |
| Practical P5 — Kafka producer/consumer | 2.2, 2.3 |

---

## 11. Open decisions

| Item | Status |
|---|---|
| Bootstrap historical dataset | Deferred to the start of Phase 2 |
| LLM provider for explainability / judge | Open — due before Phase 7.7 |
| Embeddings provider for semantic search | Open — due before Phase 5.12 |
| GraphFrames vs native GraphX weighting | Both kept; comparison table is a Phase 4 deliverable |
