# Wavelength — Music Discovery Engine

A distributed music taste-graph and recommendation system built on Apache Spark, Scala GraphX, Kafka and PySpark MLlib. It ingests **live** listening behaviour, models `users ↔ songs ↔ artists ↔ genres` as a heterogeneous graph, discovers taste communities and the "bridge songs" that connect them, and serves the results as a stateless REST API.

## Architecture

```
   Last.fm API                MusicBrainz API
        │                            │
        ▼                            ▼
 ┌──────────────────────────────────────────┐
 │   pollers  (APScheduler, every N sec)    │──── data/state/  (since-last-time cursors)
 └──────────────────┬───────────────────────┘
                    │  listening-events    catalog-releases
                    ▼           ▼
          ┌────────────────────────────┐
          │        Apache Kafka        │
          └─────────────┬──────────────┘
                        ▼
   ┌──────────────────────────────────────────────┐
   │  Spark Structured Streaming  (two queries)   │──▶ data/events_store/   append
   │  + broadcast-join enrichment                 │──▶ data/catalog_store/  upsert
   └─────────────────────┬────────────────────────┘
                         ▼
   ┌──────────────────────────────────────────────────┐
   │  Periodic batch jobs                             │
   │   sql_analytics   RDD vs DataFrame benchmark     │
   │   graph_analysis  (GraphFrames)                  │
   │   scala-modules   (native GraphX)                │
   │   ml/  ALS · classifiers · K-Means               │
   │   evaluation/                                    │
   └─────────────────────┬────────────────────────────┘
                         ▼   precomputed Parquet / JSON
   ┌──────────────────────────────────────────────────┐
   │  serving/   FastAPI — read-only, stateless       │──▶ /docs
   └──────────────────────────────────────────────────┘
```

The processing, graph, ML and serving layers never depend on how the data arrived — only the `pollers/` edge does.

## Project Structure

```
wavelength/
├── config/
│   └── settings.json            # Kafka, Spark, path and stream configuration
├── data/                        # runtime stores (gitignored)
│   ├── raw/                     # bootstrap datasets
│   ├── checkpoints/             # one checkpoint dir per streaming query
│   ├── streaming/               # file-source fallback directories
│   ├── state/                   # poller "since last time" cursors
│   ├── events_store/            # append-only listening events (Parquet)
│   └── catalog_store/           # upserted catalog dimension table
├── src/
│   ├── pollers/                 # live Last.fm + MusicBrainz pollers
│   ├── loadtest/                # isolated synthetic load-test producer
│   ├── ingestion/               # Spark Structured Streaming consumers
│   ├── sql_analytics/           # Spark SQL queries, window fns, wildcards
│   ├── graph_analysis/          # GraphFrames: communities, PageRank, bridges
│   ├── ml/
│   │   ├── recommendation/      # ALS on implicit feedback
│   │   ├── classification/      # skip vs listen models
│   │   └── clustering/          # K-Means taste segmentation
│   ├── evaluation/              # metrics, plots, experiments
│   ├── serving/                 # FastAPI REST layer
│   ├── common/                  # shared config loader
│   ├── create_kafka_topics.py   # creates listening-events, catalog-releases
│   └── hello_spark.py           # Spark connectivity smoke test
├── scala-modules/               # native Scala RDD + GraphX jobs
│   └── src/main/scala/
├── tests/
│   ├── unit/
│   └── integration/
├── report/                      # architecture doc + final report
├── notebooks/                   # exploration notebooks (gitignored)
├── results/                     # generated metrics & plots (gitignored)
├── .env.example                 # credential template
├── .gitattributes
├── .gitignore
├── docker-compose.yml           # Kafka, KRaft mode
└── requirements.txt
```

## Phase → Location Map

| Phase | Roadmap focus | Where |
|---|---|---|
| 0 | Environment setup, Kafka topics | `docker-compose.yml`, `src/create_kafka_topics.py` |
| 1 | Architecture + API contract | `report/`, this README |
| 2 | Dual-stream ingestion, RDD/SQL | `src/pollers/`, `src/ingestion/`, `src/sql_analytics/` |
| 3 | Native Scala module | `scala-modules/` |
| 4 | Graph modelling | `src/graph_analysis/`, `scala-modules/` (GraphX) |
| 5 | ML — recommendation, classification, clustering | `src/ml/` |
| 6 | Evaluation & experiments | `src/evaluation/`, `results/` |
| 7 | Serving API | `src/serving/` |
| 8 | Integration, documentation, report | `report/`, README |

## Data Provenance

| Kind | Source | Lands in |
|---|---|---|
| **Bootstrap** | Last.fm historical dataset, loaded once | `data/events_store/` |
| **Live** | Last.fm API polled on a schedule | `data/events_store/` (tagged `source=live`) |
| **Catalog** | MusicBrainz new releases | `data/catalog_store/` |
| **Load-test** | Synthetic replay, scalability only | separate `listening-events-loadtest` topic, never mixed with live data |

## Prerequisites

- **Python** 3.10+
- **Java** 17+ (Spark 4.x officially targets 17/21 — verified working on 20)
- **Spark 4.2.x** — the Python client comes from `requirements.txt` (pyspark, self-contained). `SPARK_HOME` is set system-wide to `C:\spark` (Spark 4.2.0) and is optional — but if you keep it, both must stay on the same version.
- **sbt** — only for the Scala module
- **Docker** — only for Kafka

## Quick Start

```bash
# 1. Python dependencies
pip install -r requirements.txt

# 2. Credentials
cp .env.example .env
#    fill in LASTFM_API_KEY and MB_USER_AGENT

# 3. Kafka (optional for local development)
docker compose up -d
python src/create_kafka_topics.py

# 4. Smoke tests
python src/hello_spark.py
python -m pytest tests/unit/ -v
```

GraphFrames is resolved automatically at runtime from `spark.jars.packages`
(`io.graphframes:graphframes-spark4_2.13:0.12.2`), set in `config/settings.json`.

## API Contract

| Endpoint | Returns |
|---|---|
| `GET /recommendations/{userId}` | `{songId, title, artist, score, reason}` |
| `GET /trending` | `{songId, title, trendScore, windowStart, windowEnd}` |
| `GET /communities/{userId}` | `{communityId, topGenres, size}` |
| `GET /search?q=` | wildcard search over the catalog |
| `GET /song/{songId}/explain/{userId}` | recommendation explanation |
| `GET /system/status` | counters, last retrain, catalog size |

Responses are flat, typed JSON. The API reads only precomputed Parquet/JSON —
no Spark job is ever triggered by a request.

## Tech Stack

| Layer | Technology |
|---|---|
| Sources | Last.fm API, MusicBrainz API (APScheduler polling) |
| Messaging | Apache Kafka (KRaft) |
| Streaming | Spark Structured Streaming |
| Processing | PySpark, Spark SQL, RDDs |
| Graph | GraphFrames 0.12.2, Scala GraphX |
| ML | Spark MLlib (ALS, Logistic Regression, Decision Tree, Naive Bayes, K-Means) |
| Serving | FastAPI, Pydantic |
| Build | sbt (Scala), pip (Python) |
