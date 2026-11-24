# High-Load ETL & Data Validation Pipeline

A small, reproducible demonstration of parallel ingestion from CSV/JSONL feeds into a single SQLite analytical store. Source readers stream bounded records into a bounded queue; one transactional writer normalizes values, applies revision-aware idempotent upserts, records rejects, and commits only when each feed passes a configurable quality gate. The included deterministic generator creates 24 mixed-format feeds (240,000 rows by default) so the benchmark can run locally without credentials, APIs, or external data.

## Design

```text
CSV / JSONL feeds ── parallel bounded readers ── bounded batches ── single SQLite writer
                                                                    ├── events + daily_totals
                                                                    └── per-source rejects
```

Each source has a stable name and file, with optional input-column mappings. Timestamps must carry a UTC offset and are normalized to UTC. Money-like values are integer minor units (no floating-point arithmetic); currencies are explicitly limited to EUR, USD and GBP. A newer revision replaces an older event; an older/equal replay does not. A run uses one SQLite transaction, and a failed source or quality gate rolls the run back. Only the active sources' reject records are refreshed when a run commits.

The quality gate rejects a run if any source has no records or its invalid-row rate exceeds `--max-reject-percent` (default 2%). Invalid lines are quarantined by source and line number, not silently skipped. CSV/JSONL records are capped at 64 KiB per physical line. The bounded queue limits inter-stage buffering; `--workers` caps concurrent input readers. SQLite is deliberately the single-writer analytical sink, not a claim of a distributed high-throughput database. This lab shows correctness and back-pressure mechanics; tune batch sizes, storage and partitioning and benchmark representative workloads before production use.

## Quick start

Requires Python 3.11+; runtime code uses only the standard library.

```bash
python -m venv .venv
. .venv/bin/activate
python -m pip install -e '.[test]'
python -m etl_lab generate demo --sources 24 --rows 10000
python -m etl_lab ingest demo/manifest.json run/events.sqlite --workers 4 --batch-size 1000
python -m etl_lab report run/events.sqlite
pytest -q
ruff check .
```

`generate` writes deterministic synthetic inputs into a new directory and a manifest. `ingest` prints per-source rows read, rejected, written and ignored, plus throughput. `report` reads the UTC daily aggregate view. An unsuccessful quality gate exits nonzero and reports `committed: false`.

## Bring your own feeds

Create a manifest beside the source files:

```json
[
  {"name":"orders_a","format":"csv","path":"orders.csv",
   "columns":{"event_id":"order_id","revision":"version","occurred_at":"created_at","currency":"ccy","amount_minor":"amount_cents"}},
  {"name":"orders_b","format":"jsonl","path":"other.jsonl"}
]
```

Each record needs `event_id`, positive integer `revision`, offset-aware ISO-8601 `occurred_at`, supported `currency`, and non-negative integer `amount_minor` (up to 1,000,000,000). JSONL uses canonical names unless `columns` maps them. Source paths must remain inside the manifest's directory; source names are stable keys for provenance and idempotency. Use a fresh manifest directory per run if you need to generate the demo more than once.

## Docker

```bash
docker build -t high-load-etl-validation .
docker run --rm -v "$PWD/demo:/data:ro" -v "$PWD/run:/out" \
  high-load-etl-validation ingest /data/manifest.json /out/events.sqlite
```

The image runs as a non-root user; make the output directory writable by the container's UID/GID (65532), or choose a writable bind mount. CI runs Ruff and pytest on Python 3.11–3.13 and a 24-source generated smoke test on each version.
