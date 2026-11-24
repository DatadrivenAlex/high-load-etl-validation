"""Concurrent readers, bounded buffering, and a transactional SQLite writer."""

import csv
import json
import re
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from queue import Full, Queue
from threading import Event
from time import perf_counter

FIELDS = ("event_id", "revision", "occurred_at", "currency", "amount_minor")
KEY = re.compile(r"[A-Za-z0-9_-]{1,64}\Z")
INTEGER = re.compile(r"[0-9]{1,15}\Z")
MAX_FRAME = 65_536
SCHEMA = Path(__file__).with_name("schema.sql").read_text(encoding="utf-8")
UPSERT = Path(__file__).with_name("upsert.sql").read_text(encoding="utf-8")


@dataclass(frozen=True)
class Source:
    name: str
    path: Path
    kind: str
    columns: dict[str, str]


class QualityGateError(Exception):
    def __init__(self, sources, counts):
        super().__init__("quality gate rejected: " + ", ".join(sources))
        self.counts = counts


def read_manifest(path):
    items = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(items, list) or not items:
        raise ValueError("manifest must be a nonempty JSON array")
    base = path.resolve().parent
    seen = set()
    sources = []
    for item in items:
        name, kind, relative = item["name"], item["format"], Path(item["path"])
        if not KEY.fullmatch(name) or name in seen or kind not in {"csv", "jsonl"}:
            raise ValueError("source names must be unique keys and formats must be csv or jsonl")
        file = (base / relative).resolve()
        if relative.is_absolute() or not file.is_relative_to(base):
            raise ValueError("source paths must resolve inside the manifest directory")
        columns = item.get("columns", {})
        if (
            not isinstance(columns, dict)
            or set(columns) - set(FIELDS)
            or not all(isinstance(v, str) and v for v in columns.values())
        ):
            raise ValueError("columns must map canonical field names to nonempty input names")
        mapped = {key: columns.get(key, key) for key in FIELDS}
        if len(set(mapped.values())) != len(FIELDS):
            raise ValueError("mapped input names must be distinct")
        seen.add(name)
        sources.append(Source(name, file, kind, mapped))
    return sources


def lines(file):
    number = 0
    while raw := file.readline(MAX_FRAME + 1):
        number += 1
        if len(raw) > MAX_FRAME:
            while raw and not raw.endswith(b"\n"):
                raw = file.readline(MAX_FRAME + 1)
            yield number, None, "line_too_long"
        else:
            try:
                yield number, raw.decode("utf-8"), None
            except UnicodeDecodeError:
                yield number, None, "invalid_utf8"


def records(source):
    # CSV and JSONL each use one physical UTF-8 line per record.
    with source.path.open("rb") as file:
        rows = lines(file)
        header = None
        if source.kind == "csv":
            first = next(rows, None)
            if first is None or first[2]:
                raise ValueError("missing or unreadable CSV header")
            header = next(csv.reader([first[1].removeprefix("\ufeff")], strict=True))
            if len(header) != len(set(header)) or not set(source.columns.values()) <= set(header):
                raise ValueError("CSV header is missing required columns or contains duplicates")
        for number, text, error in rows:
            if error:
                yield number, None, error
                continue
            try:
                if header is not None:
                    values = next(csv.reader([text], strict=True))
                    row = dict(zip(header, values, strict=True))
                else:
                    row = json.loads(text)
                if not isinstance(row, dict):
                    raise ValueError("expected object")
                yield number, row, None
            except (ValueError, RecursionError, csv.Error):
                yield number, None, f"invalid_{source.kind}_record"


def normalized(source, row):
    def text(field):
        value = row.get(source.columns[field])
        return value.strip() if isinstance(value, str) else ""

    def number(field):
        value = row.get(source.columns[field])
        if type(value) is int:
            return value if 0 <= value < 10**15 else -1
        return int(value) if isinstance(value, str) and INTEGER.fullmatch(value.strip()) else -1

    event_id = text("event_id")
    if not KEY.fullmatch(event_id):
        raise ValueError("invalid_event_id")
    revision = number("revision")
    if revision < 1:
        raise ValueError("invalid_revision")
    value = text("occurred_at")
    try:
        if "T" not in value:
            raise ValueError
        stamp = datetime.fromisoformat(value)
        if stamp.utcoffset() is None:
            raise ValueError
        at = stamp.astimezone(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")
    except (ValueError, OverflowError):
        raise ValueError("invalid_timestamp") from None
    currency = text("currency").upper()
    if currency not in {"EUR", "USD", "GBP"}:
        raise ValueError("invalid_currency")
    amount = number("amount_minor")
    if not 0 <= amount <= 10**9:
        raise ValueError("invalid_amount")
    return source.name, event_id, revision, at, currency, amount


def ingest(sources, database, *, workers=4, batch_size=1000, max_reject_percent=2):
    if not sources or len({s.name for s in sources}) != len(sources):
        raise ValueError("at least one source and unique source names are required")
    if not 1 <= workers <= 32 or not 1 <= batch_size <= 10_000:
        raise ValueError("workers must be 1..32 and batch size 1..10000")
    if not 0 <= max_reject_percent <= 100:
        raise ValueError("maximum reject percent must be 0..100")
    started = perf_counter()
    queue = Queue(maxsize=2 * workers)
    stop = Event()
    counts = {s.name: dict(read=0, rejected=0, written=0, ignored=0) for s in sources}

    def put(message):
        while not stop.is_set():
            try:
                queue.put(message, timeout=0.1)
                return True
            except Full:
                continue
        return False

    def reader(source):
        good, bad = [], []
        try:
            for n, row, error in records(source):
                if stop.is_set():
                    return
                if not error:
                    try:
                        good.append(normalized(source, row))
                    except ValueError as exc:
                        error = str(exc)
                if error:
                    bad.append((source.name, n, error))
                if len(good) + len(bad) >= batch_size:
                    if not put((source.name, good, bad, None)):
                        return
                    good, bad = [], []
            if good or bad:
                put((source.name, good, bad, None))
            put((source.name, None, None, None))
        except Exception as exc:
            # Failed readers report an outcome instead of leaving the writer blocked.
            put((source.name, None, None, exc))

    db = sqlite3.connect(database, timeout=30)
    try:
        db.executescript(SCHEMA)
        with ThreadPoolExecutor(max_workers=workers) as pool, db:
            db.execute("BEGIN IMMEDIATE")
            db.executemany("DELETE FROM rejects WHERE source = ?", ((s.name,) for s in sources))
            try:
                for source in sources:
                    pool.submit(reader, source)
                left = len(sources)
                while left:
                    name, good, bad, failure = queue.get()
                    if failure is not None:
                        raise RuntimeError(f"{name}: {failure}") from failure
                    if good is None:
                        left -= 1
                        continue
                    before = db.total_changes
                    db.executemany(UPSERT, good)
                    written = db.total_changes - before
                    db.executemany("INSERT INTO rejects VALUES (?, ?, ?)", bad)
                    count = counts[name]
                    count["read"] += len(good) + len(bad)
                    count["rejected"] += len(bad)
                    count["written"] += written
                    count["ignored"] += len(good) - written
                failed = [
                    name
                    for name, x in counts.items()
                    if not x["read"] or x["rejected"] * 100 > max_reject_percent * x["read"]
                ]
                if failed:
                    raise QualityGateError(failed, counts)
            finally:
                stop.set()
    finally:
        db.close()
    elapsed = perf_counter() - started
    read = sum(c["read"] for c in counts.values())
    return {
        "sources": counts,
        "rows_read": read,
        "elapsed_seconds": round(elapsed, 3),
        "rows_per_second": round(read / elapsed) if elapsed else 0,
    }
