import csv
import json
import sqlite3

import pytest

from etl_lab.demo import generate
from etl_lab.pipeline import QualityGateError, ingest, normalized, read_manifest


def row(
    event="evt_1", revision=1, timestamp="2025-01-01T02:00:00+02:00", currency="eur", amount=123
):
    return {
        "event_id": event,
        "revision": revision,
        "occurred_at": timestamp,
        "currency": currency,
        "amount_minor": amount,
    }


def source(tmp_path, rows, name="s"):
    path = tmp_path / f"{name}.jsonl"
    path.write_text("".join(json.dumps(x) + "\n" for x in rows), encoding="utf-8")
    manifest = tmp_path / f"{name}.json"
    manifest.write_text(json.dumps([{"name": name, "format": "jsonl", "path": path.name}]))
    return read_manifest(manifest)[0]


def test_normalizes_offset_to_utc_and_integer_minor_units(tmp_path):
    assert normalized(source(tmp_path, [row()]), row()) == (
        "s",
        "evt_1",
        1,
        "2025-01-01T00:00:00.000000Z",
        "EUR",
        123,
    )


@pytest.mark.parametrize(
    "data,reason",
    [
        (row(event="../x"), "invalid_event_id"),
        (row(revision=0), "invalid_revision"),
        (row(timestamp="2025-01-01T00:00:00"), "invalid_timestamp"),
        (row(currency="BTC"), "invalid_currency"),
        (row(amount=-1), "invalid_amount"),
        ({"event_id": "x"}, "invalid_revision"),
    ],
)
def test_rejects_bad_fields(tmp_path, data, reason):
    with pytest.raises(ValueError, match=reason):
        normalized(source(tmp_path, [data]), data)


def test_mixed_sources_and_daily_summary(tmp_path):
    manifest = generate(tmp_path / "inputs", sources=4, rows=50)
    db = tmp_path / "events.sqlite"
    result = ingest(read_manifest(manifest), db, workers=2, batch_size=17)
    assert result["rows_read"] == 200
    with sqlite3.connect(db) as conn:
        assert conn.execute("SELECT count(*) FROM events").fetchone()[0] == 200
        assert conn.execute("SELECT count(*) FROM daily_totals").fetchone()[0] > 0


def test_idempotency_and_latest_revision_wins(tmp_path):
    src = source(tmp_path, [row()])
    db = tmp_path / "db.sqlite"
    ingest([src], db)
    ingest([src], db)
    for version, amount in ((3, 777), (2, 1)):
        src = source(tmp_path, [row(revision=version, amount=amount)])
        result = ingest([src], db)
    with sqlite3.connect(db) as conn:
        assert conn.execute("SELECT revision, amount_minor FROM events").fetchone() == (3, 777)
    assert result["sources"]["s"]["ignored"] == 1


def test_quality_gate_rolls_back_and_reject_ledger_refreshes(tmp_path):
    src = source(tmp_path, [row(amount=10), row(currency="bad")])
    db = tmp_path / "db.sqlite"
    with pytest.raises(QualityGateError) as error:
        ingest([src], db, max_reject_percent=0)
    assert error.value.counts["s"]["rejected"] == 1
    with sqlite3.connect(db) as conn:
        assert conn.execute("SELECT count(*) FROM events").fetchone()[0] == 0
        assert conn.execute("SELECT count(*) FROM rejects").fetchone()[0] == 0
    ingest([source(tmp_path, [row(amount=10)])], db)
    with sqlite3.connect(db) as conn:
        assert conn.execute("SELECT count(*) FROM rejects").fetchone()[0] == 0


def test_duplicate_business_keys_within_batch_are_safe(tmp_path):
    src = source(tmp_path, [row(revision=1), row(revision=2, amount=9)])
    ingest([src], tmp_path / "db.sqlite", batch_size=10)
    with sqlite3.connect(tmp_path / "db.sqlite") as conn:
        assert conn.execute("SELECT revision, amount_minor FROM events").fetchone() == (2, 9)


def test_manifest_path_traversal_rejected(tmp_path):
    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps([{"name": "s", "format": "jsonl", "path": "../bad"}]))
    with pytest.raises(ValueError, match="inside"):
        read_manifest(manifest)


def test_csv_mapping(tmp_path):
    path = tmp_path / "input.csv"
    with path.open("w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerow(["key", "rev", "at", "ccy", "minor"])
        writer.writerow(["abc", "1", "2025-01-01T00:00:00Z", "USD", "7"])
    manifest = tmp_path / "mapped.json"
    mapping = dict(
        zip(
            ["event_id", "revision", "occurred_at", "currency", "amount_minor"],
            ["key", "rev", "at", "ccy", "minor"],
            strict=True,
        )
    )
    manifest.write_text(
        json.dumps([{"name": "csv", "format": "csv", "path": path.name, "columns": mapping}])
    )
    ingest(read_manifest(manifest), tmp_path / "db.sqlite")
    with sqlite3.connect(tmp_path / "db.sqlite") as conn:
        assert conn.execute("SELECT event_id, amount_minor FROM events").fetchone() == ("abc", 7)


def test_missing_source_rolls_back_without_hanging(tmp_path):
    src = source(tmp_path, [row()])
    lost = type(src)("lost", tmp_path / "absent.jsonl", "jsonl", src.columns)
    db = tmp_path / "db.sqlite"
    with pytest.raises(RuntimeError, match="lost"):
        ingest([src, lost], db, workers=2)
    with sqlite3.connect(db) as conn:
        assert conn.execute("SELECT count(*) FROM events").fetchone()[0] == 0


def test_many_sources_and_bounded_queue(tmp_path):
    sources = [source(tmp_path, [row(event=f"e{i}")], name=f"s{i}") for i in range(16)]
    result = ingest(sources, tmp_path / "db.sqlite", workers=2, batch_size=1)
    assert result["rows_read"] == 16
