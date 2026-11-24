"""Deterministic synthetic CSV and JSONL sources with mixed schema names."""

import csv
import json

from etl_lab.pipeline import FIELDS


def generate(directory, sources=24, rows=10_000):
    if sources < 1 or rows < 1:
        raise ValueError("sources and rows must be positive")
    directory.mkdir(parents=True, exist_ok=False)
    specs = []
    for index in range(sources):
        name, kind = f"source_{index:03}", "csv" if index % 2 == 0 else "jsonl"
        path = f"{name}.{kind}"
        columns = dict(zip(FIELDS, ("id", "version", "time", "unit", "minor"), strict=True))
        specs.append(
            {
                "name": name,
                "format": kind,
                "path": path,
                "columns": columns if kind == "csv" else {},
            }
        )
        with (directory / path).open("w", encoding="utf-8", newline="") as file:
            if kind == "csv":
                csv.writer(file).writerow(columns.values())
            for n in range(1, rows + 1):
                currency = "INVALID" if n % 101 == 0 else ("EUR", "USD", "GBP")[index % 3]
                values = (
                    f"e{n}",
                    1,
                    f"2025-04-{1 + n % 28:02}T01:00:00+02:00",
                    currency,
                    index * 100 + n % 1000,
                )
                if kind == "csv":
                    csv.writer(file).writerow(values)
                else:
                    file.write(json.dumps(dict(zip(FIELDS, values, strict=True))) + "\n")
    manifest = directory / "manifest.json"
    manifest.write_text(json.dumps(specs, indent=2) + "\n", encoding="utf-8")
    return manifest
