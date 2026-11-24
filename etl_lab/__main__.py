"""Command-line entry point for generation, ingestion and reports."""

import argparse
import json
import sqlite3
import sys
from pathlib import Path

from etl_lab.demo import generate
from etl_lab.pipeline import QualityGateError, ingest, read_manifest


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    gen = commands.add_parser("generate", help="create deterministic demo inputs")
    gen.add_argument("directory", type=Path)
    gen.add_argument("--sources", type=int, default=24)
    gen.add_argument("--rows", type=int, default=10_000)
    run = commands.add_parser("ingest", help="run one transactional ingestion")
    run.add_argument("manifest", type=Path)
    run.add_argument("database", type=Path)
    run.add_argument("--workers", type=int, default=4)
    run.add_argument("--batch-size", type=int, default=1000)
    run.add_argument("--max-reject-percent", type=int, default=2)
    report = commands.add_parser("report", help="print UTC daily totals by currency")
    report.add_argument("database", type=Path)
    args = parser.parse_args()
    try:
        if args.command == "generate":
            print(generate(args.directory, args.sources, args.rows))
            return 0
        if args.command == "ingest":
            args.database.parent.mkdir(parents=True, exist_ok=True)
            result = ingest(
                read_manifest(args.manifest),
                args.database,
                workers=args.workers,
                batch_size=args.batch_size,
                max_reject_percent=args.max_reject_percent,
            )
        else:
            uri = args.database.resolve().as_uri() + "?mode=ro"
            with sqlite3.connect(uri, uri=True) as db:
                db.row_factory = sqlite3.Row
                result = [
                    dict(row)
                    for row in db.execute("SELECT * FROM daily_totals ORDER BY utc_day, currency")
                ]
        print(json.dumps(result, indent=2))
        return 0
    except QualityGateError as exc:
        print(
            json.dumps({"error": str(exc), "committed": False, "sources": exc.counts}),
            file=sys.stderr,
        )
        return 2
    except (KeyError, ValueError, TypeError, RuntimeError, OSError, sqlite3.Error) as exc:
        print(json.dumps({"error": str(exc)}), file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
