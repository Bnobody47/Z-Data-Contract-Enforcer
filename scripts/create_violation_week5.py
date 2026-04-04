import argparse
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path


def parse_iso(value):
    if value is None:
        return None
    s = str(value).replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(s)
    except Exception:
        return None


def to_iso_utc(dt: datetime):
    return dt.replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")


def load_jsonl(path: Path):
    records = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                records.append(json.loads(line))
    return records


def write_jsonl(path: Path, records):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for r in records:
            f.write(json.dumps(r) + "\n")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()

    records = load_jsonl(Path(args.input))
    if not records:
        raise ValueError("No input records")

    # Pick the first record and break temporal causality:
    # recorded_at must be >= occurred_at.
    r0 = records[0]
    occurred = parse_iso(r0.get("occurred_at"))
    recorded = parse_iso(r0.get("recorded_at"))
    if occurred and recorded:
        r0["recorded_at"] = to_iso_utc(occurred - timedelta(seconds=120))

    # Break strict +1 sequence by duplicating the first record's sequence_number
    # within its aggregate_id.
    agg = r0.get("aggregate_id")
    seq = r0.get("sequence_number")
    if agg is not None and seq is not None:
        for r in records[1:]:
            if r.get("aggregate_id") == agg:
                r["sequence_number"] = seq  # duplicate → should fail strict increment
                break

    write_jsonl(Path(args.output), records)
    print(f"Wrote Week 5 violation dataset: {args.output}")


if __name__ == "__main__":
    main()

