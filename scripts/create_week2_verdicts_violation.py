import argparse
import json
from pathlib import Path


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

    # Break overall_verdict enum.
    records[0]["overall_verdict"] = "BROKEN"
    write_jsonl(Path(args.output), records)
    print(f"Wrote Week 2 verdict violation dataset: {args.output}")


if __name__ == "__main__":
    main()

