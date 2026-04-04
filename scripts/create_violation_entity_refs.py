import argparse
import json
import uuid
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

    r0 = records[0]
    bad_id = str(uuid.uuid4())  # not present in entities[].entity_id
    extracted_facts = r0.get("extracted_facts", []) or []
    if extracted_facts and isinstance(extracted_facts[0], dict):
        extracted_facts[0]["entity_refs"] = [bad_id]
        r0["extracted_facts"] = extracted_facts

    write_jsonl(Path(args.output), records)
    print(f"Wrote entity-ref violation dataset: {args.output}")


if __name__ == "__main__":
    main()

