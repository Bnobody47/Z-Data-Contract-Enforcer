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

    src = Path(args.input)
    dst = Path(args.output)
    records = load_jsonl(src)
    if not records:
        raise ValueError(f"No records in {src}")

    # Canonical "dangerous meaning change":
    # confidence 0.0–1.0 → 0–100.
    for r in records:
        for fact in r.get("extracted_facts", []) or []:
            if isinstance(fact, dict) and isinstance(fact.get("confidence"), (int, float)):
                fact["confidence"] = round(float(fact["confidence"]) * 100.0, 1)

    # Extra realistic signals for AI contracts and prompt validation:
    # - break source_path (minLength) in one record
    # - create text drift by appending a long semantic marker in one fact
    r0 = records[0]
    if isinstance(r0, dict):
        r0["source_path"] = ""  # violates source_path minLength
        extracted_facts = r0.get("extracted_facts", []) or []
        if extracted_facts and isinstance(extracted_facts[0], dict):
            txt = extracted_facts[0].get("text", "")
            marker = " DRIFT_MARKER_TRP1_WEEK7 "
            filler = "X" * 9000
            extracted_facts[0]["text"] = str(txt) + marker + filler
            r0["extracted_facts"] = extracted_facts
    write_jsonl(dst, records)
    print(f"Wrote scale-change violation dataset: {dst}")


if __name__ == "__main__":
    main()

