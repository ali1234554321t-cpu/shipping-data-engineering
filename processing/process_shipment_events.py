import json
from pathlib import Path

import pandas as pd

SOURCE = Path("data/raw/shipment_events.jsonl")
OUTPUT = Path("data/processed/shipment_events_clean.csv")


def main():
    records = []

    with SOURCE.open("r", encoding="utf-8") as file:
        for line in file:
            if line.strip():
                records.append(json.loads(line))

    df = pd.DataFrame(records)
    rows_before = len(df)

    df["event_time"] = pd.to_datetime(
        df["event_time"], errors="coerce", utc=True
    )

    df = df.dropna(
        subset=["shipment_id", "status", "location", "event_time"]
    )
    df = df.drop_duplicates()

    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUTPUT, index=False)

    print(f"Rows before cleaning: {rows_before}")
    print(f"Rows after cleaning: {len(df)}")
    print(f"Rows removed: {rows_before - len(df)}")
    print(f"Saved to: {OUTPUT}")


if __name__ == "__main__":
    main()