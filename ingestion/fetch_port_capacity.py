import json
from pathlib import Path

import requests

URL = "https://data.bts.gov/api/v3/views/y5ut-ibwt/query.json"
OUTPUT = Path("data/raw/port_capacity.json")


def main():
    all_rows = []
    page_number = 1
    page_size = 1000

    while True:
        response = requests.get(
            URL,
            params={
                "pageNumber": page_number,
                "pageSize": page_size,
            },
            timeout=30,
        )
        response.raise_for_status()
        rows = response.json()

        if not rows:
            break

        all_rows.extend(rows)
        print(f"Page {page_number}: {len(rows)} records")

        if len(rows) < page_size:
            break

        page_number += 1

    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT.write_text(
        json.dumps(all_rows, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )

    print(f"Total records: {len(all_rows)}")
    print(f"Saved to: {OUTPUT}")


if __name__ == "__main__":
    main()
