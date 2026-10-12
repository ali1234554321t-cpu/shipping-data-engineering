import pandas as pd
from pathlib import Path

source = Path("data/raw/port_capacity.json")
output = Path("data/processed/port_capacity_clean.csv")

df = pd.read_json(source)

before = len(df)

df["date"] = pd.to_datetime(df["date"], errors="coerce")
df["value1"] = pd.to_numeric(df["value1"], errors="coerce")

df = df.dropna(subset=["date", "value1"]).copy()
df = df.drop_duplicates()

output.parent.mkdir(parents=True, exist_ok=True)
df.to_csv(output, index=False)

print(f"Rows before cleaning: {before}")
print(f"Rows after cleaning: {len(df)}")
print(f"Rows removed: {before - len(df)}")
print(f"Saved to: {output}")
