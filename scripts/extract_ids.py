# scripts/extract_ids.py

import pandas as pd, os
from pathlib import Path

BASE   = Path(__file__).resolve().parent.parent
DATA   = BASE / "data"
OUTPUT = BASE / "output"

CSV  = OUTPUT / "askfeminists_valid_200_per_month.csv"
DEST = DATA / "post_ids.txt"

df = pd.read_csv(CSV, usecols=["id"])
os.makedirs(DATA, exist_ok=True)
df["id"].to_csv(DEST, index=False, header=False)
print(f"✅ wrote {len(df)} IDs → {DEST}")
