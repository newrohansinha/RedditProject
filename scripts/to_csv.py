# scripts/to_csv.py
#
# 1.  Scan AskFeminists_comments.zst once and count, for each post‑id,
#     how many good *top‑level* comments fall in each (year, month).
# 2.  Stream AskFeminists_submissions.zst and keep exactly 200 posts
#     per (year, month) *only if* the post has ≥3 such comments in
#     its own month.
#
# Output: output/askfeminists_valid_200_per_month.csv  (7 400 rows)

import zstandard, json, csv, os
from pathlib import Path
from datetime import datetime
from collections import defaultdict

BASE   = Path(__file__).resolve().parent.parent
DATA   = BASE / "data"
OUTPUT = BASE / "output"

SUB_ZST  = DATA / "AskFeminists_submissions.zst"
COMM_ZST = DATA / "AskFeminists_comments.zst"
OUT_CSV  = OUTPUT / "askfeminists_valid_200_per_month.csv"

START = datetime(2019, 7, 1)
END   = datetime(2022, 7, 31, 23, 59, 59)
SKIP  = {"[deleted]", "[removed]"}

def zst_lines(path: Path, chunk=2**26):
    with open(path, "rb") as fh:
        rdr = zstandard.ZstdDecompressor(max_window_size=2**31).stream_reader(fh)
        buf = ""
        while True:
            part = rdr.read(chunk)
            if not part:
                break
            buf += part.decode("utf-8", errors="ignore")
            *lines, buf = buf.split("\n")
            for ln in lines:
                yield ln
        rdr.close()

# ── Pass 1: count good top‑level comments per post‑id & month ───────────────
print("Pass 1: scanning comments …")
top_counts = defaultdict(int)        # key = (post_id, year, month)

for raw in zst_lines(COMM_ZST):
    try:
        c = json.loads(raw)
    except json.JSONDecodeError:
        continue

    if c.get("body", "").strip().lower() in SKIP:
        continue

    link_id  = c.get("link_id", "")
    parent_id = c.get("parent_id", "")
    if link_id != parent_id or not link_id.startswith("t3_"):
        continue                      # not a top‑level comment

    ts = int(c.get("created_utc", 0))
    dt = datetime.utcfromtimestamp(ts)
    if not (START <= dt <= END):
        continue

    pid = link_id[3:]
    top_counts[(pid, dt.year, dt.month)] += 1

print(f"✓ counted top‑level comments for {len({k[0] for k in top_counts}):,} posts")

# ── Pass 2: pick 200 posts/month that have ≥3 valid comments in‑month ───────
print("Pass 2: streaming submissions …")
FIELDS = [
    "id", "author", "created", "score",
    "num_comments", "title", "selftext_or_url"
]
kept    = 0
bucket  = defaultdict(int)   # (year, month) → kept so far (max 200)

os.makedirs(OUTPUT, exist_ok=True)
with open(OUT_CSV, "w", encoding="utf-8", newline="") as out_f:
    wr = csv.writer(out_f)
    wr.writerow(FIELDS)

    for raw in zst_lines(SUB_ZST):
        try:
            s = json.loads(raw)
        except json.JSONDecodeError:
            continue

        dt = datetime.utcfromtimestamp(int(s["created_utc"]))
        if not (START <= dt <= END):
            continue

        pid = s["id"]
        if top_counts.get((pid, dt.year, dt.month), 0) < 3:
            continue          # fewer than 3 good comments in this month

        ym = (dt.year, dt.month)
        if bucket[ym] >= 200:
            continue          # already have quota for this month

        bucket[ym] += 1
        kept += 1

        wr.writerow([
            pid,
            f"u/{s['author']}",
            dt.strftime("%Y-%m-%d %H:%M"),
            s.get("score", 0),
            s["num_comments"],
            s["title"],
            (s["selftext"] if s.get("is_self") else s.get("url", "")).replace("\n", " ")
        ])

print(f"✅ kept exactly {kept:,} submissions → {OUT_CSV}")
print("   (target is 7 400 = 37 months × 200)")
