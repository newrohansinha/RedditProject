# scripts/filter_file.py
import json, csv, random, zstandard, os
from collections import defaultdict
from datetime import datetime
from pathlib import Path

BASE   = Path(__file__).resolve().parent.parent
DATA   = BASE / "data"; OUTPUT = BASE / "output"

COMM_ZST = DATA / "AskFeminists_comments.zst"
ID_TXT   = DATA / "post_ids.txt"
SUB_CSV  = OUTPUT / "askfeminists_valid_200_per_month.csv"
OUT_CSV  = OUTPUT / "askfeminists_top_comments_exact3.csv"

random.seed(42)
SKIP = {"[deleted]", "[removed]"}
START = datetime(2019, 7, 1)
END   = datetime(2022, 7, 31, 23, 59, 59)

# ── load submission meta (year, month, post_text) ─────────────────────
meta = {}
with open(SUB_CSV, encoding="utf-8") as fh:
    for r in csv.DictReader(fh):
        dt = datetime.strptime(r["created"], "%Y-%m-%d %H:%M")
        meta[r["id"]] = {
            "year": dt.year,
            "month": dt.month,
            "text": f"{r['title']}  {r['selftext_or_url']}"
        }

IDS   = [ln.strip() for ln in open(ID_TXT) if ln.strip()]
T3SET = {f"t3_{pid}" for pid in IDS}

def zst_lines(path: Path, chunk=2**27):
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

buckets   = defaultdict(list)
remaining = set(IDS)          # stop early when empty

for raw in zst_lines(COMM_ZST):
    if not remaining:
        break

    try:
        c = json.loads(raw)
    except json.JSONDecodeError:
        continue

    body = c.get("body","").strip()
    if body.lower() in SKIP:
        continue

    link = c.get("link_id","")
    if link not in T3SET or c.get("parent_id") != link:
        continue  # not this post or not top‑level

    pid = link[3:]
    ts  = int(c["created_utc"])
    dt  = datetime.utcfromtimestamp(ts)

    if not (START <= dt <= END):
        continue                        # outside global window

    m = meta[pid]
    if dt.year != m["year"] or dt.month != m["month"]:
        continue                        # wrong month/year

    b = buckets[pid]
    if len(b) < 3:
        b.append(c)
        if len(b) == 3:
            remaining.discard(pid)
    else:
        j = random.randint(0, len(b))
        if j < 3:
            b[j] = c

# ── write rows sorted by comment timestamp ────────────────────────────
rows = []
for pid in IDS:
    bucket = buckets.get(pid, [])
    if len(bucket) != 3:
        continue
    for c in bucket:
        rows.append((int(c["created_utc"]), pid, c))

rows.sort(key=lambda t: t[0])   # ascending by timestamp

os.makedirs(OUTPUT, exist_ok=True)
with open(OUT_CSV, "w", encoding="utf-8-sig", newline="") as fh:
    wr = csv.writer(fh)
    wr.writerow(["post_id","created","author","score","link","body","post_text"])
    for ts, pid, c in rows:
        wr.writerow([
            pid,
            datetime.utcfromtimestamp(ts).strftime("%Y-%m-%d %H:%M"),
            f"u/{c['author']}",
            c.get("score", 0),
            f"https://www.reddit.com{c['permalink']}",
            c["body"].replace("\n", " "),
            meta[pid]["text"]
        ])

print(f"✅ wrote {len(rows)} comments (sorted) → {OUT_CSV}")
