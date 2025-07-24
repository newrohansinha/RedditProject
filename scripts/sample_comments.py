# chooses up to 3 random comments for each post

import pandas as pd, os, numpy as np, re

COMMENTS_CSV = r"C:\Users\rohan\reddit_pipeline\output\askfeminists_comments_subset.csv"
OUT_CSV      = r"C:\Users\rohan\reddit_pipeline\output\comments_3_per_post.csv"

df = pd.read_csv(COMMENTS_CSV)

# assume the permalink (or link column) exists exactly as written by filter_file.py
# extract the post id (the part after /comments/)
df["post_id"] = df["link"].str.extract(r"/comments/([a-z0-9]+)/")

sampled = (
    df.groupby("post_id", group_keys=False)
      .apply(lambda x: x.sample(min(3, len(x)), random_state=42))
)

os.makedirs(os.path.dirname(OUT_CSV), exist_ok=True)
sampled.to_csv(OUT_CSV, index=False)
print("wrote", OUT_CSV)
