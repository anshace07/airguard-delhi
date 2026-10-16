# data/

Everything in here stays on your laptop and is **never committed** (too big, and it changes every hour). Only the empty folder structure is in Git.

| Folder | What goes here | Written by |
|---|---|---|
| `raw/` | OpenAQ archive files, weather downloads, `stations.csv` | Riya Maurya |
| `clean/` | Cleaned readings, Parquet | Ansh |
| `features/` | Feature tables for training and the newest row per station | Ansh |
| `live/` | Every live reading received + the streaming checkpoint | Ansh (stream job) |
| `predictions/` | `latest.csv` for the dashboard + prediction history | Ansh (stream job) |
| `samples/` | Fake files for building the dashboard early (name says SAMPLE) | Riya Katiyar |

Full details: CONTRACT.md section 4. To share data between laptops, use a shared drive link, not Git.
