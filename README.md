# Shopping Sessions: DuckDB pipeline (team Ardere)

An exact implementation of `rules.md` for the Builders Buildathon *Shopping Sessions* track. It turns the click-stream CSVs in `$DATA_DIR` into `user_daily/` and `campaign_daily/` Parquet tables in `$OUTPUT_DIR`. All the heavy work runs in DuckDB SQL. Python only orchestrates, plus one scipy call for linked IDs.

## Pipeline (`pipeline/`)

| Stage | File | What it does |
|---|---|---|
| parse | `common.py` | CSV dialect: a fast native DuckDB reader with quoting off; files containing `\r` fall back to an exact `read_text` parser |
| timestamps | `timestamps.py` | All 3 `ts` forms: one regex + the native TIMESTAMP cast (hour 24 rejected explicitly) |
| links | `links.py` | Parse links, drop empty sides |
| events | `events.py` | Validate each copy → dedup on `event_id` (earliest `ingest_ts`) → drop late → compact to `(user, t, price, cat)` |
| clicks | `clicks.py` | Dedup on `click_id`, validate against the window and `campaigns.csv` |
| encode | `encode.py` | One user-ID dictionary, numbered in byte-wise order, so the smallest uid is the person's name |
| people | `people.py` | Connected components over links (`scipy.sparse.csgraph`) |
| sessions | `sessions.py` | Sessionize on distinct `(person, t)` with window functions (30 min gap) |
| outputs | `outputs.py` | ASOF-join credit (ties → smallest `click_id`), 7-day assists, both tables |

`pipeline.py` runs the stages and prints per-stage timings to stderr.

## Testing

- `make fixtures`: the 36 edge-case fixtures, run in the image with scorer flags
- `make dev && make run-dev && make check-dev`: the 1M-event dev set against `dev-answer/`
- Without Docker: `python3 scripts/run_fixtures.py fixtures -- python3 pipeline/pipeline.py`
