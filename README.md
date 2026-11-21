# Shopping Sessions (team Ardere)

An exact implementation of `rules.md` for the Builders Buildathon *Shopping Sessions* track: click-stream CSVs in `$DATA_DIR` become `user_daily/` and `campaign_daily/` Parquet tables in `$OUTPUT_DIR`.

The submitted image runs a single native binary (`rust/`). The earlier DuckDB implementation (`pipeline/`) stays in the repo as the reference it is tested against.

## Rust pipeline (`rust/src/`)

Every phase runs on all cores with no locks, and no strings are kept or compared on the hot paths:

- **IDs become exact 64-bit codes.** An ID of 1 to 10 characters from `[0-9A-Za-z]` (all generated IDs, including the upper-cased ones) packs into base-63 digits. Integer order is then exactly byte-wise order, and the output name decodes straight back from the code. Any other ID gets a hash code plus a registry entry; the registry checks that no two of them collide and compares their bytes.
- **CSV splitting is one SIMD pass.** Masks of `,`, `
` and `"` are built for each 64-byte block, and each line's quotes are counted with popcount rather than searched for field by field.

| Phase | What it does |
|---|---|
| people | On one thread, while the others parse events: links, then union-find, then a read-only table from ID code to person code (the smallest ID). Only linked IDs are in it; every other ID is its own person |
| parse | mmap every CSV, split it into 4 MB chunks and validate each event copy into a 32-byte record. The records are radix-partitioned into 256 buckets by `hash(event_id)` |
| dedup + clicks | One task pool. Each event bucket is deduplicated in a reused open-addressing table (earliest `ingest_ts` wins, before validation), then the late check runs. Click chunks are parsed alongside. Both map user to person and scatter by `hash(person)`. Clicks need no dedup: copies are identical, so they cannot change a credit or an assist |
| sweep | Per person bucket, sort by `(person, ts)` and pass over each person once. That pass yields sessions, `user_daily` rows, the credit (latest click within 7 days, ties going to the smallest `click_id`) and the assists (a sliding 7-day set of active campaigns) |
| write | Parquet through the low-level column writer, one file per thread |

## Testing

- `make fixtures`: the 36 edge-case fixtures, run in the image with scorer flags
- `make dev && make run-dev && make check-dev`: the 1M-event dev set against `dev-answer/`
- `cargo test --release` in `rust/`: unit tests for the timestamps, the CSV edge cases (the SIMD splitter against the plain one), and the order-preserving ID codes
