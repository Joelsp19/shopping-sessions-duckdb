# Shopping Sessions (team Ardere)

An exact implementation of `rules.md` for the Builders Buildathon *Shopping Sessions* track: click-stream CSVs in `$DATA_DIR` become `user_daily/` and `campaign_daily/` Parquet tables in `$OUTPUT_DIR`.

The submitted image runs a single native binary (`rust/`). The earlier DuckDB implementation (`pipeline/`) stays in the repo as the reference it is tested against.

## Rust pipeline (`rust/src/`)

Every phase runs on all cores with no locks. Records stay as slices into memory-mapped files, so no strings are copied.

| Phase | What it does |
|---|---|
| parse | mmap every CSV, split it into 4 MB chunks on line boundaries, parse the exact dialect byte by byte (`parse.rs`). Each event copy is validated and compacted to a fixed-size record: hash, 32-bit time offset, price and category packed into 32 bits |
| dedup | copies are radix-partitioned into 256 buckets by `hash(event_id)`. Each bucket is deduplicated in its own small hash table (earliest `ingest_ts` wins, before validation), then the late check runs |
| clicks, links | parsed in parallel. Clicks need no dedup: copies are identical, so they cannot change a credit or an assist |
| intern | user IDs → dense integer ids, per bucket |
| people | union-find over links; a person is named by their byte-wise smallest ID |
| sweep | events and clicks are partitioned by person and sorted by `(person << 32) \| time` keys. One pass per person yields sessions, `user_daily` rows, the credit (latest click within 7 days, ties → smallest `click_id`) and the assists (a sliding 7-day set of active campaigns) |
| write | Parquet through the low-level column writer, one file per thread |

## Testing

- `make fixtures`: the 36 edge-case fixtures, run in the image with scorer flags
- `make dev && make run-dev && make check-dev`: the 1M-event dev set against `dev-answer/`
- `cargo test --release` in `rust/`: unit tests for the timestamp and CSV edge cases
