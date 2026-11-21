"""
Events: rules.md "Events, in this order" steps 1-4 and 7, then compaction.

Result: temp table events_clean, one row per surviving event, holding only what
later steps need:
    user_id  VARCHAR    -> replaced by a uid in encode.py
    t        UINTEGER   ms since window_start (a 14-day window is ~1.2e9 ms)
    price    UINTEGER   purchase price in cents; 0 = view or cart
    cat      UTINYINT   category code of a purchase (NULL for view/cart)

Dropped because nothing downstream needs them:
    event_id   only used for dedup
    ingest_ts  only used for dedup and the late check
    event_type view vs cart never matters; purchase <=> price > 0
    product_id only used to look up the category, and only for purchases

Speed: each copy is validated and shrunk BEFORE dedup, carrying a `valid`
flag instead of being filtered (rules: dedup comes first, an invalid earliest
copy still wins and drops the event). The dedup then hashes small rows.

Also creates temp table categories(cat, category), codes in byte-wise order of
the name, so on a revenue tie the smallest code is the smallest name.
"""
import os

from common import csv_rows_sql, rows_sql, with_fallback

COLS = ["event_id", "user_id", "ts", "ingest_ts", "event_type", "product_id", "price_cents"]
DAY_MS = 86_400_000


def clean_events(con, data_dir, ws_ms, we_ms):
    # products.csv and the category dictionary ('unknown' competes like any name).
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE products AS
        SELECT product_id, category FROM ({csv_rows_sql(os.path.join(data_dir, "products.csv"), ["product_id", "category"])})
    """)
    con.execute("""
        CREATE OR REPLACE TEMP TABLE categories AS
        SELECT (row_number() OVER (ORDER BY category) - 1)::UTINYINT AS cat, category
        FROM (SELECT category FROM products UNION SELECT 'unknown')
    """)
    assert con.execute("SELECT count(*) FROM categories").fetchone()[0] <= 256

    t_type = "UINTEGER" if we_ms - ws_ms < 2**32 else "BIGINT"

    def stage(exact):
        con.execute(f"""
            CREATE OR REPLACE TEMP TABLE ev_copies AS
            WITH p AS (                              -- step 1: parse
                SELECT * FROM ({rows_sql(os.path.join(data_dir, "events", "*.csv"), COLS, exact)})
                WHERE event_id IS NOT NULL
            ), c AS (                                -- step 3 checks, computed per copy
                SELECT hash(event_id) AS h, event_id, ingest_ts, user_id, t_ms,
                       CASE WHEN is_purchase THEN price ELSE 0 END AS price,
                       CASE WHEN is_purchase THEN product_id END AS product_id,
                       user_id IS NOT NULL AND product_id IS NOT NULL
                         AND event_type IN ('view', 'cart', 'purchase')
                         AND t_ms >= {ws_ms} AND t_ms < {we_ms}
                         AND (NOT is_purchase OR price BETWEEN 1 AND 10000000) AS valid
                FROM (
                    SELECT *, event_type = 'purchase' AS is_purchase, ts_fast(ts) AS t_ms,
                           CASE WHEN regexp_full_match(price_cents, '[0-9]+')
                                THEN try_cast(nullif(ltrim(price_cents, '0'), '') AS BIGINT) END AS price
                    FROM p
                )
            )
            SELECT * FROM c
        """)
    with_fallback(stage)

    # Step 2: dedup on event_id, earliest ingest_ts wins.
    # Only ~3% of ids repeat, so a cheap integer pass finds the 64-bit hashes
    # that occur more than once. A row whose hash is unique has a unique
    # event_id and skips the expensive text-keyed aggregate; only rows with a
    # repeated hash (real duplicates, plus any hash collision) go through it,
    # grouped by the exact event_id. A collision therefore costs a little time
    # and never merges two different events. 2.4x faster at 20M, same rows.
    # (ev_copies is materialized first on purpose: inside one streaming query
    # DuckDB misestimated the group count and ran far slower.)
    # ingest_ts is always fixed-width F1, so string order == time order.
    con.execute(f"""
            CREATE OR REPLACE TEMP TABLE events_clean AS
            WITH dup AS (
                SELECT h FROM ev_copies GROUP BY h HAVING count(*) > 1
            ), d AS (
                SELECT valid, user_id, t_ms, price, product_id, ingest_ts
                FROM ev_copies ANTI JOIN dup USING (h)
                UNION ALL
                SELECT unnest(arg_min({{'valid': valid, 'user_id': user_id, 't_ms': t_ms, 'price': price,
                                        'product_id': product_id, 'ingest_ts': ingest_ts}}, ingest_ts))
                FROM (SELECT * FROM ev_copies SEMI JOIN dup USING (h))
                GROUP BY event_id
            )
            SELECT d.user_id,                        -- step 4 (late) and step 7 (category)
                   (d.t_ms - {ws_ms})::{t_type} AS t,
                   d.price::UINTEGER AS price,
                   CASE WHEN d.price > 0 THEN k.cat END AS cat
            FROM d
            LEFT JOIN products pr ON pr.product_id = d.product_id
            LEFT JOIN categories k ON k.category = coalesce(pr.category, 'unknown')
            WHERE d.valid AND f1_ms(d.ingest_ts) - d.t_ms <= {DAY_MS}
        """)
    con.execute("DROP TABLE ev_copies")
