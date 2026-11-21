"""
Clicks: rules.md "Clicks" steps 1-3.

Result: temp table clicks_clean(user_id VARCHAR, click_id VARCHAR, camp USMALLINT, t INTEGER)
    t      ms relative to window_start; negative for the 7 days before it
    camp   campaign code (campaigns.camp)
Also temp table campaigns(camp, campaign_id, channel).
"""
import os

from common import csv_rows_sql, rows_sql, with_fallback

WEEK_MS = 604_800_000


def clean_clicks(con, data_dir, ws_ms, we_ms):
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE campaigns AS
        SELECT (row_number() OVER (ORDER BY campaign_id) - 1)::USMALLINT AS camp, campaign_id, channel
        FROM ({csv_rows_sql(os.path.join(data_dir, "campaigns.csv"), ["campaign_id", "channel"])})
    """)

    # Steps 1-2: parse, drop empty click_id, dedup (copies are identical: keep any).
    # Step 3: validate.
    cols = ["click_id", "user_id", "campaign_id", "ts"]
    def stage(exact):
        con.execute(f"""
            CREATE OR REPLACE TEMP TABLE ck_copies AS
            SELECT click_id, user_id, campaign_id, ts
            FROM ({rows_sql(os.path.join(data_dir, "clicks", "*.csv"), cols, exact)})
            WHERE click_id IS NOT NULL
        """)
    with_fallback(stage)

    # Dedup in its own statement (see events.py: the planner misjudges the
    # group count when the aggregate sits on top of the CSV scan).
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE clicks_clean AS
        WITH d AS (
            SELECT click_id, any_value(user_id) AS user_id, any_value(campaign_id) AS campaign_id,
                   any_value(ts) AS ts
            FROM ck_copies GROUP BY click_id
        ), v AS (
            SELECT d.user_id, d.click_id, c.camp, ts_fast(d.ts) AS t_ms
            FROM d JOIN campaigns c ON c.campaign_id = d.campaign_id
            WHERE d.user_id IS NOT NULL
        )
        SELECT user_id, click_id, camp, (t_ms - {ws_ms})::INTEGER AS t
        FROM v
        WHERE t_ms >= {ws_ms - WEEK_MS} AND t_ms < {we_ms}
    """)
    con.execute("DROP TABLE ck_copies")
