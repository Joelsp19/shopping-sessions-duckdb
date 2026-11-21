"""
Credit, assists and the two output tables (rules.md "Clicks" steps 4-7, "Outputs").
"""
import os

from common import posix

WEEK_MS = 604_800_000


def build_outputs(con, out_dir):
    # Valid clicks per person. Credit tie-break: on equal ts the smallest
    # click_id wins, so reduce to one click per (pid, t) first.
    con.execute("""
        CREATE OR REPLACE TEMP TABLE pclicks AS
        SELECT m.pid, c.t, c.camp, c.click_id FROM clicks c JOIN pid_map m USING (uid)
    """)
    con.execute("""
        CREATE OR REPLACE TEMP TABLE credit_clicks AS
        SELECT pid, t, arg_min(camp, click_id) AS camp FROM pclicks GROUP BY pid, t
    """)

    # Credit: the latest click at or before the session start, within 7 days.
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE credited AS
        SELECT s.pid, s.start, s.day, s.revenue,
               CASE WHEN c.t IS NOT NULL AND s.start - c.t <= {WEEK_MS} THEN c.camp END AS camp
        FROM sessions s ASOF LEFT JOIN credit_clicks c ON s.pid = c.pid AND s.start >= c.t
    """)

    # Assists: every campaign with a click in [start - 7d, start], once per session.
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE assists AS
        SELECT DISTINCT s.pid, s.start, s.day, c.camp
        FROM sessions s JOIN pclicks c
          ON c.pid = s.pid AND c.t BETWEEN s.start - {WEEK_MS} AND s.start
    """)

    user_daily = """
        WITH cat_rev AS (
            SELECT pid, day, cat, sum(price) AS s FROM purchases GROUP BY ALL
        ), top AS (
            SELECT pid, day, first(cat ORDER BY s DESC, cat ASC) AS cat FROM cat_rev GROUP BY ALL
        ), u AS (
            SELECT pid, day, count(*) AS sessions, sum(n_events) AS events, sum(revenue) AS revenue_cents
            FROM sessions GROUP BY ALL
        )
        SELECT n.user_id, u.day, u.sessions::BIGINT AS sessions, u.events::BIGINT AS events,
               u.revenue_cents::BIGINT AS revenue_cents, k.category AS top_category
        FROM u
        JOIN user_ids n ON n.uid = u.pid
        LEFT JOIN top t ON t.pid = u.pid AND t.day = u.day
        LEFT JOIN categories k ON k.cat = t.cat
    """
    campaign_daily = """
        WITH cr AS (
            SELECT camp, day, count(*) AS sessions, count(*) FILTER (WHERE revenue > 0) AS purchasing_sessions,
                   sum(revenue) AS revenue_cents
            FROM credited GROUP BY ALL
        ), asst AS (
            SELECT camp, day, count(*) AS assisted_sessions FROM assists GROUP BY ALL
        ), j AS (
            SELECT coalesce(cr.camp, asst.camp) AS camp, coalesce(cr.day, asst.day) AS day,
                   coalesce(cr.sessions, 0) AS sessions,
                   coalesce(cr.purchasing_sessions, 0) AS purchasing_sessions,
                   coalesce(cr.revenue_cents, 0) AS revenue_cents,
                   coalesce(asst.assisted_sessions, 0) AS assisted_sessions,
                   cr.camp IS NULL AND cr.day IS NOT NULL AS is_organic
            FROM cr FULL OUTER JOIN asst ON cr.camp = asst.camp AND cr.day = asst.day
        )
        SELECT CASE WHEN j.is_organic THEN 'organic' ELSE c.campaign_id END AS campaign_id,
               CASE WHEN j.is_organic THEN 'organic' ELSE c.channel END AS channel,
               j.day, j.sessions::BIGINT AS sessions, j.purchasing_sessions::BIGINT AS purchasing_sessions,
               j.revenue_cents::BIGINT AS revenue_cents, j.assisted_sessions::BIGINT AS assisted_sessions
        FROM j LEFT JOIN campaigns c ON c.camp = j.camp
    """
    for name, q in (("user_daily", user_daily), ("campaign_daily", campaign_daily)):
        d = os.path.join(out_dir, name)
        os.makedirs(d, exist_ok=True)
        con.execute(f"COPY ({q}) TO '{posix(os.path.join(d, 'part-0.parquet'))}' (FORMAT parquet)")
