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
    con.execute("DROP TABLE clicks; DROP TABLE pid_map")
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

    con.execute("DROP TABLE credit_clicks")

    # Assists: every campaign with a click in [start - 7d, start], once per session.
    # Each click covers session starts in [t, t + 7d]. Merging a (person,
    # campaign)'s overlapping intervals into disjoint islands means a session
    # matches at most one island per campaign: no DISTINCT needed, and whale
    # users' tens of thousands of clicks collapse to a few islands per campaign.
    # Click times are made DISTINCT first so the window order is deterministic
    # (with ties, the two windows could order rows differently and stretch an
    # island across a gap; the exactness test caught that).
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE assists_daily AS
        WITH b AS (
            SELECT pid, camp, t,
                   coalesce(t - lag(t) OVER (PARTITION BY pid, camp ORDER BY t) > {WEEK_MS}, true)::INT AS brk
            FROM (SELECT DISTINCT pid, camp, t FROM pclicks)
        ), g AS (
            SELECT pid, camp, t,
                   sum(brk) OVER (PARTITION BY pid, camp ORDER BY t ROWS UNBOUNDED PRECEDING) AS grp
            FROM b
        ), isl AS (
            SELECT pid, camp, min(t) AS lo, max(t) + {WEEK_MS} AS hi FROM g GROUP BY pid, camp, grp
        )
        SELECT i.camp, s.day, count(*) AS assisted_sessions
        FROM sessions s JOIN isl i ON i.pid = s.pid AND s.start BETWEEN i.lo AND i.hi
        GROUP BY ALL
    """)
    con.execute("DROP TABLE pclicks")

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
            SELECT camp, day, assisted_sessions FROM assists_daily
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
