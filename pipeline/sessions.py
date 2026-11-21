"""
Sessions: rules.md "Events" steps 5-6.

Sessionizing works on DISTINCT (pid, t): events sharing a timestamp always share
a session, and with distinct timestamps the window ordering is deterministic.

Produces temp tables:
    ts_start  (pid, t, start)                  session start of every distinct timestamp
    sessions  (pid, start, day, n_events, revenue)
    purchases (pid, day, cat, price)           one row per surviving purchase
"""

GAP_MS = 1_800_000
DAY_MS = 86_400_000


def build_sessions(con, ws_ms):
    con.execute("""
        CREATE OR REPLACE TEMP TABLE ev AS
        SELECT m.pid, e.t::BIGINT AS t, e.price, e.cat FROM events e JOIN pid_map m USING (uid)
    """)
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE ts_start AS
        WITH g AS (
            SELECT pid, t, count(*) AS n, sum(price) AS rev
            FROM ev GROUP BY pid, t
        ), f AS (
            SELECT *, coalesce(t - lag(t) OVER (PARTITION BY pid ORDER BY t) > {GAP_MS}, true) AS is_new
            FROM g
        )
        SELECT pid, t, n, rev,
               max(CASE WHEN is_new THEN t END)
                   OVER (PARTITION BY pid ORDER BY t ROWS UNBOUNDED PRECEDING) AS start
        FROM f
    """)
    day = f"(DATE '1970-01-01' + (({ws_ms} + start) // {DAY_MS})::INTEGER)"
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE sessions AS
        SELECT pid, start, {day} AS day, sum(n)::BIGINT AS n_events, sum(rev)::BIGINT AS revenue
        FROM ts_start GROUP BY pid, start
    """)
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE purchases AS
        SELECT e.pid, {day} AS day, e.cat, e.price
        FROM ev e JOIN ts_start s USING (pid, t)
        WHERE e.price > 0
    """)
