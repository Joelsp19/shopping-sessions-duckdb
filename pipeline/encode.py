"""
Global user-ID dictionary and integer encoding.

user_ids(uid UINTEGER, user_id VARCHAR) covers every ID in links, surviving
events and valid clicks. uids follow byte-wise order of user_id, so a person's
smallest uid is their smallest ID: their output name.

Produces temp tables:
    user_ids  (uid, user_id)
    links     (a, b)                        distinct pairs, a < b, no self-links
    events    (uid, t, price, cat)
    clicks    (uid, click_id, camp, t)
"""


def encode(con):
    con.execute("""
        CREATE OR REPLACE TEMP TABLE user_ids AS
        SELECT (row_number() OVER (ORDER BY user_id) - 1)::UINTEGER AS uid, user_id
        FROM (SELECT a AS user_id FROM links_raw
              UNION SELECT b FROM links_raw
              UNION SELECT user_id FROM events_clean
              UNION SELECT user_id FROM clicks_clean)
    """)
    con.execute("""
        CREATE OR REPLACE TEMP TABLE links AS
        SELECT DISTINCT least(ia.uid, ib.uid) AS a, greatest(ia.uid, ib.uid) AS b
        FROM links_raw l
        JOIN user_ids ia ON ia.user_id = l.a
        JOIN user_ids ib ON ib.user_id = l.b
        WHERE ia.uid <> ib.uid
    """)
    con.execute("""
        CREATE OR REPLACE TEMP TABLE events AS
        SELECT u.uid, e.t, e.price, e.cat
        FROM events_clean e JOIN user_ids u USING (user_id)
    """)
    con.execute("""
        CREATE OR REPLACE TEMP TABLE clicks AS
        SELECT u.uid, c.click_id, c.camp, c.t
        FROM clicks_clean c JOIN user_ids u USING (user_id)
    """)
    # The string-keyed tables are no longer needed: free their memory now.
    con.execute("DROP TABLE links_raw; DROP TABLE events_clean; DROP TABLE clicks_clean; DROP TABLE products")
