"""
Links: parse links/*.csv into temp table links_raw(a VARCHAR, b VARCHAR).

Drops malformed lines and lines with an empty side. Repeats and self-links are
kept here; encode.py removes them once the IDs are integers.
"""
import os

from common import rows_sql, with_fallback


def parse_links(con, data_dir):
    def stage(exact):
        con.execute(f"""
            CREATE OR REPLACE TEMP TABLE links_raw AS
            SELECT a, b FROM ({rows_sql(os.path.join(data_dir, "links", "*.csv"), ["a", "b"], exact)})
            WHERE a IS NOT NULL AND b IS NOT NULL
        """)
    with_fallback(stage)
