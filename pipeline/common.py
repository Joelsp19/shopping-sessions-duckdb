"""
Shared parsing helpers, written as DuckDB macros so all the work stays in SQL.

CSV dialect (rules.md): split lines on '\n' only, split fields on ',', a field
is either bare (no '"') or wrapped whole in quotes; the quotes are stripped and
an empty field is NULL. Any other line is malformed and dropped.

We read files with read_text() rather than DuckDB's CSV reader, whose quote and
malformed-line handling differs from rules.md.
"""

import glob as globmod
import sys

import duckdb

MACROS = r"""
-- CSV fields ------------------------------------------------------------------
CREATE OR REPLACE MACRO field_ok(x) AS
    NOT contains(x, '"')
    OR (len(x) >= 2 AND starts_with(x, '"') AND ends_with(x, '"') AND NOT contains(x[2:-2], '"'));
CREATE OR REPLACE MACRO unq(x) AS
    nullif(CASE WHEN starts_with(x, '"') THEN x[2:-2] ELSE x END, '');

-- Calendar arithmetic (pure integer maths, never raises) ----------------------
CREATE OR REPLACE MACRO is_leap(y) AS (y % 4 = 0 AND y % 100 <> 0) OR y % 400 = 0;
CREATE OR REPLACE MACRO days_in_month(y, m) AS
    CASE WHEN m = 2 THEN 28 + is_leap(y)::INT ELSE 30 + (m + m // 8) % 2 END;
-- Days since 1970-01-01 (Howard Hinnant's days_from_civil). y >= 0 here.
CREATE OR REPLACE MACRO _yy(y, m) AS y - (m <= 2)::BIGINT;
CREATE OR REPLACE MACRO _era(yy) AS (yy - CASE WHEN yy < 0 THEN 399 ELSE 0 END) // 400;
CREATE OR REPLACE MACRO _yoe(yy) AS yy - _era(yy) * 400;
CREATE OR REPLACE MACRO civil_days(y, m, d) AS
    _era(_yy(y, m)) * 146097
    + _yoe(_yy(y, m)) * 365 + _yoe(_yy(y, m)) // 4 - _yoe(_yy(y, m)) // 100
    + (153 * (m + CASE WHEN m > 2 THEN -3 ELSE 9 END) + 2) // 5 + d - 1
    - 719468;

-- Timestamps --------------------------------------------------------------------
-- ISO forms F1 'YYYY-MM-DDTHH:MM:SSZ' (len 20) and F2 '...+HH:MM' (len 25),
-- already shape-checked by regex. Returns epoch ms, or NULL for an impossible
-- calendar value or offset.
CREATE OR REPLACE MACRO _p(x, a, b) AS x[a:b]::BIGINT;
CREATE OR REPLACE MACRO iso_ms(x) AS CASE
    WHEN _p(x, 6, 7) BETWEEN 1 AND 12
     AND _p(x, 9, 10) BETWEEN 1 AND days_in_month(_p(x, 1, 4), _p(x, 6, 7))
     AND _p(x, 12, 13) <= 23 AND _p(x, 15, 16) <= 59 AND _p(x, 18, 19) <= 59
     AND (len(x) = 20 OR (_p(x, 21, 22) <= 23 AND _p(x, 24, 25) <= 59))
    THEN (civil_days(_p(x, 1, 4), _p(x, 6, 7), _p(x, 9, 10)) * 86400
          + _p(x, 12, 13) * 3600 + _p(x, 15, 16) * 60 + _p(x, 18, 19)) * 1000
         - CASE WHEN len(x) = 20 THEN 0
                ELSE (CASE WHEN x[20] = '+' THEN 1 ELSE -1 END)
                     * (_p(x, 21, 22) * 60 + _p(x, 24, 25)) * 60000 END
END;
-- Any ts field -> epoch ms, or NULL if it is a bad timestamp.
-- F3: ASCII digits only, leading zeros allowed. Anything >= 10^18 ms is far
-- outside every window, so it is NULL here (also avoids 64-bit overflow).
CREATE OR REPLACE MACRO ts_ms(x) AS CASE
    WHEN regexp_full_match(x, '[0-9]+') THEN
        CASE WHEN len(ltrim(x, '0')) > 18 THEN NULL
             ELSE coalesce(nullif(ltrim(x, '0'), '')::BIGINT, 0) END
    WHEN regexp_full_match(x, '[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(Z|[+-][0-9]{2}:[0-9]{2})') THEN
        iso_ms(x)
END;
"""


def install_macros(con):
    from timestamps import FAST_TS_MACROS
    con.execute(MACROS)
    con.execute(FAST_TS_MACROS)


def posix(path):
    return path.replace("\\", "/")


def _sql_files(files):
    """A glob string or a list of paths -> SQL literal for read_text/read_csv."""
    if isinstance(files, str):
        return f"'{posix(files)}'"
    return "[" + ", ".join(f"'{posix(f)}'" for f in files) + "]"


def csv_rows_sql(files, names):
    """EXACT path. SELECT producing one row per well-formed data line of every
    file, with columns `names` unquoted ('' -> NULL). Header lines skipped."""
    cols = ", ".join(f"unq(p[{i + 1}]) AS {n}" for i, n in enumerate(names))
    return f"""
        SELECT {cols}
        FROM (
            SELECT string_split(line, ',') AS p
            FROM (SELECT unnest(string_split(content, chr(10))[2:]) AS line
                  FROM read_text({_sql_files(files)}))
        )
        WHERE len(p) = {len(names)}
          AND list_aggregate(list_transform(p, f -> field_ok(f)), 'bool_and')
    """


def native_rows_sql(files, names):
    """FAST path: DuckDB's parallel CSV reader in strict mode with quoting off.
    Verified identical to csv_rows_sql on all fixtures and several seeds, for
    files without '\\r'. Wrong field counts are rejected (ignore_errors), quote
    chars arrive literally and are checked/stripped like the exact path.
    A '\\r' anywhere after the first line makes the reader raise, which
    rows_with_fallback() turns into a rerun on the exact path."""
    cols = "{" + ", ".join(f"'{n}': 'VARCHAR'" for n in names) + "}"
    ok = " AND ".join(f"({n} IS NULL OR field_ok({n}))" for n in names)
    return f"""
        SELECT {", ".join(f"unq({n}) AS {n}" for n in names)}
        FROM read_csv({_sql_files(files)}, columns={cols}, header=true, delim=',', quote='', escape='',
                      auto_detect=false, ignore_errors=true)
        WHERE {ok}
    """


def _head_has_cr(path, n=4096):
    # DuckDB picks the line-ending mode from a file's first lines: a file that
    # starts with '\r\n' would be read as CRLF without any error, so those
    # files must take the exact path.
    with open(path, "rb") as f:
        return b"\r" in f.read(n)


def rows_sql(pattern, names, exact=False):
    """SELECT over all files matching `pattern`, fast path where safe."""
    files = sorted(globmod.glob(pattern))
    if not files:
        return "SELECT " + ", ".join(f"NULL::VARCHAR AS {n}" for n in names) + " WHERE false"
    if exact:
        return csv_rows_sql(files, names)
    slow = [f for f in files if _head_has_cr(f)]
    fast = [f for f in files if f not in set(slow)]
    parts = ([native_rows_sql(fast, names)] if fast else []) + ([csv_rows_sql(slow, names)] if slow else [])
    return " UNION ALL ".join(f"({p})" for p in parts)


def with_fallback(stage):
    """Run stage(exact=False); if DuckDB's CSV reader rejects a file (a '\\r'
    after the first line, or anything else it cannot read), rerun the whole
    stage on the exact path."""
    try:
        stage(False)
    except duckdb.InvalidInputException as e:
        print(f"fast CSV path failed ({str(e).splitlines()[0][:80]}); using exact path", file=sys.stderr)
        stage(True)


def count_data_lines(con, glob):
    """Non-empty data lines (for stats only)."""
    return con.execute(f"""
        SELECT count(*) FROM (
            SELECT unnest(string_split(content, chr(10))[2:]) AS line FROM read_text('{posix(glob)}')
        ) WHERE line <> ''
    """).fetchone()[0]
