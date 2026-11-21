"""
Shared CSV-dialect parsing (rules.md "CSV dialect"), as DuckDB SQL.

A line is split on '\n' only and its fields on ','. A field is either bare (no
'"') or wrapped whole in quotes; the quotes are syntax and are stripped, and an
empty field is NULL. Any other line is malformed and dropped.

Two readers produce identical rows:
  - FAST  native_rows_sql: DuckDB's parallel CSV reader with quoting turned off.
          Correct for every file without '\r' (verified on all fixtures and
          several generated datasets).
  - EXACT csv_rows_sql: read_text + string_split. Slow, used only for files
          containing '\r', which the generator never writes.
"""
import glob as globmod
import sys

import duckdb

from timestamps import FAST_TS_MACROS

CSV_MACROS = r"""
CREATE OR REPLACE MACRO field_ok(x) AS
    NOT contains(x, '"')
    OR (len(x) >= 2 AND starts_with(x, '"') AND ends_with(x, '"') AND NOT contains(x[2:-2], '"'));
CREATE OR REPLACE MACRO unq(x) AS
    nullif(CASE WHEN starts_with(x, '"') THEN x[2:-2] ELSE x END, '');
"""


def install_macros(con):
    con.execute(CSV_MACROS)
    con.execute(FAST_TS_MACROS)


def posix(path):
    return path.replace("\\", "/")


def _sql_files(files):
    """A glob string or a list of paths -> SQL literal for read_text/read_csv."""
    if isinstance(files, str):
        return f"'{posix(files)}'"
    return "[" + ", ".join(f"'{posix(f)}'" for f in files) + "]"


def csv_rows_sql(files, names):
    """EXACT path: one row per well-formed data line, columns `names` unquoted
    ('' -> NULL), header line of each file skipped."""
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
    """FAST path. Strict mode + ignore_errors drops lines with the wrong field
    count (strict_mode=false would silently truncate long lines). With
    quote='' the quote characters arrive literally and are checked/stripped
    exactly as in the exact path. A '\r' after a file's first line makes the
    reader raise, and with_fallback() reruns the stage on the exact path."""
    cols = "{" + ", ".join(f"'{n}': 'VARCHAR'" for n in names) + "}"
    ok = " AND ".join(f"({n} IS NULL OR field_ok({n}))" for n in names)
    return f"""
        SELECT {", ".join(f"unq({n}) AS {n}" for n in names)}
        FROM read_csv({_sql_files(files)}, columns={cols}, header=true, delim=',', quote='', escape='',
                      auto_detect=false, ignore_errors=true)
        WHERE {ok}
    """


def _head_has_cr(path, n=4096):
    # DuckDB picks a file's line-ending mode from its first lines: a file that
    # starts with '\r\n' would be read as CRLF without any error, so such files
    # go straight to the exact path.
    with open(path, "rb") as f:
        return b"\r" in f.read(n)


def rows_sql(pattern, names, exact=False):
    """SELECT over all files matching `pattern`, using the fast path where safe."""
    files = sorted(globmod.glob(pattern))
    if not files:
        return "SELECT " + ", ".join(f"NULL::VARCHAR AS {n}" for n in names) + " WHERE false"
    if exact:
        return csv_rows_sql(files, names)
    slow = {f for f in files if _head_has_cr(f)}
    fast = [f for f in files if f not in slow]
    parts = ([native_rows_sql(fast, names)] if fast else []) + ([csv_rows_sql(sorted(slow), names)] if slow else [])
    return " UNION ALL ".join(f"({p})" for p in parts)


def with_fallback(stage):
    """Run stage(False); if DuckDB's CSV reader rejects a file ('\r' after the
    first line, an empty file, ...), rerun the whole stage on the exact path."""
    try:
        stage(False)
    except duckdb.InvalidInputException as e:
        print(f"fast CSV path failed ({str(e).splitlines()[0][:80]}); using exact path", file=sys.stderr)
        stage(True)
