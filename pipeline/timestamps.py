"""
Fast timestamp parsing (rules.md "Timestamps").

ts_fast(x) -> epoch ms, or NULL for a bad timestamp. It must agree with the
exact, slow ts_ms() macro of the reference implementation on every input that
could fall in a window (fuzz-tested on all dev timestamps plus ~2M mutations).

Approach (measured on 10.7M rows, 8 threads):
  - one RE2 regex validates the shape of all forms             ~0.4 s
  - DuckDB's native VARCHAR -> TIMESTAMP cast does the calendar   ~1 s
    (rejects Feb 30, month 13, second 60, Feb 29 outside leap years)
  - the cast ACCEPTS hour 24 (rolls to the next day), so hour <= 23 is
    checked explicitly; after the regex, every digit is ASCII, so two-char
    string comparison is a numeric comparison
try_strptime was ~5x slower than the cast, so it is not used.
"""

ISO_RE = "[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(Z|[+-][0-9]{2}:[0-9]{2})"

FAST_TS_MACROS = rf"""
CREATE OR REPLACE MACRO _iso_utc_ms(x) AS
    CASE WHEN x[12:13] <= '23' THEN epoch_ms(try_cast(x[1:19] AS TIMESTAMP)) END;
CREATE OR REPLACE MACRO ts_fast(x) AS CASE
    WHEN regexp_full_match(x, '[0-9]+') THEN
        CASE WHEN len(ltrim(x, '0')) > 18 THEN NULL
             ELSE coalesce(nullif(ltrim(x, '0'), '')::BIGINT, 0) END
    WHEN regexp_full_match(x, '{ISO_RE}') THEN
        CASE WHEN len(x) = 20 THEN _iso_utc_ms(x)
             WHEN x[21:22] <= '23' AND x[24:25] <= '59' THEN
                 _iso_utc_ms(x) - (CASE WHEN x[20] = '+' THEN 1 ELSE -1 END)
                                  * (x[21:22]::BIGINT * 60 + x[24:25]::BIGINT) * 60000
        END
END;
-- ingest_ts is always valid F1 (rules.md), so no validation is needed.
CREATE OR REPLACE MACRO f1_ms(x) AS epoch_ms(x[1:19]::TIMESTAMP);
"""
