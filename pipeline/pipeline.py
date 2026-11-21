#!/usr/bin/env python3
"""
Shopping Sessions: DuckDB pipeline.

Reads  $DATA_DIR    (default /data)
Writes $OUTPUT_DIR  (default /output): user_daily/ and campaign_daily/ as Parquet
Temp   $SCRATCH_DIR (default /scratch)
Per-stage timings go to stderr.
"""
import json
import os
import sys
import time
from datetime import datetime

import duckdb

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from clicks import clean_clicks  # noqa: E402
from common import install_macros, posix  # noqa: E402
from encode import encode  # noqa: E402
from events import clean_events  # noqa: E402
from links import parse_links  # noqa: E402
from outputs import build_outputs  # noqa: E402
from people import build_people  # noqa: E402
from sessions import build_sessions  # noqa: E402


def read_window(data):
    """manifest.json window -> (start, end) as epoch ms."""
    with open(os.path.join(data, "manifest.json")) as f:
        manifest = json.load(f)
    return tuple(
        int(datetime.fromisoformat(manifest[k].replace("Z", "+00:00")).timestamp() * 1000)
        for k in ("window_start", "window_end")
    )


def main(data, out, scratch, ncpu, mem_gb):
    t0 = last = time.perf_counter()

    def lap(stage):
        nonlocal last
        now = time.perf_counter()
        print(f"{stage:<10}{now - last:7.2f}s", file=sys.stderr)
        last = now

    con = duckdb.connect()
    con.execute(f"SET threads = {ncpu}")
    con.execute(f"SET memory_limit = '{max(1, int(mem_gb * 0.8))}GB'")
    con.execute(f"SET temp_directory = '{posix(os.path.join(scratch, 'duckdb_tmp'))}'")
    con.execute("SET preserve_insertion_order = false")
    con.execute("SET enable_progress_bar = false")
    install_macros(con)

    ws_ms, we_ms = read_window(data)

    parse_links(con, data); lap("links")
    clean_events(con, data, ws_ms, we_ms); lap("events")
    clean_clicks(con, data, ws_ms, we_ms); lap("clicks")
    encode(con); lap("encode")
    pid_of_uid = build_people(con); lap("people")
    build_sessions(con, ws_ms, pid_of_uid); lap("sessions")
    build_outputs(con, out); lap("outputs")
    print(f"{'total':<10}{time.perf_counter() - t0:7.2f}s", file=sys.stderr)


if __name__ == "__main__":
    main(
        data=os.environ.get("DATA_DIR", "/data"),
        out=os.environ.get("OUTPUT_DIR", "/output"),
        scratch=os.environ.get("SCRATCH_DIR", "/scratch"),
        ncpu=int(os.environ.get("NCPU", os.cpu_count() or 1)),
        mem_gb=int(os.environ.get("MEM_GB", "8")),
    )
