"""
Sessions: rules.md "Events" steps 5-6, as one sort plus linear numpy passes.

Each event becomes one 64-bit key, (person << 32) | t, with t in ms since the
window start (< 2^32). Sorting the keys puts every person's events in time
order. Between two adjacent keys of the same person the key difference is the
time gap; between different people it is at least 2^32 - window > 30 min.
So "a new session starts here" is simply diff(key) > 30 min, with no
per-person grouping. Equal timestamps give a difference of 0, so they always
share a session, whatever order they were read in (rule 5).

Revenue comes from purchases alone (~3% of events): each purchase finds its
session with a binary search over the session-start keys. This replaces two
DuckDB window passes, a 20M-row group-by and a 20M-row join (13 s -> 5 s at
20M events, identical output).

Produces temp tables:
    sessions  (pid, start, day, n_events, revenue)
    purchases (pid, day, cat, price)           one row per surviving purchase
"""
import numpy as np

GAP_MS = 1_800_000
DAY_MS = 86_400_000


def build_sessions(con, ws_ms, pid_of_uid):
    e = con.execute("SELECT uid, t FROM events").fetchnumpy()
    p = con.execute("SELECT uid, t, price, cat FROM events WHERE price > 0").fetchnumpy()
    con.execute("DROP TABLE events")

    def keys(uid, t):
        return (pid_of_uid[np.asarray(uid)].astype(np.uint64) << np.uint64(32)) | np.asarray(t).astype(np.uint64)

    k = keys(e["uid"], e["t"])
    del e
    k.sort()
    n = len(k)
    new = np.empty(n, dtype=bool)
    new[:1] = True
    np.greater(np.diff(k), GAP_MS, out=new[1:])
    starts = np.flatnonzero(new)
    del new
    sk = k[starts]                                      # one key per session: (pid, start)
    n_events = np.diff(np.append(starts, n)).astype(np.int64)
    del k, starts

    pk = keys(p["uid"], p["t"])
    price = np.asarray(p["price"]).astype(np.int64)
    si = np.searchsorted(sk, pk, side="right") - 1      # session index of each purchase
    revenue = np.zeros(len(sk), dtype=np.int64)
    np.add.at(revenue, si, price)

    s_pid = (sk >> np.uint64(32)).astype(np.uint32)
    s_start = (sk & np.uint64(0xFFFFFFFF)).astype(np.int64)
    con.register("sessions_np", {"pid": s_pid, "start": s_start, "n_events": n_events, "revenue": revenue})
    con.register("purchases_np", {"pid": s_pid[si], "start": s_start[si],
                                  "cat": np.asarray(p["cat"]).astype(np.uint8), "price": price})
    day = f"(DATE '1970-01-01' + (({ws_ms} + start) // {DAY_MS})::INTEGER)"
    con.execute(f"""CREATE OR REPLACE TEMP TABLE sessions AS
                    SELECT pid, start, {day} AS day, n_events, revenue FROM sessions_np""")
    con.execute(f"""CREATE OR REPLACE TEMP TABLE purchases AS
                    SELECT pid, {day} AS day, cat, price FROM purchases_np""")
    con.unregister("sessions_np")
    con.unregister("purchases_np")
