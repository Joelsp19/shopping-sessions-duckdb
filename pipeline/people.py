"""
People: connected components over the links graph (rules.md "Linked IDs").

A vectorized union-find in numpy: each round hooks the larger root of every
edge under the smaller one, then compresses paths fully. parent[x] <= x always
holds, so each component ends at its smallest uid. uids are numbered in
byte-wise order of the ID, so that root is also the uid of the person's name.
Chains hundreds of links long converge in a handful of rounds. (Same result as
scipy.sparse.csgraph, verified on dev and 20M, without scipy's ~0.7 s import.)

Produces temp table pid_map(uid UINTEGER, pid UINTEGER) for every uid, and
returns the same mapping as a numpy array indexed by uid.
"""
import numpy as np


def components_min(n, a, b):
    """Label each node 0..n-1 with the smallest node of its connected component."""
    parent = np.arange(n, dtype=np.int64)
    while True:
        ra, rb = parent[a], parent[b]
        lo, hi = np.minimum(ra, rb), np.maximum(ra, rb)
        m = lo != hi
        if not m.any():
            return parent
        np.minimum.at(parent, hi[m], lo[m])
        while True:
            pp = parent[parent]
            if np.array_equal(pp, parent):
                break
            parent = pp


def build_people(con):
    n = con.execute("SELECT count(*) FROM user_ids").fetchone()[0]
    e = con.execute("SELECT a, b FROM links").fetchnumpy()
    con.execute("DROP TABLE links")
    pid_of_uid = components_min(n, np.asarray(e["a"], np.int64), np.asarray(e["b"], np.int64)).astype(np.uint32)
    con.register("pid_map_np", {"uid": np.arange(n, dtype=np.uint32), "pid": pid_of_uid})
    con.execute("CREATE OR REPLACE TEMP TABLE pid_map AS SELECT uid, pid FROM pid_map_np")
    con.unregister("pid_map_np")
    return pid_of_uid
