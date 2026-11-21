"""
People: connected components over the links graph (rules.md "Linked IDs").

Uses scipy's C implementation instead of iterative SQL: chains can be hundreds
of links long, which would take hundreds of label-propagation rounds.

Produces temp table pid_map(uid UINTEGER, pid UINTEGER) for every uid, where
pid is the smallest uid in the person's component. uids are numbered in
byte-wise order of the ID, so pid is also the uid of the person's name.
"""
import numpy as np
from scipy.sparse import coo_matrix
from scipy.sparse.csgraph import connected_components


def build_people(con):
    n = con.execute("SELECT count(*) FROM user_ids").fetchone()[0]
    e = con.execute("SELECT a, b FROM links").fetchnumpy()
    a = np.asarray(e["a"], dtype=np.int64)
    b = np.asarray(e["b"], dtype=np.int64)
    g = coo_matrix((np.ones(len(a), dtype=np.int8), (a, b)), shape=(n, n)).tocsr()
    _, labels = connected_components(g, directed=False)
    # uids are 0..n-1 ascending, so the first uid seen with each label is its minimum.
    _, first = np.unique(labels, return_index=True)
    pid_map = {"uid": np.arange(n, dtype=np.uint32), "pid": first[labels].astype(np.uint32)}  # noqa: F841
    con.execute("CREATE OR REPLACE TEMP TABLE pid_map AS SELECT uid, pid FROM pid_map")
