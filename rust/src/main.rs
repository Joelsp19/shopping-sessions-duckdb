//! Shopping Sessions: a single-binary, multi-threaded implementation of rules.md.
//!
//! Phases (each parallel over all cores, no locks):
//!   1. mmap every input; split files into ~4 MB chunks on line boundaries.
//!   2. parse events: exact CSV dialect, validate each copy, compact it to a
//!      fixed-size record holding slices into the mapped files; scatter copies
//!      into 256 buckets by hash(event_id) (radix partitioning).
//!   3. dedup each bucket with a small hash table (earliest ingest_ts wins,
//!      before validation); survivors scatter into 256 buckets by hash(user_id).
//!   4. parse clicks (no dedup needed: copies are identical, so they cannot
//!      change a credit or an assist) and links into the same user buckets.
//!   5. intern user IDs per bucket -> dense uids; union-find over links;
//!      a person is named by its byte-wise smallest ID.
//!   6. scatter events and clicks by person into 256 buckets; per bucket sort
//!      (person << 32 | time) keys and sweep each person once: sessions,
//!      user_daily rows, credit (latest click within 7 days, ties -> smallest
//!      click_id) and assists (a sliding 7-day active-campaign set).
//!   7. write Parquet in parallel.

mod out;
mod parse;

use memmap2::Mmap;
use parquet::data_type::ByteArray;
use parse::{fields, for_each_line, hash_bytes, price, ts_ms, DAY_MS};
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::fs::File;
use std::hash::{BuildHasherDefault, Hash, Hasher};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

const NP: usize = 256;
const GAP: u32 = 1_800_000;
const WEEK: i64 = 604_800_000;
const CHUNK: usize = 4 << 20;

// ---------- hashing: keys carry a precomputed hash; the table uses it as-is ----------
#[derive(Default)]
struct IdH(u64);
impl Hasher for IdH {
    fn finish(&self) -> u64 {
        self.0
    }
    fn write(&mut self, b: &[u8]) {
        self.0 = hash_bytes(b);
    }
    fn write_u64(&mut self, v: u64) {
        self.0 = v;
    }
}
type Map<K, V> = HashMap<K, V, BuildHasherDefault<IdH>>;

#[derive(Clone, Copy)]
struct Key<'a> {
    h: u64,
    b: &'a [u8],
}
impl<'a> Key<'a> {
    fn new(b: &'a [u8]) -> Self {
        Key { h: hash_bytes(b), b }
    }
}
impl Hash for Key<'_> {
    fn hash<H: Hasher>(&self, s: &mut H) {
        s.write_u64(self.h);
    }
}
impl PartialEq for Key<'_> {
    fn eq(&self, o: &Self) -> bool {
        self.h == o.h && self.b == o.b
    }
}
impl Eq for Key<'_> {}

/// Bucket from bits the hash table does not rely on (it uses low bits + top 7).
#[inline]
fn part(h: u64) -> usize {
    ((h >> 32) as usize) & (NP - 1)
}
#[inline]
fn pid_part(pid: u32) -> usize {
    ((pid as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15) >> 56) as usize
}

// ---------- tiny work-stealing thread pool ----------
fn par<S: Send>(nt: usize, n: usize, init: impl Fn() -> S + Sync, f: impl Fn(&mut S, usize) + Sync) -> Vec<S> {
    let next = AtomicUsize::new(0);
    let nt = nt.max(1).min(n.max(1));
    std::thread::scope(|sc| {
        let hs: Vec<_> = (0..nt)
            .map(|_| {
                let (next, init, f) = (&next, &init, &f);
                sc.spawn(move || {
                    let mut s = init();
                    loop {
                        let i = next.fetch_add(1, Ordering::Relaxed);
                        if i >= n {
                            break;
                        }
                        f(&mut s, i);
                    }
                    s
                })
            })
            .collect();
        hs.into_iter().map(|h| h.join().unwrap()).collect()
    })
}

fn par_map<T: Send>(nt: usize, n: usize, f: impl Fn(usize) -> T + Sync) -> Vec<T> {
    let res = par(nt, n, Vec::new, |v: &mut Vec<(usize, T)>, i| v.push((i, f(i))));
    let mut all: Vec<(usize, T)> = res.into_iter().flatten().collect();
    all.sort_unstable_by_key(|x| x.0);
    all.into_iter().map(|x| x.1).collect()
}

fn buckets<T>() -> Vec<Vec<T>> {
    (0..NP).map(|_| Vec::new()).collect()
}

// ---------- files ----------
fn map_file(p: &Path) -> Option<Mmap> {
    let f = File::open(p).ok()?;
    if f.metadata().ok()?.len() == 0 {
        return None;
    }
    unsafe { Mmap::map(&f).ok() }
}

fn map_dir(dir: &Path) -> Vec<Mmap> {
    let mut paths: Vec<PathBuf> = match std::fs::read_dir(dir) {
        Ok(rd) => rd
            .filter_map(|e| e.ok().map(|e| e.path()))
            .filter(|p| p.is_file() && p.extension().map_or(false, |x| x == "csv"))
            .collect(),
        Err(_) => Vec::new(),
    };
    paths.sort();
    paths.iter().filter_map(|p| map_file(p)).collect()
}

/// Data part of a file (after its header line).
fn body(d: &[u8]) -> &[u8] {
    match memchr::memchr(b'\n', d) {
        Some(i) => &d[i + 1..],
        None => &d[d.len()..],
    }
}

fn chunks(maps: &[Mmap]) -> Vec<&[u8]> {
    let mut out = Vec::new();
    for m in maps {
        let d = body(&m[..]);
        let mut s = 0;
        while s < d.len() {
            let mut e = (s + CHUNK).min(d.len());
            if e < d.len() {
                e = match memchr::memchr(b'\n', &d[e..]) {
                    Some(i) => e + i + 1,
                    None => d.len(),
                };
            }
            out.push(&d[s..e]);
            s = e;
        }
    }
    out
}

fn json_str(s: &str, key: &str) -> String {
    let i = s.find(&format!("\"{key}\"")).expect("manifest key");
    let rest = &s[i + key.len() + 2..];
    let rest = &rest[rest.find(':').expect("manifest colon") + 1..];
    let a = rest.find('"').expect("manifest value") + 1;
    let b = rest[a..].find('"').expect("manifest value end");
    rest[a..a + b].to_string()
}

// ---------- records ----------
#[derive(Clone, Copy)]
struct ECopy<'a> {
    eid: &'a [u8],
    user: &'a [u8],
    idh: u64,
    ing: i64,
    t: u32,
    pay: u32, // price << 8 | category; 0 = view or cart
    valid: bool,
}

#[derive(Clone, Copy)]
struct Ev<'a> {
    user: &'a [u8],
    uh: u64,
    t: u32,
    pay: u32,
}

#[derive(Clone, Copy)]
struct Ck<'a> {
    user: &'a [u8],
    uh: u64,
    cid: &'a [u8],
    t: u32, // ms since window_start - 7 days
    camp: u32,
}

struct URow {
    pid: u32,
    day: i32,
    sessions: i64,
    events: i64,
    revenue: i64,
    top: u16, // NO_CAT = no purchases
}
const NO_CAT: u16 = u16::MAX;

struct Sweep {
    cs: Vec<i64>,
    cp: Vec<i64>,
    cr: Vec<i64>,
    ca: Vec<i64>,
    cnt: Vec<u32>,
    pos: Vec<u32>,
    active: Vec<u32>,
    rows: Vec<URow>,
}

fn main() {
    let t0 = Instant::now();
    let mut last = Instant::now();
    let mut lap = |name: &str| {
        eprintln!("{name:<10}{:7.3}s", last.elapsed().as_secs_f64());
        last = Instant::now();
    };
    let env = |k: &str, d: &str| std::env::var(k).unwrap_or_else(|_| d.to_string());
    let data = PathBuf::from(env("DATA_DIR", "/data"));
    let outd = PathBuf::from(env("OUTPUT_DIR", "/output"));
    let nt: usize = std::env::var("NCPU")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or_else(|| std::thread::available_parallelism().map(|n| n.get()).unwrap_or(1));

    let manifest = std::fs::read_to_string(data.join("manifest.json")).expect("manifest.json");
    let ws = ts_ms(json_str(&manifest, "window_start").as_bytes()).expect("window_start");
    let we = ts_ms(json_str(&manifest, "window_end").as_bytes()).expect("window_end");
    assert!(we > ws && we - ws + WEEK < u32::MAX as i64, "window too long for 32-bit offsets");
    let d0 = ws.div_euclid(DAY_MS);
    let nd = ((we - 1).div_euclid(DAY_MS) - d0 + 1) as usize;

    // ---- products -> category codes (byte-wise order; 'unknown' competes like any name)
    let prod_map = map_file(&data.join("products.csv"));
    let mut prod_rows: Vec<(&[u8], &[u8])> = Vec::new();
    if let Some(m) = &prod_map {
        for_each_line(body(&m[..]), |l| {
            if let Some([p, c]) = fields::<2>(l) {
                prod_rows.push((p, c));
            }
        });
    }
    let mut cat_names: Vec<&[u8]> = prod_rows.iter().map(|r| r.1).collect();
    cat_names.push(b"unknown");
    cat_names.sort_unstable();
    cat_names.dedup();
    assert!(cat_names.len() <= 256, "too many categories");
    let cat_code = |c: &[u8]| cat_names.binary_search(&c).unwrap() as u32;
    let unknown = cat_code(b"unknown");
    let mut prod: Map<Key, u32> = Map::default();
    for (p, c) in &prod_rows {
        prod.insert(Key::new(p), cat_code(c));
    }

    // ---- campaigns
    let camp_map = map_file(&data.join("campaigns.csv"));
    let mut camp_ids: Vec<&[u8]> = Vec::new();
    let mut camp_ch: Vec<&[u8]> = Vec::new();
    let mut camp: Map<Key, u32> = Map::default();
    if let Some(m) = &camp_map {
        for_each_line(body(&m[..]), |l| {
            if let Some([id, ch]) = fields::<2>(l) {
                if let Entry::Vacant(e) = camp.entry(Key::new(id)) {
                    e.insert(camp_ids.len() as u32);
                    camp_ids.push(id);
                    camp_ch.push(ch);
                }
            }
        });
    }
    let ncamp = camp_ids.len();
    lap("setup");

    // ---- events: parse + validate each copy, bucket by hash(event_id)
    let ev_maps = map_dir(&data.join("events"));
    let ev_chunks = chunks(&ev_maps);
    let copies: Vec<Vec<Vec<ECopy>>> = par(nt, ev_chunks.len(), buckets::<ECopy>, |bufs, ci| {
        for_each_line(ev_chunks[ci], |line| {
            let Some([eid, user, ts, ing, ty, pr_id, pr]) = fields::<7>(line) else { return };
            if eid.is_empty() {
                return;
            }
            let idh = hash_bytes(eid);
            let ing = ts_ms(ing).unwrap_or(i64::MAX);
            let mut valid = !user.is_empty() && !pr_id.is_empty();
            let mut t = 0u32;
            let mut pay = 0u32;
            if valid {
                match ts_ms(ts) {
                    Some(x) if x >= ws && x < we => t = (x - ws) as u32,
                    _ => valid = false,
                }
            }
            if valid {
                match ty {
                    b"view" | b"cart" => {}
                    b"purchase" => match price(pr) {
                        Some(p) => {
                            let c = prod.get(&Key::new(pr_id)).copied().unwrap_or(unknown);
                            pay = (p << 8) | c;
                        }
                        None => valid = false,
                    },
                    _ => valid = false,
                }
            }
            bufs[part(idh)].push(ECopy { eid, user, idh, ing, t, pay, valid });
        });
    });
    lap("ev-parse");

    // ---- dedup per bucket (earliest ingest wins, then validate + late check)
    let survivors: Vec<Vec<Vec<Ev>>> = par_map(nt, NP, |dp| {
        let total: usize = copies.iter().map(|w| w[dp].len()).sum();
        let mut map: Map<Key, u32> = Map::with_capacity_and_hasher(total, Default::default());
        let mut best: Vec<ECopy> = Vec::with_capacity(total);
        for w in &copies {
            for c in &w[dp] {
                match map.entry(Key { h: c.idh, b: c.eid }) {
                    Entry::Vacant(e) => {
                        e.insert(best.len() as u32);
                        best.push(*c);
                    }
                    Entry::Occupied(e) => {
                        let b = &mut best[*e.get() as usize];
                        if c.ing < b.ing {
                            *b = *c;
                        }
                    }
                }
            }
        }
        let mut outb = buckets::<Ev>();
        for c in best {
            if c.valid && c.ing - (ws + c.t as i64) <= DAY_MS {
                let uh = hash_bytes(c.user);
                outb[part(uh)].push(Ev { user: c.user, uh, t: c.t, pay: c.pay });
            }
        }
        outb
    });
    drop(copies);
    lap("ev-dedup");

    // ---- clicks: parse + validate, bucket by hash(user)
    let ck_maps = map_dir(&data.join("clicks"));
    let ck_chunks = chunks(&ck_maps);
    let cbase = ws - WEEK;
    let clicks: Vec<Vec<Vec<Ck>>> = par(nt, ck_chunks.len(), buckets::<Ck>, |bufs, ci| {
        for_each_line(ck_chunks[ci], |line| {
            let Some([cid, user, cmp, ts]) = fields::<4>(line) else { return };
            if cid.is_empty() || user.is_empty() {
                return;
            }
            let Some(&c) = camp.get(&Key::new(cmp)) else { return };
            match ts_ms(ts) {
                Some(x) if x >= cbase && x < we => {
                    let uh = hash_bytes(user);
                    bufs[part(uh)].push(Ck { user, uh, cid, t: (x - cbase) as u32, camp: c });
                }
                _ => {}
            }
        });
    });

    // ---- links: parse, each side bucketed by hash(id)
    let ln_maps = map_dir(&data.join("links"));
    let ln_chunks = chunks(&ln_maps);
    let link_lists: Vec<Vec<(&[u8], &[u8])>> = par_map(nt, ln_chunks.len(), |ci| {
        let mut v = Vec::new();
        for_each_line(ln_chunks[ci], |line| {
            if let Some([a, b]) = fields::<2>(line) {
                if !a.is_empty() && !b.is_empty() {
                    v.push((a, b));
                }
            }
        });
        v
    });
    let links: Vec<(&[u8], &[u8])> = link_lists.into_iter().flatten().collect();
    let mut lsides: Vec<Vec<(u32, Key)>> = buckets();
    for (i, (a, b)) in links.iter().enumerate() {
        for (s, x) in [(0u32, a), (1u32, b)] {
            let k = Key::new(x);
            lsides[part(k.h)].push(((i as u32) << 1 | s, k));
        }
    }
    lap("ck+links");

    // ---- intern user IDs per bucket
    struct Interned<'a> {
        names: Vec<&'a [u8]>,
        lsides: Vec<(u32, u32)>,
        evs: Vec<(u32, u32, u32)>,
        cks: Vec<(u32, u32, u32, &'a [u8])>,
    }
    let interned: Vec<Interned> = par_map(nt, NP, |p| {
        let mut map: Map<Key, u32> = Map::default();
        let mut names: Vec<&[u8]> = Vec::new();
        let ls: Vec<(u32, u32)> = lsides[p].iter().map(|(i, k)| (*i, intern(*k, &mut map, &mut names))).collect();
        let mut evs = Vec::with_capacity(survivors.iter().map(|s| s[p].len()).sum());
        for s in &survivors {
            for e in &s[p] {
                evs.push((intern(Key { h: e.uh, b: e.user }, &mut map, &mut names), e.t, e.pay));
            }
        }
        let mut cks = Vec::with_capacity(clicks.iter().map(|s| s[p].len()).sum());
        for s in &clicks {
            for c in &s[p] {
                cks.push((intern(Key { h: c.uh, b: c.user }, &mut map, &mut names), c.t, c.camp, c.cid));
            }
        }
        Interned { names, lsides: ls, evs, cks }
    });
    drop(survivors);
    drop(clicks);
    let mut base = vec![0u32; NP + 1];
    for p in 0..NP {
        base[p + 1] = base[p] + interned[p].names.len() as u32;
    }
    let nuid = base[NP] as usize;
    let mut names: Vec<&[u8]> = Vec::with_capacity(nuid);
    for it in &interned {
        names.extend_from_slice(&it.names);
    }
    lap("intern");

    // ---- people: union-find over links, name = byte-wise smallest ID
    let mut ends = vec![[0u32; 2]; links.len()];
    for (p, it) in interned.iter().enumerate() {
        for &(i, local) in &it.lsides {
            ends[(i >> 1) as usize][(i & 1) as usize] = base[p] + local;
        }
    }
    let mut parent: Vec<u32> = (0..nuid as u32).collect();
    fn find(parent: &mut [u32], mut x: u32) -> u32 {
        while parent[x as usize] != x {
            let g = parent[parent[x as usize] as usize];
            parent[x as usize] = g;
            x = g;
        }
        x
    }
    for [a, b] in &ends {
        let (ra, rb) = (find(&mut parent, *a), find(&mut parent, *b));
        if ra != rb {
            let (lo, hi) = if ra < rb { (ra, rb) } else { (rb, ra) };
            parent[hi as usize] = lo;
        }
    }
    let mut pid_of: Vec<u32> = vec![0; nuid];
    for u in 0..nuid as u32 {
        pid_of[u as usize] = find(&mut parent, u);
    }
    drop(parent);
    let mut name_of: Vec<u32> = (0..nuid as u32).collect(); // per root: uid holding the smallest ID
    for u in 0..nuid {
        let r = pid_of[u] as usize;
        if names[u] < names[name_of[r] as usize] {
            name_of[r] = u as u32;
        }
    }
    lap("people");

    // ---- scatter events and clicks by person
    type EvK = (u64, u32);
    type CkK<'a> = (u64, u32, &'a [u8]);
    let scattered: Vec<(Vec<Vec<EvK>>, Vec<Vec<CkK>>)> = par_map(nt, NP, |p| {
        let it = &interned[p];
        let b = base[p];
        let mut eb: Vec<Vec<EvK>> = buckets();
        for &(l, t, pay) in &it.evs {
            let pid = pid_of[(b + l) as usize];
            eb[pid_part(pid)].push(((pid as u64) << 32 | t as u64, pay));
        }
        let mut cb: Vec<Vec<CkK>> = buckets();
        for &(l, t, c, cid) in &it.cks {
            let pid = pid_of[(b + l) as usize];
            cb[pid_part(pid)].push(((pid as u64) << 32 | t as u64, c, cid));
        }
        (eb, cb)
    });
    drop(interned);
    lap("scatter");

    // ---- per person sweep: sessions, user_daily, credit, assists
    let w = (ncamp + 1) * nd;
    let sweeps: Vec<Sweep> = par(
        nt,
        NP,
        || Sweep {
            cs: vec![0; w],
            cp: vec![0; w],
            cr: vec![0; w],
            ca: vec![0; w],
            cnt: vec![0; ncamp],
            pos: vec![0; ncamp],
            active: Vec::new(),
            rows: Vec::new(),
        },
        |st, q| {
            let mut ev: Vec<EvK> = Vec::with_capacity(scattered.iter().map(|s| s.0[q].len()).sum());
            for s in &scattered {
                ev.extend_from_slice(&s.0[q]);
            }
            ev.sort_unstable_by_key(|x| x.0);
            let mut ck: Vec<CkK> = Vec::with_capacity(scattered.iter().map(|s| s.1[q].len()).sum());
            for s in &scattered {
                ck.extend_from_slice(&s.1[q]);
            }
            ck.sort_unstable_by_key(|x| x.0);

            let mut catrev: Vec<(u32, i64)> = Vec::new();
            let mut ci = 0usize;
            let mut i = 0usize;
            while i < ev.len() {
                let pid = (ev[i].0 >> 32) as u32;
                let mut j = i;
                while j < ev.len() && (ev[j].0 >> 32) as u32 == pid {
                    j += 1;
                }
                while ci < ck.len() && ((ck[ci].0 >> 32) as u32) < pid {
                    ci += 1;
                }
                let c0 = ci;
                while ci < ck.len() && (ck[ci].0 >> 32) as u32 == pid {
                    ci += 1;
                }
                let c1 = ci;
                let (mut lo, mut hi) = (c0, c0);

                let mut cur = URow { pid, day: i32::MIN, sessions: 0, events: 0, revenue: 0, top: NO_CAT };
                let mut k = i;
                while k < j {
                    let start = ev[k].0 as u32;
                    let day = (ws + start as i64).div_euclid(DAY_MS) as i32;
                    if day != cur.day {
                        if cur.sessions > 0 {
                            cur.top = top_cat(&catrev);
                            st.rows.push(std::mem::replace(&mut cur, URow { pid, day, sessions: 0, events: 0, revenue: 0, top: NO_CAT }));
                        } else {
                            cur.day = day;
                        }
                        catrev.clear();
                    }
                    let mut prev = start;
                    let mut n = 0i64;
                    let mut rev = 0i64;
                    while k < j {
                        let t = ev[k].0 as u32;
                        if t - prev > GAP {
                            break;
                        }
                        let pay = ev[k].1;
                        if pay >= 256 {
                            let p = (pay >> 8) as i64;
                            rev += p;
                            let c = pay & 0xFF;
                            match catrev.iter_mut().find(|x| x.0 == c) {
                                Some(x) => x.1 += p,
                                None => catrev.push((c, p)),
                            }
                        }
                        n += 1;
                        prev = t;
                        k += 1;
                    }
                    cur.sessions += 1;
                    cur.events += n;
                    cur.revenue += rev;

                    // clicks in [start - 7d, start], in the click time domain
                    let sc = start as u64 + WEEK as u64;
                    while hi < c1 && (ck[hi].0 as u32) as u64 <= sc {
                        let c = ck[hi].1 as usize;
                        st.cnt[c] += 1;
                        if st.cnt[c] == 1 {
                            st.pos[c] = st.active.len() as u32;
                            st.active.push(c as u32);
                        }
                        hi += 1;
                    }
                    while lo < hi && ((ck[lo].0 as u32) as u64) < start as u64 {
                        let c = ck[lo].1 as usize;
                        st.cnt[c] -= 1;
                        if st.cnt[c] == 0 {
                            let p = st.pos[c] as usize;
                            st.active.swap_remove(p);
                            if p < st.active.len() {
                                st.pos[st.active[p] as usize] = p as u32;
                            }
                        }
                        lo += 1;
                    }
                    let di = (day as i64 - d0) as usize;
                    let credit = if hi > lo {
                        let tl = ck[hi - 1].0 as u32;
                        let mut best = hi - 1;
                        let mut m = hi - 1;
                        while m > lo {
                            m -= 1;
                            if ck[m].0 as u32 != tl {
                                break;
                            }
                            if ck[m].2 < ck[best].2 {
                                best = m;
                            }
                        }
                        ck[best].1 as usize
                    } else {
                        ncamp
                    };
                    st.cs[credit * nd + di] += 1;
                    if rev > 0 {
                        st.cp[credit * nd + di] += 1;
                    }
                    st.cr[credit * nd + di] += rev;
                    for &a in &st.active {
                        st.ca[a as usize * nd + di] += 1;
                    }
                }
                if cur.sessions > 0 {
                    cur.top = top_cat(&catrev);
                    st.rows.push(cur);
                }
                catrev.clear();
                for &a in &st.active {
                    st.cnt[a as usize] = 0;
                }
                st.active.clear();
                i = j;
            }
        },
    );
    drop(scattered);
    lap("sweep");

    // ---- outputs
    let ud = outd.join("user_daily");
    let cd = outd.join("campaign_daily");
    std::fs::create_dir_all(&ud).expect("mkdir user_daily");
    std::fs::create_dir_all(&cd).expect("mkdir campaign_daily");
    let name_bytes = |pid: u32| ByteArray::from(names[name_of[pid as usize] as usize].to_vec());
    let parts: Vec<&Vec<URow>> = sweeps.iter().map(|s| &s.rows).collect();
    par_map(nt, parts.len(), |k| {
        let rows = parts[k];
        let mut top_v = Vec::new();
        let mut top_d = Vec::with_capacity(rows.len());
        for r in rows {
            if r.top == NO_CAT {
                top_d.push(0i16);
            } else {
                top_d.push(1i16);
                top_v.push(ByteArray::from(cat_names[r.top as usize].to_vec()));
            }
        }
        out::write(
            &ud.join(format!("part-{k}.parquet")),
            out::USER_DAILY,
            vec![
                out::Col::Str(rows.iter().map(|r| name_bytes(r.pid)).collect()),
                out::Col::I32(rows.iter().map(|r| r.day).collect()),
                out::Col::I64(rows.iter().map(|r| r.sessions).collect()),
                out::Col::I64(rows.iter().map(|r| r.events).collect()),
                out::Col::I64(rows.iter().map(|r| r.revenue).collect()),
                out::Col::OptStr(top_v, top_d),
            ],
        );
    });

    let sum = |f: fn(&Sweep) -> &Vec<i64>| -> Vec<i64> {
        let mut acc = vec![0i64; w];
        for s in &sweeps {
            for (a, b) in acc.iter_mut().zip(f(s)) {
                *a += b;
            }
        }
        acc
    };
    let (cs, cp, cr, ca) = (sum(|s| &s.cs), sum(|s| &s.cp), sum(|s| &s.cr), sum(|s| &s.ca));
    let (mut c_id, mut c_ch, mut c_day, mut c_s, mut c_p, mut c_r, mut c_a) =
        (Vec::new(), Vec::new(), Vec::new(), Vec::new(), Vec::new(), Vec::new(), Vec::new());
    for c in 0..=ncamp {
        for d in 0..nd {
            let x = c * nd + d;
            if cs[x] > 0 || ca[x] > 0 {
                let (id, ch): (&[u8], &[u8]) = if c == ncamp { (b"organic", b"organic") } else { (camp_ids[c], camp_ch[c]) };
                c_id.push(ByteArray::from(id.to_vec()));
                c_ch.push(ByteArray::from(ch.to_vec()));
                c_day.push((d0 + d as i64) as i32);
                c_s.push(cs[x]);
                c_p.push(cp[x]);
                c_r.push(cr[x]);
                c_a.push(ca[x]);
            }
        }
    }
    out::write(
        &cd.join("part-0.parquet"),
        out::CAMPAIGN_DAILY,
        vec![
            out::Col::Str(c_id),
            out::Col::Str(c_ch),
            out::Col::I32(c_day),
            out::Col::I64(c_s),
            out::Col::I64(c_p),
            out::Col::I64(c_r),
            out::Col::I64(c_a),
        ],
    );
    lap("write");
    eprintln!("{:<10}{:7.3}s  ({} threads)", "total", t0.elapsed().as_secs_f64(), nt);
}

#[inline]
fn intern<'a>(k: Key<'a>, map: &mut Map<Key<'a>, u32>, names: &mut Vec<&'a [u8]>) -> u32 {
    match map.entry(k) {
        Entry::Occupied(e) => *e.get(),
        Entry::Vacant(e) => {
            let v = names.len() as u32;
            names.push(k.b);
            e.insert(v);
            v
        }
    }
}

/// Category with the highest purchase revenue; a tie goes to the smallest code,
/// and codes are in byte-wise order of the name.
fn top_cat(v: &[(u32, i64)]) -> u16 {
    let mut best: Option<(u32, i64)> = None;
    for &(c, s) in v {
        best = match best {
            None => Some((c, s)),
            Some((bc, bs)) => {
                if s > bs || (s == bs && c < bc) { Some((c, s)) } else { Some((bc, bs)) }
            }
        };
    }
    best.map_or(NO_CAT, |b| b.0 as u16)
}
