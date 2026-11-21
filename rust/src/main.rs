//! Shopping Sessions: a single-binary, multi-threaded implementation of rules.md.
//!
//! Every ID becomes an exact 64-bit code (parse::pack_id): integer order is
//! byte-wise order, so no strings are kept or compared on the hot paths.
//!
//! Phases (each parallel over all cores, no locks):
//!   1. links: parse, union-find, name each person by its smallest ID ->
//!      a read-only table user code -> person code (linked users only).
//!   2. events: parse + validate each copy into a 32-byte record, scattered
//!      into 256 buckets by hash(event_id) (radix partitioning).
//!   3. dedup each bucket (earliest ingest_ts wins, before validation), late
//!      check, map user -> person; survivors scatter by hash(person).
//!   4. clicks: parse + validate, map user -> person, scatter by hash(person).
//!      No click dedup: copies are identical, so they cannot change a credit
//!      or an assist.
//!   5. per person bucket: sort by (person, ts) and sweep each person once:
//!      sessions, user_daily rows, credit (latest click within 7 days, ties ->
//!      smallest click_id) and assists (a sliding 7-day active-campaign set).
//!   6. write Parquet in parallel.

mod out;
mod parse;

use memmap2::Mmap;
use parquet::data_type::ByteArray;
use parse::{fields, for_each_line, for_each_record, hash_bytes, mix, pack_id, price, ts_ms, unpack_id, DAY_MS, FALLBACK};
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
const INVALID: u32 = u32::MAX;

#[inline]
fn bucket(h: u64) -> usize {
    (h >> 56) as usize
}

// ---------- small-table hashing for the cold string maps (products, campaigns) ----------
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

// ---------- open-addressing table keyed by non-zero u64 codes ----------
struct Table<V: Copy + Default> {
    e: Vec<(u64, V)>, // key 0 = empty slot
    mask: usize,
}
impl<V: Copy + Default> Table<V> {
    fn new(n: usize) -> Self {
        let cap = (n * 2).next_power_of_two().max(16);
        Table { e: vec![(0, V::default()); cap], mask: cap - 1 }
    }
    /// Slot holding k, or the empty slot where it belongs.
    #[inline]
    fn slot(&self, k: u64) -> (usize, bool) {
        probe(&self.e, self.mask, k)
    }
    #[inline]
    fn get(&self, k: u64) -> Option<V> {
        let (i, f) = self.slot(k);
        if f { Some(self.e[i].1) } else { None }
    }
    #[inline]
    fn set(&mut self, i: usize, k: u64, v: V) {
        self.e[i] = (k, v);
    }
}

/// Hint the cache to load the slot where k's probe starts.
#[inline(always)]
fn prefetch<V>(e: &[(u64, V)], mask: usize, k: u64) {
    #[cfg(target_arch = "x86_64")]
    unsafe {
        use std::arch::x86_64::{_mm_prefetch, _MM_HINT_T0};
        _mm_prefetch::<_MM_HINT_T0>(e.as_ptr().add(mix(k) as usize & mask) as *const i8);
    }
}

/// Linear probing over (key, value) slots; key 0 = empty. mask = len - 1.
#[inline]
fn probe<V>(e: &[(u64, V)], mask: usize, k: u64) -> (usize, bool) {
    let mut i = mix(k) as usize & mask;
    loop {
        let x = unsafe { e.get_unchecked(i).0 };
        if x == k {
            return (i, true);
        }
        if x == 0 {
            return (i, false);
        }
        i = (i + 1) & mask;
    }
}

// ---------- ID codes; the registry resolves the rare fallback codes ----------
#[derive(Default)]
struct Coder<'a> {
    fb: HashMap<u64, &'a [u8]>,
}
impl<'a> Coder<'a> {
    #[inline]
    fn code(&mut self, s: &'a [u8]) -> u64 {
        match pack_id(s) {
            Some(v) => v,
            None => self.slow(s),
        }
    }
    #[cold]
    fn slow(&mut self, s: &'a [u8]) -> u64 {
        let h = FALLBACK | (hash_bytes(s) >> 1);
        match self.fb.entry(h) {
            Entry::Vacant(e) => {
                e.insert(s);
            }
            Entry::Occupied(e) => assert!(*e.get() == s, "ID hash collision"),
        }
        h
    }
}
type Reg<'a> = HashMap<u64, &'a [u8]>;
fn merge<'a>(reg: &mut Reg<'a>, c: Coder<'a>) {
    for (h, s) in c.fb {
        match reg.entry(h) {
            Entry::Vacant(e) => {
                e.insert(s);
            }
            Entry::Occupied(e) => assert!(*e.get() == s, "ID hash collision"),
        }
    }
}
fn id_vec(v: u64, reg: &Reg) -> Vec<u8> {
    if v < FALLBACK {
        let mut b = [0u8; 10];
        let n = unpack_id(v, &mut b);
        b[..n].to_vec()
    } else {
        reg[&v].to_vec()
    }
}
/// Byte-wise a < b on ID codes.
#[inline]
fn id_lt(a: u64, b: u64, reg: &Reg) -> bool {
    if (a | b) < FALLBACK { a < b } else { id_vec(a, reg) < id_vec(b, reg) }
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

fn reserved<T>(n: usize) -> Vec<Vec<T>> {
    (0..NP).map(|_| Vec::with_capacity(n)).collect()
}

// ---------- files ----------
fn map_file(p: &Path) -> Option<Mmap> {
    let f = File::open(p).ok()?;
    if f.metadata().ok()?.len() == 0 {
        return None;
    }
    // populate: map every page up front in one call instead of one fault per page
    let m = unsafe { memmap2::MmapOptions::new().populate().map(&f).ok()? };
    #[cfg(unix)]
    let _ = m.advise(memmap2::Advice::Sequential);
    Some(m)
}

fn map_dir(dir: &Path, nt: usize) -> Vec<Mmap> {
    let mut paths: Vec<PathBuf> = match std::fs::read_dir(dir) {
        Ok(rd) => rd
            .filter_map(|e| e.ok().map(|e| e.path()))
            .filter(|p| p.is_file() && p.extension().map_or(false, |x| x == "csv"))
            .collect(),
        Err(_) => Vec::new(),
    };
    paths.sort();
    let got = par(nt, paths.len(), Vec::new, |v: &mut Vec<(usize, Mmap)>, i| {
        if let Some(b) = map_file(&paths[i]) {
            v.push((i, b));
        }
    });
    let mut all: Vec<(usize, Mmap)> = got.into_iter().flatten().collect();
    all.sort_unstable_by_key(|x| x.0);
    all.into_iter().map(|x| x.1).collect()
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
/// One parsed copy of an event. t == INVALID marks a copy that fails validation.
#[derive(Clone, Copy)]
struct ECopy {
    eid: u64,
    user: u64,
    ing: i64,
    t: u32,   // ms since window_start
    pay: u32, // price << 8 | category; 0 = view or cart
}

#[derive(Clone, Copy)]
struct EvRec {
    p: u64,
    t: u32,
    pay: u32,
}

#[derive(Clone, Copy)]
struct CkRec {
    p: u64,
    cid: u64,
    t: u32, // ms since window_start - 7 days
    camp: u32,
}

struct URow {
    p: u64,
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
    ev: Vec<EvRec>,
    ck: Vec<CkRec>,
    bins: Vec<u16>,
}

fn main() {
    let t0 = Instant::now();
    let mut last = Instant::now();
    let mut lap = |name: &str| {
        eprintln!("{name:<10}{:7.3}s  {}", last.elapsed().as_secs_f64(), proc_stat());
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
    assert!(we > ws && we - ws + WEEK < INVALID as i64, "window too long for 32-bit offsets");
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
    let ev_maps = map_dir(&data.join("events"), nt);
    let ck_maps = map_dir(&data.join("clicks"), nt);
    let ln_maps = map_dir(&data.join("links"), nt);
    lap("setup");

    // Partition buffers are reserved up front from the input sizes (with
    // slack); a reservation is only address space until it is written.
    let ev_bytes: usize = ev_maps.iter().map(|m| m.len()).sum();
    let ck_bytes: usize = ck_maps.iter().map(|m| m.len()).sum();
    let per = |bytes: usize, line: usize| bytes / line / nt.max(1) / NP * 6 / 5 + 64;

    // ---- people (one thread: links -> union-find -> person codes) while
    // ---- every core parses events: validate each copy, bucket by hash(event_id)
    let ln_chunks = chunks(&ln_maps);
    let ev_chunks = chunks(&ev_maps);
    let ev_per = per(ev_bytes, 76);
    let ((person, link_reg), copies) = std::thread::scope(|sc| {
        let h = sc.spawn(|| {
            let t = Instant::now();
            let r = people(&ln_chunks);
            if std::env::var_os("SS_VERBOSE").is_some() {
                eprintln!("  (people {:.3}s)", t.elapsed().as_secs_f64());
            }
            r
        });
        let copies = par(nt, ev_chunks.len(), || (reserved(ev_per), Coder::default()), |(bufs, cd), ci| {
            for_each_record::<7>(ev_chunks[ci], |[eid, user, ts, ing, ty, pr_id, pr]| {
                if eid.is_empty() {
                    return;
                }
                let e = cd.code(eid);
                let ing = ts_ms(ing).unwrap_or(i64::MAX);
                let mut c = ECopy { eid: e, user: 0, ing, t: INVALID, pay: 0 };
                if !user.is_empty() && !pr_id.is_empty() {
                    if let Some(x) = ts_ms(ts) {
                        if x >= ws && x < we {
                            let ok = match kind(ty) {
                                1 => true,
                                2 => match price(pr) {
                                    Some(p) => {
                                        let cat = prod.get(&Key::new(pr_id)).copied().unwrap_or(unknown);
                                        c.pay = (p << 8) | cat;
                                        true
                                    }
                                    None => false,
                                },
                                _ => false,
                            };
                            if ok {
                                c.t = (x - ws) as u32;
                                c.user = cd.code(user);
                            }
                        }
                    }
                }
                bufs[bucket(mix(e))].push(c);
            });
        });
        (h.join().unwrap(), copies)
    });
    let mut reg: Reg = link_reg;
    let mut ev_parts = Vec::with_capacity(copies.len());
    for (b, cd) in copies {
        merge(&mut reg, cd);
        ev_parts.push(b);
    }
    let person_of = |u: u64| person.get(u).unwrap_or(u);
    lap("parse+ppl");

    // ---- one task pool: dedup each event bucket (earliest ingest wins, then
    // ---- validate + late check), and parse clicks. Both map users to people
    // ---- and scatter by hash(person). Tables and buffers are reused per thread.
    let ck_chunks = chunks(&ck_maps);
    let cbase = ws - WEEK;
    let (evo_per, cko_per) = (per(ev_bytes, 76), per(ck_bytes, 44));
    struct Work<'a> {
        tab: Vec<(u64, u32)>,
        best: Vec<ECopy>,
        ev: Vec<Vec<EvRec>>,
        ck: Vec<Vec<CkRec>>,
        pend: Vec<CkRec>,
        cd: Coder<'a>,
    }
    let work = par(
        nt,
        NP + ck_chunks.len(),
        || Work { tab: Vec::new(), best: Vec::new(), ev: reserved(evo_per), ck: reserved(cko_per), pend: Vec::with_capacity(32), cd: Coder::default() },
        |st, task| {
            if task < NP {
                let dp = task;
                let total: usize = ev_parts.iter().map(|w| w[dp].len()).sum();
                let cap = (total * 2).next_power_of_two().max(16);
                if st.tab.len() < cap {
                    st.tab.resize(cap, (0, 0));
                }
                let tab = &mut st.tab[..cap];
                let mask = cap - 1;
                let best = &mut st.best;
                best.clear();
                const AHEAD: usize = 16;
                for w in &ev_parts {
                    let src = &w[dp];
                    for (k, c) in src.iter().enumerate() {
                        if let Some(n) = src.get(k + AHEAD) {
                            prefetch(tab, mask, n.eid);
                        }
                        let (i, f) = probe(tab, mask, c.eid);
                        if f {
                            let b = &mut best[tab[i].1 as usize];
                            if c.ing < b.ing {
                                *b = *c;
                            }
                        } else {
                            tab[i] = (c.eid, best.len() as u32);
                            best.push(*c);
                        }
                    }
                }
                for x in tab.iter_mut() {
                    x.0 = 0;
                }
                for (k, c) in best.iter().enumerate() {
                    if let Some(n) = best.get(k + AHEAD) {
                        prefetch(&person.e, person.mask, n.user);
                    }
                    if c.t != INVALID && c.ing - (ws + c.t as i64) <= DAY_MS {
                        let p = person_of(c.user);
                        st.ev[bucket(mix(p))].push(EvRec { p, t: c.t, pay: c.pay });
                    }
                }
            } else {
                // valid clicks are batched with p = user code, so the person
                // lookups can be prefetched before they are resolved
                let (bufs, cd, pend) = (&mut st.ck, &mut st.cd, &mut st.pend);
                let flush = |pend: &mut Vec<CkRec>, bufs: &mut Vec<Vec<CkRec>>| {
                    for r in pend.iter() {
                        prefetch(&person.e, person.mask, r.p);
                    }
                    for r in pend.iter() {
                        let p = person_of(r.p);
                        bufs[bucket(mix(p))].push(CkRec { p, ..*r });
                    }
                    pend.clear();
                };
                for_each_record::<4>(ck_chunks[task - NP], |[cid, user, cmp, ts]| {
                    if cid.is_empty() || user.is_empty() {
                        return;
                    }
                    let Some(&c) = camp.get(&Key::new(cmp)) else { return };
                    match ts_ms(ts) {
                        Some(x) if x >= cbase && x < we => {
                            pend.push(CkRec { p: cd.code(user), cid: cd.code(cid), t: (x - cbase) as u32, camp: c });
                            if pend.len() == 32 {
                                flush(pend, bufs);
                            }
                        }
                        _ => {}
                    }
                });
                flush(pend, bufs);
            }
        },
    );
    drop(ev_parts);
    let mut evs = Vec::with_capacity(work.len());
    let mut cks = Vec::with_capacity(work.len());
    for wk in work {
        merge(&mut reg, wk.cd);
        evs.push(wk.ev);
        cks.push(wk.ck);
    }
    lap("dedup+ck");

    // ---- per person sweep: sessions, user_daily, credit, assists
    let w = (ncamp + 1) * nd;
    let reg = &reg;
    // Biggest buckets first: a bucket holding a very active person can take
    // several times the average, and must not be the last one picked up.
    let mut order: Vec<usize> = (0..NP).collect();
    let size = |q: usize| -> usize { evs.iter().map(|s| s[q].len()).sum::<usize>() + cks.iter().map(|s| s[q].len()).sum::<usize>() };
    order.sort_by_cached_key(|&q| std::cmp::Reverse(size(q)));
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
            ev: Vec::new(),
            ck: Vec::new(),
            bins: Vec::new(),
        },
        |st, task| {
            let q = order[task];
            let mut ev = std::mem::take(&mut st.ev);
            let mut ck = std::mem::take(&mut st.ck);
            ev.clear();
            ck.clear();
            let ev_src: Vec<&[EvRec]> = evs.iter().map(|s| &s[q][..]).collect();
            let ck_src: Vec<&[CkRec]> = cks.iter().map(|s| &s[q][..]).collect();
            let bits = bin_bits(ev_src.iter().map(|s| s.len()).sum());
            group_sort(&ev_src, &mut ev, &mut st.bins, bits, |x| x.p, |x| x.t);
            group_sort(&ck_src, &mut ck, &mut st.bins, bits, |x| x.p, |x| x.t);

            let mut catrev: Vec<(u32, i64)> = Vec::new();
            let mut ci = 0usize;
            let mut i = 0usize;
            while i < ev.len() {
                let p = ev[i].p;
                let mut j = i;
                while j < ev.len() && ev[j].p == p {
                    j += 1;
                }
                let op = (sub_bin(p, bits), p);
                while ci < ck.len() && (sub_bin(ck[ci].p, bits), ck[ci].p) < op {
                    ci += 1;
                }
                let c0 = ci;
                while ci < ck.len() && ck[ci].p == p {
                    ci += 1;
                }
                let c1 = ci;
                let (mut lo, mut hi) = (c0, c0);

                let mut cur = URow { p, day: i32::MIN, sessions: 0, events: 0, revenue: 0, top: NO_CAT };
                let mut k = i;
                while k < j {
                    let start = ev[k].t;
                    let day = (ws + start as i64).div_euclid(DAY_MS) as i32;
                    if day != cur.day {
                        if cur.sessions > 0 {
                            cur.top = top_cat(&catrev);
                            st.rows.push(std::mem::replace(&mut cur, URow { p, day, sessions: 0, events: 0, revenue: 0, top: NO_CAT }));
                        } else {
                            cur.day = day;
                        }
                        catrev.clear();
                    }
                    let mut prev = start;
                    let mut n = 0i64;
                    let mut rev = 0i64;
                    while k < j {
                        let t = ev[k].t;
                        if t - prev > GAP {
                            break;
                        }
                        let pay = ev[k].pay;
                        if pay >= 256 {
                            let pr = (pay >> 8) as i64;
                            rev += pr;
                            let c = pay & 0xFF;
                            match catrev.iter_mut().find(|x| x.0 == c) {
                                Some(x) => x.1 += pr,
                                None => catrev.push((c, pr)),
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
                    while hi < c1 && ck[hi].t as u64 <= sc {
                        let c = ck[hi].camp as usize;
                        st.cnt[c] += 1;
                        if st.cnt[c] == 1 {
                            st.pos[c] = st.active.len() as u32;
                            st.active.push(c as u32);
                        }
                        hi += 1;
                    }
                    while lo < hi && (ck[lo].t as u64) < start as u64 {
                        let c = ck[lo].camp as usize;
                        st.cnt[c] -= 1;
                        if st.cnt[c] == 0 {
                            let ps = st.pos[c] as usize;
                            st.active.swap_remove(ps);
                            if ps < st.active.len() {
                                st.pos[st.active[ps] as usize] = ps as u32;
                            }
                        }
                        lo += 1;
                    }
                    let di = (day as i64 - d0) as usize;
                    let credit = if hi > lo {
                        let tl = ck[hi - 1].t;
                        let mut bi = hi - 1;
                        let mut m = hi - 1;
                        while m > lo {
                            m -= 1;
                            if ck[m].t != tl {
                                break;
                            }
                            if id_lt(ck[m].cid, ck[bi].cid, reg) {
                                bi = m;
                            }
                        }
                        ck[bi].camp as usize
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
            st.ev = ev;
            st.ck = ck;
        },
    );
    drop(evs);
    drop(cks);
    lap("sweep");

    // ---- outputs
    let ud = outd.join("user_daily");
    let cd = outd.join("campaign_daily");
    std::fs::create_dir_all(&ud).expect("mkdir user_daily");
    std::fs::create_dir_all(&cd).expect("mkdir campaign_daily");
    let parts: Vec<&Vec<URow>> = sweeps.iter().map(|s| &s.rows).collect();
    let cat_bytes: Vec<ByteArray> = cat_names.iter().map(|c| ByteArray::from(c.to_vec())).collect();
    par(nt, parts.len(), || (), |_, k| {
        let rows = parts[k];
        let mut top_v = Vec::new();
        let mut top_d = Vec::with_capacity(rows.len());
        for r in rows {
            if r.top == NO_CAT {
                top_d.push(0i16);
            } else {
                top_d.push(1i16);
                top_v.push(cat_bytes[r.top as usize].clone());
            }
        }
        // every user ID in one buffer; the column holds zero-copy slices of it
        let mut buf: Vec<u8> = Vec::with_capacity(rows.len() * 10);
        let mut ends: Vec<u32> = Vec::with_capacity(rows.len());
        for r in rows {
            if r.p < FALLBACK {
                let mut b = [0u8; 10];
                let n = unpack_id(r.p, &mut b);
                buf.extend_from_slice(&b[..n]);
            } else {
                buf.extend_from_slice(reg[&r.p]);
            }
            ends.push(buf.len() as u32);
        }
        let buf = bytes::Bytes::from(buf);
        let mut ids = Vec::with_capacity(rows.len());
        let mut a = 0usize;
        for &e in &ends {
            ids.push(ByteArray::from(buf.slice(a..e as usize)));
            a = e as usize;
        }
        out::write(
            &ud.join(format!("part-{k}.parquet")),
            out::USER_DAILY,
            vec![
                out::Col::Str(ids),
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

/// Event type: 1 = view or cart, 2 = purchase, 0 = anything else.
#[inline(always)]
fn kind(t: &[u8]) -> u8 {
    match t.len() {
        4 => {
            let v = u32::from_le_bytes(t.try_into().unwrap());
            (v == u32::from_le_bytes(*b"view") || v == u32::from_le_bytes(*b"cart")) as u8
        }
        8 => 2 * (u64::from_le_bytes(t.try_into().unwrap()) == u64::from_le_bytes(*b"purchase")) as u8,
        _ => 0,
    }
}

/// Bin of a person code inside its bucket: `bits` bits just below the bucket's.
#[inline]
fn sub_bin(p: u64, bits: u32) -> usize {
    (mix(p) >> (56 - bits)) as usize & ((1 << bits) - 1)
}

/// Bin bits for a bucket of n records (~16 records per bin).
fn bin_bits(n: usize) -> u32 {
    ((n / 16).max(1).ilog2() + 1).clamp(4, 16)
}

/// Gathers the source slices into `out`, ordered by (sub_bin(person), person,
/// time): one counting-sort pass on the bin straight from the sources, then a
/// small sort of each bin (a bin holds ~1 person; persons sharing a bin are
/// still separated by the (person, time) sort).
fn group_sort<T: Copy>(src: &[&[T]], out: &mut Vec<T>, bins: &mut Vec<u16>, bits: u32, p: impl Fn(&T) -> u64, t: impl Fn(&T) -> u32) {
    let n: usize = src.iter().map(|s| s.len()).sum();
    let nb = 1usize << bits;
    let mut start = vec![0u32; nb + 1];
    bins.clear();
    for s in src {
        bins.extend(s.iter().map(|x| sub_bin(p(x), bits) as u16));
    }
    for &b in bins.iter() {
        start[b as usize + 1] += 1;
    }
    for i in 0..nb {
        start[i + 1] += start[i];
    }
    out.clear();
    out.reserve(n);
    let mut pos = start.clone();
    let mut j = 0;
    for s in src {
        for x in s.iter() {
            let k = &mut pos[bins[j] as usize];
            unsafe { out.as_mut_ptr().add(*k as usize).write(*x) };
            *k += 1;
            j += 1;
        }
    }
    unsafe { out.set_len(n) };
    for i in 0..nb {
        let (a, b) = (start[i] as usize, start[i + 1] as usize);
        if b - a > 1 {
            out[a..b].sort_unstable_by_key(|x| (p(x), t(x)));
        }
    }
}

/// LSD radix sort by a u64 key, 8 bits per pass; passes where every key has
/// the same byte are skipped. Stable.
fn radix_sort<T: Copy + Default>(v: &mut Vec<T>, key: impl Fn(&T) -> u64) {
    let n = v.len();
    if n < 2 {
        return;
    }
    let mut hist = vec![[0u32; 256]; 8];
    for x in v.iter() {
        let k = key(x);
        for b in 0..8 {
            hist[b][(k >> (8 * b)) as usize & 255] += 1;
        }
    }
    let mut tmp: Vec<T> = vec![T::default(); n];
    for b in 0..8 {
        let h = &mut hist[b];
        if h.iter().any(|&c| c as usize == n) {
            continue;
        }
        let mut sum = 0u32;
        for c in h.iter_mut() {
            let t = *c;
            *c = sum;
            sum += t;
        }
        for x in v.iter() {
            let d = (key(x) >> (8 * b)) as usize & 255;
            tmp[h[d] as usize] = *x;
            h[d] += 1;
        }
        std::mem::swap(v, &mut tmp);
    }
}

fn find(parent: &mut [u32], mut x: u32) -> u32 {
    while parent[x as usize] != x {
        let g = parent[parent[x as usize] as usize];
        parent[x as usize] = g;
        x = g;
    }
    x
}

/// Links -> union-find -> for every linked ID that does not name its person,
/// ID code -> person code (the byte-wise smallest ID of the person).
fn people<'a>(chunks: &[&'a [u8]]) -> (Table<u64>, Reg<'a>) {
    let t0 = Instant::now();
    let verbose = std::env::var_os("SS_VERBOSE").is_some();
    let mut cd = Coder::default();
    let mut edges: Vec<(u64, u64)> = Vec::new();
    for ch in chunks {
        for_each_record::<2>(ch, |[a, b]| {
            if !a.is_empty() && !b.is_empty() {
                let (x, y) = (cd.code(a), cd.code(b));
                if x != y {
                    edges.push((x, y));
                }
            }
        });
    }
    if verbose {
        eprintln!("  (people parse {:.3}s)", t0.elapsed().as_secs_f64());
    }
    let mut reg: Reg = HashMap::new();
    merge(&mut reg, cd);
    // Dense IDs by sorting the endpoints: id order = code order, which is
    // byte order for packed codes.
    let m = edges.len();
    let mut ends: Vec<(u64, u32)> = Vec::with_capacity(2 * m);
    for (k, &(x, y)) in edges.iter().enumerate() {
        ends.push((x, 2 * k as u32));
        ends.push((y, 2 * k as u32 + 1));
    }
    drop(edges);
    radix_sort(&mut ends, |e| e.0);
    let mut id = vec![0u32; 2 * m];
    let mut codes: Vec<u64> = Vec::new();
    for (k, &(c, slot)) in ends.iter().enumerate() {
        if k == 0 || ends[k - 1].0 != c {
            codes.push(c);
        }
        id[slot as usize] = codes.len() as u32 - 1;
    }
    drop(ends);
    let nu = codes.len();
    // union-find, linking the larger root under the smaller: every root is the
    // smallest id (= smallest code) of its component
    let mut parent: Vec<u32> = (0..nu as u32).collect();
    for k in 0..m {
        let (ra, rb) = (find(&mut parent, id[2 * k]), find(&mut parent, id[2 * k + 1]));
        if ra != rb {
            let (lo, hi) = if ra < rb { (ra, rb) } else { (rb, ra) };
            parent[hi as usize] = lo;
        }
    }
    drop(id);
    let mut root = vec![0u32; nu];
    for u in 0..nu {
        root[u] = find(&mut parent, u as u32);
    }
    // Packed codes compare as bytes; fallback IDs need a byte-wise pass.
    let mut best: Vec<u64> = (0..nu).map(|u| codes[root[u] as usize]).collect();
    if !reg.is_empty() {
        for u in 0..nu {
            let r = root[u] as usize;
            if id_lt(codes[u], best[r], &reg) {
                best[r] = codes[u];
            }
        }
        for u in 0..nu {
            best[u] = best[root[u] as usize];
        }
    }
    if verbose {
        eprintln!("  (people uf {:.3}s)", t0.elapsed().as_secs_f64());
    }
    let mut person: Table<u64> = Table::new(nu);
    for u in 0..nu {
        if u + 16 < nu {
            prefetch(&person.e, person.mask, codes[u + 16]);
        }
        let p = best[u];
        if p != codes[u] {
            let (i, _) = person.slot(codes[u]);
            person.set(i, codes[u], p);
        }
    }
    (person, reg)
}

/// "cpu user+sys seconds, minor faults" so far (Linux only; for profiling).
fn proc_stat() -> String {
    let Ok(st) = std::fs::read_to_string("/proc/self/stat") else { return String::new() };
    let f: Vec<&str> = st[st.rfind(')').unwrap_or(0) + 2..].split(' ').collect();
    let g = |i: usize| f.get(i).and_then(|x| x.parse::<f64>().ok()).unwrap_or(0.0);
    // fields after the name: state=0, minflt=7, utime=11, stime=12 (clock ticks)
    format!("cpu u{:6.2} s{:5.2}  minflt {:>8}", g(11) / 100.0, g(12) / 100.0, g(7))
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
