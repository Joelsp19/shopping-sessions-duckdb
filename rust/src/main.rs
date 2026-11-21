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
use parse::{fields, for_each_line, for_each_record, hash_bytes, mix, pack_id, price, ts_ms, unpack_id, TsCache, DAY_MS, FALLBACK};
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::fs::File;
use std::hash::{BuildHasherDefault, Hash, Hasher};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

const NPB: u32 = 8; // log2 of the bucket count
const NP: usize = 1 << NPB;
/// Profiling probes: how many times to run (parse, dedup+clicks, sweep).
/// Always (1, 1, 1) in a real build.
const PROBE: (usize, usize, usize) = (1, 1, 1);
const GAP: u32 = 1_800_000;
const WEEK: i64 = 604_800_000;
const CHUNK: usize = 4 << 20;
const INVALID: u32 = u32::MAX;

#[inline]
fn bucket(h: u64) -> usize {
    (h >> (64 - NPB)) as usize
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

/// `par`, plus `fin(state, thread)` run by each thread as soon as it finds no
/// more tasks (while other threads may still be working).
fn par_fin<S: Send>(
    nt: usize,
    n: usize,
    init: impl Fn() -> S + Sync,
    f: impl Fn(&mut S, usize) + Sync,
    fin: impl Fn(&mut S, usize) + Sync,
) -> Vec<S> {
    let next = AtomicUsize::new(0);
    let nt = nt.max(1).min(n.max(1));
    std::thread::scope(|sc| {
        let hs: Vec<_> = (0..nt)
            .map(|t| {
                let (next, init, f, fin) = (&next, &init, &f, &fin);
                sc.spawn(move || {
                    let mut s = init();
                    loop {
                        let i = next.fetch_add(1, Ordering::Relaxed);
                        if i >= n {
                            break;
                        }
                        f(&mut s, i);
                    }
                    fin(&mut s, t);
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
    let m = unsafe { Mmap::map(&f).ok()? };
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

/// Map a chunk's pages in one call (MADV_POPULATE_READ) just before it is
/// parsed, instead of one page fault per page. A no-op where unsupported.
fn populate(c: &[u8]) {
    #[cfg(target_os = "linux")]
    {
        extern "C" {
            fn madvise(addr: *mut std::ffi::c_void, len: usize, advice: i32) -> i32;
        }
        let a = c.as_ptr() as usize & !4095;
        let b = c.as_ptr() as usize + c.len();
        unsafe { madvise(a as *mut std::ffi::c_void, b - a, 22) };
    }
    #[cfg(not(target_os = "linux"))]
    let _ = c;
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
    flat_ev: Vec<EvRec>,
    flat_ck: Vec<CkRec>,
    sort: SortScratch,
}

fn main() {
    let t0 = Instant::now();
    #[cfg(target_os = "linux")]
    if std::env::var("SS_NOMALLOPT").map_or(true, |v| v.is_empty()) {
        // keep freed buffers in the heap (no trim, mmap only above 32 MB), so
        // later phases reuse pages instead of faulting in fresh zeroed ones
        extern "C" {
            fn mallopt(param: i32, value: i32) -> i32;
        }
        unsafe {
            mallopt(-1, i32::MAX); // M_TRIM_THRESHOLD
            mallopt(-3, 32 << 20); // M_MMAP_THRESHOLD
        }
    }
    let mut last = Instant::now();
    let stop_after = std::env::var("SS_STOP").unwrap_or_default();
    let mut lap = |name: &str| {
        eprintln!("{name:<10}{:7.3}s  {}", last.elapsed().as_secs_f64(), proc_stat());
        last = Instant::now();
        if stop_after == name {
            std::process::exit(0); // profiling: cost of the phases so far
        }
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
    assert!(we - ws + WEEK < 1 << 32, "times must fit the sort keys' low 32 bits");
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
    // distinct names first (a handful), then their byte-wise order
    let mut uniq: Map<Key, ()> = Map::default();
    uniq.insert(Key::new(b"unknown"), ());
    for r in &prod_rows {
        uniq.insert(Key::new(r.1), ());
    }
    let mut cat_names: Vec<&[u8]> = uniq.keys().map(|k| k.b).collect();
    cat_names.sort_unstable();
    assert!(cat_names.len() <= 256, "too many categories");
    let cat_of: Map<Key, u32> = cat_names.iter().enumerate().map(|(i, c)| (Key::new(c), i as u32)).collect();
    let unknown = cat_of[&Key::new(b"unknown")];
    // product -> category: packed product codes in an open-addressing table,
    // other product IDs in a map; a later row for the same product wins
    let mut prod: Table<u32> = Table::new(prod_rows.len() + 16);
    let mut prod_other: Map<Key, u32> = Map::default();
    let mut last_cat: (&[u8], u32) = (b"", u32::MAX);
    for (p, c) in &prod_rows {
        if last_cat.0 != *c || last_cat.1 == u32::MAX {
            last_cat = (c, cat_of[&Key::new(c)]);
        }
        match pack_id(p) {
            Some(k) => {
                let (i, _) = prod.slot(k);
                prod.set(i, k, last_cat.1);
            }
            None => {
                prod_other.insert(Key::new(p), last_cat.1);
            }
        }
    }
    let prod_cat = |p: &[u8]| -> u32 {
        match pack_id(p) {
            Some(k) => prod.get(k).unwrap_or(unknown),
            None => prod_other.get(&Key::new(p)).copied().unwrap_or(unknown),
        }
    };

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
    // short campaign IDs (1..=7 bytes) as one integer in an open-addressing table
    let mut camp_short: Table<u32> = Table::new(ncamp + 16);
    for (i, id) in camp_ids.iter().enumerate() {
        if let Some(k) = short_key(id) {
            let (j, f) = camp_short.slot(k);
            if !f {
                camp_short.set(j, k, i as u32);
            }
        }
    }
    let camp_of = |c: &[u8]| -> Option<u32> {
        match short_key(c) {
            Some(k) => camp_short.get(k),
            None => camp.get(&Key::new(c)).copied(),
        }
    };
    let ev_maps = map_dir(&data.join("events"), nt);
    let ck_maps = map_dir(&data.join("clicks"), nt);
    let ln_maps = map_dir(&data.join("links"), nt);
    lap("setup");

    // Partition buffers are reserved up front from the input sizes (with
    // slack); a reservation is only address space until it is written.
    let ev_bytes: usize = ev_maps.iter().map(|m| m.len()).sum();
    let ck_bytes: usize = ck_maps.iter().map(|m| m.len()).sum();
    let per = |bytes: usize, line: usize| bytes / line / nt.max(1) / NP * 6 / 5 + 64;

    // ---- people (links -> union-find -> person codes), then events: every
    // ---- core validates copies and buckets them by hash(event_id)
    let tc = Instant::now();
    let ln_chunks = chunks(&ln_maps);
    let ev_chunks = chunks(&ev_maps);
    let ev_per = per(ev_bytes, 76);
    if std::env::var_os("SS_VERBOSE").is_some() {
        eprintln!("  (chunks {:.3}s)", tc.elapsed().as_secs_f64());
    }
    // people first, on every thread (it is random-access work that slows
    // badly when it shares the machine with the event parser)
    let tp = Instant::now();
    let (person, link_reg) = people(&ln_chunks, nt);
    if std::env::var_os("SS_VERBOSE").is_some() {
        eprintln!("  (people {:.3}s)", tp.elapsed().as_secs_f64());
    }
    lap("people");
    let copies = {
        let run = || par(nt, ev_chunks.len(), || (reserved(ev_per), Coder::default(), TsCache::default()), |(bufs, cd, tc), ci| {
            populate(ev_chunks[ci]);
            for_each_record::<7>(ev_chunks[ci], #[inline(always)] |[eid, user, ts, ing, ty, pr_id, pr]| {
                if eid.is_empty() {
                    return;
                }
                let e = cd.code(eid);
                let ing = tc.ts(ing).unwrap_or(i64::MAX);
                let mut c = ECopy { eid: e, user: 0, ing, t: INVALID, pay: 0 };
                if !user.is_empty() && !pr_id.is_empty() {
                    if let Some(x) = tc.ts(ts) {
                        if x >= ws && x < we {
                            let ok = match kind(ty) {
                                1 => true,
                                2 => match price(pr) {
                                    Some(p) => {
                                        let cat = prod_cat(pr_id);
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
        for _ in 1..PROBE.0 {
            drop(run()); // profiling probe: repeat the phase
        }
        run()
    };
    let mut reg: Reg = link_reg;
    let mut ev_parts = Vec::with_capacity(copies.len());
    for (b, cd, _) in copies {
        merge(&mut reg, cd);
        ev_parts.push(b);
    }
    // linked users map to their person; everyone else names themselves
    let person_of = |u: u64| {
        let (i, f) = person.slot(u);
        let v = unsafe { person.e.get_unchecked(i).1 };
        let m = (f as u64).wrapping_neg();
        (v & m) | (u & !m)
    };
    lap("parse");

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
        tc: TsCache,
    }
    let run_dedup = || par(
        nt,
        NP + ck_chunks.len(),
        || Work { tab: Vec::new(), best: Vec::new(), ev: reserved(evo_per), ck: reserved(cko_per), pend: Vec::with_capacity(32), cd: Coder::default(), tc: TsCache::default() },
        |st, task| {
            // click chunks first: each is several times a dedup bucket's work,
            // and must not be what the last threads are still doing
            let nck = ck_chunks.len();
            if task >= nck {
                let dp = task - nck;
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
                let (bufs, cd, pend, tc) = (&mut st.ck, &mut st.cd, &mut st.pend, &mut st.tc);
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
                populate(ck_chunks[task]);
                for_each_record::<4>(ck_chunks[task], #[inline(always)] |[cid, user, cmp, ts]| {
                    if cid.is_empty() || user.is_empty() {
                        return;
                    }
                    let Some(c) = camp_of(cmp) else { return };
                    match tc.ts(ts) {
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
    for _ in 1..PROBE.1 {
        drop(run_dedup()); // profiling probe: repeat the phase
    }
    let work = run_dedup();
    // freeing ~0.7 GB unmaps every page: let a background thread do it
    std::thread::spawn(move || drop(ev_parts));
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
    if std::env::var_os("SS_VERBOSE").is_some() {
        let tot: usize = (0..NP).map(size).sum();
        eprintln!("  (buckets max {} {} {} mean {})", size(order[0]), size(order[1]), size(order[2]), tot / NP);
    }
    // ---- outputs: each thread writes its user_daily part as soon as it runs
    // ---- out of sweep work
    let ud = outd.join("user_daily");
    let cd = outd.join("campaign_daily");
    std::fs::create_dir_all(&ud).expect("mkdir user_daily");
    std::fs::create_dir_all(&cd).expect("mkdir campaign_daily");
    let cat_bytes: Vec<ByteArray> = cat_names.iter().map(|c| ByteArray::from(c.to_vec())).collect();
    let write_part = |rows: &Vec<URow>, k: usize| {
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
    };

    let run_sweep = || par_fin(
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
            flat_ev: Vec::new(),
            flat_ck: Vec::new(),
            sort: SortScratch::default(),
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
            group_sort(&ev_src, &mut ev, &mut st.flat_ev, &mut st.sort, bits, |x| x.p, |x| x.t);
            group_sort(&ck_src, &mut ck, &mut st.flat_ck, &mut st.sort, bits, |x| x.p, |x| x.t);

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
        |st, k| write_part(&st.rows, k),
    );
    for _ in 1..PROBE.2 {
        drop(run_sweep()); // profiling probe: repeat the phase
    }
    let sweeps: Vec<Sweep> = run_sweep();
    std::thread::spawn(move || drop((evs, cks)));
    lap("sweep");

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
    // outputs are written and closed; skip freeing everything else
    std::process::exit(0);
}

/// A 1..=7 byte string as one non-zero integer (bytes, length in the top byte).
#[inline(always)]
fn short_key(b: &[u8]) -> Option<u64> {
    if b.is_empty() || b.len() > 7 {
        return None;
    }
    let mut k = [0u8; 8];
    k[..b.len()].copy_from_slice(b);
    k[7] = b.len() as u8;
    Some(u64::from_le_bytes(k))
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
    (mix(p) >> (64 - NPB - bits)) as usize & ((1 << bits) - 1)
}

/// Bin bits for a bucket of n records: ~4 records per bin, so a bin is
/// sorted in a few compares and its counters stay in cache.
fn bin_bits(n: usize) -> u32 {
    (n.max(1).ilog2().saturating_sub(2)).clamp(4, 23)
}

/// Scratch space for group_sort, reused across buckets.
#[derive(Default)]
struct SortScratch {
    hist: Vec<u32>,
    tmp: Vec<u8>,
}

/// LSD radix sort of records by their 32-bit time (3 passes of 11 bits;
/// a pass where every record has the same digit is skipped).
fn radix_t<T: Copy>(g: &mut [T], tmp: &mut Vec<u8>, t: &impl Fn(&T) -> u32) {
    let n = g.len();
    let need = n * std::mem::size_of::<T>() + std::mem::align_of::<T>();
    if tmp.len() < need {
        tmp.resize(need, 0);
    }
    let off = tmp.as_ptr().align_offset(std::mem::align_of::<T>());
    let b: &mut [T] = unsafe { std::slice::from_raw_parts_mut(tmp.as_mut_ptr().add(off) as *mut T, n) };
    let mut hist = [[0u32; 2048]; 3];
    for x in g.iter() {
        let k = t(x);
        hist[0][(k & 2047) as usize] += 1;
        hist[1][(k >> 11 & 2047) as usize] += 1;
        hist[2][(k >> 22) as usize] += 1;
    }
    let (mut src, mut dst): (&mut [T], &mut [T]) = (g, b);
    let mut in_g = true;
    for d in 0..3 {
        let h = &mut hist[d];
        let sh = 11 * d as u32;
        if h[((t(&src[0]) >> sh) & 2047) as usize] as usize == n {
            continue;
        }
        let mut sum = 0u32;
        for c in h.iter_mut() {
            let x = *c;
            *c = sum;
            sum += x;
        }
        for x in src.iter() {
            let c = &mut h[((t(x) >> sh) & 2047) as usize];
            unsafe { *dst.get_unchecked_mut(*c as usize) = *x };
            *c += 1;
        }
        std::mem::swap(&mut src, &mut dst);
        in_g = !in_g;
    }
    if !in_g {
        dst.copy_from_slice(src);
    }
}

/// Gathers the source slices into `out`, ordered by (sub_bin(person), person,
/// time): one counting scatter by bin, then each (small) bin sorted by
/// (person, time). Order among equal (person, time) is unspecified.
fn group_sort<T: Copy>(
    src: &[&[T]],
    out: &mut Vec<T>,
    _flat: &mut Vec<T>,
    sc: &mut SortScratch,
    bits: u32,
    p: impl Fn(&T) -> u64,
    t: impl Fn(&T) -> u32,
) {
    let nb = 1usize << bits;
    let n: usize = src.iter().map(|s| s.len()).sum();
    let h = &mut sc.hist;
    h.clear();
    h.resize(nb + 1, 0);
    for s in src {
        for x in s.iter() {
            unsafe { *h.get_unchecked_mut(sub_bin(p(x), bits) + 1) += 1 };
        }
    }
    for i in 0..nb {
        h[i + 1] += h[i];
    }
    // h[b] = start of bin b; scatter advances h[b] to the start of bin b+1
    out.clear();
    out.reserve(n);
    let dst = out.as_mut_ptr();
    for s in src {
        for x in s.iter() {
            unsafe {
                let c = h.get_unchecked_mut(sub_bin(p(x), bits));
                dst.add(*c as usize).write(*x);
                *c += 1;
            }
        }
    }
    unsafe { out.set_len(n) };
    // now h[b] = end of bin b
    let key = |x: &T| ((p(x) as u128) << 32) | t(x) as u128;
    let mut a = 0usize;
    for b in 0..nb {
        let e = h[b] as usize;
        let len = e - a;
        if len > 1 {
            let g = &mut out[a..e];
            if len <= 24 {
                for i in 1..len {
                    let v = g[i];
                    let kv = key(&v);
                    let mut j = i;
                    while j > 0 && key(&g[j - 1]) > kv {
                        g[j] = g[j - 1];
                        j -= 1;
                    }
                    g[j] = v;
                }
            } else if len >= 256 && g.iter().all(|x| p(x) == p(&g[0])) {
                radix_t(g, &mut sc.tmp, &t);
            } else {
                g.sort_unstable_by_key(|x| key(x));
            }
        }
        a = e;
    }
}

/// LSD radix sort of (key, index) pairs by the low 55 bits of the key, 11 bits
/// per pass (histograms stay in L1); passes where every key has the same digit
/// are skipped. Stable.
#[allow(dead_code)]
fn radix11(v: &mut Vec<(u64, u32)>, tmp: &mut Vec<(u64, u32)>, bits: u32) {
    const P: usize = 5;
    debug_assert!(bits <= 55);
    let n = v.len();
    if n < 2 {
        return;
    }
    let mut hist = [[0u32; 2048]; P];
    for &(k, _) in v.iter() {
        unsafe {
            *hist[0].get_unchecked_mut((k & 2047) as usize) += 1;
            *hist[1].get_unchecked_mut((k >> 11 & 2047) as usize) += 1;
            *hist[2].get_unchecked_mut((k >> 22 & 2047) as usize) += 1;
            *hist[3].get_unchecked_mut((k >> 33 & 2047) as usize) += 1;
            *hist[4].get_unchecked_mut((k >> 44 & 2047) as usize) += 1;
        }
    }
    tmp.clear();
    tmp.reserve(n);
    unsafe { tmp.set_len(n) };
    for d in 0..P {
        let h = &mut hist[d];
        let sh = 11 * d as u32;
        if h[((v[0].0 >> sh) & 2047) as usize] as usize == n {
            continue;
        }
        let mut sum = 0u32;
        for c in h.iter_mut() {
            let x = *c;
            *c = sum;
            sum += x;
        }
        let dst = tmp.as_mut_ptr();
        for &x in v.iter() {
            unsafe {
                let c = h.get_unchecked_mut(((x.0 >> sh) & 2047) as usize);
                dst.add(*c as usize).write(x);
                *c += 1;
            }
        }
        std::mem::swap(v, tmp);
    }
}

/// Links -> union-find -> for every linked ID that does not name its person,
/// ID code -> person code (the byte-wise smallest ID of the person).
/// A raw pointer that may be shared across the worker threads; every use
/// writes disjoint slots.
#[derive(Clone, Copy)]
struct SharedPtr<T>(*mut T);
unsafe impl<T> Send for SharedPtr<T> {}
unsafe impl<T> Sync for SharedPtr<T> {}
impl<T> SharedPtr<T> {
    #[inline(always)]
    unsafe fn write(self, i: usize, v: T) {
        self.0.add(i).write(v)
    }
    #[inline(always)]
    fn ptr(self) -> *mut T {
        self.0
    }
}

/// Links -> union-find -> for every linked ID that does not name its person,
/// ID code -> person code (the byte-wise smallest ID of the person).
/// Everything but the union-find itself runs on all threads.
fn people<'a>(chunks: &[&'a [u8]], nt: usize) -> (Table<u64>, Reg<'a>) {
    let t0 = Instant::now();
    let verbose = std::env::var_os("SS_VERBOSE").is_some();
    let lapv = |what: &str| {
        if verbose {
            eprintln!("  (people {what} {:.3}s)", t0.elapsed().as_secs_f64());
        }
    };
    // 1. parse
    let parts = par(nt, chunks.len(), || (Vec::<(u64, u64)>::new(), Coder::default()), |(e, cd), ci| {
        for_each_record::<2>(chunks[ci], #[inline(always)] |[a, b]| {
            if !a.is_empty() && !b.is_empty() {
                let (x, y) = (cd.code(a), cd.code(b));
                if x != y {
                    e.push((x, y));
                }
            }
        });
    });
    let mut reg: Reg = HashMap::new();
    let mut edges: Vec<(u64, u64)> = Vec::with_capacity(parts.iter().map(|p| p.0.len()).sum());
    for (e, cd) in parts {
        merge(&mut reg, cd);
        edges.extend_from_slice(&e);
    }
    let m = edges.len();
    let n2 = 2 * m;
    lapv("parse");
    if m == 0 {
        return (Table::new(0), reg);
    }
    let end_code = |j: usize| if j & 1 == 0 { edges[j >> 1].0 } else { edges[j >> 1].1 };

    // 2. endpoints sorted by code: sample splitters, partition, sort partitions
    const NPART: usize = 256;
    let mut sample: Vec<u64> = (0..4096).map(|i| end_code(i * n2 / 4096)).collect();
    sample.sort_unstable();
    let split: Vec<u64> = (1..NPART).map(|k| sample[k * 4096 / NPART]).collect();
    let part_of = |c: u64| split.partition_point(|&x| x <= c);
    let ranges = nt.max(1) * 4;
    let rng = |r: usize| (r * m / ranges, (r + 1) * m / ranges);
    // per range: partition of each endpoint and a histogram
    let hists = par(nt, ranges, || Vec::<(usize, Vec<u32>)>::new(), |out, r| {
        let mut h = vec![0u32; NPART];
        let (lo, hi) = rng(r);
        for j in 2 * lo..2 * hi {
            h[part_of(end_code(j))] += 1;
        }
        out.push((r, h));
    });
    let mut hist: Vec<Vec<u32>> = vec![Vec::new(); ranges];
    for v in hists {
        for (r, h) in v {
            hist[r] = h;
        }
    }
    // offsets: partition-major, then range
    let mut off = vec![vec![0u32; NPART]; ranges];
    let mut pstart = vec![0usize; NPART + 1];
    let mut acc = 0u32;
    for pt in 0..NPART {
        pstart[pt] = acc as usize;
        for r in 0..ranges {
            off[r][pt] = acc;
            acc += hist[r][pt];
        }
    }
    pstart[NPART] = acc as usize;
    let mut ends: Vec<(u64, u32)> = Vec::with_capacity(n2);
    let ep = SharedPtr(ends.as_mut_ptr());
    par(nt, ranges, || (), |_, r| {
        let mut o = off[r].clone();
        let (lo, hi) = rng(r);
        for j in 2 * lo..2 * hi {
            let c = end_code(j);
            let pt = part_of(c);
            unsafe { ep.write(o[pt] as usize, (c, j as u32)) };
            o[pt] += 1;
        }
    });
    unsafe { ends.set_len(n2) };
    // sort each partition and count its distinct codes
    let ends_p = SharedPtr(ends.as_mut_ptr());
    let uniq = par(nt, NPART, || Vec::<(usize, u32)>::new(), |out, pt| {
        let (a, b) = (pstart[pt], pstart[pt + 1]);
        let g = unsafe { std::slice::from_raw_parts_mut(ends_p.ptr().add(a), b - a) };
        g.sort_unstable_by_key(|e| e.0);
        let mut u = 0u32;
        for k in 0..g.len() {
            if k == 0 || g[k - 1].0 != g[k].0 {
                u += 1;
            }
        }
        out.push((pt, u));
    });
    let mut ubase = vec![0u32; NPART + 1];
    for v in uniq {
        for (pt, u) in v {
            ubase[pt + 1] = u;
        }
    }
    for pt in 0..NPART {
        ubase[pt + 1] += ubase[pt];
    }
    let nu = ubase[NPART] as usize;
    lapv("sort");

    // 3. dense ids (id order = code order) and each endpoint's id
    let mut codes: Vec<u64> = Vec::with_capacity(nu);
    let mut id: Vec<u32> = Vec::with_capacity(n2);
    let (cp, ip) = (SharedPtr(codes.as_mut_ptr()), SharedPtr(id.as_mut_ptr()));
    par(nt, NPART, || (), |_, pt| {
        let g = &ends[pstart[pt]..pstart[pt + 1]];
        let mut next = ubase[pt];
        for k in 0..g.len() {
            if k == 0 || g[k - 1].0 != g[k].0 {
                unsafe { cp.write(next as usize, g[k].0) };
                next += 1;
            }
            unsafe { ip.write(g[k].1 as usize, next - 1) };
        }
    });
    unsafe {
        codes.set_len(nu);
        id.set_len(n2);
    }
    std::thread::spawn(move || drop((edges, ends)));

    // 4. union-find on all threads, lock-free: a root is only ever linked
    // under a smaller one, by a compare-and-swap that fails if it stopped being
    // a root (then the union retries). Links only point to smaller ids, so
    // every root is the smallest id (= smallest code) of its component.
    // Path halving only ever points a node at one of its ancestors.
    use std::sync::atomic::AtomicU32;
    let parent: Vec<AtomicU32> = (0..nu as u32).map(AtomicU32::new).collect();
    #[inline(always)]
    fn cfind(p: &[AtomicU32], mut x: u32) -> u32 {
        loop {
            let px = p[x as usize].load(Ordering::Acquire);
            if px == x {
                return x;
            }
            let gx = p[px as usize].load(Ordering::Acquire);
            if gx != px {
                p[x as usize].store(gx, Ordering::Release);
            }
            x = gx;
        }
    }
    let par_n = nt.max(1) * 8;
    par(nt, par_n, || (), |_, r| {
        for k in r * m / par_n..(r + 1) * m / par_n {
            let (a, b) = (id[2 * k], id[2 * k + 1]);
            loop {
                let (ra, rb) = (cfind(&parent, a), cfind(&parent, b));
                if ra == rb {
                    break;
                }
                let (lo, hi) = if ra < rb { (ra, rb) } else { (rb, ra) };
                if parent[hi as usize].compare_exchange(hi, lo, Ordering::AcqRel, Ordering::Acquire).is_ok() {
                    break;
                }
            }
        }
    });
    lapv("uf");
    // roots, read-only and in parallel
    let mut root: Vec<u32> = Vec::with_capacity(nu);
    let rp = SharedPtr(root.as_mut_ptr());
    par(nt, par_n, || (), |_, r| {
        for u in r * nu / par_n..(r + 1) * nu / par_n {
            let mut x = u as u32;
            loop {
                let px = parent[x as usize].load(Ordering::Relaxed);
                if px == x {
                    break;
                }
                x = px;
            }
            unsafe { rp.write(u, x) };
        }
    });
    unsafe { root.set_len(nu) };
    // Packed codes compare as bytes; fallback IDs need a byte-wise pass.
    let mut best_root: Vec<u64> = Vec::new();
    if !reg.is_empty() {
        best_root = codes.clone();
        for u in 0..nu {
            let r = root[u] as usize;
            if id_lt(codes[u], best_root[r], &reg) {
                best_root[r] = codes[u];
            }
        }
    }
    let person_code = |u: usize| if best_root.is_empty() { codes[root[u] as usize] } else { best_root[root[u] as usize] };

    // 5. person table, filled in parallel: a slot is claimed by a
    // compare-and-swap on its key, then its value is written
    let person: Table<u64> = Table::new(nu);
    let (tp, tmask) = (SharedPtr(person.e.as_ptr() as *mut (u64, u64)), person.mask);
    par(nt, par_n, || (), |_, r| {
        let (lo, hi) = (r * nu / par_n, (r + 1) * nu / par_n);
        for u in lo..hi {
            if u + 16 < hi {
                prefetch(&person.e, tmask, codes[u + 16]);
            }
            let (k, p) = (codes[u], person_code(u));
            if p == k {
                continue;
            }
            let mut i = mix(k) as usize & tmask;
            loop {
                let key = unsafe { &*(tp.ptr().add(i) as *const std::sync::atomic::AtomicU64) };
                if key.compare_exchange(0, k, Ordering::Relaxed, Ordering::Relaxed).is_ok() {
                    unsafe { (*tp.ptr().add(i)).1 = p };
                    break;
                }
                i = (i + 1) & tmask;
            }
        }
    });
    lapv("table");
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
