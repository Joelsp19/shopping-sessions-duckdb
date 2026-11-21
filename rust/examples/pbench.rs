#[path = "../src/parse.rs"]
#[allow(dead_code)]
mod parse;
use parse::*;
use std::time::Instant;
fn main() {
    let p = std::env::args().nth(1).unwrap();
    let mut buf = Vec::new();
    for i in 0..12 {
        let f = format!("{p}/part-{i:05}.csv");
        let d = std::fs::read(&f).unwrap();
        let s = d.iter().position(|&b| b == b'\n').unwrap() + 1;
        buf.extend_from_slice(&d[s..]);
    }
    let n = buf.iter().filter(|&&b| b == b'\n').count();
    eprintln!("{} MB, {} lines", buf.len() >> 20, n);
    let ws = ts_ms(b"2026-09-07T00:00:00Z").unwrap();
    for rep in 0..3 {
        let t = Instant::now();
        let mut acc = 0u64;
        for_each_record::<7>(&buf, |f| acc = acc.wrapping_add(f[0].len() as u64 + f[3].len() as u64));
        let a = t.elapsed().as_nanos() as f64 / n as f64;
        let t = Instant::now();
        for_each_record_v1::<7>(&buf, |f| acc = acc.wrapping_add(f[0].len() as u64 + f[3].len() as u64));
        let a1 = t.elapsed().as_nanos() as f64 / n as f64;
        let t = Instant::now();
        let mut c0 = 0u64;
        for_each_record::<7>(&buf, |_| c0 += 1);
        let a2 = t.elapsed().as_nanos() as f64 / n as f64;
        let t = Instant::now();
        let mut pc = 0u64;
        let mut bb = 0;
        while bb + 64 <= buf.len() { let (m, nl, q) = masks(buf[bb..bb+64].try_into().unwrap()); pc += (m.count_ones() + nl.count_ones() + q.count_ones()) as u64; bb += 64; }
        let a3 = t.elapsed().as_nanos() as f64 / n as f64;
        let t = Instant::now();
        let mut ent = [0u64; 200];
        let mut tot = 0u64;
        let mut bb = 0; let mut q_seen = 0u64;
        while bb + 64 <= buf.len() {
            let (m, nl, qm) = masks(buf[bb..bb+64].try_into().unwrap());
            let mut mm = m; let mut idx = 0usize;
            while mm != 0 { for _ in 0..8 {
                let off = mm.trailing_zeros();
                let low = 1u64.wrapping_shl(off).wrapping_sub(1);
                let q = q_seen + (qm & low).count_ones() as u64;
                let isnl = (nl.wrapping_shr(off) & 1) << 63;
                unsafe { *ent.get_unchecked_mut(idx) = (bb as u64 + off as u64) | (q << 32) | isnl };
                idx += 1; mm &= mm.wrapping_sub(1);
            } }
            tot = tot.wrapping_add(ent[(m.count_ones() as usize).saturating_sub(1)]);
            q_seen += qm.count_ones() as u64;
            bb += 64;
        }
        let a4 = t.elapsed().as_nanos() as f64 / n as f64;
        eprintln!("flatten-only {a4:.1} ({})", tot & 1);
        eprintln!("masks-only {a3:.1} ({pc})");
        eprintln!("v2 {a:.0} v1 {a1:.0} v2-count {a2:.0}");
        let t = Instant::now();
        for_each_record::<7>(&buf, |f| {
            acc = acc.wrapping_add(pack_id(f[0]).unwrap_or(1) ^ pack_id(f[1]).unwrap_or(3));
        });
        let b = t.elapsed().as_nanos() as f64 / n as f64;
        let t = Instant::now();
        for_each_record::<7>(&buf, |f| {
            acc = acc.wrapping_add(ts_ms(f[2]).unwrap_or(1) as u64 ^ ts_ms(f[3]).unwrap_or(3) as u64);
        });
        let c = t.elapsed().as_nanos() as f64 / n as f64;
        let t = Instant::now();
        let mut bufs: Vec<Vec<[u64; 4]>> = (0..256).map(|_| Vec::with_capacity(n / 200)).collect();
        for_each_record::<7>(&buf, |f| {
            let e = pack_id(f[0]).unwrap_or(1);
            let x = ts_ms(f[2]).unwrap_or(0) - ws;
            let g = ts_ms(f[3]).unwrap_or(0);
            let u = pack_id(f[1]).unwrap_or(1);
            bufs[(mix(e) >> 56) as usize].push([e, u, g as u64, x as u64 | (f[4].len() as u64) << 40]);
        });
        let d = t.elapsed().as_nanos() as f64 / n as f64;
        eprintln!("rep{rep}: split {a:.0}  +ids {b:.0}  +ts {c:.0}  full+scatter {d:.0} ns/line  ({})", acc & 1);
    }
}
