#[path = "../src/parse.rs"]
#[allow(dead_code)]
mod parse;
use std::time::Instant;
fn main() {
    let p = std::env::args().nth(1).unwrap();
    let mut buf = Vec::new();
    for i in 0..12 {
        let d = std::fs::read(format!("{p}/part-{i:05}.csv")).unwrap();
        buf.extend_from_slice(&d[d.iter().position(|&b| b == b'\n').unwrap() + 1..]);
    }
    let n = buf.iter().filter(|&&b| b == b'\n').count();
    for _ in 0..8 {
        let t = Instant::now();
        let mut c = 0u64;
        for ch in buf.chunks(4 << 20) { parse::for_each_record::<7>(ch, |f| c += f[6].len() as u64); }
        eprintln!("v2 {:.1} ns/line ({c})", t.elapsed().as_nanos() as f64 / n as f64);
    }
}
