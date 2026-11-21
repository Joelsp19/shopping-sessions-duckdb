//! Instruction-count bench: run once per mode under cachegrind; compare totals.
#[path = "../src/parse.rs"]
#[allow(dead_code)]
mod parse;
use parse::*;
fn main() {
    let p = std::env::args().nth(1).unwrap();
    let mode = std::env::var("MODE").unwrap_or_default();
    let mut buf = Vec::new();
    for i in 0..12 {
        let d = std::fs::read(format!("{p}/part-{i:05}.csv")).unwrap();
        buf.extend_from_slice(&d[d.iter().position(|&b| b == b'\n').unwrap() + 1..]);
    }
    let n = buf.iter().filter(|&&b| b == b'\n').count();
    let mut acc = 0u64;
    for ch in buf.chunks(4 << 20) {
        match mode.as_str() {
            "split" => for_each_record::<7>(ch, |f| acc += f[6].len() as u64),
            "v3" => for_each_record_v3::<7>(ch, |f| acc += f[6].len() as u64),
            "ids" => for_each_record::<7>(ch, |f| acc ^= pack_id(f[0]).unwrap_or(1) ^ pack_id(f[1]).unwrap_or(3)),
            "ts" => for_each_record::<7>(ch, |f| acc ^= (ts_ms(f[2]).unwrap_or(1) ^ ts_ms(f[3]).unwrap_or(3)) as u64),
            "splitall" => for_each_record::<7>(ch, |f| acc += f.iter().map(|x| x.len() as u64 ^ x.as_ptr() as u64).sum::<u64>()),
            "tsc" => {
                let mut c = TsCache::default();
                for_each_record::<7>(ch, |f| acc ^= (c.ts(f[2]).unwrap_or(1) ^ c.ts(f[3]).unwrap_or(3)) as u64)
            }
            "ing" => for_each_record::<7>(ch, |f| acc ^= ts_ms(f[3]).unwrap_or(3) as u64),
            "ingc" => {
                let mut c = TsCache::default();
                for_each_record::<7>(ch, |f| acc ^= c.ts(f[3]).unwrap_or(3) as u64)
            }
            _ => acc += ch.len() as u64,
        }
    }
    println!("lines {n} acc {acc}");
}
