//! Exact rules.md parsing: CSV dialect, timestamps, prices, hashing.
//! Byte-level and allocation-free: every value stays a slice into the mapped file.

use memchr::memchr;

pub const DAY_MS: i64 = 86_400_000;

/// Split one line (without its '\n') into exactly N fields and strip syntax quotes.
/// None = malformed (wrong field count, or a quote anywhere but around a whole field).
/// An empty slice in the result means NULL (rules: an empty field means null, `""` too).
#[inline]
pub fn fields<const N: usize>(line: &[u8]) -> Option<[&[u8]; N]> {
    let mut out: [&[u8]; N] = [&line[..0]; N];
    let mut start = 0usize;
    for i in 0..N - 1 {
        let rel = memchr(b',', &line[start..])?;
        out[i] = &line[start..start + rel];
        start += rel + 1;
    }
    let last = &line[start..];
    if memchr(b',', last).is_some() {
        return None; // too many fields
    }
    out[N - 1] = last;
    if memchr(b'"', line).is_some() {
        for f in out.iter_mut() {
            *f = unquote(f)?;
        }
    }
    Some(out)
}

#[inline]
fn unquote(f: &[u8]) -> Option<&[u8]> {
    if memchr(b'"', f).is_none() {
        return Some(f);
    }
    let n = f.len();
    if n >= 2 && f[0] == b'"' && f[n - 1] == b'"' && memchr(b'"', &f[1..n - 1]).is_none() {
        Some(&f[1..n - 1])
    } else {
        None
    }
}

/// Iterate the data lines of a chunk (split on '\n' only; '\r' is ordinary data).
#[inline]
pub fn for_each_line<'a>(chunk: &'a [u8], mut f: impl FnMut(&'a [u8])) {
    let mut p = 0usize;
    while p < chunk.len() {
        let e = match memchr(b'\n', &chunk[p..]) {
            Some(i) => p + i,
            None => chunk.len(),
        };
        f(&chunk[p..e]);
        p = e + 1;
    }
}

#[inline]
fn d1(b: u8) -> Option<i64> {
    if b.is_ascii_digit() { Some((b - b'0') as i64) } else { None }
}
#[inline]
fn d2(s: &[u8], i: usize) -> Option<i64> {
    Some(d1(s[i])? * 10 + d1(s[i + 1])?)
}

/// Days since 1970-01-01 (Howard Hinnant's days_from_civil).
#[inline]
pub fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let mp = if m > 2 { m - 3 } else { m + 9 };
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

#[inline]
fn is_leap(y: i64) -> bool {
    (y % 4 == 0 && y % 100 != 0) || y % 400 == 0
}

/// 'YYYY-MM-DDTHH:MM:SS' at s[0..19], strictly validated -> epoch ms (UTC).
#[inline]
fn iso19(s: &[u8]) -> Option<i64> {
    if s[4] != b'-' || s[7] != b'-' || s[10] != b'T' || s[13] != b':' || s[16] != b':' {
        return None;
    }
    let y = d2(s, 0)? * 100 + d2(s, 2)?;
    let mo = d2(s, 5)?;
    let d = d2(s, 8)?;
    let h = d2(s, 11)?;
    let mi = d2(s, 14)?;
    let se = d2(s, 17)?;
    if !(1..=12).contains(&mo) || h > 23 || mi > 59 || se > 59 {
        return None;
    }
    let dim = match mo {
        2 => if is_leap(y) { 29 } else { 28 },
        4 | 6 | 9 | 11 => 30,
        _ => 31,
    };
    if d < 1 || d > dim {
        return None;
    }
    Some(((days_from_civil(y, mo, d) * 24 + h) * 60 + mi) * 60_000 + se * 1000)
}

/// Any `ts` field -> epoch ms, or None for a bad timestamp (rules.md "Timestamps").
/// F3 values too long for 64 bits are returned as None: they are outside every window.
#[inline]
pub fn ts_ms(s: &[u8]) -> Option<i64> {
    let n = s.len();
    if n == 0 {
        return None;
    }
    if s.iter().all(u8::is_ascii_digit) {
        let mut i = 0;
        while i < n && s[i] == b'0' {
            i += 1;
        }
        if n - i > 18 {
            return None;
        }
        let mut v: i64 = 0;
        for &b in &s[i..] {
            v = v * 10 + (b - b'0') as i64;
        }
        return Some(v);
    }
    if n == 20 && s[19] == b'Z' {
        return iso19(s);
    }
    if n == 25 && (s[19] == b'+' || s[19] == b'-') && s[22] == b':' {
        let oh = d2(s, 20)?;
        let om = d2(s, 23)?;
        if oh > 23 || om > 59 {
            return None;
        }
        let off = (oh * 60 + om) * 60_000;
        let base = iso19(s)?;
        return Some(if s[19] == b'+' { base - off } else { base + off });
    }
    None
}

/// Purchase price: ASCII digits only, value 1..=10,000,000 (leading zeros allowed).
#[inline]
pub fn price(s: &[u8]) -> Option<u32> {
    if s.is_empty() || !s.iter().all(u8::is_ascii_digit) {
        return None;
    }
    let mut i = 0;
    while i < s.len() && s[i] == b'0' {
        i += 1;
    }
    if s.len() - i > 8 {
        return None;
    }
    let mut v: u32 = 0;
    for &b in &s[i..] {
        v = v * 10 + (b - b'0') as u32;
    }
    if (1..=10_000_000).contains(&v) { Some(v) } else { None }
}

/// Fast 64-bit hash of a byte string (wyhash-style mixing + murmur finalizer).
#[inline]
pub fn hash_bytes(b: &[u8]) -> u64 {
    let mut h: u64 = 0x9E37_79B9_7F4A_7C15 ^ (b.len() as u64).wrapping_mul(0xA076_1D64_78BD_642F);
    let mut i = 0;
    while i + 8 <= b.len() {
        let v = u64::from_le_bytes(b[i..i + 8].try_into().unwrap());
        h = (h ^ v).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        h ^= h >> 31;
        i += 8;
    }
    if i < b.len() {
        let mut buf = [0u8; 8];
        buf[..b.len() - i].copy_from_slice(&b[i..]);
        h = (h ^ u64::from_le_bytes(buf)).wrapping_mul(0x94D0_49BB_1331_11EB);
        h ^= h >> 29;
    }
    h ^= h >> 33;
    h = h.wrapping_mul(0xFF51_AFD7_ED55_8CCD);
    h ^= h >> 33;
    h = h.wrapping_mul(0xC4CE_B9FE_1A85_EC53);
    h ^ (h >> 33)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn timestamps() {
        assert_eq!(ts_ms(b"2026-10-03T23:58:10Z"), ts_ms(b"2026-10-03T19:58:10-04:00"));
        assert_eq!(ts_ms(b"1791071890000"), ts_ms(b"0001791071890000"));
        assert_eq!(ts_ms(b"2026-10-04T01:30:00+02:00"), ts_ms(b"2026-10-03T23:30:00Z"));
        for bad in [&b"2026-10-03T24:00:00Z"[..], b"2026-02-29T00:00:00Z", b"2026-10-03T10:00:00+24:00",
                    b"2026-10-03T10:00:00+05:60", b"2026-10-03T10:00:00+0200", b"1791071890000.0", b"-5",
                    b"2026-10-03 10:00:00Z", b"2026-10-03t10:00:00Z", b"2026-10-03T10:00:00z", b"2026-10-03T23:59:60Z"] {
            assert_eq!(ts_ms(bad), None, "{}", String::from_utf8_lossy(bad));
        }
        assert!(ts_ms(b"2028-02-29T00:00:00Z").is_some());
        assert_eq!(ts_ms(b"1970-01-01T00:00:00Z"), Some(0));
    }
    #[test]
    fn csv() {
        assert_eq!(fields::<2>(b"\"u4\",\"u1\""), Some([&b"u4"[..], b"u1"]));
        assert_eq!(fields::<2>(b"u1,u2,u3"), None);
        assert_eq!(fields::<2>(b"u1,u5\""), None);
        assert_eq!(fields::<2>(b""), None);
        assert_eq!(fields::<2>(b"u1"), None);
        assert_eq!(fields::<2>(b"\"\",x"), Some([&b""[..], b"x"]));
        assert_eq!(price(b"0250"), Some(250));
        assert_eq!(price(b"10000001"), None);
        assert_eq!(price(b"1000\r"), None);
    }
}
