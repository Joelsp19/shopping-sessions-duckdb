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

/// An identity the optimizer cannot see through. Without it LLVM turns the
/// movemask results into 64 separate bits and the bit loops into hundreds of
/// scalar instructions per block.
#[inline(always)]
fn opaque(mut x: u64) -> u64 {
    #[cfg(target_arch = "x86_64")]
    unsafe {
        std::arch::asm!("/* {0} */", inout(reg) x, options(pure, nomem, nostack, preserves_flags));
    }
    x
}

/// Masks of one 64-byte block: (',' or '\n', '\n', '"'); bit i is byte i.
#[inline(always)]
pub fn masks(p: &[u8; 64]) -> (u64, u64, u64) {
    #[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
    unsafe {
        use std::arch::x86_64::*;
        let (c, n, q) = (_mm256_set1_epi8(b',' as i8), _mm256_set1_epi8(b'\n' as i8), _mm256_set1_epi8(b'"' as i8));
        let (mut rs, mut rn, mut rq) = (0u64, 0u64, 0u64);
        for k in 0..2 {
            let v = _mm256_loadu_si256(p.as_ptr().add(32 * k) as *const __m256i);
            let mn = _mm256_cmpeq_epi8(v, n);
            rs |= (_mm256_movemask_epi8(_mm256_or_si256(_mm256_cmpeq_epi8(v, c), mn)) as u32 as u64) << (32 * k);
            rn |= (_mm256_movemask_epi8(mn) as u32 as u64) << (32 * k);
            rq |= (_mm256_movemask_epi8(_mm256_cmpeq_epi8(v, q)) as u32 as u64) << (32 * k);
        }
        return (opaque(rs), opaque(rn), opaque(rq));
    }
    #[cfg(all(target_arch = "x86_64", not(target_feature = "avx2")))]
    unsafe {
        use std::arch::x86_64::*;
        let (c, n, q) = (_mm_set1_epi8(b',' as i8), _mm_set1_epi8(b'\n' as i8), _mm_set1_epi8(b'"' as i8));
        let (mut rs, mut rn, mut rq) = (0u64, 0u64, 0u64);
        for k in 0..4 {
            let v = _mm_loadu_si128(p.as_ptr().add(16 * k) as *const __m128i);
            let mn = _mm_cmpeq_epi8(v, n);
            rs |= (_mm_movemask_epi8(_mm_or_si128(_mm_cmpeq_epi8(v, c), mn)) as u16 as u64) << (16 * k);
            rn |= (_mm_movemask_epi8(mn) as u16 as u64) << (16 * k);
            rq |= (_mm_movemask_epi8(_mm_cmpeq_epi8(v, q)) as u16 as u64) << (16 * k);
        }
        return (opaque(rs), opaque(rn), opaque(rq));
    }
    #[allow(unreachable_code)]
    {
        let (mut rs, mut rn, mut rq) = (0u64, 0u64, 0u64);
        for (i, &b) in p.iter().enumerate() {
            rs |= ((b == b',' || b == b'\n') as u64) << i;
            rn |= ((b == b'\n') as u64) << i;
            rq |= ((b == b'"') as u64) << i;
        }
        (rs, rn, rq)
    }
}

/// Every well-formed line of a chunk as exactly N unquoted fields: the same
/// results as `fields` on each '\n'-separated line (malformed lines skipped).
///
/// Two stages per batch of up to 128 blocks, with no dependency chain from
/// one field to the next: (1) the ',' / '\n' positions of every block are
/// listed and quotes counted per block; (2) a line is N consecutive listed
/// separators (N-1 commas, then '\n'). A field is quoted when it starts and
/// ends with '"' (length >= 2); the line is well formed iff its quote count is
/// exactly 2 per quoted field (any other quote makes `fields` reject it).
const PF_AHEAD: usize = 1024;

#[inline]
pub fn for_each_record<'a, const N: usize>(chunk: &'a [u8], mut f: impl FnMut([&'a [u8]; N])) {
    const NB: usize = 128;
    const W: usize = NB * 64;
    let len = chunk.len();
    let mut pos = [0u32; W + 64];
    let mut qm = [0u64; NB + 1];
    let mut qb = [0u32; NB + 1];
    let mut s = 0usize;
    while s + 64 <= len {
        let nb = ((len - s) / 64).min(NB);
        let region = nb * 64;
        let bytes = &chunk[s..s + region];
        // stage 1
        let mut np = 0usize;
        let mut qacc = 0u32;
        for i in 0..nb {
            #[cfg(target_arch = "x86_64")]
            unsafe {
                // the hardware prefetcher stops at page ends; stay ahead of it
                use std::arch::x86_64::{_mm_prefetch, _MM_HINT_T0};
                _mm_prefetch::<_MM_HINT_T0>(chunk.as_ptr().wrapping_add(s + i * 64 + PF_AHEAD) as *const i8);
            }
            let blk: &[u8; 64] = bytes[i * 64..i * 64 + 64].try_into().unwrap();
            let (ms, _, mq) = masks(blk);
            qm[i] = mq;
            qb[i] = qacc;
            qacc += mq.count_ones();
            let off = (i * 64) as u32;
            let cnt = ms.count_ones() as usize;
            let mut m = ms;
            for j in 0..8 {
                unsafe { *pos.get_unchecked_mut(np + j) = off + m.trailing_zeros() };
                m &= m.wrapping_sub(1);
            }
            if cnt > 8 {
                let mut j = np + 8;
                while m != 0 {
                    unsafe { *pos.get_unchecked_mut(j) = off + m.trailing_zeros() };
                    m &= m - 1;
                    j += 1;
                }
            }
            np += cnt;
        }
        qm[nb] = 0;
        qb[nb] = qacc;
        let qcount = |x: usize| -> u32 {
            let i = x >> 6;
            unsafe { *qb.get_unchecked(i) + (*qm.get_unchecked(i) & ((1u64 << (x & 63)) - 1)).count_ones() }
        };
        // stage 2
        let at = |i: usize| unsafe { *bytes.get_unchecked(i) };
        let mut k = 0usize;
        let mut cur = 0usize;
        while k + N <= np {
            let mut e = [0usize; N];
            for j in 0..N {
                e[j] = unsafe { *pos.get_unchecked(k + j) } as usize;
            }
            let mut ok = at(e[N - 1]) == b'\n';
            for j in 0..N - 1 {
                ok &= at(e[j]) == b',';
            }
            if ok {
                let mut out: [&'a [u8]; N] = [&chunk[..0]; N];
                let mut a = cur;
                let mut expect = 0u32;
                for j in 0..N {
                    let b = e[j];
                    let q = ((b >= a + 2) & (at(a) == b'"') & (at(b.wrapping_sub(1).min(region - 1)) == b'"')) as usize;
                    out[j] = unsafe { chunk.get_unchecked(s + a + q..s + b - q) };
                    expect += 2 * q as u32;
                    a = b + 1;
                }
                if qcount(e[N - 1]) - qcount(cur) == expect {
                    f(out);
                }
                cur = e[N - 1] + 1;
                k += N;
            } else {
                // wrong field count: skip to the line's newline
                let mut j = k;
                while j < np && at(pos[j] as usize) != b'\n' {
                    j += 1;
                }
                if j >= np {
                    break;
                }
                cur = pos[j] as usize + 1;
                k = j + 1;
            }
        }
        if cur == 0 {
            // no whole line in the batch: one (long) line the slow way
            let end = memchr(b'\n', &chunk[s..]).map_or(len, |i| s + i);
            if let Some(r) = fields::<N>(&chunk[s..end]) {
                f(r);
            }
            s = end + 1;
            continue;
        }
        s += cur;
    }
    if s < len {
        for_each_line(&chunk[s..], |l| {
            if let Some(r) = fields::<N>(l) {
                f(r);
            }
        });
    }
}

/// Every well-formed line of a chunk as exactly N unquoted fields: the same
/// results as `fields` on each '\n'-separated line (malformed lines skipped).
///
/// Field at a time: 32 bytes are loaded at the field's start and the first
/// ',', '\n' or '"' found with one compare. A plain field must end at its
/// separator; a field opening with '"' must close at the first special byte
/// with '"' and be followed by its separator. Any line that does not fit (a
/// field over 31 bytes, quotes inside, wrong field count) goes to `fields`.
#[inline]
pub fn for_each_record_f<'a, const N: usize>(chunk: &'a [u8], mut f: impl FnMut([&'a [u8]; N])) {
    #[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
    {
        use std::arch::x86_64::*;
        let len = chunk.len();
        let ptr = chunk.as_ptr();
        let mut start = 0usize;
        // fast path while every window of the line is surely inside the chunk
        while start + 33 * N + 32 <= len {
            let mut out: [&'a [u8]; N] = [&chunk[..0]; N];
            let mut s = start;
            let mut ok = 1u32;
            // One field: (ok bit, first byte, length, next field's start).
            // Bit 32 caps the search: a field reaching it is valid only if its
            // separator sits exactly there.
            #[inline(always)]
            unsafe fn field(ptr: *const u8, s: usize, want: u8) -> (u32, usize, usize, usize) {
                let v = _mm256_loadu_si256(ptr.add(s) as *const __m256i);
                let qv = _mm256_cmpeq_epi8(v, _mm256_set1_epi8(b'"' as i8));
                let sv = _mm256_or_si256(
                    _mm256_cmpeq_epi8(v, _mm256_set1_epi8(b',' as i8)),
                    _mm256_cmpeq_epi8(v, _mm256_set1_epi8(10)),
                );
                let qm = opaque(_mm256_movemask_epi8(qv) as u32 as u64);
                let m = opaque(_mm256_movemask_epi8(_mm256_or_si256(qv, sv)) as u32 as u64) | (1 << 32);
                let q0 = (qm & 1) as usize;
                let pos = (m & !(q0 as u64)).trailing_zeros() as usize; // 1..=32 if quoted
                let closing = ((qm >> pos) & 1) as usize; // bit 32 of qm is 0
                let ok = (*ptr.add(s + pos + q0) == want) as u32 & (q0 <= closing) as u32;
                (ok, s + q0, pos - q0, s + pos + q0 + 1)
            }
            for k in 0..N - 1 {
                let (o, a, l, nx) = unsafe { field(ptr, s, b',') };
                ok &= o;
                out[k] = unsafe { std::slice::from_raw_parts(ptr.add(a), l) };
                s = nx;
            }
            {
                let (o, a, l, nx) = unsafe { field(ptr, s, 10) };
                ok &= o;
                out[N - 1] = unsafe { std::slice::from_raw_parts(ptr.add(a), l) };
                s = nx;
            }
            if ok != 0 {
                f(out);
                start = s;
            } else {
                // reference splitter for this one line
                let end = match memchr(b'\n', &chunk[start..]) {
                    Some(i) => start + i,
                    None => len,
                };
                if let Some(r) = fields::<N>(&chunk[start..end]) {
                    f(r);
                }
                start = end + 1;
            }
        }
        if start < len {
            for_each_line(&chunk[start..], |l| {
                if let Some(r) = fields::<N>(l) {
                    f(r);
                }
            });
        }
        return;
    }
    #[allow(unreachable_code)]
    for_each_record_v3(chunk, f)
}

/// Every well-formed line of a chunk as exactly N unquoted fields (the same
/// rules as `fields`), found with one SIMD pass over the chunk instead of a
/// search per field. Malformed lines (wrong field count, bad quoting, empty)
/// are skipped. Lines end at '\n' only; the last may lack one.
/// Quotes are only counted: a line is well quoted exactly when every field
/// that starts with '"' also ends with one and those pairs are all its quotes.
///
/// Per batch of 64-byte blocks, the comma and newline bits are flattened into
/// two position lists (8 writes at a time, whether or not they exist). A line
/// is well formed when exactly N-1 commas lie before its newline; quotes are
/// counted once per line from the blocks' quote masks.
#[inline]
pub fn for_each_record_v3<'a, const N: usize>(chunk: &'a [u8], mut f: impl FnMut([&'a [u8]; N])) {
    const B: usize = 16; // blocks per batch
    let len = chunk.len();
    assert!(len < (u32::MAX - 64) as usize, "chunk too large");
    assert!(N >= 2 && N <= 16);
    let mut cm = [0u32; 64 * B + 64 + 16 + 32]; // comma positions (+ unfinished line's)
    let mut nlp = [0u32; 64 * B + 16]; // newline positions
    let mut qm = [0u64; B]; // quote mask per block
    let mut qb = [0u64; B]; // quotes before each block
    let mut ccnt = 0usize;
    let mut start = 0usize; // first byte of the current line
    let mut q_line = 0u64; // quotes before the current line
    let mut q_seen = 0u64; // quotes before `base`
    let mut bad = false; // the unfinished line already has too many commas
    let mut base = 0usize;
    #[inline(always)]
    fn flatten<const K: usize>(out: &mut [u32], mut idx: usize, base: usize, mut m: u64) -> usize {
        let cnt = m.count_ones() as usize;
        let end = idx + cnt;
        loop {
            for _ in 0..K {
                debug_assert!(idx < out.len());
                unsafe { *out.get_unchecked_mut(idx) = (base as u32).wrapping_add(m.trailing_zeros()) };
                idx += 1;
                m &= m.wrapping_sub(1);
            }
            if idx >= end {
                return end;
            }
        }
    }
    loop {
        let bbase = base;
        let mut nb = 0usize;
        let mut ncnt = 0usize;
        while nb < B && base < len {
            let (sep, nl, q) = if base + 64 <= len {
                masks(chunk[base..base + 64].try_into().unwrap())
            } else {
                let mut buf = [0u8; 64];
                buf[..len - base].copy_from_slice(&chunk[base..]);
                masks(&buf)
            };
            ccnt = flatten::<8>(&mut cm, ccnt, base, sep & !nl);
            ncnt = flatten::<2>(&mut nlp, ncnt, base, nl);
            qm[nb] = q;
            qb[nb] = q_seen;
            q_seen += q.count_ones() as u64;
            base += 64;
            nb += 1;
        }
        let last = base >= len;
        if last && len > 0 && chunk[len - 1] != 10 {
            nlp[ncnt] = len as u32; // the last line may lack its '\n'
            ncnt += 1;
        }
        for k in 0..N {
            cm[ccnt + k] = u32::MAX;
        }
        let mut ci = 0usize;
        for j in 0..ncnt {
            let end = nlp[j] as usize;
            let bi = (end - bbase) >> 6;
            let cq = if bi < nb { qb[bi] + (qm[bi] & ((1u64 << ((end - bbase) & 63)) - 1)).count_ones() as u64 } else { q_seen };
            if !bad && (cm[ci + N - 2] as usize) < end && cm[ci + N - 1] as usize > end {
                let nq = cq - q_line;
                let mut out: [&'a [u8]; N] = [&chunk[..0]; N];
                let mut s = start;
                let mut bad_q = 0usize;
                let mut paired = 0usize;
                let ptr = chunk.as_ptr();
                for k in 0..N {
                    let e = if k + 1 < N { cm[ci + k] as usize } else { end };
                    // An empty field's first byte is its separator (or, at the
                    // very end, the comma before it), never a quote.
                    let c0 = unsafe { *ptr.add(s.min(len - 1)) };
                    let c1 = unsafe { *ptr.add(e.max(1) - 1) };
                    let q0 = (c0 == b'"') as usize;
                    let q1 = ((c1 == b'"') & (e >= s + 2)) as usize;
                    bad_q |= q0 & !q1;
                    paired += q0;
                    out[k] = unsafe { std::slice::from_raw_parts(ptr.add(s + q0), (e - s).saturating_sub(2 * q0)) };
                    s = e + 1;
                }
                ci += N - 1;
                if bad_q & 1 == 0 && 2 * paired as u64 == nq {
                    f(out);
                }
            } else {
                while (cm[ci] as usize) < end {
                    ci += 1;
                }
                bad = false;
            }
            start = end + 1;
            q_line = cq;
        }
        // commas of the unfinished line move to the front
        let rest = ccnt - ci;
        if rest >= N {
            bad = true;
            ccnt = 0;
        } else {
            cm.copy_within(ci..ccnt, 0);
            ccnt = rest;
        }
        if last {
            return;
        }
    }
}

#[allow(dead_code)]
pub fn for_each_record_v2<'a, const N: usize>(chunk: &'a [u8], mut f: impl FnMut([&'a [u8]; N])) {
    const NL: u64 = 1 << 63;
    const B: usize = 16; // blocks per batch
    const CAP: usize = 64 * B + 72 + 64;
    let len = chunk.len();
    assert!(len < 1 << 31, "chunk too large");
    let mut ent = [0u64; CAP];
    let mut cnt = 0usize;
    let mut start = 0usize; // first byte of the current line
    let mut q_line = 0u64; // quotes before the current line
    let mut q_seen = 0u64; // quotes before the current block
    let mut bad = false; // the current (unfinished) line is already malformed
    let mut base = 0usize;
    let mut done = false;
    while !done {
        // ---- stage 1: flatten up to B blocks
        for _ in 0..B {
            if base >= len {
                break;
            }
            let (m, nl, qm) = if base + 64 <= len {
                masks(chunk[base..base + 64].try_into().unwrap())
            } else {
                let mut buf = [0u8; 64];
                buf[..len - base].copy_from_slice(&chunk[base..]);
                masks(&buf)
            };
            let mut mm = m;
            let mut idx = cnt;
            while mm != 0 {
                for _ in 0..8 {
                    let off = mm.trailing_zeros();
                    let low = 1u64.wrapping_shl(off).wrapping_sub(1);
                    let q = q_seen + (qm & low).count_ones() as u64;
                    let isnl = (nl.wrapping_shr(off) & 1) << 63;
                    debug_assert!(idx < CAP);
                    unsafe { *ent.get_unchecked_mut(idx) = (base as u64 + off as u64) | (q << 32) | isnl };
                    idx += 1;
                    mm &= mm.wrapping_sub(1);
                }
            }
            cnt += m.count_ones() as usize;
            q_seen += qm.count_ones() as u64;
            base += 64;
        }
        if base >= len {
            done = true;
            if start < len {
                // last line without '\n': a virtual newline at the end
                ent[cnt] = len as u64 | (q_seen << 32) | NL;
                cnt += 1;
            }
        }
        // ---- stage 2: whole lines
        let mut i = 0usize;
        while i < cnt {
            if !bad && i + N <= cnt {
                let e = &ent[i..i + N];
                let mut any = 0u64;
                for k in 0..N - 1 {
                    any |= e[k];
                }
                if any & NL == 0 && e[N - 1] & NL != 0 {
                    // N fields; check the quoting without branches
                    let nq = (e[N - 1] >> 32 & 0x7FFF_FFFF) - q_line;
                    let mut out: [&'a [u8]; N] = [&chunk[..0]; N];
                    let mut s = start;
                    let mut ok = 1u64;
                    let mut paired = 0u64;
                    for k in 0..N {
                        let end = e[k] as u32 as usize;
                        let n = end - s;
                        let c0 = unsafe { *chunk.get_unchecked(s.min(len - 1)) };
                        let c1 = unsafe { *chunk.get_unchecked(end.max(1) - 1) };
                        let q0 = ((n > 0) & (c0 == b'"')) as usize;
                        let q1 = ((n >= 2) & (c1 == b'"')) as u64;
                        ok &= (q0 as u64 ^ 1) | q1;
                        paired += 2 * q0 as u64;
                        let a = s + q0;
                        out[k] = unsafe { chunk.get_unchecked(a..(end - q0).max(a)) };
                        s = end + 1;
                    }
                    start = s;
                    q_line = e[N - 1] >> 32 & 0x7FFF_FFFF;
                    i += N;
                    if ok != 0 && paired == nq {
                        f(out);
                    }
                    continue;
                }
            }
            // malformed, flagged, or unfinished: skip to the line's newline
            match (i..cnt).find(|&k| ent[k] & NL != 0) {
                Some(k) => {
                    start = ent[k] as u32 as usize + 1;
                    q_line = ent[k] >> 32 & 0x7FFF_FFFF;
                    bad = false;
                    i = k + 1;
                }
                None => break,
            }
        }
        // keep the unfinished line's entries (a line with N or more is malformed)
        let rest = cnt - i.min(cnt);
        if rest >= N {
            bad = true;
            cnt = 0;
        } else {
            ent.copy_within(i..cnt, 0);
            cnt = rest;
        }
    }
}

#[allow(dead_code)]
pub fn for_each_record_v1<'a, const N: usize>(chunk: &'a [u8], mut f: impl FnMut([&'a [u8]; N])) {
    let len = chunk.len();
    let mut seps = [0usize; N];
    let mut nsep = 0usize;
    let mut start = 0usize;
    let mut q_line = 0u32; // quotes before the current line
    let mut q_seen = 0u32; // quotes before the current block
    let mut emit = |start: usize, end: usize, seps: &[usize; N], nsep: usize, nq: u32| {
        if nsep != N - 1 {
            return;
        }
        let mut out: [&'a [u8]; N] = [&chunk[..0]; N];
        let mut s = start;
        for i in 0..N - 1 {
            out[i] = &chunk[s..seps[i]];
            s = seps[i] + 1;
        }
        out[N - 1] = &chunk[s..end];
        if nq != 0 {
            let mut paired = 0u32;
            for x in out.iter_mut() {
                let n = x.len();
                if n > 0 && x[0] == b'"' {
                    if n < 2 || x[n - 1] != b'"' {
                        return;
                    }
                    *x = &x[1..n - 1];
                    paired += 2;
                }
            }
            if paired != nq {
                return;
            }
        }
        f(out);
    };
    let mut base = 0usize;
    while base < len {
        let (mut m, nl, qm) = if base + 64 <= len {
            masks(chunk[base..base + 64].try_into().unwrap())
        } else {
            let mut buf = [0u8; 64];
            buf[..len - base].copy_from_slice(&chunk[base..]);
            masks(&buf)
        };
        while m != 0 {
            let off = m.trailing_zeros();
            let pos = base + off as usize;
            m &= m - 1;
            if (nl >> off) & 1 == 0 {
                if nsep < N - 1 {
                    seps[nsep] = pos;
                }
                nsep += 1;
            } else {
                let q_before = q_seen + (qm & ((1u64 << off) - 1)).count_ones();
                emit(start, pos, &seps, nsep, q_before - q_line);
                start = pos + 1;
                nsep = 0;
                q_line = q_before;
            }
        }
        q_seen += qm.count_ones();
        base += 64;
    }
    if start < len {
        emit(start, len, &seps, nsep, q_seen - q_line);
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
pub const fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
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

/// Days since 1970 of the first of each month, for years Y0..Y0+NY, and the
/// month lengths; dates outside take the scalar path.
const Y0: i64 = 1900;
const NY: usize = 300;
static MONTH0: [i32; NY * 16] = {
    let mut t = [0i32; NY * 16];
    let mut y = 0;
    while y < NY {
        let mut m = 1;
        while m <= 12 {
            t[y * 16 + m] = days_from_civil(Y0 + y as i64, m as i64, 1) as i32;
            m += 1;
        }
        y += 1;
    }
    t
};
static MONTH_LEN: [u8; NY * 16] = {
    let mut t = [0u8; NY * 16];
    let mut y = 0;
    while y < NY {
        let yy = Y0 + y as i64;
        let leap = (yy % 4 == 0 && yy % 100 != 0) || yy % 400 == 0;
        let mut m = 1;
        while m <= 12 {
            t[y * 16 + m] = match m {
                2 => if leap { 29 } else { 28 },
                4 | 6 | 9 | 11 => 30,
                _ => 31,
            };
            m += 1;
        }
        y += 1;
    }
    t
};

/// 'YYYY-MM-DDTHH:MM:SS' at s[0..19] (s.len() >= 20), strictly validated -> epoch ms (UTC).
#[inline(always)]
fn iso19(s: &[u8]) -> Option<i64> {
    #[cfg(all(target_arch = "x86_64", target_feature = "ssse3", target_feature = "sse4.1"))]
    unsafe {
        use std::arch::x86_64::*;
        debug_assert!(s.len() >= 20);
        // a = s[0..16], b = s[4..20]
        let a = _mm_loadu_si128(s.as_ptr() as *const __m128i);
        let b = _mm_loadu_si128(s.as_ptr().add(4) as *const __m128i);
        let zero = _mm_set1_epi8(b'0' as i8);
        let nine = _mm_set1_epi8(9);
        let ta = _mm_sub_epi8(a, zero);
        let tb = _mm_sub_epi8(b, zero);
        let da = _mm_movemask_epi8(_mm_cmpeq_epi8(_mm_min_epu8(ta, nine), ta)) as u32;
        let db = _mm_movemask_epi8(_mm_cmpeq_epi8(_mm_min_epu8(tb, nine), tb)) as u32;
        let tmpl = _mm_setr_epi8(0, 0, 0, 0, b'-' as i8, 0, 0, b'-' as i8, 0, 0, b'T' as i8, 0, 0, b':' as i8, 0, 0);
        let sa = _mm_movemask_epi8(_mm_cmpeq_epi8(a, tmpl)) as u32;
        // digits at 0-3,5-6,8-9,11-12,14-15; separators at 4,7,10,13; then s[16]=':', s[17..19] digits
        const DA: u32 = 0b1101_1011_0110_1111;
        const SA: u32 = 0b0010_0100_1001_0000;
        if da & DA != DA || sa & SA != SA || db & 0x6000 != 0x6000 || s[16] != b':' {
            return None;
        }
        // pairs (yh, yl, mo, d, h, mi, se, -) as 16-bit lanes
        let pa = _mm_shuffle_epi8(ta, _mm_setr_epi8(0, 1, 2, 3, 5, 6, 8, 9, 11, 12, 14, 15, -1, -1, -1, -1));
        let v = _mm_maddubs_epi16(pa, _mm_setr_epi8(10, 1, 10, 1, 10, 1, 10, 1, 10, 1, 10, 1, 0, 0, 0, 0));
        // seconds in lane 6 (s[17..19] = b[13..15])
        let sec = _mm_maddubs_epi16(_mm_shuffle_epi8(tb, _mm_setr_epi8(-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, 13, 14, -1, -1)), _mm_setr_epi8(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 10, 1, 0, 0));
        let v = _mm_or_si128(v, sec);
        // lanes: yh yl mo d h mi se 0
        let bad = _mm_or_si128(
            _mm_cmpgt_epi16(v, _mm_setr_epi16(99, 99, 12, 31, 23, 59, 59, 0)),
            _mm_cmpgt_epi16(_mm_setr_epi16(0, 0, 1, 1, 0, 0, 0, 0), v),
        );
        if _mm_movemask_epi8(bad) != 0 {
            return None;
        }
        let y = _mm_extract_epi16::<0>(v) as i64 * 100 + _mm_extract_epi16::<1>(v) as i64;
        let mo = _mm_extract_epi16::<2>(v) as usize;
        let d = _mm_extract_epi16::<3>(v) as i64;
        let hms = _mm_madd_epi16(_mm_srli_si128::<8>(v), _mm_setr_epi16(60, 1, 0, 0, 0, 0, 0, 0)); // h*60+mi
        let hm = _mm_cvtsi128_si32(hms) as i64;
        let se = _mm_extract_epi16::<6>(v) as i64;
        let yi = (y - Y0) as usize;
        if yi >= NY {
            return iso19_scalar(s);
        }
        let k = yi * 16 + mo;
        if d > MONTH_LEN[k] as i64 {
            return None;
        }
        return Some(((MONTH0[k] as i64 + d - 1) * 1440 + hm) * 60_000 + se * 1000);
    }
    #[allow(unreachable_code)]
    iso19_scalar(s)
}

/// Reference version of iso19.
fn iso19_scalar(s: &[u8]) -> Option<i64> {
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
#[inline(always)]
pub fn ts_ms(s: &[u8]) -> Option<i64> {
    let n = s.len();
    if n == 20 && s[19] == b'Z' {
        return iso19(s);
    }
    if n == 25 {
        if (s[19] == b'+' || s[19] == b'-') && s[22] == b':' {
            let oh = d2(s, 20)?;
            let om = d2(s, 23)?;
            if oh > 23 || om > 59 {
                return None;
            }
            let off = (oh * 60 + om) * 60_000;
            let base = iso19(s)?;
            return Some(if s[19] == b'+' { base - off } else { base + off });
        }
    }
    if n >= 1 && n <= 16 {
        return digits16(s);
    }
    ts_ms_scalar(s)
}

/// ts_ms with a small cache of validated dates: a line's 'YYYY-MM-DDT' is
/// looked up by its exact bytes, and only 'HH:MM:SS' is parsed (SWAR).
pub struct TsCache {
    e: Box<[(u64, u32, i64); 512]>,
}
impl Default for TsCache {
    fn default() -> Self {
        TsCache { e: Box::new([(0, u32::MAX, 0); 512]) }
    }
}
impl TsCache {
    #[inline(always)]
    pub fn ts(&mut self, s: &[u8]) -> Option<i64> {
        let n = s.len();
        if n == 20 && s[19] == b'Z' {
            return self.iso19(s);
        }
        if n == 25 {
            if (s[19] == b'+' || s[19] == b'-') && s[22] == b':' {
                let oh = d2(s, 20)?;
                let om = d2(s, 23)?;
                if oh > 23 || om > 59 {
                    return None;
                }
                let off = (oh * 60 + om) * 60_000;
                let base = self.iso19(s)?;
                return Some(if s[19] == b'+' { base - off } else { base + off });
            }
        }
        if n >= 1 && n <= 16 {
            return digits16(s);
        }
        ts_ms_scalar(s)
    }

    /// iso19 for s.len() >= 20.
    #[inline(always)]
    pub fn iso19(&mut self, s: &[u8]) -> Option<i64> {
        debug_assert!(s.len() >= 20);
        let a = u64::from_le_bytes(s[0..8].try_into().unwrap());
        let b = u32::from_le_bytes(s[8..12].try_into().unwrap()) & 0x00FF_FFFF;
        let x = u64::from_le_bytes(s[11..19].try_into().unwrap());
        // HH:MM:SS
        const M: u64 = 0xFFFF_00FF_FF00_FFFF;
        const ZERO: u64 = 0x3030_0030_3000_3030;
        const SEP: u64 = 0x0000_3A00_003A_0000;
        let y = (x ^ ZERO) & M;
        let ok_shape = (x & 0xF0F0_FFF0_F0FF_F0F0) == (ZERO | SEP);
        let ok_dig = (y + 0x0606_0006_0600_0606) & 0xF0F0_00F0_F000_F0F0 == 0;
        if !(ok_shape & ok_dig) {
            return None;
        }
        let v = y * 10 + (y >> 8);
        let (h, mi, se) = ((v & 0xFF) as i64, ((v >> 24) & 0xFF) as i64, ((v >> 48) & 0xFF) as i64);
        if (h > 23) | (mi > 59) | (se > 59) {
            return None;
        }
        let tod = (h * 3600 + mi * 60 + se) * 1000;
        let i = ((a ^ (b as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15)).wrapping_mul(0x9E37_79B9_7F4A_7C15) >> 55) as usize;
        let e = unsafe { self.e.get_unchecked_mut(i) };
        if e.0 == a && e.1 == b {
            return Some(e.2 + tod);
        }
        let r = iso19(s)?;
        *e = (a, b, r - tod);
        Some(r)
    }
}

/// 1..=16 ASCII digits -> value; None if any byte is not a digit.
#[inline(always)]
fn digits16(s: &[u8]) -> Option<i64> {
    let n = s.len();
    debug_assert!((1..=16).contains(&n));
    #[cfg(all(target_arch = "x86_64", target_feature = "ssse3", target_feature = "sse4.1"))]
    unsafe {
        use std::arch::x86_64::*;
        let v = if (s.as_ptr() as usize & 4095) <= 4096 - 16 {
            _mm_loadu_si128(s.as_ptr() as *const __m128i)
        } else {
            let mut b = [0u8; 16];
            b[..n].copy_from_slice(s);
            _mm_loadu_si128(b.as_ptr() as *const __m128i)
        };
        let t = _mm_sub_epi8(v, _mm_set1_epi8(b'0' as i8));
        let dm = _mm_movemask_epi8(_mm_cmpeq_epi8(_mm_min_epu8(t, _mm_set1_epi8(9)), t)) as u32;
        let want = (1u32 << n) - 1;
        if dm & want != want {
            return None;
        }
        // right-align the n digits (indexes < 0 select zero)
        let iota = _mm_setr_epi8(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15);
        let r = _mm_shuffle_epi8(t, _mm_add_epi8(iota, _mm_set1_epi8(n as i8 - 16)));
        let x = _mm_maddubs_epi16(r, _mm_setr_epi8(10, 1, 10, 1, 10, 1, 10, 1, 10, 1, 10, 1, 10, 1, 10, 1));
        let x = _mm_madd_epi16(x, _mm_setr_epi16(100, 1, 100, 1, 100, 1, 100, 1));
        let x = _mm_packus_epi32(x, x);
        let x = _mm_madd_epi16(x, _mm_setr_epi16(10000, 1, 10000, 1, 10000, 1, 10000, 1));
        let hi = _mm_cvtsi128_si32(x) as i64;
        let lo = _mm_extract_epi32::<1>(x) as i64;
        return Some(hi * 100_000_000 + lo);
    }
    #[allow(unreachable_code)]
    ts_ms_scalar(s)
}

/// Reference version of ts_ms.
pub fn ts_ms_scalar(s: &[u8]) -> Option<i64> {
    let n = s.len();
    if n == 20 && s[19] == b'Z' {
        return iso19_scalar(s);
    }
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
    if n == 25 && (s[19] == b'+' || s[19] == b'-') && s[22] == b':' {
        let oh = d2(s, 20)?;
        let om = d2(s, 23)?;
        if oh > 23 || om > 59 {
            return None;
        }
        let off = (oh * 60 + om) * 60_000;
        let base = iso19_scalar(s)?;
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

// ---------- IDs as exact, order-preserving 64-bit codes ----------
//
// An ID of 1..=10 characters from [0-9A-Za-z] packs into 10 six-bit digits
// ('0'-'9' -> 1..10, 'A'-'Z' -> 11..36, 'a'-'z' -> 37..62, 0 = padding after
// the end), first character in the high bits. Digit order follows ASCII and
// padding sorts first, so comparing two codes as integers is exactly comparing
// the IDs byte by byte, and equal codes mean equal IDs. Codes use 60 bits.
// Any other non-empty ID becomes FALLBACK | hash and is looked up in a
// registry (which also proves that no two such IDs share a hash).

pub const FALLBACK: u64 = 1 << 63;
const BAD: u8 = 0xFF;
static SYM: [u8; 256] = {
    let mut t = [BAD; 256];
    let mut i = 0;
    while i < 10 {
        t[b'0' as usize + i] = 1 + i as u8;
        i += 1;
    }
    let mut i = 0;
    while i < 26 {
        t[b'A' as usize + i] = 11 + i as u8;
        t[b'a' as usize + i] = 37 + i as u8;
        i += 1;
    }
    t
};
static CHR: [u8; 64] = {
    let mut t = [0u8; 64];
    let mut i = 0;
    while i < 256 {
        if SYM[i] != BAD {
            t[SYM[i] as usize] = i as u8;
        }
        i += 1;
    }
    t
};

/// Packed code of an ID, or None if it needs the fallback (or is empty).
#[inline]
pub fn pack_id(s: &[u8]) -> Option<u64> {
    let n = s.len();
    if n == 0 || n > 10 {
        return None;
    }
    #[cfg(all(target_arch = "x86_64", target_feature = "ssse3", target_feature = "sse4.1"))]
    unsafe {
        use std::arch::x86_64::*;
        // 16 bytes from the ID's start: straight from memory unless that could
        // cross into an unmapped page
        let v = if (s.as_ptr() as usize & 4095) <= 4096 - 16 {
            _mm_loadu_si128(s.as_ptr() as *const __m128i)
        } else {
            let mut b = [0u8; 16];
            b[..n].copy_from_slice(s);
            _mm_loadu_si128(b.as_ptr() as *const __m128i)
        };
        let iota = _mm_setr_epi8(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15);
        let inb = _mm_cmpgt_epi8(_mm_set1_epi8(n as i8), iota);
        let range = |lo: u8, span: u8| {
            let x = _mm_sub_epi8(v, _mm_set1_epi8(lo as i8));
            _mm_cmpeq_epi8(_mm_min_epu8(x, _mm_set1_epi8(span as i8)), x)
        };
        let (isd, isu, isl) = (range(b'0', 9), range(b'A', 25), range(b'a', 25));
        let ok = _mm_or_si128(_mm_or_si128(isd, isu), isl);
        if _mm_movemask_epi8(_mm_or_si128(ok, _mm_xor_si128(inb, _mm_set1_epi8(-1)))) != 0xFFFF {
            return None;
        }
        let off = _mm_or_si128(
            _mm_or_si128(_mm_and_si128(isd, _mm_set1_epi8(0x2F)), _mm_and_si128(isu, _mm_set1_epi8(0x36))),
            _mm_and_si128(isl, _mm_set1_epi8(0x3C)),
        );
        let d = _mm_and_si128(_mm_sub_epi8(v, off), inb);
        let w = _mm_maddubs_epi16(d, _mm_set1_epi16(0x0140)); // d[2j] * 64 + d[2j+1]
        let u = _mm_madd_epi16(w, _mm_set1_epi32(0x0001_1000)); // w[2k] * 4096 + w[2k+1]
        let u0 = _mm_cvtsi128_si32(u) as u32 as u64;
        let u1 = _mm_extract_epi32::<1>(u) as u32 as u64;
        let u2 = _mm_extract_epi32::<2>(u) as u32 as u64;
        return Some(u0 << 36 | u1 << 12 | u2 >> 12);
    }
    #[allow(unreachable_code)]
    pack_id_scalar(s)
}

/// Reference version of pack_id.
pub fn pack_id_scalar(s: &[u8]) -> Option<u64> {
    let n = s.len();
    if n == 0 || n > 10 {
        return None;
    }
    let mut v: u64 = 0;
    let mut bad = 0u8;
    for i in 0..10 {
        let d = if i < n { SYM[s[i] as usize] } else { 0 };
        bad |= (d == BAD) as u8;
        v = v << 6 | d as u64;
    }
    if bad != 0 { None } else { Some(v) }
}

/// The ID bytes of a packed code.
pub fn unpack_id(v: u64, out: &mut [u8; 10]) -> usize {
    let mut n = 0;
    while n < 10 {
        let d = (v >> (6 * (9 - n))) & 63;
        if d == 0 {
            break;
        }
        out[n] = CHR[d as usize];
        n += 1;
    }
    n
}

/// Cheap 64-bit hash: a multiply carries every input bit into the high half,
/// and folding the high half down mixes the low bits too.
#[inline(always)]
pub fn mix(k: u64) -> u64 {
    let x = k.wrapping_mul(0x9E37_79B9_7F4A_7C15);
    x ^ (x >> 32)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn ids() {
        let ids: [&[u8]; 9] = [b"0", b"00", b"9", b"A", b"UAB2YRQYJ", b"Z", b"a", b"u9wteausv", b"zzzzzzzzzz"];
        for w in ids.windows(2) {
            assert!(pack_id(w[0]).unwrap() < pack_id(w[1]).unwrap(), "{:?}", w);
        }
        for s in ids {
            let mut b = [0u8; 10];
            let n = unpack_id(pack_id(s).unwrap(), &mut b);
            assert_eq!(&b[..n], s);
            assert!(pack_id(s).unwrap() < FALLBACK);
        }
        for bad in [&b""[..], b" u1", b"u1 ", b"u_1", b"zzzzzzzzzzz", b"u1\r"] {
            assert_eq!(pack_id(bad), None);
        }
        // SIMD packing agrees with the scalar reference, including at page ends
        let mut page = vec![0u8; 3 * 4096];
        let base = (4096 - (page.as_ptr() as usize & 4095)) & 4095;
        let mut x: u64 = 99;
        let alpha = b"09AZaz_ -azAZ09\r\"/:@[`{";
        for rep in 0..20000 {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            let n = (x % 12) as usize;
            let at = base + 4096 - n - (rep % 3) * (x as usize >> 40 & 7);
            for i in 0..n {
                page[at + i] = alpha[((x >> (i * 5)) % alpha.len() as u64) as usize];
            }
            let s = &page[at..at + n];
            assert_eq!(pack_id(s), pack_id_scalar(s), "{:?}", s);
        }
    }
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
        // the SIMD path agrees with the scalar one on mutated valid timestamps
        let mut x: u64 = 7;
        let base: &[u8] = b"2026-09-07T13:45:59Z";
        let alpha = b"0123456789-T:Z/ 9";
        for _ in 0..200000 {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            let mut t = base.to_vec();
            for k in 0..(x % 3) {
                let i = ((x >> (8 + 8 * k)) % 20) as usize;
                t[i] = alpha[((x >> (30 + 5 * k)) % alpha.len() as u64) as usize];
            }
            if x % 5 == 0 {
                t[0] = b'1' + (x >> 50) as u8 % 3; // other centuries
            }
            assert_eq!(iso19(&t), iso19_scalar(&t), "{}", String::from_utf8_lossy(&t));
        }
        let mut tc = TsCache::default();
        let shapes: [&[u8]; 6] = [b"2026-09-07T13:45:59Z", b"2026-09-07T13:45:59+05:30", b"1789316537000", b"0001789316537000", b"12345678901234567", b"7"];
        let alpha = b"0123456789-T:Z+ 9";
        for _ in 0..300000 {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            let mut t = shapes[(x % 6) as usize].to_vec();
            if x >> 60 == 0 {
                t.truncate(((x >> 20) % (t.len() as u64 + 1)) as usize);
            }
            for k in 0..(x >> 3) % 3 {
                if t.is_empty() {
                    break;
                }
                let i = ((x >> (8 + 8 * k)) % t.len() as u64) as usize;
                t[i] = alpha[((x >> (30 + 5 * k)) % alpha.len() as u64) as usize];
            }
            assert_eq!(ts_ms(&t), ts_ms_scalar(&t), "{}", String::from_utf8_lossy(&t));
            assert_eq!(tc.ts(&t), ts_ms_scalar(&t), "cache {}", String::from_utf8_lossy(&t));
        }
        for y in [1899, 1900, 1970, 2000, 2024, 2100, 2199, 2200, 9999] {
            for (m, d) in [(2, 28), (2, 29), (2, 30), (4, 31), (12, 31), (1, 1), (13, 1), (0, 5)] {
                let t = format!("{y:04}-{m:02}-{d:02}T00:00:00Z");
                assert_eq!(iso19(t.as_bytes()), iso19_scalar(t.as_bytes()), "{t}");
            }
        }
    }
    #[test]
    fn csv() {
        assert_eq!(fields::<2>(b"\"u4\",\"u1\""), Some([&b"u4"[..], b"u1"]));
        assert_eq!(fields::<2>(b"u1,u2,u3"), None);
        assert_eq!(fields::<2>(b"u1,u5\""), None);
        assert_eq!(fields::<2>(b""), None);
        assert_eq!(fields::<2>(b"u1"), None);
        assert_eq!(fields::<2>(b"\"\",x"), Some([&b""[..], b"x"]));
        // the SIMD splitter agrees with `fields` line by line (lines cross 64-byte blocks)
        let mut text = Vec::new();
        let samples: [&[u8]; 13] = [b"\"u4\",\"u1\"", b"u1,u2,u3", b"u1,u5\"", b"", b"u1", b"\"\",x", b"a,\"b\"\"", b" u1 ,u2\r", b"\"a\"b\",c", b"\",x", b"x\",\"y\"", b"\"\"\"", b"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa,b"];
        for r in 0..40 {
            let l = samples[r % samples.len()];
            text.extend_from_slice(l);
            text.push(b'\n');
        }
        text.extend_from_slice(b"last,line");
        let mut want: Vec<[&[u8]; 2]> = Vec::new();
        for_each_line(&text, |l| want.extend(fields::<2>(l)));
        let mut got: Vec<[&[u8]; 2]> = Vec::new();
        for_each_record::<2>(&text, |r| got.push(r));
        assert_eq!(got, want);
        // random text over the dialect's special bytes, against the reference splitter
        let mut x: u64 = 0x1234_5678;
        for len in [0usize, 1, 63, 64, 65, 500, 5000, 40000] {
            for rep in 0..20 {
                let alpha: &[u8] = match rep % 4 {
                    0 => b"ab,\"\n\r",
                    1 => b"abcdefgh,,\"\n",
                    2 => b"abcdefghijklmnopqrstuvwxyz0123456789ABCDEFGHIJ,\n\"",
                    _ => b"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaa,\",\"\n",
                };
                let text: Vec<u8> = (0..len)
                    .map(|_| {
                        x ^= x << 13;
                        x ^= x >> 7;
                        x ^= x << 17;
                        alpha[(x % alpha.len() as u64) as usize]
                    })
                    .collect();
                let mut want: Vec<[&[u8]; 3]> = Vec::new();
                for_each_line(&text, |l| want.extend(fields::<3>(l)));
                let mut got: Vec<[&[u8]; 3]> = Vec::new();
                for_each_record::<3>(&text, |r| got.push(r));
                assert_eq!(got, want, "len {len} rep {rep}");
                let mut want: Vec<[&[u8]; 2]> = Vec::new();
                for_each_line(&text, |l| want.extend(fields::<2>(l)));
                let mut got: Vec<[&[u8]; 2]> = Vec::new();
                for_each_record::<2>(&text, |r| got.push(r));
                assert_eq!(got, want, "len {len} rep {rep}");
            }
        }
        // structured lines: realistic and edge-case fields, every field count
        let mut x: u64 = 0xDEAD_BEEF;
        let mut rnd = move || {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x
        };
        let mut text = Vec::new();
        for _ in 0..30000 {
            let nf = match rnd() % 10 {
                0 => 6,
                1 => 8,
                _ => 7,
            };
            for k in 0..nf {
                if k > 0 {
                    text.push(b',');
                }
                let r = rnd();
                let body: Vec<u8> = match r % 9 {
                    0 => Vec::new(),
                    1 => b"u9wteausv".to_vec(),
                    2 => b"2026-09-16T18:24:20Z".to_vec(),
                    3 => vec![b'a'; (29 + (r >> 8) % 7) as usize], // 29..=35 bytes
                    4 => b"a\"b".to_vec(),
                    5 => b"x,y".to_vec(),
                    6 => b"\r".to_vec(),
                    7 => vec![b'z'; (r >> 8) as usize % 40],
                    _ => b"1789316537000".to_vec(),
                };
                match (r >> 20) % 4 {
                    0 => {
                        text.push(b'"');
                        text.extend_from_slice(&body);
                        text.push(b'"');
                    }
                    1 if r % 31 == 0 => {
                        text.push(b'"');
                        text.extend_from_slice(&body);
                    }
                    _ => text.extend_from_slice(&body),
                }
            }
            text.push(b'\n');
        }
        let mut want: Vec<[&[u8]; 7]> = Vec::new();
        for_each_line(&text, |l| want.extend(fields::<7>(l)));
        let mut got: Vec<[&[u8]; 7]> = Vec::new();
        for_each_record::<7>(&text, |r| got.push(r));
        assert!(want.len() > 1000);
        assert_eq!(got, want);
        assert_eq!(price(b"0250"), Some(250));
        assert_eq!(price(b"10000001"), None);
        assert_eq!(price(b"1000\r"), None);
    }
}
