// Performance benchmark harness for the write path (op-log's own producer,
// worker hot path, write tick, tail scan, flush/shutdown). Not run by
// `cargo test`: every benchmark is `#[ignore]`d and meant for `--release`,
// where the numbers are meaningful — in a debug build they measure the cost
// of not optimizing, not the cost of the code.
//
// Run: cargo test --release -- bench_ --ignored --nocapture --test-threads=1
//
// `--test-threads=1` matters: the benchmarks measure wall-clock time and
// would contend with each other (and with unrelated background load on the
// machine) if run in parallel. See context/runs/2026-08-26-code-review-auto-fix-performance.md
// for the numbers this harness produced and what they were used to decide.
#![allow(clippy::all, dead_code, unused)]

use crate::messages::{OpLogDefinition, OpLogOption, OpLogType};
use crate::{OpLog, OpLogWorker};
use chrono::{DateTime, TimeZone, Utc};
use flate2::Compression;
use flate2::write::ZlibEncoder;
use std::alloc::{GlobalAlloc, Layout, System};
use std::collections::HashSet;
use std::io::Write;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::time::{Duration, Instant};
use tokio::io::AsyncWriteExt;

struct Counting;
static ALLOCS: AtomicU64 = AtomicU64::new(0);
static ALLOC_BYTES: AtomicU64 = AtomicU64::new(0);
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, l: Layout) -> *mut u8 {
        ALLOCS.fetch_add(1, Relaxed);
        ALLOC_BYTES.fetch_add(l.size() as u64, Relaxed);
        unsafe { System.alloc(l) }
    }
    unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
        unsafe { System.dealloc(p, l) }
    }
    unsafe fn realloc(&self, p: *mut u8, l: Layout, n: usize) -> *mut u8 {
        ALLOCS.fetch_add(1, Relaxed);
        ALLOC_BYTES.fetch_add(n as u64, Relaxed);
        unsafe { System.realloc(p, l, n) }
    }
}
#[global_allocator]
static A: Counting = Counting;

fn allocs() -> (u64, u64) {
    (ALLOCS.load(Relaxed), ALLOC_BYTES.load(Relaxed))
}

fn tmp(tag: &str) -> PathBuf {
    let d = std::env::temp_dir().join(format!("op-log-bench-{tag}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).unwrap();
    d
}

fn fmt(d: Duration) -> String {
    let us = d.as_secs_f64() * 1e6;
    if us < 1000.0 {
        format!("{us:.1}us")
    } else if us < 1e6 {
        format!("{:.2}ms", us / 1000.0)
    } else {
        format!("{:.3}s", us / 1e6)
    }
}

fn stats(label: &str, runs: &[Duration], per: Option<(u64, &str)>) {
    let mut v = runs.to_vec();
    v.sort();
    let min = v[0];
    let med = v[v.len() / 2];
    let max = v[v.len() - 1];
    let spread = (max.as_secs_f64() - min.as_secs_f64()) / med.as_secs_f64() * 100.0;
    let mut line = format!(
        "  {label:<58} min {:>9}  med {:>9}  max {:>9}  spread {spread:>5.1}%",
        fmt(min),
        fmt(med),
        fmt(max)
    );
    if let Some((n, unit)) = per {
        let ns = med.as_secs_f64() * 1e9 / n as f64;
        let rate = n as f64 / med.as_secs_f64();
        line += &format!("  | {ns:>9.1} ns/{unit}  {rate:>12.0} {unit}/s");
    }
    println!("{line}");
}

const WORDS: &[&str] = &[
    "request", "response", "user", "session", "token", "cache", "miss", "hit", "timeout", "retry",
    "queue", "worker", "shard", "primary", "replica", "commit", "rollback", "index", "scan",
    "merge", "alpha", "bravo", "charlie", "delta", "echo", "foxtrot", "golf", "hotel", "india",
    "juliet",
];

/// Realistic log line of roughly `len` bytes: numbers vary, vocabulary repeats.
fn line(i: usize, len: usize) -> String {
    let mut s = format!(
        "GET /api/v1/users/{}/profile status={} elapsed={}ms ip=10.0.{}.{} rid={:08x} ",
        i * 7919 % 100_000,
        if i % 17 == 0 { 500 } else { 200 },
        i * 31 % 900,
        i % 256,
        (i * 13) % 256,
        (i as u64).wrapping_mul(0x9E3779B97F4A7C15) >> 32
    );
    let mut k = i;
    while s.len() < len {
        k = k.wrapping_mul(1_103_515_245).wrapping_add(12_345);
        s.push_str(WORDS[(k >> 8) % WORDS.len()]);
        s.push(' ');
    }
    s.truncate(len);
    s
}

fn worker(
    dir: &std::path::Path,
    t: OpLogType,
    opts: HashSet<OpLogOption>,
    flush: Duration,
) -> OpLogWorker {
    let (tx, rx) = tokio::sync::mpsc::channel(32);
    let mut w = OpLogWorker::new(rx);
    std::mem::forget(tx);
    w.def(
        OpLogDefinition::new("app", dir.to_str().unwrap())
            .log_type(t)
            .options(opts)
            .flush_interval(flush)
            .header("ts, message"),
    );
    w
}

const REPS: usize = 5;

// ---------------------------------------------------------------- 1. producer
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn bench_1_producer() {
    let dir = tmp("producer");
    let text = line(1, 100);
    println!("\n=== 1. producer: OpLog::log() (multi_thread rt, 4 workers) ===");

    // (a) burst of 1000 into an empty channel (all accepted), name undefined -> worker discards fast
    let op = OpLog::new();
    let mut runs = vec![];
    let mut al = vec![];
    for _ in 0..REPS {
        tokio::time::sleep(Duration::from_millis(50)).await; // let the worker drain
        let a0 = allocs().0;
        let t = Instant::now();
        for _ in 0..1000 {
            op.log("nobody", Utc::now(), &text);
        }
        runs.push(t.elapsed());
        al.push(allocs().0 - a0);
    }
    stats(
        "burst 1000 (channel has room), 1 thread",
        &runs,
        Some((1000, "call")),
    );
    println!(
        "    allocs/call: {:?}",
        al.iter().map(|a| *a as f64 / 1000.0).collect::<Vec<_>>()
    );

    // (b) sustained 1M from one thread, undefined name (worker discards): includes drops
    let mut runs = vec![];
    for _ in 0..3 {
        let t = Instant::now();
        for _ in 0..1_000_000 {
            op.log("nobody", Utc::now(), &text);
        }
        runs.push(t.elapsed());
    }
    let dropped = op.0.dropped_logs.load(Relaxed);
    stats(
        "sustained 1M, 1 thread, undefined name",
        &runs,
        Some((1_000_000, "call")),
    );
    println!("    dropped so far (channel full): {dropped}");

    // (c) sustained with a defined name (worker does real work + queue cap), 1 thread
    op.def(
        OpLogDefinition::new("app", dir.to_str().unwrap())
            .flush_interval(Duration::from_secs(3600)),
    );
    tokio::time::sleep(Duration::from_millis(50)).await;
    let d0 = op.0.dropped_logs.load(Relaxed);
    let mut runs = vec![];
    for _ in 0..3 {
        let t = Instant::now();
        for _ in 0..1_000_000 {
            op.log("app", Utc::now(), &text);
        }
        runs.push(t.elapsed());
    }
    let d1 = op.0.dropped_logs.load(Relaxed);
    stats(
        "sustained 1M, 1 thread, defined name",
        &runs,
        Some((1_000_000, "call")),
    );
    println!(
        "    dropped during 3M calls: {} ({:.1}%)",
        d1 - d0,
        (d1 - d0) as f64 / 3e6 * 100.0
    );

    // (d) 4 threads x 250k, defined name
    let mut runs = vec![];
    for _ in 0..3 {
        let t = Instant::now();
        std::thread::scope(|s| {
            for _ in 0..4 {
                let op = op.clone();
                let text = &text;
                s.spawn(move || {
                    for _ in 0..250_000 {
                        op.log("app", Utc::now(), text);
                    }
                });
            }
        });
        runs.push(t.elapsed());
    }
    stats(
        "4 threads x 250k, defined name (wall)",
        &runs,
        Some((1_000_000, "call")),
    );

    // (e) 4 threads, undefined name
    let mut runs = vec![];
    for _ in 0..3 {
        let t = Instant::now();
        std::thread::scope(|s| {
            for _ in 0..4 {
                let op = op.clone();
                let text = &text;
                s.spawn(move || {
                    for _ in 0..250_000 {
                        op.log("nobody", Utc::now(), text);
                    }
                });
            }
        });
        runs.push(t.elapsed());
    }
    stats(
        "4 threads x 250k, undefined name (wall)",
        &runs,
        Some((1_000_000, "call")),
    );

    // (f) components: Utc::now, 2x to_string
    let mut runs = vec![];
    for _ in 0..REPS {
        let t = Instant::now();
        for _ in 0..1_000_000 {
            std::hint::black_box(Utc::now());
        }
        runs.push(t.elapsed());
    }
    stats("component: Utc::now()", &runs, Some((1_000_000, "call")));
    let mut runs = vec![];
    for _ in 0..REPS {
        let t = Instant::now();
        for _ in 0..1_000_000 {
            std::hint::black_box(("app".to_string(), text.to_string()));
        }
        runs.push(t.elapsed());
    }
    stats(
        "component: 2x to_string (name + 100 B text)",
        &runs,
        Some((1_000_000, "call")),
    );

    op.shutdown().await;
    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------- 2. worker hot path
#[tokio::test]
#[ignore]
async fn bench_2_worker_log() {
    let dir = tmp("worker");
    println!("\n=== 2. worker hot path: OpLogWorker::log() (current_thread) ===");
    let date = Utc.with_ymd_and_hms(2026, 8, 26, 12, 34, 56).unwrap();
    let cases: Vec<(&str, OpLogType, HashSet<OpLogOption>, usize, usize)> = vec![
        (
            "PerDay, 100 B",
            OpLogType::PerDay,
            HashSet::new(),
            100,
            50_000,
        ),
        (
            "PerDay, 1 KB",
            OpLogType::PerDay,
            HashSet::new(),
            1024,
            5_000,
        ),
        (
            "PerDay+UseSubDirectories, 100 B",
            OpLogType::PerDay,
            HashSet::from([OpLogOption::UseSubDirectories]),
            100,
            50_000,
        ),
        (
            "PerHour, 100 B",
            OpLogType::PerHour,
            HashSet::new(),
            100,
            50_000,
        ),
        (
            "NoSplit, 100 B",
            OpLogType::NoSplit,
            HashSet::new(),
            100,
            50_000,
        ),
        (
            "PerDay+NoAddDateToLog, 100 B",
            OpLogType::PerDay,
            HashSet::from([OpLogOption::NoAddDateToLog]),
            100,
            50_000,
        ),
    ];
    for (label, t, opts, len, n) in cases {
        let texts: Vec<String> = (0..n).map(|i| line(i, len)).collect();
        let mut runs = vec![];
        let mut al = vec![];
        let mut ab = vec![];
        for _ in 0..REPS {
            let mut w = worker(&dir, t, opts.clone(), Duration::from_secs(3600));
            let (a0, b0) = allocs();
            let ti = Instant::now();
            for s in &texts {
                w.log("app", date, s);
            }
            runs.push(ti.elapsed());
            let (a1, b1) = allocs();
            al.push((a1 - a0) as f64 / n as f64);
            ab.push((b1 - b0) as f64 / n as f64);
            assert_eq!(
                w.definitions["app"]
                    .files
                    .values()
                    .map(|f| f.logs.len())
                    .sum::<usize>(),
                n
            );
        }
        stats(
            &format!("{label} (n={n})"),
            &runs,
            Some((n as u64, "entry")),
        );
        println!(
            "    allocs/entry: {:.2}  alloc bytes/entry: {:.0}",
            al[0], ab[0]
        );
    }

    // components
    println!("  -- components (PerDay, 100 B) --");
    let texts: Vec<String> = (0..50_000).map(|i| line(i, 100)).collect();
    let mut runs = vec![];
    for _ in 0..REPS {
        let ti = Instant::now();
        for _ in 0..50_000 {
            std::hint::black_box(date.with_timezone(&chrono_tz::Europe::Warsaw));
        }
        runs.push(ti.elapsed());
    }
    stats("with_timezone(Warsaw)", &runs, Some((50_000, "entry")));
    let wd = date.with_timezone(&chrono_tz::Europe::Warsaw);
    let mut runs = vec![];
    let a0 = allocs().0;
    for _ in 0..REPS {
        let ti = Instant::now();
        for s in &texts {
            std::hint::black_box(crate::add_to_log::format_entry(
                &OpLogType::PerDay,
                false,
                &wd,
                s,
            ));
        }
        runs.push(ti.elapsed());
    }
    stats(
        "format_entry (chrono %H:%M:%S.%3f + format!)",
        &runs,
        Some((50_000, "entry")),
    );
    println!(
        "    allocs/entry: {:.2}",
        (allocs().0 - a0) as f64 / (50_000 * REPS) as f64
    );
    let mut runs = vec![];
    let a0 = allocs().0;
    for _ in 0..REPS {
        let ti = Instant::now();
        for _ in 0..50_000 {
            let d = wd.format("%Y_%m_%d");
            std::hint::black_box((format!("{}_{}.log", "app", d), "logs".to_string()));
        }
        runs.push(ti.elapsed());
    }
    stats(
        "log_name format! + path.to_string()",
        &runs,
        Some((50_000, "entry")),
    );
    println!(
        "    allocs/entry: {:.2}",
        (allocs().0 - a0) as f64 / (50_000 * REPS) as f64
    );
    let mut runs = vec![];
    let a0 = allocs().0;
    for _ in 0..REPS {
        let ti = Instant::now();
        for _ in 0..50_000 {
            std::hint::black_box(format!("{}/{}", "logs", "app_2026_08_26.log"));
        }
        runs.push(ti.elapsed());
    }
    stats(
        "file_name key format! (get_log_file)",
        &runs,
        Some((50_000, "entry")),
    );
    println!(
        "    allocs/entry: {:.2}",
        (allocs().0 - a0) as f64 / (50_000 * REPS) as f64
    );
    let mut runs = vec![];
    for _ in 0..REPS {
        let ti = Instant::now();
        for _ in 0..50_000 {
            std::hint::black_box(tokio::time::Instant::now());
        }
        runs.push(ti.elapsed());
    }
    stats(
        "tokio Instant::now() (x3 per entry)",
        &runs,
        Some((50_000, "call")),
    );

    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------- 3. write
async fn measure_write(
    label: &str,
    dir: &std::path::Path,
    t: OpLogType,
    n: usize,
    len: usize,
    reps: usize,
) {
    let date = Utc::now();
    let texts: Vec<String> = (0..n).map(|i| line(i, len)).collect();
    let mut runs = vec![];
    let mut al = vec![];
    let mut ticks = 0;
    let mut frames = 0u64;
    let mut file_len = 0u64;
    let mut raw_total = 0usize;
    for r in 0..reps {
        let sub = dir.join(format!("{}", r));
        let mut w = worker(&sub, t, HashSet::new(), Duration::from_millis(0));
        for s in &texts {
            w.log("app", date, s);
        }
        raw_total = w.definitions["app"]
            .files
            .values()
            .map(|f| f.queued_bytes + f.logs.len())
            .sum();
        let a0 = allocs().0;
        let ti = Instant::now();
        ticks = 0;
        loop {
            w.write_to_files().await;
            ticks += 1;
            if w.definitions["app"]
                .files
                .values()
                .all(|f| f.logs.is_empty())
            {
                break;
            }
        }
        runs.push(ti.elapsed());
        al.push(allocs().0 - a0);
        let f = w.definitions["app"].files.values().next().unwrap();
        assert!(!f.write_error_logged, "write failed");
        let p = std::path::Path::new(&f.path).join(&f.log_name);
        let raw = std::fs::read(&p).unwrap();
        file_len = raw.len() as u64;
        frames = count_frames(&raw);
    }
    stats(label, &runs, Some((n as u64, "entry")));
    println!(
        "    ticks={ticks} frames={frames} raw={raw_total} B -> file={file_len} B (ratio {:.2}x)  allocs/tick={:.0}",
        raw_total as f64 / file_len as f64,
        al[0] as f64 / ticks as f64
    );
}

fn count_frames(raw: &[u8]) -> u64 {
    let mut pos = 10;
    let mut n = 0;
    while pos < raw.len() {
        assert_eq!(raw[pos], 0xff);
        pos += 3;
        let mut size = 0usize;
        let mut shift = 0;
        loop {
            let b = raw[pos] ^ 0xc5;
            pos += 1;
            size |= ((b & 0x7f) as usize) << shift;
            if b & 0x80 == 0 {
                break;
            }
            shift += 7;
        }
        pos += size;
        n += 1;
    }
    n
}

#[tokio::test]
#[ignore]
async fn bench_3_write() {
    let dir = tmp("write");
    println!(
        "\n=== 3. write: write_to_files() end to end (current_thread; NoSplit unless noted) ==="
    );
    measure_write(
        "typical: 1 x 200 B (one entry per 10 s tick)",
        &dir.join("t1"),
        OpLogType::NoSplit,
        1,
        200,
        REPS,
    )
    .await;
    measure_write(
        "typical: 50 x 200 B (10 KB per tick)",
        &dir.join("t50"),
        OpLogType::NoSplit,
        50,
        200,
        REPS,
    )
    .await;
    measure_write(
        "typical: 200 x 1 KB (200 KB per tick)",
        &dir.join("t200"),
        OpLogType::NoSplit,
        200,
        1024,
        REPS,
    )
    .await;
    measure_write(
        "typical: 500 x 4 KB (2 MB per tick)",
        &dir.join("t500"),
        OpLogType::NoSplit,
        500,
        4096,
        3,
    )
    .await;
    measure_write(
        "peak: 10 000 x 1 KB (10 MB queued)",
        &dir.join("peak"),
        OpLogType::NoSplit,
        10_000,
        1024,
        3,
    )
    .await;
    measure_write(
        "peak PerDay: 10 000 x 1 KB",
        &dir.join("peakday"),
        OpLogType::PerDay,
        10_000,
        1024,
        3,
    )
    .await;

    // ---- breakdown of one 10 000 x 1 KB tick: zlib / xor / io
    println!("  -- breakdown, 10 000 x 1 KB, in-memory parts --");
    let date = Utc::now().with_timezone(&chrono_tz::Europe::Warsaw);
    let entries: Vec<String> = (0..10_000)
        .map(|i| crate::add_to_log::format_entry(&OpLogType::PerDay, false, &date, &line(i, 1024)))
        .collect();
    for (label, chunk_cap) in [
        (
            "zlib level 6, 64 000 B chunks, 2 MB cap (as written)",
            2 * 1024 * 1024usize,
        ),
        ("zlib level 6, no cap (all 10 MB)", usize::MAX),
    ] {
        let mut runs = vec![];
        let mut out_len = 0;
        let mut consumed_n = 0;
        for _ in 0..REPS {
            let ti = Instant::now();
            let mut bytes: Vec<u8> = Vec::with_capacity(1024);
            let mut enc = ZlibEncoder::new(Vec::new(), Compression::default());
            let mut consumed = 0;
            loop {
                bytes.clear();
                while let Some(l) = entries.get(consumed) {
                    bytes.extend_from_slice(l.as_bytes());
                    bytes.push(0x0a);
                    consumed += 1;
                    if bytes.len() > 64000 {
                        break;
                    }
                }
                if bytes.is_empty() {
                    break;
                }
                enc.write_all(&bytes).unwrap();
                if enc.get_ref().len() > chunk_cap {
                    break;
                }
            }
            let a = enc.finish().unwrap();
            out_len = a.len();
            consumed_n = consumed;
            std::hint::black_box(a);
            runs.push(ti.elapsed());
        }
        stats(label, &runs, Some((consumed_n as u64, "entry")));
        println!("    consumed {consumed_n} entries -> {out_len} B compressed");
    }
    for lvl in 0u32..=9 {
        let mut runs = vec![];
        let mut out_len = 0;
        for _ in 0..3 {
            let ti = Instant::now();
            let mut enc = ZlibEncoder::new(Vec::new(), Compression::new(lvl));
            for l in &entries {
                enc.write_all(l.as_bytes()).unwrap();
                enc.write_all(b"\n").unwrap();
            }
            let a = enc.finish().unwrap();
            out_len = a.len();
            std::hint::black_box(a);
            runs.push(ti.elapsed());
        }
        stats(
            &format!("zlib level {lvl}, all 10 MB (reference only)"),
            &runs,
            Some((10_000, "entry")),
        );
        println!("    -> {out_len} B");
    }
    // XOR loop over a 2 MB buffer
    let mut buf = vec![0u8; 2 * 1024 * 1024];
    for (i, b) in buf.iter_mut().enumerate() {
        *b = (i * 31 % 251) as u8;
    }
    let mut runs = vec![];
    for _ in 0..REPS {
        let mut a = buf.clone();
        let ti = Instant::now();
        let size = a.len();
        let rnd: u8 = 77;
        let mut sum = 0u32;
        let mut xor: u32 = (rnd as u32 * size as u32) & 0xFFF;
        for byte in a.iter_mut() {
            sum += *byte as u32;
            sum &= 0xff;
            xor *= 2903;
            xor += 71;
            xor &= 0xfff;
            *byte ^= (xor & 0xff) as u8;
        }
        std::hint::black_box((a, sum));
        runs.push(ti.elapsed());
    }
    stats(
        "XOR+checksum loop over 2 MB",
        &runs,
        Some((2 * 1024 * 1024, "byte")),
    );

    // ---- I/O round trips, tokio::fs vs std::fs
    println!("  -- I/O round trips (tokio::fs = spawn_blocking) on NTFS, existing file, warm --");
    let iod = dir.join("io");
    std::fs::create_dir_all(&iod).unwrap();
    let p = iod.join("app.log");
    std::fs::write(&p, b"OPLog 1.0\n").unwrap();
    let small = vec![0xAAu8; 8];
    let mid = vec![0x55u8; 300 * 1024];
    let big = vec![0x55u8; 2 * 1024 * 1024];
    macro_rules! io_bench {
        ($label:expr, $n:expr, $body:expr) => {{
            let mut runs = vec![];
            for _ in 0..REPS {
                let ti = Instant::now();
                for _ in 0..$n {
                    $body;
                }
                runs.push(ti.elapsed());
            }
            stats($label, &runs, Some(($n, "op")));
        }};
    }
    io_bench!("tokio create_dir_all (exists)", 200, {
        tokio::fs::create_dir_all(&iod).await.unwrap()
    });
    io_bench!("std  create_dir_all (exists)", 200, {
        std::fs::create_dir_all(&iod).unwrap()
    });
    io_bench!("tokio metadata", 200, {
        tokio::fs::metadata(&p).await.unwrap()
    });
    io_bench!("std  metadata", 200, { std::fs::metadata(&p).unwrap() });
    io_bench!("tokio open(append)", 200, {
        tokio::fs::File::options()
            .append(true)
            .open(&p)
            .await
            .unwrap()
    });
    io_bench!("std  open(append)", 200, {
        std::fs::File::options().append(true).open(&p).unwrap()
    });
    io_bench!("tokio open+write_all(8 B)+flush (+drop)", 200, {
        let mut f = tokio::fs::File::options()
            .append(true)
            .open(&p)
            .await
            .unwrap();
        f.write_all(&small).await.unwrap();
        f.flush().await.unwrap();
    });
    io_bench!("tokio open+write 8 B+write 300 KB+flush", 100, {
        let mut f = tokio::fs::File::options()
            .append(true)
            .open(&p)
            .await
            .unwrap();
        f.write_all(&small).await.unwrap();
        f.write_all(&mid).await.unwrap();
        f.flush().await.unwrap();
    });
    std::fs::write(&p, b"OPLog 1.0\n").unwrap();
    io_bench!("tokio open+write 8 B+write 2 MB+flush", 20, {
        let mut f = tokio::fs::File::options()
            .append(true)
            .open(&p)
            .await
            .unwrap();
        f.write_all(&small).await.unwrap();
        f.write_all(&big).await.unwrap();
        f.flush().await.unwrap();
    });
    std::fs::write(&p, b"OPLog 1.0\n").unwrap();
    io_bench!("std   open+write 8 B+write 2 MB+flush", 20, {
        let mut f = std::fs::File::options().append(true).open(&p).unwrap();
        f.write_all(&small).unwrap();
        f.write_all(&big).unwrap();
        f.flush().unwrap();
    });
    std::fs::write(&p, b"OPLog 1.0\n").unwrap();
    io_bench!("tokio spawn_blocking(|| ()) round trip", 1000, {
        tokio::task::spawn_blocking(|| ()).await.unwrap()
    });
    io_bench!(
        "tokio cut_interrupted_tail (tiny file, spawn_blocking)",
        200,
        { super::cut_interrupted_tail(&p).await.unwrap() }
    );
    io_bench!(
        "full sequence: create_dir_all+metadata+open+2 writes(8 B, 300 KB)+flush",
        100,
        {
            tokio::fs::create_dir_all(&iod).await.unwrap();
            let _ = tokio::fs::metadata(&p).await.unwrap();
            let mut f = tokio::fs::File::options()
                .append(true)
                .open(&p)
                .await
                .unwrap();
            f.write_all(&small).await.unwrap();
            f.write_all(&mid).await.unwrap();
            f.flush().await.unwrap();
        }
    );
    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------- 4. tail scan
fn synth_file(path: &std::path::Path, target: u64, payload: usize) -> u64 {
    let mut out = Vec::with_capacity(target as usize + payload + 16);
    out.extend_from_slice(b"OPLog 1.0\n");
    let mut k = 1u32;
    let mut frames = 0;
    while (out.len() as u64) < target {
        out.extend_from_slice(&[0xff, 0x01, 0x02]);
        let mut rest = payload;
        loop {
            let mut a = (rest & 0x7f) as u8;
            rest >>= 7;
            if rest != 0 {
                a |= 0x80;
            }
            out.push(a ^ 0xc5);
            if rest == 0 {
                break;
            }
        }
        for _ in 0..payload {
            k = k.wrapping_mul(1_103_515_245).wrapping_add(12_345);
            out.push((k >> 16) as u8);
        }
        frames += 1;
    }
    std::fs::write(path, &out).unwrap();
    frames
}

#[test]
#[ignore]
fn bench_4_tail_scan() {
    let dir = tmp("tail");
    println!("\n=== 4. cut_interrupted_tail_up_to (blocking; file warm in page cache) ===");
    for (mb, payload) in [
        (1u64, 10_000usize),
        (1, 200),
        (64, 10_000),
        (64, 200),
        (64, 100_000),
    ] {
        let p = dir.join(format!("f{mb}_{payload}.log"));
        let frames = synth_file(&p, mb * 1024 * 1024, payload);
        let mut runs = vec![];
        for _ in 0..REPS {
            let ti = Instant::now();
            super::cut_interrupted_tail_up_to(&p, u64::MAX).unwrap();
            runs.push(ti.elapsed());
        }
        stats(
            &format!("{mb} MB, {payload} B payload/frame ({frames} frames)"),
            &runs,
            Some((frames, "frame")),
        );
    }
    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------- 5. flush / shutdown
#[tokio::test]
#[ignore]
async fn bench_5_flush_shutdown() {
    let dir = tmp("flush");
    println!("\n=== 5. flush() / shutdown() ===");
    // worker flush, empty queue (1 def, 1 file already created)
    let mut runs = vec![];
    for r in 0..REPS {
        let mut w = worker(
            &dir.join(format!("e{r}")),
            OpLogType::NoSplit,
            HashSet::new(),
            Duration::from_millis(0),
        );
        w.log("app", Utc::now(), "x");
        w.write_to_files().await;
        let ti = Instant::now();
        w.flush().await;
        runs.push(ti.elapsed());
    }
    stats("OpLogWorker::flush(), empty queue, 1 file", &runs, None);
    let mut runs = vec![];
    for r in 0..REPS {
        let mut w = worker(
            &dir.join(format!("n{r}")),
            OpLogType::NoSplit,
            HashSet::new(),
            Duration::from_millis(0),
        );
        let ti = Instant::now();
        w.flush().await;
        runs.push(ti.elapsed());
    }
    stats("OpLogWorker::flush(), no files at all", &runs, None);
    // idle write tick with 3 definitions, 1 file each, empty
    let mut runs = vec![];
    for r in 0..REPS {
        let (_tx, rx) = tokio::sync::mpsc::channel(32);
        let mut w = OpLogWorker::new(rx);
        for name in ["log", "ai", "api"] {
            w.def(
                OpLogDefinition::new(name, dir.join(format!("i{r}")).to_str().unwrap())
                    .flush_interval(Duration::from_millis(0)),
            );
            w.log(name, Utc::now(), "x");
        }
        w.write_to_files().await;
        let ti = Instant::now();
        for _ in 0..1000 {
            w.write_to_files().await;
        }
        runs.push(ti.elapsed());
    }
    stats(
        "idle write tick, 3 defs x 1 file, empty queues",
        &runs,
        Some((1000, "tick")),
    );
    // worker flush with 10 000 x 1 KB
    let texts: Vec<String> = (0..10_000).map(|i| line(i, 1024)).collect();
    let mut runs = vec![];
    for r in 0..3 {
        let mut w = worker(
            &dir.join(format!("f{r}")),
            OpLogType::NoSplit,
            HashSet::new(),
            Duration::from_secs(10),
        );
        for s in &texts {
            w.log("app", Utc::now(), s);
        }
        let ti = Instant::now();
        w.flush().await;
        runs.push(ti.elapsed());
        assert_eq!(w.log_count(), 0);
    }
    stats("OpLogWorker::flush(), 10 000 x 1 KB queued", &runs, None);
    // flush during an error episode (blocked path): 5 s deadline loop
    {
        let blocker = dir.join("blocker");
        std::fs::write(&blocker, b"file").unwrap();
        let mut w = worker(
            &blocker.join("logs"),
            OpLogType::NoSplit,
            HashSet::new(),
            Duration::from_secs(10),
        );
        for s in &texts[..1000] {
            w.log("app", Utc::now(), s);
        }
        let a0 = allocs().0;
        let ti = Instant::now();
        w.flush().await;
        let el = ti.elapsed();
        println!(
            "  {:<58} {} ({} allocs; deadline loop, 1000 x 1 KB re-encoded each pass)",
            "OpLogWorker::flush() during write error episode",
            fmt(el),
            allocs().0 - a0
        );
    }
    // handle shutdown
    let mut runs = vec![];
    for r in 0..3 {
        let op = OpLog::new();
        op.def(
            OpLogDefinition::new("app", dir.join(format!("s{r}")).to_str().unwrap())
                .flush_interval(Duration::from_secs(10)),
        );
        tokio::task::yield_now().await;
        let ti = Instant::now();
        op.shutdown().await;
        runs.push(ti.elapsed());
    }
    stats("OpLog::shutdown(), empty", &runs, None);
    let mut runs = vec![];
    for r in 0..3 {
        let op = OpLog::new();
        op.def(
            OpLogDefinition::new("app", dir.join(format!("sf{r}")).to_str().unwrap())
                .flush_interval(Duration::from_secs(10)),
        );
        for s in &texts[..1000] {
            op.log("app", Utc::now(), s);
        }
        let ti = Instant::now();
        op.shutdown().await;
        runs.push(ti.elapsed());
    }
    stats(
        "OpLog::shutdown(), 1000 x 1 KB in channel (cap 1024)",
        &runs,
        None,
    );
    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------- 6. format variants (candidate C1)
mod fmtv {
    use chrono::format::{Fixed, Item, Numeric, Pad, StrftimeItems};
    use chrono::{DateTime, Utc};
    use chrono_tz::Tz;
    use std::fmt::Write as _;

    pub const LOG_PERDAY: &[Item<'static>] = &[
        Item::Numeric(Numeric::Hour, Pad::Zero),
        Item::Literal(":"),
        Item::Numeric(Numeric::Minute, Pad::Zero),
        Item::Literal(":"),
        Item::Numeric(Numeric::Second, Pad::Zero),
        Item::Fixed(Fixed::Nanosecond3),
    ];
    pub const PATH_PERDAY: &[Item<'static>] = &[
        Item::Numeric(Numeric::Year, Pad::Zero),
        Item::Literal("_"),
        Item::Numeric(Numeric::Month, Pad::Zero),
        Item::Literal("_"),
        Item::Numeric(Numeric::Day, Pad::Zero),
    ];

    pub fn v0(date: &DateTime<Tz>, text: &str) -> String {
        format!("{} {text}", date.format("%H:%M:%S.%3f"))
    }
    pub fn v1(date: &DateTime<Tz>, text: &str) -> String {
        let mut s = String::with_capacity(12 + 1 + text.len());
        date.format("%H:%M:%S.%3f")
            .write_to(&mut s)
            .expect("constant format");
        s.push(' ');
        s.push_str(text);
        s
    }
    pub fn v2(date: &DateTime<Tz>, text: &str) -> String {
        let mut s = String::with_capacity(12 + 1 + text.len());
        date.format_with_items(LOG_PERDAY.iter())
            .write_to(&mut s)
            .expect("constant format");
        s.push(' ');
        s.push_str(text);
        s
    }
    pub fn v1w(date: &DateTime<Tz>, text: &str) -> String {
        // write! with Display (still the intermediate String inside chrono)
        let mut s = String::with_capacity(12 + 1 + text.len());
        write!(s, "{} {text}", date.format("%H:%M:%S.%3f")).unwrap();
        s
    }
    pub fn p0(name: &str, date: &DateTime<Tz>) -> String {
        format!("{}_{}.log", name, date.format("%Y_%m_%d"))
    }
    pub fn p2(name: &str, date: &DateTime<Tz>) -> String {
        let mut s = String::with_capacity(name.len() + 1 + 10 + 4);
        s.push_str(name);
        s.push('_');
        date.format_with_items(PATH_PERDAY.iter())
            .write_to(&mut s)
            .expect("constant format");
        s.push_str(".log");
        s
    }
    pub fn items_equal() {
        let parsed: Vec<Item> = StrftimeItems::new("%H:%M:%S.%3f").collect();
        println!(
            "    %H:%M:%S.%3f parses to {parsed:?} (Nanosecond3NoDot is chrono-internal; public Nanosecond3 prints the dot itself)"
        );
        let parsed: Vec<Item> = StrftimeItems::new("%Y_%m_%d").collect();
        assert_eq!(
            parsed,
            PATH_PERDAY.to_vec(),
            "path items differ from strftime parse"
        );
        let parsed: Vec<Item> = StrftimeItems::new("%d %H:%M:%S.%3f").collect();
        println!("    %d %H:%M:%S.%3f parses to {parsed:?}");
        let parsed: Vec<Item> = StrftimeItems::new("%Y_%m_%d_%H").collect();
        println!("    %Y_%m_%d_%H parses to {parsed:?}");
    }
}

#[test]
#[ignore]
fn bench_6_format_variants() {
    use chrono::TimeZone;
    println!("\n=== 6. candidate C1: format_entry / file name formatting variants ===");
    fmtv::items_equal();
    let date = Utc
        .with_ymd_and_hms(2026, 8, 26, 12, 34, 56)
        .unwrap()
        .with_timezone(&chrono_tz::Europe::Warsaw);
    let texts: Vec<String> = (0..50_000).map(|i| line(i, 100)).collect();
    macro_rules! m {
        ($label:expr, $f:expr) => {{
            let mut runs = vec![];
            let a0 = allocs().0;
            for _ in 0..REPS {
                let ti = Instant::now();
                for s in &texts {
                    std::hint::black_box($f(&date, s));
                }
                runs.push(ti.elapsed());
            }
            stats($label, &runs, Some((50_000, "entry")));
            println!(
                "    allocs/entry: {:.2}",
                (allocs().0 - a0) as f64 / (50_000 * REPS) as f64
            );
        }};
    }
    m!(
        "v0 current: format!(\"{} {text}\", date.format(F))",
        fmtv::v0
    );
    m!(
        "v1w: write!(String::with_capacity, \"{} {text}\", ..)",
        fmtv::v1w
    );
    m!(
        "v1: date.format(F).write_to(&mut String::with_capacity)",
        fmtv::v1
    );
    m!("v2: v1 + const Item slice (format_with_items)", fmtv::v2);
    m!(
        "p0 current: format!(\"{}_{}.log\", name, date.format(P))",
        |d, _s| fmtv::p0("app", d)
    );
    m!("p2: push_str + write_to + const items", |d, _s| fmtv::p2(
        "app", d
    ));

    // byte-for-byte proof over many dates: hourly 2024..2027 (covers DST), leap-second nanos, range extremes
    let mut dates: Vec<DateTime<Utc>> = vec![];
    let start = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();
    for h in 0..(4 * 366 * 24) {
        dates.push(start + chrono::Duration::hours(h) + chrono::Duration::milliseconds(h % 1000));
    }
    for m in 0..3600 {
        dates.push(
            Utc.with_ymd_and_hms(2026, 3, 29, 0, 0, 0).unwrap()
                + chrono::Duration::seconds(m * 3)
                + chrono::Duration::nanoseconds(m * 12345),
        );
    }
    for m in 0..3600 {
        dates.push(
            Utc.with_ymd_and_hms(2026, 10, 25, 0, 0, 0).unwrap() + chrono::Duration::seconds(m * 3),
        );
    }
    dates.push(Utc.timestamp_opt(1_483_228_799, 1_500_000_000).unwrap()); // leap second representation
    dates.push(Utc.timestamp_opt(1_483_228_799, 1_999_999_999).unwrap());
    dates.push(DateTime::<Utc>::MIN_UTC);
    dates.push(DateTime::<Utc>::MAX_UTC);
    dates.push(DateTime::<Utc>::MAX_UTC - chrono::Duration::hours(3));
    dates.push(DateTime::<Utc>::MIN_UTC + chrono::Duration::hours(3));
    dates.push(Utc.with_ymd_and_hms(0, 1, 1, 0, 0, 0).unwrap());
    dates.push(Utc.with_ymd_and_hms(-1, 12, 31, 23, 59, 59).unwrap());
    dates.push(Utc.with_ymd_and_hms(9999, 12, 31, 23, 59, 59).unwrap());
    dates.push(Utc.with_ymd_and_hms(10000, 1, 1, 0, 0, 0).unwrap());
    dates.push(Utc.with_ymd_and_hms(-10000, 1, 1, 0, 0, 0).unwrap());
    let samples = [
        "",
        "x",
        "  padded  ",
        "multi\nline",
        "ŻAŁOŚĆ ąę",
        &"y".repeat(5000),
    ];
    let mut n = 0;
    for d in &dates {
        let w = d.with_timezone(&chrono_tz::Europe::Warsaw);
        for t in samples {
            let a = std::panic::catch_unwind(|| fmtv::v0(&w, t));
            let b = std::panic::catch_unwind(|| fmtv::v2(&w, t));
            match (a, b) {
                (Ok(a), Ok(b)) => assert_eq!(a, b, "date {d}"),
                (Err(_), Err(_)) => println!("    both panic for {d}"),
                _ => panic!("panic behaviour differs for {d}"),
            }
            n += 1;
        }
        let a = std::panic::catch_unwind(|| fmtv::p0("app", &w));
        let b = std::panic::catch_unwind(|| fmtv::p2("app", &w));
        match (a, b) {
            (Ok(a), Ok(b)) => assert_eq!(a, b, "date {d}"),
            (Err(_), Err(_)) => println!("    both panic (path) for {d}"),
            _ => panic!("panic behaviour differs (path) for {d}"),
        }
        n += 1;
    }
    println!("    byte proof: {n} cases, old == new");
}

// ---------------------------------------------------------------- 7. proof: real code after C1 vs the pre-change formulas
#[tokio::test]
#[ignore]
async fn bench_7_proof_c1() {
    use chrono::TimeZone;
    println!("\n=== 7. proof: OpLogWorker::log() after C1 == pre-change formulas ===");
    fn old_entry(
        t: &OpLogType,
        no_date: bool,
        date: &DateTime<chrono_tz::Tz>,
        text: &str,
    ) -> String {
        let f = match t {
            OpLogType::NoSplit => "",
            OpLogType::PerHour => "%H:%M:%S.%3f",
            OpLogType::PerDay => "%H:%M:%S.%3f",
            OpLogType::PerMonth => "%d %H:%M:%S.%3f",
        };
        if f.is_empty() || no_date {
            text.trim().to_string()
        } else {
            format!("{} {text}", date.format(f))
        }
    }
    fn old_names(
        t: &OpLogType,
        subdirs: bool,
        date: &DateTime<chrono_tz::Tz>,
        name: &str,
        path: &str,
    ) -> (String, String) {
        let f = match t {
            OpLogType::NoSplit => "",
            OpLogType::PerHour => "%Y_%m_%d_%H",
            OpLogType::PerDay => "%Y_%m_%d",
            OpLogType::PerMonth => "%Y_%m",
        };
        let d = date.format(f);
        if f.is_empty() {
            (format!("{}.log", name), path.to_string())
        } else if subdirs {
            (format!("{}.log", name), format!("{}/{}", path, d))
        } else {
            (format!("{}_{}.log", name, d), path.to_string())
        }
    }
    let mut dates: Vec<DateTime<Utc>> = vec![];
    let start = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();
    for h in 0..(4 * 366 * 24) {
        dates.push(start + chrono::Duration::hours(h) + chrono::Duration::milliseconds(h % 1000));
    }
    for m in 0..3600 {
        dates.push(
            Utc.with_ymd_and_hms(2026, 3, 29, 0, 0, 0).unwrap()
                + chrono::Duration::seconds(m * 3)
                + chrono::Duration::nanoseconds(m * 12345),
        );
    }
    for m in 0..3600 {
        dates.push(
            Utc.with_ymd_and_hms(2026, 10, 25, 0, 0, 0).unwrap() + chrono::Duration::seconds(m * 3),
        );
    }
    dates.push(Utc.timestamp_opt(1_483_228_799, 1_500_000_000).unwrap());
    dates.push(Utc.timestamp_opt(1_483_228_799, 1_999_999_999).unwrap());
    dates.push(DateTime::<Utc>::MIN_UTC);
    dates.push(DateTime::<Utc>::MAX_UTC);
    dates.push(DateTime::<Utc>::MAX_UTC - chrono::Duration::hours(3));
    dates.push(DateTime::<Utc>::MIN_UTC + chrono::Duration::hours(3));
    dates.push(Utc.with_ymd_and_hms(0, 1, 1, 0, 0, 0).unwrap());
    dates.push(Utc.with_ymd_and_hms(-1, 12, 31, 23, 59, 59).unwrap());
    dates.push(Utc.with_ymd_and_hms(9999, 12, 31, 23, 59, 59).unwrap());
    dates.push(Utc.with_ymd_and_hms(10000, 1, 1, 0, 0, 0).unwrap());
    dates.push(Utc.with_ymd_and_hms(-10000, 1, 1, 0, 0, 0).unwrap());
    let samples = [
        "",
        "x",
        "  padded  ",
        "multi\nline",
        "ŻAŁOŚĆ ąę",
        &"y".repeat(5000),
    ];
    let pairs = [
        ("app", "logs"),
        ("a/b", ""),
        ("", "."),
        ("x", "C:/deep/dir/"),
    ];
    let mut n = 0u64;
    for t in [
        OpLogType::NoSplit,
        OpLogType::PerHour,
        OpLogType::PerDay,
        OpLogType::PerMonth,
    ] {
        for (subdirs, no_date) in [(false, false), (true, false), (false, true), (true, true)] {
            let mut opts = HashSet::new();
            if subdirs {
                opts.insert(OpLogOption::UseSubDirectories);
            }
            if no_date {
                opts.insert(OpLogOption::NoAddDateToLog);
            }
            for (name, path) in pairs {
                let (tx, rx) = tokio::sync::mpsc::channel(1);
                std::mem::forget(tx);
                let mut w = OpLogWorker::new(rx);
                w.def(
                    OpLogDefinition::new(name, path)
                        .log_type(t)
                        .options(opts.clone())
                        .flush_interval(Duration::from_secs(1)),
                );
                for d in &dates {
                    let wd = d.with_timezone(&chrono_tz::Europe::Warsaw);
                    let (oname, opath) = old_names(&t, subdirs, &wd, name, path);
                    let okey = format!("{opath}/{oname}");
                    for text in samples {
                        w.log(name, *d, text);
                        let def = &mut w.definitions.get_mut(name).unwrap();
                        assert_eq!(def.files.len(), 1, "one file per date");
                        let (key, file) = def.files.iter_mut().next().unwrap();
                        assert_eq!(key, &okey);
                        assert_eq!(file.log_name, oname);
                        assert_eq!(file.path, opath);
                        let got = file.logs.pop_back().unwrap();
                        file.queued_bytes -= got.len();
                        assert_eq!(got, old_entry(&t, no_date, &wd, text), "{} {d}", t as u8);
                        n += 1;
                    }
                    w.definitions.get_mut(name).unwrap().files.clear();
                }
            }
        }
    }
    println!(
        "    {n} cases (dates x types x options x name/path pairs x texts): keys, names, paths and entries identical"
    );
}

// ---------------------------------------------------------------- 8. completeness (stage 3)
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn bench_8_completeness() {
    use chrono::TimeZone;
    let dir = tmp("stage3");
    println!("\n=== 8. stage 3: paths the profile skipped ===");
    let texts: Vec<String> = (0..200).map(|i| line(i, 1024)).collect();

    // (a) three definitions written in one tick, 200 x 1 KB each
    let mut runs = vec![];
    for r in 0..REPS {
        let (tx, rx) = tokio::sync::mpsc::channel(32);
        std::mem::forget(tx);
        let mut w = OpLogWorker::new(rx);
        let d = dir.join(format!("multi{r}"));
        for name in ["log", "ai", "api"] {
            w.def(
                OpLogDefinition::new(name, d.to_str().unwrap())
                    .flush_interval(Duration::from_millis(0)),
            );
            for s in &texts {
                w.log(name, Utc::now(), s);
            }
        }
        let ti = Instant::now();
        w.write_to_files().await;
        runs.push(ti.elapsed());
        assert_eq!(w.log_count(), 0);
    }
    stats(
        "tick: 3 definitions x 200 x 1 KB (3 files, new)",
        &runs,
        None,
    );
    let mut runs = vec![];
    for r in 0..REPS {
        let (tx, rx) = tokio::sync::mpsc::channel(32);
        std::mem::forget(tx);
        let mut w = OpLogWorker::new(rx);
        let d = dir.join(format!("multi{r}"));
        for name in ["log", "ai", "api"] {
            w.def(
                OpLogDefinition::new(name, d.to_str().unwrap())
                    .flush_interval(Duration::from_millis(0)),
            );
            for s in &texts {
                w.log(name, Utc::now(), s);
            }
        }
        let ti = Instant::now();
        w.write_to_files().await;
        runs.push(ti.elapsed());
        assert_eq!(w.log_count(), 0);
    }
    stats(
        "tick: 3 definitions x 200 x 1 KB (3 files, existing: tail scan + append)",
        &runs,
        None,
    );

    // (b) PerHour rollover: first tick creates hour H file, next tick creates H+1 file; then plain append
    let mut new_file = vec![];
    let mut roll = vec![];
    let mut append = vec![];
    for r in 0..REPS {
        let mut w = worker(
            &dir.join(format!("roll{r}")),
            OpLogType::PerHour,
            HashSet::new(),
            Duration::from_millis(0),
        );
        let h0 = Utc.with_ymd_and_hms(2026, 8, 26, 10, 0, 0).unwrap();
        w.log("app", h0, "entry in hour 10");
        let ti = Instant::now();
        w.write_to_files().await;
        new_file.push(ti.elapsed());
        w.log("app", h0 + chrono::Duration::hours(1), "entry in hour 11");
        let ti = Instant::now();
        w.write_to_files().await;
        roll.push(ti.elapsed());
        w.log(
            "app",
            h0 + chrono::Duration::hours(1),
            "another entry in hour 11",
        );
        let ti = Instant::now();
        w.write_to_files().await;
        append.push(ti.elapsed());
        assert_eq!(w.definitions["app"].files.len(), 2);
    }
    stats(
        "PerHour: tick creating the first file (create+magic+header)",
        &new_file,
        None,
    );
    stats(
        "PerHour: rollover tick (new file for H+1, old file idle)",
        &roll,
        None,
    );
    stats(
        "PerHour: append tick (2 files in map, 1 with entries)",
        &append,
        None,
    );

    // (c) failing tick (path blocked by a file) — the per-500 ms cost of an error episode
    {
        let blocker = dir.join("blocker");
        std::fs::write(&blocker, b"file").unwrap();
        let mut w = worker(
            &blocker.join("logs"),
            OpLogType::NoSplit,
            HashSet::new(),
            Duration::from_millis(0),
        );
        for s in &texts {
            w.log("app", Utc::now(), s);
        }
        let mut runs = vec![];
        for _ in 0..REPS {
            let ti = Instant::now();
            for _ in 0..100 {
                w.write_to_files().await;
            }
            runs.push(ti.elapsed());
        }
        stats(
            "failing tick (path error, fails before encoding), 200 x 1 KB queued",
            &runs,
            Some((100, "tick")),
        );
    }
    // (d) first tick after an error episode on a 64 MB NoSplit file: tail scan + append
    {
        let d = dir.join("big");
        std::fs::create_dir_all(&d).unwrap();
        let p = d.join("app.log");
        let frames = synth_file(&p, 64 * 1024 * 1024, 10_000);
        let mut runs = vec![];
        for _ in 0..REPS {
            let mut w = worker(
                &d,
                OpLogType::NoSplit,
                HashSet::new(),
                Duration::from_millis(0),
            );
            w.log("app", Utc::now(), "after the episode");
            let ti = Instant::now();
            w.write_to_files().await;
            runs.push(ti.elapsed());
            assert_eq!(w.log_count(), 0);
        }
        stats(
            &format!("first tick on a 64 MB file ({frames} frames): tail scan + append"),
            &runs,
            None,
        );
    }
    // (e) get_info on the handle (empty queue) and log_bundle producer cost
    let op = OpLog::new();
    op.def(
        OpLogDefinition::new("app", dir.join("info").to_str().unwrap())
            .flush_interval(Duration::from_secs(10)),
    );
    let mut runs = vec![];
    for _ in 0..REPS {
        let ti = Instant::now();
        for _ in 0..100 {
            op.get_info().await.unwrap();
        }
        runs.push(ti.elapsed());
    }
    stats(
        "OpLog::get_info() round trip, empty queue",
        &runs,
        Some((100, "call")),
    );
    let mut runs = vec![];
    let mut al = vec![];
    for _ in 0..REPS {
        tokio::time::sleep(Duration::from_millis(50)).await;
        let a0 = allocs().0;
        let ti = Instant::now();
        let mut b = crate::messages::OpLogBundle::new();
        for s in &texts {
            b = b.add_log(crate::messages::OpLogData {
                log_name: "app".to_string(),
                log: s.clone(),
                date: Utc::now(),
            });
        }
        op.log_bundle(b);
        runs.push(ti.elapsed());
        al.push(allocs().0 - a0);
    }
    stats(
        "OpLog::log_bundle() with 200 x 1 KB (producer side)",
        &runs,
        Some((200, "entry")),
    );
    println!("    allocs/entry: {:.2}", al[0] as f64 / 200.0);
    // (f) real drop rate: burst of 50 000 x 100 B from one thread at full speed, then count what reached the worker
    let text = line(1, 100);
    for burst in [1_000usize, 10_000, 50_000] {
        op.get_info().await; // drain
        let ti = Instant::now();
        for _ in 0..burst {
            op.log("app", Utc::now(), &text);
        }
        let el = ti.elapsed();
        tokio::time::sleep(Duration::from_millis(300)).await;
        let info = op.get_info().await.unwrap();
        println!(
            "  burst {burst:>6} x 100 B in {:>9}: reached the worker {:>6} ({:.1}%), dropped at the channel {:>6}",
            fmt(el),
            info.number_of_logs,
            info.number_of_logs as f64 / burst as f64 * 100.0,
            burst.saturating_sub(info.number_of_logs)
        );
    }
    op.shutdown().await;
    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------- 9. proof: the constant formats cannot make write_to fail
#[test]
#[ignore]
fn bench_9_expect_unreachable() {
    use chrono::format::{Fixed, Item, Numeric, StrftimeItems};
    println!("\n=== 9. proof: write_date(..).expect(..) is unreachable ===");
    for f in [
        "",
        "%H:%M:%S.%3f",
        "%d %H:%M:%S.%3f",
        "%Y_%m_%d_%H",
        "%Y_%m_%d",
        "%Y_%m",
    ] {
        let items: Vec<Item> = StrftimeItems::new(f).collect();
        for it in &items {
            match it {
                Item::Literal(_) | Item::Space(_) => {}
                Item::Numeric(n, _) => assert!(
                    matches!(
                        n,
                        Numeric::Hour
                            | Numeric::Minute
                            | Numeric::Second
                            | Numeric::Day
                            | Numeric::Month
                            | Numeric::Year
                    ),
                    "{f}: {it:?}"
                ),
                Item::Fixed(Fixed::Internal(_)) => {} // Nanosecond3NoDot: needs `time` only
                other => panic!("{f}: unexpected item {other:?}"),
            }
        }
        assert!(
            !items.contains(&Item::Error),
            "{f} parses without an error item"
        );
        println!(
            "    {f:<18} -> {} items, no Item::Error, only date/time numerics + literals",
            items.len()
        );
    }
}
