// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::{
    hint::black_box,
    sync::{
        Arc, Barrier,
        atomic::{AtomicU64, Ordering::Relaxed},
    },
    thread,
    time::Instant,
};
#[derive(Default)]
struct Counters {
    values: [AtomicU64; 16],
}
#[derive(Clone, Copy, Debug)]
enum Mode {
    Baseline,
    Disabled,
    CountersOnly,
    Current,
}
#[inline(never)]
fn run(mode: Mode, shared: &Arc<Counters>, n: usize, deletes: bool) -> u64 {
    let optional = black_box(if matches!(mode, Mode::Disabled) {
        None
    } else {
        Some(shared)
    });
    let mut checksum = 0u64;
    for i in 0..n {
        let bytes = black_box(100_000 + (i as u64 & 1023));
        checksum = checksum.wrapping_add(bytes);
        match mode {
            Mode::Baseline => {}
            Mode::Disabled => {
                if let Some(c) = optional {
                    let c = Arc::clone(c);
                    update(&c, bytes, deletes);
                }
            }
            Mode::CountersOnly => update(shared, bytes, deletes),
            Mode::Current => {
                let c = Arc::clone(shared);
                update(&c, bytes, deletes);
            }
        }
    }
    black_box(checksum)
}
#[inline(always)]
fn update(c: &Counters, bytes: u64, deletes: bool) {
    c.values[6].fetch_add(1, Relaxed);
    c.values[8].fetch_add(bytes, Relaxed);
    if deletes {
        c.values[9].fetch_add(2, Relaxed);
        c.values[11].fetch_add(200, Relaxed);
    }
}
fn sample(mode: Mode, workers: usize, deletes: bool, total: usize) -> f64 {
    let c = Arc::new(Counters::default());
    let ready = Arc::new(Barrier::new(workers + 1));
    let start = Arc::new(Barrier::new(workers + 1));
    thread::scope(|s| {
        let handles: Vec<_> = (0..workers)
            .map(|_| {
                let (c, ready, start) = (c.clone(), ready.clone(), start.clone());
                s.spawn(move || {
                    ready.wait();
                    start.wait();
                    run(mode, &c, total / workers, deletes)
                })
            })
            .collect();
        ready.wait();
        let now = Instant::now();
        start.wait();
        for h in handles {
            black_box(h.join().unwrap());
        }
        let ns = now.elapsed().as_nanos() as f64 / total as f64;
        let expected = if matches!(mode, Mode::CountersOnly | Mode::Current) {
            (total / workers * workers) as u64
        } else {
            0
        };
        assert_eq!(c.values[6].load(Relaxed), expected);
        assert_eq!(
            c.values[9].load(Relaxed),
            if deletes { expected * 2 } else { 0 }
        );
        ns
    })
}
fn main() {
    let total = 2_000_000;
    println!("workers,deletes,mode,median_ns_per_file,min_ns_per_file,max_ns_per_file");
    for workers in [1, 4, 8] {
        for deletes in [false, true] {
            let modes = [
                Mode::Baseline,
                Mode::Disabled,
                Mode::CountersOnly,
                Mode::Current,
            ];
            let mut samples = [vec![], vec![], vec![], vec![]];
            for round in 0..8 {
                for k in 0..4 {
                    let idx = (k + round) % 4;
                    let ns = sample(modes[idx], workers, deletes, total);
                    if round > 0 {
                        samples[idx].push(ns);
                    }
                }
            }
            for (mode, mut v) in modes.into_iter().zip(samples) {
                v.sort_by(f64::total_cmp);
                println!(
                    "{workers},{deletes},{mode:?},{:.3},{:.3},{:.3}",
                    v[3], v[0], v[6]
                );
            }
        }
    }
}
