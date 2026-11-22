use std::collections::BTreeMap;
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

static ENABLED: OnceLock<bool> = OnceLock::new();
static PROFILE: Mutex<Profile> = Mutex::new(Profile {
    last_report: None,
    stages: BTreeMap::new(),
});

struct Profile {
    last_report: Option<Instant>,
    stages: BTreeMap<&'static str, Histogram>,
}

#[derive(Default)]
struct Histogram {
    buckets: [u64; 32],
    count: u64,
    total_us: u64,
}

pub(super) fn enabled() -> bool {
    *ENABLED.get_or_init(|| std::env::var("RUSTA_ACK_PROFILE").as_deref() == Ok("1"))
}

pub(super) fn start() -> Option<Instant> {
    enabled().then(Instant::now)
}

pub(super) fn finish(stage: &'static str, start: Option<Instant>) {
    if let Some(start) = start {
        record(stage, start.elapsed());
    }
}

pub(super) fn record(stage: &'static str, duration: Duration) {
    let us = duration.as_micros().min(u128::from(u32::MAX)) as u32;
    let bucket = us.max(1).ilog2() as usize;
    let mut profile = PROFILE.lock().expect("ack profile lock");
    let histogram = profile.stages.entry(stage).or_default();
    histogram.buckets[bucket] += 1;
    histogram.count += 1;
    histogram.total_us += u64::from(us);
}

pub(super) fn report() {
    if !enabled() {
        return;
    }
    let mut profile = PROFILE.lock().expect("ack profile lock");
    let now = Instant::now();
    let last = profile.last_report.get_or_insert(now);
    if now.duration_since(*last) < Duration::from_secs(1) {
        return;
    }
    *last = now;
    let stages = std::mem::take(&mut profile.stages);
    drop(profile);
    for (stage, histogram) in stages {
        eprintln!(
            "rusta_ack_profile stage={stage} count={} total_us={} buckets={:?}",
            histogram.count, histogram.total_us, histogram.buckets
        );
    }
}
