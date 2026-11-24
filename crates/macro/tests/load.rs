//! The load pipeline against a fixture source: cache hits, forced refetch, offline
//! fallback to the stale cache, and the ISM override file.

use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use tr_macro::{Indicator, Point, Source, load};

struct Fixtures {
    calls: Arc<AtomicUsize>,
    fail: bool,
}

impl Source for Fixtures {
    fn series(&self, _id: &str) -> impl Future<Output = anyhow::Result<Vec<Point>>> + Send {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let fail = self.fail;
        async move {
            if fail {
                anyhow::bail!("simulated offline");
            }
            Ok(vec![
                Point { ts: 1_600_000_000, value: 100.0 },
                Point { ts: 1_602_592_000, value: 101.0 },
                Point { ts: 1_605_184_000, value: 102.0 },
                Point { ts: 1_607_776_000, value: 103.0 },
                Point { ts: 1_610_368_000, value: 104.0 },
            ])
        }
    }
}

fn temp_dir(tag: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("tr-macro-load-{}-{tag}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

#[tokio::test]
async fn without_a_dir_every_indicator_comes_from_seeds() {
    let src = Fixtures { calls: Arc::new(AtomicUsize::new(0)), fail: false };
    let snap = load(&src, None, false).await;
    assert_eq!(src.calls.load(Ordering::SeqCst), 0, "no network without a cache dir");
    assert_eq!(snap.stats.len(), 4);
    assert!(snap.live.is_empty());
    assert_eq!(snap.fetched_at, 0);
    assert_eq!(snap.components.len(), 10);
    assert!(snap.errors.is_empty());
}

#[tokio::test]
async fn cache_fetch_force_and_offline_fallback() {
    let dir = temp_dir("cache");
    let calls = Arc::new(AtomicUsize::new(0));
    let src = Fixtures { calls: calls.clone(), fail: false };

    // Cold: three FRED series fetched and cached; ISM stays seeded.
    let snap = load(&src, Some(&dir), false).await;
    assert_eq!(calls.load(Ordering::SeqCst), 3);
    assert_eq!(snap.live.len(), 3);
    assert!(snap.fetched_at > 0);
    assert!(snap.stat(Indicator::Gdp).unwrap().level == 104.0);

    // Warm: the fresh cache answers without touching the source.
    let snap = load(&src, Some(&dir), false).await;
    assert_eq!(calls.load(Ordering::SeqCst), 3);
    assert_eq!(snap.live.len(), 3);

    // Forced: refetches even though the cache is fresh.
    load(&src, Some(&dir), true).await;
    assert_eq!(calls.load(Ordering::SeqCst), 6);

    // Offline + forced: stale cache still serves, with the errors surfaced.
    let offline = Fixtures { calls: calls.clone(), fail: true };
    let snap = load(&offline, Some(&dir), true).await;
    assert_eq!(snap.live.len(), 3);
    assert_eq!(snap.errors.len(), 3);
    assert_eq!(snap.stat(Indicator::M2).unwrap().level, 104.0);

    std::fs::remove_dir_all(&dir).unwrap();
}

#[tokio::test]
async fn ism_override_file_wins_over_the_seed() {
    let dir = temp_dir("ism");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("ism.csv"), "date,pmi,new_orders\n2026-11-01,56.1,57.0\n").unwrap();
    let src = Fixtures { calls: Arc::new(AtomicUsize::new(0)), fail: false };
    let snap = load(&src, Some(&dir), false).await;
    let pmi = snap.stat(Indicator::Ism).unwrap();
    assert_eq!(pmi.level, 56.1);
    assert_eq!(snap.components.len(), 1, "only the columns the override carries");
    assert_eq!(snap.components[0].name, "New Orders");
    std::fs::remove_dir_all(&dir).unwrap();
}
