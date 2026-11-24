//! Live FRED round-trip: `cargo test -p tr-macro -- --ignored`.

use tr_macro::{Indicator, Source, fred, load};

#[tokio::test]
#[ignore = "network"]
async fn fred_series_fetch_parse_and_cache() {
    let src = fred::FredSource::new().unwrap();
    for id in ["GDPC1", "M2SL", "CPIAUCSL"] {
        let pts = src.series(id).await.unwrap();
        assert!(pts.len() > 300, "{id} too short");
        assert!(pts.windows(2).all(|w| w[0].ts < w[1].ts));
        assert!(pts.last().unwrap().value > 0.0);
    }
    let dir = std::env::temp_dir().join(format!("tr-macro-live-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let snap = load(&src, Some(&dir), true).await;
    assert!(snap.errors.is_empty(), "{:?}", snap.errors);
    assert_eq!(snap.live.len(), 3);
    assert_eq!(snap.stats.len(), 4);
    assert!(snap.stat(Indicator::Ism).unwrap().level > 40.0);
    // Second load is served from the fresh cache without touching FRED.
    let warm = load(&src, Some(&dir), false).await;
    assert_eq!(warm.fetched_at, snap.fetched_at);
    std::fs::remove_dir_all(&dir).unwrap();
}
