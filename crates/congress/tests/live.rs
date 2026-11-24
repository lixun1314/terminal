//! Network tests against the live disclosure sites; run with
//! `cargo test -p tr-congress --test live -- --ignored`.
//! Sites that geo-block (Senate eFD) need a proxy: `TR_PROXY` or `proxy.txt`.

use tr_congress::refresh::{HttpSource, Source};

#[tokio::test]
#[ignore = "network"]
async fn senate_session_search_and_ptr() {
    let s = HttpSource::new().unwrap();
    let since = chrono::Utc::now().date_naive() - chrono::TimeDelta::days(30);
    let reports = s.senate_reports(Some(since)).await.unwrap();
    assert!(!reports.is_empty(), "no Senate filings in the last 30 days");
    let ptr = s.senate_ptr(&reports[0]).await.unwrap();
    assert!(ptr.contains("Transaction Date"), "PTR page did not load");
}
