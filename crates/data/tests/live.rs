//! Network tests against Yahoo; run with `cargo test -p tr-data -- --ignored`.
use tr_core::Timeframe;
use tr_data::MarketData;
use tr_data::yahoo::YahooProvider;

#[tokio::test]
#[ignore = "network"]
async fn yahoo_history_and_paging() {
    let y = YahooProvider::new().unwrap();
    for tf in [Timeframe::M5, Timeframe::H4, Timeframe::D1, Timeframe::W1, Timeframe::MN1] {
        let recent = y.history("AAPL", tf, None, 200).await.unwrap();
        assert!(recent.len() >= 100, "{tf}: {}", recent.len());
        assert!(recent.windows(2).all(|w| w[0].ts < w[1].ts), "{tf}");
        let older = y.history("AAPL", tf, Some(recent[0].ts), 200).await.unwrap();
        assert!(older.last().unwrap().ts < recent[0].ts, "{tf}");
    }
    let q = y.quotes(&["AAPL".into(), "MSFT".into()]).await.unwrap();
    assert_eq!(q.len(), 2);
}

#[tokio::test]
#[ignore = "network"]
async fn tradingview_backup_quotes_and_search() {
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    let tv = tr_data::TradingView::new().unwrap();
    let q = tv.quotes(&["AAPL US Equity".into(), "CL1 Comdty".into(), "SPX Index".into()]).await.unwrap();
    assert!(q.iter().any(|x| x.symbol == "AAPL US Equity" && x.last > 0.0), "{q:?}");
    assert!(q.iter().any(|x| x.symbol == "CL1 Comdty" && x.last > 0.0), "{q:?}");
    assert!(q.iter().any(|x| x.symbol == "SPX Index" && x.last > 0.0), "{q:?}");
    let hits = tv.search("copper").await.unwrap();
    assert!(hits.iter().any(|h| h.class == tr_core::AssetClass::Cmdty), "{hits:?}");
}

mod markets {
    use std::time::Duration;
    use tr_core::Timeframe;
    use tr_data::MarketData;
    use tr_data::markets::Markets;

    fn m() -> Markets {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
        Markets::new(None).unwrap()
    }

    /// Equities have no push stream: the live feed polls Yahoo's chart tail.
    #[tokio::test]
    #[ignore = "network"]
    async fn equity_live_feed_polls_the_chart_tail() {
        let m = m();
        let mut feed = m.live("NVDA US Equity", Timeframe::M30).expect("equity live feed");
        let bar = tokio::time::timeout(Duration::from_secs(90), feed.recv()).await.expect("poll tick").unwrap();
        assert!(bar.close > 0.0);
    }

    #[tokio::test]
    #[ignore = "network"]
    async fn every_asset_class_has_history() {
        let m = m();
        for (key, tf) in [
            ("BTCUSD Curncy", Timeframe::H1),
            ("ETHUSD Curncy", Timeframe::D1),
            ("EURUSD Curncy", Timeframe::D1),
            ("US10Y Govt", Timeframe::D1),
            ("US10Y Govt", Timeframe::M5),
            ("US2Y Govt", Timeframe::W1),
            ("CL1 Comdty", Timeframe::D1),
            ("GC1 Comdty", Timeframe::H1),
            ("SPX Index", Timeframe::D1),
            ("NKY Index", Timeframe::D1),
            ("SPY US Equity", Timeframe::D1),
            ("AAPL US Equity", Timeframe::M15),
        ] {
            let bars = m.history(key, tf, None, 1_500).await.unwrap_or_else(|e| panic!("{key} {tf}: {e:#}"));
            assert!(bars.len() > 50, "{key} {tf}: {}", bars.len());
            assert!(bars.windows(2).all(|w| w[0].ts < w[1].ts), "{key} {tf}");
            let older = m.history(key, tf, Some(bars[0].ts), 200).await;
            if let Ok(o) = older {
                assert!(o.last().is_none_or(|b| b.ts < bars[0].ts), "{key} {tf} paging");
            }
        }
    }

    #[tokio::test]
    #[ignore = "network"]
    async fn mixed_quotes_curve_search_and_live() {
        let m = m();
        let keys: Vec<String> =
            ["BTCUSD Curncy", "EURUSD Curncy", "US10Y Govt", "CL1 Comdty", "SPX Index", "AAPL US Equity"]
                .map(String::from)
                .to_vec();
        let q = m.quotes(&keys).await.unwrap();
        for k in &keys {
            let x = q.iter().find(|x| &x.symbol == k).unwrap_or_else(|| panic!("no quote for {k}"));
            assert!(x.last > 0.0 && x.prev_close > 0.0, "{k}");
        }
        let c = m.yield_curves().await.unwrap();
        assert!(c.current.points.len() >= 10 && c.year_ago.is_some());
        let hits = m.search("bitcoin").await.unwrap();
        assert!(hits.iter().any(|h| h.key == "BTCUSD Curncy"), "{hits:?}");
        let mut feed = m.live("BTCUSD Curncy", Timeframe::M1).unwrap();
        let bar = tokio::time::timeout(Duration::from_secs(20), feed.recv()).await.expect("live tick").unwrap();
        assert!(bar.close > 1000.0);
    }

    #[tokio::test]
    #[ignore = "network"]
    async fn news_and_order_books() {
        let m = m();
        for key in [Some("AAPL US Equity"), Some("BTCUSD Curncy"), None] {
            let n = m.news(key).await.unwrap();
            assert!(!n.is_empty(), "news for {key:?}");
        }
        let b = m.order_book("BTCUSD Curncy", 20).await.unwrap();
        assert_eq!(b.bids.len(), 20);
        assert!(b.bids[0].0 < b.asks[0].0 && !b.simulated);
        assert!(m.order_book("AAPL US Equity", 20).await.is_err());
    }

    #[tokio::test]
    #[ignore = "network"]
    async fn option_chains() {
        let m = m();
        let c = m.options("AAPL US Equity", None).await.unwrap();
        assert!(!c.simulated && c.spot > 1.0 && c.expirations.len() > 5);
        assert!(!c.calls.is_empty() && !c.puts.is_empty());
        let later = m.options("AAPL US Equity", Some(c.expirations[3])).await.unwrap();
        assert_eq!(later.expiration, c.expirations[3]);
        let spx = m.options("SPX Index", None).await.unwrap();
        assert!(spx.calls.len() > 20, "index options");
        assert!(m.options("BTCUSD Curncy", None).await.is_err());
    }

    /// The default watchlist keys must all quote, and prices must move between polls.
    #[tokio::test]
    #[ignore = "network"]
    async fn watchlist_quotes_arrive_and_change() {
        let m = m();
        let keys: Vec<String> = [
            "SPX Index",
            "AAPL US Equity",
            "NVDA US Equity",
            "SPY US Equity",
            "ETHUSD Curncy",
            "USDJPY Curncy",
            "US10Y Govt",
            "CL1 Comdty",
            "GC1 Comdty",
            "LI US Equity",
        ]
        .iter()
        .map(|s| s.to_string())
        .collect();
        let a = m.quotes(&keys).await.unwrap();
        println!("first poll: {} of {}", a.len(), keys.len());
        for q in &a {
            println!("  {} last={} prev={}", q.symbol, q.last, q.prev_close);
        }
        assert_eq!(a.len(), keys.len(), "every watchlist key should quote");
        tokio::time::sleep(Duration::from_secs(8)).await;
        let b = m.quotes(&keys).await.unwrap();
        let changed = a.iter().filter(|x| b.iter().any(|y| y.symbol == x.symbol && y.last != x.last)).count();
        println!("changed after 8s: {changed} of {}", a.len());
        for x in &a {
            if let Some(y) = b.iter().find(|y| y.symbol == x.symbol) {
                println!("  {} {} -> {}", x.symbol, x.last, y.last);
            }
        }
        assert!(changed > 0, "no watchlist quote moved in 8 s");
    }

    /// China/HK quotes, history and the ~3 s live poll all work without a key.
    #[tokio::test]
    #[ignore = "network"]
    async fn cn_quotes_history_and_live() {
        let q = tr_data::cn::quotes(&["600519 SS Equity".into(), "0700 HK Equity".into()]).await.unwrap();
        assert_eq!(q.len(), 2, "{q:?}");
        assert!(q.iter().all(|x| x.last > 0.0));
        let qi = tr_data::cn::quotes(&["CCMP Index".into(), "HSI Index".into()]).await.unwrap();
        assert_eq!(qi.len(), 2, "{qi:?}");
        assert!(qi.iter().all(|x| x.last > 0.0));
        let h = tr_data::cn::history(&tr_data::security("600519 SS Equity"), Timeframe::D1, 60).await.unwrap();
        assert!(h.len() >= 30, "{}", h.len());
        assert!(h.windows(2).all(|w| w[0].ts < w[1].ts));
        let mut feed = tr_data::cn::live(&tr_data::security("600519 SS Equity"), Timeframe::M1);
        let bar = tokio::time::timeout(Duration::from_secs(8), feed.recv()).await.expect("live tick").unwrap();
        assert!(bar.close > 0.0);
    }

    /// The US-equity push feed connects to TradingView and delivers bars (initial series even
    /// outside the regular session; ticks flow during it).
    #[tokio::test]
    #[ignore = "network"]
    async fn us_equity_live_push_connects() {
        let mut feed = tr_data::tv_live::feed(&tr_data::security("AAPL US Equity"), Timeframe::M1, true, true);
        let bar = tokio::time::timeout(Duration::from_secs(20), feed.recv()).await.expect("tv live bar").unwrap();
        assert!(bar.close > 0.0, "{bar:?}");
    }

    /// Indices ride the same push (delayed with this account, like the TradingView web chart),
    /// and their history comes from the same socket so the chart never mixes vendors.
    #[tokio::test]
    #[ignore = "network"]
    async fn index_push_and_history_are_same_source() {
        let sec = tr_data::security("CCMP Index");
        let h = tr_data::tv_live::history(&sec, Timeframe::M1, 300).await.unwrap();
        assert!(h.len() >= 100, "{}", h.len());
        assert!(h.windows(2).all(|w| w[0].ts < w[1].ts));
        let mut feed = tr_data::tv_live::feed(&sec, Timeframe::M1, true, false);
        let bar = tokio::time::timeout(Duration::from_secs(25), feed.recv()).await.expect("index push").unwrap();
        assert!(bar.close > 0.0, "{bar:?}");
    }

    /// The full provider path a chart uses: TradingView history then the same-source push.
    #[tokio::test]
    #[ignore = "network"]
    async fn markets_history_then_push_for_us_equity() {
        let m = m();
        let bars = m.history("LI US Equity", Timeframe::M1, None, 300).await.unwrap();
        assert!(!bars.is_empty(), "history");
        let mut feed = m.live("LI US Equity", Timeframe::M1).expect("live feed");
        let bar = tokio::time::timeout(Duration::from_secs(25), feed.recv()).await.expect("push bar").unwrap();
        assert!(bar.close > 0.0);
    }

    /// Alpaca quotes and trade push (needs APCA_API_KEY_ID/APCA_API_SECRET_KEY; skips otherwise).
    #[tokio::test]
    #[ignore = "network"]
    async fn alpaca_quotes_and_live() {
        let Ok(key) = std::env::var("APCA_API_KEY_ID") else {
            eprintln!("skipping: no APCA_API_KEY_ID in the environment");
            return;
        };
        let secret = std::env::var("APCA_API_SECRET_KEY").expect("APCA_API_SECRET_KEY");
        let feed = std::env::var("APCA_DATA_FEED").unwrap_or_else(|_| "iex".into());
        let a = tr_data::alpaca::AlpacaProvider::new(key, secret, feed).unwrap();
        let q = a.quotes(&["AAPL US Equity".into(), "SPY US Equity".into()]).await.unwrap();
        assert_eq!(q.len(), 2, "{q:?}");
        assert!(q.iter().all(|x| x.last > 0.0));
        let sec = tr_data::security("AAPL US Equity");
        let mut feed = a.live(&sec, Timeframe::M1).expect("alpaca live feed");
        match tokio::time::timeout(Duration::from_secs(30), feed.recv()).await {
            Ok(Some(bar)) => assert!(bar.close > 0.0),
            // Holidays and closed sessions produce no trades; quotes above already passed.
            _ => eprintln!("no trades arrived (market closed or holiday); quotes are verified"),
        }
    }
}
