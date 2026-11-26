//! TradingView as a *backup* quote and search source.
//!
//! The scanner endpoint batches quotes without any API key; a logged-in session cookie
//! (`sessionid` + `sessionid_sign`) upgrades US equities and FX to realtime, while
//! futures and mainland A-shares stay 10–15 minutes delayed. The cookie is optional and
//! never logged: it comes from `TR_TV_COOKIE` or the first line of
//! `<data dir>/tradingview.cookie`.
//!
//! This is an undocumented private API: it is used only to fill gaps when the primary
//! source fails (Yahoo blocked/throttled). See the README for the terms-of-use caveat.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use serde_json::json;
use tr_core::{AssetClass, Security, catalog};

use crate::{ExtQuote, ExtSession, Quote, SearchHit, security};

const SCAN: &str = "https://scanner.tradingview.com/global/scan";
const SEARCH: &str = "https://symbol-search.tradingview.com/symbol_search/v3/";
const UA: &str =
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36";

/// A TradingView symbol string (`NASDAQ:AAPL`, `NYMEX:CL1!`).
pub type TvSymbol = String;

/// Where the stable TradingView conventions are known without a lookup.
fn static_symbol(sec: &Security) -> Option<TvSymbol> {
    let t = sec.ticker.as_str();
    let s = match sec.class {
        AssetClass::Cmdty => match t {
            "CL1" => "NYMEX:CL1!",
            "CO1" => "NYMEX:BZ1!",
            "NG1" => "NYMEX:NG1!",
            "XB1" => "NYMEX:RB1!",
            "HO1" => "NYMEX:HO1!",
            "GC1" => "COMEX:GC1!",
            "SI1" => "COMEX:SI1!",
            "HG1" => "COMEX:HG1!",
            "PL1" => "NYMEX:PL1!",
            "PA1" => "NYMEX:PA1!",
            "ALI1" => "COMEX:ALI1!",
            "TIO1" => "COMEX:TIO1!",
            "C1" => "CBOT:ZC1!",
            "W1" => "CBOT:ZW1!",
            "S1" => "CBOT:ZS1!",
            "KC1" => "ICEUS:KC1!",
            "CT1" => "ICEUS:CT1!",
            "SB1" => "ICEUS:SB1!",
            "CC1" => "ICEUS:CC1!",
            "LE1" => "CME:LE1!",
            "HE1" => "CME:HE1!",
            "OJ1" => "ICEUS:OJ1!",
            _ => return None,
        },
        AssetClass::Govt => match t {
            "US1M" => "TVC:US01M",
            "US3M" => "TVC:US03M",
            "US6M" => "TVC:US06M",
            "US1Y" => "TVC:US01Y",
            "US2Y" => "TVC:US02Y",
            "US3Y" => "TVC:US03Y",
            "US5Y" => "TVC:US05Y",
            "US7Y" => "TVC:US07Y",
            "US10Y" => "TVC:US10Y",
            "US20Y" => "TVC:US20Y",
            "US30Y" => "TVC:US30Y",
            _ => return None,
        },
        AssetClass::Fx => return Some(format!("FX:{}", t)),
        AssetClass::Crypto => {
            let (base, quote) = sec.pair()?;
            return Some(format!("BINANCE:{base}{}", if matches!(quote, "USD" | "USDT") { "USDT" } else { quote }));
        }
        AssetClass::Index => match t {
            "SPX" => "SP:SPX",
            "INDU" => "DJ:DJI",
            "CCMP" => "NASDAQ:IXIC",
            "NDX" => "NASDAQ:NDX",
            "VIX" => "CBOE:VIX",
            "NKY" => "TVC:NI225",
            "HSI" => "TVC:HSI",
            "DAX" => "TVC:DAX",
            "UKX" => "TVC:UKX",
            "SX5E" => "TVC:SX5E",
            "CAC" => "TVC:CAC40",
            "SHCOMP" => "SSE:000001",
            "KOSPI" => "KRX:KOSPI",
            _ => return None,
        },
        // Equities, ETFs and other indices are resolved by symbol search (exchange unknown).
        AssetClass::Equity | AssetClass::Etf | AssetClass::Perp => return None,
    };
    Some(s.into())
}

/// Backup provider; construct once and reuse (it caches symbol resolutions).
pub struct TradingView {
    client: reqwest::Client,
    /// key → resolved TradingView symbol (`None` = no match, cached).
    resolved: Mutex<HashMap<String, Option<TvSymbol>>>,
}

impl TradingView {
    pub fn new() -> Result<Self> {
        let cookie = cookie();
        let mut headers = reqwest::header::HeaderMap::new();
        headers
            .insert(reqwest::header::ORIGIN, reqwest::header::HeaderValue::from_static("https://cn.tradingview.com"));
        headers
            .insert(reqwest::header::REFERER, reqwest::header::HeaderValue::from_static("https://cn.tradingview.com/"));
        if let Some(c) = &cookie {
            headers.insert(reqwest::header::COOKIE, c.parse().context("tradingview cookie has invalid characters")?);
        }
        let client = tr_core::http::client_builder()
            .tls_backend_rustls()
            .user_agent(UA)
            .default_headers(headers)
            .timeout(Duration::from_secs(20))
            .build()?;
        Ok(Self { client, resolved: Mutex::new(HashMap::new()) })
    }

    /// Whether a session cookie is configured (realtime US equities/FX when it is).
    pub fn cookie_configured(&self) -> bool {
        cookie().is_some()
    }

    /// Batch quotes for market keys; unmapped keys are left out.
    pub async fn quotes(&self, keys: &[String]) -> Result<Vec<Quote>> {
        let mut pairs: Vec<(String, TvSymbol)> = Vec::new();
        for key in keys {
            if let Some(sym) = self.symbol(key).await {
                pairs.push((key.clone(), sym));
            }
        }
        if pairs.is_empty() {
            return Ok(Vec::new());
        }
        let tickers: Vec<&str> = pairs.iter().map(|(_, s)| s.as_str()).collect();
        let body = json!({
            "symbols": { "tickers": tickers },
            "columns": [
                "close", "change_abs", "change",
                "postmarket_close", "postmarket_change", "postmarket_change_abs",
                "premarket_close", "premarket_change", "premarket_change_abs"
            ]
        });
        let resp = self.client.post(SCAN).json(&body).send().await.context("tradingview scanner")?;
        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();
        if !status.is_success() {
            bail!("tradingview scanner: HTTP {status}");
        }
        let rows = parse_scan(&text)?;
        let mut out = Vec::new();
        let now = SystemTime::now().duration_since(UNIX_EPOCH).map_or(0, |d| d.as_secs() as i64);
        let ext_session = us_extended_session(now);
        for (key, sym) in pairs {
            let Some(row) = rows.iter().find(|r| r.symbol == sym) else { continue };
            let Some(last) = row.close.filter(|v| *v > 0.0) else { continue };
            let prev_close = row
                .change_abs
                .map(|c| last - c)
                .or_else(|| row.change_pct.map(|p| last / (1.0 + p / 100.0)))
                .filter(|v| *v > 0.0)
                .unwrap_or(last);
            // Extended-hours prints only for listings with US pre/post sessions.
            let ext = match ext_session {
                Some(session) if matches!(security(&key).class, AssetClass::Equity | AssetClass::Etf) => {
                    extended_quote(row, session)
                }
                _ => None,
            };
            out.push(Quote { symbol: key, last, prev_close, ts: now, spark: vec![last], ext });
        }
        Ok(out)
    }

    /// Symbol search, mapped back to the app's market keys where the catalog knows them.
    pub async fn search(&self, query: &str) -> Result<Vec<SearchHit>> {
        let resp = self
            .client
            .get(SEARCH)
            .query(&[("text", query), ("hl", "1"), ("lang", "en"), ("domain", "production")])
            .send()
            .await
            .context("tradingview search")?;
        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();
        if !status.is_success() {
            bail!("tradingview search: HTTP {status}");
        }
        Ok(parse_search(&text))
    }

    /// The TradingView symbol for a market key, resolving equities/indices by symbol search
    /// (cached, including misses).
    pub async fn resolve(&self, key: &str) -> Option<TvSymbol> {
        self.symbol(key).await
    }

    /// Resolve a market key to a TradingView symbol, caching lookups (including misses).
    async fn symbol(&self, key: &str) -> Option<TvSymbol> {
        if let Some(hit) = self.resolved.lock().expect("symbol cache").get(key) {
            return hit.clone();
        }
        let sec = security(key);
        let mut sym = static_symbol(&sec);
        if sym.is_none() && matches!(sec.class, AssetClass::Equity | AssetClass::Etf | AssetClass::Index) {
            sym = self.lookup(&sec.ticker).await;
        }
        self.resolved.lock().expect("symbol cache").insert(key.to_string(), sym.clone());
        sym
    }

    /// Exact-ticker search for a listed security (the exchange varies by listing).
    async fn lookup(&self, ticker: &str) -> Option<TvSymbol> {
        let hits = self.search(ticker).await.ok()?;
        let hit = hits.into_iter().find(|h| {
            !h.key.starts_with("xyz:") && h.key.split(' ').next().is_some_and(|t| t.eq_ignore_ascii_case(ticker))
        })?;
        let exchange = hit.exchange?;
        let symbol = hit.key.split(' ').next()?;
        Some(format!("{exchange}:{symbol}"))
    }
}

/// A session cookie from the environment or the data directory, without logging it.
fn cookie() -> Option<String> {
    let raw = std::env::var("TR_TV_COOKIE").ok().or_else(|| {
        #[cfg(not(target_arch = "wasm32"))]
        {
            tr_core::http::config_line("tradingview.cookie")
        }
        #[cfg(target_arch = "wasm32")]
        {
            None
        }
    })?;
    let raw = raw.trim();
    raw.contains("sessionid").then(|| raw.to_string())
}

/// The session cookie, if configured (realtime entitlements). Exposed for the live feed.
pub fn session_cookie() -> Option<String> {
    cookie()
}

/// The TradingView symbol for a security, where the convention is known.
pub fn tv_symbol_for(sec: &Security) -> Option<TvSymbol> {
    static_symbol(sec)
}

/// US pre/post-market session at `ts`, or `None` during the regular session and on
/// weekends. Holidays are not modelled (the quote simply keeps the last regular print).
pub fn us_extended_session(ts: i64) -> Option<ExtSession> {
    use chrono::{Datelike, Timelike};
    let t = chrono::DateTime::from_timestamp(ts, 0)?.with_timezone(&chrono_tz::America::New_York);
    if matches!(t.weekday(), chrono::Weekday::Sat | chrono::Weekday::Sun) {
        return None;
    }
    let mins = t.hour() * 60 + t.minute();
    match mins {
        240..=569 => Some(ExtSession::Pre), // 04:00–09:29 ET
        570..=959 => None,                  // regular session
        _ => Some(ExtSession::Post),        // 16:00–03:59 ET (keeps the last after-hours print)
    }
}

/// The session right now, for callers that only need "is the market outside its regular hours".
pub fn extended_session_now() -> Option<ExtSession> {
    let now = SystemTime::now().duration_since(UNIX_EPOCH).ok()?.as_secs() as i64;
    us_extended_session(now)
}

/// Strip search highlighting from a TradingView symbol/description.
fn strip_tags(s: &str) -> String {
    s.replace("<em>", "").replace("</em>", "").replace("&amp;", "&")
}

fn extended_quote(row: &ScanRow, session: ExtSession) -> Option<ExtQuote> {
    let (price, pct, abs) = match session {
        ExtSession::Pre => (row.pre_close, row.pre_pct, row.pre_abs),
        ExtSession::Post => (row.post_close, row.post_pct, row.post_abs),
    };
    price.filter(|v| *v > 0.0).map(|price| ExtQuote {
        price,
        change: abs.or(pct.map(|p| price - price / (1.0 + p / 100.0))).unwrap_or(0.0),
        change_pct: pct.unwrap_or(0.0),
        session,
    })
}

#[derive(Debug, Default)]
struct ScanRow {
    symbol: String,
    close: Option<f64>,
    change_abs: Option<f64>,
    change_pct: Option<f64>,
    post_close: Option<f64>,
    post_pct: Option<f64>,
    post_abs: Option<f64>,
    pre_close: Option<f64>,
    pre_pct: Option<f64>,
    pre_abs: Option<f64>,
}

fn parse_scan(text: &str) -> Result<Vec<ScanRow>> {
    let v: serde_json::Value = serde_json::from_str(text).context("parsing tradingview scanner")?;
    Ok(v["data"]
        .as_array()
        .map(|a| {
            a.iter()
                .filter_map(|r| {
                    let symbol = r["s"].as_str()?.to_string();
                    let d = r["d"].as_array()?;
                    Some(ScanRow {
                        symbol,
                        close: d.first().and_then(|x| x.as_f64()),
                        change_abs: d.get(1).and_then(|x| x.as_f64()),
                        change_pct: d.get(2).and_then(|x| x.as_f64()),
                        post_close: d.get(3).and_then(|x| x.as_f64()),
                        post_pct: d.get(4).and_then(|x| x.as_f64()),
                        post_abs: d.get(5).and_then(|x| x.as_f64()),
                        pre_close: d.get(6).and_then(|x| x.as_f64()),
                        pre_pct: d.get(7).and_then(|x| x.as_f64()),
                        pre_abs: d.get(8).and_then(|x| x.as_f64()),
                    })
                })
                .collect()
        })
        .unwrap_or_default())
}

fn parse_search(text: &str) -> Vec<SearchHit> {
    let v: serde_json::Value = serde_json::from_str(text).unwrap_or_default();
    v["symbols"].as_array().map(|a| a.iter().filter_map(search_hit).collect()).unwrap_or_default()
}

fn search_hit(q: &serde_json::Value) -> Option<SearchHit> {
    let ticker = strip_tags(q["symbol"].as_str()?).trim_end_matches('!').to_string();
    if ticker.is_empty() {
        return None;
    }
    let class = match q["type"].as_str().unwrap_or_default() {
        "stock" => AssetClass::Equity,
        "fund" | "etf" => AssetClass::Etf,
        "index" | "economic" => AssetClass::Index,
        "futures" => AssetClass::Cmdty,
        "forex" => AssetClass::Fx,
        "crypto" => AssetClass::Crypto,
        _ => return None,
    };
    let name = strip_tags(q["description"].as_str().unwrap_or(""));
    let exchange = q["exchange"].as_str().map(str::to_string);
    // Prefer the catalog key when it already lists this ticker in the same class.
    let key = catalog::CATALOG
        .iter()
        .find(|l| l.class == class && l.ticker.eq_ignore_ascii_case(&ticker))
        .map(|l| l.key())
        .unwrap_or_else(|| Security::new(ticker, class).key());
    Some(SearchHit { key, name, class, exchange })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_the_stable_symbols() {
        assert_eq!(static_symbol(&security("CL1 Comdty")).as_deref(), Some("NYMEX:CL1!"));
        assert_eq!(static_symbol(&security("HG1 Comdty")).as_deref(), Some("COMEX:HG1!"));
        assert_eq!(static_symbol(&security("TIO1 Comdty")).as_deref(), Some("COMEX:TIO1!"));
        assert_eq!(static_symbol(&security("US10Y Govt")).as_deref(), Some("TVC:US10Y"));
        assert_eq!(static_symbol(&security("EURUSD Curncy")).as_deref(), Some("FX:EURUSD"));
        assert_eq!(static_symbol(&security("BTCUSD Curncy")).as_deref(), Some("BINANCE:BTCUSDT"));
        assert_eq!(static_symbol(&security("SPX Index")).as_deref(), Some("SP:SPX"));
        assert_eq!(static_symbol(&security("AAPL US Equity")), None, "resolved by search");
    }

    #[test]
    fn parses_scanner_rows() {
        let text = r#"{"totalCount":2,"data":[
            {"s":"NASDAQ:AAPL","d":[349.08,6.52,1.9033,353.31,-0.21,-0.75,348.2,-0.25,-0.88]},
            {"s":"NYMEX:CL1!","d":[81.16,null,-1.23,null,null,null,null,null,null]}
        ]}"#;
        let rows = parse_scan(text).unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].symbol, "NASDAQ:AAPL");
        assert_eq!(rows[0].close, Some(349.08));
        assert_eq!(rows[0].change_abs, Some(6.52));
        assert_eq!(rows[0].post_close, Some(353.31));
        assert_eq!(rows[0].pre_close, Some(348.2));
        assert_eq!(rows[1].change_abs, None);
        assert_eq!(rows[1].post_close, None);
        assert!(parse_scan("not json").is_err());
        assert!(parse_scan("{}").unwrap().is_empty());
    }

    #[test]
    fn extended_quotes_and_us_sessions() {
        let ts = |s: &str| chrono::DateTime::parse_from_rfc3339(s).unwrap().timestamp();
        assert_eq!(us_extended_session(ts("2026-11-24T13:00:00Z")), Some(ExtSession::Pre), "08:00 EST");
        assert_eq!(us_extended_session(ts("2026-11-24T17:00:00Z")), None, "12:00 EST");
        assert_eq!(us_extended_session(ts("2026-11-24T22:00:00Z")), Some(ExtSession::Post), "17:00 EST");
        assert_eq!(us_extended_session(ts("2026-11-25T02:00:00Z")), Some(ExtSession::Post), "21:00 EST");
        assert_eq!(us_extended_session(ts("2026-11-28T17:00:00Z")), None, "Saturday");
        let row = ScanRow {
            symbol: "NASDAQ:AAPL".into(),
            close: Some(354.0),
            post_close: Some(353.31),
            post_pct: Some(-0.21),
            post_abs: Some(-0.75),
            pre_close: Some(348.2),
            pre_pct: Some(-0.25),
            pre_abs: Some(-0.88),
            ..Default::default()
        };
        let post = extended_quote(&row, ExtSession::Post).unwrap();
        assert_eq!(post.price, 353.31);
        assert_eq!(post.change, -0.75);
        assert_eq!(post.session.label(), "AH");
        let pre = extended_quote(&row, ExtSession::Pre).unwrap();
        assert_eq!(pre.price, 348.2);
        assert_eq!(pre.session.label(), "PRE");
        let empty = ScanRow { symbol: "X".into(), ..Default::default() };
        assert!(extended_quote(&empty, ExtSession::Post).is_none());
    }

    #[test]
    fn parses_search_hits_into_market_keys() {
        let text = r#"{"symbols":[
            {"symbol":"<em>AAPL</em>","description":"Apple Inc.","type":"stock","exchange":"NASDAQ"},
            {"symbol":"HG1!","description":"Copper Futures","type":"futures","exchange":"COMEX"},
            {"symbol":"BTCUSDT","description":"Bitcoin / TetherUS","type":"crypto","exchange":"BINANCE"},
            {"symbol":"OZ1!","description":"Oats Futures","type":"futures","exchange":"CBOT"},
            {"symbol":"BOND","description":"Some Bond","type":"bond","exchange":"FINRA"}
        ]}"#;
        let hits = parse_search(text);
        assert_eq!(hits.len(), 4, "bonds are skipped");
        assert_eq!(hits[0].key, "AAPL US Equity");
        assert_eq!(hits[0].class, AssetClass::Equity);
        assert_eq!(hits[0].exchange.as_deref(), Some("NASDAQ"));
        assert_eq!(hits[1].key, "HG1 Comdty", "catalog key wins");
        assert_eq!(hits[2].key, "BTCUSDT Curncy");
        assert_eq!(hits[3].key, "OZ1 Comdty");
    }
}
