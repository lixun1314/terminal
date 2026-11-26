//! Alpaca Market Data v2 (free tier uses the IEX feed).
//! Keys come from `APCA_API_KEY_ID` / `APCA_API_SECRET_KEY`.

use std::time::Duration;

use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::Deserialize;
use serde_json::json;
use tokio::sync::mpsc;
use tr_core::{AssetClass, Bar, Security, Timeframe};

use crate::{LiveFeed, MarketData, Quote, rt, security, ws};

const BASE: &str = "https://data.alpaca.markets/v2/stocks";

/// US equity/ETF symbols only; A/HK suffixes have their own provider.
fn alpaca_symbol(sec: &Security) -> Option<String> {
    (matches!(sec.class, AssetClass::Equity | AssetClass::Etf)
        && !matches!(sec.ticker.rsplit_once('.'), Some((_, "SS" | "SZ" | "HK"))))
    .then(|| sec.ticker.clone())
}

pub struct AlpacaProvider {
    client: reqwest::Client,
    key: String,
    secret: String,
    feed: String,
}

impl AlpacaProvider {
    pub fn new(key: String, secret: String, feed: String) -> Result<Self> {
        let client = tr_core::http::client_builder().tls_backend_rustls().timeout(Duration::from_secs(20)).build()?;
        Ok(Self { client, key, secret, feed })
    }
}

fn timeframe(tf: Timeframe) -> &'static str {
    match tf {
        Timeframe::M1 => "1Min",
        Timeframe::M5 => "5Min",
        Timeframe::M15 => "15Min",
        Timeframe::M30 => "30Min",
        Timeframe::H1 => "1Hour",
        Timeframe::H4 => "4Hour",
        Timeframe::D1 => "1Day",
        Timeframe::W1 => "1Week",
        Timeframe::MN1 => "1Month",
    }
}

#[derive(Deserialize)]
struct BarsPage {
    #[serde(default)]
    bars: Option<Vec<RawBar>>,
    next_page_token: Option<String>,
}

#[derive(Deserialize)]
struct RawBar {
    t: DateTime<Utc>,
    o: f64,
    h: f64,
    l: f64,
    c: f64,
    v: f64,
}

/// Parse one page of `/v2/stocks/{symbol}/bars`.
pub fn parse_bars_page(json: &str) -> Result<(Vec<Bar>, Option<String>)> {
    let page: BarsPage = serde_json::from_str(json).context("parsing Alpaca bars")?;
    let bars =
        page.bars.unwrap_or_default().into_iter().map(|b| Bar::new(b.t.timestamp(), b.o, b.h, b.l, b.c, b.v)).collect();
    Ok((bars, page.next_page_token.filter(|t| !t.is_empty())))
}

fn parse_ts(s: &str) -> Option<i64> {
    DateTime::parse_from_rfc3339(s).ok().map(|d| d.timestamp())
}

/// Batch snapshots (`/v2/stocks/snapshots`): last trade price and previous daily close.
pub fn parse_snapshots(text: &str, pairs: &[(String, String)]) -> Result<Vec<Quote>> {
    let v: serde_json::Value = serde_json::from_str(text).context("parsing Alpaca snapshots")?;
    let obj = v.as_object().context("Alpaca snapshots: not an object")?;
    let num = |p: &serde_json::Value| p.as_f64().filter(|x| *x > 0.0);
    let mut out = Vec::new();
    for (key, sym) in pairs {
        let Some(s) = obj.get(sym) else { continue };
        let Some(last) =
            num(&s["latestTrade"]["p"]).or_else(|| num(&s["minuteBar"]["c"])).or_else(|| num(&s["dailyBar"]["c"]))
        else {
            continue;
        };
        let prev = num(&s["prevDailyBar"]["c"]).or_else(|| num(&s["dailyBar"]["o"])).unwrap_or(last);
        let ts = s["latestTrade"]["t"]
            .as_str()
            .and_then(parse_ts)
            .or_else(|| s["minuteBar"]["t"].as_str().and_then(parse_ts))
            .unwrap_or_else(|| Utc::now().timestamp());
        out.push(Quote { symbol: key.clone(), last, prev_close: prev, ts, spark: vec![], ext: None });
    }
    Ok(out)
}

impl AlpacaProvider {
    /// Batch quotes from snapshots (IEX on the free feed).
    pub async fn quotes(&self, keys: &[String]) -> Result<Vec<Quote>> {
        let pairs: Vec<(String, String)> =
            keys.iter().filter_map(|k| alpaca_symbol(&security(k)).map(|s| (k.clone(), s))).collect();
        if pairs.is_empty() {
            return Ok(Vec::new());
        }
        let syms: Vec<&str> = pairs.iter().map(|(_, s)| s.as_str()).collect();
        let resp = self
            .client
            .get(format!("{BASE}/snapshots"))
            .header("APCA-API-KEY-ID", &self.key)
            .header("APCA-API-SECRET-KEY", &self.secret)
            .query(&[("symbols", syms.join(",")), ("feed", self.feed.clone())])
            .send()
            .await?;
        if !resp.status().is_success() {
            bail!("alpaca snapshots: HTTP {}", resp.status());
        }
        parse_snapshots(&resp.text().await?, &pairs)
    }

    /// Realtime trades over the Alpaca websocket, aggregated into chart bars.
    pub fn live(&self, sec: &Security, tf: Timeframe) -> Option<LiveFeed> {
        if !tf.is_intraday() {
            return None;
        }
        let sym = alpaca_symbol(sec)?;
        let (tx, rx) = mpsc::channel(512);
        let (key, secret, feed) = (self.key.clone(), self.secret.clone(), self.feed.clone());
        rt::spawn(async move {
            let mut backoff = 1u64;
            while !tx.is_closed() {
                match alpaca_trades(&key, &secret, &feed, &sym, tf, &tx).await {
                    Ok(()) => {
                        if tx.is_closed() {
                            return;
                        }
                        backoff = 1;
                    }
                    Err(e) => {
                        tracing::debug!("alpaca live {sym}: {e:#}");
                        rt::sleep(Duration::from_secs(backoff)).await;
                        backoff = (backoff * 2).min(30);
                    }
                }
            }
        });
        Some(rx)
    }
}

async fn alpaca_trades(
    key: &str,
    secret: &str,
    feed: &str,
    sym: &str,
    tf: Timeframe,
    tx: &mpsc::Sender<Bar>,
) -> Result<()> {
    let mut sock =
        ws::Socket::connect_with_headers(&format!("wss://stream.data.alpaca.markets/v2/{feed}"), &[]).await?;
    let _ = sock.next().await.context("alpaca ws: no welcome")??;
    sock.send(json!({"action":"auth","key":key,"secret":secret}).to_string()).await?;
    wait_for(&mut sock, "authenticated").await?;
    sock.send(json!({"action":"subscribe","trades":[sym]}).to_string()).await?;
    let mut cur: Option<Bar> = None;
    while let Some(msg) = sock.next().await {
        let Ok(items) = serde_json::from_str::<Vec<serde_json::Value>>(&msg?) else { continue };
        for m in items {
            match m["T"].as_str() {
                Some("t") if m["S"].as_str() == Some(sym) => {
                    let (Some(price), size) = (m["p"].as_f64(), m["s"].as_f64().unwrap_or(0.0)) else {
                        continue;
                    };
                    let ts = m["t"].as_str().and_then(parse_ts).unwrap_or_else(|| Utc::now().timestamp());
                    let bucket = ts - ts.rem_euclid(bucket_secs(tf));
                    let bar = match &mut cur {
                        Some(b) if b.ts == bucket => {
                            b.high = b.high.max(price);
                            b.low = b.low.min(price);
                            b.close = price;
                            b.volume += size;
                            *b
                        }
                        _ => {
                            let b =
                                Bar { ts: bucket, open: price, high: price, low: price, close: price, volume: size };
                            cur = Some(b);
                            b
                        }
                    };
                    if tx.send(bar).await.is_err() {
                        return Ok(());
                    }
                }
                Some("error") => bail!("alpaca ws: {}", m["msg"].as_str().unwrap_or("error")),
                _ => {}
            }
        }
    }
    bail!("alpaca ws closed")
}

async fn wait_for(sock: &mut ws::Socket, what: &str) -> Result<()> {
    for _ in 0..10 {
        let Some(t) = sock.next().await else { bail!("alpaca ws closed") };
        let text = t?;
        if text.contains(what) {
            return Ok(());
        }
        if text.contains("\"error\"") {
            bail!("alpaca ws: {text}");
        }
    }
    bail!("alpaca ws: no {what}")
}

fn bucket_secs(tf: Timeframe) -> i64 {
    match tf {
        Timeframe::M1 => 60,
        Timeframe::M5 => 300,
        Timeframe::M15 => 900,
        Timeframe::M30 => 1800,
        Timeframe::H1 => 3600,
        Timeframe::H4 => 14400,
        _ => 86400,
    }
}

#[async_trait]
impl MarketData for AlpacaProvider {
    fn name(&self) -> &str {
        "alpaca"
    }

    async fn history(&self, symbol: &str, tf: Timeframe, end: Option<i64>, limit: usize) -> Result<Vec<Bar>> {
        // Alpaca free data has a 15 minute delay for the latest bars on the SIP feed;
        // IEX is real-time. Walk backwards from `end` with sort=desc.
        let end_ts = end.unwrap_or_else(|| Utc::now().timestamp());
        let end_dt = DateTime::<Utc>::from_timestamp(end_ts - 1, 0).unwrap_or_else(Utc::now);
        let mut out: Vec<Bar> = Vec::with_capacity(limit);
        let mut token: Option<String> = None;
        while out.len() < limit {
            let mut q: Vec<(&str, String)> = vec![
                ("timeframe", timeframe(tf).into()),
                ("end", end_dt.to_rfc3339()),
                ("start", "2000-01-01T00:00:00Z".into()),
                ("limit", (limit - out.len()).min(10_000).to_string()),
                ("adjustment", "split".into()),
                ("feed", self.feed.clone()),
                ("sort", "desc".into()),
            ];
            if let Some(t) = &token {
                q.push(("page_token", t.clone()));
            }
            let resp = self
                .client
                .get(format!("{BASE}/{symbol}/bars"))
                .header("APCA-API-KEY-ID", &self.key)
                .header("APCA-API-SECRET-KEY", &self.secret)
                .query(&q)
                .send()
                .await?;
            if !resp.status().is_success() {
                bail!("alpaca {symbol}: HTTP {}", resp.status());
            }
            let (bars, next) = parse_bars_page(&resp.text().await?)?;
            let n = bars.len();
            out.extend(bars);
            token = next;
            if token.is_none() || n == 0 {
                break;
            }
        }
        out.sort_by_key(|b| b.ts);
        out.dedup_by_key(|b| b.ts);
        Ok(crate::clip(out, end, limit))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_fixture() {
        let (bars, next) = parse_bars_page(include_str!("../tests/fixtures/alpaca_bars.json")).unwrap();
        assert_eq!(bars.len(), 2);
        assert_eq!(bars[1].close, 336.64);
        assert_eq!(bars[0].ts, 1_791_432_000);
        assert!(next.is_none());
        let (empty, _) = parse_bars_page(r#"{"bars":null,"next_page_token":null}"#).unwrap();
        assert!(empty.is_empty());
    }

    #[test]
    fn parses_snapshots() {
        let text = r#"{
            "AAPL": {"latestTrade":{"t":"2026-11-26T15:30:00Z","p":351.25},
                     "prevDailyBar":{"t":"2026-11-25T05:00:00Z","o":349.0,"c":349.08}},
            "MSFT": {"minuteBar":{"t":"2026-11-26T15:29:00Z","c":493.6},"dailyBar":{"o":492.0,"c":493.6}}
        }"#;
        let pairs = vec![
            ("AAPL US Equity".to_string(), "AAPL".to_string()),
            ("MSFT US Equity".to_string(), "MSFT".to_string()),
            ("BRK.B US Equity".to_string(), "BRK.B".to_string()),
        ];
        let q = parse_snapshots(text, &pairs).unwrap();
        assert_eq!(q.len(), 2, "missing symbols are skipped");
        assert_eq!(q[0].last, 351.25);
        assert_eq!(q[0].prev_close, 349.08);
        assert_eq!(q[0].symbol, "AAPL US Equity");
        assert_eq!(q[1].last, 493.6);
        assert_eq!(q[1].prev_close, 492.0);
    }
}
