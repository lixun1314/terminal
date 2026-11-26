//! TradingView chart push + source-consistent history.
//!
//! US equities/ETFs are realtime when a session cookie is present; indices and futures are
//! **delayed but pushed** (`delayed_streaming_*`) — the same behaviour the TradingView web
//! chart shows. To avoid mixing two vendors' price scales, a symbol that uses TV push also
//! loads its chart history from TV (one-shot over the same socket); Yahoo/Tencent remain the
//! fallbacks. `TR_PUSH=yahoo` disables TV entirely.
//!
//! Protocol: `~m~<len>~m~<json>` frames over `wss://data.tradingview.com/socket.io/websocket`,
//! `~h~N` heartbeats echoed back. `Origin` is mandatory (403 without it).

use std::time::Duration;

use anyhow::{Result, anyhow, bail};
use tokio::sync::mpsc;
use tr_core::{AssetClass, Bar, Security, Timeframe};

use crate::tradingview::TradingView;
use crate::{LiveFeed, cn, rt, ws};

const URL: &str = "wss://data.tradingview.com/socket.io/websocket?from=chart%2F&type=chart";
const UA: &str =
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36";

/// Classes whose charts can ride TradingView's push feed.
pub fn eligible(sec: &Security) -> bool {
    !cn::is_cn(sec)
        && matches!(
            sec.class,
            AssetClass::Equity
                | AssetClass::Etf
                | AssetClass::Index
                | AssetClass::Fx
                | AssetClass::Cmdty
                | AssetClass::Govt
        )
}

/// `TR_PUSH=yahoo` (or `off`/`0`/`false`) disables the TradingView push source, and so does
/// the Settings › Data source = Yahoo preference.
pub fn push_enabled() -> bool {
    if matches!(crate::source_pref(), crate::SourcePref::Yahoo | crate::SourcePref::Alpaca) {
        return false;
    }
    match std::env::var("TR_PUSH") {
        Ok(v) => !matches!(v.trim().to_ascii_lowercase().as_str(), "yahoo" | "off" | "0" | "false"),
        Err(_) => true,
    }
}

fn resolution(tf: Timeframe) -> &'static str {
    match tf {
        Timeframe::M1 => "1",
        Timeframe::M5 => "5",
        Timeframe::M15 => "15",
        Timeframe::M30 => "30",
        Timeframe::H1 => "60",
        Timeframe::H4 => "240",
        Timeframe::D1 => "1D",
        Timeframe::W1 => "1W",
        Timeframe::MN1 => "1M",
    }
}

fn frame(m: &str, p: serde_json::Value) -> String {
    let b = serde_json::json!({ "m": m, "p": p }).to_string();
    format!("~m~{}~m~{b}", b.len())
}

fn headers() -> Vec<(&'static str, &'static str)> {
    vec![("Origin", "https://cn.tradingview.com"), ("User-Agent", UA)]
}

/// The TradingView symbol for a security (catalog conventions, then a symbol search).
pub async fn tv_symbol(sec: &Security) -> Option<String> {
    let tv = TradingView::new().ok()?;
    tv.resolve(&sec.key()).await
}

async fn connect(sec: &Security) -> Result<(ws::Socket, String)> {
    let sym = tv_symbol(sec).await.ok_or_else(|| anyhow!("no TradingView symbol for {}", sec.ticker))?;
    let cookie = crate::tradingview::session_cookie();
    let mut hs = headers();
    if let Some(c) = &cookie {
        hs.push(("Cookie", c.as_str()));
    }
    let sock = ws::Socket::connect_with_headers(URL, &hs).await?;
    Ok((sock, sym))
}

/// A one-shot history fetch over the chart socket: latest `limit` bars of `tf`.
pub async fn history(sec: &Security, tf: Timeframe, limit: usize) -> Result<Vec<Bar>> {
    let (mut sock, sym) = connect(sec).await?;
    sock.send(frame("set_auth_token", serde_json::json!(["unauthorized_user_token"]))).await?;
    sock.send(frame("chart_create_session", serde_json::json!(["cs_hist", ""]))).await?;
    let symarg = serde_json::json!({ "adjustment": "splits", "symbol": sym }).to_string();
    sock.send(frame("resolve_symbol", serde_json::json!(["cs_hist", "sym1", format!("={symarg}")]))).await?;
    let count = limit.clamp(300, 5000);
    sock.send(frame("create_series", serde_json::json!(["cs_hist", "s1", "s1", "sym1", resolution(tf), count, ""])))
        .await?;
    let mut out: Vec<Bar> = Vec::new();
    for _ in 0..40 {
        let Some(msg) = rt::timeout(Duration::from_secs(20), sock.next()).await.ok().flatten() else {
            break;
        };
        let text = msg?;
        for body in bodies(&text) {
            if let Some(h) = body.strip_prefix("~h~") {
                sock.send(format!("~m~{}~m~{h}", body.len())).await?;
                continue;
            }
            let Ok(v) = serde_json::from_str::<serde_json::Value>(body) else { continue };
            match v["m"].as_str().unwrap_or_default() {
                "timescale_update" => {
                    if let Some(bars) = v["p"][1]["s1"]["s"].as_array() {
                        out = bars.iter().filter_map(parse_bar).collect();
                    }
                    if !out.is_empty() {
                        out.sort_by_key(|b| b.ts);
                        out.dedup_by_key(|b| b.ts);
                        let skip = out.len().saturating_sub(limit);
                        return Ok(out.split_off(skip));
                    }
                }
                "critical_error" | "protocol_error" => bail!("tradingview: history rejected for {sym}"),
                _ => {}
            }
        }
    }
    if out.is_empty() {
        bail!("tradingview: no history for {sym}");
    }
    Ok(out)
}

/// A live feed: TradingView push (when `allow_tv`), Tencent quotes, then Yahoo's tail poll.
pub fn feed(sec: &Security, tf: Timeframe, allow_tv: bool, allow_cn: bool) -> LiveFeed {
    let (tx, rx) = mpsc::channel(256);
    let sec = sec.clone();
    rt::spawn(async move {
        let mut tier = if allow_tv && push_enabled() && eligible(&sec) {
            0u8
        } else if allow_cn {
            1
        } else {
            2
        };
        tracing::debug!("live {} {}: tier {tier} (0=TV, 1=Tencent, 2=Yahoo)", sec.ticker, tf);
        let mut backoff = 1u64;
        while !tx.is_closed() {
            let res = match tier {
                0 => tv_run(&sec, tf, &tx).await,
                1 => tencent_run(&sec, tf, &tx).await,
                _ => yahoo_run(&sec, tf, &tx).await,
            };
            match res {
                Ok(()) => {
                    if tx.is_closed() {
                        return;
                    }
                    tier = if allow_tv && push_enabled() && eligible(&sec) {
                        0
                    } else if allow_cn {
                        1
                    } else {
                        2
                    };
                    backoff = 1;
                }
                Err(e) => {
                    tracing::debug!("live tier {tier} for {}: {e:#}", sec.ticker);
                    // Stay on the source that produced the chart's history; only an unknown
                    // source falls through to the next tier. Switching vendors mid-chart is
                    // what produced mismatched scales before.
                    let pinned = (tier == 0 && allow_tv) || (tier == 1 && allow_cn);
                    if !pinned {
                        tier = (tier + 1).min(2);
                    }
                    rt::sleep(Duration::from_secs(backoff)).await;
                    backoff = (backoff * 2).min(30);
                }
            }
        }
    });
    rx
}

async fn tv_run(sec: &Security, tf: Timeframe, tx: &mpsc::Sender<Bar>) -> Result<()> {
    let (mut sock, sym) = connect(sec).await?;
    sock.send(frame("set_auth_token", serde_json::json!(["unauthorized_user_token"]))).await?;
    sock.send(frame("chart_create_session", serde_json::json!(["cs_live", ""]))).await?;
    let symarg = serde_json::json!({ "adjustment": "splits", "symbol": sym }).to_string();
    sock.send(frame("resolve_symbol", serde_json::json!(["cs_live", "sym1", format!("={symarg}")]))).await?;
    sock.send(frame("create_series", serde_json::json!(["cs_live", "s1", "s1", "sym1", resolution(tf), 300, ""])))
        .await?;
    while !tx.is_closed() {
        let Some(msg) = sock.next().await else { bail!("tv socket closed") };
        let text = msg?;
        for body in bodies(&text) {
            if let Some(h) = body.strip_prefix("~h~") {
                sock.send(format!("~m~{}~m~{h}", body.len())).await?;
                continue;
            }
            let Ok(v) = serde_json::from_str::<serde_json::Value>(body) else { continue };
            match v["m"].as_str().unwrap_or_default() {
                "timescale_update" | "du" => {
                    if let Some(bars) = v["p"][1]["s1"]["s"].as_array() {
                        for b in bars.iter().filter_map(parse_bar) {
                            if tx.send(b).await.is_err() {
                                return Ok(());
                            }
                        }
                    }
                }
                "critical_error" | "protocol_error" => bail!("tv protocol error"),
                _ => {}
            }
        }
    }
    Ok(())
}

fn bodies(text: &str) -> Vec<&str> {
    let toks: Vec<&str> = text.split("~m~").collect();
    (2..toks.len()).step_by(2).map(|i| toks[i]).collect()
}

fn parse_bar(b: &serde_json::Value) -> Option<Bar> {
    let a = b["v"].as_array()?;
    if a.len() < 6 {
        return None;
    }
    let n = |i: usize| a[i].as_f64().unwrap_or(0.0);
    (n(0) > 0.0).then(|| Bar { ts: n(0) as i64, open: n(1), high: n(2), low: n(3), close: n(4), volume: n(5) })
}

async fn tencent_run(sec: &Security, tf: Timeframe, tx: &mpsc::Sender<Bar>) -> Result<()> {
    let key = sec.key();
    let mut fails = 0u32;
    let mut cur: Option<Bar> = None;
    let mut tick = rt::interval(Duration::from_secs(3));
    while !tx.is_closed() {
        tick.tick().await;
        match cn::quotes(std::slice::from_ref(&key)).await {
            Ok(q) if !q.is_empty() => {
                fails = 0;
                let q = &q[0];
                let bucket = q.ts - q.ts.rem_euclid(bucket_secs(tf));
                let bar = match &mut cur {
                    Some(b) if b.ts == bucket => {
                        b.high = b.high.max(q.last);
                        b.low = b.low.min(q.last);
                        b.close = q.last;
                        *b
                    }
                    _ => {
                        let b = Bar { ts: bucket, open: q.last, high: q.last, low: q.last, close: q.last, volume: 0.0 };
                        cur = Some(b);
                        b
                    }
                };
                if tx.send(bar).await.is_err() {
                    return Ok(());
                }
            }
            _ => {
                fails += 1;
                if fails >= 3 {
                    bail!("tencent live failed");
                }
            }
        }
    }
    Ok(())
}

async fn yahoo_run(sec: &Security, tf: Timeframe, tx: &mpsc::Sender<Bar>) -> Result<()> {
    let y = crate::yahoo::YahooProvider::new()?;
    let sym = crate::yahoo::yahoo_symbol(sec);
    let every = Duration::from_secs(if tf.is_intraday() { 30 } else { 300 });
    let mut fails = 0u32;
    let mut tick = rt::interval(every);
    while !tx.is_closed() {
        tick.tick().await;
        match y.tail(&sym, tf).await {
            Ok(bars) => {
                fails = 0;
                for b in bars {
                    if tx.send(b).await.is_err() {
                        return Ok(());
                    }
                }
            }
            Err(_) => {
                fails += 1;
                if fails >= 3 {
                    bail!("yahoo live failed");
                }
            }
        }
    }
    Ok(())
}

fn bucket_secs(tf: Timeframe) -> i64 {
    match tf {
        Timeframe::M1 => 60,
        Timeframe::M5 => 300,
        Timeframe::M15 => 900,
        Timeframe::M30 => 1800,
        Timeframe::H1 => 3600,
        Timeframe::H4 => 14400,
        Timeframe::D1 => 86400,
        Timeframe::W1 => 604800,
        Timeframe::MN1 => 2592000,
    }
}
