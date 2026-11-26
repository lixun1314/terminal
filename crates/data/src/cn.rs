//! China A-shares (Shanghai/Shenzhen) and Hong Kong equities, plus US equities as a
//! cookie-free near-realtime tier, via Tencent's public quote endpoint (Sina fallback).
//!
//! Quotes are ~3 s delayed and need no key; responses are GBK-encoded. History comes from
//! Tencent's fqkline (day/week/month) and mkline (minute) JSON. This is a free public
//! endpoint for personal use; see the README for the terms caveat.

use std::time::Duration;

use anyhow::{Context, Result, bail};
use tokio::sync::mpsc;
use tr_core::{AssetClass, Bar, Security, Timeframe};

use crate::{LiveFeed, Quote, rt};

const TENCENT_QUOTE: &str = "https://qt.gtimg.cn/q=";
const SINA_QUOTE: &str = "https://hq.sinajs.cn/list=";
const TENCENT_KLINE: &str = "https://web.ifzq.gtimg.cn/appstock/app/fqkline/get";
const TENCENT_MKLINE: &str = "https://web.ifzq.gtimg.cn/appstock/app/kline/mkline";
const UA: &str = "Mozilla/5.0 (compatible; terminal)";

/// Whether this listing is served by the China/HK/US-Tencent source.
pub fn is_cn(sec: &Security) -> bool {
    if sec.class == AssetClass::Index {
        return index_symbol(&sec.ticker).is_some();
    }
    sec.class == AssetClass::Equity && matches!(suffix(sec), Some("SS" | "SZ" | "HK"))
}

/// Tencent index codes for the indices its free feed carries (VIX is stale there, so no).
fn index_symbol(ticker: &str) -> Option<&'static str> {
    Some(match ticker {
        "DJI" | "INDU" => "usDJI",
        "IXIC" | "CCMP" => "usIXIC",
        "SPX" => "usINX",
        "NDX" => "usNDX",
        "HSI" => "hkHSI",
        "SHCOMP" => "sh000001",
        _ => return None,
    })
}

fn is_us_index(ticker: &str) -> bool {
    matches!(ticker, "DJI" | "INDU" | "IXIC" | "CCMP" | "SPX" | "NDX")
}

fn suffix(sec: &Security) -> Option<&str> {
    sec.ticker.rsplit_once('.').map(|(_, s)| s)
}

/// Tencent symbol: `sh600519`, `sz000001`, `hk00700`, `usAAPL`, or an index code.
pub fn tencent_symbol(sec: &Security) -> String {
    if sec.class == AssetClass::Index
        && let Some(s) = index_symbol(&sec.ticker)
    {
        return s.to_string();
    }
    match sec.ticker.rsplit_once('.') {
        Some((b, "SS")) => format!("sh{b}"),
        Some((b, "SZ")) => format!("sz{b}"),
        Some((b, "HK")) => format!("hk{b:0>5}"),
        _ => format!("us{}", sec.ticker),
    }
}

fn client() -> Result<reqwest::Client> {
    Ok(tr_core::http::client_builder().tls_backend_rustls().user_agent(UA).timeout(Duration::from_secs(15)).build()?)
}

fn decode_gbk(bytes: &[u8]) -> String {
    encoding_rs::GBK.decode(bytes).0.into_owned()
}

/// Batch quotes; Tencent first, Sina as fallback.
pub async fn quotes(keys: &[String]) -> Result<Vec<Quote>> {
    let secs: Vec<Security> = keys.iter().map(|k| crate::security(k)).collect();
    let syms: Vec<String> = secs.iter().map(tencent_symbol).collect();
    let c = client()?;
    if let Ok(t) = c.get(format!("{TENCENT_QUOTE}{}", syms.join(","))).send().await
        && let Ok(b) = t.bytes().await
    {
        let text = decode_gbk(&b);
        let out = parse_tencent(&text, keys, &syms);
        if !out.is_empty() {
            return Ok(out);
        }
    }
    // Sina fallback (needs a finance Referer).
    let t = c
        .get(format!("{SINA_QUOTE}{}", syms.join(",")))
        .header("Referer", "https://finance.sina.com.cn/")
        .send()
        .await
        .context("sina quote")?;
    let text = decode_gbk(&t.bytes().await?);
    Ok(parse_sina(&text, keys, &syms))
}

fn ts_of(s: &str) -> i64 {
    for fmt in ["%Y%m%d%H%M%S", "%Y/%m/%d %H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y%m%d%H%M"] {
        if let Ok(d) = chrono::NaiveDateTime::parse_from_str(s, fmt) {
            return d.and_utc().timestamp();
        }
    }
    chrono::Utc::now().timestamp()
}

fn parse_tencent(text: &str, keys: &[String], syms: &[String]) -> Vec<Quote> {
    let mut out = Vec::new();
    for line in text.split(';') {
        let line = line.trim();
        let Some((var, rest)) = line.split_once('=') else { continue };
        let sym = var.trim().trim_start_matches("v_");
        let Some(idx) = syms.iter().position(|s| s == sym) else { continue };
        let f: Vec<&str> = rest.trim().trim_matches('"').split('~').collect();
        if f.len() < 31 {
            continue;
        }
        let (Ok(last), Ok(prev)) = (f[3].parse::<f64>(), f[4].parse::<f64>()) else { continue };
        if last <= 0.0 {
            continue;
        }
        out.push(Quote {
            symbol: keys[idx].clone(),
            last,
            prev_close: if prev > 0.0 { prev } else { last },
            ts: ts_of(f[30]),
            spark: vec![],
            ext: None,
        });
    }
    out
}

fn parse_sina(text: &str, keys: &[String], syms: &[String]) -> Vec<Quote> {
    let mut out = Vec::new();
    for line in text.lines() {
        let Some((var, rest)) = line.split_once('=') else { continue };
        let sym = var.trim().trim_start_matches("var hq_str_");
        let Some(idx) = syms.iter().position(|s| s == sym) else { continue };
        let f: Vec<&str> = rest.trim().trim_matches('"').split(',').collect();
        if f.len() < 32 {
            continue;
        }
        let (Ok(last), Ok(prev)) = (f[3].parse::<f64>(), f[2].parse::<f64>()) else { continue };
        if last <= 0.0 {
            continue;
        }
        out.push(Quote {
            symbol: keys[idx].clone(),
            last,
            prev_close: if prev > 0.0 { prev } else { last },
            ts: ts_of(&format!("{} {}", f[30], f[31])),
            spark: vec![],
            ext: None,
        });
    }
    out
}

/// Daily/weekly/monthly history (fqkline) or minute history (mkline).
pub async fn history(sec: &Security, tf: Timeframe, limit: usize) -> Result<Vec<Bar>> {
    // Tencent has no US-index minute bars; the caller falls back to Yahoo for those.
    if sec.class == AssetClass::Index && tf.is_intraday() && is_us_index(&sec.ticker) {
        bail!("tencent: no intraday history for {}", sec.ticker);
    }
    let sym = tencent_symbol(sec);
    let c = client()?;
    let url = if tf.is_intraday() {
        let m = match tf {
            Timeframe::M1 => 1,
            Timeframe::M5 => 5,
            Timeframe::M15 => 15,
            Timeframe::M30 => 30,
            Timeframe::H1 => 60,
            Timeframe::H4 => 240,
            _ => 5,
        };
        format!("{TENCENT_MKLINE}?param={sym},m{m},,{limit}")
    } else {
        let p = match tf {
            Timeframe::W1 => "week",
            Timeframe::MN1 => "month",
            _ => "day",
        };
        format!("{TENCENT_KLINE}?param={sym},{p},,,{limit},qfq")
    };
    let t = c.get(&url).send().await.context("tencent kline")?;
    let v: serde_json::Value = t.json().await.context("tencent kline json")?;
    let node = &v["data"][&sym];
    let rows = if tf.is_intraday() {
        let m = match tf {
            Timeframe::M1 => "m1",
            Timeframe::M5 => "m5",
            Timeframe::M15 => "m15",
            Timeframe::M30 => "m30",
            Timeframe::H1 => "m60",
            Timeframe::H4 => "m240",
            _ => "m5",
        };
        node[m].as_array()
    } else {
        let p = match tf {
            Timeframe::W1 => "week",
            Timeframe::MN1 => "month",
            _ => "day",
        };
        node[format!("qfq{p}")].as_array().or_else(|| node[p].as_array())
    };
    let Some(rows) = rows else { bail!("tencent kline: no rows for {sym}") };
    let mut out = Vec::new();
    for r in rows {
        let a = r.as_array().context("kline row")?;
        if a.len() < 6 {
            continue;
        }
        let date = a[0].as_str().unwrap_or_default();
        let ts = if tf.is_intraday() {
            chrono::NaiveDateTime::parse_from_str(date, "%Y%m%d%H%M").map(|d| d.and_utc().timestamp()).unwrap_or(0)
        } else {
            chrono::NaiveDate::parse_from_str(&date[..10.min(date.len())], "%Y-%m-%d")
                .map(|d| d.and_hms_opt(0, 0, 0).unwrap().and_utc().timestamp())
                .unwrap_or(0)
        };
        if ts == 0 {
            continue;
        }
        let num = |i: usize| a.get(i).and_then(|x| x.as_str()).and_then(|s| s.parse::<f64>().ok()).unwrap_or(0.0);
        out.push(Bar { ts, open: num(1), high: num(3), low: num(4), close: num(2), volume: num(5) });
    }
    out.sort_by_key(|b| b.ts);
    Ok(out)
}

/// ~3 s poll of the quote, emitted as bars for the chart's live merge. Works for CN/HK and
/// US (`usAAPL`) listings; used as the middle fallback tier behind TradingView push.
pub fn live(sec: &Security, tf: Timeframe) -> LiveFeed {
    let (tx, rx) = mpsc::channel(64);
    let key = sec.key();
    rt::spawn(async move {
        let mut cur: Option<Bar> = None;
        let mut tick = rt::interval(Duration::from_secs(3));
        while !tx.is_closed() {
            tick.tick().await;
            let Ok(q) = quotes(std::slice::from_ref(&key)).await else { continue };
            let Some(q) = q.first() else { continue };
            let bucket = bucket_ts(q.ts, tf);
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
                return;
            }
        }
    });
    rx
}

fn bucket_ts(ts: i64, tf: Timeframe) -> i64 {
    let secs = match tf {
        Timeframe::M1 => 60,
        Timeframe::M5 => 300,
        Timeframe::M15 => 900,
        Timeframe::M30 => 1800,
        Timeframe::H1 => 3600,
        Timeframe::H4 => 14400,
        Timeframe::D1 => 86400,
        Timeframe::W1 => 604800,
        Timeframe::MN1 => 2592000,
    };
    ts - ts.rem_euclid(secs)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_symbols() {
        assert_eq!(tencent_symbol(&crate::security("600519 SS Equity")), "sh600519");
        assert_eq!(tencent_symbol(&crate::security("000001 SZ Equity")), "sz000001");
        assert_eq!(tencent_symbol(&crate::security("0700 HK Equity")), "hk00700");
        assert_eq!(tencent_symbol(&crate::security("AAPL US Equity")), "usAAPL");
        assert_eq!(tencent_symbol(&crate::security("CCMP Index")), "usIXIC");
        assert_eq!(tencent_symbol(&crate::security("SPX Index")), "usINX");
        assert_eq!(tencent_symbol(&crate::security("HSI Index")), "hkHSI");
        assert!(is_cn(&crate::security("600519 SS Equity")));
        assert!(is_cn(&crate::security("CCMP Index")));
        assert!(!is_cn(&crate::security("VIX Index")), "Tencent's VIX feed is stale");
        assert!(!is_cn(&crate::security("AAPL US Equity")));
    }

    #[test]
    fn parses_tencent_and_sina() {
        let keys = vec!["600519 SS Equity".to_string()];
        let syms = vec!["sh600519".to_string()];
        let t = r#"v_sh600519="1~贵州茅台~600519~1460.00~1439.00~1425.00~42924~23737~19187~1458.88~5~1458.51~1~1458.50~1~1458.47~1~1458.11~1~1460.00~108~1460.11~1~1460.26~1~1460.28~1~1460.30~2~1460.40~3~1460.50~4~1460.60~5~1460.70~6~20261124160815~21.00~1.46~1468.00~1425.00~1460.00/42924/6260000000~42924~626000~0.34~20.5~~1468.00~1425.00~2.99~18340.00~18340.00~1.46~1582.90~1295.10~1.0~0~1458.00~20.5~20.5~~~626000.0000~0~0~ ~GP-A~""#;
        let q = parse_tencent(t, &keys, &syms);
        assert_eq!(q.len(), 1);
        assert_eq!(q[0].last, 1460.0);
        assert_eq!(q[0].prev_close, 1439.0);
        let s = r#"var hq_str_sh600519="贵州茅台,1425.00,1439.00,1460.00,1468.00,1425.00,1459.00,1460.00,42924,6260000000,100,1459.00,100,1458.90,100,1458.80,100,1458.70,100,1458.60,100,1460.00,100,1460.10,100,1460.20,100,1460.30,100,1460.40,2026-11-24,16:08:15,00,""#;
        let q = parse_sina(s, &keys, &syms);
        assert_eq!(q.len(), 1);
        assert_eq!(q[0].last, 1460.0);
        assert_eq!(q[0].prev_close, 1439.0);
    }
}
