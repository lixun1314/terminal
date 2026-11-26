//! Routes market keys to the free source for each asset class.
//!
//! | class | history | quotes | live |
//! |---|---|---|---|
//! | Crypto | Binance klines (Coinbase fallback) | Binance 24h ticker | Binance kline websocket (Coinbase ticker fallback) |
//! | Govt | Treasury.gov daily curve (FRED fallback); Yahoo `^TNX` etc. intraday | Treasury.gov | Yahoo tail poll (intraday only) |
//! | Equity/ETF | Alpaca if keyed, else Yahoo | Yahoo spark | Yahoo tail poll |
//! | FX/Index/Comdty | Yahoo (`EURUSD=X`, `^GSPC`, `CL=F`) | Yahoo spark | Yahoo tail poll |
//! | HIP-3 perps (`xyz:AAPL`) | — | — | — |
//!
//! HIP-3 perps come only from the Terminal server ([`crate::remote::RemoteMarkets`]).

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{Result, anyhow, bail};
use async_trait::async_trait;
use tokio::sync::mpsc;
use tr_core::{AssetClass, Bar, Security, Timeframe, catalog};

use crate::alpaca::AlpacaProvider;
use crate::cn;
use crate::crypto::CryptoProvider;
use crate::rt;
use crate::tradingview::TradingView;
use crate::treasury::TreasuryProvider;
use crate::tv_live;
use crate::yahoo::{YahooProvider, yahoo_symbol};
use crate::{
    LiveFeed, MarketData, NewsItem, OptionChain, OrderBook, Quote, SearchHit, YieldCurves, perp_needs_server, security,
};

pub struct Markets {
    yahoo: Arc<YahooProvider>,
    crypto: CryptoProvider,
    treasury: TreasuryProvider,
    /// Fills quotes/search gaps when the primary sources fail (Yahoo blocked/throttled).
    tv: Option<TradingView>,
    /// Where each symbol's chart history last came from, so the live feed stays same-source.
    hist_src: Mutex<HashMap<String, (Instant, HistSrc)>>,
    /// Yahoo daily spark (sparkline + previous close), reused between fast price polls.
    spark_cache: Mutex<HashMap<String, (Instant, Quote)>>,
    alpaca: Option<AlpacaProvider>,
    name: String,
}

impl Markets {
    pub fn new(alpaca: Option<AlpacaProvider>) -> Result<Self> {
        let name = if alpaca.is_some() { "alpaca+yahoo+binance+ust" } else { "yahoo+binance+ust" }.to_string();
        let tv = match TradingView::new() {
            Ok(tv) => {
                tracing::info!(
                    "tradingview backup ready{}",
                    if tv.cookie_configured() { " (session cookie: realtime US/FX)" } else { "" }
                );
                Some(tv)
            }
            Err(e) => {
                tracing::warn!("tradingview backup unavailable: {e:#}");
                None
            }
        };
        Ok(Self {
            yahoo: Arc::new(YahooProvider::new()?),
            crypto: CryptoProvider::new()?,
            treasury: TreasuryProvider::new()?,
            tv,
            hist_src: Mutex::new(HashMap::new()),
            spark_cache: Mutex::new(HashMap::new()),
            alpaca,
            name,
        })
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum HistSrc {
    Tv,
    Cn,
    Alpaca,
}

/// Yahoo daily spark (sparkline + previous close) only needs refreshing this often; the
/// realtime price comes from the overlay, so 1 s polls don't re-hit Yahoo every second.
fn spark_ttl() -> Duration {
    std::env::var("TR_SPARK_TTL_SECS")
        .ok()
        .and_then(|v| v.parse().ok())
        .filter(|&s: &u64| s > 0)
        .map(Duration::from_secs)
        .unwrap_or(Duration::from_secs(60))
}

#[async_trait]
impl MarketData for Markets {
    fn name(&self) -> &str {
        &self.name
    }

    fn realtime_quotes(&self) -> bool {
        self.tv.as_ref().is_some_and(|t| t.cookie_configured())
    }

    async fn history(&self, symbol: &str, tf: Timeframe, end: Option<i64>, limit: usize) -> Result<Vec<Bar>> {
        let sec = security(symbol);
        // Source-consistent charts: a symbol using TV push also loads its history from TV.
        if tv_live::push_enabled() && tv_live::eligible(&sec) {
            if end.is_some() {
                return Ok(Vec::new()); // one-shot TV history has no older pages
            }
            match tv_live::history(&sec, tf, limit).await {
                Ok(b) if !b.is_empty() => {
                    tracing::debug!("tv history ok {} {tf} ({} bars)", sec.ticker, b.len());
                    self.hist_src.lock().expect("hist src").insert(sec.key(), (Instant::now(), HistSrc::Tv));
                    return Ok(b);
                }
                Ok(_) => {}
                Err(e) => tracing::debug!("tv history {} {tf}: {e:#}", sec.ticker),
            }
        }
        // Tencent fallback (A/HK equities and indices; shared data directory, no key).
        if cn::is_cn(&sec) && !(sec.class == AssetClass::Index && crate::source_pref() == crate::SourcePref::Yahoo) {
            if end.is_some() {
                return Ok(Vec::new()); // the Tencent feed has no older pages
            }
            match cn::history(&sec, tf, limit).await {
                Ok(b) if !b.is_empty() => {
                    self.hist_src.lock().expect("hist src").insert(sec.key(), (Instant::now(), HistSrc::Cn));
                    return Ok(b);
                }
                Ok(_) => {}
                Err(e) => tracing::debug!("cn history {} {tf}: {e:#}", sec.ticker),
            }
        }
        // Alpaca source: same-provider history for US equities/ETFs.
        if let Some(a) = self.alpaca.as_ref()
            && crate::source_pref() == crate::SourcePref::Alpaca
            && matches!(sec.class, AssetClass::Equity | AssetClass::Etf)
            && !cn::is_cn(&sec)
        {
            if end.is_some() {
                return Ok(Vec::new());
            }
            match a.history(&sec.ticker, tf, None, limit).await {
                Ok(b) if !b.is_empty() => {
                    self.hist_src.lock().expect("hist src").insert(sec.key(), (Instant::now(), HistSrc::Alpaca));
                    return Ok(b);
                }
                Ok(_) => {}
                Err(e) => tracing::debug!("alpaca history {} {tf}: {e:#}", sec.ticker),
            }
        }
        match sec.class {
            AssetClass::Perp => Err(perp_needs_server(symbol)),
            AssetClass::Crypto => self.crypto.history(&sec, tf, end, limit).await,
            AssetClass::Govt if tf.is_intraday() => match catalog::by_security(&sec).and_then(|l| l.yahoo) {
                Some(y) => self.yahoo.history(y, tf, end, limit).await,
                None => bail!("intraday yields are not available for {}; use D, W or M", sec.ticker),
            },
            AssetClass::Govt => self.treasury.history(&sec, tf, end, limit).await,
            AssetClass::Equity | AssetClass::Etf if self.alpaca.is_some() => {
                let a = self.alpaca.as_ref().expect("checked");
                match a.history(&sec.ticker, tf, end, limit).await {
                    Ok(b) if !b.is_empty() => Ok(b),
                    _ => self.yahoo.history(&yahoo_symbol(&sec), tf, end, limit).await,
                }
            }
            _ => self.yahoo.history(&yahoo_symbol(&sec), tf, end, limit).await,
        }
    }

    async fn quotes(&self, symbols: &[String]) -> Result<Vec<Quote>> {
        let use_alpaca = crate::source_pref() == crate::SourcePref::Alpaca && self.alpaca.is_some();
        let (mut crypto, mut govt, mut other, mut cnv, mut alp) =
            (Vec::new(), Vec::new(), Vec::new(), Vec::new(), Vec::new());
        for key in symbols {
            let sec = security(key);
            match sec.class {
                AssetClass::Perp => {}
                AssetClass::Crypto => crypto.push((key.clone(), sec)),
                AssetClass::Govt => govt.push((key.clone(), sec)),
                AssetClass::Equity | AssetClass::Etf if use_alpaca && !cn::is_cn(&sec) => alp.push(key.clone()),
                AssetClass::Equity if cn::is_cn(&sec) => cnv.push(key.clone()),
                _ => other.push((key.clone(), sec)),
            }
        }
        let (c, g, y, cnq, ap) = tokio::join!(
            async { if crypto.is_empty() { Ok(vec![]) } else { self.crypto.quotes(&crypto).await } },
            async { if govt.is_empty() { Ok(vec![]) } else { self.treasury.quotes(&govt).await } },
            async { if cnv.is_empty() { Ok(vec![]) } else { cn::quotes(&cnv).await } },
            async {
                let (Some(a), false) = (&self.alpaca, alp.is_empty()) else { return Ok(vec![]) };
                match a.quotes(&alp).await {
                    Ok(q) if !q.is_empty() => {
                        tracing::debug!("alpaca: {} quote(s)", q.len());
                        Ok(q)
                    }
                    // Never leave equity rows blank when Alpaca is unreachable: fill via Yahoo.
                    _ => {
                        let ys: Vec<String> = alp.iter().map(|k| yahoo_symbol(&security(k))).collect();
                        let quotes = self.yahoo.quotes(&ys).await?;
                        Ok(alp
                            .iter()
                            .zip(&ys)
                            .filter_map(|(k, y)| {
                                quotes.iter().find(|q| &q.symbol == y).map(|q| Quote { symbol: k.clone(), ..q.clone() })
                            })
                            .collect())
                    }
                }
            },
            async {
                if other.is_empty() {
                    return Ok(vec![]);
                }
                // Reuse the daily spark between fast polls; only stale keys re-hit Yahoo.
                let ttl = spark_ttl();
                let mut out = Vec::new();
                let mut stale: Vec<(String, Security)> = Vec::new();
                {
                    let cache = self.spark_cache.lock().expect("spark cache");
                    for (key, sec) in &other {
                        match cache.get(key) {
                            Some((at, q)) if at.elapsed() < ttl => out.push(q.clone()),
                            _ => stale.push((key.clone(), sec.clone())),
                        }
                    }
                }
                if !stale.is_empty() {
                    let ys: Vec<String> = stale.iter().map(|(_, s)| yahoo_symbol(s)).collect();
                    match self.yahoo.quotes(&ys).await {
                        Ok(quotes) => {
                            let mut cache = self.spark_cache.lock().expect("spark cache");
                            for ((key, _), y) in stale.iter().zip(&ys) {
                                if let Some(q) = quotes.iter().find(|q| &q.symbol == y) {
                                    let q = Quote { symbol: key.clone(), ..q.clone() };
                                    cache.insert(key.clone(), (Instant::now(), q.clone()));
                                    out.push(q);
                                }
                            }
                        }
                        // Keep whatever the cache already holds rather than losing the panel.
                        Err(e) if out.is_empty() => return Err(e),
                        Err(e) => tracing::debug!("yahoo spark refresh failed, using cache: {e:#}"),
                    }
                }
                Ok::<_, anyhow::Error>(out)
            }
        );
        let mut out = Vec::new();
        let mut errors = Vec::new();
        for r in [c, g, y, cnq, ap] {
            match r {
                Ok(q) => out.extend(q),
                Err(e) => errors.push(format!("{e:#}")),
            }
        }
        // TradingView does two jobs here: it fills keys the primary sources left out, and —
        // with a logged-in session — it overlays realtime prices for US equities/ETFs, FX
        // and Treasury yields. Its futures and the headline index stay delayed, so those
        // keep the primary quote (and its sparkline).
        if let Some(tv) = &self.tv
            && !matches!(crate::source_pref(), crate::SourcePref::Yahoo | crate::SourcePref::Alpaca)
        {
            let realtime = tv.cookie_configured();
            let ext = crate::tradingview::extended_session_now();
            let wanted: Vec<String> = symbols
                .iter()
                .filter(|k| {
                    let class = security(k).class;
                    (realtime
                        && matches!(class, AssetClass::Equity | AssetClass::Etf | AssetClass::Fx | AssetClass::Govt))
                        || (ext.is_some() && matches!(class, AssetClass::Equity | AssetClass::Etf))
                        || !out.iter().any(|q| &q.symbol == *k)
                })
                .cloned()
                .collect();
            if !wanted.is_empty() {
                match tv.quotes(&wanted).await {
                    Ok(fetched) => {
                        if !fetched.is_empty() {
                            tracing::debug!("tradingview: {} quote(s) for {} key(s)", fetched.len(), wanted.len());
                        }
                        for q in fetched {
                            match out.iter_mut().find(|o| o.symbol == q.symbol) {
                                // Keep the primary sparkline while taking the fresh price.
                                Some(slot) if realtime => {
                                    let spark = std::mem::take(&mut slot.spark);
                                    *slot = Quote { spark, ..q };
                                }
                                // Outside a session with no realtime entitlement: only the
                                // extended print is newer than the primary quote.
                                Some(slot) => {
                                    if q.ext.is_some() {
                                        slot.ext = q.ext;
                                    }
                                }
                                None => out.push(q),
                            }
                        }
                    }
                    Err(e) => errors.push(format!("tradingview: {e:#}")),
                }
            }
        }
        // Tencent index fallback: fill index quotes the primary left out (VIX stays on Yahoo,
        // where Tencent's feed is stale).
        let missing_idx: Vec<String> = symbols
            .iter()
            .filter(|k| security(k).class == AssetClass::Index && !out.iter().any(|q| &q.symbol == *k))
            .cloned()
            .collect();
        if !missing_idx.is_empty()
            && crate::source_pref() != crate::SourcePref::Yahoo
            && let Ok(q) = cn::quotes(&missing_idx).await
        {
            out.extend(q);
        }
        if out.is_empty() && !errors.is_empty() {
            return Err(anyhow!(errors.join("; ")));
        }
        Ok(out)
    }

    fn live(&self, symbol: &str, tf: Timeframe) -> Option<LiveFeed> {
        let sec = security(symbol);
        if sec.class == AssetClass::Crypto {
            return Some(CryptoProvider::live(&sec, tf));
        }
        if sec.class == AssetClass::Equity && cn::is_cn(&sec) {
            return Some(cn::live(&sec, tf));
        }
        // Alpaca source: realtime IEX trades push for the same-provider history.
        if crate::source_pref() == crate::SourcePref::Alpaca
            && matches!(sec.class, AssetClass::Equity | AssetClass::Etf)
            && !cn::is_cn(&sec)
        {
            let same_source = self
                .hist_src
                .lock()
                .expect("hist src")
                .get(&sec.key())
                .is_some_and(|(t, s)| t.elapsed() < Duration::from_secs(6 * 3600) && *s == HistSrc::Alpaca);
            if same_source {
                if !tf.is_intraday() {
                    return None; // daily charts stay static between reloads
                }
                if let Some(a) = &self.alpaca
                    && let Some(feed) = a.live(&sec, tf)
                {
                    return Some(feed);
                }
            }
        }
        // TradingView push for everything it serves (stocks realtime with a session; indices
        // and futures delayed-but-pushed, like the TradingView web chart). Same-source only.
        if tv_live::push_enabled() && tv_live::eligible(&sec) {
            let src = self
                .hist_src
                .lock()
                .expect("hist src")
                .get(&sec.key())
                .filter(|(t, _)| t.elapsed() < Duration::from_secs(6 * 3600))
                .map(|(_, s)| *s);
            let allow_tv = src == Some(HistSrc::Tv);
            // Tencent is a fallback for A/HK equities and indices as long as the chart did
            // not come from TradingView and the Yahoo-only source is not selected.
            let allow_cn = crate::source_pref() != crate::SourcePref::Yahoo
                && src != Some(HistSrc::Tv)
                && matches!(sec.class, AssetClass::Equity | AssetClass::Etf | AssetClass::Index);
            return Some(tv_live::feed(&sec, tf, allow_tv, allow_cn));
        }
        // Yahoo has no push stream for these classes, so poll the chart tail and let the
        // worker's live merge keep the chart current while the market trades.
        let polled = matches!(
            sec.class,
            AssetClass::Equity | AssetClass::Etf | AssetClass::Fx | AssetClass::Index | AssetClass::Cmdty
        ) || (sec.class == AssetClass::Govt && tf.is_intraday());
        if !polled {
            return None;
        }
        let every = poll_interval(tf)?;
        let yahoo = self.yahoo.clone();
        let sym = yahoo_symbol(&sec);
        let (tx, rx) = mpsc::channel(64);
        rt::spawn(async move {
            let mut tick = rt::interval(every);
            tick.tick().await; // history was just loaded; the first poll is one interval later
            while !tx.is_closed() {
                tick.tick().await;
                match yahoo.tail(&sym, tf).await {
                    Ok(bars) => {
                        for b in bars {
                            if tx.send(b).await.is_err() {
                                return;
                            }
                        }
                    }
                    Err(e) => tracing::debug!("yahoo live poll {sym} {tf}: {e:#}"),
                }
            }
        });
        Some(rx)
    }

    async fn search(&self, query: &str) -> Result<Vec<SearchHit>> {
        let primary = self.yahoo.search(query).await;
        if primary.as_ref().is_ok_and(|h| !h.is_empty()) {
            return primary;
        }
        match &self.tv {
            Some(tv) => tv.search(query).await.or(primary),
            None => primary,
        }
    }

    async fn news(&self, symbol: Option<&str>) -> Result<Vec<NewsItem>> {
        let q = match symbol.map(security) {
            Some(sec) => match sec.class {
                // Yahoo has no crypto headlines; use CoinDesk's feed.
                AssetClass::Crypto => return self.crypto.news(&sec).await,
                // A linked perp shares its underlying's headlines.
                AssetClass::Perp => match tr_core::perps::link_for(&sec.ticker) {
                    Some(l) => yahoo_symbol(&security(l.underlying)),
                    None => return Ok(Vec::new()),
                },
                AssetClass::Govt => catalog::by_security(&sec).and_then(|l| l.yahoo).unwrap_or("^TNX").to_string(),
                _ => yahoo_symbol(&sec),
            },
            None => "stock market".into(),
        };
        self.yahoo.news(&q).await
    }

    /// Listed options on US equities, ETFs and indices (Yahoo).
    async fn options(&self, symbol: &str, expiration: Option<i64>) -> Result<OptionChain> {
        let sec = security(symbol);
        match sec.class {
            AssetClass::Equity | AssetClass::Etf | AssetClass::Index => {
                let y = yahoo_symbol(&sec);
                // Yahoo lists S&P 500 index options under ^SPX, not the ^GSPC quote symbol.
                let y = if y == "^GSPC" { "^SPX".to_string() } else { y };
                self.yahoo.options(&y, expiration).await
            }
            _ => bail!("listed options are available for US equities, ETFs and indices, not {}", sec.key()),
        }
    }

    async fn order_book(&self, symbol: &str, depth: usize) -> Result<OrderBook> {
        let sec = security(symbol);
        match sec.class {
            AssetClass::Crypto => self.crypto.order_book(symbol, &sec, depth).await,
            _ => bail!("no free public order book for {}", sec.key()),
        }
    }

    async fn yield_curves(&self) -> Result<YieldCurves> {
        self.treasury.curves().await
    }
}

/// How often a polled live feed refetches the chart tail. Intraday bars move during the
/// session; daily and longer bars at most once a day. `TR_LIVE_POLL_SECS` overrides the
/// intraday cadence (0 disables polling). Yahoo rate-limits aggressive polling, so keep
/// this well above the quote poll's floor.
fn poll_interval(tf: Timeframe) -> Option<Duration> {
    if let Ok(v) = std::env::var("TR_LIVE_POLL_SECS") {
        let secs: u64 = v.parse().ok()?;
        return (secs > 0).then(|| Duration::from_secs(secs));
    }
    Some(Duration::from_secs(if tf.is_intraday() { 30 } else { 300 }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn poll_cadence_matches_the_timeframe() {
        assert_eq!(poll_interval(Timeframe::M1), Some(Duration::from_secs(30)));
        assert_eq!(poll_interval(Timeframe::M30), Some(Duration::from_secs(30)));
        assert_eq!(poll_interval(Timeframe::D1), Some(Duration::from_secs(300)));
        assert_eq!(poll_interval(Timeframe::W1), Some(Duration::from_secs(300)));
    }
}

/// Market key for a security (convenience for callers holding a [`Security`]).
pub fn key(sec: &Security) -> String {
    sec.key()
}
