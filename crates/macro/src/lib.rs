//! US macro-economic indicators for the terminal: real GDP, M2, CPI and the ISM
//! manufacturing PMI with its components.
//!
//! GDP/M2/CPI come from FRED's keyless public CSV endpoint (`fredgraph.csv`), cached on
//! disk under `<data dir>/macro/` and refreshed after [`TTL`]. The ISM report is not on
//! FRED, so its history ships as a bundled seed (`data/ism.csv`, extracted from the PTM
//! template workbook by `scripts/extract_ism_seed.py`); a fresher extract dropped at
//! `<data dir>/macro/ism.csv` overrides the seed without a rebuild. Without a cache
//! directory (browser build) every series falls back to its bundled seed.
//!
//! Transformations follow the PTM methodology: M2 and CPI are read as year-over-year
//! prints with a historical percentile, GDP as quarter-over-quarter annualised growth,
//! and the PMI against the 50 boom/bust line.

pub mod fred;
pub mod seed;
#[cfg(not(target_arch = "wasm32"))]
pub mod store;

use std::collections::BTreeMap;
use std::future::Future;
use std::path::Path;
use std::time::Duration;

use anyhow::Result;
use serde::{Deserialize, Serialize};

/// How long a cached FRED series stays fresh before a pane asks for a refetch.
pub const TTL: Duration = Duration::from_secs(12 * 3600);

/// The four headline indicators, in display order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default, Serialize, Deserialize)]
pub enum Indicator {
    #[default]
    Gdp,
    M2,
    Cpi,
    Ism,
}

impl Indicator {
    pub const ALL: [Indicator; 4] = [Indicator::Gdp, Indicator::M2, Indicator::Cpi, Indicator::Ism];

    pub fn label(self) -> &'static str {
        match self {
            Indicator::Gdp => "Real GDP",
            Indicator::M2 => "M2 Money Supply",
            Indicator::Cpi => "CPI (Urban Consumers)",
            Indicator::Ism => "ISM Manufacturing PMI",
        }
    }

    pub fn short(self) -> &'static str {
        match self {
            Indicator::Gdp => "GDP",
            Indicator::M2 => "M2",
            Indicator::Cpi => "CPI",
            Indicator::Ism => "ISM PMI",
        }
    }

    /// FRED series id; `None` = bundled seed only (ISM is not distributed by FRED).
    pub fn fred_id(self) -> Option<&'static str> {
        match self {
            Indicator::Gdp => Some("GDPC1"),
            Indicator::M2 => Some("M2SL"),
            Indicator::Cpi => Some("CPIAUCSL"),
            Indicator::Ism => None,
        }
    }

    /// What the headline number on the card and chart is.
    pub fn headline(self) -> Headline {
        match self {
            Indicator::Gdp => Headline::QoqAnnualised,
            Indicator::M2 | Indicator::Cpi => Headline::Yoy,
            Indicator::Ism => Headline::Level,
        }
    }

    /// Observations per year, for year-over-year lags.
    pub fn per_year(self) -> usize {
        match self {
            Indicator::Gdp => 4,
            _ => 12,
        }
    }
}

/// The transformation shown as an indicator's headline.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Headline {
    Level,
    Yoy,
    QoqAnnualised,
}

impl Headline {
    pub fn label(self) -> &'static str {
        match self {
            Headline::Level => "level",
            Headline::Yoy => "YoY",
            Headline::QoqAnnualised => "QoQ ann.",
        }
    }
}

/// One observation: Unix seconds (UTC midnight of the period start) and its value.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct Point {
    pub ts: i64,
    pub value: f64,
}

/// Percent change against the observation `lag` periods back; leading entries dropped.
pub fn pct_change(points: &[Point], lag: usize) -> Vec<Point> {
    points
        .iter()
        .enumerate()
        .skip(lag)
        .filter_map(|(i, p)| {
            let prev = points[i - lag].value;
            (prev != 0.0).then(|| Point { ts: p.ts, value: (p.value / prev - 1.0) * 100.0 })
        })
        .collect()
}

/// Quarter-over-quarter growth, annualised: ((v / v_prev)^4 - 1) * 100.
pub fn qoq_annualised(points: &[Point]) -> Vec<Point> {
    points
        .windows(2)
        .filter(|w| w[0].value != 0.0)
        .map(|w| Point { ts: w[1].ts, value: ((w[1].value / w[0].value).powi(4) - 1.0) * 100.0 })
        .collect()
}

/// Where `value` ranks within the whole history, 0–100 (share of observations at or below).
pub fn percentile(points: &[Point], value: f64) -> Option<f64> {
    if points.is_empty() {
        return None;
    }
    let at_or_below = points.iter().filter(|p| p.value <= value).count();
    Some(at_or_below as f64 / points.len() as f64 * 100.0)
}

/// The headline transformation of a level series.
pub fn headline_series(ind: Indicator, levels: &[Point]) -> Vec<Point> {
    match ind.headline() {
        Headline::Level => levels.to_vec(),
        Headline::Yoy => pct_change(levels, ind.per_year()),
        Headline::QoqAnnualised => qoq_annualised(levels),
    }
}

/// Card-ready summary of one indicator.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Stat {
    pub indicator: Indicator,
    /// Period start of the latest observation.
    pub as_of: i64,
    pub level: f64,
    pub prev_level: Option<f64>,
    pub headline: f64,
    pub prev_headline: Option<f64>,
    /// Percent change of the level vs one period back (point diff for the PMI is derived
    /// by the view from `level - prev_level`).
    pub mom: Option<f64>,
    pub yoy: Option<f64>,
    /// Historical percentile of the headline print (M2/CPI, per PTM).
    pub percentile: Option<f64>,
}

/// One ISM component's latest reading and the one before it.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Component {
    pub name: String,
    pub as_of: i64,
    pub latest: f64,
    pub prev: Option<f64>,
}

/// Everything the Macro pane draws, produced off the UI thread.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Snapshot {
    /// Unix seconds when the FRED side was last fetched (0 = seeds only).
    pub fetched_at: i64,
    pub stats: Vec<Stat>,
    /// Level series per indicator, ascending.
    pub levels: BTreeMap<Indicator, Vec<Point>>,
    /// Headline-transformed series per indicator, ascending.
    pub headlines: BTreeMap<Indicator, Vec<Point>>,
    pub components: Vec<Component>,
    /// Indicators served from FRED (network or cache) rather than the bundled seed.
    pub live: Vec<Indicator>,
    /// Per-series fetch problems; the pane shows them but still draws what it has.
    pub errors: Vec<String>,
}

impl Snapshot {
    pub fn stat(&self, ind: Indicator) -> Option<&Stat> {
        self.stats.iter().find(|s| s.indicator == ind)
    }

    /// Assemble cards and chart series from raw level series.
    pub fn build(
        levels: BTreeMap<Indicator, Vec<Point>>,
        components: Vec<Component>,
        fetched_at: i64,
        live: Vec<Indicator>,
        errors: Vec<String>,
    ) -> Self {
        let mut stats = Vec::new();
        let mut headlines = BTreeMap::new();
        for ind in Indicator::ALL {
            let Some(pts) = levels.get(&ind) else { continue };
            let Some(last) = pts.last() else { continue };
            let head = headline_series(ind, pts);
            let mom = pct_change(pts, 1).last().map(|p| p.value);
            let yoy = pct_change(pts, ind.per_year()).last().map(|p| p.value);
            let percentile = match ind {
                Indicator::M2 | Indicator::Cpi => head.last().and_then(|p| percentile(&head, p.value)),
                _ => None,
            };
            stats.push(Stat {
                indicator: ind,
                as_of: last.ts,
                level: last.value,
                prev_level: pts.iter().rev().nth(1).map(|p| p.value),
                headline: head.last().map_or(last.value, |p| p.value),
                prev_headline: head.iter().rev().nth(1).map(|p| p.value),
                mom,
                yoy,
                percentile,
            });
            headlines.insert(ind, head);
        }
        Snapshot { fetched_at, stats, levels, headlines, components, live, errors }
    }
}

/// Where a [`load`] pulls FRED series from; the seam tests substitute fixtures for.
pub trait Source: Sync {
    fn series(&self, id: &str) -> impl Future<Output = Result<Vec<Point>>> + Send;
}

/// Load every indicator: ISM from the override file or seed, FRED series from cache when
/// fresh, else the network, else the stale cache, else the seed. `dir` is the macro cache
/// directory (`<data dir>/macro`); `None` (browser) serves seeds only.
pub async fn load<S: Source>(src: &S, dir: Option<&Path>, force: bool) -> Snapshot {
    let now = chrono::Utc::now().timestamp();
    let mut levels = BTreeMap::new();
    let mut live = Vec::new();
    let mut errors = Vec::new();
    let mut fetched_at = 0;

    let (ism, components) = match dir {
        Some(d) => seed::ism_override(d).unwrap_or_else(seed::ism),
        None => seed::ism(),
    };
    levels.insert(Indicator::Ism, ism);

    for ind in Indicator::ALL {
        let Some(id) = ind.fred_id() else { continue };
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(d) = dir {
            let cached = store::read(d, id);
            let fresh = cached.as_ref().is_some_and(|(at, _)| now - at < TTL.as_secs() as i64);
            if !force && fresh {
                let (at, pts) = cached.unwrap();
                fetched_at = fetched_at.max(at);
                live.push(ind);
                levels.insert(ind, pts);
                continue;
            }
            match src.series(id).await {
                Ok(pts) if !pts.is_empty() => {
                    fetched_at = now;
                    live.push(ind);
                    if let Err(e) = store::write(d, id, now, &pts) {
                        errors.push(format!("{id} cache: {e:#}"));
                    }
                    levels.insert(ind, pts);
                    continue;
                }
                Ok(_) => errors.push(format!("{id}: empty response")),
                Err(e) => errors.push(format!("{id}: {e:#}")),
            }
            if let Some((at, pts)) = cached {
                fetched_at = fetched_at.max(at);
                live.push(ind);
                levels.insert(ind, pts);
                continue;
            }
        }
        #[cfg(target_arch = "wasm32")]
        let _ = (src, force, now);
        levels.insert(ind, seed::fred(ind));
    }
    Snapshot::build(levels, components, fetched_at, live, errors)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pts(values: &[f64]) -> Vec<Point> {
        values.iter().enumerate().map(|(i, v)| Point { ts: i as i64 * 30 * 86400, value: *v }).collect()
    }

    #[test]
    fn pct_change_and_qoq() {
        let p = pts(&[100.0, 110.0, 121.0]);
        let mom = pct_change(&p, 1);
        assert_eq!(mom.len(), 2);
        assert!((mom[0].value - 10.0).abs() < 1e-9);
        let yoy = pct_change(&pts(&[100.0, 0.0, 0.0, 0.0, 105.0]), 4);
        assert!((yoy[0].value - 5.0).abs() < 1e-9);
        let q = qoq_annualised(&pts(&[100.0, 101.0]));
        assert!((q[0].value - (1.01f64.powi(4) - 1.0) * 100.0).abs() < 1e-9);
        assert!(pct_change(&pts(&[0.0, 5.0]), 1).is_empty(), "zero base dropped");
    }

    #[test]
    fn percentile_ranks_the_print() {
        let p = pts(&[1.0, 2.0, 3.0, 4.0]);
        assert_eq!(percentile(&p, 4.0), Some(100.0));
        assert_eq!(percentile(&p, 1.0), Some(25.0));
        assert_eq!(percentile(&p, 2.5), Some(50.0));
        assert_eq!(percentile(&[], 1.0), None);
    }

    #[test]
    fn headline_follows_the_indicator() {
        let quarterly = pts(&[100.0, 101.0, 102.0, 103.0, 104.0]);
        let gdp = headline_series(Indicator::Gdp, &quarterly);
        assert_eq!(gdp.len(), 4, "qoq annualised");
        let monthly: Vec<Point> = (0..24).map(|i| Point { ts: i * 30 * 86400, value: 100.0 + i as f64 }).collect();
        assert_eq!(headline_series(Indicator::M2, &monthly).len(), 12, "yoy needs a year");
        assert_eq!(headline_series(Indicator::Ism, &monthly).len(), 24, "pmi is the level");
    }

    #[test]
    fn build_summarises_cards() {
        let mut levels = BTreeMap::new();
        let m2: Vec<Point> =
            (0..24).map(|i| Point { ts: i * 30 * 86400, value: 100.0 * 1.01f64.powi(i as i32) }).collect();
        levels.insert(Indicator::M2, m2.clone());
        let snap = Snapshot::build(levels, Vec::new(), 42, vec![Indicator::M2], Vec::new());
        let s = snap.stat(Indicator::M2).unwrap();
        assert!((s.headline - s.yoy.unwrap()).abs() < 1e-9, "M2 headline is YoY");
        assert!(s.percentile.is_some());
        assert_eq!(snap.fetched_at, 42);
        assert!(snap.stat(Indicator::Gdp).is_none(), "missing series skipped");
    }
}
