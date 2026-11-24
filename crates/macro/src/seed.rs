//! Bundled seed histories so the pane works offline and in the browser: FRED snapshots
//! taken at bundle time, plus the ISM workbook extract (see `scripts/extract_ism_seed.py`).

use std::path::Path;

use crate::{Component, Indicator, Point, fred};

/// Column order of `data/ism.csv`, matching the extractor.
pub const ISM_COLUMNS: [(&str, &str); 11] = [
    ("pmi", "PMI"),
    ("new_orders", "New Orders"),
    ("production", "Production"),
    ("employment", "Employment"),
    ("deliveries", "Supplier Deliveries"),
    ("inventories", "Inventories"),
    ("customer_inventories", "Customer Inventories"),
    ("prices", "Prices"),
    ("order_backlog", "Order Backlog"),
    ("exports", "Exports"),
    ("imports", "Imports"),
];

const GDPC1: &str = include_str!("../data/GDPC1.csv");
const M2SL: &str = include_str!("../data/M2SL.csv");
const CPIAUCSL: &str = include_str!("../data/CPIAUCSL.csv");
const ISM: &str = include_str!("../data/ism.csv");

/// The bundled FRED snapshot for an indicator.
pub fn fred_snapshot(ind: Indicator) -> Vec<Point> {
    let csv = match ind {
        Indicator::Gdp => GDPC1,
        Indicator::M2 => M2SL,
        Indicator::Cpi => CPIAUCSL,
        Indicator::Ism => ISM,
    };
    fred::parse_csv(csv).unwrap_or_default()
}

/// Alias used by [`crate::load`] for the seed fallback.
pub fn fred(ind: Indicator) -> Vec<Point> {
    fred_snapshot(ind)
}

/// Parse the wide ISM CSV into the PMI series plus each component's series.
pub fn parse_ism(csv: &str) -> (Vec<Point>, Vec<(usize, Vec<Point>)>) {
    let mut cols: Vec<Vec<Point>> = vec![Vec::new(); ISM_COLUMNS.len()];
    for (i, line) in csv.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() || i == 0 && line.starts_with("date,") {
            continue;
        }
        let mut cells = line.split(',');
        let Some(date) = cells.next() else { continue };
        let Ok(ts) = fred::parse_date(date) else { continue };
        for col in cols.iter_mut() {
            let Some(cell) = cells.next() else { break };
            if let Ok(value) = cell.trim().parse::<f64>() {
                col.push(Point { ts, value });
            }
        }
    }
    let pmi = cols[0].clone();
    (pmi, cols.into_iter().enumerate().skip(1).collect())
}

/// The bundled ISM history: PMI plus the ten components' latest readings.
pub fn ism() -> (Vec<Point>, Vec<Component>) {
    let (pmi, cols) = parse_ism(ISM);
    (pmi, components(&cols))
}

/// A fresher extract dropped in the cache directory wins over the bundled seed.
pub fn ism_override(dir: &Path) -> Option<(Vec<Point>, Vec<Component>)> {
    let csv = std::fs::read_to_string(dir.join("ism.csv")).ok()?;
    let (pmi, cols) = parse_ism(&csv);
    if pmi.is_empty() {
        return None;
    }
    Some((pmi, components(&cols)))
}

fn components(cols: &[(usize, Vec<Point>)]) -> Vec<Component> {
    cols.iter()
        .filter_map(|(k, pts)| {
            let last = pts.last()?;
            Some(Component {
                name: ISM_COLUMNS[*k].1.to_string(),
                as_of: last.ts,
                latest: last.value,
                prev: pts.iter().rev().nth(1).map(|p| p.value),
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn seeds_parse_and_reach_recent_periods() {
        for ind in [Indicator::Gdp, Indicator::M2, Indicator::Cpi] {
            let pts = fred_snapshot(ind);
            assert!(pts.len() > 300, "{} too short: {}", ind.short(), pts.len());
            assert!(pts.windows(2).all(|w| w[0].ts < w[1].ts));
            assert!(pts.last().unwrap().ts > 1_760_000_000, "{} stale seed", ind.short());
        }
        let (pmi, comps) = ism();
        assert!(pmi.len() > 900);
        assert_eq!(comps.len(), 10);
        assert!((pmi.last().unwrap().value - 55.5).abs() < 1e-9, "Oct 2026 PMI");
        assert!(comps.iter().all(|c| c.latest > 0.0));
    }

    #[test]
    fn wide_ism_keeps_sparse_columns_aligned() {
        let csv = "date,pmi,new_orders,production\n1948-01-01,51.7,53.3,\n1948-02-01,50.2,,55.2\n";
        let (pmi, cols) = parse_ism(csv);
        assert_eq!(pmi.len(), 2);
        assert_eq!(cols.len(), 10, "all component columns kept, empty ones included");
        assert_eq!(cols[0].1.len(), 1, "new orders skips the blank");
        assert_eq!(cols[1].1.len(), 1, "production skips the blank");
        assert_eq!(cols[1].1[0].ts, pmi[1].ts);
        assert!(cols[2..].iter().all(|(_, c)| c.is_empty()));
    }
}
