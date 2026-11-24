//! FRED's keyless public CSV endpoint: `fredgraph.csv?id=<series>` returns the whole
//! history as `observation_date,<id>` rows, with `.` for missing observations.

use std::future::Future;
use std::time::Duration;

use anyhow::{Context, Result, bail};

use crate::{Point, Source};

const FRED: &str = "https://fred.stlouisfed.org/graph/fredgraph.csv";

/// Parse a `fredgraph.csv` body into ascending points, skipping `.`/blank observations.
pub fn parse_csv(csv: &str) -> Result<Vec<Point>> {
    let mut out = Vec::new();
    for (i, line) in csv.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() || i == 0 && line.starts_with("observation_date") {
            continue;
        }
        let (date, value) = line.split_once(',').context(format!("fred csv line {}: {line:?}", i + 1))?;
        let value = value.trim();
        if value.is_empty() || value == "." {
            continue;
        }
        let value: f64 = value.parse().context(format!("fred csv value {value:?}"))?;
        let ts = parse_date(date.trim())?;
        out.push(Point { ts, value });
    }
    out.sort_by_key(|p| p.ts);
    Ok(out)
}

/// `YYYY-MM-DD` as Unix seconds at UTC midnight.
pub fn parse_date(date: &str) -> Result<i64> {
    chrono::NaiveDate::parse_from_str(date, "%Y-%m-%d")
        .context(format!("fred date {date:?}"))?
        .and_hms_opt(0, 0, 0)
        .context("midnight")
        .map(|d| d.and_utc().timestamp())
}

/// The live FRED source used by the desktop worker.
pub struct FredSource {
    pub client: reqwest::Client,
}

impl FredSource {
    pub fn new() -> Result<Self> {
        // FRED's edge is picky: rustls handshakes are aborted, and over the system TLS
        // stack only HTTP/1.1 with a curl-like UA and an Accept header gets through
        // (probe-verified against fredgraph.csv). Enabling native-tls flips reqwest's
        // workspace-wide default, so every other provider pins `tls_backend_rustls()`.
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert(reqwest::header::ACCEPT, reqwest::header::HeaderValue::from_static("*/*"));
        let builder = tr_core::http::client_builder()
            .user_agent("curl/8.7.1")
            .http1_only()
            .default_headers(headers)
            .timeout(Duration::from_secs(30));
        #[cfg(not(target_arch = "wasm32"))]
        let builder = builder.tls_backend_native();
        let client = builder.build()?;
        Ok(FredSource { client })
    }

    pub async fn fetch(&self, id: &str) -> Result<Vec<Point>> {
        let url = format!("{FRED}?id={id}");
        let resp = self.client.get(&url).send().await.context(format!("fred {id}"))?;
        if !resp.status().is_success() {
            bail!("fred {id}: HTTP {}", resp.status());
        }
        let body = resp.text().await.context(format!("fred {id} body"))?;
        if body.trim_start().starts_with('<') {
            bail!("fred {id}: HTML instead of CSV (series unavailable)");
        }
        parse_csv(&body)
    }
}

impl Source for FredSource {
    fn series(&self, id: &str) -> impl Future<Output = Result<Vec<Point>>> + Send {
        self.fetch(id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_observations_and_skips_missing() {
        let csv = "observation_date,GDPC1\n1947-01-01,2182.681\n1947-04-01,.\n1947-07-01,2190.0\n";
        let pts = parse_csv(csv).unwrap();
        assert_eq!(pts.len(), 2);
        assert_eq!(pts[0].ts, parse_date("1947-01-01").unwrap());
        assert!(pts.windows(2).all(|w| w[0].ts < w[1].ts));
    }

    #[test]
    fn rejects_html_and_garbage() {
        assert!(parse_csv("<!DOCTYPE html>").is_err());
        assert!(parse_csv("observation_date,X\n2020-01-01,abc\n").is_err());
        assert!(parse_date("01/02/2020").is_err());
    }
}
