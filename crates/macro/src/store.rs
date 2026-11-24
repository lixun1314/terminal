//! Disk cache for FRED series: one JSON document per series under `<data dir>/macro/`,
//! written atomically so a crash mid-refresh never leaves a half series.

use std::path::Path;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

use crate::Point;

#[derive(Serialize, Deserialize)]
struct Doc {
    fetched_at: i64,
    points: Vec<Point>,
}

/// The cached series and when it was fetched, if the file exists and parses.
pub fn read(dir: &Path, id: &str) -> Option<(i64, Vec<Point>)> {
    let text = std::fs::read_to_string(dir.join(format!("{id}.json"))).ok()?;
    let doc: Doc = serde_json::from_str(&text).ok()?;
    (!doc.points.is_empty()).then_some((doc.fetched_at, doc.points))
}

pub fn write(dir: &Path, id: &str, fetched_at: i64, points: &[Point]) -> Result<()> {
    std::fs::create_dir_all(dir).context(format!("creating {}", dir.display()))?;
    let doc = Doc { fetched_at, points: points.to_vec() };
    let json = serde_json::to_vec(&doc)?;
    let tmp = dir.join(format!("{id}.json.tmp"));
    std::fs::write(&tmp, &json).context(format!("writing {}", tmp.display()))?;
    std::fs::rename(&tmp, dir.join(format!("{id}.json"))).context("renaming cache into place")?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_through_a_temp_dir() {
        let dir = std::env::temp_dir().join(format!("tr-macro-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        assert!(read(&dir, "GDPC1").is_none());
        let pts = vec![Point { ts: 1, value: 2.0 }, Point { ts: 2, value: 3.0 }];
        write(&dir, "GDPC1", 99, &pts).unwrap();
        let (at, back) = read(&dir, "GDPC1").unwrap();
        assert_eq!(at, 99);
        assert_eq!(back, pts);
        std::fs::write(dir.join("M2SL.json"), "not json").unwrap();
        assert!(read(&dir, "M2SL").is_none(), "corrupt cache ignored");
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
