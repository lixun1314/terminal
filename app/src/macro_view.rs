//! The Macro pane (`MACRO`): headline cards for real GDP, M2, CPI and the ISM
//! manufacturing PMI, a line chart of the selected indicator's headline transform, and
//! the ISM components table. Data arrives app-globally from the worker (`Request::Macro`)
//! like the yield curve pane; the chart is an egui painter line over calendar time
//! (monthly/quarterly points), not the bar-indexed GPU pipeline.

use egui::{Align2, Pos2, Rect, RichText, Sense, Stroke, StrokeKind, Ui, pos2, vec2};
use serde::{Deserialize, Serialize};
use tr_macro::{Headline, Indicator, Point, Snapshot};

use crate::congress_view::Action;
use crate::theme::{self, amber, mono, muted, text};
use crate::widgets;

/// Chart span for the selected indicator.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum MacroRange {
    Y1,
    Y3,
    Y5,
    #[default]
    Y10,
    Max,
}

impl MacroRange {
    pub const ALL: [MacroRange; 5] = [MacroRange::Y1, MacroRange::Y3, MacroRange::Y5, MacroRange::Y10, MacroRange::Max];

    pub fn label(self) -> &'static str {
        match self {
            MacroRange::Y1 => "1Y",
            MacroRange::Y3 => "3Y",
            MacroRange::Y5 => "5Y",
            MacroRange::Y10 => "10Y",
            MacroRange::Max => "MAX",
        }
    }

    fn years(self) -> Option<f64> {
        match self {
            MacroRange::Y1 => Some(1.0),
            MacroRange::Y3 => Some(3.0),
            MacroRange::Y5 => Some(5.0),
            MacroRange::Y10 => Some(10.0),
            MacroRange::Max => None,
        }
    }
}

/// Per-pane Macro state, persisted in layouts.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct MacroPane {
    pub indicator: Indicator,
    pub range: MacroRange,
}

impl Default for MacroPane {
    fn default() -> Self {
        MacroPane { indicator: Indicator::Ism, range: MacroRange::default() }
    }
}

const CARD_H: f32 = 68.0;

/// Draw the pane; returns congress-style actions plus whether Refresh was clicked.
pub fn show(ui: &mut Ui, pane: &mut MacroPane, data: Option<&Snapshot>, loading: bool) -> (Vec<Action>, bool) {
    let out = Vec::new();
    let mut refresh = false;
    ui.horizontal(|ui| {
        ui.label(RichText::new("US MACRO").font(mono(12.0)).color(amber()));
        if loading {
            ui.add(egui::Spinner::new().size(12.0));
        }
        if ui.small_button("Refresh").on_hover_text("Refetch FRED now (MACRO REFRESH)").clicked() {
            refresh = true;
        }
    });
    let Some(d) = data else {
        ui.label(RichText::new("Loading macro indicators (FRED + bundled ISM seed)…").font(mono(12.0)).color(muted()));
        return (out, refresh);
    };

    // Headline cards: click to chart an indicator.
    let gap = 6.0;
    let cw = ((ui.available_width() - 3.0 * gap) / 4.0).max(140.0);
    ui.horizontal(|ui| {
        for ind in Indicator::ALL {
            if card(ui, d, ind, pane.indicator == ind, cw) {
                pane.indicator = ind;
            }
            ui.add_space(gap);
        }
    });
    ui.add_space(4.0);

    // Span picker, then the chart of the selected headline.
    let labels: Vec<&str> = MacroRange::ALL.iter().map(|r| r.label()).collect();
    let sel = MacroRange::ALL.iter().position(|&r| r == pane.range).unwrap_or(3);
    if let Some(i) = widgets::segmented(ui, ("macro-range",), &labels, Some(sel), 18.0) {
        pane.range = MacroRange::ALL[i];
    }
    chart(ui, d, pane);

    if pane.indicator == Indicator::Ism {
        components(ui, d);
    }

    // Provenance: which series are live, how old the ISM seed is, fetch problems.
    let live: Vec<&str> = d.live.iter().map(|i| i.short()).collect();
    let ism_as_of = d.stat(Indicator::Ism).map(|s| period(Indicator::Ism, s.as_of)).unwrap_or_else(|| "n/a".into());
    let fetched = if d.fetched_at > 0 {
        chrono::DateTime::from_timestamp(d.fetched_at, 0)
            .map(|t| t.format("%d %b %H:%M UTC").to_string())
            .unwrap_or_default()
    } else {
        "seeds only".into()
    };
    ui.label(
        RichText::new(format!("FRED live: {} · ISM seed as of {ism_as_of} · fetched {fetched}", live.join(", ")))
            .font(mono(10.5))
            .color(muted()),
    );
    if cfg!(target_arch = "wasm32") {
        ui.label(
            RichText::new("Browser build shows bundled seeds; FRED refresh needs the desktop app.")
                .font(mono(10.5))
                .color(muted()),
        );
    }
    for e in &d.errors {
        ui.label(RichText::new(e).font(mono(10.5)).color(theme::down()));
    }
    (out, refresh)
}

/// One headline card; returns whether it was clicked.
fn card(ui: &mut Ui, d: &Snapshot, ind: Indicator, selected: bool, w: f32) -> bool {
    let (rect, resp) = ui.allocate_exact_size(vec2(w, CARD_H), Sense::click());
    let p = ui.painter_at(rect);
    p.rect_filled(rect, theme::CONTROL_RADIUS, if selected { theme::control_hover() } else { theme::header() });
    if selected {
        p.rect_stroke(rect.shrink(0.5), theme::CONTROL_RADIUS, Stroke::new(1.0, theme::accent()), StrokeKind::Inside);
    }
    let Some(s) = d.stat(ind) else {
        p.text(rect.left_top() + vec2(8.0, 8.0), Align2::LEFT_TOP, ind.short(), mono(10.5), muted());
        p.text(rect.left_top() + vec2(8.0, 26.0), Align2::LEFT_TOP, "no data", mono(12.0), muted());
        return resp.clicked();
    };
    let (big, col) = match ind {
        Indicator::Ism => (format!("{:.1}", s.level), if s.level >= 50.0 { theme::up() } else { theme::down() }),
        _ => (format!("{:+.1}%", s.headline), if s.headline >= 0.0 { theme::up() } else { theme::down() }),
    };
    let sub = match ind {
        Indicator::Gdp => format!("YoY {:+.1}% · ${:.1}T", s.yoy.unwrap_or(0.0), s.level / 1000.0),
        Indicator::M2 => format!("MoM {:+.2}% · pctl {:.0}%", s.mom.unwrap_or(0.0), s.percentile.unwrap_or(0.0)),
        Indicator::Cpi => format!("MoM {:+.2}% · idx {:.1}", s.mom.unwrap_or(0.0), s.level),
        Indicator::Ism => {
            let delta = s.prev_level.map_or(0.0, |prev| s.level - prev);
            format!("Δ {delta:+.1} · {}", if s.level >= 50.0 { "expansion" } else { "contraction" })
        }
    };
    let x = rect.left() + 8.0;
    p.text(
        pos2(x, rect.top() + 7.0),
        Align2::LEFT_TOP,
        ind.short(),
        mono(10.5),
        if selected { amber() } else { muted() },
    );
    p.text(pos2(x, rect.top() + 21.0), Align2::LEFT_TOP, big, mono(16.0), col);
    p.text(pos2(x, rect.top() + 42.0), Align2::LEFT_TOP, sub, mono(10.0), text());
    p.text(pos2(x, rect.top() + 55.0), Align2::LEFT_TOP, period(ind, s.as_of), mono(10.0), muted());
    resp.on_hover_text(format!("{} — click to chart", ind.label())).clicked()
}

/// Line chart of the selected indicator's headline over calendar time.
fn chart(ui: &mut Ui, d: &Snapshot, pane: &MacroPane) {
    let ind = pane.indicator;
    let Some(all) = d.headlines.get(&ind) else { return };
    let now = chrono::Utc::now().timestamp();
    let cutoff = pane.range.years().map_or(i64::MIN, |y| now - (y * 365.25 * 86400.0) as i64);
    let pts: Vec<&Point> = all.iter().filter(|p| p.ts >= cutoff).collect();
    if pts.len() < 2 {
        ui.label(RichText::new("Not enough history in this span.").font(mono(11.0)).color(muted()));
        return;
    }
    let avail = ui.available_size();
    let h = (avail.y * 0.52).max(140.0);
    let (rect, resp) = ui.allocate_exact_size(vec2(avail.x, h), Sense::hover());
    let p = ui.painter_at(rect);
    let plot = Rect::from_min_max(rect.min + vec2(8.0, 8.0), rect.max - vec2(56.0, 22.0));
    let (mut lo, mut hi) =
        pts.iter().fold((f64::INFINITY, f64::NEG_INFINITY), |(a, b), q| (a.min(q.value), b.max(q.value)));
    // Keep the reference line in view: 0 for growth rates, 50 for the PMI.
    let reference = if ind.headline() == Headline::Level { 50.0 } else { 0.0 };
    lo = lo.min(reference);
    hi = hi.max(reference);
    let pad = ((hi - lo) * 0.12).max(0.1);
    let (lo, hi) = (lo - pad, hi + pad);
    let (t0, t1) = (pts[0].ts, pts[pts.len() - 1].ts);
    let x_of = |ts: i64| plot.left() + plot.width() * ((ts - t0) as f32 / (t1 - t0).max(1) as f32);
    let y_of = |v: f64| plot.bottom() - plot.height() * ((v - lo) / (hi - lo)) as f32;
    let fmt = |v: f64| if ind.headline() == Headline::Level { format!("{v:.0}") } else { format!("{v:+.1}%") };
    for t in tr_chart::scale::price_ticks(lo, hi, 6) {
        let y = y_of(t);
        p.hline(plot.x_range(), y, Stroke::new(1.0, theme::grid()));
        p.text(pos2(plot.right() + 6.0, y), Align2::LEFT_CENTER, fmt(t), mono(10.5), theme::axis_text());
    }
    let ts: Vec<i64> = pts.iter().map(|q| q.ts).collect();
    let gap = (90.0 * pts.len() as f64 / plot.width() as f64).max(1.0);
    for (j, label) in tr_chart::scale::time_ticks(&ts, 0, ts.len(), gap, tr_core::Timeframe::MN1) {
        let x = x_of(ts[j]);
        p.vline(x, plot.y_range(), Stroke::new(1.0, theme::grid()));
        p.text(pos2(x, plot.bottom() + 10.0), Align2::CENTER_CENTER, label, mono(10.5), theme::axis_text());
    }
    // Reference line: the PMI boom/bust 50, or zero for growth rates.
    let y = y_of(reference);
    p.hline(plot.x_range(), y, Stroke::new(1.0, muted().gamma_multiply(0.7)));
    p.text(pos2(plot.left() + 4.0, y - 4.0), Align2::LEFT_BOTTOM, fmt(reference), mono(9.5), muted());
    let line: Vec<Pos2> = pts.iter().map(|q| pos2(x_of(q.ts), y_of(q.value))).collect();
    p.add(egui::Shape::line(line.clone(), Stroke::new(1.8, amber())));
    if let Some(last) = line.last() {
        p.circle_filled(*last, 2.6, amber());
    }
    // Hover: nearest observation's date and print.
    if let Some(pos) = resp.hover_pos()
        && plot.contains(pos)
    {
        let frac = (((pos.x - plot.left()) / plot.width()).clamp(0.0, 1.0)) as f64;
        let want = t0 as f64 + frac * (t1 - t0) as f64;
        if let Some(q) = pts.iter().min_by_key(|q| (q.ts as f64 - want).abs() as i64) {
            let x = x_of(q.ts);
            p.vline(x, plot.y_range(), Stroke::new(1.0, muted()));
            p.circle_filled(pos2(x, y_of(q.value)), 3.0, amber());
            p.text(
                plot.left_top() + vec2(4.0, 2.0),
                Align2::LEFT_TOP,
                format!("{}  {}", period(ind, q.ts), fmt(q.value)),
                mono(11.0),
                text(),
            );
        }
    }
    ui.add_space(4.0);
}

/// The ISM components table: latest, previous, change and expansion state.
fn components(ui: &mut Ui, d: &Snapshot) {
    if d.components.is_empty() {
        return;
    }
    ui.label(
        RichText::new(" COMPONENT              LATEST   PREV    Δ   STATE")
            .font(mono(11.0))
            .color(amber())
            .background_color(theme::header()),
    );
    egui::ScrollArea::vertical().id_salt("macro-ism").auto_shrink([false, false]).show(ui, |ui| {
        for c in &d.components {
            // Deliveries are inverted: >50 means slower deliveries (pessimistic per PTM).
            let deliveries = c.name == "Supplier Deliveries";
            let strong = if deliveries { c.latest < 50.0 } else { c.latest >= 50.0 };
            let state = match (deliveries, c.latest >= 50.0) {
                (true, true) => "slower",
                (true, false) => "faster",
                (false, true) => "expansion",
                (false, false) => "contraction",
            };
            let delta = c.prev.map_or(0.0, |p| c.latest - p);
            let line = format!(
                " {:<22} {:>6.1} {:>6} {:+5.1}   ",
                c.name,
                c.latest,
                c.prev.map_or("n/a".into(), |p| format!("{p:.1}")),
                delta
            );
            ui.horizontal(|ui| {
                ui.label(RichText::new(line).font(mono(11.5)).color(text()));
                ui.label(RichText::new(state).font(mono(11.5)).color(if strong { theme::up() } else { theme::down() }));
            });
        }
    });
}

/// `2026 Q3` for quarterly GDP, `Sep 2026` for monthly prints.
fn period(ind: Indicator, ts: i64) -> String {
    chrono::DateTime::from_timestamp(ts, 0)
        .map(|t| {
            if ind == Indicator::Gdp {
                let q = (t.format("%m").to_string().parse::<u32>().unwrap_or(1) - 1) / 3 + 1;
                format!("{} Q{q}", t.format("%Y"))
            } else {
                t.format("%b %Y").to_string()
            }
        })
        .unwrap_or_default()
}
