#!/usr/bin/env python3
"""Extract the ISM manufacturing seed CSV bundled with the app.

Reads the PTM template workbook (finance_excel/ISM_Manufacturing_Index_P8.xlsx by
default) and writes one wide CSV: date plus the composite PMI and the ten component
indices. The app bundles the result (crates/macro/data/ism.csv) and prefers a runtime
copy in <data dir>/macro/ism.csv, so a fresh extract can be dropped in without a rebuild.

Usage: python3 scripts/extract_ism_seed.py [workbook.xlsx] [out.csv]
"""

import sys
from datetime import datetime

import openpyxl

SHEETS = [
    ("PMI", "pmi"),
    ("New Orders", "new_orders"),
    ("Production", "production"),
    ("Employment", "employment"),
    ("Deliveries", "deliveries"),
    ("Inventories", "inventories"),
    ("Customer Inventories", "customer_inventories"),
    ("Prices", "prices"),
    ("Order Backlog", "order_backlog"),
    ("Exports", "exports"),
    ("Imports", "imports"),
]


def series(ws):
    """date -> value from column A/B, skipping headers, blanks and note rows."""
    out = {}
    for row in ws.iter_rows(min_row=2, values_only=True):
        if not row or len(row) < 2:
            continue
        date, value = row[0], row[1]
        if isinstance(date, datetime):
            key = date.strftime("%Y-%m-%d")
        elif isinstance(date, str):
            try:
                key = datetime.strptime(date.strip()[:10], "%Y-%m-%d").strftime("%Y-%m-%d")
            except ValueError:
                continue
        else:
            continue
        try:
            out[key] = float(value)
        except (TypeError, ValueError):
            continue
    return out


def main():
    src = sys.argv[1] if len(sys.argv) > 1 else "/Users/lixun/lixun_dev/opencode/finance_excel/ISM_Manufacturing_Index_P8.xlsx"
    dst = sys.argv[2] if len(sys.argv) > 2 else "crates/macro/data/ism.csv"
    wb = openpyxl.load_workbook(src, read_only=True, data_only=True)
    cols = {name: series(wb[sheet]) for sheet, name in SHEETS}
    dates = sorted(set().union(*[set(c) for c in cols.values()]))
    with open(dst, "w") as f:
        f.write("date," + ",".join(name for _, name in SHEETS) + "\n")
        for d in dates:
            vals = [cols[name].get(d) for _, name in SHEETS]
            f.write(d + "," + ",".join("" if v is None else f"{v:g}" for v in vals) + "\n")
    print(f"{dst}: {len(dates)} months {dates[0]} .. {dates[-1]}")
    for _, name in SHEETS:
        s = cols[name]
        print(f"  {name:20s} {len(s):4d} points, latest {max(s)} = {s[max(s)]:g}")


if __name__ == "__main__":
    main()
