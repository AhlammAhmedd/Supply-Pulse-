#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
validate_qcommerce_data.py
==========================

Proves that a dataset produced by generate_qcommerce_data.py is trustworthy
BEFORE you build a pipeline on top of it.

The idea: the dirty files + the answer key must add up to a perfectly
consistent world. So the validator

  1. SCHEMA        - every file has exactly the documented columns.
  2. TRACEABILITY  - every issue in dq_manifest.csv is really in the files
                     (the dirty value is found at that record / column).
  3. REPAIR        - it reverts every logged issue (drops logged duplicates,
                     puts back clean values) -> a "repaired" dataset.
  4. INVARIANTS    - on the repaired data, all business rules must hold:
                     primary keys, foreign keys, order totals, line totals,
                     inventory (WMS) == stock in CDC, sales in WMS == order lines,
                     CDC before/after chain, SCD2 has no gaps/overlaps, ...
                     If even one fails, noise was injected WITHOUT being logged.
  5. KPIs          - recomputes the business KPIs from the files themselves.
  6. PROFILE       - how dirty is each file (issues per row, per category).

It only uses the Python standard library and streams the big files, so it
also works on the 1M preset.

USAGE
    python validate_qcommerce_data.py --data ./qcommerce_data
Writes  <data>/validation_report.md  and  <data>/validation_results.json
Exit code 0 = every check passed.
"""

from __future__ import annotations

import argparse
import bisect
import csv
import json
import os
import sys
import time
from collections import Counter, defaultdict
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import generate_qcommerce_data as gen  # noqa: E402  (schemas + time helpers live there)

UTC = timezone.utc
TOL = 0.011    # money tolerance (EGP)

F_VEN, F_PRD, F_MENU = "partners_system/vendors.csv", "catalog_pim/products.csv", "restaurant_portal/menu_items.csv"
F_CDC, F_WMS = "ops_db_cdc/vendor_items_events.csv", "wms/inventory_movements.csv"
F_CUS, F_ORD, F_ITM = "customer_app/customers.csv", "customer_app/orders.csv", "customer_app/order_items.csv"
KEYS = {F_VEN: lambda r: r["VendorID"], F_PRD: lambda r: r["product_id"], F_MENU: lambda r: r["menu_item_id"],
        F_WMS: lambda r: r["movement_id"], F_CUS: lambda r: r["customer_id"], F_ORD: lambda r: r["order_id"],
        F_ITM: lambda r: f"{r['order_id']}#{r['line_number']}"}


# =============================================================================
# Report helper
# =============================================================================
class Report:
    def __init__(self):
        self.checks = []

    def check(self, group, name, failures, total, detail=""):
        """Register one check. failures == 0 -> PASS."""
        status = "PASS" if failures == 0 else "FAIL"
        self.checks.append({"group": group, "check": name, "status": status,
                            "failures": failures, "checked": total, "detail": detail})
        print(f"  [{status}] {group} | {name}: {failures} failures / {total} checked {detail}")

    @property
    def ok(self):
        return all(c["status"] == "PASS" for c in self.checks)


def utc_sec(iso_z):
    return int(datetime.strptime(iso_z, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=UTC).timestamp())


def local_naive_sec(s):
    d = datetime.strptime(s, "%Y-%m-%d %H:%M:%S")
    return gen.local_to_ts(d.year, d.month, d.day, d.hour, d.minute, d.second)


def local_dmy_sec(s):
    d = datetime.strptime(s, "%d/%m/%Y %H:%M:%S")
    return gen.local_to_ts(d.year, d.month, d.day, d.hour, d.minute, d.second)


# =============================================================================
# 1. Manifest (answer key) loading
# =============================================================================
def load_manifest(path):
    fixes = defaultdict(dict)                         # file -> key -> [(col, clean_json, dirty_json)]
    rowdups = defaultdict(lambda: defaultdict(list))  # file -> key -> [dirty row dict]
    cdc_dups, entity = set(), defaultdict(set)
    issues_per_row = defaultdict(Counter)             # file -> key -> number of field issues
    cats = defaultdict(Counter)
    with open(path, encoding="utf-8") as f:
        for r in csv.DictReader(f):
            base = r["source_file"].split("#")[0]
            key, col = r["record_key"], r["column"]
            cats[base][r["issue_category"]] += 1
            if col == "*row":
                if base == F_CDC:
                    cdc_dups.add(int(key.split("=")[1]))
                else:
                    rowdups[base][key].append(json.loads(r["dirty_value"]))
            elif col == "*entity":
                entity[base].add(key)
            else:
                k = int(key.split("=")[1]) if base == F_CDC else key
                fixes[base].setdefault(k, []).append((col, r["clean_value"], r["dirty_value"]))
                issues_per_row[base][k] += 1
    return fixes, rowdups, cdc_dups, entity, issues_per_row, cats


# =============================================================================
# 2. Generic CSV pass: schema + traceability + repair (streaming)
# =============================================================================
def repaired_rows(data, file, M, rep, trace):
    fixes, rowdups = M[0][file], M[1][file]
    columns = gen.cols(file)
    keyfn = KEYS[file]
    pending = {k: [tuple(gen.cell(d.get(c)) for c in columns) for d in lst] for k, lst in rowdups.items()}
    with open(os.path.join(data, file), encoding="utf-8", newline="") as f:
        rd = csv.reader(f)
        header = next(rd)
        rep.check("1. schema", f"{file} columns match the data dictionary",
                  0 if header == columns else 1, 1, "" if header == columns else f"got {header}")
        for raw in rd:
            row = dict(zip(header, raw))
            k = keyfn(row)
            if k in pending and tuple(raw) in pending[k]:
                pending[k].remove(tuple(raw))
                trace["dup_found"] += 1
                continue
            for col, clean_j, dirty_j in fixes.get(k, ()):
                found = row.get(col) == gen.cell(json.loads(dirty_j))
                row[col] = gen.cell(json.loads(clean_j))
                trace["fix_found" if found else "fix_missing"] += 1
            yield row
    trace["dup_missing"] += sum(len(v) for v in pending.values())


def check_pk(rep, file, keys):
    c = Counter(keys)
    dups = sum(n - 1 for n in c.values() if n > 1)
    rep.check("3. keys", f"{file} primary key unique after repair", dups, len(keys))


# =============================================================================
# 3. Main validation
# =============================================================================
def validate(data):
    t0 = time.time()
    rep = Report()
    A = os.path.join(data, "_answer_key")
    summary = json.load(open(os.path.join(A, "generation_summary.json"), encoding="utf-8"))
    print("Loading answer key ...")
    M = load_manifest(os.path.join(A, "dq_manifest.csv"))
    fixes, rowdups, cdc_dups, entity, issues_per_row, cats = M
    trace = Counter()

    # ---------------- master data ----------------
    print("Vendors / products / menu / customers ...")
    vendors, ven_keys = {}, []
    for r in repaired_rows(data, F_VEN, M, rep, trace):
        ven_keys.append(r["VendorID"])
        vendors[r["VendorID"]] = r
    check_pk(rep, F_VEN, ven_keys)
    names = Counter(v["Vendor_Name"] for v in vendors.values())
    rep.check("4. domains", "realism: every vendor display name is unique",
              sum(n - 1 for n in names.values() if n > 1), len(vendors))
    zone_city = {v["Area"]: v["City"] for v in vendors.values()}
    bad = sum(1 for v in vendors.values() if v["Vendor_Type"] not in gen.VENDOR_TYPES
              or v["Integration_Type"] not in gen.INTEGRATIONS or v["Status"] not in ("active", "inactive"))
    rep.check("4. domains", "vendors: type / integration / status are canonical after repair", bad, len(vendors))
    bad = sum(1 for v in vendors.values() if not (1 <= float(v["Rating"]) <= 5) or float(v["Commission_Pct"]) > 100)
    rep.check("4. domains", "vendors: rating 1-5 and commission <= 100 after repair", bad, len(vendors))

    products, barcode_to_sku, prod_keys = {}, {}, []
    for r in repaired_rows(data, F_PRD, M, rep, trace):
        prod_keys.append(r["product_id"])
        products[r["product_id"]] = r
        if r["product_id"] not in entity[F_PRD]:
            barcode_to_sku[r["barcode"]] = "SKU-" + r["product_id"][1:]
    check_pk(rep, F_PRD, prod_keys)
    # Excel keeps 6 significant digits (+/- 5,000,000): two DIFFERENT products closer than 10,000,000
    # could not be told apart. Deliberate duplicate catalog entries (same barcode) are excluded.
    codes = sorted({int(r["barcode"]) for k, r in products.items() if k not in entity[F_PRD]})
    close = sum(1 for a, b in zip(codes, codes[1:]) if b - a < 10_000_000)
    rep.check("4. domains", "realism: a barcode in Excel notation maps to exactly one product", close, len(codes))
    bad = sum(1 for p in products.values() if gen.ean13(p["barcode"][:12]) != p["barcode"])
    rep.check("4. domains", "products: every barcode is a valid EAN-13 after repair", bad, len(products))
    bad = sum(1 for p in products.values() if float(p["base_price_egp"]) <= 0)
    rep.check("4. domains", "products: base price > 0 after repair", bad, len(products))

    menu, menu_keys = {}, []
    for r in repaired_rows(data, F_MENU, M, rep, trace):
        menu_keys.append(r["menu_item_id"])
        menu[r["menu_item_id"]] = r
    check_pk(rep, F_MENU, menu_keys)
    bad = sum(1 for m in menu.values() if m["vendor_id"] not in vendors or vendors[m["vendor_id"]]["Vendor_Type"] != "restaurant")
    rep.check("5. foreign keys", "menu_items.vendor_id -> a restaurant in vendors", bad, len(menu))

    customers, cus_keys = set(), []
    for r in repaired_rows(data, F_CUS, M, rep, trace):
        cus_keys.append(r["customer_id"])
        customers.add(r["customer_id"])
    check_pk(rep, F_CUS, cus_keys)

    # ---------------- scenario labels ----------------
    print("Scenario labels ...")
    passed, scen_count, rejects = {}, Counter(), Counter()
    anomaly_total = anomaly_rejected = 0
    with open(os.path.join(A, "scenario_events.csv"), encoding="utf-8") as f:
        for r in csv.DictReader(f):
            passed[int(r["lsn"])] = r["expected_gate_result"] == "PASS"
            scen_count[r["scenario"]] += 1
            if r["expected_gate_result"] == "REJECT":
                rejects[r["reject_reason"]] += 1
            if r["scenario"].startswith("DQ_ANOMALY"):
                anomaly_total += 1
                anomaly_rejected += r["expected_gate_result"] == "REJECT"

    # ---------------- CDC (flat CSV) ----------------
    print("CDC events (streaming) ...")
    vendor_int = {k: v["Integration_Type"] for k, v in vendors.items()}
    seen, dup_unlogged, n_lines = set(), 0, 0
    per_key = defaultdict(list)   # (vendor_id, item_ref) -> [(lsn, sec, price, avail, qty, op, captured_ms)]
    late_seen, max_lsn_seen, orphan_cdc = 0, 0, 0
    with open(os.path.join(data, F_CDC), encoding="utf-8", newline="") as f:
        rd = csv.reader(f)
        header = next(rd)
        expected_cols = gen.cols(F_CDC)
        rep.check("1. schema", f"{F_CDC} columns match the data dictionary",
                  0 if header == expected_cols else 1, 1, "" if header == expected_cols else f"got {header}")
        for raw in rd:
            n_lines += 1
            row = dict(zip(header, raw))
            lsn = int(row["lsn"])
            if lsn in seen:                       # exact re-delivery of an event already read
                if lsn not in cdc_dups:
                    dup_unlogged += 1
                trace["dup_found"] += 1
                continue
            seen.add(lsn)
            if lsn < max_lsn_seen:
                late_seen += 1
            max_lsn_seen = max(max_lsn_seen, lsn)
            vid, ref = f"V{int(row['store_id']):04d}", row["item_ref"]
            if vid not in vendors or (ref not in menu and ("P" + ref[4:]) not in products):
                orphan_cdc += 1
            for col, clean_j, dirty_j in fixes[F_CDC].get(lsn, ()):
                found = row.get(col) == gen.cell(json.loads(dirty_j))
                trace["fix_found" if found else "fix_missing"] += 1
                row[col] = gen.cell(json.loads(clean_j))
            sec = price = avail = qty = None
            if row["op"] != "d":
                sec = int(local_naive_sec(row["updated_at"]))
                if row["item_price"] != "":
                    price = float(row["item_price"]) / (100.0 if vendor_int.get(vid) == "POS_LEGACY" else 1.0)
                avail = row["is_available"] == "true"
                qty = int(row["qty_on_hand"]) if row["qty_on_hand"] != "" else None
            per_key[(vid, ref)].append((lsn, sec, price, avail, qty, row["op"], int(row["captured_at_ms"])))
    rep.check("2. traceability", "every duplicated CDC event is logged in the manifest", dup_unlogged, n_lines - len(seen))
    rep.check("5. foreign keys", "CDC store_id / item_ref -> vendors and catalog/menu", orphan_cdc, len(seen))
    missing_lbl = sum(1 for l in seen if l not in passed) + sum(1 for l in passed if l not in seen)
    rep.check("6. CDC", "scenario_events.csv labels exactly the CDC events (1:1 on lsn)", missing_lbl, len(seen))

    # Restaurants open at 07:00 at the earliest and close by 02:00: no manager / POS change at 03-06 h.
    night = sum(1 for (vid, ref), evs in per_key.items()
                if vendors.get(vid, {}).get("Vendor_Type") == "restaurant"
                for e in evs if e[1] is not None and 3 <= gen.to_local(e[1]).hour < 7)
    rep.check("6. CDC", "realism: no restaurant change between 03:00 and 06:59 (Cairo)", night, len(seen))
    order_bad = 0
    for evs in per_key.values():
        evs.sort()
        for i in range(1, len(evs)):
            if evs[i][6] < evs[i - 1][6]:
                order_bad += 1
    rep.check("6. CDC", "capture time never decreases with LSN for the same item", order_bad, len(seen))
    rep.check("6. CDC", "late-arriving events present in delivery order (expected > 0)",
              0 if late_seen > 0 else 1, len(seen), f"({late_seen} events delivered after a newer LSN)")

    # ---------------- expected SCD2 ----------------
    print("Expected SCD2 ...")
    scd = defaultdict(list)
    with open(os.path.join(A, "expected_scd2_vendor_items.csv"), encoding="utf-8") as f:
        for r in csv.DictReader(f):
            scd[(r["vendor_id"], r["item_ref"])].append(
                (int(r["version"]), float(r["price_egp"]), r["is_available"] == "true", utc_sec(r["valid_from_utc"]),
                 utc_sec(r["valid_to_utc"]) if r["valid_to_utc"] else None, r["is_current"] == "true"))
    n_scd = sum(len(v) for v in scd.values())
    list_price = {**{"SKU-" + k[1:]: float(r["base_price_egp"]) for k, r in products.items()},
                  **{k: float(r["price_egp"]) for k, r in menu.items()}}
    over_band = sum(1 for (vid, ref), vs in scd.items() for v in vs
                    if ref in list_price and v[1] > 1.35 * list_price[ref] + 0.01)
    rep.check("7. SCD2 truth", "realism: no accepted price above 1.35 x the list price", over_band, n_scd)
    gaps = overlaps = multi_current = numbering = no_change = unexplained = zero_len = 0
    for key, vs in scd.items():
        vs.sort()
        if [v[0] for v in vs] != list(range(1, len(vs) + 1)):
            numbering += 1
        if sum(v[5] for v in vs) > 1:
            multi_current += 1
        pass_secs = {e[1] for e in per_key.get(key, ()) if e[1] is not None and passed.get(e[0])}
        for i, v in enumerate(vs):
            if v[4] is not None and v[4] <= v[3]:
                zero_len += 1
            if v[3] not in pass_secs:
                unexplained += 1
            if i:
                p = vs[i - 1]
                if p[4] is None:
                    overlaps += 1
                elif p[4] < v[3]:
                    gaps += 1
                elif p[4] > v[3]:
                    overlaps += 1
                if (p[1], p[2]) == (v[1], v[2]):
                    no_change += 1
    rep.check("7. SCD2 truth", "versions numbered 1..n per key", numbering, len(scd))
    rep.check("7. SCD2 truth", "no gaps between consecutive versions", gaps, n_scd)
    rep.check("7. SCD2 truth", "no overlapping versions", overlaps, n_scd)
    rep.check("7. SCD2 truth", "at most one current version per key", multi_current, len(scd))
    rep.check("7. SCD2 truth", "no zero-length versions (same-second changes collapsed)", zero_len, n_scd)
    rep.check("7. SCD2 truth", "consecutive versions really differ (no-op updates ignored)", no_change, n_scd)
    rep.check("7. SCD2 truth", "every version starts at an ACCEPTED CDC event of that key", unexplained, n_scd)
    scd_starts = {(k, v[3]) for k, vs in scd.items() for v in vs}
    leaked = 0
    for key, evs in per_key.items():
        ok_secs = {e[1] for e in evs if passed.get(e[0])}
        for e in evs:
            if not passed.get(e[0]) and (key, e[1]) in scd_starts and e[1] not in ok_secs:
                leaked += 1
    rep.check("7. SCD2 truth", "no version is created by a REJECTED (DLQ) event", leaked, sum(rejects.values()))

    # ---------------- orders ----------------
    print("Orders ...")
    orders = {}
    n_deliv = on_time = bad_promo = 0
    per_customer = Counter()
    per_customer_day, cust_zones, per_weekday, days_of_weekday = Counter(), defaultdict(set), Counter(), defaultdict(set)
    bad_zone_city = 0
    bad_total = bad_status = bad_reason = bad_deliv = fk_v = fk_c = 0
    ord_keys = []
    for r in repaired_rows(data, F_ORD, M, rep, trace):
        oid = r["order_id"]
        ord_keys.append(oid)
        sub, disc, fee, svc, tot = (float(r[c]) for c in ("subtotal", "discount_amount", "delivery_fee", "service_fee", "total_amount"))
        if abs(sub - disc + fee + svc - tot) > TOL:
            bad_total += 1
        if r["status"] not in gen.ORDER_STATUS:
            bad_status += 1
        if (r["status"] == "delivered") == bool(r["cancel_reason"]):
            bad_reason += 1
        t = utc_sec(r["order_ts"])
        if r["delivered_at"] and utc_sec(r["delivered_at"]) < t:
            bad_deliv += 1
        fk_v += r["vendor_id"] not in vendors
        fk_c += r["customer_id"] not in customers
        orders[oid] = (sub, None, r["vendor_id"], t, r["status"], r["cancel_reason"])
        # realism: delivery time vs ETA, orders per customer, weekend-only promo
        if r["status"] == "delivered" and r["delivered_at"]:
            n_deliv += 1
            on_time += (utc_sec(r["delivered_at"]) - t) / 60 <= float(r["estimated_delivery_min"])
        per_customer[r["customer_id"]] += 1
        loc = gen.to_local(t)
        per_customer_day[(r["customer_id"], loc.date())] += 1
        cust_zones[r["customer_id"]].add(r["delivery_zone"])
        per_weekday[loc.weekday()] += 1
        days_of_weekday[loc.weekday()].add(loc.date())
        if r["vendor_id"] in vendors and zone_city.get(r["delivery_zone"]) != vendors[r["vendor_id"]]["City"]:
            bad_zone_city += 1
        if r["promo_code"] == "WEEKEND20" and gen.to_local(t).weekday() not in gen.EGYPT_WEEKEND:
            bad_promo += 1
    check_pk(rep, F_ORD, ord_keys)
    n_o = len(orders)
    del ord_keys
    rep.check("8. orders", "total = subtotal - discount + delivery_fee + service_fee", bad_total, n_o)
    rep.check("4. domains", "orders: status canonical after repair", bad_status, n_o)
    rep.check("8. orders", "cancel_reason present exactly for non-delivered orders", bad_reason, n_o)
    rep.check("8. orders", "delivered_at >= order_ts", bad_deliv, n_o)
    # Realism guards (added after v3.0 shipped with ~30% on-time delivery and one 5,136-order customer)
    pct_on_time = 100 * on_time / max(1, n_deliv)
    rep.check("8. orders", "realism: 70-92% of delivered orders arrive within the ETA",
              0 if 70 <= pct_on_time <= 92 else 1, 1, f"{pct_on_time:.1f}% on time")
    heaviest = max(per_customer.values()) if per_customer else 0
    rep.check("8. orders", "realism: no customer places more than 60 orders in the window",
              sum(1 for n in per_customer.values() if n > 60), len(per_customer), f"max {heaviest} orders")
    rep.check("8. orders", "realism: no customer places more than 4 orders on one day",
              sum(1 for n in per_customer_day.values() if n > 4), len(per_customer_day),
              f"max {max(per_customer_day.values()) if per_customer_day else 0} a day")
    rep.check("8. orders", "realism: each customer always receives orders in one home zone",
              sum(1 for zs in cust_zones.values() if len(zs) > 1), len(cust_zones))
    rep.check("8. orders", "realism: delivery zone is in the vendor's city", bad_zone_city, n_o)
    avg_day = {d: per_weekday[d] / max(1, len(days_of_weekday[d])) for d in per_weekday}
    rep.check("8. orders", "realism: Friday is busier than Monday (weekday demand)",
              0 if avg_day.get(4, 0) > avg_day.get(0, 0) else 1, 1,
              f"Fri {avg_day.get(4, 0):.0f} vs Mon {avg_day.get(0, 0):.0f} orders/day")
    del per_customer_day, cust_zones
    rep.check("8. orders", "realism: WEEKEND20 promo used only on Friday / Saturday (Cairo)", bad_promo, n_o)
    del per_customer
    rep.check("5. foreign keys", "orders.vendor_id -> vendors", fk_v, n_o)
    rep.check("5. foreign keys", "orders.customer_id -> customers", fk_c, n_o)

    print("Order items (streaming) ...")
    sums, counts, dark_qty = defaultdict(float), Counter(), Counter()
    bad_line = fk_o = fk_i = stale_price = lines = scd_price_diff = dup_item_keys = 0
    leak_egp = 0.0
    seen_items = set()
    for r in repaired_rows(data, F_ITM, M, rep, trace):
        lines += 1
        k = f"{r['order_id']}#{r['line_number']}"
        if k in seen_items:
            dup_item_keys += 1
        seen_items.add(k)
        q, up, lt, vp = int(r["quantity"]), float(r["unit_price"]), float(r["line_total"]), float(r["vendor_confirmed_price"])
        if abs(q * up - lt) > TOL or q <= 0:
            bad_line += 1
        o = orders.get(r["order_id"])
        if o is None:
            fk_o += 1
            continue
        if r["item_ref"] not in menu and ("P" + r["item_ref"][4:]) not in products:
            fk_i += 1
        sums[r["order_id"]] += lt
        counts[r["order_id"]] += 1
        if abs(vp - up) > 0.001:
            stale_price += 1
            leak_egp += (vp - up) * q
        if vendors[o[2]]["Vendor_Type"] == "dark_store" and o[4] in ("delivered", "refunded", "failed"):
            dark_qty[r["order_id"]] += q
        # app price vs the SCD2 price valid at order time (difference = cache / sync lag)
        vs = scd.get((o[2], r["item_ref"]))
        if vs:
            i = bisect.bisect_right([v[3] for v in vs], o[3]) - 1
            if i >= 0 and abs(vs[i][1] - up) > 0.001:
                scd_price_diff += 1
    del seen_items
    rep.check("3. keys", f"{F_ITM} primary key (order_id, line_number) unique after repair", dup_item_keys, lines)
    rep.check("8. orders", "line_total = quantity x unit_price and quantity > 0", bad_line, lines)
    rep.check("5. foreign keys", "order_items.order_id -> orders", fk_o, lines)
    rep.check("5. foreign keys", "order_items.item_ref -> menu_items / catalog", fk_i, lines)
    bad_sub = sum(1 for oid, o in orders.items() if abs(sums[oid] - o[0]) > TOL)
    bad_cnt = sum(1 for oid in orders if counts[oid] == 0)
    rep.check("8. orders", "subtotal = sum of line_total", bad_sub, n_o)
    rep.check("8. orders", "every order has at least one line", bad_cnt, n_o)

    # ---------------- WMS / inventory ----------------
    print("Inventory movements ...")
    balance, wms_sale = Counter(), Counter()
    negative_bal = fk_w = bad_type = 0
    mv_keys, movements = [], []
    for r in repaired_rows(data, F_WMS, M, rep, trace):
        mv_keys.append(r["movement_id"])
        vid = "V" + r["warehouse_code"][3:]
        sku = barcode_to_sku.get(r["product_barcode"])
        if vid not in vendors or sku is None:
            fk_w += 1
            continue
        if r["movement_type"] not in gen.MOVEMENT_TYPES:
            bad_type += 1
        q = int(r["quantity"])
        movements.append((local_dmy_sec(r["movement_ts"]), r["movement_id"], (vid, sku), q))
        if r["movement_type"] == "SALE":
            wms_sale[r["reference_id"]] += -q
    check_pk(rep, F_WMS, mv_keys)
    rep.check("5. foreign keys", "WMS warehouse_code -> vendor, barcode -> catalog", fk_w, len(mv_keys))
    rep.check("4. domains", "WMS movement_type canonical after repair", bad_type, len(mv_keys))
    movements.sort()
    for _ts, _id, key, q in movements:
        balance[key] += q
        if balance[key] < 0:
            negative_bal += 1
    rep.check("9. inventory", "running stock balance never negative", negative_bal, len(movements))
    final_cdc = {}
    for key, evs in per_key.items():
        if vendors.get(key[0], {}).get("Vendor_Type") == "dark_store":
            last = evs[-1]
            final_cdc[key] = 0 if last[5] == "d" else last[4]
    recon_bad = sum(1 for key, q in final_cdc.items() if int(balance.get(key, 0)) != int(q))
    rep.check("9. inventory", "WMS final balance == last qty_on_hand in CDC (every dark-store item)", recon_bad, len(final_cdc))
    sale_bad = sum(1 for oid in set(dark_qty) | set(wms_sale) if dark_qty[oid] != wms_sale[oid])
    rep.check("9. inventory", "WMS SALE quantity per order == ordered quantity (dark stores)", sale_bad, len(dark_qty))

    # ---------------- traceability totals ----------------
    rep.check("2. traceability", "every field-level issue in the manifest is found in the files",
              trace["fix_missing"], trace["fix_found"] + trace["fix_missing"])
    rep.check("2. traceability", "every logged duplicate row is found in the files",
              trace["dup_missing"], trace["dup_found"] + trace["dup_missing"])

    # ---------------- KPIs recomputed from the files ----------------
    stale_cancel = sum(1 for o in orders.values() if o[5] == "item_unavailable")
    kpi = {
        "orders": n_o, "order_lines": lines,
        "orders_cancelled_item_unavailable": stale_cancel,
        "orders_cancelled_item_unavailable_pct": round(100 * stale_cancel / max(1, n_o), 2),
        "order_lines_stale_price": stale_price,
        "order_lines_stale_price_pct": round(100 * stale_price / max(1, lines), 2),
        "price_leakage_egp_absorbed_by_platform": round(leak_egp, 2),
        "order_lines_where_app_price_differs_from_SCD2_price_at_order_time": scd_price_diff,
    }
    g = summary["kpis"]
    rep.check("10. KPIs", "recomputed stale-availability cancellations == generator KPI",
              abs(stale_cancel - g["orders_cancelled_because_app_showed_sold_out_item"]), n_o)
    rep.check("10. KPIs", "recomputed stale-price lines == generator KPI",
              abs(stale_price - g["order_lines_with_stale_price"]), lines)

    # ---------------- scenario coverage ----------------
    need = ["HAPPY_PRICE_CHANGE", "PROMO_START", "PROMO_END", "DQ_ANOMALY_ZERO", "DQ_ANOMALY_NEGATIVE",
            "DQ_ANOMALY_FAT_UP", "DQ_ANOMALY_FAT_DOWN", "DQ_ANOMALY_NULL_NAME", "DQ_CORRECTION",
            "SCD_STRESS_TOGGLE", "SCD_STRESS_SAME_SECOND", "SCD_STRESS_NOOP", "SCD_STRESS_LATE_ARRIVAL",
            "SALE_DEDUCTION", "RESTOCK", "WASTE", "STOCK_ADJUSTMENT", "STOCKOUT_REPORTED", "DELIST"]
    missing = [s for s in need if scen_count[s] == 0]
    rep.check("11. coverage", "every business scenario is present", len(missing), len(need),
              ("missing: " + ", ".join(missing)) if missing else "")
    rep.check("11. coverage", "every manager-typo anomaly is labelled REJECT (goes to DLQ)",
              anomaly_total - anomaly_rejected, anomaly_total)
    busy = Counter()
    for key, evs in per_key.items():
        if key[1].startswith("M"):
            for e in evs:
                if e[1] is not None:
                    busy[(key, e[1] // 86400)] += 1
    stress_days = sum(1 for n in busy.values() if n >= 5)
    rep.check("11. coverage", "restaurant items with >= 5 changes in one day exist (SCD2 stress)",
              0 if stress_days else 1, len(busy), f"({stress_days} item-days)")

    # ---------------- noise profile ----------------
    prof = {}
    row_counts = summary["row_counts"]
    for file, per_row in issues_per_row.items():
        dist = Counter(min(n, 4) for n in per_row.values())
        rows = row_counts.get(file, 0)
        prof[file] = {"rows": rows, "rows_with_issues": len(per_row),
                      "rows_with_issues_pct": round(100 * len(per_row) / max(1, rows), 1),
                      "rows_with_1_issue": dist[1], "rows_with_2_issues": dist[2], "rows_with_3_issues": dist[3],
                      "rows_with_4plus_issues": dist[4], "issues_by_category": dict(cats[file])}
    multi = sum(p["rows_with_2_issues"] + p["rows_with_3_issues"] + p["rows_with_4plus_issues"] for p in prof.values())
    rep.check("12. noise", "multi-issue rows exist (clustered noise, not one issue per row)", 0 if multi else 1, 1, f"({multi} rows)")

    results = {"generated_at_utc": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"), "data": os.path.abspath(data),
               "all_passed": rep.ok, "checks": rep.checks, "kpis": kpi, "scenarios": dict(scen_count),
               "dlq_reject_reasons": dict(rejects), "noise_profile": prof, "row_counts": row_counts,
               "runtime_s": round(time.time() - t0, 1)}
    with open(os.path.join(data, "validation_results.json"), "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    write_markdown(os.path.join(data, "validation_report.md"), results)
    print(f"\n{'ALL CHECKS PASSED' if rep.ok else 'SOME CHECKS FAILED'}  ({len(rep.checks)} checks, {results['runtime_s']} s)")
    return rep.ok


def write_markdown(path, R):
    L = ["# Validation report", "", f"Data: `{R['data']}`  ", f"Generated: {R['generated_at_utc']}  ",
         f"Result: **{'ALL CHECKS PASSED' if R['all_passed'] else 'SOME CHECKS FAILED'}** "
         f"({len(R['checks'])} checks, {R['runtime_s']} s)", "", "## Checks", "",
         "| Group | Check | Status | Failures | Checked |", "|---|---|---|---|---|"]
    for c in R["checks"]:
        L.append(f"| {c['group']} | {c['check']} {c['detail']} | {c['status']} | {c['failures']} | {c['checked']} |")
    L += ["", "## Row counts", "", "| File | Rows |", "|---|---|"]
    L += [f"| {k} | {v:,} |" for k, v in R["row_counts"].items()]
    L += ["", "## KPIs recomputed from the files", "", "| KPI | Value |", "|---|---|"]
    L += [f"| {k} | {v} |" for k, v in R["kpis"].items()]
    L += ["", "## Noise profile", "", "| File | Rows | Rows with issues | 1 issue | 2 | 3 | 4+ |", "|---|---|---|---|---|---|---|"]
    for f, p in R["noise_profile"].items():
        L.append(f"| {f} | {p['rows']:,} | {p['rows_with_issues']:,} ({p['rows_with_issues_pct']}%) | {p['rows_with_1_issue']:,} | "
                 f"{p['rows_with_2_issues']:,} | {p['rows_with_3_issues']:,} | {p['rows_with_4plus_issues']:,} |")
    L += ["", "## DLQ reject reasons (expected)", "", "| Reason | Events |", "|---|---|"]
    L += [f"| {k} | {v:,} |" for k, v in sorted(R["dlq_reject_reasons"].items(), key=lambda x: -x[1])]
    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(L) + "\n")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", default="./qcommerce_data")
    sys.exit(0 if validate(ap.parse_args().data) else 1)
