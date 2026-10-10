"""Loads master data (vendors, products, menu items) from the generated CSVs."""
import csv
import os
import re

from rules import Reference, parse_number


def _col(row, *names):
    low = {k.strip().lower(): v for k, v in row.items()}
    for n in names:
        if n in low:
            return low[n]
    return None


def _integration(raw):
    s = (raw or "").strip().lower()
    if "legacy" in s or s.startswith("pos"):
        return "POS_LEGACY"
    if "portal" in s or "manual" in s:
        return "MANUAL_PORTAL"
    if "wms" in s:
        return "WMS"
    if "api" in s:
        return "API"
    return s.upper()


def load_reference(data_dir: str) -> Reference:
    base = data_dir if os.path.isdir(os.path.join(data_dir, "ops_db_cdc")) else os.path.join(data_dir, "qcommerce_data")
    integration, list_price = {}, {}

    with open(os.path.join(base, "partners_system", "vendors.csv"), newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            vid = _col(r, "vendorid", "vendor_id", "id")
            m = re.search(r"(\d+)", vid or "")
            if m:
                integration[int(m.group(1))] = _integration(_col(r, "integration_type"))

    with open(os.path.join(base, "catalog_pim", "products.csv"), newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            m = re.search(r"(\d+)", _col(r, "product_id") or "")
            price = parse_number(_col(r, "base_price_egp"))
            if m and price and price > 0:
                list_price[f"SKU-{int(m.group(1)):05d}"] = price

    with open(os.path.join(base, "restaurant_portal", "menu_items.csv"), newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            ref = (_col(r, "menu_item_id") or "").strip()
            price = parse_number(_col(r, "price_egp"))
            if ref and price and price > 0:
                if price >= 1000:            # master-data unit noise: some menu prices are in piasters
                    price /= 100.0           # (clean menu prices are all below 500 EGP)
                list_price[ref] = price

    return Reference(integration, list_price)
