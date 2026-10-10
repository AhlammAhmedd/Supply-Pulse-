#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
generate_qcommerce_data.py  (v3.2)
==================================

v3.2 = v3.1 + logic fixes found by a full audit of the v3.0 data (same files, same columns,
same noise; only the behaviour of the simulated world changed):
  * delivery time depends on the distance and on busy hours (~80% on time, worse at peak)  [v3.1]
  * customer ordering frequency capped (heaviest users ~1-2 orders a day, not 188)          [v3.1, tighter]
  * WEEKEND20 only on Friday / Saturday                                                     [v3.1]
  * every vendor name is unique (no two "Burger Lab - Miami")
  * product barcodes are spaced so a barcode in Excel notation (6 significant digits) still
    points to exactly ONE product (the deliberate duplicate catalog entries keep their twin's barcode)
  * every customer has a home zone; orders go to that zone, from vendors within delivery range
  * demand varies by weekday (Thursday / Friday busiest)
  * vendor-side changes (managers, POS syncs, stock-outs, promos) only while the vendor is open;
    dark-store waste checks in the morning, cycle counts at night
  * prices stay within a realistic band of the list price (no menu price x2.3 in one month)

v3 changes: fewer columns (only what the analysis and the model use), the CDC
file is a flat CSV (one row per change), less noise (~12% of rows), order
hours follow an Egyptian pattern per vendor type, 1.0-1.5M rows in total.

Synthetic, deliberately messy, multi-source data for a Talabat / Breadfast-style
platform (restaurants + quick-commerce groceries), built for a project about
STALE PRICES and STALE AVAILABILITY shown in the customer app.

THE STORY THE DATA TELLS
------------------------
* Vendors (restaurants, dark stores, supermarkets, pharmacies) change prices and
  availability all day. Those changes land in an operational PostgreSQL table
  `vendor_items` and are captured by CDC (one CSV row per change).
* Every change happens twice: first in the PHYSICAL world (the kitchen runs out
  of chicken, the manager decides a new price) and later in the SYSTEM (somebody
  reports it, the POS syncs it). The delay depends on the vendor's integration.
* On top of that the app reads a cache that refreshes every 15 minutes.
  So customers sometimes order items that are already sold out (-> cancelled
  orders) or pay an old price (-> stale price). Your pipeline must fix that.
* Dark stores are platform-owned warehouses: every sale, restock, waste and stock
  count is also written to a Warehouse Management System (WMS) -> inventory data.

THREE EXPLICIT BUSINESS SCENARIOS (labelled in _answer_key/scenario_events.csv)
------------------------------------------------------------------------------
1. Happy path        : normal price changes (e.g. a meal goes from 100 -> 120 EGP),
                       promotions that start and end.
2. DQ anomalies      : human typos by vendor managers: price 0, negative price
                       (-50), fat-finger x10 / /10, missing item name (NULL).
                       Each anomaly is usually corrected minutes later.
                       -> these events MUST be rejected by your DQ gate (DLQ).
3. SCD2 stress test  : restaurants toggling availability many times in one day
                       (kitchen runs out, restocks), including:
                       * two changes in the SAME second (net zero change)
                       * "no-op" updates (row saved without any change)
                       * late-arriving events (delivered after newer events)
                       -> your SCD2 must have no duplicates, gaps or overlaps.

DATA-QUALITY NOISE (separate from the scenarios, logged in dq_manifest.csv)
--------------------------------------------------------------------------
Nine categories: column naming, missing values, duplicates, wrong data types,
date formats, value representation, business terminology, invalid values,
units/formats. Noise is CLUSTERED on purpose, like in real life:
* "garbage rows"   : a few % of rows get several broken columns at once,
* "sloppy vendors" : vendors that type data manually get ~4x more errors,
* "bad release"    : during 2 days a buggy app release writes broken orders.

ANSWER KEY (do not open before building your pipeline)
------------------------------------------------------
_answer_key/dq_manifest.csv                 every injected noise issue (clean vs dirty value)
_answer_key/scenario_events.csv             every CDC event: scenario + expected DQ-gate result
_answer_key/expected_scd2_vendor_items.csv  the SCD2 table a correct pipeline produces
_answer_key/column_mapping.csv              same concept, different column names across sources
_answer_key/data_dictionary.json            machine-readable schema of every file
_answer_key/generation_summary.json         counts, parameters, KPIs

USAGE
-----
    python generate_qcommerce_data.py                       # small preset (fast, for development)
    python generate_qcommerce_data.py --preset full         # ~1.3M rows in total (a few minutes)
    python generate_qcommerce_data.py --dirty-rate 0        # no noise (scenarios still present)
    python generate_qcommerce_data.py --anomaly-rate 0      # no manager typos

Only the Python standard library is used. Output is written in a streaming way.
The same --seed always produces exactly the same files.
"""

from __future__ import annotations

import argparse
import bisect
import csv
import heapq
import json
import math
import os
import random
import re
import sys
from collections import Counter
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from functools import lru_cache

UTC = timezone.utc

# =============================================================================
# 1. ISSUE CATEGORIES (values of dq_manifest.issue_category)
# =============================================================================
MISSING = "MISSING_VALUE"
DUPLICATE = "DUPLICATE_RECORD"
WRONG_TYPE = "WRONG_DATA_TYPE"
DATE_FMT = "DATE_FORMAT"
REPR = "VALUE_REPRESENTATION"
TERM = "BUSINESS_TERMINOLOGY"
INVALID = "INVALID_VALUE"
UNIT = "UNIT_FORMAT"

GARBAGE_MULT = 5.0        # how much more error-prone a "garbage row" is
SLOPPY_MULT = 4.0         # how much more error-prone a sloppy vendor's rows are
CACHE_REFRESH_S = 900     # the app cache refreshes every 15 minutes (the "as-is" problem)
POS_BATCH_S = 1800        # legacy POS integrations push changes every 30 minutes
REORDER_POINT = 8         # dark store re-orders when stock falls to this level


# =============================================================================
# 2. CONFIGURATION
# =============================================================================
PRESETS = {
    # small: seconds to generate, perfect while you build and debug the pipeline
    "small": dict(vendors=100, customers=5000, orders=15000, days=30),
    # full: ~1.3M rows across all files (the dataset to model and analyse)
    "full": dict(vendors=320, customers=42000, orders=190000, days=30),
}


@dataclass
class Config:
    out_dir: str
    seed: int
    start: float          # window start, epoch seconds UTC
    days: int
    n_vendors: int
    n_customers: int
    n_orders: int
    dirty_rate: float     # base probability to corrupt an eligible field
    anomaly_rate: float   # multiplier for manager typos (scenario 2)

    @property
    def end(self) -> float:
        return self.start + self.days * 86400


# =============================================================================
# 3. REFERENCE DATA
# =============================================================================
CITIES = {
    "CAI": {"en": "Cairo", "weight": 0.55,
            "variants": ["cairo", "CAIRO", "القاهرة", "CAI", " Cairo ", "Cairo Governorate"],
            "zones": {"Maadi": (29.960, 31.257), "Nasr City": (30.056, 31.340),
                      "Heliopolis": (30.091, 31.322), "New Cairo": (30.030, 31.470),
                      "Zamalek": (30.061, 31.219), "Mokattam": (30.018, 31.290),
                      "Shubra": (30.106, 31.245), "Downtown": (30.047, 31.238)}},
    "GIZ": {"en": "Giza", "weight": 0.25,
            "variants": ["giza", "GIZA", "الجيزة", "GIZ", "El Giza"],
            "zones": {"Dokki": (30.038, 31.211), "Mohandessin": (30.056, 31.200),
                      "Sheikh Zayed": (30.044, 30.976), "6th of October": (29.972, 30.944),
                      "Haram": (29.990, 31.150)}},
    "ALX": {"en": "Alexandria", "weight": 0.20,
            "variants": ["alexandria", "ALEXANDRIA", "الإسكندرية", "ALX", "Alex"],
            "zones": {"Smouha": (31.215, 29.946), "Sidi Gaber": (31.219, 29.942),
                      "Miami": (31.268, 30.003), "Gleem": (31.241, 29.963),
                      "Stanley": (31.236, 29.949)}},
}
ZONE_VARIANTS = {
    "6th of October": ["6 October", "6th October", "October City", "6 أكتوبر"],
    "Nasr City": ["Nasr city", "Madinet Nasr", "مدينة نصر"],
    "New Cairo": ["new cairo", "Fifth Settlement", "التجمع الخامس"],
    "Heliopolis": ["Masr El Gedida", "مصر الجديدة"],
    "Sheikh Zayed": ["El Sheikh Zayed", "Zayed"],
    "Mohandessin": ["Mohandeseen", "المهندسين"],
    "Maadi": ["El Maadi", "المعادي"],
}

ALL_CATEGORIES = ["Dairy & Eggs", "Bakery", "Fruits & Vegetables", "Meat & Poultry", "Beverages",
                  "Snacks", "Pantry", "Household", "Personal Care", "Baby Care"]

# Integration type = HOW a vendor's changes reach the platform. Drives latency,
# price unit (legacy POS sends piasters) and error rate (manual portal = typos).
INTEGRATIONS = {
    "API": {"variants": ["api", "Direct API", "REST"], "anomaly_mult": 1.0},
    "POS_LEGACY": {"variants": ["POS", "legacy_pos", "POS (legacy)"], "anomaly_mult": 1.5},
    "MANUAL_PORTAL": {"variants": ["portal", "Manual", "Vendor Portal"], "anomaly_mult": 5.0},
    "WMS": {"variants": ["wms", "Internal WMS"], "anomaly_mult": 0.3},
}

VENDOR_TYPES = {
    "restaurant": {
        "weight": 0.45, "variants": ["Restaurant", "resto", "F&B", "food_vendor", "مطعم"],
        "commission": (18.0, 28.0), "integrations": {"API": 0.35, "POS_LEGACY": 0.25, "MANUAL_PORTAL": 0.40},
        "popularity": 1.6, "prep": (12, 30), "fees": [10, 15, 20, 25],
    },
    "dark_store": {
        "weight": 0.15, "variants": ["Dark Store", "darkstore", "DS", "MFC", "micro_fulfillment_center"],
        "commission": (0.0, 0.0), "integrations": {"WMS": 1.0},
        "popularity": 1.6, "prep": (5, 8), "fees": [15], "assortment": (0.75, 0.95),
        "categories": ALL_CATEGORIES,
    },
    "supermarket": {
        "weight": 0.30, "variants": ["Supermarket", "Super Market", "grocery", "Grocery Store", "hypermarket"],
        "commission": (10.0, 18.0), "integrations": {"API": 0.40, "POS_LEGACY": 0.45, "MANUAL_PORTAL": 0.15},
        "popularity": 0.8, "prep": (10, 20), "fees": [10, 15, 20], "assortment": (0.40, 0.70),
        "categories": ALL_CATEGORIES,
    },
    "pharmacy": {
        "weight": 0.10, "variants": ["Pharmacy", "PHARM", "drugstore", "صيدلية"],
        "commission": (8.0, 14.0), "integrations": {"API": 0.50, "POS_LEGACY": 0.40, "MANUAL_PORTAL": 0.10},
        "popularity": 0.4, "prep": (8, 15), "fees": [10, 15], "assortment": (0.60, 0.90),
        "categories": ["Personal Care", "Baby Care", "Beverages"],
    },
}
GROCERY_PREFIXES = {
    "dark_store": ["QuickBox", "Daily Dash", "Fresh Hub"],
    "supermarket": ["Al Rahma Market", "Nile Fresh", "Bab El Kheir", "Green Basket", "City Mart",
                    "El Salam Supermarket", "Family Market", "Baraka Market"],
    "pharmacy": ["Al Shefaa Pharmacy", "Dawaa Plus", "Care Pharmacy", "Nile Pharmacy"],
}

# Restaurant menus: (menu section, item name, (min, max) EGP, vegetarian, kcal)
CUISINES = {
    "Burgers": {"names": ["Smash Point", "Burger Lab", "Grill Brothers"],
                "variants": ["burger", "American", "Fast Food", "برجر"],
                "menu": [("Burgers", "Classic Beef Burger", (120, 180), False, 650),
                         ("Burgers", "Cheese Burger", (135, 195), False, 720),
                         ("Burgers", "Crispy Chicken Burger", (110, 165), False, 680),
                         ("Burgers", "Double Smash Burger", (180, 260), False, 950),
                         ("Sides", "French Fries", (40, 65), True, 380),
                         ("Sides", "Onion Rings", (45, 70), True, 420),
                         ("Drinks", "Soft Drink", (25, 35), True, 140),
                         ("Desserts", "Chocolate Milkshake", (70, 110), True, 520)]},
    "Pizza": {"names": ["Pizza Forno", "Slice House", "Napoli Corner"],
              "variants": ["Italian", "pizza", "بيتزا"],
              "menu": [("Pizza", "Margherita Pizza", (130, 200), True, 800),
                       ("Pizza", "Pepperoni Pizza", (160, 240), False, 950),
                       ("Pizza", "Chicken Ranch Pizza", (170, 250), False, 980),
                       ("Pizza", "Four Cheese Pizza", (170, 240), True, 1000),
                       ("Sides", "Garlic Bread", (45, 70), True, 350),
                       ("Sides", "Chicken Wings", (90, 140), False, 600),
                       ("Drinks", "Soft Drink", (25, 35), True, 140)]},
    "Egyptian": {"names": ["Koshary Masr", "Abu Ali Koshary", "Bait El Foul"],
                 "variants": ["Oriental", "egyptian", "Koshary", "مصري"],
                 "menu": [("Koshary", "Koshary Small", (35, 55), True, 550),
                          ("Koshary", "Koshary Large", (55, 85), True, 850),
                          ("Sandwiches", "Foul Sandwich", (12, 20), True, 300),
                          ("Sandwiches", "Taameya Sandwich", (12, 20), True, 320),
                          ("Mains", "Hawawshi", (70, 110), False, 750),
                          ("Mains", "Molokhia with Rice", (80, 120), True, 600),
                          ("Desserts", "Rice Pudding", (30, 45), True, 280),
                          ("Drinks", "Soft Drink", (25, 35), True, 140)]},
    "Shawarma": {"names": ["Shawarma Station", "Shawarma Corner", "Wrap & Roll"],
                 "variants": ["Syrian", "Lebanese", "شاورما"],
                 "menu": [("Sandwiches", "Chicken Shawarma Sandwich", (65, 95), False, 550),
                          ("Sandwiches", "Beef Shawarma Sandwich", (85, 120), False, 600),
                          ("Plates", "Chicken Shawarma Plate", (150, 210), False, 900),
                          ("Plates", "Shawarma Fattah", (160, 230), False, 1100),
                          ("Sides", "Garlic Sauce", (10, 20), True, 120),
                          ("Sides", "French Fries", (40, 65), True, 380),
                          ("Drinks", "Soft Drink", (25, 35), True, 140)]},
    "Grills": {"names": ["Fahm Grills", "Mashwiyat El Nil", "Kofta House"],
               "variants": ["BBQ", "Kebab", "مشويات"],
               "menu": [("Grills", "Half Grilled Chicken", (150, 210), False, 700),
                        ("Grills", "Kofta Plate", (200, 280), False, 850),
                        ("Grills", "Mixed Grill", (350, 480), False, 1300),
                        ("Sides", "Oriental Rice", (35, 50), True, 400),
                        ("Sides", "Tahini Salad", (25, 40), True, 200),
                        ("Drinks", "Soft Drink", (25, 35), True, 140)]},
    "Desserts": {"names": ["Sweet Spot", "Konafa Lab", "Om Ali House"],
                 "variants": ["Sweets", "Oriental Sweets", "حلويات"],
                 "menu": [("Desserts", "Konafa with Cream", (70, 110), True, 600),
                          ("Desserts", "Basbousa", (50, 80), True, 450),
                          ("Desserts", "Om Ali", (60, 95), True, 650),
                          ("Desserts", "Nutella Waffle", (80, 120), True, 700),
                          ("Desserts", "Ice Cream Scoop", (35, 50), True, 200),
                          ("Drinks", "Turkish Coffee", (30, 45), True, 10)]},
}
SECTION_VARIANTS = {
    "Burgers": ["burgers", "Sandwiches", "برجر"], "Sides": ["Add-ons", "Extras", "sides", "مقبلات"],
    "Drinks": ["Beverages", "drinks", "مشروبات"], "Desserts": ["Sweets", "desserts", "حلويات"],
    "Pizza": ["Pizzas", "pizza", "بيتزا"], "Koshary": ["koshary", "Main Dishes"],
    "Sandwiches": ["sandwiches", "Sandwich", "ساندوتشات"], "Mains": ["Main Dishes", "Main Course", "أطباق رئيسية"],
    "Plates": ["Meals", "plates", "وجبات"], "Grills": ["BBQ", "grills", "مشويات"],
}

CATEGORY_VARIANTS = {
    "Dairy & Eggs": ["Dairy and Eggs", "dairy & eggs", "DAIRY", "Dairy", "ألبان وبيض"],
    "Bakery": ["bakery", "Bread & Bakery", "مخبوزات"],
    "Fruits & Vegetables": ["Fruits and Vegetables", "Fresh Produce", "F&V", "fruits & vegetables", "خضار وفاكهة"],
    "Meat & Poultry": ["Meat and Poultry", "Butchery", "meat & poultry", "لحوم ودواجن"],
    "Beverages": ["Drinks", "beverages", "BEVERAGES", "مشروبات"],
    "Snacks": ["Snacks & Sweets", "snacks", "Chips & Snacks", "سناكس"],
    "Pantry": ["Food Cupboard", "Pantry Staples", "pantry", "بقالة"],
    "Household": ["Cleaning", "Home Care", "household", "منظفات"],
    "Personal Care": ["Beauty & Personal Care", "personal care", "Health & Beauty", "عناية شخصية"],
    "Baby Care": ["Baby", "Mother & Baby", "baby care", "مستلزمات أطفال"],
}
PERISHABLE = {"Dairy & Eggs", "Bakery", "Fruits & Vegetables", "Meat & Poultry"}
SHELF_LIFE = {"Dairy & Eggs": 14, "Bakery": 4, "Fruits & Vegetables": 7, "Meat & Poultry": 5,
              "Beverages": 270, "Snacks": 180, "Pantry": 540, "Household": 730,
              "Personal Care": 730, "Baby Care": 540}

# (category, item, brands, [(pack size, price factor)], (min, max) EGP for the first size)
PRODUCT_TEMPLATES = [
    ("Dairy & Eggs", "Full Cream Milk", ["Juhayna", "Almarai", "Lamar"], [("1 L", 1.0), ("500 ml", 0.55)], (38, 48)),
    ("Dairy & Eggs", "Greek Yogurt", ["Juhayna", "Danone", "Almarai"], [("170 g", 1.0)], (18, 28)),
    ("Dairy & Eggs", "White Cheese", ["Domty", "President", "Juhayna"], [("500 g", 1.0), ("250 g", 0.55)], (45, 70)),
    ("Dairy & Eggs", "Eggs", ["Farm Fresh", "Wadi Food", "El Watania"], [("30 pcs", 1.0), ("12 pcs", 0.42)], (150, 190)),
    ("Dairy & Eggs", "Butter", ["Lurpak", "President", "Lactoland"], [("200 g", 1.0)], (90, 160)),
    ("Bakery", "Baladi Bread", ["Bakery Fresh", "Fino House"], [("5 pcs", 1.0)], (10, 20)),
    ("Bakery", "Toast Bread", ["Rich Bake", "Fino House"], [("600 g", 1.0)], (35, 50)),
    ("Bakery", "Croissant", ["Molto", "Fino House"], [("6 pcs", 1.0)], (40, 70)),
    ("Fruits & Vegetables", "Tomatoes", ["Local Farm"], [("1 kg", 1.0), ("500 g", 0.52)], (15, 30)),
    ("Fruits & Vegetables", "Potatoes", ["Local Farm"], [("1 kg", 1.0)], (18, 28)),
    ("Fruits & Vegetables", "Bananas", ["Local Farm", "Dole"], [("1 kg", 1.0)], (30, 45)),
    ("Fruits & Vegetables", "Cucumbers", ["Local Farm"], [("1 kg", 1.0)], (20, 35)),
    ("Fruits & Vegetables", "Oranges", ["Local Farm"], [("1 kg", 1.0)], (20, 35)),
    ("Meat & Poultry", "Whole Chicken", ["Koki", "Wataniya", "Cairo Poultry"], [("1.2 kg", 1.0)], (150, 210)),
    ("Meat & Poultry", "Minced Beef", ["Halwani", "Butcher's Choice"], [("500 g", 1.0)], (220, 300)),
    ("Meat & Poultry", "Chicken Breast", ["Koki", "Wataniya"], [("1 kg", 1.0)], (240, 320)),
    ("Beverages", "Mineral Water", ["Nestle", "Baraka", "Aquafina"], [("1.5 L", 1.0), ("600 ml", 0.5), ("6 x 1.5 L", 5.5)], (8, 12)),
    ("Beverages", "Cola", ["Pepsi", "Coca-Cola"], [("1 L", 1.0), ("330 ml", 0.5)], (20, 30)),
    ("Beverages", "Orange Juice", ["Juhayna", "Beyti"], [("1 L", 1.0)], (45, 60)),
    ("Beverages", "Tea", ["Lipton", "El Arosa"], [("100 bags", 1.0), ("250 g", 0.9)], (90, 150)),
    ("Beverages", "Instant Coffee", ["Nescafe"], [("200 g", 1.0)], (250, 350)),
    ("Snacks", "Potato Chips", ["Chipsy", "Lays", "Doritos"], [("77 g", 1.0), ("150 g", 1.8)], (10, 15)),
    ("Snacks", "Chocolate Bar", ["Cadbury", "Galaxy", "Corona"], [("45 g", 1.0)], (15, 30)),
    ("Snacks", "Biscuits", ["Biskrem", "Todo"], [("6 pcs", 1.0)], (20, 35)),
    ("Pantry", "Rice", ["Al Doha", "Abu Kass"], [("1 kg", 1.0), ("5 kg", 4.7)], (30, 45)),
    ("Pantry", "Pasta", ["Regina", "El Maleka"], [("400 g", 1.0), ("1 kg", 2.3)], (14, 22)),
    ("Pantry", "Sunflower Oil", ["Crystal", "Afia"], [("1 L", 1.0), ("2.2 L", 2.1)], (80, 110)),
    ("Pantry", "Sugar", ["El Osra", "Al Doha"], [("1 kg", 1.0)], (30, 40)),
    ("Pantry", "Lentils", ["Al Doha"], [("500 g", 1.0)], (35, 50)),
    ("Household", "Dishwashing Liquid", ["Fairy", "Pril"], [("650 ml", 1.0)], (40, 60)),
    ("Household", "Laundry Detergent", ["Ariel", "Persil"], [("2.5 kg", 1.0)], (250, 350)),
    ("Household", "Tissues", ["Fine", "Zeina"], [("5 packs", 1.0)], (60, 90)),
    ("Personal Care", "Shampoo", ["Pantene", "Head & Shoulders", "Dove"], [("400 ml", 1.0), ("200 ml", 0.55)], (90, 160)),
    ("Personal Care", "Toothpaste", ["Signal", "Colgate"], [("100 ml", 1.0)], (40, 70)),
    ("Personal Care", "Soap", ["Dove", "Lux"], [("4 x 125 g", 1.0)], (60, 110)),
    ("Baby Care", "Diapers Size 4", ["Pampers", "Molfix", "BabyJoy"], [("54 pcs", 1.0)], (350, 550)),
    ("Baby Care", "Baby Wipes", ["Pampers", "Molfix"], [("64 pcs", 1.0)], (50, 90)),
    ("Baby Care", "Infant Formula", ["Bebelac", "Nan"], [("400 g", 1.0)], (250, 400)),
]

FIRST_M = ["Ahmed", "Mohamed", "Mahmoud", "Omar", "Youssef", "Mostafa", "Karim", "Hassan", "Ali", "Khaled",
           "Amr", "Tarek", "Ibrahim", "Hossam", "Ziad", "Seif", "Marwan", "Mina", "Bishoy", "Fady"]
FIRST_F = ["Fatma", "Mariam", "Nour", "Salma", "Aya", "Sara", "Hana", "Yasmin", "Dina", "Mona",
           "Rana", "Habiba", "Farida", "Malak", "Menna", "Reem", "Heba", "Marina", "Mirna", "Christine"]
LAST = ["Hassan", "Ibrahim", "Mahmoud", "El Sayed", "Abdelrahman", "Mostafa", "Fawzy", "Saleh", "Naguib",
        "Girgis", "Hanna", "Shawky", "Ramadan", "Salem", "Farouk", "Kamal", "Lotfy", "Zaki", "Hegazy", "Soliman"]
EMAIL_DOMAINS = ["gmail.com", "yahoo.com", "hotmail.com", "outlook.com"]

# canonical value -> other spellings / synonyms
ORDER_STATUS = {"delivered": ["DELIVERED", "Delivered", "completed", "fulfilled", "done"],
                "cancelled": ["canceled", "CANCELLED", "cancelled_by_customer", "rejected", "void"],
                "refunded": ["returned", "REFUNDED", "refund"],
                "failed": ["delivery_failed", "FAILED", "undelivered"]}
CANCEL_REASONS = {"item_unavailable": ["out_of_stock", "OOS", "Item Unavailable", "item not available"],
                  "customer_cancelled": ["cancelled_by_customer", "Customer Request", "customer"],
                  "vendor_busy": ["restaurant_busy", "store_busy", "Vendor Busy"],
                  "payment_failed": ["payment_declined", "Payment Failed"],
                  "rider_unavailable": ["no_rider", "Rider Unavailable"],
                  "customer_unreachable": ["customer_no_show", "Customer Unreachable"],
                  "missing_item": ["missing_items", "Missing Item"],
                  "quality_issue": ["bad_quality", "Quality Issue", "food_quality"]}
PAYMENT = {"cash": ["Cash", "COD", "cash_on_delivery", "Cash on Delivery", "كاش"],
           "card": ["credit_card", "VISA", "Credit Card", "online_card"],
           "wallet": ["e-wallet", "Mobile Wallet", "vodafone_cash"]}
PLATFORM = {"ios": ["iOS", "iPhone", "IOS", "apple"], "android": ["Android", "ANDROID", "android_app"],
            "web": ["Web", "website", "desktop", "WEB"]}
VENDOR_STATUS = {"active": ["Active", "ACTIVE", "1", "Y", "enabled", "live"],
                 "inactive": ["Inactive", "0", "N", "disabled", "paused"]}
BOOL_TEXT = {True: ["TRUE", "True", "yes", "Y", "1", 1], False: ["FALSE", "False", "no", "N", "0", 0]}
AVAIL_VARIANTS = {True: ["Y", 1, "IN_STOCK", "available", "yes"], False: ["N", 0, "OOS", "sold_out", "no"]}
GENDER = {"M": ["Male", "male", "m", "ذكر"], "F": ["Female", "female", "f", "أنثى"]}
ITEM_TYPE_VARIANTS = {"grocery": ["Grocery", "GROCERY", "retail_item"], "menu": ["MENU", "menu_item", "food"]}
MOVEMENT_TYPES = {"OPENING_BALANCE": ["opening", "OB", "initial_stock"],
                  "RESTOCK": ["Restock", "GRN", "INBOUND", "goods_received"],
                  "SALE": ["sale", "OUTBOUND", "PICK", "order_pick"],
                  "WASTE": ["write_off", "EXPIRED", "damage", "Waste"],
                  "ADJUSTMENT": ["adj", "cycle_count", "Stock Count"]}
PROMO_CODES = {None: 0, "FRESH10": 10, "SAVE15": 15, "WEEKEND20": 20}

# local-hour demand curve (Cairo time, 0..23)
# Egyptian demand by hour (Cairo local time, index = hour 0..23). These are
# ASSUMPTIONS (no public hourly data was available), modelled on common habits:
#   restaurants: late lunch peak 3-4 pm, dinner peak 9-10 pm, orders until ~2 am
#   groceries  : morning peak 9-10 am (breakfast items), second peak 6-8 pm
#   pharmacies : spread over the day, slightly higher in the evening
HOUR_CURVES = {
    "restaurant":  [5, 3, 1.5, 0.6, 0.3, 0.3, 0.5, 1.5, 2.5, 2.5, 2, 2.5, 3.5, 5, 6.5, 8, 8, 6.5, 5.5, 6, 7.5, 8.5, 8, 6.5],
    "dark_store":  [1.2, 0.6, 0.3, 0.2, 0.2, 0.4, 1.5, 4, 6.5, 7.5, 7, 6, 5, 4.5, 4, 4, 4.5, 5, 6, 6.5, 6, 4.5, 3, 2],
    "supermarket": [1.2, 0.6, 0.3, 0.2, 0.2, 0.4, 1.5, 4, 6.5, 7.5, 7, 6, 5, 4.5, 4, 4, 4.5, 5, 6, 6.5, 6, 4.5, 3, 2],
    "pharmacy":    [2, 1.5, 1, 0.6, 0.4, 0.4, 0.6, 1.5, 2.5, 3, 3, 3, 3, 3, 3, 3.5, 3.5, 4, 4.5, 5, 5.5, 5, 4, 3],
}
# Busy hours (Cairo local) per vendor type = hours whose demand is >= 75% of that type's peak.
# During busy hours riders are scarcer, so some orders pick up an extra delay (see on_ORDER).
PEAK_HOURS = {vt: {h for h, w in enumerate(curve) if w >= 0.75 * max(curve)} for vt, curve in HOUR_CURVES.items()}
# Rider travel: fixed handling time + minutes per km between the vendor and the customer's zone.
TRAVEL_BASE_MIN, TRAVEL_MIN_PER_KM = 6.0, 2.5
# How far each vendor type delivers (km, straight line from the vendor to the centre of the zone).
DELIVERY_RADIUS_KM = {"restaurant": 9.0, "dark_store": 6.0, "supermarket": 8.0, "pharmacy": 8.0}
# Cap for the per-customer ordering-frequency weight (Pareto tail). v3.0 had no cap and produced one
# customer with 5,136 orders in a month; with this cap the heaviest users order ~1-2 times a day.
CUSTOMER_WEIGHT_CAP = 20.0
# Relative demand per weekday (Python weekday(): Monday=0 ... Sunday=6). Egypt: the weekend is
# Friday + Saturday, Thursday evening starts it -> Thursday / Friday are the busiest days.
WEEKDAY_DEMAND = {0: 0.92, 1: 0.92, 2: 0.95, 3: 1.10, 4: 1.15, 5: 1.05, 6: 0.95}
# Minimum distance between two products' barcodes. Excel shows a 13-digit barcode with 6 significant
# digits (6.22528E+12 = +/- 5,000,000); a gap of 20,000,000 keeps every such value unambiguous.
BARCODE_MIN_GAP = 20_000_000
# A vendor's regular selling price stays within this band of the master-data list price.
PRICE_BAND = (0.85, 1.30)
EGYPT_WEEKEND = {4, 5}           # Python weekday(): Friday=4, Saturday=5
NULL_TOKENS = {"", "n/a", "null", "none", "nan", "-"}


# =============================================================================
# 4. FILE SCHEMAS (single source of truth: writers, validator and documentation)
#    column -> (logical type, nullable by design, description)
# =============================================================================
SCHEMAS = {
    "partners_system/vendors.csv": {
        "system": "Partners / onboarding system (CRM)", "grain": "one row per vendor (store / restaurant)",
        "key": "VendorID",
        "columns": [
            ("VendorID", "string", False, "Vendor id, format V0012. Same vendor is store_id=12 in CDC and WH-0012 in the WMS."),
            ("Vendor_Name", "string", False, "Display name of the store / restaurant: '<brand> - <zone>' (dark stores: '<brand> <zone> DS-<n>'). Unique."),
            ("Vendor_Type", "string", False, "restaurant | dark_store | supermarket | pharmacy (many synonyms in the data)."),
            ("Cuisine", "string", True, "Restaurants only (Burgers, Pizza, Egyptian, Shawarma, Grills, Desserts). Empty for other types by design."),
            ("Integration_Type", "string", False, "How the vendor's changes reach the platform: API | POS_LEGACY | MANUAL_PORTAL | WMS. POS_LEGACY sends CDC prices in PIASTERS."),
            ("City", "string", False, "Cairo | Giza | Alexandria."),
            ("Area", "string", False, "Zone / district of the vendor (called delivery_zone in orders)."),
            ("Commission_Pct", "decimal", False, "Platform commission in percent (15 = 15%). 0 for platform-owned dark stores."),
            ("Avg_Prep_Time_Min", "integer", False, "Average preparation / picking time in minutes."),
            ("Rating", "decimal", False, "Average customer rating on a 1-5 scale."),
            ("Onboarded_On", "date", False, "Date the vendor joined the platform, dd/mm/yyyy."),
            ("Status", "string", False, "active | inactive. Inactive vendors list nothing and receive no orders."),
        ]},
    "catalog_pim/products.csv": {
        "system": "Catalog / PIM (product information management)", "grain": "one row per grocery product",
        "key": "product_id",
        "columns": [
            ("product_id", "string", False, "Catalog id, format P00045. The same product is SKU-00045 in CDC and in order items."),
            ("barcode", "string", False, "EAN-13 barcode (Egypt prefix 622). The product key used by the WMS. Two different products are always >= 20,000,000 apart, so a barcode shown by Excel as 6.22528E+12 still identifies one product."),
            ("product_name", "string", False, "Brand + item + pack size."),
            ("brand", "string", False, "Brand name."),
            ("category", "string", False, "One of 10 categories (many labels in the data)."),
            ("pack_size", "string", False, "Pack size with unit, e.g. 1 L, 500 g, 30 pcs."),
            ("base_price_egp", "decimal", False, "Recommended list price in EGP (not the selling price of a store)."),
            ("is_perishable", "boolean", False, "True for dairy, bakery, fresh produce, meat (these get expiry dates and waste)."),
            ("is_active", "boolean", False, "Inactive products are not listed by any store."),
        ]},
    "restaurant_portal/menu_items.csv": {
        "system": "Restaurant portal (menus typed by restaurant managers)", "grain": "one row per menu item of a restaurant",
        "key": "menu_item_id",
        "columns": [
            ("menu_item_id", "string", False, "Menu item id, format M0000123. Same value is item_ref in CDC and order items."),
            ("vendor_id", "string", False, "Restaurant, format V0012."),
            ("item_name", "string", False, "Meal name as typed by the restaurant."),
            ("menu_section", "string", False, "Section of the menu (Burgers, Sides, Drinks ...)."),
            ("price_egp", "decimal", False, "Menu price in EGP when the item was created (current price lives in CDC)."),
            ("is_vegetarian", "boolean", False, "Vegetarian flag."),
        ]},
    "ops_db_cdc/vendor_items_events.csv": {
        "system": "Operational PostgreSQL table vendor_items, changes captured by CDC (Debezium-style)",
        "grain": "one row per change of one item in one store", "key": "lsn",
        "columns": [
            ("lsn", "long", False, "Log sequence number: unique id of the change, increasing in commit order. Use it to order and de-duplicate."),
            ("op", "string", False, "r = initial snapshot, c = insert, u = update, d = delete (on d the value columns are empty)."),
            ("captured_at_ms", "long", False, "When CDC captured the change, epoch milliseconds UTC."),
            ("store_id", "integer", False, "Vendor number (12 = V0012)."),
            ("item_ref", "string", False, "SKU-00045 for groceries, M0000123 for menu items."),
            ("item_name", "string", False, "Display name. Empty here is a manager typo -> reject (DLQ)."),
            ("item_price", "decimal", False, "Selling price after the change. EGP, except POS_LEGACY vendors: piasters (currency PT)."),
            ("currency", "string", False, "EGP or PT (piasters)."),
            ("qty_on_hand", "integer", True, "Units in stock for groceries; empty for menu items (restaurants do not count stock)."),
            ("is_available", "boolean", False, "Shown as available in the app."),
            ("updated_by", "string", False, "Who made the change: mgr_0012 (manager), partner_api, pos_sync, wms_sync, catalog_team."),
            ("updated_at", "timestamp", False, "Business time of the change, LOCAL Cairo time WITHOUT timezone."),
        ]},
    "wms/inventory_movements.csv": {
        "system": "Warehouse Management System of the dark stores", "grain": "one row per stock movement",
        "key": "movement_id",
        "columns": [
            ("movement_id", "string", False, "Movement id, format MV-00000001."),
            ("warehouse_code", "string", False, "Dark store, format WH-0012 (= VendorID V0012)."),
            ("product_barcode", "string", False, "EAN-13 barcode, joins to products.barcode."),
            ("movement_type", "string", False, "OPENING_BALANCE | RESTOCK | SALE | WASTE | ADJUSTMENT."),
            ("quantity", "integer", False, "SIGNED quantity: + into stock, - out of stock."),
            ("unit_cost_egp", "decimal", True, "Purchase cost per unit; only for RESTOCK and OPENING_BALANCE."),
            ("expiry_date", "date", True, "Batch expiry date (YYYY-MM-DD); only for inbound movements of perishable products."),
            ("reference_id", "string", True, "order_id for SALE, purchase order PO-xxxxxx for RESTOCK."),
            ("movement_ts", "timestamp", False, "Local Cairo time, dd/mm/yyyy HH:MM:SS."),
        ]},
    "customer_app/customers.csv": {
        "system": "Customer app backend", "grain": "one row per customer account", "key": "customer_id",
        "columns": [
            ("customer_id", "string", False, "Account id, format CUS-000001. One person can own two accounts (near-duplicates)."),
            ("full_name", "string", False, "Customer name."),
            ("phone", "string", False, "Mobile, canonical E.164 (+201XXXXXXXXX)."),
            ("email", "string", False, "Email address."),
            ("city", "string", False, "Home city."),
            ("signup_ts", "timestamp", False, "Account creation time, ISO-8601 UTC."),
            ("is_pro_subscriber", "boolean", False, "Paid subscription: free delivery."),
        ]},
    "customer_app/orders.csv": {
        "system": "Customer app backend", "grain": "one row per order", "key": "order_id",
        "columns": [
            ("order_id", "string", False, "Order id, format ORD-00000001, increasing with order time."),
            ("customer_id", "string", False, "FK to customers."),
            ("vendor_id", "string", False, "FK to vendors (V0012)."),
            ("order_ts", "timestamp", False, "Order placed, ISO-8601 UTC."),
            ("delivered_at", "timestamp", True, "Delivery time; empty when the order was not delivered."),
            ("status", "string", False, "delivered | cancelled | refunded | failed."),
            ("cancel_reason", "string", True, "Reason for cancelled / failed / refunded orders. item_unavailable = the app showed a sold-out item as available."),
            ("payment_method", "string", False, "cash | card | wallet."),
            ("promo_code", "string", True, "Promo code used, if any."),
            ("delivery_zone", "string", False, "Zone the order was delivered to = the customer's home zone (always the same for one customer, in the vendor's city, within the vendor's delivery range)."),
            ("subtotal", "decimal", False, "Sum of line_total, EGP."),
            ("discount_amount", "decimal", False, "Promo discount in EGP (capped at 100)."),
            ("delivery_fee", "decimal", False, "EGP; 0 for pro subscribers."),
            ("service_fee", "decimal", False, "3% of subtotal, min 2, max 15 EGP."),
            ("total_amount", "decimal", False, "subtotal - discount_amount + delivery_fee + service_fee."),
            ("estimated_delivery_min", "integer", False, "ETA shown to the customer in minutes."),
            ("customer_rating", "integer", True, "1-5 stars, only some delivered orders are rated."),
        ]},
    "customer_app/order_items.csv": {
        "system": "Customer app backend", "grain": "one row per order line", "key": "order_id + line_number",
        "columns": [
            ("order_id", "string", False, "FK to orders."),
            ("line_number", "integer", False, "1..n inside the order."),
            ("item_ref", "string", False, "SKU-00045 (grocery) or M0000123 (menu item)."),
            ("item_name", "string", False, "Name shown in the app at order time."),
            ("quantity", "integer", False, "Units ordered."),
            ("unit_price", "decimal", False, "Price SHOWN IN THE APP at order time (may be stale vs the real price)."),
            ("line_total", "decimal", False, "quantity x unit_price."),
            ("vendor_confirmed_price", "decimal", False, "Price the vendor really charged. If it differs from unit_price the app showed a STALE price."),
        ]},
}
SCENARIO_COLUMNS = ["lsn", "vendor_id", "item_ref", "op", "scenario", "expected_gate_result",
                    "reject_reason", "event_time_utc", "delivered_late"]
SCD_COLUMNS = ["vendor_id", "item_ref", "item_type", "version", "price_egp", "is_available",
               "valid_from_utc", "valid_to_utc", "is_current"]
MANIFEST_COLUMNS = ["issue_id", "source_file", "record_key", "column", "issue_category",
                    "issue_detail", "clean_value", "dirty_value"]


def cols(path):
    """Column names of a CSV file from SCHEMAS."""
    return [c[0] for c in SCHEMAS[path]["columns"]]


# =============================================================================
# 5. TIME HELPERS (all internal times are float epoch seconds in UTC)
# =============================================================================
def last_weekday_of_month(year, month, weekday):
    first_next = date(year + 1, 1, 1) if month == 12 else date(year, month + 1, 1)
    d = first_next - timedelta(days=1)
    while d.weekday() != weekday:
        d -= timedelta(days=1)
    return d


@lru_cache(maxsize=None)
def cairo_offset_hours(d: date) -> int:
    """Egypt DST (since 2023): UTC+3 from the last Friday of April to the last
    Thursday of October, else UTC+2 (approximated at day level)."""
    return 3 if last_weekday_of_month(d.year, 4, 4) <= d <= last_weekday_of_month(d.year, 10, 3) else 2


TZ = {2: timezone(timedelta(hours=2)), 3: timezone(timedelta(hours=3))}


def to_local(ts: float) -> datetime:
    dt = datetime.fromtimestamp(ts, UTC)
    return dt.astimezone(TZ[cairo_offset_hours(dt.date())])


def local_to_ts(y, m, d, h=0, mi=0, s=0) -> float:
    """Cairo local wall-clock time -> epoch seconds UTC."""
    naive = datetime(y, m, d) + timedelta(hours=h, minutes=mi, seconds=s)
    return (naive - timedelta(hours=cairo_offset_hours(naive.date()))).replace(tzinfo=UTC).timestamp()


def fmt_ts(ts: float, style: str):
    """One instant rendered the way different systems write timestamps."""
    if style == "epoch_ms":
        return int(ts * 1000)
    if style == "iso_utc":
        return datetime.fromtimestamp(ts, UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
    loc = to_local(ts)
    if style == "iso_local_tz":
        return loc.isoformat(timespec="seconds")
    if style == "local_naive":
        return loc.strftime("%Y-%m-%d %H:%M:%S")
    if style == "dmy_local":
        return loc.strftime("%d/%m/%Y %H:%M")
    if style == "dmy_local_s":
        return loc.strftime("%d/%m/%Y %H:%M:%S")
    if style == "mdy_local_12h":
        return loc.strftime("%m/%d/%Y %I:%M %p")
    if style == "text_local":
        return loc.strftime("%d-%b-%Y %H:%M")
    raise ValueError(style)


def fmt_date(d: date, style: str) -> str:
    return d.strftime({"iso": "%Y-%m-%d", "dmy": "%d/%m/%Y", "mdy": "%m/%d/%Y", "text": "%b %d, %Y",
                       "compact": "%Y%m%d", "dmy_dash": "%d-%m-%Y"}[style])


def local_date(ts: float) -> date:
    return to_local(ts).date()


# =============================================================================
# 6. VALUE HELPERS
# =============================================================================
def quarter(x):
    """Grocery prices end in .00/.25/.50/.75."""
    return round(round(x * 4) / 4, 2)


def round5(x):
    """Restaurant prices are multiples of 5 EGP."""
    return float(max(5, int(round(x / 5.0)) * 5))


def _n(x):
    return f"{x:g}"


def pack_size_variants(size):
    """Same pack size written with another unit / spelling (UNIT_FORMAT)."""
    m = re.fullmatch(r"(\d+(?:\.\d+)?) (L|ml|kg|g|pcs|bags|packs)", size)
    if not m:
        return [size.replace(" ", ""), size.replace(" x ", "x"), size.replace(" x ", "*")]
    n, u = float(m.group(1)), m.group(2)
    out = [f"{_n(n)}{u}"]
    out += {"L": [f"{_n(n * 1000)} ml", f"{_n(n)} Liter"], "ml": [f"{_n(n / 1000)} L", f"{_n(n)} ML"],
            "kg": [f"{_n(n * 1000)} g", f"{_n(n)} KG"], "g": [f"{_n(n / 1000)} kg", f"{_n(n)} gm"],
            "pcs": [f"{_n(n)} pieces", f"{_n(n)} PCS"]}.get(u, [])
    return out


def ean13(body12: str) -> str:
    """Append the EAN-13 check digit to a 12-digit body."""
    s = sum(int(c) * (3 if i % 2 else 1) for i, c in enumerate(body12))
    return body12 + str((10 - s % 10) % 10)


def random_phone(rng):
    return "+20" + rng.choice(["10", "11", "12", "15"]) + f"{rng.randint(0, 99_999_999):08d}"


def phone_variants(e164):
    nat = e164[3:]
    loc = "0" + nat
    return [loc, "0020" + nat, f"+20 {nat[:3]} {nat[3:6]} {nat[6:]}", nat, f"{loc[:4]}-{loc[4:7]}-{loc[7:]}", "20" + nat]


def classify_variant(canonical, variant):
    """Same word written differently (REPR) or a different business word (TERM)?"""
    def norm(s):
        return str(s).strip().lower().replace("_", " ").replace("-", " ")
    return REPR if norm(canonical) == norm(variant) else TERM


def is_null(v):
    return v is None or (isinstance(v, str) and v.strip().lower() in NULL_TOKENS)


def parse_number(v):
    """Lenient numeric parser used by the DQ gate: accepts '44.75 EGP', '44,75', '12.0'."""
    if v is None or isinstance(v, bool):
        return None
    if isinstance(v, (int, float)):
        return float(v)
    s = str(v).strip()
    if s.lower() in NULL_TOKENS:
        return None
    s = re.sub(r"[^0-9,.\-]", "", s)
    if "," in s and "." not in s:
        s = s.replace(",", ".")
    else:
        s = s.replace(",", "")
    try:
        return float(s)
    except ValueError:
        return None


TRUE_SET = {"true", "1", "y", "yes", "in_stock", "available"}
FALSE_SET = {"false", "0", "n", "no", "out_of_stock", "oos", "out-of-stock", "unavailable", "sold_out"}


def parse_bool(v):
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        return bool(v) if v in (0, 1) else None
    if v is None:
        return None
    s = str(v).strip().lower()
    return True if s in TRUE_SET else False if s in FALSE_SET else None


def poisson_times(rng, rate_per_day, t0, t1):
    """Random event instants of a Poisson process between t0 and t1."""
    out = []
    if rate_per_day <= 0 or t1 <= t0:
        return out
    lam = rate_per_day / 86400.0
    t = t0
    while True:
        t += rng.expovariate(lam)
        if t >= t1:
            return out
        out.append(t)


def window_times(rng, rate_per_day, t0, t1, hour_ok):
    """Poisson instants restricted to the local hours where hour_ok(hour) is True. The rate is scaled
    up so the number of events per day stays `rate_per_day` (only their time of day changes)."""
    ok_hours = sum(1 for h in range(24) if hour_ok(h))
    if ok_hours == 0:
        return []
    return [t for t in poisson_times(rng, rate_per_day * 24.0 / ok_hours, t0, t1) if hour_ok(to_local(t).hour)]


def dist_km(lat1, lng1, lat2, lng2):
    """Great-circle distance in km (haversine)."""
    r = math.pi / 180
    a = (math.sin((lat2 - lat1) * r / 2) ** 2
         + math.cos(lat1 * r) * math.cos(lat2 * r) * math.sin((lng2 - lng1) * r / 2) ** 2)
    return 2 * 6371.0 * math.asin(math.sqrt(a))


def cell(v):
    """How a Python value is written into a CSV cell (None -> empty, booleans -> true/false)."""
    if v is None:
        return ""
    if isinstance(v, bool):
        return "true" if v else "false"
    return str(v)


# =============================================================================
# 7. DIRT INJECTOR (noise + streaming manifest)
# =============================================================================
@lru_cache(maxsize=20000)
def _variant_options(values, detail, category, canonical):
    return [(category or classify_variant(canonical, x), detail, (lambda _v, x=x: x)) for x in values]


class DirtInjector:
    MISSING_TOKENS = ["", "N/A", "null", "NULL", "-", "None", "nan"]

    def __init__(self, rng, rate, manifest_path):
        self.rng, self.rate = rng, rate
        os.makedirs(os.path.dirname(manifest_path), exist_ok=True)
        self._f = open(manifest_path, "w", newline="", encoding="utf-8")
        self._w = csv.writer(self._f, lineterminator="\n")
        self._w.writerow(MANIFEST_COLUMNS)
        self.count = 0
        self.by_cat, self.by_file = Counter(), Counter()
        self._touched = set()     # columns already corrupted in the current row
        self.row_mult = 1.0

    def begin_row(self, base_mult=1.0):
        """Start a new record. A few % of rows become 'garbage rows' with a much
        higher error probability, so several columns break in the same row."""
        self._touched = set()
        garbage = self.rate > 0 and self.rng.random() < self.rate * 0.8
        self.row_mult = base_mult * (GARBAGE_MULT if garbage else 1.0)

    def roll(self, weight=1.0):
        return self.rate > 0 and self.rng.random() < min(0.95, self.rate * weight * self.row_mult)

    def log(self, source, key, column, category, detail, clean, dirty):
        self._touched.add(column)
        self.count += 1
        self.by_cat[category] += 1
        self.by_file[source.split("#")[0]] += 1
        self._w.writerow([self.count, source, key, column, category, detail,
                          json.dumps(clean, ensure_ascii=False, default=str),
                          json.dumps(dirty, ensure_ascii=False, default=str)])

    def miss(self):
        return (MISSING, "value missing (null / empty / placeholder token)",
                lambda _v: self.rng.choice([None] + self.MISSING_TOKENS))

    @staticmethod
    def variants(values, detail, category=None, canonical=None):
        """Options that replace a value by an alternative spelling (cached: the same
        lists are requested millions of times at the 1M scale)."""
        return _variant_options(tuple(values), detail, category, canonical)

    def corrupt(self, row, col, source, key, options, weight=1.0, force=False):
        """Maybe replace row[col] by ONE randomly chosen option and log it.
        force=True bypasses the probability (used for systematic bugs)."""
        if not options or col in self._touched or self.rate <= 0:
            return False
        if not force and not self.roll(weight):
            return False
        category, detail, fn = self.rng.choice(options)
        clean = row.get(col)
        dirty = fn(clean)
        if dirty == clean and type(dirty) is type(clean):
            return False
        row[col] = dirty
        self.log(source, key, col, category, detail, clean, dirty)
        return True

    def close(self):
        self._f.close()


# =============================================================================
# 8. STREAMING SINKS
# =============================================================================
class CsvSink:
    """CSV writer that also re-emits random rows later in the file (duplicates,
    as after a double export / double posting), logging each in the manifest."""

    def __init__(self, path, columns, dirt, source, key_fn, dup_weight=0.0, mutate=None):
        os.makedirs(os.path.dirname(path), exist_ok=True)
        self.f = open(path, "w", newline="", encoding="utf-8")
        self.w = csv.writer(self.f, lineterminator="\n")
        self.w.writerow(columns)
        self.columns, self.dirt, self.source = columns, dirt, source
        self.key_fn, self.dup_weight, self.mutate = key_fn, dup_weight, mutate
        self.pending, self.n, self.seq = [], 0, 0

    def _raw(self, row):
        self.w.writerow([cell(row.get(c)) for c in self.columns])
        self.n += 1

    def write(self, row):
        self._raw(row)
        while self.pending and self.pending[0][0] <= self.n:
            self._raw(heapq.heappop(self.pending)[2])
        d = self.dirt
        if self.dup_weight > 0 and d.rate > 0 and d.rng.random() < d.rate * self.dup_weight:
            dup = dict(row)
            detail = "exact duplicate row"
            if self.mutate:
                detail = self.mutate(dup) or detail
            self.seq += 1
            heapq.heappush(self.pending, (self.n + d.rng.randint(1, 3000), self.seq, dup))
            d.log(self.source, self.key_fn(row), "*row", DUPLICATE, detail, None,
                  {c: dup.get(c) for c in self.columns})

    def close(self):
        while self.pending:
            self._raw(heapq.heappop(self.pending)[2])
        self.f.close()


class CdcSink:
    """CSV writer that emits CDC rows in DELIVERY order (late arrivals and
    re-deliveries come after newer events), using a small re-order buffer."""
    COLUMNS = ['lsn', 'op', 'captured_at_ms', 'store_id', 'item_ref', 'item_name', 'item_price', 'currency', 'qty_on_hand', 'is_available', 'updated_by', 'updated_at']

    def __init__(self, path):
        os.makedirs(os.path.dirname(path), exist_ok=True)
        self.f = open(path, "w", newline="", encoding="utf-8")
        self.w = csv.writer(self.f, lineterminator="\n")
        self.w.writerow(self.COLUMNS)
        self.heap, self.seq, self.n = [], 0, 0
        self.ops = Counter()

    def push(self, deliver_ts, row):
        self.seq += 1
        heapq.heappush(self.heap, (deliver_ts, self.seq, row))

    def flush_until(self, ts):
        while self.heap and self.heap[0][0] < ts:
            row = heapq.heappop(self.heap)[2]
            self.w.writerow([cell(row.get(c)) for c in self.COLUMNS])
            self.n += 1
            self.ops[row["op"]] += 1

    def close(self):
        self.flush_until(float("inf"))
        self.f.close()


# =============================================================================
# 9. MASTER DATA
# =============================================================================
def weighted(rng, mapping):
    keys = list(mapping)
    return rng.choices(keys, weights=[mapping[k] if isinstance(mapping[k], (int, float)) else mapping[k]["weight"] for k in keys])[0]


def generate_vendors(cfg, rng, dirt):
    SRC = "partners_system/vendors.csv"
    vendors, used_names = [], set()
    for i in range(1, cfg.n_vendors + 1):
        vid = f"V{i:04d}"
        city = weighted(rng, CITIES)
        vtype = weighted(rng, VENDOR_TYPES)
        spec = VENDOR_TYPES[vtype]
        integ = weighted(rng, spec["integrations"])
        cuisine = rng.choice(list(CUISINES)) if vtype == "restaurant" else None
        # A display name is "<brand> - <zone>" and must be unique: a real platform never lists two
        # "Burger Lab - Miami". Draw brand + zone again (same city) until the name is free.
        for _attempt in range(500):
            zone = rng.choice(list(CITIES[city]["zones"]))
            if vtype == "restaurant":
                name = f"{rng.choice(CUISINES[cuisine]['names'])} - {zone}"
            elif vtype == "dark_store":
                name = f"{rng.choice(GROCERY_PREFIXES[vtype])} {zone} DS-{i:02d}"
            else:
                name = f"{rng.choice(GROCERY_PREFIXES[vtype])} - {zone}"
            if name not in used_names:
                break
        else:
            raise RuntimeError(f"no free vendor name left for {vtype} in {city}")
        used_names.add(name)
        la, lo = CITIES[city]["zones"][zone]
        # opening hours (close_h may be > 24 = after midnight)
        if vtype == "dark_store":
            open_h, close_h = 0, 24
        elif vtype == "restaurant":
            # Egyptian (foul / taameya) places open early for breakfast
            open_h = rng.randint(7, 8) if cuisine == "Egyptian" else rng.randint(9, 12)
            close_h = rng.randint(23, 26)
        elif vtype == "supermarket":
            open_h, close_h = 8, 24
        else:
            open_h, close_h = (0, 24) if rng.random() < 0.3 else (9, 26)
        lo_c, hi_c = spec["commission"]
        v = {
            "vendor_id": vid, "num": i, "name": name, "type": vtype, "cuisine": cuisine,
            "integration": integ, "city": city, "zone": zone,
            "lat": round(la + rng.uniform(-0.012, 0.012), 6), "lng": round(lo + rng.uniform(-0.012, 0.012), 6),
            "commission": round(rng.uniform(lo_c, hi_c), 1) if hi_c > 0 else 0.0,
            "open_h": open_h, "close_h": close_h, "prep": rng.randint(*spec["prep"]),
            "rating": round(rng.uniform(3.4, 4.9), 1),
            "onboarded": local_date(cfg.start) - timedelta(days=rng.randint(30, 900)),
            "status": "active" if rng.random() > 0.05 else "inactive",
            "fee": rng.choice(spec["fees"]), "phone": random_phone(rng),
            # sloppy vendors type data by hand and make many more mistakes
            "sloppy": (integ == "MANUAL_PORTAL" and rng.random() < 0.5) or (vtype != "dark_store" and rng.random() < 0.05),
        }
        vendors.append(v)

    def t_open(h):
        return "23:59" if h == 24 else f"{h % 24:02d}:00"

    def conflicting(dup):
        if random.Random(dup["VendorID"]).random() < 0.4:
            dup["Status"] = "inactive" if str(dup["Status"]).lower() in ("active", "1", "y", "enabled", "live") else "active"
            return "duplicate VendorID with conflicting Status"
        return None

    sink = CsvSink(f"{cfg.out_dir}/{SRC}", cols(SRC), dirt, SRC, lambda r: r["VendorID"], dup_weight=0.7, mutate=conflicting)
    for v in vendors:
        dirt.begin_row(SLOPPY_MULT if v["sloppy"] else 1.0)
        row = {"VendorID": v["vendor_id"], "Vendor_Name": v["name"], "Vendor_Type": v["type"],
               "Cuisine": v["cuisine"], "Integration_Type": v["integration"], "City": CITIES[v["city"]]["en"],
               "Area": v["zone"], "Commission_Pct": v["commission"], "Avg_Prep_Time_Min": v["prep"],
               "Rating": v["rating"], "Onboarded_On": fmt_date(v["onboarded"], "dmy"), "Status": v["status"]}
        k = v["vendor_id"]
        dirt.corrupt(row, "City", SRC, k, dirt.variants(CITIES[v["city"]]["variants"], "city written differently", REPR), weight=3)
        dirt.corrupt(row, "Vendor_Type", SRC, k, dirt.variants(VENDOR_TYPES[v["type"]]["variants"], "vendor type label differs", canonical=v["type"]), weight=3)
        if v["cuisine"]:
            dirt.corrupt(row, "Cuisine", SRC, k, dirt.variants(CUISINES[v["cuisine"]]["variants"], "cuisine label differs", canonical=v["cuisine"]) + [dirt.miss()], weight=2)
        dirt.corrupt(row, "Integration_Type", SRC, k, dirt.variants(INTEGRATIONS[v["integration"]]["variants"], "integration label differs", canonical=v["integration"]), weight=2)
        dirt.corrupt(row, "Area", SRC, k, [dirt.miss()] + dirt.variants(ZONE_VARIANTS.get(v["zone"], []), "zone spelled differently", REPR), weight=2)
        if v["commission"] > 0:
            dirt.corrupt(row, "Commission_Pct", SRC, k, [
                (UNIT, "percentage stored as fraction (15 -> 0.15)", lambda x: round(x / 100, 4)),
                (WRONG_TYPE, "percentage stored as text with %", lambda x: f"{x}%"),
                (INVALID, "commission above 100%", lambda x: round(x * 10, 1))], weight=2)
        dirt.corrupt(row, "Rating", SRC, k, [
            (WRONG_TYPE, "decimal comma", lambda x: str(x).replace(".", ",")),
            (UNIT, "rating on a 0-50 scale", lambda x: int(round(x * 10))),
            (INVALID, "rating outside 1-5", lambda _x: rng.choice([0, 6.5, 9]))])
        dirt.corrupt(row, "Onboarded_On", SRC, k, [
            (DATE_FMT, "date in another format", lambda _x, d=v["onboarded"]: fmt_date(d, rng.choice(["iso", "mdy", "text", "compact"]))),
            (INVALID, "onboarding date in the future", lambda _x: fmt_date(local_date(cfg.end) + timedelta(days=rng.randint(30, 400)), "dmy")),
            dirt.miss()], weight=2)
        dirt.corrupt(row, "Status", SRC, k, dirt.variants(VENDOR_STATUS[v["status"]], "status flag written differently", REPR), weight=3)
        sink.write(row)
    sink.close()
    return vendors


def generate_products(cfg, rng, dirt):
    SRC = "catalog_pim/products.csv"
    products, pid = [], 0
    used_codes = []

    def new_barcode():
        """Random Egyptian EAN-13 at least BARCODE_MIN_GAP away from every other product's barcode,
        so that its Excel form (6.22528E+12) can only belong to one product."""
        while True:
            b = ean13("622" + f"{rng.randint(0, 10**9 - 1):09d}")
            if all(abs(int(b) - u) >= BARCODE_MIN_GAP for u in used_codes):
                used_codes.append(int(b))
                return b

    for category, item, brands, sizes, (pmin, pmax) in PRODUCT_TEMPLATES:
        for brand in brands:
            base = rng.uniform(pmin, pmax)
            for size, factor in sizes:
                pid += 1
                products.append({
                    "product_id": f"P{pid:05d}", "sku": f"SKU-{pid:05d}", "barcode": new_barcode(),
                    "name": f"{brand} {item} {size}", "brand": brand, "category": category, "pack_size": size,
                    "price": quarter(base * factor), "perishable": category in PERISHABLE,
                    "shelf_life": SHELF_LIFE[category], "active": rng.random() > 0.03,
                    "created": local_date(cfg.start) - timedelta(days=rng.randint(1, 700))})
    rows = []
    for p in products:
        dirt.begin_row()
        row = {"product_id": p["product_id"], "barcode": p["barcode"], "product_name": p["name"], "brand": p["brand"],
               "category": p["category"], "pack_size": p["pack_size"], "base_price_egp": p["price"],
               "is_perishable": "true" if p["perishable"] else "false",
               "is_active": "true" if p["active"] else "false"}
        k = p["product_id"]
        dirt.corrupt(row, "barcode", SRC, k, [
            (WRONG_TYPE, "barcode turned into scientific notation (Excel)", lambda b: f"{int(b):.5E}"),
            (WRONG_TYPE, "barcode stored as float", lambda b: f"{b}.0"),
            (INVALID, "wrong EAN-13 check digit", lambda b: b[:-1] + str((int(b[-1]) + 3) % 10))], weight=2)
        dirt.corrupt(row, "product_name", SRC, k, [
            (REPR, "name in upper case", lambda x: x.upper()),
            (REPR, "extra spaces", lambda x: "  " + x.replace(" ", "  ", 1) + " ")])
        dirt.corrupt(row, "brand", SRC, k, [dirt.miss()], weight=1.5)
        dirt.corrupt(row, "category", SRC, k, dirt.variants(CATEGORY_VARIANTS[p["category"]], "category label differs", canonical=p["category"]), weight=3)
        dirt.corrupt(row, "pack_size", SRC, k, dirt.variants(pack_size_variants(p["pack_size"]), "pack size in another unit/format", UNIT), weight=4)
        dirt.corrupt(row, "base_price_egp", SRC, k, [
            (WRONG_TYPE, "price as text with currency", lambda x: f"{x:.2f} EGP"),
            (WRONG_TYPE, "decimal comma", lambda x: f"{x:.2f}".replace(".", ",")),
            (INVALID, "negative price", lambda x: -x), (INVALID, "zero price", lambda _x: 0), dirt.miss()], weight=2)
        dirt.corrupt(row, "is_perishable", SRC, k, dirt.variants(BOOL_TEXT[p["perishable"]], "boolean written differently", REPR), weight=2)
        dirt.corrupt(row, "is_active", SRC, k, dirt.variants(BOOL_TEXT[p["active"]], "boolean written differently", REPR), weight=3)
        rows.append(row)
    # near-duplicate catalog entries: same product, new id (never sold, catalog noise)
    nxt = pid
    if dirt.rate > 0:
        for p in list(products):
            if rng.random() < dirt.rate * 1.5:
                nxt += 1
                nid = f"P{nxt:05d}"
                rows.insert(rng.randint(0, len(rows)), {
                    "product_id": nid, "barcode": p["barcode"], "product_name": p["name"].lower(), "brand": p["brand"],
                    "category": p["category"], "pack_size": rng.choice(pack_size_variants(p["pack_size"])),
                    "base_price_egp": p["price"], "is_perishable": "true" if p["perishable"] else "false",
                    "is_active": "true"})
                dirt.log(SRC, nid, "*entity", DUPLICATE, f"same product as {p['product_id']} under a new id (same barcode)", None, None)
    sink = CsvSink(f"{cfg.out_dir}/{SRC}", cols(SRC), dirt, SRC, lambda r: r["product_id"], dup_weight=0.5)
    for r in rows:
        sink.write(r)
    sink.close()
    return products


def generate_menu_items(cfg, rng, dirt, vendors):
    SRC = "restaurant_portal/menu_items.csv"
    items, n = [], 0
    sink = CsvSink(f"{cfg.out_dir}/{SRC}", cols(SRC), dirt, SRC, lambda r: r["menu_item_id"], dup_weight=0.5)
    for v in vendors:
        if v["type"] != "restaurant":
            continue
        menu = CUISINES[v["cuisine"]]["menu"]
        chosen = [m for m in menu if rng.random() < 0.85] or menu[:3]
        level = rng.uniform(0.9, 1.15)          # restaurant price positioning
        for section, name, (pmin, pmax), veg, kcal in chosen:
            n += 1
            m = {"menu_item_id": f"M{n:07d}", "vendor": v, "name": name, "section": section,
                 "price": round5(rng.uniform(pmin, pmax) * level), "prep": max(3, v["prep"] + rng.randint(-5, 8)),
                 "kcal": kcal + rng.randint(-60, 60), "veg": veg,
                 "created": v["onboarded"] + timedelta(days=rng.randint(0, 20))}
            items.append(m)
            dirt.begin_row(SLOPPY_MULT if v["sloppy"] else 1.0)
            row = {"menu_item_id": m["menu_item_id"], "vendor_id": v["vendor_id"], "item_name": name,
                   "menu_section": section, "price_egp": m["price"], "is_vegetarian": "true" if veg else "false"}
            k = m["menu_item_id"]
            dirt.corrupt(row, "item_name", SRC, k, [
                dirt.miss(), (REPR, "name in upper case", lambda x: x.upper()),
                (REPR, "extra spaces / trailing dot", lambda x: f" {x}. ")], weight=2)
            dirt.corrupt(row, "menu_section", SRC, k, dirt.variants(SECTION_VARIANTS.get(section, [section.lower()]), "menu section label differs", canonical=section), weight=3)
            dirt.corrupt(row, "price_egp", SRC, k, [
                (WRONG_TYPE, "price as text with LE", lambda x: f"{int(x)} LE"),
                (UNIT, "price in piasters", lambda x: int(x * 100)), dirt.miss()], weight=2)
            dirt.corrupt(row, "is_vegetarian", SRC, k, dirt.variants(BOOL_TEXT[veg], "boolean written differently", REPR), weight=2)
            sink.write(row)
    sink.close()
    return items


def generate_customers(cfg, rng, dirt):
    SRC = "customer_app/customers.csv"
    people = []
    for i in range(1, cfg.n_customers + 1):
        g = rng.choice(["M", "F"])
        first, last = rng.choice(FIRST_M if g == "M" else FIRST_F), rng.choice(LAST)
        city = weighted(rng, CITIES)
        c = {"customer_id": f"CUS-{i:06d}", "name": f"{first} {last}", "gender": g, "city": city,
             "zone": rng.choice(list(CITIES[city]["zones"])),     # home zone = where orders are delivered
             "signup": cfg.start - rng.uniform(86400, 800 * 86400), "phone": random_phone(rng),
             "email": f"{first}.{last}".lower().replace(" ", "") + f"{rng.randint(1, 999)}@{rng.choice(EMAIL_DOMAINS)}",
             "platform": rng.choices(["android", "ios", "web"], weights=[60, 32, 8])[0],
             "pro": rng.random() < 0.12, "lang": "ar" if rng.random() < 0.7 else "en", "dup_of": None}
        people.append(c)
    # near-duplicate accounts: same person, new id, phone typed differently
    if dirt.rate > 0:
        nxt = cfg.n_customers
        for c in list(people):
            if rng.random() < dirt.rate:
                nxt += 1
                people.append({**c, "customer_id": f"CUS-{nxt:06d}", "dup_of": c["customer_id"],
                               "signup": min(c["signup"] + rng.uniform(86400, 200 * 86400), cfg.start - 3600)})
    rng.shuffle(people)
    sink = CsvSink(f"{cfg.out_dir}/{SRC}", cols(SRC), dirt, SRC, lambda r: r["customer_id"], dup_weight=0.5)
    for c in people:
        dirt.begin_row()
        k = c["customer_id"]
        row = {"customer_id": k, "full_name": c["name"], "phone": c["phone"], "email": c["email"],
               "city": CITIES[c["city"]]["en"], "signup_ts": fmt_ts(c["signup"], "iso_utc"),
               "is_pro_subscriber": "true" if c["pro"] else "false"}
        if c["dup_of"]:
            row["full_name"] = rng.choice([c["name"].upper(), c["name"].lower(), c["name"]])
            row["phone"] = rng.choice(phone_variants(c["phone"]))
            dirt.log(SRC, k, "*entity", DUPLICATE, f"same person as {c['dup_of']} (second account)", None, None)
        else:
            dirt.corrupt(row, "full_name", SRC, k, [(REPR, "name in different case", lambda x: rng.choice([x.upper(), x.lower()])),
                                                     (REPR, "extra spaces", lambda x: " " + x.replace(" ", "  ") + " ")])
            dirt.corrupt(row, "phone", SRC, k, dirt.variants(phone_variants(c["phone"]), "phone in another format", UNIT), weight=5)
        dirt.corrupt(row, "email", SRC, k, [dirt.miss(), (INVALID, "malformed email", lambda e: rng.choice(
            [e.replace("@", "@@"), e.split(".")[0] + "@gmail", e.replace("@", " "), e.replace(".com", ",com")]))], weight=2)
        dirt.corrupt(row, "city", SRC, k, dirt.variants(CITIES[c["city"]]["variants"], "city written differently", REPR) + [dirt.miss()], weight=3)
        dirt.corrupt(row, "signup_ts", SRC, k, [
            (DATE_FMT, "timestamp in another format", lambda _x, s=c["signup"]: fmt_ts(s, rng.choice(["dmy_local", "mdy_local_12h", "epoch_ms", "text_local"]))),
            (INVALID, "signup in the future", lambda _x: fmt_ts(cfg.end + rng.randint(10, 400) * 86400, "iso_utc"))], weight=2)
        dirt.corrupt(row, "is_pro_subscriber", SRC, k, dirt.variants(BOOL_TEXT[c["pro"]], "boolean written differently", REPR), weight=2)
        sink.write(row)
    sink.close()
    return people


# =============================================================================
# 10. SIMULATION (CDC + WMS + ORDERS in one chronological event loop)
# =============================================================================
class Listing:
    """One row of the operational table vendor_items: an item sold by a vendor."""
    __slots__ = ("vendor", "ref", "item_type", "name", "clean_name", "regular", "price", "qty", "avail",
                 "listed", "promo", "perishable", "barcode", "shelf_life", "tl_t", "tl_price", "tl_avail",
                 "tl_ok", "last_after", "last_commit", "restock_pending", "anomaly", "pre_anomaly",
                 "gate_ref", "scd_cur", "scd_prev", "scd_ver", "unit_cost",
                 "phys", "phys_price", "pending_price")

    def __init__(self, vendor, ref, item_type, name, price, qty, perishable=False, barcode=None, shelf_life=None):
        self.vendor, self.ref, self.item_type = vendor, ref, item_type
        self.name = self.clean_name = name
        self.regular = self.price = price
        self.qty = qty
        self.avail = (qty > 0) if qty is not None else True
        self.listed, self.promo = False, False
        self.perishable, self.barcode, self.shelf_life = perishable, barcode, shelf_life
        self.tl_t, self.tl_price, self.tl_avail, self.tl_ok = [], [], [], []   # app-view timeline
        self.last_after, self.last_commit = None, 0.0
        self.restock_pending, self.anomaly, self.pre_anomaly = False, None, None
        self.gate_ref, self.scd_cur, self.scd_prev, self.scd_ver = None, None, None, 0
        self.unit_cost = None
        # PHYSICAL reality (what the kitchen / shelf really has) vs. what the DB says.
        # The gap between the two is the root cause of stale data in the app.
        self.phys = self.avail               # really available right now?
        self.phys_price = price              # price the vendor really charges right now
        self.pending_price = None            # new price decided but not yet typed into the system


class Simulator:
    def __init__(self, cfg, rng, dirt, vendors, products, menu_items, customers):
        self.cfg, self.rng, self.dirt = cfg, rng, dirt
        o = cfg.out_dir
        self.vendors, self.customers = vendors, customers
        self.cdc = CdcSink(f"{o}/ops_db_cdc/vendor_items_events.csv")
        W = "wms/inventory_movements.csv"
        self.wms = CsvSink(f"{o}/{W}", cols(W), dirt, W, lambda r: r["movement_id"], dup_weight=0.6)
        O, I = "customer_app/orders.csv", "customer_app/order_items.csv"
        self.orders = CsvSink(f"{o}/{O}", cols(O), dirt, O, lambda r: r["order_id"], dup_weight=0.6)
        self.items = CsvSink(f"{o}/{I}", cols(I), dirt, I, lambda r: f"{r['order_id']}#{r['line_number']}", dup_weight=0.5)
        os.makedirs(f"{o}/_answer_key", exist_ok=True)
        self.scen_f = open(f"{o}/_answer_key/scenario_events.csv", "w", newline="", encoding="utf-8")
        self.scen = csv.writer(self.scen_f, lineterminator="\n")
        self.scen.writerow(SCENARIO_COLUMNS)
        self.scd_f = open(f"{o}/_answer_key/expected_scd2_vendor_items.csv", "w", newline="", encoding="utf-8")
        self.scd = csv.writer(self.scd_f, lineterminator="\n")
        self.scd.writerow(SCD_COLUMNS)
        self.heap, self.seq = [], 0
        self.used_lsn = set()
        self.mv_seq = self.order_seq = 0
        self.stats = Counter()
        self.by_vendor = {}
        self.listings = []
        # 2 days of a buggy app release (systematic, multi-column noise in orders)
        d0 = rng.randint(5, max(5, cfg.days - 4))
        self.bug_window = (cfg.start + d0 * 86400, cfg.start + (d0 + 2) * 86400)
        self._build_listings(products, menu_items)

    # ---------------------------------------------------------------- setup --
    def push(self, t, kind, l=None, arg=None):
        self.seq += 1
        heapq.heappush(self.heap, (t, self.seq, kind, l, arg))

    def _build_listings(self, products, menu_items):
        rng = self.rng
        for m in menu_items:
            v = m["vendor"]
            if v["status"] != "active":
                continue
            l = Listing(v, m["menu_item_id"], "menu", m["name"], m["price"], None)
            l.gate_ref = m["price"]                 # DQ reference = menu price (master data)
            self.by_vendor.setdefault(v["vendor_id"], []).append(l)
        for v in self.vendors:
            if v["status"] != "active" or v["type"] == "restaurant":
                continue
            spec = VENDOR_TYPES[v["type"]]
            cands = [p for p in products if p["active"] and p["category"] in spec["categories"]]
            chosen = rng.sample(cands, k=max(1, round(len(cands) * rng.uniform(*spec["assortment"]))))
            markup = rng.uniform(0.95, 1.10)
            for p in chosen:
                if v["type"] == "dark_store":
                    qty = 0 if rng.random() < 0.03 else rng.randint(20, 150)
                else:
                    qty = 0 if rng.random() < 0.05 else rng.randint(10, 150)
                l = Listing(v, p["sku"], "grocery", p["name"], quarter(p["price"] * markup * rng.uniform(0.98, 1.03)), qty,
                            p["perishable"], p["barcode"], p["shelf_life"])
                l.unit_cost = round(p["price"] * rng.uniform(0.70, 0.85), 2)
                l.gate_ref = p["price"]             # DQ reference = catalog list price (master data)
                self.by_vendor.setdefault(v["vendor_id"], []).append(l)
        for ls in self.by_vendor.values():
            self.listings.extend(ls)

    def report_delay(self, v):
        """Seconds between a physical stock-out and the moment the vendor reports it."""
        r = self.rng
        return {"MANUAL_PORTAL": r.uniform(900, 5400), "POS_LEGACY": r.uniform(0, 1200),
                "API": r.uniform(60, 600), "WMS": 0.0}[v["integration"]]

    def price_delay(self, v):
        """Seconds between a vendor deciding a new price and typing it into the system."""
        r = self.rng
        return {"MANUAL_PORTAL": r.uniform(6 * 3600, 72 * 3600), "POS_LEGACY": r.uniform(0, 3 * 3600),
                "API": r.uniform(0, 900), "WMS": 0.0}[v["integration"]]

    def next_open(self, v, t):
        """t if the vendor is open at t, otherwise the next opening time + a few minutes."""
        for _ in range(48):
            if self.is_open(v, to_local(t).hour):
                return t
            t = (math.floor(t / 3600) + 1) * 3600 + self.rng.uniform(60, 1200)
        return t

    def _schedule(self):
        """Pre-schedule every non-order event of every listing (business time).
        Changes made BY THE VENDOR (managers, POS terminals, kitchens, shelves) happen only while the
        vendor is open. Dark stores run 24 h: waste is written off at the morning quality check
        (05-09 h) and cycle counts happen at night (01-05 h), as in a real warehouse."""
        cfg, rng = self.cfg, self.rng
        S, E = cfg.start, cfg.end
        for l in self.listings:
            v = l.vendor
            open_h = lambda h, v=v: self.is_open(v, h)
            if rng.random() < 0.03:
                first, op = self.next_open(v, S + rng.uniform(3600, (E - S) * 0.8)), "c"
            else:
                first, op = S + rng.uniform(0, 900), "r"
            self.push(first, "INIT", l, op)
            menu = l.item_type == "menu"
            for t in window_times(rng, 0.03 if menu else 0.04, first + 60, E, open_h):
                # the vendor changes the real price at t_phys, the system only learns it at t
                t_phys = max(first + 30, t - self.price_delay(v))
                self.push(t_phys, "PHYS_PRICE", l, (rng.uniform(1.05, 1.15) if menu else rng.gauss(1.03, 0.05), t))
            for t in window_times(rng, 0.005 if menu else 0.015, first + 60, E, open_h):
                self.push(t, "PROMO_START", l, rng.uniform(0.75, 0.90))
            a_rate = 0.0015 * INTEGRATIONS[v["integration"]]["anomaly_mult"] * (2 if v["sloppy"] else 1) * cfg.anomaly_rate
            for t in window_times(rng, a_rate, first + 60, E, open_h):
                self.push(t, "ANOMALY", l, rng.choice(["zero", "negative", "fat_up", "fat_down", "null_name"]))
            if menu:
                for t in window_times(rng, 0.25, first + 60, E, open_h):
                    self.push(max(first + 30, t - self.report_delay(v)), "PHYS_OFF", l)   # kitchen runs out
                    self.push(t, "TOGGLE", l, False)                                      # ... reported later
                    # back on the menu 30 min - 5 h later; if the kitchen has closed by then, at the next opening
                    self.push(self.next_open(v, t + rng.uniform(1800, 5 * 3600)), "TOGGLE", l, True)
            elif v["type"] == "dark_store":
                if l.perishable:
                    for t in window_times(rng, 0.12, first + 60, E, lambda h: 5 <= h < 9):
                        self.push(t, "WASTE", l)
                for t in window_times(rng, 0.02, first + 60, E, lambda h: 1 <= h < 5):
                    self.push(t, "ADJUST", l)
            else:
                for t in window_times(rng, 0.4, first + 60, E, open_h):
                    self.push(t, "POS_SYNC", l)
                for t in window_times(rng, 0.2, first + 60, E, open_h):   # shelf empties before the system knows
                    self.push(t, "PHYS_STOCKOUT", l)
                if v["type"] == "supermarket" and rng.random() < 0.015:
                    self.push(self.next_open(v, first + rng.uniform(3600, max(3601, E - first - 60))), "DELIST", l)
        # SCD2 stress test: some restaurants toggle items many times in a day
        for v in self.vendors:
            if v["type"] != "restaurant" or v["status"] != "active" or rng.random() > 0.20:
                continue
            ls = self.by_vendor.get(v["vendor_id"], [])
            if not ls:
                continue
            for day in rng.sample(range(cfg.days), k=min(cfg.days, rng.randint(2, 4))):
                d = local_date(S) + timedelta(days=day)
                for l in rng.sample(ls, k=min(len(ls), rng.randint(1, 3))):
                    t = local_to_ts(d.year, d.month, d.day, rng.randint(12, 19), rng.randint(0, 59), rng.randint(0, 59))
                    for _ in range(rng.randint(4, 10)):
                        if not (S < t < E):
                            break
                        r = rng.random()
                        sub = "same_second" if r < 0.15 else "noop" if r < 0.25 else "late" if r < 0.35 else "toggle"
                        self.push(t, "STRESS", l, sub)
                        t += rng.uniform(180, 2700)

    def _schedule_orders(self):
        """Order requests: customer -> vendor type -> day (weekday demand) -> hour (Egyptian curve of
        that type) -> an open vendor that delivers to the customer's home zone (closer = more likely)."""
        cfg, rng = self.cfg, self.rng
        types = list(VENDOR_TYPES)
        # per (zone, type, local hour): open vendors within delivery range + cumulative weights
        index, type_weight = {}, {}
        self.zone_dist = {}
        for c, cd in CITIES.items():
            for z, (zla, zlo) in cd["zones"].items():
                for vt in types:
                    pool = []
                    for v in self.vendors:
                        if v["city"] != c or v["type"] != vt or v["status"] != "active" or not self.by_vendor.get(v["vendor_id"]):
                            continue
                        d = dist_km(v["lat"], v["lng"], zla, zlo)
                        self.zone_dist[(v["vendor_id"], z)] = d
                        if d <= DELIVERY_RADIUS_KM[vt]:
                            pool.append((v, math.exp(-d / 3.0)))     # nearer vendors are chosen more often
                    type_weight[(z, vt)] = sum(w for _v, w in pool) * VENDOR_TYPES[vt]["popularity"]
                    for h in range(24):
                        vs = [(v, w) for v, w in pool if self.is_open(v, h)]
                        cum, acc = [], 0.0
                        for _v, w in vs:
                            acc += w
                            cum.append(acc)
                        index[(z, vt, h)] = ([v for v, _w in vs], cum)
        # Ordering frequency per customer: heavy-tailed (a few loyal users order a lot), but capped
        # so nobody orders hundreds of times a month. Duplicate accounts order less (weight x0.3).
        weights = [min(rng.paretovariate(1.3), CUSTOMER_WEIGHT_CAP) * (0.3 if c["dup_of"] else 1.0)
                   for c in self.customers]
        n = int(cfg.n_orders * 1.15)               # some requests fail (closed vendor, nothing in stock)
        custs = rng.choices(self.customers, weights=weights, k=n)
        d0 = local_date(cfg.start)
        day_w = [WEEKDAY_DEMAND[(d0 + timedelta(days=i)).weekday()] for i in range(cfg.days)]
        per_day = Counter()                        # nobody places more than 4 orders on one day
        for c in custs:
            tw = [type_weight[(c["zone"], vt)] for vt in types]
            if not sum(tw):
                continue
            vt = rng.choices(types, weights=tw)[0]
            h = rng.choices(range(24), weights=HOUR_CURVES[vt])[0]
            d = d0 + timedelta(days=rng.choices(range(cfg.days), weights=day_w)[0])
            t = local_to_ts(d.year, d.month, d.day, h, rng.randint(0, 59), rng.randint(0, 59))
            if not (cfg.start <= t < cfg.end):
                continue
            vs, cw = index[(c["zone"], vt, h)]
            if not vs or per_day[(c["customer_id"], d)] >= 4:
                continue
            per_day[(c["customer_id"], d)] += 1
            self.push(t, "ORDER", None, (c, rng.choices(vs, cum_weights=cw)[0]))

    @staticmethod
    def is_open(v, local_hour):
        if v["close_h"] - v["open_h"] >= 24:
            return True
        h = local_hour if local_hour >= v["open_h"] else local_hour + 24
        return h < v["close_h"]

    # ------------------------------------------------------------- helpers --
    def writer_of(self, l, manual=True):
        """updated_by value for a change made by the vendor itself."""
        v = l.vendor
        return {"MANUAL_PORTAL": f"mgr_{v['num']:04d}", "API": "partner_api", "POS_LEGACY": "pos_sync",
                "WMS": "catalog_team" if manual else "wms_sync"}[v["integration"]]

    def commit_time(self, l, t):
        """When the change is committed in PostgreSQL (and gets its LSN)."""
        integ = l.vendor["integration"]
        if integ == "POS_LEGACY":
            c = math.ceil(t / POS_BATCH_S) * POS_BATCH_S + self.rng.uniform(5, 120)
        elif integ == "WMS":
            c = t + self.rng.uniform(0.05, 1.0)
        else:
            c = t + self.rng.uniform(0.2, 4.0)
        c = max(c, l.last_commit + 0.00001)       # commits of one row are ordered
        l.last_commit = c
        return c

    def gate(self, after, l):
        """Reference DQ gate. A correct pipeline rejects exactly these events to
        the DLQ; everything else must be cleaned/cast and accepted."""
        if is_null(after.get("item_name")):
            return False, "NULL_ITEM_NAME", None
        p = parse_number(after.get("item_price"))
        if p is None:
            return False, "MISSING_OR_UNPARSEABLE_PRICE", None
        if l.vendor["integration"] == "POS_LEGACY":
            p /= 100.0                             # piasters -> EGP
        p = round(p, 2)
        if p <= 0:
            return False, "NON_POSITIVE_PRICE", None
        # Outlier test against the MASTER-DATA list price, never against a price learned
        # from the stream: one accepted typo would otherwise poison the reference.
        if p > 3 * l.gate_ref or p < 0.2 * l.gate_ref:
            return False, "PRICE_OUTLIER", None
        av = parse_bool(after.get("is_available"))
        if av is None:
            return False, "INVALID_AVAILABILITY", None
        if l.item_type == "grocery":
            q = parse_number(after.get("qty_on_hand"))
            if q is None:
                return False, "MISSING_STOCK", None
            if q < 0:
                return False, "NEGATIVE_STOCK", None
            if (q > 0) != av:
                return False, "AVAILABILITY_CONFLICT", None
        return True, None, (p, av)

    # SCD2 builder (truth). Versions on (price_egp, is_available) at second precision.
    def scd_write(self, l, p, a, frm, to, current):
        l.scd_ver += 1
        self.scd.writerow([l.vendor["vendor_id"], l.ref, l.item_type, l.scd_ver, f"{p:.2f}", "true" if a else "false",
                           fmt_ts(frm, "iso_utc"), "" if to is None else fmt_ts(to, "iso_utc"), "true" if current else "false"])
        self.stats["scd_versions"] += 1

    def scd_apply(self, l, ts, p, a):
        cur = l.scd_cur
        if cur is None:
            l.scd_cur = [p, a, ts]
            return
        if cur[0] == p and cur[1] == a:
            return                                  # no-op update -> no new version
        if ts <= cur[2]:                            # same second -> zero-length version
            prev = l.scd_prev
            if prev is not None and prev[0] == p and prev[1] == a and prev[3] == cur[2]:
                l.scd_cur, l.scd_prev = [prev[0], prev[1], prev[2]], None   # net change = none
            else:
                cur[0], cur[1] = p, a
            return
        if l.scd_prev is not None:
            self.scd_write(l, *l.scd_prev, current=False)
        l.scd_prev = (cur[0], cur[1], cur[2], ts)
        l.scd_cur = [p, a, ts]

    def scd_close(self, l, ts=None):
        if l.scd_prev is not None:
            self.scd_write(l, *l.scd_prev, current=False)
        if l.scd_cur is not None:
            self.scd_write(l, l.scd_cur[0], l.scd_cur[1], l.scd_cur[2], ts, current=ts is None)
        l.scd_prev = l.scd_cur = None

    # ------------------------------------------------------------- emit CDC --
    def emit(self, l, t, op, scenario, updated_by, late=False):
        rng, dirt = self.rng, self.dirt
        v = l.vendor
        commit = self.commit_time(l, t)
        lsn = int(commit * 1_000_000)
        while lsn in self.used_lsn:
            lsn += 1
        self.used_lsn.add(lsn)
        key = f"lsn={lsn}"
        after = None
        if op != "d":
            legacy = v["integration"] == "POS_LEGACY"
            after = {"item_name": l.name,
                     "item_price": None if l.price is None else (int(round(l.price * 100)) if legacy else l.price),
                     "currency": "PT" if legacy else "EGP", "qty_on_hand": l.qty, "is_available": l.avail,
                     "updated_by": updated_by, "updated_at": fmt_ts(t, "local_naive")}
            self._noise_cdc(after, key, l, t)
        # app view timeline (state as committed in the DB)
        self._timeline(l, commit, op != "d" and l.listed and l.avail, l.anomaly is None and op != "d")
        if op == "d":
            ok, reason, parsed = True, None, None
            self.scd_close(l, int(t))
        else:
            ok, reason, parsed = self.gate(after, l)
            if ok:
                self.scd_apply(l, int(t), parsed[0], parsed[1])
        self.stats["gate_pass" if ok else "gate_reject"] += 1
        self.stats["scenario:" + scenario] += 1
        self.scen.writerow([lsn, v["vendor_id"], l.ref, op, scenario, "PASS" if ok else "REJECT", reason or "",
                            fmt_ts(t, "iso_utc"), "true" if late else "false"])
        # one flat CSV row per change (on a delete only the keys are filled)
        row = {"lsn": lsn, "op": op, "captured_at_ms": int(commit * 1000), "store_id": v["num"], "item_ref": l.ref}
        if after is not None:
            row.update(after)
        self.cdc.push(commit + (rng.uniform(900, 7200) if late else 0.0), row)
        if dirt.rate > 0 and rng.random() < dirt.rate * 0.4:     # at-least-once re-delivery (exact copy)
            self.cdc.push(commit + rng.uniform(1, 7200), dict(row))
            dirt.log("ops_db_cdc/vendor_items_events.csv", key, "*row", DUPLICATE,
                     "same CDC event delivered twice (at-least-once)", None, None)

    def _timeline(self, l, commit, avail, ok):
        l.tl_t.append(commit)
        l.tl_price.append(l.price)
        l.tl_avail.append(avail)
        l.tl_ok.append(ok)

    def _noise_cdc(self, after, key, l, t):
        """Serialization-level noise. INVALID business values come from the
        explicit anomaly scenario, not from here (except negative stock)."""
        dirt, rng = self.dirt, self.rng
        S = "ops_db_cdc/vendor_items_events.csv"
        v = l.vendor
        dirt.begin_row((SLOPPY_MULT if v["sloppy"] else 1.0) * (0.5 if v["type"] == "dark_store" else 1.0))
        dirt.corrupt(after, "item_price", S, key, [
            (WRONG_TYPE, "price text with currency suffix", lambda x, a=after: f"{x} {a['currency']}"),
            (WRONG_TYPE, "decimal comma", lambda x: str(x).replace(".", ","))], weight=0.7)
        if v["integration"] == "POS_LEGACY":
            dirt.corrupt(after, "currency", S, key, [(MISSING, "currency missing on a piaster price", lambda _v: None)], weight=2)
        else:
            dirt.corrupt(after, "currency", S, key, dirt.variants(["egp", "LE", "E£", "جنيه"], "currency code differs", REPR), weight=0.5)
        if l.item_type == "grocery":
            dirt.corrupt(after, "qty_on_hand", S, key, [
                (WRONG_TYPE, "integer serialized as decimal text", lambda q: f"{q}.0"),
                (INVALID, "negative stock", lambda _q: -rng.randint(1, 20))], weight=0.5)
        dirt.corrupt(after, "is_available", S, key, dirt.variants(AVAIL_VARIANTS[bool(after["is_available"])],
                     "availability flag written differently", REPR), weight=1.5)
        dirt.corrupt(after, "updated_at", S, key, [
            (DATE_FMT, "timestamp in another format", lambda _x, tt=t: fmt_ts(tt, rng.choice(
                ["iso_local_tz", "dmy_local_s", "mdy_local_12h", "epoch_ms", "iso_utc"])))], weight=0.7)

    # ------------------------------------------------------------- WMS rows --
    def wms_row(self, l, t, mtype, qty, ref=None, unit_cost=None, expiry=None, by="wms_auto"):
        dirt, rng = self.dirt, self.rng
        self.mv_seq += 1
        k = f"MV-{self.mv_seq:08d}"
        row = {"movement_id": k, "warehouse_code": f"WH-{l.vendor['num']:04d}", "product_barcode": l.barcode,
               "movement_type": mtype, "quantity": qty, "unit_cost_egp": unit_cost,
               "expiry_date": expiry.isoformat() if expiry else None, "reference_id": ref,
               "movement_ts": fmt_ts(t, "dmy_local_s")}
        W = "wms/inventory_movements.csv"
        dirt.begin_row(0.8)
        num = l.vendor["num"]
        dirt.corrupt(row, "warehouse_code", W, k, dirt.variants([f"wh-{num:04d}", f"WH{num}", f"{num:04d}", f"WH-{num}"],
                     "warehouse code in another format", REPR))
        dirt.corrupt(row, "product_barcode", W, k, [
            (WRONG_TYPE, "barcode in scientific notation (Excel)", lambda b: f"{int(b):.5E}"),
            (WRONG_TYPE, "barcode stored as float", lambda b: f"{b}.0")])
        dirt.corrupt(row, "movement_type", W, k, dirt.variants(MOVEMENT_TYPES[mtype], "movement type label differs", canonical=mtype), weight=1.5)
        if qty < 0:
            dirt.corrupt(row, "quantity", W, k, [(REPR, "outbound quantity without minus sign (sign convention)", lambda q: abs(q)),
                                                 (WRONG_TYPE, "quantity with unit text", lambda q: f"{q} units")])
        else:
            dirt.corrupt(row, "quantity", W, k, [(WRONG_TYPE, "quantity with unit text", lambda q: f"{q} units")])
        if unit_cost is not None:
            dirt.corrupt(row, "unit_cost_egp", W, k, [(WRONG_TYPE, "decimal comma", lambda x: str(x).replace(".", ",")), dirt.miss()])
        if expiry is not None:
            dirt.corrupt(row, "expiry_date", W, k, [
                (DATE_FMT, "date in another format", lambda _x, d=expiry: fmt_date(d, rng.choice(["dmy", "mdy", "text"]))),
                (INVALID, "expiry date before the movement date", lambda _x, d=expiry: (local_date(t) - timedelta(days=rng.randint(1, 30))).isoformat())])
        dirt.corrupt(row, "movement_ts", W, k, [
            (DATE_FMT, "timestamp in another format", lambda _x, tt=t: fmt_ts(tt, rng.choice(["local_naive", "iso_local_tz", "epoch_ms"])))], weight=0.7)
        self.wms.write(row)
        self.stats["wms:" + mtype] += 1

    def check_reorder(self, l, t):
        if l.vendor["type"] == "dark_store" and l.qty <= REORDER_POINT and not l.restock_pending and l.listed:
            l.restock_pending = True
            self.push(t + self.rng.uniform(2 * 3600, 14 * 3600), "RESTOCK", l)

    # --------------------------------------------------------- event handlers --
    def on_INIT(self, t, l, op):
        l.listed = True
        self.emit(l, t, op, "SNAPSHOT" if op == "r" else "NEW_LISTING", self.writer_of(l, manual=False))
        if l.vendor["type"] == "dark_store":
            exp = local_date(t) + timedelta(days=l.shelf_life) if l.perishable else None
            self.wms_row(l, t, "OPENING_BALANCE", l.qty, unit_cost=l.unit_cost, expiry=exp)
            self.check_reorder(l, t)

    def on_PHYS_PRICE(self, t, l, arg):
        """The vendor starts charging a new price (real world). Not in the DB yet."""
        factor, t_entry = arg
        if not l.listed or l.promo or l.pending_price is not None:
            return
        lo, hi = PRICE_BAND[0] * l.gate_ref, PRICE_BAND[1] * l.gate_ref
        if l.item_type == "menu":
            new = round5(min(hi, l.regular * factor))      # restaurants raise prices, within the band
            if new <= l.regular:
                return                                      # already at the top of the band: no change
        else:
            new = quarter(min(hi, max(lo, l.regular * max(0.85, min(1.20, factor)))))
            if new == l.regular:
                return
        l.pending_price = new
        l.phys_price = new
        self.push(max(t, t_entry), "PRICE_ENTRY", l, new)

    def on_PRICE_ENTRY(self, t, l, new):
        """Happy path: the new price is typed into the system -> CDC update."""
        if l.pending_price is None:
            return
        if not l.listed:                           # item was delisted: the change is dropped
            l.pending_price = None
            return
        if l.anomaly:                              # a typo is being fixed: try again in an hour
            self.push(self.next_open(l.vendor, t + 3600), "PRICE_ENTRY", l, new)
            return
        l.pending_price = None
        l.regular = new
        if not l.promo:
            l.price = new
        l.phys_price = l.price
        self.emit(l, t, "u", "HAPPY_PRICE_CHANGE", self.writer_of(l))

    def on_PHYS_OFF(self, t, l, _):
        """Kitchen runs out of an ingredient: really unavailable, app still says available."""
        if l.listed and l.avail:
            l.phys = False

    def on_PHYS_STOCKOUT(self, t, l, _):
        """Shelf is empty (walk-in customers bought the rest); reported with a delay."""
        if l.listed and l.avail and l.phys:
            l.phys = False
            self.push(self.next_open(l.vendor, t + self.report_delay(l.vendor)), "STOCKOUT_REPORT", l)

    def on_STOCKOUT_REPORT(self, t, l, _):
        if not l.listed or l.phys:                 # restocked in the meantime
            return
        l.qty, l.avail = 0, False
        self.emit(l, t, "u", "STOCKOUT_REPORTED", self.writer_of(l))

    def on_PROMO_START(self, t, l, factor):
        if not l.listed or l.anomaly or l.promo or l.pending_price is not None:
            return
        l.price = round5(l.regular * factor) if l.item_type == "menu" else quarter(l.regular * factor)
        if l.price == l.regular:
            return
        l.promo = True
        l.phys_price = l.price
        self.emit(l, t, "u", "PROMO_START", self.writer_of(l))
        self.push(self.next_open(l.vendor, t + self.rng.uniform(86400, 5 * 86400)), "PROMO_END", l)

    def on_PROMO_END(self, t, l, _):
        if not l.promo:
            return
        l.promo = False
        if l.anomaly:                       # will be restored by the correction
            l.pre_anomaly = (l.regular, l.pre_anomaly[1])
            l.phys_price = l.regular
            return
        l.price = l.phys_price = l.regular
        if l.listed:
            self.emit(l, t, "u", "PROMO_END", self.writer_of(l))

    def on_ANOMALY(self, t, l, kind):
        if not l.listed or l.anomaly or l.price is None:
            return
        l.pre_anomaly = (l.price, l.name)
        if kind == "zero":
            l.price = 0.0
        elif kind == "negative":
            l.price = -50.0 if self.rng.random() < 0.5 else -l.price
        elif kind == "fat_up":
            l.price = round(l.price * 10, 2)
        elif kind == "fat_down":
            l.price = round(l.price / 10, 2)
        else:
            l.name = None
        l.anomaly = kind
        self.emit(l, t, "u", "DQ_ANOMALY_" + kind.upper(), self.writer_of(l))
        delay = self.rng.uniform(120, 3600) if self.rng.random() < 0.85 else self.rng.uniform(4 * 3600, 12 * 3600)
        self.push(self.next_open(l.vendor, t + delay), "CORRECTION", l)

    def on_CORRECTION(self, t, l, _):
        if not l.anomaly:
            return
        l.price, l.name = l.pre_anomaly
        l.anomaly = None
        if l.listed:
            self.emit(l, t, "u", "DQ_CORRECTION", self.writer_of(l))

    def on_TOGGLE(self, t, l, target):
        if not l.listed or l.avail == target:
            return
        l.avail = l.phys = target
        self.emit(l, t, "u", "AVAILABILITY_TOGGLE", self.writer_of(l))

    def on_STRESS(self, t, l, sub):
        if not l.listed:
            return
        self._stress(t, l, sub)
        l.phys = l.avail

    def _stress(self, t, l, sub):
        by = self.writer_of(l)
        if sub == "same_second":                     # flip and flip back in the same second
            l.avail = not l.avail
            self.emit(l, t, "u", "SCD_STRESS_SAME_SECOND", by)
            l.avail = not l.avail
            self.emit(l, t, "u", "SCD_STRESS_SAME_SECOND", by)
        elif sub == "noop":                          # row saved without any change
            self.emit(l, t, "u", "SCD_STRESS_NOOP", by)
        elif sub == "late":                          # delivered after newer events
            l.avail = not l.avail
            self.emit(l, t, "u", "SCD_STRESS_LATE_ARRIVAL", by, late=True)
        else:
            l.avail = not l.avail
            self.emit(l, t, "u", "SCD_STRESS_TOGGLE", by)

    def on_POS_SYNC(self, t, l, _):
        if not l.listed:
            return
        if l.qty == 0 or self.rng.random() < 0.25:
            l.qty += self.rng.randint(20, 100)
        else:
            l.qty = max(0, l.qty - self.rng.randint(1, 30))
        l.avail = l.phys = l.qty > 0
        self.emit(l, t, "u", "POS_STOCK_SYNC", self.writer_of(l))

    def on_RESTOCK(self, t, l, _):
        l.restock_pending = False
        if not l.listed:
            return
        n = self.rng.randint(40, 120)
        l.qty += n
        l.avail = l.phys = True
        self.emit(l, t, "u", "RESTOCK", "wms_sync")
        exp = local_date(t) + timedelta(days=l.shelf_life) if l.perishable else None
        self.wms_row(l, t, "RESTOCK", n, ref=f"PO-{self.rng.randint(100000, 999999)}",
                     unit_cost=round(l.unit_cost * self.rng.uniform(0.97, 1.05), 2), expiry=exp,
                     by=f"supervisor_{self.rng.randint(1, 20):02d}")

    def on_WASTE(self, t, l, _):
        if not l.listed or l.qty <= 0:
            return
        w = min(l.qty, self.rng.randint(1, 4))
        l.qty -= w
        l.avail = l.phys = l.qty > 0
        self.emit(l, t, "u", "WASTE", "wms_sync")
        self.wms_row(l, t, "WASTE", -w, by=f"supervisor_{self.rng.randint(1, 20):02d}")
        self.check_reorder(l, t)

    def on_ADJUST(self, t, l, _):
        if not l.listed:
            return
        new = max(0, l.qty + self.rng.choice([-3, -2, -1, 1, 2]))
        delta = new - l.qty
        if delta == 0:
            return
        l.qty = new
        l.avail = l.phys = l.qty > 0
        self.emit(l, t, "u", "STOCK_ADJUSTMENT", "wms_sync")
        self.wms_row(l, t, "ADJUSTMENT", delta, by=f"supervisor_{self.rng.randint(1, 20):02d}")
        self.check_reorder(l, t)

    def on_DELIST(self, t, l, _):
        if not l.listed:
            return
        l.listed = False
        l.avail = False
        self.emit(l, t, "d", "DELIST", self.writer_of(l))

    def on_ORDER(self, t, _l, req):
        c, v = req
        rng, dirt = self.rng, self.dirt
        listings = self.by_vendor[v["vendor_id"]]
        cutoff = math.floor(t / CACHE_REFRESH_S) * CACHE_REFRESH_S     # what the cached app shows
        cands = listings if len(listings) <= 14 else rng.sample(listings, 14)
        sel = []
        for l in cands:
            i = bisect.bisect_right(l.tl_t, cutoff) - 1
            if i >= 0 and l.tl_ok[i] and l.tl_avail[i]:
                sel.append((l, l.tl_price[i]))
        if not sel:
            self.stats["order_requests_dropped"] += 1
            return
        n = min(len(sel), rng.choices([1, 2, 3, 4, 5, 6], weights=[22, 26, 22, 15, 10, 5])[0])
        lines = []
        stale_unavailable = False
        for l, app_price in rng.sample(sel, n):
            q = rng.choices([1, 2, 3], weights=[70, 22, 8])[0] if l.item_type == "menu" else rng.choices([1, 2, 3, 4], weights=[60, 25, 10, 5])[0]
            if (not l.listed) or (not l.avail) or (not l.phys) or (l.qty is not None and l.qty < q):
                stale_unavailable = True               # app said "available", reality says no
            if abs(l.phys_price - app_price) > 0.001:  # app price differs from what the vendor really charges
                self.stats["order_lines_stale_price"] += 1
            lines.append((l, q, app_price, l.phys_price))
        self.stats["order_lines"] += len(lines)

        if stale_unavailable:
            status, reason = "cancelled", "item_unavailable"
            self.stats["orders_cancelled_stale_availability"] += 1
        else:
            status = rng.choices(["delivered", "cancelled", "refunded", "failed"], weights=[90, 5.5, 2.5, 2])[0]
            reason = {"delivered": None,
                      "cancelled": rng.choices(["customer_cancelled", "vendor_busy", "payment_failed"], weights=[50, 30, 20])[0],
                      "refunded": rng.choice(["missing_item", "quality_issue"]),
                      "failed": rng.choice(["rider_unavailable", "customer_unreachable"])}[status]
        self.order_seq += 1
        oid = f"ORD-{self.order_seq:08d}"

        # dark store: picking deducts stock (CDC update + WMS SALE movement)
        if v["type"] == "dark_store" and status in ("delivered", "refunded", "failed"):
            for l, q, _p, _vp in lines:
                l.qty -= q
                l.avail = l.phys = l.qty > 0
                self.emit(l, t + 1, "u", "SALE_DEDUCTION", "wms_sync")
                self.wms_row(l, t + 1, "SALE", -q, ref=oid, by=f"picker_{rng.randint(1, 40):02d}")
                self.check_reorder(l, t + 1)

        subtotal = round(sum(round(q * p, 2) for _l, q, p, _vp in lines), 2)
        local = to_local(t)
        promo = rng.choices(list(PROMO_CODES), weights=[85, 7, 5, 3])[0]
        if promo == "WEEKEND20" and local.weekday() not in EGYPT_WEEKEND:
            promo = None                                   # the weekend code only works on Friday / Saturday
        discount = round(min(100.0, subtotal * PROMO_CODES[promo] / 100), 2)
        fee = 0.0 if c["pro"] else float(v["fee"])
        service = round(min(15.0, max(2.0, subtotal * 0.03)), 2)
        total = round(subtotal - discount + fee + service, 2)

        # ---- delivery time -------------------------------------------------------------
        # ETA shown in the app = prep time + the upper part of the usual travel range + a small buffer,
        # so on a normal hour most orders arrive on (or before) time.
        # Actual = real prep (can run a bit long) + real travel + (busy hours only) a rider-shortage delay.
        # Result: ~95% on time off-peak, ~70% at peak, ~80% overall -> a real, explainable pattern.
        # Travel depends on the distance between the vendor and the customer's zone.
        d_km = self.zone_dist[(v["vendor_id"], c["zone"])]
        travel_mid = TRAVEL_BASE_MIN + TRAVEL_MIN_PER_KM * d_km
        eta = int(round(v["prep"] * 1.1 + travel_mid * 1.3)) + rng.randint(0, 5)   # the app pads its ETA a little
        prep_real = v["prep"] * rng.uniform(0.8, 1.3)
        travel_real = travel_mid * rng.uniform(0.75, 1.25)
        busy = local.hour in PEAK_HOURS[v["type"]]
        peak_delay = rng.uniform(3, 28) if busy and rng.random() < 0.35 else 0.0
        actual = prep_real + travel_real + peak_delay
        delivered = t + actual * 60 if status in ("delivered", "refunded") else None
        rating = None
        if status == "delivered" and rng.random() < 0.45:
            late = actual - eta
            if late <= 0:
                w = [4, 4, 10, 27, 55]          # on time -> mostly 4-5 stars
            elif late <= 15:
                w = [10, 10, 22, 30, 28]        # a bit late -> mixed
            else:
                w = [30, 20, 25, 15, 10]        # very late -> mostly low
            rating = rng.choices([1, 2, 3, 4, 5], weights=w)[0]
        zone = c["zone"]                                   # delivered to the customer's home zone
        platform = c["platform"] if rng.random() < 0.85 else rng.choice(["android", "ios", "web"])
        payment = rng.choices(["cash", "card", "wallet"], weights=[55, 30, 15])[0]
        row = {"order_id": oid, "customer_id": c["customer_id"], "vendor_id": v["vendor_id"],
               "order_ts": fmt_ts(t, "iso_utc"), "delivered_at": fmt_ts(delivered, "iso_utc") if delivered else None,
               "status": status, "cancel_reason": reason, "payment_method": payment,
               "promo_code": promo, "delivery_zone": zone, "subtotal": subtotal,
               "discount_amount": discount, "delivery_fee": fee, "service_fee": service, "total_amount": total,
               "estimated_delivery_min": eta, "customer_rating": rating}
        self._noise_order(row, t, oid, status, reason, payment, platform, zone, delivered)
        self.orders.write(row)
        self.stats["orders"] += 1
        self.stats["orders_" + status] += 1

        I = "customer_app/order_items.csv"
        for ln, (l, q, p, vp) in enumerate(lines, start=1):
            irow = {"order_id": oid, "line_number": ln, "item_ref": l.ref,
                    "item_name": l.clean_name, "quantity": q, "unit_price": p, "line_total": round(q * p, 2),
                    "vendor_confirmed_price": vp}
            ik = f"{oid}#{ln}"
            dirt.begin_row()
            alt_ref = ("P" + l.ref[4:]) if l.item_type == "grocery" else l.ref.lower()
            dirt.corrupt(irow, "item_ref", I, ik, [(REPR, "catalog/other id format used instead of item_ref", lambda _x, a=alt_ref: a),
                                                   (REPR, "id in lower case", lambda x: x.lower())])
            dirt.corrupt(irow, "item_name", I, ik, [dirt.miss(), (REPR, "name in upper case", lambda x: x.upper())])
            dirt.corrupt(irow, "quantity", I, ik, [(INVALID, "zero or negative quantity", lambda _q: rng.choice([0, -1])),
                                                   (WRONG_TYPE, "quantity with unit text", lambda x: f"{x} pcs")])
            dirt.corrupt(irow, "unit_price", I, ik, [(WRONG_TYPE, "decimal comma", lambda x: f"{x:.2f}".replace(".", ",")), dirt.miss()])
            dirt.corrupt(irow, "line_total", I, ik, [(UNIT, "amount in piasters", lambda x: int(round(x * 100)))], weight=0.4)
            dirt.corrupt(irow, "vendor_confirmed_price", I, ik, [(WRONG_TYPE, "decimal comma", lambda x: f"{x:.2f}".replace(".", ","))], weight=0.4)
            self.items.write(irow)

    def _noise_order(self, row, t, k, status, reason, payment, platform, zone, delivered):
        dirt, rng, cfg = self.dirt, self.rng, self.cfg
        O = "customer_app/orders.csv"
        dirt.begin_row()
        if self.bug_window[0] <= t < self.bug_window[1] and dirt.rate > 0:
            # buggy release 5.2.0: several columns broken in the SAME rows
            if rng.random() < 0.9:
                dirt.corrupt(row, "order_ts", O, k, [(DATE_FMT, "app release 5.2.0 bug: epoch milliseconds", lambda _x, tt=t: fmt_ts(tt, "epoch_ms"))], force=True)
            if rng.random() < 0.6:
                dirt.corrupt(row, "payment_method", O, k, [(REPR, "app release 5.2.0 bug: upper-case code", lambda x: x.upper())], force=True)
            if rng.random() < 0.4:
                dirt.corrupt(row, "service_fee", O, k, [(MISSING, "app release 5.2.0 bug: fee not saved", lambda _x: None)], force=True)
        dirt.corrupt(row, "order_ts", O, k, [
            (DATE_FMT, "timestamp in another format", lambda _x, tt=t: fmt_ts(tt, rng.choice(["epoch_ms", "dmy_local", "iso_local_tz", "local_naive", "mdy_local_12h"]))),
            (INVALID, "order timestamp in the future", lambda _x: fmt_ts(cfg.end + rng.randint(1, 365) * 86400, "iso_utc"))], weight=1.5)
        if delivered:
            dirt.corrupt(row, "delivered_at", O, k, [
                (INVALID, "delivered before the order was placed", lambda _x, tt=t: fmt_ts(tt - rng.randint(5, 120) * 60, "iso_utc")), dirt.miss()])
        dirt.corrupt(row, "status", O, k, dirt.variants(ORDER_STATUS[status], "order status label differs", canonical=status), weight=3)
        if reason:
            dirt.corrupt(row, "cancel_reason", O, k, dirt.variants(CANCEL_REASONS[reason], "cancel reason label differs", canonical=reason) + [dirt.miss()], weight=3)
        dirt.corrupt(row, "payment_method", O, k, dirt.variants(PAYMENT[payment], "payment label differs", canonical=payment), weight=2.5)
        dirt.corrupt(row, "vendor_id", O, k, [(REPR, "vendor id without prefix/padding", lambda x: str(int(x[1:]))),
                                              (REPR, "vendor id in another format", lambda x: f"v-{x[1:]}")])
        dirt.corrupt(row, "customer_id", O, k, [(INVALID, "customer_id not in customers (orphan FK)", lambda _x: f"CUS-9{rng.randint(10000, 99999)}")], weight=0.4)
        dirt.corrupt(row, "delivery_zone", O, k, [dirt.miss()] + dirt.variants(ZONE_VARIANTS.get(zone, []), "zone spelled differently", REPR), weight=2)
        dirt.corrupt(row, "subtotal", O, k, [(UNIT, "amount in piasters instead of EGP", lambda x: int(round(x * 100)))], weight=0.5)
        dirt.corrupt(row, "discount_amount", O, k, [(REPR, "discount stored as a negative number", lambda x: -x if x else x)], weight=0.5 if row["discount_amount"] else 0)
        dirt.corrupt(row, "total_amount", O, k, [
            (WRONG_TYPE, "amount as text with currency", lambda x: f"{x:.2f} EGP"),
            (INVALID, "negative total", lambda x: -x),
            (INVALID, "total does not match the components", lambda x: round(x + rng.choice([-1, 1]) * rng.uniform(5, 50), 2))])
        dirt.corrupt(row, "service_fee", O, k, [dirt.miss()], weight=0.5)
        dirt.corrupt(row, "estimated_delivery_min", O, k, [(WRONG_TYPE, "number with unit text", lambda x: f"{x} min")])
        if row["customer_rating"] is not None:
            dirt.corrupt(row, "customer_rating", O, k, [(INVALID, "rating outside 1-5", lambda _x: rng.choice([0, 6, 10])),
                                                        (WRONG_TYPE, "rating as text", lambda x: f"{x} stars")], weight=1.5)

    # ---------------------------------------------------------------- run ---
    def run(self):
        self._schedule()
        self._schedule_orders()
        total = len(self.heap)
        print(f"simulating ~{total:,} scheduled events ...", file=sys.stderr, flush=True)
        done = 0
        while self.heap:
            t, _s, kind, l, arg = heapq.heappop(self.heap)
            if t >= self.cfg.end:                  # follow-up events after the window are not simulated
                continue
            self.cdc.flush_until(t - 4 * 3600)
            getattr(self, "on_" + kind)(t, l, arg)
            done += 1
            if done % 250_000 == 0:
                print(f"  {done:,} events done, orders so far: {self.stats['orders']:,}", file=sys.stderr, flush=True)
        for l in self.listings:
            if l.listed:
                self.scd_close(l, None)
        self.cdc.close()
        for s in (self.wms, self.orders, self.items):
            s.close()
        self.scen_f.close()
        self.scd_f.close()


# =============================================================================
# 11. COLUMN MAPPING (the COLUMN_NAMING problem; seed for your business glossary)
# =============================================================================
COLUMN_MAPPING = [
    ("vendor", "vendor_id", "partners_system/vendors.csv", "VendorID", "V0012"),
    ("vendor", "vendor_id", "ops_db_cdc/vendor_items_events.csv", "store_id", "12 -> V0012"),
    ("vendor", "vendor_id", "wms/inventory_movements.csv", "warehouse_code", "WH-0012 -> V0012"),
    ("vendor", "vendor_id", "restaurant_portal/menu_items.csv", "vendor_id", "V0012"),
    ("vendor", "vendor_id", "customer_app/orders.csv", "vendor_id", "V0012, sometimes 12 or v-0012"),
    ("vendor", "integration_type", "partners_system/vendors.csv", "Integration_Type", "decides price unit and sync latency"),
    ("location", "zone", "partners_system/vendors.csv", "Area", "Area and delivery_zone are the same concept"),
    ("location", "zone", "customer_app/orders.csv", "delivery_zone", ""),
    ("location", "city", "partners_system/vendors.csv", "City", ""),
    ("location", "city", "customer_app/customers.csv", "city", ""),
    ("product", "product_id", "catalog_pim/products.csv", "product_id", "P00045"),
    ("product", "product_id", "catalog_pim/products.csv", "barcode", "EAN-13, the WMS key"),
    ("product", "product_id", "wms/inventory_movements.csv", "product_barcode", "barcode -> product_id"),
    ("item", "item_ref", "ops_db_cdc/vendor_items_events.csv", "item_ref", "SKU-00045 (=P00045) or M0000123"),
    ("item", "item_ref", "customer_app/order_items.csv", "item_ref", "SKU-00045 / M0000123, sometimes P00045"),
    ("item", "item_ref", "restaurant_portal/menu_items.csv", "menu_item_id", "M0000123"),
    ("item", "item_name", "catalog_pim/products.csv", "product_name", ""),
    ("item", "item_name", "restaurant_portal/menu_items.csv", "item_name", ""),
    ("item", "item_name", "ops_db_cdc/vendor_items_events.csv", "item_name", ""),
    ("price", "list_price_egp", "catalog_pim/products.csv", "base_price_egp", "reference price, NOT the selling price"),
    ("price", "list_price_egp", "restaurant_portal/menu_items.csv", "price_egp", "price at creation"),
    ("price", "selling_price_egp", "ops_db_cdc/vendor_items_events.csv", "item_price", "/100 for POS_LEGACY vendors"),
    ("price", "app_price_egp", "customer_app/order_items.csv", "unit_price", "what the app showed (may be stale)"),
    ("price", "vendor_price_egp", "customer_app/order_items.csv", "vendor_confirmed_price", "what the vendor really charged"),
    ("stock", "qty_on_hand", "ops_db_cdc/vendor_items_events.csv", "qty_on_hand", "null for menu items"),
    ("stock", "qty_change", "wms/inventory_movements.csv", "quantity", "signed"),
    ("stock", "is_available", "ops_db_cdc/vendor_items_events.csv", "is_available", ""),
    ("time", "event_time", "ops_db_cdc/vendor_items_events.csv", "updated_at", "LOCAL time, no tz"),
    ("time", "capture_time", "ops_db_cdc/vendor_items_events.csv", "captured_at_ms", "epoch ms UTC"),
    ("time", "event_time", "wms/inventory_movements.csv", "movement_ts", "LOCAL time dd/mm/yyyy"),
    ("time", "order_time", "customer_app/orders.csv", "order_ts", "UTC"),
    ("customer", "customer_id", "customer_app/customers.csv", "customer_id", "second accounts exist"),
    ("customer", "phone_e164", "customer_app/customers.csv", "phone", ""),
    ("order", "order_status", "customer_app/orders.csv", "status", "synonyms"),
]


# =============================================================================
# 12. MAIN
# =============================================================================
def parse_args():
    ap = argparse.ArgumentParser(description="Generate messy multi-source Talabat-style data.")
    ap.add_argument("--out", default="./qcommerce_data")
    ap.add_argument("--preset", choices=list(PRESETS), default="small")
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--start", default="2026-09-01", help="window start date (Cairo local midnight)")
    ap.add_argument("--days", type=int)
    ap.add_argument("--vendors", type=int)
    ap.add_argument("--customers", type=int)
    ap.add_argument("--orders", type=int)
    ap.add_argument("--dirty-rate", type=float, default=0.015)
    ap.add_argument("--anomaly-rate", type=float, default=1.0)
    a = ap.parse_args()
    p = PRESETS[a.preset]
    y, m, d = map(int, a.start.split("-"))
    return a.preset, Config(out_dir=a.out, seed=a.seed, start=local_to_ts(y, m, d),
                            days=a.days or p["days"], n_vendors=a.vendors or p["vendors"],
                            n_customers=a.customers or p["customers"], n_orders=a.orders or p["orders"],
                            dirty_rate=a.dirty_rate, anomaly_rate=a.anomaly_rate)


def count_lines(path):
    with open(path, "rb") as f:
        return sum(1 for _ in f)


def main():
    preset, cfg = parse_args()
    rng = random.Random(cfg.seed)
    o = cfg.out_dir
    dirt = DirtInjector(rng, cfg.dirty_rate, f"{o}/_answer_key/dq_manifest.csv")

    vendors = generate_vendors(cfg, rng, dirt)
    products = generate_products(cfg, rng, dirt)
    menu = generate_menu_items(cfg, rng, dirt, vendors)
    customers = generate_customers(cfg, rng, dirt)
    sim = Simulator(cfg, rng, dirt, vendors, products, menu, customers)
    sim.run()
    dirt.close()

    with open(f"{o}/_answer_key/column_mapping.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f, lineterminator="\n")
        w.writerow(["business_entity", "canonical_column", "source_file", "source_column", "note"])
        w.writerows(COLUMN_MAPPING)
    with open(f"{o}/_answer_key/data_dictionary.json", "w", encoding="utf-8") as f:
        json.dump({p: {**s, "columns": [dict(name=c[0], type=c[1], nullable=c[2], description=c[3]) for c in s["columns"]]}
                   for p, s in SCHEMAS.items()}, f, ensure_ascii=False, indent=2)

    files = list(SCHEMAS)
    st = sim.stats
    summary = {
        "preset": preset, "seed": cfg.seed, "dirty_rate": cfg.dirty_rate, "anomaly_rate": cfg.anomaly_rate,
        "window_utc": [fmt_ts(cfg.start, "iso_utc"), fmt_ts(cfg.end, "iso_utc")],
        "row_counts": {f: count_lines(f"{o}/{f}") - 1 for f in files},
        "cdc_ops": dict(sim.cdc.ops),
        "vendors_by_type": dict(Counter(v["type"] for v in vendors)),
        "vendors_by_integration": dict(Counter(v["integration"] for v in vendors)),
        "listings": len(sim.listings),
        "scenarios": {k.split(":", 1)[1]: n for k, n in sorted(st.items()) if k.startswith("scenario:")},
        "dq_gate": {"PASS": st["gate_pass"], "REJECT": st["gate_reject"]},
        "expected_scd2_rows": st["scd_versions"],
        "wms_movements": {k.split(":", 1)[1]: n for k, n in st.items() if k.startswith("wms:")},
        "orders": {k: n for k, n in st.items() if k.startswith("orders")},
        "kpis": {
            "orders_cancelled_because_app_showed_sold_out_item": st["orders_cancelled_stale_availability"],
            "share_of_orders_cancelled_stale_availability_pct": round(100 * st["orders_cancelled_stale_availability"] / max(1, st["orders"]), 2),
            "order_lines_with_stale_price": st["order_lines_stale_price"],
            "share_of_order_lines_with_stale_price_pct": round(100 * st["order_lines_stale_price"] / max(1, st["order_lines"]), 2),
        },
        "app_release_bug_window_utc": [fmt_ts(sim.bug_window[0], "iso_utc"), fmt_ts(sim.bug_window[1], "iso_utc")],
        "injected_noise_by_category": dict(dirt.by_cat),
        "injected_noise_by_file": dict(dirt.by_file),
        "injected_noise_total": dirt.count,
    }
    with open(f"{o}/_answer_key/generation_summary.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)
    print(json.dumps({k: summary[k] for k in ("row_counts", "cdc_ops", "scenarios", "dq_gate", "kpis",
                                              "injected_noise_by_category")}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
