"""
QA gate rules (pure functions, no Kafka): raw vendor_items row -> CleanItem, or Reject(reason).

Order of checks follows the reference gate of the data generator, so the result can be
compared with _answer_key/scenario_events.csv (see evaluate.py).
"""
import re
from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_UP
from typing import Optional
from zoneinfo import ZoneInfo

from pydantic import BaseModel

CAIRO = ZoneInfo("Africa/Cairo")
NULL_TOKENS = {"", "n/a", "null", "none", "nan", "-"}
TRUE_SET = {"true", "1", "y", "yes", "in_stock", "available"}
FALSE_SET = {"false", "0", "n", "no", "out_of_stock", "oos", "out-of-stock", "unavailable", "sold_out"}


class Reject(Exception):
    def __init__(self, reason: str):
        super().__init__(reason)
        self.reason = reason


class CleanItem(BaseModel):
    vendor_id: str                 # V0012
    item_ref: str                  # SKU-00045 | M0000123
    item_type: str                 # grocery | menu
    item_name: str
    price_egp: Decimal             # 2 decimals, always EGP
    is_available: bool
    qty_on_hand: Optional[int]     # None for menu items
    updated_by: Optional[str]
    event_time_utc: Optional[datetime]
    src_lsn: Optional[int]


class Reference:
    """Master data used by the gate: integration type per vendor + list price per item."""
    def __init__(self, integration: dict, list_price: dict):
        self.integration = integration      # {12: "POS_LEGACY", ...}
        self.list_price = list_price        # {"SKU-00045": 31.5, "M0000123": 55.0}


def is_null(v) -> bool:
    return v is None or (isinstance(v, str) and v.strip().lower() in NULL_TOKENS)


def parse_number(v) -> Optional[float]:
    """Lenient: accepts '44.75 EGP', '44,75', '12.0', 91, '91.0'."""
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


def parse_bool(v) -> Optional[bool]:
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        return bool(v) if v in (0, 1) else None
    if v is None:
        return None
    s = str(v).strip().lower()
    return True if s in TRUE_SET else False if s in FALSE_SET else None


def parse_time_utc(v) -> Optional[datetime]:
    """Business time in many formats -> aware UTC datetime.
    Naive values are LOCAL Cairo time; epoch values and 'Z' are UTC; offsets are honoured.
    dd/mm/yyyy HH:MM:SS is day-first; mm/dd/yyyy hh:mm AM|PM is month-first."""
    if is_null(v):
        return None
    s = str(v).strip()
    try:
        if re.fullmatch(r"\d{13}", s):                       # epoch milliseconds (UTC)
            return datetime.fromtimestamp(int(s) / 1000, tz=timezone.utc)
        if re.fullmatch(r"\d{10}", s):                       # epoch seconds (UTC)
            return datetime.fromtimestamp(int(s), tz=timezone.utc)
        m = re.fullmatch(r"(\d{2})/(\d{2})/(\d{4}) (\d{1,2}):(\d{2})(?::(\d{2}))?\s*([AaPp][Mm])?", s)
        if m:                                                # Cairo local; see the day/month rule below
            d, mo, y, hh, mi, ss, ap = m.groups()
            hh = int(hh)
            if ap:                                           # 12-hour clock files are US style: mm/dd/yyyy
                d, mo = mo, d
                hh = hh % 12 + (12 if ap.lower() == "pm" else 0)
            dt = datetime(int(y), int(mo), int(d), hh, int(mi), int(ss or 0), tzinfo=CAIRO)
            return dt.astimezone(timezone.utc)
        iso = s.replace("Z", "+00:00").replace(" ", "T", 1)
        dt = datetime.fromisoformat(iso)
        if dt.tzinfo is None:                                # naive = Cairo local
            dt = dt.replace(tzinfo=CAIRO)
        return dt.astimezone(timezone.utc)
    except ValueError:
        return None


def validate(raw: dict, ref: Reference) -> CleanItem:
    """raw: dict with the vendor_items columns (strings). Raises Reject(reason)."""
    store_id = int(raw["store_id"])
    item_ref = str(raw["item_ref"]).strip()
    item_type = "grocery" if item_ref.upper().startswith("SKU") else "menu"

    if is_null(raw.get("item_name")):
        raise Reject("NULL_ITEM_NAME")

    p = parse_number(raw.get("item_price"))
    if p is None:
        raise Reject("MISSING_OR_UNPARSEABLE_PRICE")
    if ref.integration.get(store_id) == "POS_LEGACY":
        p /= 100.0                                           # piasters -> EGP
    price = Decimal(str(round(p, 2))).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    if price <= 0:
        raise Reject("NON_POSITIVE_PRICE")

    lp = ref.list_price.get(item_ref)                        # master data, never learned from the stream
    if lp and (float(price) > 3 * lp or float(price) < 0.2 * lp):
        raise Reject("PRICE_OUTLIER")

    av = parse_bool(raw.get("is_available"))
    if av is None:
        raise Reject("INVALID_AVAILABILITY")

    qty = None
    if item_type == "grocery":
        q = parse_number(raw.get("qty_on_hand"))
        if q is None:
            raise Reject("MISSING_STOCK")
        if q < 0:
            raise Reject("NEGATIVE_STOCK")
        if (q > 0) != av:
            raise Reject("AVAILABILITY_CONFLICT")
        qty = int(q)

    lsn = raw.get("src_lsn", raw.get("lsn"))
    return CleanItem(
        vendor_id=f"V{store_id:04d}", item_ref=item_ref, item_type=item_type,
        item_name=str(raw["item_name"]).strip(), price_egp=price, is_available=av, qty_on_hand=qty,
        updated_by=(str(raw["updated_by"]).strip() or None) if raw.get("updated_by") else None,
        event_time_utc=parse_time_utc(raw.get("updated_at")),
        src_lsn=int(lsn) if lsn not in (None, "") else None)
