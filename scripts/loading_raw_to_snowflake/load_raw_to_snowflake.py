"""
load_raw_to_snowflake.py - copies the 8 source CSVs into SUPPLY_CHAIN_DB.RAW, untouched.

What it does for every CSV (same discovery rule as services/replayer/replayer.py):
  1. reads the header and turns each column name into lower_snake_case, exactly like the
     replayer does for Postgres (VendorID -> vendorid, Vendor_Name -> vendor_name).
     Snowflake stores unquoted names in UPPER case, so dbt sees VENDORID, VENDOR_NAME ...
  2. CREATE OR REPLACE TABLE RAW.<file name> with every column VARCHAR
     (raw = no types, no cleaning: "44,75", "N/A", "3 pcs" must survive so dbt can fix them)
  3. PUT  : uploads the file to the table's own stage (@%TABLE), gzip-compressed
  4. COPY : loads it; empty cells become NULL, text such as 'null' / 'N/A' stays as text
  5. compares the row count in Snowflake with the number of records in the file

Usage (PowerShell, from the repo root, venv active):
    python scripts/load_raw_to_snowflake.py --dry-run          # no Snowflake: shows tables + columns
    python scripts/load_raw_to_snowflake.py                    # load all 8 files
    python scripts/load_raw_to_snowflake.py --only products menu_items order_items orders
    python scripts/load_raw_to_snowflake.py --account shared   # ONLY the one person who loads the shared account

Connection settings come from the .env file (see .env.example). Nothing secret is in this file.
"""
import argparse
import csv
import glob
import os
import re
import sys
from pathlib import Path

csv.field_size_limit(10_000_000)          # some text cells are long; the default limit is 128 KB

SKIP_DIRS = {"_answer_key"}               # the answer key is NOT a source system: never load it


def ident(name: str) -> str:
    """Same rule as replayer.ident(): non-alphanumerics -> '_', lower case, no leading digit."""
    s = re.sub(r"[^0-9a-zA-Z]+", "_", name.strip()).strip("_").lower()
    return "c_" + s if s[:1].isdigit() else s


def find_root(data_dir: str) -> str:
    """Accepts ./data (source folders directly inside) or ./data/qcommerce_data."""
    for cand in (data_dir, os.path.join(data_dir, "qcommerce_data")):
        if os.path.isdir(os.path.join(cand, "ops_db_cdc")):
            return cand
    sys.exit(f"cannot find the source folders (ops_db_cdc, customer_app, ...) under {data_dir}")


def source_files(root: str):
    """Yields (table_name, path) for every CSV in every source folder."""
    for d in sorted(os.listdir(root)):
        full = os.path.join(root, d)
        if d.startswith((".", "_")) or d in SKIP_DIRS or not os.path.isdir(full):
            continue
        for f in sorted(glob.glob(os.path.join(full, "*.csv"))):
            yield ident(Path(f).stem), f


def read_header_and_count(path: str):
    """Header columns + number of RECORDS (a quoted cell may contain a newline, so we do not count lines)."""
    with open(path, newline="", encoding="utf-8-sig") as f:   # utf-8-sig also strips a BOM if present
        r = csv.reader(f)
        header = next(r)
        n = sum(1 for _ in r)
    return header, n


def connect(prefix: str):
    """prefix '' = personal account (SNOWFLAKE_*), 'SHARED_' = shared account (SHARED_SNOWFLAKE_*)."""
    import snowflake.connector
    from dotenv import load_dotenv
    load_dotenv()                                           # reads .env in the current folder
    env = lambda k, d=None: os.environ.get(prefix + k, d)
    if not env("SNOWFLAKE_ACCOUNT"):
        sys.exit(f"{prefix}SNOWFLAKE_ACCOUNT is not set (check your .env)")
    params = dict(
        account=env("SNOWFLAKE_ACCOUNT"),
        user=env("SNOWFLAKE_USER"),
        password=env("SNOWFLAKE_PASSWORD"),
        role=env("SNOWFLAKE_ROLE", "SYSADMIN"),
        warehouse=env("SNOWFLAKE_WAREHOUSE", "QC_WH"),
        database=env("SNOWFLAKE_DATABASE", "SUPPLY_CHAIN_DB"),
        schema=env("SNOWFLAKE_RAW_SCHEMA", "RAW"),
    )
    auth = (env("SNOWFLAKE_AUTHENTICATOR", "") or "").strip()
    if auth and auth.lower() != "snowflake":                # e.g. username_password_mfa (MFA push)
        params["authenticator"] = auth
        params["client_request_mfa_token"] = True           # ask once, reuse the token if allowed
    return snowflake.connector.connect(**params)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--data-dir", default=os.environ.get("DATA_DIR", "data"))
    ap.add_argument("--only", nargs="*", help="table names to load, e.g. products order_items")
    ap.add_argument("--dry-run", action="store_true", help="print the plan, do not connect")
    ap.add_argument("--account", choices=["personal", "shared"], default="personal",
                    help="which credentials from .env to use (shared = the SHARED_* variables)")
    a = ap.parse_args()

    root = find_root(a.data_dir)
    files = [(t, p) for t, p in source_files(root) if not a.only or t in a.only]
    if not files:
        sys.exit("no CSV matched")

    conn = None if a.dry_run else connect("SHARED_" if a.account == "shared" else "")
    cur = conn.cursor() if conn else None
    ok = True
    for table, path in files:
        header, n = read_header_and_count(path)
        cols = [ident(h).upper() for h in header]
        if len(set(cols)) != len(cols):
            sys.exit(f"duplicate column names after cleaning in {path}: {cols}")
        T = table.upper()
        print(f"\n{T}  <- {path}\n  {n:,} records | columns: {', '.join(cols)}")
        if a.dry_run:
            continue

        cur.execute(f"CREATE OR REPLACE TABLE {T} (" + ", ".join(f"{c} VARCHAR" for c in cols) + ")")
        uri = Path(path).resolve().as_posix()               # Windows: C:/Users/... (forward slashes)
        cur.execute(f"PUT 'file://{uri}' @%{T} AUTO_COMPRESS = TRUE OVERWRITE = TRUE")
        cur.execute(f"""
            COPY INTO {T}
            FROM @%{T}
            FILE_FORMAT = (
                TYPE = CSV
                SKIP_HEADER = 1
                FIELD_OPTIONALLY_ENCLOSED_BY = '"'
                EMPTY_FIELD_AS_NULL = TRUE   -- empty cell        -> NULL
                NULL_IF = ()                 -- 'null', 'N/A' ... -> kept as TEXT (dbt cleans them)
                ENCODING = 'UTF8'            -- Arabic values
                ESCAPE_UNENCLOSED_FIELD = NONE  -- a backslash is just a character
            )
            ON_ERROR = ABORT_STATEMENT       -- any broken row stops the load: nothing half-loaded
            PURGE = TRUE                     -- delete the uploaded file from the stage afterwards
        """)
        got = cur.execute(f"SELECT COUNT(*) FROM {T}").fetchone()[0]
        same = got == n
        ok &= same
        print(f"  file={n:,}  snowflake={got:,}  {'OK' if same else 'MISMATCH'}")

    if conn:
        conn.close()
        print("\nALL OK" if ok else "\nSOME TABLES DO NOT MATCH")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
