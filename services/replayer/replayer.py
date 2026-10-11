"""
Loads ALL source data into PostgreSQL (the single source system) and replays the CDC file,
so that Debezium captures everything from the write-ahead log.

  seed     : create one raw table per CSV (schema = folder name, table = file name, all columns
             TEXT, plus a row_id key) and COPY the file in. Covers vendors, products, menu items,
             customers, orders, order items and inventory movements. Run it BEFORE the connector,
             so Debezium's initial snapshot carries these rows to Kafka (op = r).
  snapshot : load all op='r' rows in one go.  Run BEFORE registering the connector,
             so Debezium's initial snapshot reads them (they appear as op='r').
  stream   : replay op = c / u / d in file order, one transaction per event.
             Run AFTER the connector is registered (they appear as op = c / u / d).
  verify   : compare the table in Postgres with the state the file should produce
             (--stage snapshot after `snapshot`, --stage all after a full `stream`).

Notes
  * Values are loaded RAW (text). Empty strings become NULL. Nothing is cleaned here.
  * Exact repeated lsn rows (the DUPLICATE_RECORD noise) are skipped: a real database
    cannot hold the same change twice, so the duplicate never reaches the WAL.
  * Debezium's own LSN (in source.lsn) is Postgres' LSN, not the file's. The file's lsn
    is kept in the src_lsn column to match _answer_key/scenario_events.csv.
"""
import argparse
import csv
import glob
import os
import re
import sys
import time

COLS = ["src_lsn", "store_id", "item_ref", "item_name", "item_price", "currency",
        "qty_on_hand", "is_available", "updated_by", "updated_at"]

UPSERT = f"""
INSERT INTO public.vendor_items ({", ".join(COLS)})
VALUES ({", ".join(["%s"] * len(COLS))})
ON CONFLICT (store_id, item_ref) DO UPDATE SET
  src_lsn = EXCLUDED.src_lsn, item_name = EXCLUDED.item_name,
  item_price = EXCLUDED.item_price, currency = EXCLUDED.currency,
  qty_on_hand = EXCLUDED.qty_on_hand, is_available = EXCLUDED.is_available,
  updated_by = EXCLUDED.updated_by, updated_at = EXCLUDED.updated_at
"""
DELETE = "DELETE FROM public.vendor_items WHERE store_id = %s AND item_ref = %s"


def clean(v):
    if v is None:
        return None
    v = v.strip()
    return v or None


def load_events(path):
    events, seen, dupes = [], set(), 0
    with open(path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            row = {k.strip().lower(): v for k, v in row.items()}
            lsn = row["lsn"].strip()
            if lsn in seen:
                dupes += 1
                continue
            seen.add(lsn)
            events.append(row)
    return events, dupes


def params(e):
    return (int(e["lsn"]), int(e["store_id"]), e["item_ref"].strip(), clean(e["item_name"]),
            clean(e["item_price"]), clean(e["currency"]), clean(e["qty_on_hand"]),
            clean(e["is_available"]), clean(e["updated_by"]), clean(e["updated_at"]))


SKIP_DIRS = {"ops_db_cdc"}     # the change log is replayed into public.vendor_items, not seeded


def find_root(data_dir):
    """Accepts ./data (folders directly inside) or ./data/qcommerce_data."""
    for cand in (data_dir, os.path.join(data_dir, "qcommerce_data")):
        if os.path.isdir(os.path.join(cand, "ops_db_cdc")):
            return cand
    sys.exit(f"cannot find the ops_db_cdc folder under {data_dir}")


def ident(name):
    s = re.sub(r"[^0-9a-zA-Z]+", "_", name.strip()).strip("_").lower()
    return "c_" + s if s[:1].isdigit() else s


def source_files(root):
    """(schema, table, path) for every CSV to seed: folder -> schema, file -> table."""
    for d in sorted(os.listdir(root)):
        full = os.path.join(root, d)
        if d.startswith(("_", ".")) or d in SKIP_DIRS or not os.path.isdir(full):
            continue
        for f in sorted(glob.glob(os.path.join(full, "*.csv"))):
            yield ident(d), ident(os.path.splitext(os.path.basename(f))[0]), f


def count_records(path):
    with open(path, newline="", encoding="utf-8") as f:
        r = csv.reader(f)
        next(r)
        return sum(1 for _ in r)


def seed(root, dry_run):
    conn = None if dry_run else connect()
    for schema, table, path in source_files(root):
        with open(path, newline="", encoding="utf-8") as f:
            header = next(csv.reader(f))
        cols = [ident(h) for h in header]
        if len(set(cols)) != len(cols):
            sys.exit(f"duplicate column names after cleaning in {path}: {cols}")
        n = count_records(path)
        if dry_run:
            print(f"{schema}.{table}: {n} rows, columns: {', '.join(cols)}")
            continue
        collist = ", ".join(f'"{c}"' for c in cols)
        with conn.cursor() as cur:
            cur.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')
            cur.execute(f'CREATE TABLE IF NOT EXISTS "{schema}"."{table}" '
                        f'(row_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY, '
                        + ", ".join(f'"{c}" text' for c in cols) + ")")
            cur.execute(f'TRUNCATE "{schema}"."{table}" RESTART IDENTITY')   # safe to re-run
            with open(path, encoding="utf-8", newline="") as f:
                with cur.copy(f'COPY "{schema}"."{table}" ({collist}) FROM STDIN WITH (FORMAT csv, HEADER true)') as cp:
                    while chunk := f.read(1 << 20):
                        cp.write(chunk)
        conn.commit()
        print(f"{schema}.{table}: {n} rows loaded", flush=True)
    if not dry_run:
        print("seed done. Next: load vendor_items (snapshot), then register the connector.")


def verify_seed(root):
    ok = True
    with connect().cursor() as cur:
        for schema, table, path in source_files(root):
            n = count_records(path)
            cur.execute(f'SELECT count(*) FROM "{schema}"."{table}"')
            got = cur.fetchone()[0]
            ok &= (n == got)
            print(f"{schema}.{table:<20} file={n:>7}  postgres={got:>7}  {'OK' if n == got else 'MISMATCH'}")
    print("VERIFY OK: every table matches its file" if ok else "VERIFY FAILED")
    return 0 if ok else 1


def connect():
    import psycopg
    return psycopg.connect(
        host=os.environ.get("PGHOST", "postgres"), port=os.environ.get("PGPORT", "5432"),
        dbname=os.environ.get("PGDATABASE", "ops"), user=os.environ.get("PGUSER", "postgres"),
        password=os.environ.get("PGPASSWORD", "postgres"), autocommit=False)


def verify(events, stage):
    """Computes the table state the file SHOULD produce and compares it with Postgres."""
    expected = {}
    for e in events:                                   # same order the replayer applies them
        if e["op"].strip() == "r":
            expected[(int(e["store_id"]), e["item_ref"].strip())] = int(e["lsn"])
    if stage == "all":
        for e in events:
            op = e["op"].strip()
            key = (int(e["store_id"]), e["item_ref"].strip())
            if op == "d":
                expected.pop(key, None)
            elif op in ("c", "u"):
                expected[key] = int(e["lsn"])
    with connect().cursor() as cur:
        cur.execute("SELECT store_id, item_ref, src_lsn FROM public.vendor_items")
        actual = {(r[0], r[1]): r[2] for r in cur.fetchall()}
    missing = expected.keys() - actual.keys()
    extra = actual.keys() - expected.keys()
    stale = [k for k in expected.keys() & actual.keys() if expected[k] != actual[k]]
    print(f"expected rows: {len(expected)}   rows in postgres: {len(actual)}")
    print(f"missing: {len(missing)}   unexpected: {len(extra)}   not at latest version: {len(stale)}")
    ok = not (missing or extra or stale)
    print("VERIFY OK: postgres matches the file" if ok else "VERIFY FAILED")
    return 0 if ok else 1


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("mode", choices=["seed", "snapshot", "stream", "verify"])
    ap.add_argument("--data-dir", default=os.environ.get("DATA_DIR", "/data"))
    ap.add_argument("--file", default=None, help="CDC events file (default: <data>/ops_db_cdc/vendor_items_events.csv)")
    ap.add_argument("--rate", type=float, default=200.0, help="events/second in stream mode (0 = no limit)")
    ap.add_argument("--limit", type=int, default=0, help="stop after N events (0 = all)")
    ap.add_argument("--stage", choices=["seed", "snapshot", "all"], default="all",
                    help="verify only: seed = the CSV tables, snapshot = vendor_items after the snapshot, all = after the full stream")
    ap.add_argument("--dry-run", action="store_true", help="read and count only, no database")
    a = ap.parse_args()

    root = find_root(a.data_dir)
    if a.file is None:
        a.file = os.path.join(root, "ops_db_cdc", "vendor_items_events.csv")
    if a.mode == "seed":
        return seed(root, a.dry_run)
    if a.mode == "verify" and a.stage == "seed":
        return verify_seed(root)
    events, dupes = load_events(a.file)
    if a.mode == "verify":
        return verify(events, a.stage)
    if a.mode == "snapshot":
        todo = [e for e in events if e["op"].strip() == "r"]
    else:
        todo = [e for e in events if e["op"].strip() in ("c", "u", "d")]
    if a.limit:
        todo = todo[: a.limit]
    print(f"{a.mode}: {len(todo)} events to apply ({dupes} duplicate lsn rows skipped)", flush=True)
    if a.dry_run:
        return

    conn = connect()

    if a.mode == "snapshot":
        with conn.cursor() as cur:
            cur.executemany(UPSERT, [params(e) for e in todo])
        conn.commit()
        print("snapshot rows loaded. Now register the connector (make connector).")
        return

    conn.autocommit = True   # one transaction per event = one change event each
    delay = 1.0 / a.rate if a.rate else 0.0
    counts = {"c": 0, "u": 0, "d": 0}
    with conn.cursor() as cur:
        for i, e in enumerate(todo, 1):
            op = e["op"].strip()
            if op == "d":
                cur.execute(DELETE, (int(e["store_id"]), e["item_ref"].strip()))
            else:                      # c and u are both an upsert
                cur.execute(UPSERT, params(e))
            counts[op] += 1
            if i % 1000 == 0:
                print(f"  {i}/{len(todo)} applied", flush=True)
            if delay:
                time.sleep(delay)
    print("stream done:", counts)


if __name__ == "__main__":
    sys.exit(main())
