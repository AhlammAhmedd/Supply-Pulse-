# Data-Pulse: q-commerce CDC pipeline

Branch: **`first-try`**

PostgreSQL is the single source system. Debezium reads its write-ahead log (WAL) and streams every table to
Kafka. A Python QA gate validates the live `vendor_items` changes and splits them into a valid topic and a
dead-letter (DLQ) topic. Everything runs in Docker.

```
data/*.csv --replayer--> PostgreSQL (WAL) --> Debezium --> Kafka (ops.<schema>.<table>)
                                                              |
                                                       QA gate (Python)
                                          vendor_items.valid <-+-> vendor_items.dlq
```

Next phases (not in this branch yet): Snowflake bronze -> dbt (SCD2 + star schema) -> Airflow -> dashboard.

---

## 1. Install (once)

| Tool | Notes |
|---|---|
| **Docker Desktop** (Windows / Mac) or Docker Engine + Compose plugin (Linux) | Windows: use the **WSL2** backend. Give Docker at least **8 GB RAM**. |
| **Git** | On Windows use **Git Bash**. |
| **make** | Optional. Without it, copy the command from [docs/runbook.md](docs/runbook.md). |

You do **not** need Python, Java, Kafka or dbt on your machine.

## 2. Get the code

```bash
git clone <repo-url>
cd Data-Pulse-Grad-Project
git checkout first-try
```

## 3. Configure

```bash
make init                       # creates .env from .env.example
```

Open `.env` and set `POSTGRES_PASSWORD`. Keep `POSTGRES_DB=qcommerce` (it must match
`database.dbname` in `infra/debezium/connector.json`). Change a port only if it is already taken on your machine.

**Windows / Git Bash:** the Makefile already sets `MSYS_NO_PATHCONV=1`. For commands you type yourself that
contain a Linux path (like `/opt/kafka/...`), run `export MSYS_NO_PATHCONV=1` once in that terminal.

## 4. Get the data

The `data/` folder is **not in git**. Everyone must use the same data, so generate it with the same settings:

```bash
docker compose run --rm generator python generate_qcommerce_data.py --out /data --preset full --seed 42
```

(`make gen` produces the smaller `small` preset, which gives different counts.) Check that `data/` now has the
folders `catalog_pim`, `customer_app`, `ops_db_cdc`, `partners_system`, `restaurant_portal`, `wms`, `_answer_key`.
Alternatively, a teammate can zip their `data/` folder and share it.

## 5. Run it, step by step

```bash
# 1. Start Postgres and wait until it is "healthy"
docker compose up -d postgres
docker compose ps

# 2. Load the data into Postgres
make seed                       # all CSV tables (takes about a minute for the full preset)
make snapshot                   # starting rows of vendor_items
make proof                      # WAL level, publication, rows per table
make verify                     # expect: every table OK, vendor_items at the SNAPSHOT stage

# 3. Start Kafka and Debezium, then register the connector (once)
docker compose up -d kafka connect kafka-ui
docker compose ps               # wait until kafka is "healthy"
make connector                  # then:
make status                     # expect RUNNING for the connector and its task

# 4. Start the QA gate BEFORE streaming
make gate
make gate-logs                  # Ctrl+C leaves the log view; the gate keeps running

# 5. Replay the changes
make stream-fast                # 283,639 events, prints progress and an estimate

# 6. Check the result
make verify                     # expect: vendor_items at the FINAL stage (12501 rows)
make evaluate                   # expect: RESULT: OK
```

Kafka UI: http://localhost:8088 (topics and messages).

## 6. Make it a live stream (demo)

So far the table topics carried a one-time snapshot, and `vendor_items` carried replayed changes. This adds a
**continuous stream of new orders**: a simulator keeps inserting orders into Postgres (then updates each one to
delivered / cancelled a few seconds later), and Debezium streams every insert and update through Kafka.

Use three terminals (all in the repo folder):

```bash
make live-monitor                    # A: watch the stream arrive in Kafka, with latency
make live ARGS="--rate 5"            # B: 5 new orders per second (Ctrl+C to stop; orders in progress are completed)
make stream ARGS="--rate 20"         # C (optional): live vendor_items changes, watched by the QA gate (make gate-logs)
```

Terminal A prints one line every 5 seconds, for example
`[12:03:05] last 5s: placed 24, delivered 21, cancelled 2, lines 58 | latency ms p50 40 p95 120 max 210`.
The latency is the time from the Postgres commit to a Kafka consumer. `no new events` means nothing is arriving:
check that `make live` is running and `make status` shows RUNNING.

Live orders have ids like `LIVE-<timestamp>-000001`. After a live run the order topics hold more messages than
the table above, which is expected. `make verify` still passes and shows `(+N live rows)`.

## 7. What you should see (full preset, seed 42)

**Postgres** (`make verify`): products 114, customers 42,910, order_items 582,733, orders 211,938,
vendors 323, menu_items 758, inventory_movements 164,248; `vendor_items` 12,192 rows after the snapshot and
12,501 after the full stream.

**Kafka** (message count per topic):

```bash
export MSYS_NO_PATHCONV=1
for t in ops.public.vendor_items ops.customer_app.orders ops.customer_app.order_items \
         ops.customer_app.customers ops.wms.inventory_movements ops.partners_system.vendors \
         ops.restaurant_portal.menu_items ops.catalog_pim.products vendor_items.valid vendor_items.dlq; do
  docker compose exec kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server kafka:9092 --topic $t
done
```

| Topic | Messages |
|---|---|
| ops.public.vendor_items | 295,831 |
| ops.customer_app.orders | 211,938 |
| ops.customer_app.order_items | 582,733 |
| ops.customer_app.customers | 42,910 |
| ops.wms.inventory_movements | 164,248 |
| ops.partners_system.vendors | 323 |
| ops.restaurant_portal.menu_items | 758 |
| ops.catalog_pim.products | 114 |
| vendor_items.valid | 294,084 |
| vendor_items.dlq | 1,747 |

The seven table topics equal the Postgres row counts. `valid + dlq = 295,831`.

**QA gate** reject reasons after the full stream: NEGATIVE_STOCK 922, PRICE_OUTLIER 329, NON_POSITIVE_PRICE 325,
NULL_ITEM_NAME 171. `make evaluate` compares the gate with the answer key: 294,082 correct passes, 1,747 correct
rejects, 2 known misses (price outliers with no list price in the master data).

## 8. Day to day

| Command | Containers | Data |
|---|---|---|
| `docker compose stop` / `make down` | stopped / removed | **kept** |
| `make reset` (`down -v`) | removed | **deleted** (Postgres and Kafka) |

After a stop, run `docker compose up -d` and `make gate`. The data and the Debezium connector are kept, so do
**not** seed or register again. Never run `make seed` or `make snapshot` after the connector exists: the rows
would be sent to Kafka a second time. To start over, use `make reset` and repeat section 5.

## 9. Troubleshooting

| Problem | Fix |
|---|---|
| `pip ... Name or service not known` while building | Docker DNS: add `"dns": ["8.8.8.8","1.1.1.1"]` in Docker Desktop > Settings > Docker Engine, apply and restart; or add `network: host` under `build:` for that service. |
| `curl: error encountered when reading a file` / paths like `C:/Program Files/Git/...` | Git Bash rewrote a path. `export MSYS_NO_PATHCONV=1`, or use `make`. |
| `make connector` says `409 already exists` | Normal after a restart. Run `make status`. |
| `permission denied for schema ...` in the Connect log | Run `make seed` (it grants read access to `cdc_user`), then `docker compose exec connect curl -s -X POST "http://localhost:8083/connectors/qcommerce-cdc/restart?includeTasks=true"`. |
| `make verify`: "matches neither stage" | The stream is partly applied. Finish it: `make stream-fast ARGS="--skip <events already applied>"`, or `make reset` and start over. |
| `make live-monitor` prints `no new events` | Start `make live` in another terminal and check `make status`. The first report can take a few seconds. |
| `make stream-fast` seems slow | It prints speed and time left every 5,000 events. For a quick test: `make stream-fast ARGS="--limit 20000"`. |
| `port is already allocated` | Change that port in your `.env`. |
| `no matching manifest for linux/arm64` (Apple Silicon) | Add `platform: linux/amd64` to that service in `docker-compose.yml`. |
| Containers killed or restarting | Raise Docker memory to 8 GB or more. |
| `^M` or "missing separator" errors | CRLF line endings. VS Code: set `files.eol` to `\n` (done in `.vscode/settings.json`), then `git add --renormalize .`. |
| Want a clean start | `make reset`. Needed after any change to `infra/postgres/init.sql`. |

## 10. Team rules

- **Never commit** `.env`, `data/`, keys or passwords. Only `.env.example` is committed.
- Line endings are LF (`.gitattributes`).
- Before a pull request: `make evaluate` and `docker compose config -q`. CI runs the same checks.
- Replay speed is for development: use `--limit` for quick tests, and the full stream only for demos.

## 11. Project layout

```
docker-compose.yml  Makefile  .env.example
infra/postgres/     init.sql (schemas, vendor_items, WAL publication), proof.sql, checks.sql
infra/debezium/     connector.json
services/replayer/  seed / snapshot / stream / verify
services/qa_gate/   Kafka consumer, Pydantic rules, evaluate.py, live_monitor.py
services/simulator/ live_orders.py (new orders streamed into Postgres)
airflow/dags/       cdc_bootstrap.py (connector health check)
scripts/data_generator/  generate_qcommerce_data.py
docs/               architecture.md, runbook.md
```
