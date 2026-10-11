# Runbook: every make target and the Docker command behind it

Without `make` (for example plain Windows), run the command from the right-hand column.
In Git Bash, put `MSYS_NO_PATHCONV=1` in front of commands that contain a path like `/proof.sql`.

# Q-commerce CDC platform

Only Docker is required. `make` is optional: every target is just a shortcut for a
`docker compose` command (table below), so on Windows without `make` copy the command.

## Run order

| Step | make target | What it does |
|---|---|---|
| 1 | `make up` | starts postgres, kafka, debezium connect, kafka-ui (http://localhost:8088) |
| 2 | `make gen` | generates the CSV data into `./data/qcommerce_data` |
| 3a | `make seed` | loads ALL csv tables into postgres (BEFORE the connector) |
| 3b | `make snapshot` | loads the `op=r` rows of vendor_items into postgres (BEFORE the connector) |
| 4 | `make connector` | registers Debezium: it snapshots the table, then follows the WAL |
| 5 | `make status` | connector health, expect `RUNNING` |
| 6 | `make gate` | starts the QA gate (valid -> `vendor_items.valid`, rejected -> `vendor_items.dlq`) |
| 7 | `make stream ARGS="--limit 500 --rate 20"` | replays inserts/updates/deletes into postgres |

## Check that data really arrived

| Where | Command | What to look for |
|---|---|---|
| Postgres | `make check` | rows > 0, both grocery and menu items, dirty values kept |
| Postgres vs file | `make verify ARGS="--stage snapshot"` (after step 3) or `make verify` (after a full stream) | `VERIFY OK` |
| Postgres by hand | `make psql`, then `SELECT count(*) FROM vendor_items;` | |
| Kafka | `make peek` | change events with `op` r / c / u / d |
| Kafka UI | http://localhost:8088 -> Topics | `ops.public.vendor_items`, `vendor_items.valid`, `vendor_items.dlq` |
| QA gate | `make gate-logs` | `routed:` counters and the DLQ reasons |
| QA gate rules | `make evaluate` | matches the answer key (PASS/REJECT) |

## What each make target runs

| make target | docker command behind it |
|---|---|
| up | `docker compose up -d` |
| gen | `docker compose run --rm generator` |
| seed | `docker compose run --rm replayer seed` |
| snapshot | `docker compose run --rm replayer snapshot` |
| proof | `docker compose exec postgres psql -U postgres -d ops -f /proof.sql` |
| connector | `docker compose exec connect curl -s -X POST -H "Content-Type: application/json" --data @/connectors/connector.json http://localhost:8083/connectors` |
| status | `docker compose exec connect curl -s http://localhost:8083/connectors/qcommerce-cdc/status` |
| stream | `docker compose run --rm replayer stream` (+ `--limit`, `--rate`) |
| verify | `docker compose run --rm replayer verify` |
| check | `docker compose exec postgres psql -U postgres -d ops -f /checks.sql` |
| psql | `docker compose exec postgres psql -U postgres -d ops` |
| peek | `docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic ops.public.vendor_items --from-beginning --max-messages 5` |
| gate | `docker compose --profile gate up -d --build qa-gate` |
| gate-logs | `docker compose logs -f --tail 20 qa-gate` |
| evaluate | `docker compose run --rm --build --entrypoint python qa-gate evaluate.py` |
| airflow | `docker compose --profile airflow up -d airflow` |
| down | `docker compose --profile gate --profile airflow down` |
| reset | same as down, plus `-v` (deletes all data) |

Topic `ops.public.vendor_items`: Debezium envelope `{before, after, op, ts_ms, source}`.
`op`: r = snapshot, c = insert, u = update, d = delete.
Source columns are TEXT on purpose (raw, dirty); the QA gate cleans or rejects.
