# Git Bash on Windows rewrites paths like /connectors/x.json before Docker sees them; this stops that.
export MSYS_NO_PATHCONV=1

TOPIC ?= ops.public.vendor_items

.PHONY: init up gen seed snapshot proof connector status stream stream-fast verify check psql peek gate gate-logs evaluate airflow down reset

init:       ## create your .env from the template (first time only)
	@test -f .env || cp .env.example .env

up:         ## start postgres, kafka, debezium connect, kafka-ui
	docker compose up -d

gen:        ## generate the CSV data into ./data/qcommerce_data
	docker compose run --rm generator

seed:       ## 1a) load ALL the CSV tables (vendors, orders, customers...) into postgres
	docker compose run --rm --build replayer seed

snapshot:   ## 1b) load the op=r rows of vendor_items into postgres (before the connector exists)
	docker compose run --rm --build replayer snapshot

connector:  ## 2) register Debezium (it snapshots the table, then follows the WAL)
	docker compose exec connect curl -s -X POST -H "Content-Type: application/json" --data @/connectors/connector.json http://localhost:8083/connectors

status:     ## connector health (look for "RUNNING")
	docker compose exec connect curl -s http://localhost:8083/connectors/qcommerce-cdc/status

stream:     ## 3) replay inserts/updates/deletes (try: make stream ARGS="--limit 500 --rate 20")
	docker compose run --rm --build replayer stream $(ARGS)

proof:      ## evidence that postgres is ready (WAL, publication, rows per table, samples)
	docker compose exec postgres sh -c 'psql -U "$$POSTGRES_USER" -d "$$POSTGRES_DB" -f /proof.sql'

stream-fast: ## same as stream, as fast as possible (resume: make stream-fast ARGS="--skip 85000")
	docker compose run --rm --build replayer stream --rate 0 --batch 200 $(ARGS)

verify:     ## tables vs files, then which stage vendor_items is at (snapshot or final)
	docker compose run --rm --build replayer verify --stage seed
	docker compose run --rm --build replayer verify --stage auto

check:      ## run quick sanity SQL on postgres (counts, dirty values, CDC slot)
	docker compose exec postgres sh -c 'psql -U "$$POSTGRES_USER" -d "$$POSTGRES_DB" -f /checks.sql'

psql:       ## open a SQL shell on the source database
	docker compose exec postgres sh -c 'psql -U "$$POSTGRES_USER" -d "$$POSTGRES_DB"'

peek:       ## show 5 messages of a topic (try: make peek TOPIC=vendor_items.dlq)
	docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic $(TOPIC) --from-beginning --max-messages 5

gate:       ## start the QA gate (needs make gen first)
	docker compose --profile gate up -d --build qa-gate

gate-logs:  ## follow the QA gate counters
	docker compose logs -f --tail 20 qa-gate

evaluate:   ## test the gate rules against the answer key (no Kafka needed)
	docker compose run --rm --build --entrypoint python qa-gate evaluate.py

airflow:    ## start Airflow (UI http://localhost:8080, admin password is in the logs)
	docker compose --profile airflow up -d airflow

down:       ## stop everything, keep the data
	docker compose --profile gate --profile airflow down

reset:      ## stop everything and DELETE all data volumes
	docker compose --profile gate --profile airflow down -v
