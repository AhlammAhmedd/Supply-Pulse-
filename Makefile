.PHONY: init up gen snapshot proof verify psql down reset

init:       ## create your .env from the template (first time only)
	@test -f .env || cp .env.example .env

up:         ## start postgres (WAL on)
	docker compose up -d postgres

gen:        ## generate the CSV data into ./data/qcommerce_data
	docker compose run --rm generator

snapshot:   ## load the initial rows (try a sample first: make snapshot ARGS="--limit 200")
	docker compose run --rm replayer snapshot $(ARGS)

proof:      ## print the evidence that step 1 worked (WAL, replica identity, publication, rows, sample)
	docker compose exec postgres psql -U postgres -d ops -f /proof.sql

verify:     ## compare postgres with the file (only after a FULL snapshot)
	docker compose run --rm replayer verify --stage snapshot

psql:       ## open a SQL shell on the database
	docker compose exec postgres psql -U postgres -d ops

down:       ## stop, keep data
	docker compose down

reset:      ## stop and DELETE the data (needed if you change init.sql)
	docker compose down -v
