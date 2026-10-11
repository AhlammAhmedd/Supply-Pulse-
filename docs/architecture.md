# Architecture

```
CSV files (data/)                                        one source of truth: PostgreSQL
      |  replayer seed / snapshot / stream                         |
      v                                                            v
PostgreSQL (wal_level=logical) --WAL--> Debezium (Kafka Connect) --> Kafka
 schemas: public, partners_system, catalog_pim,                      ops.<schema>.<table>
 restaurant_portal, customer_app, wms                                      |
                                                                           v
                                                          QA gate (Python + Pydantic)
                                             vendor_items.valid <----+----> vendor_items.dlq
                                                    |                              |
                                                    v                              v
                                             bronze -> dbt (staging, SCD2, marts) -> dashboard
Airflow supervises the connector and runs dbt on a schedule.
```

## Decisions
- **Postgres holds all data.** Debezium's initial snapshot sends existing rows (`op = r`); later changes arrive as `c / u / d`. Raw files are never loaded into the warehouse directly.
- **Raw TEXT columns** in the source tables keep the injected dirty values; the QA gate and dbt clean them.
- **`REPLICA IDENTITY FULL`** on `vendor_items` so updates and deletes carry the old row.
- **QA gate** validates only `vendor_items` (price, stock, availability, time); it is checked against `data/_answer_key/scenario_events.csv` by `make evaluate`.
- **SCD2** for vendor item prices and availability is built from the event history with window functions (not a dbt snapshot), and checked against `data/_answer_key/expected_scd2_vendor_items.csv`.

## Topics
| Topic | Content |
|---|---|
| `ops.<schema>.<table>` | Debezium envelope `{before, after, op, ts_ms, source}` |
| `vendor_items.valid` | clean, typed events (EGP, UTC) and deletes |
| `vendor_items.dlq` | rejected events with `reject_reason` and the raw row |
