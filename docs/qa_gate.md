# QA gate: vendor_items

Scope: only the `ops.public.vendor_items` change stream. Other tables and pipelines are untouched.

```
ops.public.vendor_items --> normalise --> field checks --> history checks --+--> vendor_items.valid
   (r / c / u / d)          (rules.py)    (rules.py)       (state.py)       +--> vendor_items.dlq
                                                                            +--> vendor_items.quarantine
```

Every event goes to exactly one topic. Only **accepted** events change the item state, so a rejected,
quarantined, duplicate or older event can never overwrite a newer valid version.

## Event types

| Debezium `op` | `event_type` | Handling |
|---|---|---|
| `r` | snapshot | validated like an update; older than the current state -> `STALE_EVENT` |
| `c` | insert | validated; an insert after a delete re-lists the item (`relisted: true`, no price-jump check) |
| `u` | update | validated; compared with the last accepted version |
| `d` | delete | accepted when the key is readable (`MISSING_KEY` otherwise); a second delete -> `DUPLICATE_DELETE`; later older updates stay stale |
| other (`t`, ...) without `after` | | ignored |

## Normalisation (accepted events)

`vendor_id` `V0012` (accepts `12`, `V0012`, `v-0012`), `item_ref` upper-case `SKU-00045` / `M0000123`,
`item_name` trimmed with single spaces, `price_egp` 2 decimals (POS_LEGACY piasters / 100), `currency` always
`EGP` (`LE`, `E£`, `جنيه`, `PT`, ... map to it; an empty value is assumed EGP and flagged `currency_assumed`),
`is_available` boolean, `qty_on_hand` integer (null for menu items), `event_time_utc` (naive source times are
Cairo time), plus `event_type`, `op`, `cdc_ts_ms`, `pg_lsn`.

## Reject reasons (DLQ)

| Reason | Check |
|---|---|
| `MISSING_VENDOR_ID`, `INVALID_VENDOR_ID`, `UNKNOWN_VENDOR` | vendor id present, numeric, in `vendors.csv` |
| `MISSING_ITEM_REF`, `INVALID_ITEM_REF` | `SKU-nnnnn` or `Mnnnnnnn` |
| `UNKNOWN_PRODUCT` | SKU is not in `catalog_pim/products.csv` |
| `UNKNOWN_MENU_ITEM`, `ITEM_VENDOR_MISMATCH` | menu item is in `menu_items.csv` and belongs to this vendor |
| `NULL_ITEM_NAME` | name present |
| `MISSING_OR_UNPARSEABLE_PRICE`, `NON_POSITIVE_PRICE`, `PRICE_OUTLIER` | price parses, > 0, within 0.2x to 3x of the list price |
| `UNSUPPORTED_CURRENCY` | not an EGP spelling |
| `INVALID_AVAILABILITY` | flag parses |
| `MISSING_STOCK`, `NEGATIVE_STOCK`, `NON_INTEGER_STOCK`, `AVAILABILITY_CONFLICT` | groceries: integer stock >= 0, consistent with the flag |
| `MISSING_TIMESTAMP`, `INVALID_TIMESTAMP`, `TIMESTAMP_OUT_OF_RANGE`, `FUTURE_TIMESTAMP` | `updated_at` parses and is plausible |
| `INVALID_LSN` | `src_lsn` is an integer |
| `DUPLICATE_EVENT` | the same `src_lsn` was already accepted for this item (Kafka redelivery) |
| `STALE_EVENT` | event time is older than the last accepted version |
| `OUT_OF_ORDER_EVENT` | same instant, but a lower `src_lsn` than the accepted version |
| `DUPLICATE_DELETE`, `MISSING_KEY` | deletes |
| `MALFORMED_EVENT`, `NOT_JSON`, `MISSING_AFTER_IMAGE`, `UNSUPPORTED_OP` | the event itself is unreadable |

A DLQ message is `{reject_reason, detail?, item?, raw, op, event_type, cdc_ts_ms, pg_lsn}`. `raw` is always the
original row. Events that failed only a history check also carry `item`, the clean version, so they can be replayed.

## Quarantine

A row that passed every field check but moves the price more than the thresholds from the **last accepted**
price goes to `vendor_items.quarantine` with `quarantine_reason: SUSPICIOUS_PRICE_CHANGE` and the evidence
(`previous_price_egp`, `new_price_egp`, `ratio`, `confirmations`). It does not change the baseline, so the next
normal price is still compared with the last good one. If the same new price arrives `QUARANTINE_CONFIRM_COUNT`
times in a row (within 2 %), it is treated as a real repricing: accepted, flagged `quarantine_released`, and it
becomes the new baseline.

## Configuration (environment of the `qa-gate` service; override in `.env`)

| Variable | Default | Meaning |
|---|---|---|
| `PRICE_MAX_INCREASE_RATIO` | 1.6 | new / old above this is quarantined |
| `PRICE_MIN_DECREASE_RATIO` | 0.6 | new / old below this is quarantined |
| `PRICE_JUMP_MIN_ABS_EGP` | 0.5 | ignore smaller absolute changes |
| `QUARANTINE_CONFIRM_COUNT` | 3 | agreeing events that release a quarantine (0 = never) |
| `STALE_ACTION` | `dlq` | `dlq`, or `accept` = pass downstream flagged `late_arrival` without touching the state |
| `DEDUP_WINDOW` | 16 | `src_lsn` values remembered per item |
| `MAX_FUTURE_SKEW_SECONDS` | 0 | reject event times later than now + N s (0 = off: replayed data can be dated after the wall clock) |
| `STATEFUL_CHECKS` | true | false = field checks only |
| `STATE_REBUILD` | true | on start, re-read `vendor_items.valid` to restore the item state |

On the generated data legitimate price moves stay between 0.67x and 1.5x, so the defaults leave them alone.

## Things to know

- Item state is in memory and rebuilt from `vendor_items.valid` on start. After a crash between producing and
  committing, redelivered events appear in the DLQ as `DUPLICATE_EVENT`: expected, and harmless.
- With `STALE_ACTION=dlq` (default) the roughly 75 late-arriving events in the generated data (scenario
  `SCD_STRESS_LATE_ARRIVAL`) land in the DLQ with their clean `item`, so they are not in `vendor_items.valid`.
  Use `accept` if the later SCD2 step should receive them.
- A delete has no business timestamp, so it cannot be ordered against updates by time.
- `evaluate.py` still tests the field rules against the answer key. The new reasons only run after the old
  checks, so they cannot change an existing PASS / REJECT outcome for that data. History checks and routing are
  covered by `tests/qa_gate/test_vendor_items_qa.py`.
- The bronze loader's `^vendor_items\..*` pattern will pick up `vendor_items.quarantine` by itself.
