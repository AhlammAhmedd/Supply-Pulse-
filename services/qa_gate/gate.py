"""
QA gate: reads Debezium change events, validates / cleans them with Pydantic rules, routes:
    valid   -> topic vendor_items.valid   (clean, typed, EGP, UTC)
    invalid -> topic vendor_items.dlq     (raw event + reject_reason)
At-least-once: offsets are committed only after the produced messages are flushed.
"""
import json
import os
import signal
import time
from collections import Counter

from confluent_kafka import Consumer, Producer

from reference import load_reference
from rules import Reject, validate

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
SRC_PATTERN = os.environ.get("SRC_TOPIC_PATTERN", r"^ops\.public\.vendor_items$")
VALID_TOPIC = os.environ.get("VALID_TOPIC", "vendor_items.valid")
DLQ_TOPIC = os.environ.get("DLQ_TOPIC", "vendor_items.dlq")
DATA_DIR = os.environ.get("DATA_DIR", "/data")


def _key(row):
    try:
        return f"V{int(row['store_id']):04d}|{str(row['item_ref']).strip()}"
    except (KeyError, TypeError, ValueError):
        return "unknown"


def process(envelope: dict, ref):
    """Debezium envelope -> (topic, key, value dict), or None if there is nothing to route."""
    op = envelope.get("op")
    meta = {"op": op, "cdc_ts_ms": envelope.get("ts_ms"), "pg_lsn": (envelope.get("source") or {}).get("lsn")}

    if op == "d":                                        # a delete is valid by definition
        before = envelope.get("before") or {}
        return VALID_TOPIC, _key(before), {
            "vendor_id": _key(before).split("|")[0], "item_ref": before.get("item_ref"), **meta}

    after = envelope.get("after")
    if after is None:
        return None
    try:
        item = validate(after, ref)
    except Reject as r:
        return DLQ_TOPIC, _key(after), {"reject_reason": r.reason, "raw": after, **meta}
    except (KeyError, TypeError, ValueError) as e:       # unreadable event: never crash, send to the DLQ
        return DLQ_TOPIC, _key(after), {"reject_reason": "MALFORMED_EVENT", "detail": str(e), "raw": after, **meta}
    return VALID_TOPIC, _key(after), {**json.loads(item.model_dump_json()), **meta}


def main():
    ref = load_reference(DATA_DIR)
    print(f"reference loaded: {len(ref.integration)} vendors, {len(ref.list_price)} list prices", flush=True)

    consumer = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": "qa-gate",
                         "auto.offset.reset": "earliest", "enable.auto.commit": False})
    producer = Producer({"bootstrap.servers": BOOTSTRAP, "linger.ms": 20})
    consumer.subscribe([SRC_PATTERN])      # regex: works even before Debezium has created the topic

    running = True
    signal.signal(signal.SIGTERM, lambda *_: globals().__setitem__("running", False))
    stats, reasons, pending, last_log, last_shown = Counter(), Counter(), 0, time.time(), None

    def commit():
        nonlocal pending
        if pending:
            producer.flush()
            consumer.commit(asynchronous=False)
            pending = 0

    try:
        while running:
            msg = consumer.poll(1.0)
            if msg is None:
                commit()
            elif msg.error():
                print("consumer error:", msg.error(), flush=True)
            elif msg.value() is not None:
                try:
                    out = process(json.loads(msg.value()), ref)
                except json.JSONDecodeError:
                    out = (DLQ_TOPIC, "unknown", {"reject_reason": "NOT_JSON", "raw": msg.value().decode("utf-8", "replace")})
                if out:
                    topic, key, value = out
                    producer.produce(topic, key=key, value=json.dumps(value, ensure_ascii=False))
                    producer.poll(0)
                    stats[topic] += 1
                    if topic == DLQ_TOPIC:
                        reasons[value["reject_reason"]] += 1
                pending += 1
                if pending >= 500:
                    commit()
            if time.time() - last_log > 10:
                shown = (dict(stats), dict(reasons))
                if shown != last_shown:                      # only print when something changed
                    print(f"routed: {dict(stats)}  dlq reasons: {dict(reasons)}", flush=True)
                    last_shown = shown
                last_log = time.time()
    finally:
        commit()
        consumer.close()


if __name__ == "__main__":
    main()
