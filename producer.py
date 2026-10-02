"""
producer.py
Streaming Data Analytics - Assignment 2
Domain: Shipment & Delivery (Logistics / Supply Chain)

Reads shipment scan events from sample_shipment_scans.csv and publishes them
to the Kafka topic 'shipment-scans', one event at a time, so that the file
behaves like a live feed coming from the hub scanning systems.

Each message is keyed by consignment_id. Keying matters here because all the
scans of one parcel must land in the same partition, otherwise a consumer
could see 'delivered' before 'out_for_delivery'.

Run:
    python producer.py
    python producer.py --delay 0.1 --limit 100
"""

import argparse
import csv
import json
import sys
import time

from kafka import KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable

TOPIC = "shipment-scans"
BOOTSTRAP = "localhost:9092"
DATA_FILE = "sample_shipment_scans.csv"


def build_producer(bootstrap):
    """Create the Kafka producer. Values go out as JSON, keys as plain bytes."""
    return KafkaProducer(
        bootstrap_servers=[bootstrap],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
        acks="all",          # wait for the broker to confirm, we do not want to lose scans
        retries=3,
    )


def clean(row):
    """Convert the CSV row into the JSON event we want on the topic."""
    return {
        "scan_id": row["scan_id"],
        "consignment_id": row["consignment_id"],
        "scan_type": row["scan_type"],
        "hub_code": row["hub_code"],
        "destination_city": row["destination_city"],
        "pincode": row["pincode"],
        "rider_id": row["rider_id"] or None,
        "weight_kg": float(row["weight_kg"]),
        "cod_amount": int(row["cod_amount"]),
        "failure_reason": row["failure_reason"] or None,
        "event_time": row["event_time"],
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--file", default=DATA_FILE, help="path to the sample data CSV")
    ap.add_argument("--topic", default=TOPIC, help="Kafka topic to publish to")
    ap.add_argument("--bootstrap", default=BOOTSTRAP, help="Kafka bootstrap server")
    ap.add_argument("--delay", type=float, default=0.25,
                    help="seconds to wait between two events")
    ap.add_argument("--limit", type=int, default=0,
                    help="stop after this many events (0 means send all)")
    args = ap.parse_args()

    try:
        producer = build_producer(args.bootstrap)
    except NoBrokersAvailable:
        print(f"Could not reach a Kafka broker at {args.bootstrap}.")
        print("Start Kafka first, then run this script again.")
        sys.exit(1)

    sent = 0
    failed = 0

    print(f"Publishing to topic '{args.topic}' ...\n")

    with open(args.file, newline="") as f:
        for row in csv.DictReader(f):
            event = clean(row)
            try:
                producer.send(args.topic, key=event["consignment_id"], value=event)
                sent += 1
                print(f"[{sent:>4}] {event['consignment_id']}  "
                      f"{event['scan_type']:<16} {event['destination_city']:<10} "
                      f"{event['event_time']}")
            except KafkaError as e:
                failed += 1
                print(f"  failed to send {event['scan_id']}: {e}")

            if args.limit and sent >= args.limit:
                break
            time.sleep(args.delay)

    producer.flush()
    producer.close()

    print(f"\nDone. {sent} events sent, {failed} failed.")


if __name__ == "__main__":
    main()
