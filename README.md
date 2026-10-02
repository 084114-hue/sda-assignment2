# SDA Assignment 2 — Shipment & Delivery Streaming Producer

Streaming Data Analytics, PGDM
Aashish Bakshi · Roll No. 084114

Industry: Logistics / Supply Chain
Domain: Shipment & Delivery
Assignment 1 issue: https://github.com/aditya-dua/SDA/issues/72

## What this does

`producer.py` reads shipment scan events from `sample_shipment_scans.csv` and
publishes them one by one to the Kafka topic **`shipment-scans`**, so the file
behaves like a live feed coming off the hub scanning systems.

Every message is keyed by `consignment_id`. This is deliberate. All scans of
one parcel must go to the same partition, otherwise a consumer could read
`delivered` before `out_for_delivery` and the delivery status would be wrong.

## Files

| File | Purpose |
|---|---|
| `producer.py` | Kafka producer, reads the CSV and publishes to `shipment-scans` |
| `sample_shipment_scans.csv` | 786 scan events across 160 consignments |
| `requirements.txt` | Python dependency |

## Message format

```json
{
  "scan_id": "SC000364",
  "consignment_id": "CN700075",
  "scan_type": "picked_up",
  "hub_code": "DEL-GGN-01",
  "destination_city": "Gurugram",
  "pincode": "122002",
  "rider_id": null,
  "weight_kg": 2.15,
  "cod_amount": 0,
  "failure_reason": null,
  "event_time": "2026-08-14 07:02:00"
}
```

`scan_type` moves through `picked_up → arrived_at_hub → sorted →
out_for_delivery`, and then ends in either `delivered` or `failed_attempt`.
About 7% of consignments have no final scan, which is how a parcel still in
transit looks on the topic.

## How to run

```bash
pip install -r requirements.txt

# start Kafka, then create the topic
kafka-topics.sh --create --topic shipment-scans \
  --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1

# run the producer
python producer.py

# faster, and only the first 100 events
python producer.py --delay 0.05 --limit 100
```

To watch the messages arriving:

```bash
kafka-console-consumer.sh --topic shipment-scans \
  --bootstrap-server localhost:9092 --from-beginning
```

## How the data was generated

Written with a short Python script using the `random` module with a fixed seed,
so the file is reproducible. Hub codes, destination cities and pincodes are real
Delhi NCR values. Volumes and the roughly 12% failed-attempt rate were chosen to
look like a mid-sized third party logistics operator in the region.
