import csv, random
from datetime import datetime, timedelta

random.seed(42)

HUBS = [
    ("DEL-GGN-01", "Gurugram", "122001"),
    ("DEL-NOI-02", "Noida", "201301"),
    ("DEL-PTP-03", "Delhi", "110034"),
    ("DEL-OKH-04", "Delhi", "110020"),
    ("DEL-FBD-05", "Faridabad", "121001"),
]

CITIES = {
    "Gurugram": ["122001", "122002", "122009", "122018"],
    "Noida": ["201301", "201303", "201310"],
    "Delhi": ["110034", "110020", "110092", "110058", "110075"],
    "Faridabad": ["121001", "121003"],
    "Ghaziabad": ["201001", "201012"],
}

RIDERS = [f"RDR{str(i).zfill(3)}" for i in range(101, 131)]
FAIL_REASONS = ["customer not available", "address not found",
                "customer refused", "office closed", "payment not ready"]

rows = []
start = datetime(2026, 8, 14, 6, 0, 0)
scan_no = 1

for i in range(1, 161):                       # 160 consignments
    cnote = f"CN{str(700000 + i)}"
    hub_code, hub_city, _ = random.choice(HUBS)
    dest_city = random.choice(list(CITIES.keys()))
    pincode = random.choice(CITIES[dest_city])
    rider = random.choice(RIDERS)
    weight = round(random.uniform(0.3, 12.0), 2)
    cod = random.choice([0, 0, 0, 499, 899, 1250, 2300])

    t = start + timedelta(minutes=random.randint(0, 600))

    # every consignment follows the same hop sequence
    journey = ["picked_up", "arrived_at_hub", "sorted", "out_for_delivery"]

    # decide the final status
    roll = random.random()
    if roll < 0.78:
        journey.append("delivered")
    elif roll < 0.93:
        journey.append("failed_attempt")
    # remaining 7% are still in transit, so no final scan

    for scan_type in journey:
        t += timedelta(minutes=random.randint(25, 180))
        reason = ""
        if scan_type == "failed_attempt":
            reason = random.choice(FAIL_REASONS)
        rows.append({
            "scan_id": f"SC{str(scan_no).zfill(6)}",
            "consignment_id": cnote,
            "scan_type": scan_type,
            "hub_code": hub_code,
            "destination_city": dest_city,
            "pincode": pincode,
            "rider_id": rider if scan_type in ("out_for_delivery", "delivered", "failed_attempt") else "",
            "weight_kg": weight,
            "cod_amount": cod if scan_type == "delivered" else 0,
            "failure_reason": reason,
            "event_time": t.strftime("%Y-%m-%d %H:%M:%S"),
        })
        scan_no += 1

rows.sort(key=lambda r: r["event_time"])

with open("/home/claude/sample_shipment_scans.csv", "w", newline="") as f:
    w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
    w.writeheader()
    w.writerows(rows)

print("rows:", len(rows))
print("consignments: 160")
