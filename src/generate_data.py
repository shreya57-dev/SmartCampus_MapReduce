"""Create synthetic Smart Campus CSV files.

The rows are invented demonstration records. They are not extracted from a
real university system. The random seed is fixed so the files can be
regenerated with the same contents.

Run from the project root:

    python src/generate_data.py
"""

import csv
import random
from datetime import datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
SEED = 42

DATES = [datetime(2026, 10, 5) + timedelta(days=offset) for offset in range(5)]
HOURS = list(range(8, 21))

ACCESS_POINTS = [
    "AP-LIB-01",
    "AP-LIB-02",
    "AP-CAFE-01",
    "AP-HOSTEL-A",
    "AP-ADMIN-01",
]

PRICES = {
    "Tea": 10.0,
    "Coffee": 15.0,
    "Samosa": 20.0,
    "Sandwich": 40.0,
    "Biryani": 80.0,
    "Juice": 30.0,
    "Maggi": 40.0,
    "Idli": 25.0,
    "Chai, Cutting": 8.0,
}

# (basket, weight). Baskets are the distinct items bought together.
BASKETS = [
    (["Tea", "Samosa"], 40),
    (["Tea", "Samosa", "Juice"], 15),
    (["Coffee", "Sandwich"], 22),
    (["Biryani", "Juice"], 18),
    (["Maggi", "Tea"], 12),
    (["Idli", "Coffee"], 10),
    (["Sandwich"], 8),
    (["Chai, Cutting", "Samosa"], 10),
    (["Biryani", "Juice", "Tea"], 8),
]


def write_csv(filename, header, rows):
    path = DATA_DIR / filename
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle, lineterminator="\n")
        writer.writerow(header)
        writer.writerows(rows)
    print(f"wrote {path.name}: {len(rows)} data rows")


def student_id(number):
    return f"S{number:03d}"


def choose_weighted(options):
    basket, = random.choices(
        [item[0] for item in options],
        weights=[item[1] for item in options],
        k=1,
    )
    return list(basket)


def generate_wifi():
    """Sessions across a teaching week, with one deliberately busy hour."""
    rows = []
    session_students = [student_id(number) for number in range(1, 31)]
    for day in DATES:
        for hour in HOURS:
            is_peak = day.date().isoformat() == "2026-10-07" and hour == 13
            session_count = 20 if is_peak else random.randint(2, 4)
            for _ in range(session_count):
                minute = random.randint(0, 59)
                second = random.randint(0, 59)
                when = day.replace(hour=hour, minute=minute, second=second)
                if is_peak:
                    data_mb = round(random.uniform(100.0, 180.0), 2)
                    duration = random.randint(20, 90)
                    access_point = random.choice(["AP-LIB-01", "AP-CAFE-01", "AP-HOSTEL-A"])
                else:
                    data_mb = round(random.uniform(2.0, 28.0), 2)
                    duration = random.randint(5, 50)
                    access_point = random.choice(ACCESS_POINTS)
                rows.append(
                    [
                        random.choice(session_students),
                        when.strftime("%Y-%m-%d %H:%M:%S"),
                        access_point,
                        f"{data_mb:.2f}",
                        str(duration),
                    ]
                )
    rows.sort(key=lambda row: (row[1], row[0]))
    write_csv(
        "wifi_logs.csv",
        ["StudentID", "Timestamp", "AccessPoint", "DataMB", "DurationMin"],
        rows,
    )


def generate_canteen():
    """Item lines that share a TransactionID when they were bought together."""
    rows = []
    students = [student_id(number) for number in range(1, 31)]
    transaction_number = 1
    # 140 random baskets spread over the week, plus one explicit duplicate-item bill.
    for _ in range(140):
        day = random.choice(DATES)
        hour = random.choice(list(range(8, 20)))
        minute = random.randint(0, 59)
        when = day.replace(hour=hour, minute=minute, second=random.randint(0, 59))
        transaction_id = f"T{transaction_number:05d}"
        transaction_number += 1
        student = random.choice(students)
        for item in choose_weighted(BASKETS):
            quantity = random.choice([1, 1, 1, 2])
            amount = PRICES[item] * quantity
            rows.append(
                [
                    transaction_id,
                    when.strftime("%Y-%m-%d %H:%M:%S"),
                    student,
                    item,
                    str(quantity),
                    f"{amount:.2f}",
                ]
            )

    # Same item appears twice on one bill. Pair generation must count Tea once.
    duplicate_time = "2026-10-06 11:05:00"
    duplicate_id = f"T{transaction_number:05d}"
    for item, quantity in (("Tea", 1), ("Tea", 2), ("Samosa", 1)):
        rows.append(
            [
                duplicate_id,
                duplicate_time,
                "S008",
                item,
                str(quantity),
                f"{PRICES[item] * quantity:.2f}",
            ]
        )

    rows.sort(key=lambda row: (row[1], row[0], row[3]))
    write_csv(
        "canteen.csv",
        ["TransactionID", "Timestamp", "StudentID", "Item", "Quantity", "Amount"],
        rows,
    )


def generate_library():
    """ENTRY and EXIT rows. Only a positive-duration EXIT is a visit."""
    rows = []
    normals = [student_id(number) for number in range(1, 21)]
    # Bounded so every normal average stays well below the anomaly cut.
    normal_durations = [30, 35, 40, 45, 32, 38]
    for index, student in enumerate(normals):
        for visit_number in range(3):
            day = DATES[(index + visit_number) % len(DATES)]
            start_hour = 9 + ((index + visit_number) % 6)
            duration = normal_durations[(index + visit_number) % len(normal_durations)]
            start = day.replace(hour=start_hour, minute=(visit_number * 7) % 60, second=0)
            end = start + timedelta(minutes=duration)
            rows.append([student, start.strftime("%Y-%m-%d %H:%M:%S"), "ENTRY", "0"])
            rows.append([student, end.strftime("%Y-%m-%d %H:%M:%S"), "EXIT", str(duration)])

    anomalies = {
        "S901": (160, 170, 165),
        "S902": (180, 190, 185),
        "S903": (200, 210, 205),
    }
    for offset, (student, durations) in enumerate(anomalies.items()):
        for visit_number, duration in enumerate(durations):
            day = DATES[visit_number]
            start = day.replace(hour=9, minute=10 + offset, second=0)
            end = start + timedelta(minutes=duration)
            rows.append([student, start.strftime("%Y-%m-%d %H:%M:%S"), "ENTRY", "0"])
            rows.append([student, end.strftime("%Y-%m-%d %H:%M:%S"), "EXIT", str(duration)])

    # Well-formed ENTRY with no matching EXIT. It must not become a visit.
    rows.append(["S021", "2026-10-05 09:00:00", "ENTRY", "0"])

    rows.sort(key=lambda row: (row[1], row[0], row[2]))
    write_csv(
        "library.csv",
        ["StudentID", "Timestamp", "EntryType", "DurationMin"],
        rows,
    )


def generate_transport():
    """Route-hour speeds. Congested groups stay strictly below 20 km/h."""
    rows = []
    routes = {
        "R1": ["BUS-01", "BUS-04"],
        "R2": ["BUS-02", "BUS-05"],
        "R3": ["BUS-03", "BUS-06"],
    }
    for day in DATES:
        for hour in range(8, 20):
            for minute in (0, 15, 30, 45):
                when = day.replace(hour=hour, minute=minute, second=0)
                stamp = when.strftime("%Y-%m-%d %H:%M:%S")
                for route_id, vehicles in routes.items():
                    for vehicle_id in vehicles:
                        if route_id == "R1" and hour in (8, 9):
                            speed = round(random.uniform(10.0, 18.0), 2)
                        elif route_id == "R2" and hour == 17:
                            speed = round(random.uniform(12.0, 19.0), 2)
                        elif route_id == "R3" and hour == 12:
                            speed = 20.0
                        else:
                            speed = round(random.uniform(30.0, 46.0), 2)
                        rows.append(
                            [vehicle_id, stamp, route_id, f"{speed:.2f}"]
                        )
    rows.sort(key=lambda row: (row[1], row[2], row[0]))
    write_csv(
        "transport.csv",
        ["VehicleID", "Timestamp", "RouteID", "SpeedKmph"],
        rows,
    )


def main():
    random.seed(SEED)
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    generate_wifi()
    generate_canteen()
    generate_library()
    generate_transport()
    print("Synthetic datasets are ready in data/.")


if __name__ == "__main__":
    main()
