"""Local MapReduce runner for SmartCampusAnalytics.

This script simulates the Hadoop Streaming sequence on one Windows machine:

1. Run the mapper on each input file.
2. Sort mapper output by the key (the field before the first tab).
3. Run the reducer on that sorted stream so equal keys arrive together.

It is not a Hadoop cluster. There is no HDFS, no YARN, and no parallel
task attempt. Multi-stage jobs are chained by writing the reducer output
of one stage into the mapper input of the next stage.

Run from the project root:

    python run_all.py
"""

import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
SRC = ROOT / "src"
DATA = ROOT / "data"
OUT = ROOT / "outputs"
INTERMEDIATE = OUT / "intermediate"


class StageError(RuntimeError):
    """A mapper or reducer exited with an error."""


def run_mapreduce(mapper, reducer, inputs, output_path):
    """Run one map, sort, and reduce stage. Return the reducer stdout."""
    mapper_chunks = []
    for input_path in inputs:
        text = Path(input_path).read_text(encoding="utf-8")
        completed = subprocess.run(
            [sys.executable, str(mapper)],
            input=text,
            text=True,
            encoding="utf-8",
            capture_output=True,
            cwd=str(SRC),
            check=False,
        )
        if completed.stderr:
            sys.stderr.write(completed.stderr)
        if completed.returncode != 0:
            raise StageError(
                f"{Path(mapper).name} failed on {input_path} "
                f"with exit code {completed.returncode}"
            )
        mapper_chunks.append(completed.stdout)

    records = []
    for line in "".join(mapper_chunks).splitlines():
        if not line.strip():
            continue
        if "\t" not in line:
            raise StageError(f"{Path(mapper).name} emitted a line without a tab: {line!r}")
        key, value = line.split("\t", 1)
        records.append((key, value))

    # Hadoop sorts by key. Equal keys stay adjacent for the reducer.
    records.sort(key=lambda record: record[0])
    sorted_text = "".join(f"{key}\t{value}\n" for key, value in records)

    completed = subprocess.run(
        [sys.executable, str(reducer)],
        input=sorted_text,
        text=True,
        encoding="utf-8",
        capture_output=True,
        cwd=str(SRC),
        check=False,
    )
    if completed.stderr:
        sys.stderr.write(completed.stderr)
    if completed.returncode != 0:
        raise StageError(
            f"{Path(reducer).name} failed with exit code {completed.returncode}"
        )

    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(completed.stdout, encoding="utf-8", newline="\n")
    return completed.stdout


def parse_kv_file(path):
    rows = []
    for line in Path(path).read_text(encoding="utf-8").splitlines():
        if not line.strip() or "\t" not in line:
            continue
        key, value = line.split("\t", 1)
        rows.append((key, value))
    return rows


def count_data_rows(path):
    text = Path(path).read_text(encoding="utf-8").splitlines()
    if not text:
        return 0
    return max(len(text) - 1, 0)


def build_summary():
    """Read the job outputs that were just written and describe them."""
    job1 = parse_kv_file(OUT / "job1_peak_wifi.txt")
    job2 = parse_kv_file(OUT / "job2_access_points.txt")
    job3 = parse_kv_file(OUT / "job3_canteen_items.txt")
    job4 = parse_kv_file(OUT / "job4_copurchase.txt")
    students = parse_kv_file(OUT / "job5_student_totals.txt")
    global_rows = parse_kv_file(OUT / "job5_global_average.txt")
    anomalies = parse_kv_file(OUT / "job5_anomalies.txt")
    transport = parse_kv_file(OUT / "job6_congestion.txt")

    lines = []
    lines.append("SmartCampusAnalytics local simulation summary")
    lines.append("This file was produced by run_all.py from the CSV files in data/.")
    lines.append("It is a single-process MapReduce simulation, not a Hadoop cluster run.")
    lines.append("")
    lines.append("Dataset row counts (excluding the header):")
    for name in ("wifi_logs.csv", "canteen.csv", "library.csv", "transport.csv"):
        lines.append(f"  {name}: {count_data_rows(DATA / name)}")
    lines.append("")

    peak = [row for row in job1 if row[0] == "PEAK"]
    hours = [row for row in job1 if row[0] != "PEAK"]
    lines.append("Job 1 - Peak Wi-Fi usage")
    lines.append(f"  Date-hour groups: {len(hours)}")
    if peak:
        hour_key, data_mb, duration, sessions = peak[0][1].split(",")
        lines.append(
            f"  Peak hour: {hour_key} | total data MB: {data_mb} | "
            f"total duration min: {duration} | sessions: {sessions}"
        )
    else:
        lines.append("  Peak hour: none")
    lines.append("  Columns in job1_peak_wifi.txt: hour, then data_mb,duration_min,sessions")
    lines.append("  The PEAK line repeats the winning hour and those three totals.")
    lines.append("")

    def sort_by_first_number(rows):
        def key_of(row):
            try:
                return float(row[1].split(",")[0])
            except (ValueError, IndexError):
                return 0.0
        return sorted(rows, key=key_of, reverse=True)

    lines.append("Job 2 - Access point utilisation")
    lines.append(f"  Access points: {len(job2)}")
    lines.append("  Columns: access_point, then data_mb,duration_min,sessions,avg_duration_min")
    for key, value in sort_by_first_number(job2):
        data_mb, duration, sessions, average = value.split(",")
        lines.append(
            f"  {key}: data MB {data_mb}, duration min {duration}, "
            f"sessions {sessions}, avg duration min {average}"
        )
    lines.append("")

    lines.append("Job 3 - Canteen item frequency")
    lines.append(f"  Items: {len(job3)}")
    lines.append("  Columns: item, then total_quantity,total_revenue")
    for key, value in sorted(job3, key=lambda row: float(row[1].split(",")[1]), reverse=True):
        quantity, revenue = value.split(",")
        lines.append(f"  {key}: quantity {quantity}, revenue {revenue}")
    lines.append("")

    lines.append("Job 4 - Top co-purchased items")
    lines.append(f"  Distinct unordered pairs: {len(job4)}")
    lines.append("  Columns: item_a::item_b, then distinct transaction count")
    lines.append("  Ranking below is by count. The job file itself is sorted by pair name.")
    ranked_pairs = sorted(job4, key=lambda row: (-int(row[1]), row[0]))
    for key, value in ranked_pairs[:10]:
        lines.append(f"  {key}: {value} transactions")
    lines.append("")

    lines.append("Job 5 - Library usage anomaly")
    lines.append(f"  Students with at least one valid EXIT visit: {len(students)}")
    if global_rows:
        duration, visits, average = global_rows[0][1].split(",")
        lines.append(
            f"  Overall: total duration min {duration}, visits {visits}, "
            f"average visit min {average}"
        )
        lines.append(f"  Threshold: student average > 1.5 x {average}")
    lines.append(f"  Anomalous students: {len(anomalies)}")
    lines.append(
        "  Anomaly columns: student, then total_duration,visit_count,"
        "student_average,overall_average,threshold"
    )
    for key, value in anomalies:
        lines.append(f"  {key}: {value}")
    lines.append("  S021 has only an ENTRY row and does not appear, because ENTRY is not a visit.")
    lines.append("")

    congested = [row for row in transport if row[1].endswith(",CONGESTED")]
    normal = [row for row in transport if row[1].endswith(",NORMAL")]
    lines.append("Job 6 - Transport congestion")
    lines.append(f"  Route-hour groups: {len(transport)}")
    lines.append(f"  Congested (average speed < 20 km/h): {len(congested)}")
    lines.append(f"  Normal: {len(normal)}")
    lines.append("  Columns: RouteID|YYYY-MM-DD HH, then average_speed,readings,STATUS")
    boundary = [
        row for row in transport if row[0].startswith("R3|") and row[0].endswith(" 12")
    ]
    lines.append(f"  R3 12:00 groups (planted at exactly 20 km/h): {len(boundary)}")
    for key, value in boundary:
        lines.append(f"  {key}: {value}")
    lines.append("  Sample congested groups:")
    for key, value in congested[:6]:
        lines.append(f"  {key}: {value}")
    lines.append("")
    lines.append("Output files:")
    for name in (
        "job1_peak_wifi.txt",
        "job2_access_points.txt",
        "job3_canteen_items.txt",
        "job4_copurchase.txt",
        "job5_student_totals.txt",
        "job5_global_average.txt",
        "job5_anomalies.txt",
        "job6_congestion.txt",
    ):
        path = OUT / name
        size = path.stat().st_size if path.exists() else 0
        lines.append(f"  outputs/{name} ({size} bytes)")
    return "\n".join(lines) + "\n"


def main():
    OUT.mkdir(parents=True, exist_ok=True)
    INTERMEDIATE.mkdir(parents=True, exist_ok=True)
    for old in INTERMEDIATE.glob("*.txt"):
        old.unlink()

    required = [
        DATA / "wifi_logs.csv",
        DATA / "canteen.csv",
        DATA / "library.csv",
        DATA / "transport.csv",
    ]
    missing = [str(path) for path in required if not path.exists()]
    if missing:
        raise SystemExit(
            "Missing datasets: "
            + ", ".join(missing)
            + ". Generate them with: python src/generate_data.py"
        )

    run_mapreduce(
        SRC / "job1_wifi_mapper.py",
        SRC / "job1_wifi_reducer.py",
        [DATA / "wifi_logs.csv"],
        OUT / "job1_peak_wifi.txt",
    )
    run_mapreduce(
        SRC / "job2_ap_mapper.py",
        SRC / "job2_ap_reducer.py",
        [DATA / "wifi_logs.csv"],
        OUT / "job2_access_points.txt",
    )
    run_mapreduce(
        SRC / "job3_canteen_mapper.py",
        SRC / "job3_canteen_reducer.py",
        [DATA / "canteen.csv"],
        OUT / "job3_canteen_items.txt",
    )
    run_mapreduce(
        SRC / "job4_stage1_mapper.py",
        SRC / "job4_stage1_reducer.py",
        [DATA / "canteen.csv"],
        INTERMEDIATE / "job4_stage1_pairs.txt",
    )
    run_mapreduce(
        SRC / "job4_stage2_mapper.py",
        SRC / "job4_stage2_reducer.py",
        [INTERMEDIATE / "job4_stage1_pairs.txt"],
        OUT / "job4_copurchase.txt",
    )
    run_mapreduce(
        SRC / "job5_stage1_mapper.py",
        SRC / "job5_stage1_reducer.py",
        [DATA / "library.csv"],
        OUT / "job5_student_totals.txt",
    )
    run_mapreduce(
        SRC / "job5_global_mapper.py",
        SRC / "job5_global_reducer.py",
        [OUT / "job5_student_totals.txt"],
        OUT / "job5_global_average.txt",
    )
    run_mapreduce(
        SRC / "job5_filter_mapper.py",
        SRC / "job5_filter_reducer.py",
        [OUT / "job5_student_totals.txt", OUT / "job5_global_average.txt"],
        OUT / "job5_anomalies.txt",
    )
    run_mapreduce(
        SRC / "job6_transport_mapper.py",
        SRC / "job6_transport_reducer.py",
        [DATA / "transport.csv"],
        OUT / "job6_congestion.txt",
    )

    expected = [
        OUT / "job1_peak_wifi.txt",
        OUT / "job2_access_points.txt",
        OUT / "job3_canteen_items.txt",
        OUT / "job4_copurchase.txt",
        OUT / "job5_student_totals.txt",
        OUT / "job5_global_average.txt",
        OUT / "job5_anomalies.txt",
        OUT / "job6_congestion.txt",
    ]
    empty = [path.name for path in expected if path.stat().st_size == 0]
    if empty:
        raise SystemExit("Empty output files: " + ", ".join(empty))

    summary = build_summary()
    (OUT / "summary.txt").write_text(summary, encoding="utf-8", newline="\n")
    sys.stdout.write(summary)
    sys.stdout.write("\nLocal simulation finished. Results are in outputs/.\n")


if __name__ == "__main__":
    main()
