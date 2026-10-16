"""Unit and end-to-end tests for the six Smart Campus MapReduce jobs.

Run from the project root:

    python -m unittest discover -s tests -v
"""

import csv
import subprocess
import sys
import unittest
from collections import defaultdict
from itertools import combinations
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
DATA = ROOT / "data"
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(SRC))

from streaming_io import (  # noqa: E402
    ANOMALY_FACTOR,
    CONGESTION_SPEED_KMPH,
    hour_bucket,
    parse_timestamp,
)
import run_all  # noqa: E402


def setUpModule():
    """Regenerate the fixed synthetic CSVs so the end-to-end job reads them."""
    completed = subprocess.run(
        [sys.executable, str(SRC / "generate_data.py")],
        text=True,
        encoding="utf-8",
        capture_output=True,
        check=False,
    )
    if completed.returncode != 0:
        raise RuntimeError(completed.stderr or completed.stdout)


def run_script(script_name, text):
    completed = subprocess.run(
        [sys.executable, str(SRC / script_name)],
        input=text,
        text=True,
        encoding="utf-8",
        capture_output=True,
        check=False,
    )
    return completed.returncode, completed.stdout, completed.stderr


def parse_output(stdout):
    rows = []
    for line in stdout.splitlines():
        if not line.strip():
            continue
        key, value = line.split("\t", 1)
        rows.append((key, value))
    return rows


def read_csv(path):
    with Path(path).open(encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


class MapperReducerTests(unittest.TestCase):
    def test_wifi_mapper_key_and_header(self):
        text = (
            "StudentID,Timestamp,AccessPoint,DataMB,DurationMin\n"
            "S001,2026-10-05 09:15:00,AP-LIB-01,12.50,30\n"
        )
        code, stdout, stderr = run_script("job1_wifi_mapper.py", text)
        self.assertEqual(code, 0, stderr)
        self.assertEqual(stdout, "2026-10-05 09\t12.500000,30.000000,1\n")
        self.assertNotIn("StudentID", stdout)

    def test_wifi_mapper_skips_malformed_and_keeps_valid(self):
        text = (
            "StudentID,Timestamp,AccessPoint,DataMB,DurationMin\n"
            "short,row\n"
            "S001,not-a-time,AP-LIB-01,10,5\n"
            "S002,2026-10-05 10:00:00,AP-LIB-01,abc,5\n"
            "S003,2026-10-05 10:05:00,AP-LIB-01,-1,5\n"
            "S004,2026-10-05 11:00:00,AP-CAFE-01,8.25,15\n"
        )
        code, stdout, stderr = run_script("job1_wifi_mapper.py", text)
        self.assertEqual(code, 0, stderr)
        self.assertIn("WARN:", stderr)
        self.assertEqual(parse_output(stdout), [("2026-10-05 11", "8.250000,15.000000,1")])

    def test_empty_input_produces_no_stdout(self):
        for script in (
            "job1_wifi_mapper.py",
            "job1_wifi_reducer.py",
            "job3_canteen_mapper.py",
            "job6_transport_reducer.py",
        ):
            code, stdout, stderr = run_script(script, "")
            self.assertEqual(code, 0, stderr)
            self.assertEqual(stdout, "")

    def test_quoted_comma_item_stays_one_field(self):
        text = (
            "TransactionID,Timestamp,StudentID,Item,Quantity,Amount\n"
            'T1,2026-10-05 10:00:00,S001,"Chai, Cutting",1,8.00\n'
        )
        code, stdout, stderr = run_script("job4_stage1_mapper.py", text)
        self.assertEqual(code, 0, stderr)
        self.assertEqual(parse_output(stdout), [("T1", "Chai, Cutting")])

    def test_reducer_aggregates_only_consecutive_keys(self):
        # Unsorted duplicate keys must stay as separate groups. Hadoop's
        # shuffle, not the reducer, is responsible for bringing them together.
        text = "Tea\t1.000000,10.000000\nCoffee\t2.000000,30.000000\nTea\t1.000000,10.000000\n"
        code, stdout, stderr = run_script("job3_canteen_reducer.py", text)
        self.assertEqual(code, 0, stderr)
        self.assertEqual(
            parse_output(stdout),
            [
                ("Tea", "1.000000,10.000000"),
                ("Coffee", "2.000000,30.000000"),
                ("Tea", "1.000000,10.000000"),
            ],
        )

    def test_runner_sort_merges_the_same_item(self):
        sample = ROOT / "outputs" / "intermediate" / "sample_canteen.csv"
        sample.parent.mkdir(parents=True, exist_ok=True)
        sample.write_text(
            "TransactionID,Timestamp,StudentID,Item,Quantity,Amount\n"
            "T1,2026-10-05 10:00:00,S001,Tea,1,10.00\n"
            "T2,2026-10-05 10:05:00,S002,Coffee,2,30.00\n"
            "T3,2026-10-05 10:10:00,S003,Tea,1,10.00\n",
            encoding="utf-8",
            newline="\n",
        )
        destination = ROOT / "outputs" / "intermediate" / "sample_job3.txt"
        run_all.run_mapreduce(
            SRC / "job3_canteen_mapper.py",
            SRC / "job3_canteen_reducer.py",
            [sample],
            destination,
        )
        rows = parse_output(destination.read_text(encoding="utf-8"))
        self.assertEqual(
            rows,
            [
                ("Coffee", "2.000000,30.000000"),
                ("Tea", "2.000000,20.000000"),
            ],
        )

    def test_job1_reducer_sums_and_picks_peak(self):
        text = (
            "2026-10-05 09\t10.000000,5.000000,1\n"
            "2026-10-05 09\t2.500000,5.000000,1\n"
            "2026-10-05 13\t40.000000,10.000000,1\n"
            "2026-10-07 13\t40.000000,10.000000,1\n"
        )
        code, stdout, stderr = run_script("job1_wifi_reducer.py", text)
        self.assertEqual(code, 0, stderr)
        rows = dict(parse_output(stdout))
        self.assertEqual(rows["2026-10-05 09"], "12.500000,10.000000,2")
        self.assertEqual(rows["2026-10-05 13"], "40.000000,10.000000,1")
        # Equal data totals: the earlier hour key is kept.
        self.assertEqual(rows["PEAK"], "2026-10-05 13,40.000000,10.000000,1")

    def test_job2_average_session_duration(self):
        text = (
            "AP-LIB-01\t10.000000,30.000000,1\n"
            "AP-LIB-01\t5.000000,10.000000,1\n"
        )
        code, stdout, stderr = run_script("job2_ap_reducer.py", text)
        self.assertEqual(code, 0, stderr)
        self.assertEqual(parse_output(stdout), [("AP-LIB-01", "15.000000,40.000000,2,20.000000")])

    def test_pair_generation_is_unordered_and_unique(self):
        text = (
            "T1\tSamosa\n"
            "T1\tTea\n"
            "T1\tTea\n"
            "T2\tTea\n"
            "T2\tSamosa\n"
            "T3\tSandwich\n"
        )
        code, stdout, stderr = run_script("job4_stage1_reducer.py", text)
        self.assertEqual(code, 0, stderr)
        self.assertEqual(parse_output(stdout), [("Samosa::Tea", "1"), ("Samosa::Tea", "1")])

        code, stdout, stderr = run_script("job4_stage2_reducer.py", stdout)
        self.assertEqual(code, 0, stderr)
        self.assertEqual(parse_output(stdout), [("Samosa::Tea", "2")])

    def test_library_anomaly_threshold_is_strict(self):
        # overall duration 200 over 10 visits => average 20, threshold 30.
        # S1 average 30 is not strictly greater. S2 average 31 is.
        text = "0|GLOBAL\t200,10\n1|S1\t30,1\n1|S2\t31,1\n"
        code, stdout, stderr = run_script("job5_filter_reducer.py", text)
        self.assertEqual(code, 0, stderr)
        rows = parse_output(stdout)
        self.assertEqual([key for key, _value in rows], ["S2"])
        parts = rows[0][1].split(",")
        self.assertAlmostEqual(float(parts[2]), 31.0, places=5)
        self.assertAlmostEqual(float(parts[3]), 20.0, places=5)
        self.assertAlmostEqual(float(parts[4]), 30.0, places=5)

    def test_library_mapper_counts_only_positive_exits(self):
        text = (
            "StudentID,Timestamp,EntryType,DurationMin\n"
            "S001,2026-10-05 09:00:00,ENTRY,0\n"
            "S001,2026-10-05 09:40:00,EXIT,40\n"
            "S002,2026-10-05 10:00:00,EXIT,0\n"
            "S003,2026-10-05 11:00:00,EXIT,not-a-number\n"
            "S004,2026-10-05 12:00:00,VISITOR,15\n"
        )
        code, stdout, stderr = run_script("job5_stage1_mapper.py", text)
        self.assertEqual(code, 0, stderr)
        self.assertIn("WARN:", stderr)
        self.assertEqual(parse_output(stdout), [("S001", "40.000000,1")])

    def test_global_average_uses_totals_not_mean_of_means(self):
        # Two students: averages 10 and 100. The mean of averages is 55.
        # The visit-weighted average is (10 + 200) / 3 = 70.
        text = "S1\t10.000000,1,10.000000\nS2\t200.000000,2,100.000000\n"
        code, stdout, stderr = run_script("job5_global_mapper.py", text)
        self.assertEqual(code, 0, stderr)
        code, stdout, stderr = run_script("job5_global_reducer.py", stdout)
        self.assertEqual(code, 0, stderr)
        key, value = parse_output(stdout)[0]
        self.assertEqual(key, "GLOBAL")
        duration, visits, average = value.split(",")
        self.assertEqual(visits, "3")
        self.assertAlmostEqual(float(duration), 210.0, places=5)
        self.assertAlmostEqual(float(average), 70.0, places=5)

    def test_congestion_threshold_is_strictly_below_20(self):
        text = (
            "R1|2026-10-05 08\t30.000000,2\n"
            "R3|2026-10-05 12\t40.000000,2\n"
            "R9|2026-10-05 15\t45.000000,2\n"
        )
        code, stdout, stderr = run_script("job6_transport_reducer.py", text)
        self.assertEqual(code, 0, stderr)
        rows = dict(parse_output(stdout))
        self.assertTrue(rows["R1|2026-10-05 08"].endswith(",CONGESTED"))
        self.assertTrue(rows["R3|2026-10-05 12"].endswith(",NORMAL"))
        self.assertTrue(rows["R9|2026-10-05 15"].endswith(",NORMAL"))
        self.assertAlmostEqual(float(rows["R1|2026-10-05 08"].split(",")[0]), 15.0, places=5)
        self.assertAlmostEqual(float(rows["R3|2026-10-05 12"].split(",")[0]), 20.0, places=5)


class EndToEndTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        run_all.main()
        cls.job1 = dict(parse_output((ROOT / "outputs" / "job1_peak_wifi.txt").read_text(encoding="utf-8")))
        cls.job2 = dict(parse_output((ROOT / "outputs" / "job2_access_points.txt").read_text(encoding="utf-8")))
        cls.job3 = dict(parse_output((ROOT / "outputs" / "job3_canteen_items.txt").read_text(encoding="utf-8")))
        cls.job4 = dict(parse_output((ROOT / "outputs" / "job4_copurchase.txt").read_text(encoding="utf-8")))
        cls.students = dict(parse_output((ROOT / "outputs" / "job5_student_totals.txt").read_text(encoding="utf-8")))
        cls.global_row = dict(parse_output((ROOT / "outputs" / "job5_global_average.txt").read_text(encoding="utf-8")))
        cls.anomalies = dict(parse_output((ROOT / "outputs" / "job5_anomalies.txt").read_text(encoding="utf-8")))
        cls.transport = dict(parse_output((ROOT / "outputs" / "job6_congestion.txt").read_text(encoding="utf-8")))

    def test_outputs_are_non_empty(self):
        self.assertGreater(len(self.job1), 1)
        self.assertGreater(len(self.job2), 0)
        self.assertGreater(len(self.job3), 0)
        self.assertGreater(len(self.job4), 0)
        self.assertGreater(len(self.students), 0)
        self.assertIn("GLOBAL", self.global_row)
        self.assertGreater(len(self.anomalies), 0)
        self.assertGreater(len(self.transport), 0)
        summary = (ROOT / "outputs" / "summary.txt").read_text(encoding="utf-8")
        self.assertIn("Peak hour:", summary)
        self.assertIn("local simulation", summary.lower())

    def test_job1_matches_independent_totals(self):
        expected = defaultdict(lambda: [0.0, 0.0, 0])
        for row in read_csv(DATA / "wifi_logs.csv"):
            when = parse_timestamp(row["Timestamp"])
            key = hour_bucket(when)
            expected[key][0] += float(row["DataMB"])
            expected[key][1] += float(row["DurationMin"])
            expected[key][2] += 1
        self.assertEqual(set(self.job1) - {"PEAK"}, set(expected))
        for key, (data_mb, duration, sessions) in expected.items():
            got_data, got_duration, got_sessions = self.job1[key].split(",")
            self.assertAlmostEqual(float(got_data), data_mb, places=4)
            self.assertAlmostEqual(float(got_duration), duration, places=4)
            self.assertEqual(int(got_sessions), sessions)
        peak_key = min(expected, key=lambda key: (-expected[key][0], key))
        self.assertTrue(self.job1["PEAK"].startswith(peak_key + ","))
        self.assertTrue(self.job1["PEAK"].startswith("2026-10-07 13,"))

    def test_job2_matches_independent_averages(self):
        expected = defaultdict(lambda: [0.0, 0.0, 0])
        for row in read_csv(DATA / "wifi_logs.csv"):
            key = row["AccessPoint"]
            expected[key][0] += float(row["DataMB"])
            expected[key][1] += float(row["DurationMin"])
            expected[key][2] += 1
        self.assertEqual(set(self.job2), set(expected))
        for key, (data_mb, duration, sessions) in expected.items():
            got_data, got_duration, got_sessions, got_avg = self.job2[key].split(",")
            self.assertAlmostEqual(float(got_data), data_mb, places=4)
            self.assertAlmostEqual(float(got_duration), duration, places=4)
            self.assertEqual(int(got_sessions), sessions)
            self.assertAlmostEqual(float(got_avg), duration / sessions, places=4)

    def test_job3_matches_independent_item_totals(self):
        expected = defaultdict(lambda: [0.0, 0.0])
        for row in read_csv(DATA / "canteen.csv"):
            expected[row["Item"]][0] += float(row["Quantity"])
            expected[row["Item"]][1] += float(row["Amount"])
        self.assertEqual(set(self.job3), set(expected))
        for item, (quantity, revenue) in expected.items():
            got_quantity, got_revenue = self.job3[item].split(",")
            self.assertAlmostEqual(float(got_quantity), quantity, places=4)
            self.assertAlmostEqual(float(got_revenue), revenue, places=4)

    def test_job4_matches_independent_pair_counts(self):
        baskets = defaultdict(set)
        for row in read_csv(DATA / "canteen.csv"):
            baskets[row["TransactionID"]].add(row["Item"])
        expected = defaultdict(int)
        for items in baskets.values():
            for left, right in combinations(sorted(items), 2):
                expected[f"{left}::{right}"] += 1
        self.assertEqual(self.job4, {key: str(count) for key, count in expected.items()})
        # The duplicate Tea rows on one bill contribute Tea::Samosa once, not twice.
        self.assertGreater(expected["Samosa::Tea"], 0)

    def test_job5_matches_threshold_rule(self):
        totals = defaultdict(lambda: [0.0, 0])
        for row in read_csv(DATA / "library.csv"):
            if row["EntryType"].upper() != "EXIT":
                continue
            duration = float(row["DurationMin"])
            if duration <= 0:
                continue
            totals[row["StudentID"]][0] += duration
            totals[row["StudentID"]][1] += 1
        self.assertNotIn("S021", totals)
        self.assertNotIn("S021", self.students)
        overall_duration = sum(value[0] for value in totals.values())
        overall_visits = sum(value[1] for value in totals.values())
        overall_average = overall_duration / overall_visits
        threshold = ANOMALY_FACTOR * overall_average
        got_duration, got_visits, got_average = self.global_row["GLOBAL"].split(",")
        self.assertAlmostEqual(float(got_duration), overall_duration, places=4)
        self.assertEqual(int(got_visits), overall_visits)
        self.assertAlmostEqual(float(got_average), overall_average, places=4)
        expected_flags = {
            student
            for student, (duration, visits) in totals.items()
            if (duration / visits) > threshold
        }
        self.assertEqual(set(self.anomalies), expected_flags)
        self.assertEqual(expected_flags, {"S901", "S902", "S903"})
        for student in expected_flags:
            student_average = totals[student][0] / totals[student][1]
            self.assertGreater(student_average, threshold)

    def test_job6_matches_congestion_rule(self):
        groups = defaultdict(lambda: [0.0, 0])
        for row in read_csv(DATA / "transport.csv"):
            when = parse_timestamp(row["Timestamp"])
            key = f"{row['RouteID']}|{hour_bucket(when)}"
            groups[key][0] += float(row["SpeedKmph"])
            groups[key][1] += 1
        self.assertEqual(set(self.transport), set(groups))
        congested = 0
        for key, (speed_sum, count) in groups.items():
            average = speed_sum / count
            status = "CONGESTED" if average < CONGESTION_SPEED_KMPH else "NORMAL"
            got_average, got_count, got_status = self.transport[key].split(",")
            self.assertAlmostEqual(float(got_average), average, places=4)
            self.assertEqual(int(got_count), count)
            self.assertEqual(got_status, status)
            if status == "CONGESTED":
                congested += 1
            if key.startswith("R3|") and key.endswith(" 12"):
                self.assertEqual(status, "NORMAL")
                self.assertAlmostEqual(average, 20.0, places=4)
        self.assertEqual(congested, 15)


if __name__ == "__main__":
    unittest.main()
