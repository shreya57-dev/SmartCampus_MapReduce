# Expected Output Worksheet

**Mini Project:** Smart Campus Analytics using Python Hadoop Streaming

The expected values below are based on the generated input datasets and the successful execution of `python run_all.py`. The Student Result column records the observed result from the local simulation.

| Job | Expected Key | Expected Value | Student Result |
|---|---|---|---|
| Job 1 – Peak Wi-Fi | Date-hour (`YYYY-MM-DD HH`) | Peak hour: `2026-10-07 13`; total data: `3032.74 MB`; total duration: `1111 min`; sessions: `20` | **Pass** — Peak hour and totals match the generated output. |
| Job 2 – Access Point | Access point ID | `AP-CAFE-01`: data `2213.48 MB`; duration `1731 min`; sessions `52`; average duration `33.288462 min` | **Pass** — Five access points were aggregated. AP-CAFE-01 has the highest data usage. |
| Job 3 – Canteen | Item name | Tea: quantity `84`, revenue `840`; Biryani: quantity `47`, revenue `3760` | **Pass** — Nine items were aggregated. Tea has the highest quantity; Biryani has the highest revenue. |
| Job 4 – Co-Purchase | Sorted item pair (`item_a::item_b`) | `Samosa::Tea`: `49` transactions | **Pass** — Nine distinct unordered pairs were counted; Samosa::Tea is the most frequent. |
| Job 5 – Library | Student ID | Overall average: `56.086957 min`; threshold: `84.130435 min`; anomalies: S901 (`165 min`), S902 (`185 min`), S903 (`205 min`) | **Pass** — Three students exceed the strict `1.5 × overall average` threshold. |
| Job 6 – Transport | Route ID and date-hour (`RouteID|YYYY-MM-DD HH`) | Congested if average speed is strictly below `20 km/h`; `15` congested groups and `165` normal groups | **Pass** — 180 route-hour groups were processed. Groups averaging exactly `20 km/h` are normal. |

## Execution Summary

- Input datasets: 215 Wi-Fi records, 298 canteen item lines, 139 library gate events, and 1,440 transport readings, excluding headers.
- All six jobs completed successfully in the local MapReduce simulation.
- All 20 automated tests passed.
- Output files were generated in the `outputs/` directory.

**Execution limitation:** The results above were produced using a single-process local simulation. They are not results from a Hadoop cluster execution.
