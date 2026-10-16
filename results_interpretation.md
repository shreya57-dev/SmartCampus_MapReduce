# Interpretation of results

These figures come from the local MapReduce simulation `python run_all.py`, using the synthetic CSVs generated with seed 42. Python 3.12.3 produced the files in `outputs/`. The patterns below describe that demonstration week (5–9 October 2026). They are not measurements of a real campus, and they are not the output of a Hadoop cluster.

The full machine-written summary is `outputs/summary.txt`.

## Job 1 — Peak Wi-Fi usage

The job built 65 date-hour groups, which is one group for every hour from 08:00 through 20:00 on each of the five weekdays.

The peak line is:

```text
PEAK    2026-10-07 13,3032.740000,1111.000000,20
```

Wednesday 7 October 2026 at 13:00 is the peak hour. Twenty sessions transferred 3032.74 MB and stayed connected for 1111 minutes in total. A normal hour in the same file is much smaller. The first hour in the output, Monday 08:00, is only 60.23 MB across 4 sessions. The peak hour was generated as a heavy lunch-hour load, and the MapReduce total confirms it is the unique maximum: every other hour has at most four sessions of at most 28 MB each, so none of them can approach 3000 MB.

Operational reading for the sample: capacity planning would look at Wednesday early afternoon first. The `PEAK` record is trustworthy in this run because one reducer saw all 65 hours. Session count and duration are reported beside the data total, but the ranking itself uses data volume only.

## Job 2 — Access point utilisation

Five access points appear.

| Access point | Total data (MB) | Total duration (min) | Sessions | Average session (min) |
| --- | ---: | ---: | ---: | ---: |
| AP-CAFE-01 | 2213.480000 | 1731.000000 | 52 | 33.288462 |
| AP-HOSTEL-A | 1442.330000 | 1309.000000 | 40 | 32.725000 |
| AP-LIB-01 | 1186.060000 | 1282.000000 | 41 | 31.268293 |
| AP-LIB-02 | 673.010000 | 1152.000000 | 46 | 25.043478 |
| AP-ADMIN-01 | 584.170000 | 980.000000 | 36 | 27.222222 |

`AP-CAFE-01` carries the most data and the most sessions. `AP-LIB-02` has more sessions than `AP-HOSTEL-A` (46 against 40) but far less data (673 MB against 1442 MB), and its average session is the shortest at about 25 minutes. Data volume and session count are different utilisation measures. An access point can be busy in headcount and still move less traffic if each session is light.

The average is total connected minutes divided by the number of sessions, so a few long sessions pull it up. It is not the average of an average.

For the sample week, the cafe and hostel points are the first places to check radio capacity. The admin point is the quietest by data.

## Job 3 — Canteen item frequency

Nine items were sold.

| Item | Quantity | Revenue |
| --- | ---: | ---: |
| Biryani | 47 | 3760.00 |
| Juice | 56 | 1680.00 |
| Sandwich | 37 | 1480.00 |
| Samosa | 69 | 1380.00 |
| Tea | 84 | 840.00 |
| Coffee | 42 | 630.00 |
| Maggi | 9 | 360.00 |
| Idli | 11 | 275.00 |
| Chai, Cutting | 14 | 112.00 |

Tea is the volume leader and only the fifth item by revenue. Biryani is the revenue leader from fewer units because each line is more expensive. A canteen that restocked from quantity alone would over-order tea relative to the money it brings in. A canteen that restocked from revenue alone would under-prepare tea, samosa, and juice, which are the high-count items.

`Chai, Cutting` is present as its own item. The comma inside the name did not split the field, and its 14 units are a separate total from `Tea`.

Revenue is the sum of CSV line amounts. Those amounts were already price times quantity when the file was created.

## Job 4 — Top co-purchased items

Nine unordered pairs occur in at least one multi-item bill. Ranked by distinct transactions:

| Pair | Transactions |
| --- | ---: |
| Samosa::Tea | 49 |
| Biryani::Juice | 38 |
| Coffee::Sandwich | 26 |
| Juice::Tea | 20 |
| Biryani::Tea | 10 |
| Chai, Cutting::Samosa | 10 |
| Juice::Samosa | 10 |
| Coffee::Idli | 7 |
| Maggi::Tea | 6 |

`Samosa::Tea` is the dominant combination, in 49 different bills. `Biryani::Juice` is second. `Coffee::Sandwich` is a stable third. The map output file itself is alphabetical (`Biryani::Juice` first, `Samosa::Tea` last) because Hadoop order is key order, not count order. The ranking above is the reporting order.

The pair key always puts the alphabetically earlier name on the left, so the reverse basket is not a second product. Transaction `T00141` (student S008, 6 October 2026, 11:05) contains two Tea lines and one Samosa line. That bill adds 1 to `Samosa::Tea`, not 2, and it does not create a Tea–Tea pair. Single-item sandwich bills add quantity and revenue in Job 3 and add no pair here.

For the sample menu, a useful placement is tea beside samosas, juice beside biryani, and coffee beside sandwiches. That is a description of this generated week, not a market study.

## Job 5 — Library usage anomaly

The gate log has 139 rows. After the ENTRY/EXIT rule, 69 of them are valid visits belonging to 23 students. Total duration is 3870 minutes.

```text
overall average = 3870 / 69 = 56.086957 minutes
threshold       = 1.5 × 56.086957 = 84.130435 minutes
```

A student is reported only when their own average visit is strictly longer than 84.130435 minutes.

| Student | Total minutes | Visits | Average minutes | Decision |
| --- | ---: | ---: | ---: | --- |
| S901 | 495 | 3 | 165 | above threshold |
| S902 | 555 | 3 | 185 | above threshold |
| S903 | 615 | 3 | 205 | above threshold |
| S001–S020 | (each average is between 30 and 45) | 3 each | at most 45 | not listed |
| S021 | no valid visit | 0 | — | absent |

S901, S902, and S903 stay about three to four times as long as a typical visit in this file. They are the anomaly list in `outputs/job5_anomalies.txt`. The twenty ordinary students sit well below the cut, so the 1.5× rule is not flagging normal variation in this sample.

S021's only record is an ENTRY at 09:00 on 5 October 2026 with duration 0. The student may have failed to tap out. The job does not invent a duration for that event, and S021 does not change the campus average.

The 56-minute campus figure is weighted by visits. It is not the mean of the 23 student averages. That distinction matters if some students visit once and others visit every day. Here every counted student has three visits, so the two formulas would be close, but the implementation still divides the grand totals, which is the required definition.

## Job 6 — Transport congestion

The job produced 180 route-hour groups: 3 routes × 5 days × 12 hours (08:00–19:00). Each group contains 8 readings (4 quarter-hours × 2 vehicles).

- 15 groups are `CONGESTED` because their average speed is strictly below 20 km/h.
- 165 groups are `NORMAL`.

Congested pattern, repeated on every weekday in the file:

- Route R1 at 08:00 and at 09:00 (10 groups). Sample: `R1|2026-10-05 08` averages 14.481250 km/h.
- Route R2 at 17:00 (5 groups).

R1's slow period is the morning arrival window. R2's slow period is the 17:00 departure window. The averages sit around 14–15 km/h, not just under the cut by a rounding error.

Boundary check, route R3 at 12:00 on all five days:

```text
R3|2026-10-05 12    20.000000,8,NORMAL
R3|2026-10-06 12    20.000000,8,NORMAL
R3|2026-10-07 12    20.000000,8,NORMAL
R3|2026-10-08 12    20.000000,8,NORMAL
R3|2026-10-09 12    20.000000,8,NORMAL
```

Exactly 20 km/h is normal, which matches the rule "strictly below 20". The other R3 hours, and the midday and evening hours of R1 outside 08:00–09:00, stay in the 30–46 km/h band and are normal.

Because every group has the same number of readings, the flag is caused by speed, not by one hour having more samples. A real GPS feed would also vary the sample count; this reducer already divides by the actual count, so that case would still be an average.

For the sample timetable, the congestion response is specific: extra time or an extra bus on R1 in the morning peak, and on R2 at 17:00. R3 at noon is the control case that must not be flagged.
