# Student Implementation Worksheet — Answers

**Mini Project:** Smart Campus Analytics using Python Hadoop Streaming

**Execution environment:** Python-based local MapReduce simulation using `run_all.py`. The project has been tested successfully with 20 passing automated tests.

## Q1. For Job 1, what should the Mapper output key be? Why?

**Answer:** The Mapper output key should be the date and hour extracted from the Wi-Fi session timestamp, in the format `YYYY-MM-DD HH`.

For example, `2026-10-07 13`.

This groups all Wi-Fi sessions occurring during the same hour on the same date, allowing the Reducer to calculate total data consumption, total session duration, and session count for that hour.

## Q2. What is the Mapper output value? Can you design a more efficient representation?

**Answer:** The Mapper emits the value `DataMB,DurationMin,1`, where `1` represents one Wi-Fi session.

For example:

`2026-10-07 13    120.0,35.0,1`

A more efficient representation for aggregation is a compact tuple of numeric partial sums `(total_data, total_duration, session_count)`. A Combiner can aggregate these values locally before they are transferred to the Reducer, reducing the number of intermediate records sent during shuffle.

## Q3. Why is the hour extracted from the timestamp instead of using the complete timestamp as the key?

**Answer:** The objective is to analyse Wi-Fi usage per hour, not per individual timestamp. If the complete timestamp were used, sessions occurring at different minutes within the same hour would be assigned different keys.

Extracting the hour groups those sessions together and enables the Reducer to calculate meaningful hourly totals.

## Q4. Which operations can be performed using a Combiner?

**Answer:** A Combiner can perform associative aggregation operations, such as summing data usage, session durations, session counts, canteen quantities, revenue, pair counts, and transport speed sums and counts.

For example, Job 1 can locally combine:

`(DataMB1, Duration1, 1) + (DataMB2, Duration2, 1)`

into:

`(DataMB1 + DataMB2, Duration1 + Duration2, 2)`

A Combiner must preserve a format that can be aggregated again. Final averages, global peak selection, and congestion classification should not be calculated directly in a sum Combiner because they require complete grouped totals.

## Q5. For Job 2, define the key and value emitted by the Mapper.

**Answer:** For Access Point Utilisation, the Mapper emits:

- **Key:** `AccessPoint`
- **Value:** `DataMB,DurationMin,1`

For example:

`AP-CAFE-01    100.0,30.0,1`

The Reducer groups records by access point and calculates total data, total duration, number of sessions, and average session duration.

The average session duration is:

`TotalDurationMin / TotalSessions`

## Q6. For Job 4, explain how you will generate every unique pair of products within a transaction.

**Answer:** The first MapReduce stage groups canteen records by `TransactionID`.

For each transaction, the Reducer collects distinct item names into a set, sorts them, and generates every combination of two different items.

For example, a transaction containing Tea, Samosa, and Juice produces three unique pairs:

- `Juice::Samosa`
- `Juice::Tea`
- `Samosa::Tea`

Each pair is emitted with a count of `1`. The second MapReduce stage sums these counts across transactions.

Using a set prevents repeated occurrences of the same item in one transaction from generating duplicate pairs.

## Q7. Why must the product pair be sorted, e.g., (Coffee, Sandwich) rather than sometimes (Sandwich, Coffee)?

**Answer:** Product pairs are unordered because purchasing Coffee and Sandwich is the same combination as purchasing Sandwich and Coffee.

Sorting the two item names produces a consistent key, such as `Coffee::Sandwich`, regardless of their order in the original transaction.

Without sorting, the two arrangements would be counted as separate pairs, producing incorrect co-purchase frequencies.

## Q8. For Job 5, how will you compute the overall average using more than one MapReduce job?

**Answer:** Job 5 uses multiple MapReduce stages.

**Stage 1:** Group valid library EXIT records by `StudentID`. Calculate each student's total visit duration and number of completed visits.

**Stage 2:** Aggregate the per-student totals into campus-wide total duration and total visit count. Calculate:

`OverallAverage = TotalDuration / TotalVisitCount`

**Stage 3:** Compare each student's average visit duration with `1.5 × OverallAverage`. Output students whose average is strictly greater than this threshold.

In the generated dataset, the overall average is **56.086957 minutes**, and the threshold is **84.130435 minutes**. Students S901, S902, and S903 exceed this threshold.

## Q9. For Job 6, explain why RouteID and Hour form a composite key.

**Answer:** Transport congestion must be analysed separately for each route during each hour.

Therefore, the Mapper uses a composite key combining `RouteID` and the date-hour, such as:

`R1|2026-10-05 08`

This ensures that speed readings for different routes or different hours are not mixed.

The Reducer calculates average speed for each route-hour group and labels it `CONGESTED` when the average is strictly below 20 km/h; otherwise, it labels the group `NORMAL`.

## Q10. Which jobs require more than one MapReduce stage? Justify your answer.

**Answer:** Jobs 4 and 5 require multiple MapReduce stages in the implemented solution.

**Job 4 — Top Co-Purchased Items:** The first stage groups items by transaction and generates unique pairs. The second stage aggregates how frequently each pair occurs across transactions.

**Job 5 — Library Usage Anomaly:** The first stage calculates student-level visit statistics, the second calculates the campus-wide average, and the third compares individual student averages against the global threshold.

Job 1 can calculate hourly totals and identify the peak using one Reducer. If multiple Reducers are used for hourly aggregation, an additional global maximum stage is necessary to determine the campus-wide peak correctly.

## Q11. What happens if a reducer receives millions of values for one key?

**Answer:** The Reducer responsible for that key may become a performance bottleneck. It can take much longer than other Reducers, increase disk and memory pressure, and delay completion of the entire job.

A streaming Reducer can process values incrementally without storing every value in memory, but it still must process all records for the key.

Possible optimisations include using a Combiner for partial aggregation, reducing shuffle traffic, and splitting suitable large aggregations into multiple stages.

## Q12. Explain how data skew could affect the Smart Campus application.

**Answer:** Data skew occurs when some keys receive significantly more records than others.

For example, a heavily used campus Wi-Fi access point may generate many more sessions than other access points. Similarly, popular canteen item pairs may appear in far more transactions than uncommon pairs.

Because all values for the same key must reach the same Reducer, a heavily loaded key can cause one Reducer to work much longer than others, reducing parallel efficiency.

Combiners, careful partitioning, and multi-stage aggregation can help mitigate skew.

## Q13. Design a custom partitioning strategy for Job 6.

**Answer:** A custom Partitioner could assign transport records to Reducers based on `RouteID` while preserving the full `RouteID|DateHour` key for aggregation.

For example:

`Partition = hash(RouteID) % NumberOfReducers`

This sends all hourly records for a particular route to the same Reducer while keeping different route-hour combinations as separate keys.

The advantage is that each Reducer can process a consistent subset of routes. However, if one route produces substantially more readings than others, this approach may cause data skew. In that case, hashing the complete route-hour key can distribute the workload more evenly.

This is a proposed cluster optimisation, not a custom partitioner implemented in the current local simulation.

## Q14. What changes are necessary if the input contains malformed records?

**Answer:** The Mapper should validate each input record before processing it.

Necessary checks include:

- Skipping CSV headers and records with missing required fields.
- Parsing quoted CSV fields correctly.
- Validating timestamps and numeric fields.
- Rejecting invalid values, such as negative speed or non-positive completed library visit duration.
- Writing diagnostic warnings to standard error instead of standard output.
- Skipping invalid records while continuing to process valid records.

The current project already performs input validation for its supported CSV formats. For larger datasets, rejected-record counters and separate error logs could be added to monitor data quality.

## Q15. Suggest one additional campus analytics problem that can be solved using MapReduce.

**Answer:** An additional application is **Campus Classroom Occupancy Analysis**.

A university could analyse classroom entry records to determine which classrooms experience the highest occupancy during each hour.

The Mapper could emit:

- **Key:** `ClassroomID|DateHour`
- **Value:** `1`

The Reducer would sum the values to obtain the number of recorded entries for each classroom-hour.

If reliable entry and exit events are available, a more advanced multi-stage solution could calculate actual occupancy over time.

This analysis could help the university identify heavily used classrooms, improve room allocation, and plan facility resources.

---

## Execution and Validation Note

The six required jobs were executed using the local Python MapReduce simulation. The generated results include:

- **Job 1:** Peak Wi-Fi usage of 3,032.74 MB on 7 October 2026 at 13:00.
- **Job 2:** AP-CAFE-01 had the highest data usage, at 2,213.48 MB.
- **Job 3:** Tea had the highest quantity sold (84 units); Biryani had the highest revenue (3,760).
- **Job 4:** Samosa::Tea was the most frequent co-purchased pair (49 transactions).
- **Job 5:** Three anomalous students were identified: S901, S902, and S903.
- **Job 6:** 15 route-hour groups were classified as congested.

All **20 automated tests passed**.

**Important:** These results were produced by local simulation, not by executing a distributed Hadoop cluster.
