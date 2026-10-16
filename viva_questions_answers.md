# Viva Questions and Answers

**Mini Project:** Smart Campus Analytics using Python Hadoop Streaming

## 1. What are the three major phases of MapReduce?

The three major phases are **Map, Shuffle and Sort, and Reduce**.

- **Map:** Processes input records and emits intermediate key-value pairs.
- **Shuffle and Sort:** Transfers intermediate records to the appropriate Reducers and groups values by key.
- **Reduce:** Aggregates or processes the values for each key and produces the final output.

## 2. What is the difference between Mapper output and Reducer output?

Mapper output consists of intermediate key-value pairs used for grouping and aggregation. Reducer output contains the processed results.

For example, in Job 1, the Mapper emits a date-hour key with data usage, duration, and a count. The Reducer combines the records to calculate hourly totals.

## 3. Why does Hadoop sort Mapper output before it reaches the Reducer?

Sorting places records with the same key together. This allows the Reducer to process all values for one key consecutively.

In Job 2, sorting by access point ensures that all records for `AP-CAFE-01` can be aggregated together.

## 4. What is a Combiner and when is it safe to use one?

A Combiner performs local aggregation on Mapper output before data is transferred across the network. It reduces shuffle traffic.

It is safe when the operation can be combined repeatedly without changing the final result. For example, summing data usage, duration, and session counts is suitable. Calculating final averages directly is generally unsuitable unless the intermediate representation preserves the necessary sum and count.

## 5. What is a Partitioner?

A Partitioner decides which Reducer receives a particular intermediate key. All records with the same key must go to the same Reducer.

The default Hadoop Partitioner commonly uses a hash of the key. A custom Partitioner can distribute keys according to the application's requirements.

## 6. Can a MapReduce job have zero reducers?

Yes. A MapReduce job can run without Reducers, commonly called a map-only job. The Mapper output becomes the job output without the usual reduce aggregation.

However, the Smart Campus jobs that aggregate values by key require Reducers to calculate their totals and averages.

## 7. What happens when there are multiple reducers?

Multiple Reducers process different key partitions in parallel, potentially improving performance.

However, a Reducer only sees the keys assigned to its partition. In Job 1, multiple Reducers could each identify a local peak, but an additional global aggregation stage would be required to determine the campus-wide peak.

## 8. What is a composite key?

A composite key contains multiple fields used together to identify a group.

In Job 6, the key combines `RouteID` and the date-hour, for example `R1|2026-10-05 08`. This allows the application to calculate average speed separately for each route during each hour.

## 9. How would you handle duplicate records?

First, I would define what counts as a duplicate according to the dataset and the problem requirements. If records have unique identifiers, those identifiers can help detect duplicates.

Duplicates can be removed using a separate deduplication stage or by maintaining suitable keys and state. For Job 4, the implementation uses a set of distinct item names within each transaction to prevent repeated items from generating duplicate pairs.

## 10. Why are multiple MapReduce jobs sometimes necessary?

Some problems require intermediate results before the final answer can be calculated.

In Job 4, the first stage generates item pairs per transaction and the second counts pairs across transactions. In Job 5, separate stages calculate student statistics, the overall average, and the students exceeding the threshold.

## 11. What is data locality?

Data locality means moving computation close to where the input data is stored, rather than transferring large amounts of data over the network.

Hadoop can schedule tasks near the machines storing their input blocks, reducing network traffic and potentially improving performance.

## 12. What is the effect of an excessively large key group?

If one key contains a very large number of records, its Reducer may take much longer than others and become a bottleneck. It may also experience increased memory or disk pressure.

A Combiner can reduce the number of intermediate records when the operation is suitable. Other solutions include partitioning independent keys effectively or using multiple aggregation stages.

## 13. How can you reduce network traffic during shuffle?

Network traffic can be reduced by using Combiners for suitable aggregations, filtering invalid or unnecessary records in the Mapper, emitting compact values, and avoiding unnecessary intermediate output.

For example, locally summing Wi-Fi data and session duration reduces the number of records sent to the Reducer.

## 14. How would you test your Mapper independently?

I would supply a small set of sample records to the Mapper through standard input and inspect its standard output.

I would verify the emitted keys and values, check header handling, test malformed records and empty input, and compare the results with manually calculated expectations.

In this project, Mapper and Reducer behaviour is also tested using Python's `unittest` framework. The current test suite contains 20 passing tests.

## 15. How would you modify the solution to work with millions of records?

I would store the datasets in HDFS and run the Python scripts through Hadoop Streaming. I would use multiple Mapper and Reducer tasks, appropriate partitioning, and Combiners where safe to reduce shuffle traffic.

I would also monitor data skew, validate malformed input, and use multiple MapReduce stages for global calculations. Finally, I would test the distributed execution and compare its results against independently calculated expected values.

---

## Project-Specific Results to Remember

- **Job 1:** Peak Wi-Fi usage was 3,032.74 MB at 13:00 on 7 October 2026.
- **Job 2:** AP-CAFE-01 had the highest data usage: 2,213.48 MB.
- **Job 3:** Tea had the highest quantity sold (84); Biryani had the highest revenue (3,760).
- **Job 4:** Samosa::Tea was the most frequent pair, appearing in 49 transactions.
- **Job 5:** S901, S902, and S903 exceeded the library-visit anomaly threshold.
- **Job 6:** 15 route-hour groups were congested. An average speed of exactly 20 km/h is classified as normal.

**Execution note:** The project was verified using a local Python MapReduce simulation. It was not executed on a Hadoop cluster.
