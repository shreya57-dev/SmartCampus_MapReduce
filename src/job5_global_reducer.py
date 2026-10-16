"""Job 5 global reducer: overall average visit duration.

Input values for key ALL: ``total_duration,visit_count``.
overall_average = sum(total_duration) / sum(visit_count).

Output: ``GLOBAL\\t<total_duration>,<visit_count>,<overall_average>``

One reducer is required so every student total reaches the same ALL group.
The local runner and the sample Hadoop command both use a single reducer.
"""

import sys

from streaming_io import emit, format_float, iter_kv, parse_float, warn


def main():
    total_duration = 0.0
    visits = 0
    saw_all = False

    for key, value in iter_kv():
        if key != "ALL":
            warn(f"job5 global reducer: unexpected key {key!r}, skipped")
            continue
        parts = value.split(",")
        if len(parts) != 2:
            warn(f"job5 global reducer: expected duration,count but got {value!r}")
            continue
        duration = parse_float(parts[0])
        count = parse_float(parts[1])
        if duration is None or count is None or duration < 0 or count <= 0:
            warn(f"job5 global reducer: invalid value {value!r}")
            continue
        saw_all = True
        total_duration += duration
        visits += int(count)

    if not saw_all or visits == 0:
        warn("job5 global reducer: no visit totals to average")
        return
    average = total_duration / visits
    emit("GLOBAL", f"{format_float(total_duration)},{visits},{format_float(average)}")


if __name__ == "__main__":
    try:
        main()
    except BrokenPipeError:
        sys.exit(0)
