"""Job 5 global mapper: feed per-student totals into one global key.

Input is stage 1 output: ``StudentID\\ttotal_duration,visit_count,average``.
Output key:   ALL
Output value: ``total_duration,visit_count``

The student average is not forwarded. The global reducer must divide the
sum of durations by the sum of visits, which is not the mean of the averages.
"""

import sys

from streaming_io import emit, format_float, iter_kv, parse_float, warn


def main():
    for key, value in iter_kv():
        parts = value.split(",")
        if len(parts) < 2:
            warn(f"job5 global mapper: short record for {key}: {value!r}")
            continue
        duration = parse_float(parts[0])
        visits = parse_float(parts[1])
        if duration is None or visits is None or duration < 0 or visits <= 0:
            warn(f"job5 global mapper: invalid record for {key}: {value!r}")
            continue
        emit("ALL", f"{format_float(duration)},{int(visits)}")


if __name__ == "__main__":
    try:
        main()
    except BrokenPipeError:
        sys.exit(0)
