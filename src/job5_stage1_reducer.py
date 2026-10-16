"""Job 5 stage 1 reducer: total duration and visit count per student.

Input values: ``duration_min,count``.
Output value: ``total_duration,visit_count,student_average``.

student_average = total_duration / visit_count.
"""

import sys

from streaming_io import emit, format_float, iter_kv, parse_float, warn


def parse_value(value):
    parts = value.split(",")
    if len(parts) != 2:
        warn(f"job5 stage1: expected duration,count but got {value!r}")
        return None
    duration = parse_float(parts[0])
    count = parse_float(parts[1])
    if duration is None or count is None or duration <= 0 or count <= 0:
        warn(f"job5 stage1: invalid value {value!r}")
        return None
    return duration, int(count)


def main():
    current_key = None
    total_duration = 0.0
    visits = 0

    def flush():
        if current_key is None or visits == 0:
            return
        average = total_duration / visits
        emit(
            current_key,
            f"{format_float(total_duration)},{visits},{format_float(average)}",
        )

    for key, value in iter_kv():
        parsed = parse_value(value)
        if parsed is None:
            continue
        duration, count = parsed
        if current_key is None:
            current_key = key
        elif key != current_key:
            flush()
            current_key = key
            total_duration = 0.0
            visits = 0
        total_duration += duration
        visits += count

    flush()


if __name__ == "__main__":
    try:
        main()
    except BrokenPipeError:
        sys.exit(0)
