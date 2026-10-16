"""Job 2 reducer: access-point utilisation.

For each access point, sum data, duration, and session count from consecutive
values ``data_mb,duration_min,count``.

Average session duration = total duration / session count.

Output value: ``data_mb,duration_min,sessions,avg_duration_min``
"""

import sys

from streaming_io import emit, format_float, iter_kv, parse_float, warn


def parse_value(value):
    parts = value.split(",")
    if len(parts) != 3:
        warn(f"job2: expected data,duration,count but got {value!r}")
        return None
    data_mb = parse_float(parts[0])
    duration = parse_float(parts[1])
    count = parse_float(parts[2])
    if None in (data_mb, duration, count) or data_mb < 0 or duration < 0 or count <= 0:
        warn(f"job2: invalid value {value!r}")
        return None
    return data_mb, duration, int(count)


def main():
    current_key = None
    total_data = 0.0
    total_duration = 0.0
    sessions = 0

    def flush():
        if current_key is None or sessions == 0:
            return
        average = total_duration / sessions
        emit(
            current_key,
            f"{format_float(total_data)},{format_float(total_duration)},{sessions},{format_float(average)}",
        )

    for key, value in iter_kv():
        parsed = parse_value(value)
        if parsed is None:
            continue
        data_mb, duration, count = parsed
        if current_key is None:
            current_key = key
        elif key != current_key:
            flush()
            current_key = key
            total_data = 0.0
            total_duration = 0.0
            sessions = 0
        total_data += data_mb
        total_duration += duration
        sessions += count

    flush()


if __name__ == "__main__":
    try:
        main()
    except BrokenPipeError:
        sys.exit(0)
