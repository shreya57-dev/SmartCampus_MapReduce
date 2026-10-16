"""Job 1 reducer: peak Wi-Fi usage.

Input is sorted and grouped by ``YYYY-MM-DD HH``. Consecutive equal keys are
aggregated one value at a time. Values are ``data_mb,duration_min,count``.

Output:
    <hour>\\t<data_mb>,<duration_min>,<sessions>
    PEAK\\t<hour>,<data_mb>,<duration_min>,<sessions>

The PEAK line is the hour with the greatest total data. Ties keep the
earlier hour key. This line is correct when the job uses one reducer,
because only then does a single process see every hour. On a cluster,
set ``-numReduceTasks 1`` or add a second job that finds the maximum.
"""

import sys

from streaming_io import emit, format_float, iter_kv, parse_float, warn


def parse_value(value, label):
    parts = value.split(",")
    if len(parts) != 3:
        warn(f"{label}: expected data,duration,count but got {value!r}")
        return None
    data_mb = parse_float(parts[0])
    duration = parse_float(parts[1])
    count = parse_float(parts[2])
    if data_mb is None or duration is None or count is None:
        warn(f"{label}: non-numeric value {value!r}")
        return None
    if data_mb < 0 or duration < 0 or count <= 0:
        warn(f"{label}: invalid value {value!r}")
        return None
    return data_mb, duration, int(count)


def main():
    current_key = None
    total_data = 0.0
    total_duration = 0.0
    sessions = 0
    peak = None

    def flush():
        nonlocal peak
        if current_key is None or sessions == 0:
            return
        emit(
            current_key,
            f"{format_float(total_data)},{format_float(total_duration)},{sessions}",
        )
        # Strictly greater total data wins. An equal total keeps the earlier key.
        if peak is None or total_data > peak[0]:
            peak = (total_data, current_key, total_duration, sessions)

    for key, value in iter_kv():
        parsed = parse_value(value, "job1")
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
    if peak is not None:
        emit(
            "PEAK",
            f"{peak[1]},{format_float(peak[0])},{format_float(peak[2])},{peak[3]}",
        )


if __name__ == "__main__":
    try:
        main()
    except BrokenPipeError:
        sys.exit(0)
