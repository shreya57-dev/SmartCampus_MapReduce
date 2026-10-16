"""Job 6 reducer: average speed and congestion flag per route-hour.

Input values: ``speed_sum,count``. The first field is a partial sum of speeds,
not a speed that should be multiplied by the count again.
average_speed = total_speed / reading_count.

The route-hour is CONGESTED when average_speed is strictly below
20 km/h. An average of exactly 20 is NORMAL.

Output value: ``average_speed,reading_count,CONGESTED|NORMAL``
"""

import sys

from streaming_io import CONGESTION_SPEED_KMPH, emit, format_float, iter_kv, parse_float, warn


def parse_value(value):
    parts = value.split(",")
    if len(parts) != 2:
        warn(f"job6: expected speed,count but got {value!r}")
        return None
    speed = parse_float(parts[0])
    count = parse_float(parts[1])
    if speed is None or count is None or speed < 0 or count <= 0:
        warn(f"job6: invalid value {value!r}")
        return None
    return speed, int(count)


def main():
    current_key = None
    total_speed = 0.0
    readings = 0

    def flush():
        if current_key is None or readings == 0:
            return
        average = total_speed / readings
        status = "CONGESTED" if average < CONGESTION_SPEED_KMPH else "NORMAL"
        emit(current_key, f"{format_float(average)},{readings},{status}")

    for key, value in iter_kv():
        parsed = parse_value(value)
        if parsed is None:
            continue
        speed, count = parsed
        if current_key is None:
            current_key = key
        elif key != current_key:
            flush()
            current_key = key
            total_speed = 0.0
            readings = 0
        total_speed += speed
        readings += count

    flush()


if __name__ == "__main__":
    try:
        main()
    except BrokenPipeError:
        sys.exit(0)
