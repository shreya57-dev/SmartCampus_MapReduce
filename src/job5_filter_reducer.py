"""Job 5 filter reducer: students above the anomaly threshold.

The first group must be ``0|GLOBAL``. Its overall average is

    overall_average = total_duration / total_visit_count

A student is emitted only when

    student_average > ANOMALY_FACTOR * overall_average

with ANOMALY_FACTOR = 1.5. Equality is not an anomaly.

Output value:
    total_duration,visit_count,student_average,overall_average,threshold

This comparison is valid for one reducer, which sees the global record
before the student records because the mapper prefixes sort keys with
``0|`` and ``1|``.
"""

import sys

from streaming_io import ANOMALY_FACTOR, emit, format_float, iter_kv, parse_float, warn


def main():
    overall_duration = None
    overall_visits = None
    overall_average = None
    threshold = None

    for key, value in iter_kv():
        parts = value.split(",")
        if len(parts) != 2:
            warn(f"job5 filter reducer: expected duration,count but got {value!r}")
            continue
        duration = parse_float(parts[0])
        visits = parse_float(parts[1])
        if duration is None or visits is None or visits <= 0:
            warn(f"job5 filter reducer: invalid value {value!r}")
            continue

        if key == "0|GLOBAL":
            if overall_average is None:
                overall_duration = duration
                overall_visits = int(visits)
                overall_average = overall_duration / overall_visits
                threshold = ANOMALY_FACTOR * overall_average
            else:
                warn("job5 filter reducer: extra GLOBAL record ignored")
            continue

        if not key.startswith("1|"):
            warn(f"job5 filter reducer: unexpected key {key!r}")
            continue
        if threshold is None:
            warn("job5 filter reducer: student arrived before GLOBAL; cannot filter")
            sys.exit(1)

        student_id = key.split("|", 1)[1]
        student_average = duration / visits
        if student_average > threshold:
            emit(
                student_id,
                ",".join(
                    [
                        format_float(duration),
                        str(int(visits)),
                        format_float(student_average),
                        format_float(overall_average),
                        format_float(threshold),
                    ]
                ),
            )


if __name__ == "__main__":
    try:
        main()
    except BrokenPipeError:
        sys.exit(0)
