"""Job 5 filter mapper: prepare the anomaly comparison.

Input is the concatenation of two earlier outputs:

- stage 1: ``StudentID\\ttotal_duration,visit_count,student_average``
- global:  ``GLOBAL\\ttotal_duration,visit_count,overall_average``

Output keys are ordered so the global record sorts first when one reducer
is used:

- ``0|GLOBAL`` then the global totals
- ``1|<StudentID>`` then that student's totals
"""

import sys

from streaming_io import emit, iter_kv, parse_float, warn


def main():
    for key, value in iter_kv():
        parts = value.split(",")
        if len(parts) < 2:
            warn(f"job5 filter mapper: short record {key!r}, skipped")
            continue
        duration = parse_float(parts[0])
        visits = parse_float(parts[1])
        if duration is None or visits is None or visits <= 0:
            warn(f"job5 filter mapper: invalid record {key!r} {value!r}, skipped")
            continue
        if key == "GLOBAL":
            emit("0|GLOBAL", f"{duration},{int(visits)}")
        else:
            emit(f"1|{key}", f"{duration},{int(visits)}")


if __name__ == "__main__":
    try:
        main()
    except BrokenPipeError:
        sys.exit(0)
