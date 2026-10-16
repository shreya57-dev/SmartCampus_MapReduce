"""Job 1 mapper: peak Wi-Fi usage.

Input CSV columns: StudentID, Timestamp, AccessPoint, DataMB, DurationMin.
Output key:   date and hour, ``YYYY-MM-DD HH``
Output value: ``data_mb,duration_min,1``

Each input row is one Wi-Fi session. The trailing 1 lets the reducer count
sessions while summing data and duration, including after a combiner.
"""

import sys

from streaming_io import (
    emit,
    format_float,
    has_columns,
    hour_bucket,
    is_header_row,
    iter_csv_rows,
    parse_float,
    parse_timestamp,
    warn,
)


def main():
    for lineno, cells in iter_csv_rows():
        if is_header_row(cells, "StudentID"):
            continue
        if not has_columns(cells, 5, lineno, "wifi"):
            continue
        when = parse_timestamp(cells[1])
        data_mb = parse_float(cells[3])
        duration = parse_float(cells[4])
        if when is None:
            warn(f"wifi line {lineno}: bad timestamp {cells[1]!r}, skipped")
            continue
        if data_mb is None or duration is None:
            warn(f"wifi line {lineno}: non-numeric data or duration, skipped")
            continue
        if data_mb < 0 or duration < 0:
            warn(f"wifi line {lineno}: negative data or duration, skipped")
            continue
        emit(hour_bucket(when), f"{format_float(data_mb)},{format_float(duration)},1")


if __name__ == "__main__":
    try:
        main()
    except BrokenPipeError:
        sys.exit(0)
