"""Job 2 mapper: access-point utilisation.

Input CSV columns: StudentID, Timestamp, AccessPoint, DataMB, DurationMin.
Output key:   AccessPoint
Output value: ``data_mb,duration_min,1``
"""

import sys

from streaming_io import (
    emit,
    format_float,
    has_columns,
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
        access_point = cells[2].strip()
        when = parse_timestamp(cells[1])
        data_mb = parse_float(cells[3])
        duration = parse_float(cells[4])
        if not access_point:
            warn(f"wifi line {lineno}: empty access point, skipped")
            continue
        if when is None:
            warn(f"wifi line {lineno}: bad timestamp {cells[1]!r}, skipped")
            continue
        if data_mb is None or duration is None or data_mb < 0 or duration < 0:
            warn(f"wifi line {lineno}: invalid data or duration, skipped")
            continue
        emit(access_point, f"{format_float(data_mb)},{format_float(duration)},1")


if __name__ == "__main__":
    try:
        main()
    except BrokenPipeError:
        sys.exit(0)
