"""Job 6 mapper: transport congestion.

Input CSV columns: VehicleID, Timestamp, RouteID, SpeedKmph.
Output key:   ``<RouteID>|<YYYY-MM-DD HH>``
Output value: ``speed_sum,1`` (the speed itself is the partial sum of one reading)

The composite key keeps route and date-hour together so the reducer can
average every reading that belongs to the same route-hour.
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
        if is_header_row(cells, "VehicleID"):
            continue
        if not has_columns(cells, 4, lineno, "transport"):
            continue
        route_id = cells[2].strip()
        when = parse_timestamp(cells[1])
        speed = parse_float(cells[3])
        if not route_id:
            warn(f"transport line {lineno}: empty route, skipped")
            continue
        if "|" in route_id:
            warn(f"transport line {lineno}: route {route_id!r} contains '|', skipped")
            continue
        if when is None:
            warn(f"transport line {lineno}: bad timestamp {cells[1]!r}, skipped")
            continue
        if speed is None or speed < 0:
            warn(f"transport line {lineno}: invalid speed, skipped")
            continue
        emit(f"{route_id}|{hour_bucket(when)}", f"{format_float(speed)},1")


if __name__ == "__main__":
    try:
        main()
    except BrokenPipeError:
        sys.exit(0)
