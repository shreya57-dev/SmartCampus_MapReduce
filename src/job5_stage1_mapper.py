"""Job 5 stage 1 mapper: library visit durations.

Input CSV columns: StudentID, Timestamp, EntryType, DurationMin.

How ENTRY and EXIT are interpreted
---------------------------------
- ENTRY marks an arrival. Its DurationMin is ignored, even when it is 0.
- EXIT closes a visit. It counts only when DurationMin is numeric and
  strictly greater than 0. That positive EXIT is one valid visit-duration
  record.
- Any other EntryType is skipped.
- Duration is taken from the DurationMin column. It is not recomputed from
  the timestamp difference, so an EXIT cannot be double-counted by also
  pairing it with the previous ENTRY.

Output key:   StudentID
Output value: ``duration_min,1``
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
        if not has_columns(cells, 4, lineno, "library"):
            continue
        student_id = cells[0].strip()
        when = parse_timestamp(cells[1])
        entry_type = cells[2].strip().upper()
        duration = parse_float(cells[3])
        if not student_id:
            warn(f"library line {lineno}: empty student id, skipped")
            continue
        if when is None:
            warn(f"library line {lineno}: bad timestamp {cells[1]!r}, skipped")
            continue
        if entry_type != "EXIT":
            continue
        if duration is None or duration <= 0:
            warn(f"library line {lineno}: EXIT without a positive duration, skipped")
            continue
        emit(student_id, f"{format_float(duration)},1")


if __name__ == "__main__":
    try:
        main()
    except BrokenPipeError:
        sys.exit(0)
