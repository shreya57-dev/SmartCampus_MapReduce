"""Job 3 mapper: canteen item frequency.

Input CSV columns: TransactionID, Timestamp, StudentID, Item, Quantity, Amount.
Output key:   Item
Output value: ``quantity,amount``

Amount is the line total already stored in the CSV (unit price x quantity),
so the reducer sums Amount directly and does not multiply again.
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
        if is_header_row(cells, "TransactionID"):
            continue
        if not has_columns(cells, 6, lineno, "canteen"):
            continue
        item = cells[3].strip()
        when = parse_timestamp(cells[1])
        quantity = parse_float(cells[4])
        amount = parse_float(cells[5])
        if not item:
            warn(f"canteen line {lineno}: empty item, skipped")
            continue
        if when is None:
            warn(f"canteen line {lineno}: bad timestamp {cells[1]!r}, skipped")
            continue
        if quantity is None or amount is None or quantity <= 0 or amount < 0:
            warn(f"canteen line {lineno}: invalid quantity or amount, skipped")
            continue
        emit(item, f"{format_float(quantity)},{format_float(amount)}")


if __name__ == "__main__":
    try:
        main()
    except BrokenPipeError:
        sys.exit(0)
