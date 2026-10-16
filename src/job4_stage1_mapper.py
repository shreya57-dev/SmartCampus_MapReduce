"""Job 4 stage 1 mapper: co-purchased items.

Input CSV columns: TransactionID, Timestamp, StudentID, Item, Quantity, Amount.
Output key:   TransactionID
Output value: Item

One transaction is stored as several CSV rows. Grouping by TransactionID
collects the basket before pairs are generated.
"""

import sys

from streaming_io import (
    emit,
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
        transaction_id = cells[0].strip()
        item = cells[3].strip()
        when = parse_timestamp(cells[1])
        quantity = parse_float(cells[4])
        if not transaction_id or not item:
            warn(f"canteen line {lineno}: missing transaction or item, skipped")
            continue
        if when is None:
            warn(f"canteen line {lineno}: bad timestamp {cells[1]!r}, skipped")
            continue
        if quantity is None or quantity <= 0:
            warn(f"canteen line {lineno}: invalid quantity, skipped")
            continue
        if "::" in item:
            warn(f"canteen line {lineno}: item {item!r} contains '::', skipped")
            continue
        emit(transaction_id, item)


if __name__ == "__main__":
    try:
        main()
    except BrokenPipeError:
        sys.exit(0)
