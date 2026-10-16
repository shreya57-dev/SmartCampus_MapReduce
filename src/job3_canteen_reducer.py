"""Job 3 reducer: canteen item frequency.

Consecutive values for one item are ``quantity,amount``.
Output value: ``total_quantity,total_revenue``.
"""

import sys

from streaming_io import emit, format_float, iter_kv, parse_float, warn


def parse_value(value):
    parts = value.split(",")
    if len(parts) != 2:
        warn(f"job3: expected quantity,amount but got {value!r}")
        return None
    quantity = parse_float(parts[0])
    amount = parse_float(parts[1])
    if quantity is None or amount is None or quantity <= 0 or amount < 0:
        warn(f"job3: invalid value {value!r}")
        return None
    return quantity, amount


def main():
    current_key = None
    total_quantity = 0.0
    total_revenue = 0.0
    rows = 0

    def flush():
        if current_key is None or rows == 0:
            return
        emit(current_key, f"{format_float(total_quantity)},{format_float(total_revenue)}")

    for key, value in iter_kv():
        parsed = parse_value(value)
        if parsed is None:
            continue
        quantity, amount = parsed
        if current_key is None:
            current_key = key
        elif key != current_key:
            flush()
            current_key = key
            total_quantity = 0.0
            total_revenue = 0.0
            rows = 0
        total_quantity += quantity
        total_revenue += amount
        rows += 1

    flush()


if __name__ == "__main__":
    try:
        main()
    except BrokenPipeError:
        sys.exit(0)
