"""Job 4 stage 2 reducer: distinct transactions per item pair.

Consecutive values are counts. Their sum is the number of distinct
transactions that contained the pair, because stage 1 emits 1 once per
transaction.

Output value: integer transaction count.
"""

import sys

from streaming_io import emit, iter_kv, parse_float, warn


def main():
    current_key = None
    total = 0

    def flush():
        if current_key is None or total <= 0:
            return
        emit(current_key, str(total))

    for key, value in iter_kv():
        count = parse_float(value)
        if count is None or count <= 0:
            warn(f"job4 stage2 reducer: invalid count {value!r}")
            continue
        if current_key is None:
            current_key = key
        elif key != current_key:
            flush()
            current_key = key
            total = 0
        total += int(count)

    flush()


if __name__ == "__main__":
    try:
        main()
    except BrokenPipeError:
        sys.exit(0)
