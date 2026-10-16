"""Job 4 stage 1 reducer: unordered item pairs inside one transaction.

Input values for one TransactionID are item names. The reducer keeps the
distinct names, sorts them, and emits every unique unordered pair.

Pair key: ``<earlier item>::<later item>``
Pair value: ``1`` (this transaction contains the pair once)

Sorting the two names makes ``Tea + Samosa`` and ``Samosa + Tea`` the same
key. A repeated item on the same bill is inserted only once, so it is not
paired with itself. A one-item bill emits nothing.

Only the current transaction's items are held in memory.
"""

import sys
from itertools import combinations

from streaming_io import emit, iter_kv


def main():
    current_key = None
    seen = set()

    def flush():
        if current_key is None or len(seen) < 2:
            return
        ordered = sorted(seen)
        for left, right in combinations(ordered, 2):
            emit(f"{left}::{right}", "1")

    for key, value in iter_kv():
        item = value.strip()
        if not item:
            continue
        if current_key is None:
            current_key = key
        elif key != current_key:
            flush()
            current_key = key
            seen = set()
        seen.add(item)

    flush()


if __name__ == "__main__":
    try:
        main()
    except BrokenPipeError:
        sys.exit(0)
