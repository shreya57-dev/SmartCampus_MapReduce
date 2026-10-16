"""Job 4 stage 2 mapper: pass pair counts through.

Input and output are ``item_a::item_b\\tcount``. The mapper checks the
record and emits it unchanged so the reducer can sum counts for each pair.
"""

import sys

from streaming_io import emit, iter_kv, parse_float, warn


def main():
    for key, value in iter_kv():
        if "::" not in key:
            warn(f"job4 stage2: pair key {key!r} has no '::', skipped")
            continue
        left, right = key.split("::", 1)
        if not left or not right or left >= right:
            warn(f"job4 stage2: pair {key!r} is not a sorted unique pair, skipped")
            continue
        count = parse_float(value)
        if count is None or count <= 0:
            warn(f"job4 stage2: invalid count {value!r}, skipped")
            continue
        emit(key, str(int(count)))


if __name__ == "__main__":
    try:
        main()
    except BrokenPipeError:
        sys.exit(0)
