"""Shared stdin/stdout helpers for Hadoop Streaming Python scripts.

Hadoop Streaming protocol used by every job:
- The mapper reads records from stdin and writes one tab-separated
  record per line to stdout: ``<key>\\t<value>``.
- The shuffle/sort phase groups identical keys together and presents
  those groups to the reducer in sorted key order.
- The reducer reads already grouped key/value lines from stdin and
  writes aggregated ``<key>\\t<value>`` lines to stdout.
- Diagnostics go to stderr only, so they never become part of the data.

These helpers use only the Python standard library.
"""

import csv
import sys
from datetime import datetime

TIMESTAMP_FORMATS = ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M")

# Library anomaly rule: student average visit duration must be strictly
# greater than this factor times the overall average visit duration.
ANOMALY_FACTOR = 1.5

# A route-hour is congested when its average speed is strictly below this
# value, in kilometres per hour.
CONGESTION_SPEED_KMPH = 20.0


def warn(message):
    """Write a diagnostic to stderr. Never use stdout for this."""
    sys.stderr.write(f"WARN: {message}\n")


def emit(key, value):
    """Write one Hadoop Streaming record. The key must not contain a tab."""
    key = str(key)
    value = str(value)
    if ("\t" in key) or ("\n" in key) or ("\r" in key):
        warn(f"skipping record because the key is not a single field: {key!r}")
        return
    if ("\n" in value) or ("\r" in value):
        warn(f"skipping record because the value contains a newline: {value!r}")
        return
    sys.stdout.write(f"{key}\t{value}\n")


def iter_kv(stream=None):
    """Yield ``(key, value)`` pairs from tab-separated reducer input.

    Blank lines and lines without a tab are reported and skipped. This
    function does not group keys. Each reducer walks consecutive equal
    keys itself, which is the Hadoop Streaming contract.
    """
    if stream is None:
        stream = sys.stdin
    for lineno, raw in enumerate(stream, start=1):
        line = raw.strip("\r\n")
        if not line.strip():
            continue
        if "\t" not in line:
            warn(f"line {lineno}: missing tab separator, skipped")
            continue
        key, value = line.split("\t", 1)
        if key == "":
            warn(f"line {lineno}: empty key, skipped")
            continue
        yield key, value.strip()


def iter_csv_rows(stream=None):
    """Yield ``(line_number, cells)`` from a CSV stream, including the header.

    ``csv.reader`` handles quoted fields, so a comma inside quotes stays
    inside one column. Empty rows are skipped.
    """
    if stream is None:
        stream = sys.stdin
    reader = csv.reader(stream)
    for lineno, row in enumerate(reader, start=1):
        cells = [cell.strip() for cell in row]
        if not any(cells):
            continue
        yield lineno, cells


def is_header_row(cells, first_column_name):
    """Return True when this row is the CSV header for the expected schema."""
    return bool(cells) and cells[0].lower() == first_column_name.lower()


def has_columns(cells, minimum, lineno, label):
    """Return False and warn when a row is shorter than the schema."""
    if len(cells) < minimum:
        warn(
            f"{label} line {lineno}: expected at least {minimum} columns, "
            f"found {len(cells)}, skipped"
        )
        return False
    return True


def parse_float(token):
    """Parse a float, or return None when the token is not numeric."""
    try:
        return float(token.strip())
    except (AttributeError, TypeError, ValueError):
        return None


def parse_timestamp(token):
    """Parse a campus timestamp, or return None when it does not match."""
    if token is None:
        return None
    text = token.strip()
    for fmt in TIMESTAMP_FORMATS:
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    return None


def hour_bucket(when):
    """Return the date-hour key ``YYYY-MM-DD HH`` used by Jobs 1 and 6."""
    return when.strftime("%Y-%m-%d %H")


def format_float(number):
    """Format a float with six decimal places for stable intermediate files."""
    return f"{float(number):.6f}"
