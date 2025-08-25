import csv
import uuid
import re
import math
import argparse
from typing import List, Optional
from datetime import datetime, timezone
from pathlib import Path

# Default settings (can be overridden by CLI)
CSV_FILE = "File-Name.csv"
TABLE = "dbo.Table_Name"
OUT_BASE = "File-Name-inserts"
CHUNK_SIZE = 1000
DEFAULT_EXCLUDES = ["RowVersion"]
DEFAULT_ENCODING = "utf-8-sig"

# Helper regexes
NUMERIC_RE = re.compile(r"^[+-]?\d+(?:\.\d+)?$")
ISO_DT_RE = re.compile(
    r"^\d{4}-\d{2}-\d{2}"
    r"(?:T\d{2}:\d{2}:\d{2}"
    r"(?:\.\d{1,9})?"
    r"(?:Z|[+\-]\d{2}:\d{2})?"
    r")?$"
)

def as_sql_datetime2_126(s: str) -> str:
    """Render ISO-8601 string as SQL Server CONVERT(datetime2(7), ..., 126).
    If timezone info exists, convert to UTC and strip tz. Pad fractional seconds to 7 digits.
    """
    v = s.strip()
    if re.match(r"^\d{4}-\d{2}-\d{2}$", v):
        v += "T00:00:00"
    if v.endswith("Z"):
        v = v[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(v)
        if dt.tzinfo is not None:
            dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
        v = dt.strftime("%Y-%m-%dT%H:%M:%S.%f")
        sec, frac = v.split(".")
        frac = (frac + "0")[:7]
        v = f"{sec}.{frac}"
    except Exception:
        # If parsing fails, fall back to the provided string with style 126
        pass
    return f"CONVERT(datetime2(7), '{v}', 126)"

def to_sql_literal(val: Optional[str]) -> str:
    """Convert a CSV string to an SQL literal for SQL Server.
    - NULL/empty -> NULL
    - ISO-8601 date/time -> datetime2(7) via CONVERT style 126
    - numeric -> pass-through
    - otherwise -> N'...'
    """
    if val is None:
        return "NULL"
    v = str(val).strip()
    if v == "" or v.upper() == "NULL":
        return "NULL"
    if ISO_DT_RE.match(v):
        return as_sql_datetime2_126(v)
    if NUMERIC_RE.match(v):
        return v
    v = v.replace("'", "''")
    return f"N'{v}'"

def sniff_delimiter(path: str, encoding: str = DEFAULT_ENCODING) -> str:
    """Try to detect CSV delimiter. Defaults to comma if sniffing fails."""
    sample = Path(path).read_text(encoding=encoding)[:2048]
    with open(path, "r", encoding=encoding, newline="") as f:
        try:
            dialect = csv.Sniffer().sniff(sample, delimiters=[",", ";", "|", "\t"])
            return dialect.delimiter
        except Exception:
            return ","

def write_chunk_sql(
    idx: int,
    rows: List[dict],
    columns: List[str],
    table: str,
    out_base: str,
    id_column: Optional[str],
    generate_uuid: bool,
) -> str:
    """Write a single chunk's INSERT statements into an .sql file."""
    out_path = f"{out_base}-part-{idx:03d}.sql"
    with open(out_path, "w", encoding="utf-8") as out:
        out.write(f"-- Generated from chunk {idx}\n")
        out.write(f"-- Table: {table}\n")
        out.write(f"-- Columns: {', '.join(columns)}\n")
        out.write(f"-- Rows in this file: {len(rows)}\n\n")
        out.write("BEGIN TRAN;\n")
        for r in rows:
            vals = []
            for c in columns:
                if id_column and generate_uuid and c == id_column:
                    vals.append(f"'{uuid.uuid4()}'")
                else:
                    vals.append(to_sql_literal(r.get(c, "")))
            out.write(
                f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({', '.join(vals)});\n"
            )
        out.write("COMMIT TRAN;\nGO\n")
    return out_path

def build_columns(
    csv_columns: List[str],
    exclude: List[str],
    id_column: Optional[str],
    generate_uuid: bool,
) -> List[str]:
    """Prepare final column order. Exclude specified columns. If id_column is set and generate_uuid
    is True, ensure it exists and place it first (CSV value ignored)."""
    cols = [c for c in (csv_columns or []) if c and c not in exclude]
    if id_column and generate_uuid:
        if id_column not in cols:
            cols = [id_column] + cols
        else:
            cols = [id_column] + [c for c in cols if c != id_column]
    return cols

def parse_args():
    p = argparse.ArgumentParser(description="Convert CSV to SQL Server INSERT statements split into chunks.")
    p.add_argument("--csv", dest="csv_path", default=CSV_FILE, help="Input CSV file path.")
    p.add_argument("--table", dest="table", default=TABLE, help="Target table name.")
    p.add_argument("--out-base", dest="out_base", default=OUT_BASE, help="Output .sql filename base (parts will be appended).")
    p.add_argument("--chunk-size", dest="chunk_size", type=int, default=CHUNK_SIZE, help="Max number of rows per .sql file.")
    p.add_argument("--id-column", dest="id_column", default="Id", help="Column name for which to auto-generate UUIDs (ignored if --no-generate-uuid). Use empty string to disable.")
    p.add_argument("--no-generate-uuid", dest="no_generate_uuid", action="store_true", help="Do not auto-generate UUIDs for the id column; use CSV values instead.")
    p.add_argument("--exclude-columns", dest="exclude_columns", nargs="*", default=DEFAULT_EXCLUDES, help="Column names to exclude entirely (default: RowVersion).")
    p.add_argument("--encoding", dest="encoding", default=DEFAULT_ENCODING, help="CSV file encoding (default: utf-8-sig).")
    p.add_argument("--delimiter", dest="delimiter", default=None, help="CSV delimiter. If not provided, will be sniffed.")
    return p.parse_args()

def main():
    args = parse_args()

    csv_path = args.csv_path
    table = args.table
    out_base = args.out_base
    chunk_size = max(1, int(args.chunk_size))
    id_column = (args.id_column or "").strip() or None
    generate_uuid = not args.no_generate_uuid and bool(id_column)
    exclude_columns = args.exclude_columns or []
    encoding = args.encoding

    delim = args.delimiter or sniff_delimiter(csv_path, encoding=encoding)

    with open(csv_path, "r", encoding=encoding, newline="") as f:
        reader = csv.DictReader(f, delimiter=delim)
        csv_cols = reader.fieldnames or []

        # Normalize exclusions case-sensitively as provided; user must match exact header text
        cols = build_columns(csv_cols, exclude_columns, id_column, generate_uuid)

        all_rows = list(reader)
        total = len(all_rows)
        if total == 0:
            print("CSV appears to be empty. Nothing to do.")
            return

        parts = math.ceil(total / chunk_size)
        paths: List[str] = []
        for i in range(parts):
            start = i * chunk_size
            end = min(start + chunk_size, total)
            chunk = all_rows[start:end]
            path = write_chunk_sql(
                i + 1,
                chunk,
                cols,
                table,
                out_base,
                id_column,
                generate_uuid,
            )
            paths.append(path)

    print(f"Done: {total} rows, {parts} file(s) generated.")
    for pth in paths:
        print(" -", pth)

if __name__ == "__main__":
    main()
