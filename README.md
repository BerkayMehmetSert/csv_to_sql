# CSV to SQL (SQL Server) Converter

A small utility to convert CSV files into SQL Server INSERT statements, optionally splitting output into multiple .sql files (chunks). It also:
- Detects the CSV delimiter (comma/semicolon/pipe/tab) automatically unless specified.
- Converts ISO-8601 date/time values to `datetime2(7)` using `CONVERT(..., 126)`.
- Treats blank/`NULL` cells as SQL `NULL`.
- Preserves numeric values without quoting.
- Can auto-generate UUIDs (GUIDs) for a chosen id column.

This repository contains two scripts. The recommended, generic, and configurable one is `csv_to_sql.py`.

## Requirements
- Python 3.8+

## Usage (csv_to_sql.py)

Basic example:

```
python convertertwo.py --csv File-Name.csv --table dbo.Table_Name --out-base File-Name
```

This will generate files like `File-Name-inserts-part-001.sql`, `File-Name-inserts-part-002.sql`, ... with up to 1000 rows per file (default chunk size).

### Arguments
- `--csv` (string): Input CSV file path. Default: `File-Name.csv`.
- `--table` (string): Target table name. Default: `dbo.Table_Name`.
- `--out-base` (string): Output .sql filename base. Files will be created as `<out-base>-part-XXX.sql`. Default: `File-Name-inserts`.
- `--chunk-size` (int): Max number of rows per .sql file. Default: `1000`.
- `--id-column` (string): Column for which to auto-generate UUIDs. Default: `Id`. Set to empty string to disable.
- `--no-generate-uuid` (flag): If provided, UUIDs will not be auto-generated for `--id-column`; CSV values are used instead.
- `--exclude-columns` (list): Column names to exclude entirely. Default: `RowVersion`.
- `--encoding` (string): CSV file encoding. Default: `utf-8-sig`.
- `--delimiter` (string): CSV delimiter. If not set, it will be auto-detected among `, ; | \t`.

### UUID generation behavior
If `--id-column` is set and `--no-generate-uuid` is NOT provided (default), the script ensures the `id-column` exists in the output columns and places it first. The value will be a freshly generated UUID for each row, ignoring any value from the CSV.

If you pass `--no-generate-uuid`, the CSV value for that column will be used as-is.

To completely disable special handling, set `--id-column` to an empty string, e.g. `--id-column ""`.

### Date/Time handling
Strings matching ISO-8601-like formats (e.g. `2024-01-01`, `2024-01-01T12:34:56`, `2024-01-01T12:34:56.123Z`, `2024-01-01T12:34:56+03:00`) are converted to `CONVERT(datetime2(7), '...', 126)`. If the input has a timezone, it is converted to UTC before formatting and then timezone information is removed.

### Examples
- Generate inserts with default options and auto UUIDs on `Id`:
```
python convertertwo.py --csv New-File-Name.csv --table dbo.Member_Stamp --out-base Member-Stamp-inserts
```

- Use a different id column name and still auto-generate UUIDs:
```
python convertertwo.py --csv File-Name.csv --table dbo.Table_Name --out-base File-Name-inserts --id-column ExampleId
```

- Disable UUID generation and respect CSV values for the id column:
```
python convertertwo.py --csv File-Name.csv --table dbo.Table_Name --out-base File-Name-inserts --no-generate-uuid
```

- Exclude extra columns and increase chunk size:
```
python convertertwo.py --csv Data.csv --table dbo.Data --out-base Data-inserts --exclude-columns RowVersion ExtraCol --chunk-size 5000
```

- Provide delimiter explicitly (e.g., semicolon):
```
python convertertwo.py --csv Data.csv --table dbo.Data --out-base Data-inserts --delimiter ";"
```

## Notes
- The script writes a transaction wrapper (`BEGIN TRAN; ... COMMIT TRAN;`) and a `GO` batch separator per output file.
- Numeric detection is simple and culture-invariant (dot as decimal separator). If your CSV uses locale-specific formats, consider normalizing them before conversion.
- The script assumes column names in `--exclude-columns` match the CSV headers exactly (case-sensitive).
