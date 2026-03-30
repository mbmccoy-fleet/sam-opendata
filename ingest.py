"""
Ingest SAM.gov Entity Registration .dat files into DuckDB.

Reads pipe-delimited .dat files from ZIP archives, extracts the fields
needed for entity linking, and loads them into a DuckDB database.

Usage:
    uv run python ingest.py                          # load latest V2 file only
    uv run python ingest.py --all                    # load all available snapshots
    uv run python ingest.py --file path/to/file.zip  # load a specific file
"""

import argparse
import re
import zipfile
from pathlib import Path

import duckdb

DB_PATH = Path(__file__).parent / "sam.duckdb"
DATA_DIR = Path(__file__).parent / "data" / "Data Services" / "Entity Registration"

# Column index → field name mapping (0-indexed, from V2 layout)
COLUMN_MAP = {
    0: "uei",
    3: "cage_code",
    5: "extract_code",
    7: "initial_reg_date",
    8: "expiration_date",
    9: "last_update_date",
    10: "activation_date",
    11: "legal_name",
    12: "dba_name",
    13: "division_name",
    14: "division_number",
    15: "phys_addr1",
    16: "phys_addr2",
    17: "phys_city",
    18: "phys_state",
    19: "phys_zip",
    20: "phys_zip4",
    21: "phys_country",
    22: "phys_congress",
    26: "entity_url",
    27: "entity_structure",
    28: "state_of_incorp",
    29: "country_of_incorp",
    31: "bus_type_string",
    32: "primary_naics",
    34: "naics_string",
    46: "gov_poc_first",
    48: "gov_poc_last",
    90: "elec_poc_first",
    92: "elec_poc_last",
    115: "exclusion_flag",
    121: "evs_source",
}

FIELDS = list(COLUMN_MAP.values())

# Regex to strip common legal entity suffixes for name normalization
_SUFFIX_RE = re.compile(
    r"\b(inc|incorporated|llc|l\.?l\.?c\.?|ltd|limited|corp|corporation|"
    r"co|company|pllc|lp|l\.?p\.?|plc|gmbh|sa|srl|pty)\b\.?",
    re.IGNORECASE,
)
_WHITESPACE_RE = re.compile(r"\s+")


def normalize_name(name: str | None) -> str | None:
    if not name or not name.strip():
        return None
    n = name.lower().strip()
    n = _SUFFIX_RE.sub("", n)
    n = _WHITESPACE_RE.sub(" ", n).strip()
    return n if n else None


def snapshot_date_from_filename(filename: str) -> str:
    """Extract a date string from a ZIP filename.

    Handles both naming conventions:
      SAM_PUBLIC_MONTHLY_V2_20260301.ZIP  → 2026-03-01
      SAM_PUBLIC_MONTHLY_2025_NOV_MODIFIED.zip → 2025-11-01
    """
    # Try V2 format: YYYYMMDD
    m = re.search(r"_(\d{4})(\d{2})(\d{2})\.", filename)
    if m:
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"

    # Try historical format: YYYY_MON
    months = {
        "JAN": "01", "FEB": "02", "MAR": "03", "APR": "04",
        "MAY": "05", "JUN": "06", "JUL": "07", "AUG": "08",
        "SEP": "09", "OCT": "10", "NOV": "11", "DEC": "12",
    }
    m = re.search(r"_(\d{4})_([A-Z]{3})_", filename, re.IGNORECASE)
    if m:
        year = m.group(1)
        month = months.get(m.group(2).upper(), "01")
        return f"{year}-{month}-01"

    return "1970-01-01"  # fallback


def parse_dat_from_zip(zip_path: Path) -> list[dict]:
    """Parse a .dat file from a ZIP archive into a list of row dicts."""
    snapshot = snapshot_date_from_filename(zip_path.name)
    rows = []

    with zipfile.ZipFile(zip_path) as zf:
        dat_files = [n for n in zf.namelist() if n.lower().endswith(".dat")]
        if not dat_files:
            print(f"  WARNING: No .dat file found in {zip_path.name}")
            return rows

        with zf.open(dat_files[0]) as f:
            for line_num, raw_line in enumerate(f):
                line = raw_line.decode("latin-1").rstrip("\r\n")

                # Skip header (BOF) and empty lines
                if line_num == 0 and line.startswith("BOF"):
                    continue
                if not line or line.startswith("!end"):
                    continue

                fields = line.split("|")
                if len(fields) < 122:
                    continue  # malformed

                row = {}
                for col_idx, field_name in COLUMN_MAP.items():
                    val = fields[col_idx].strip() if col_idx < len(fields) else ""
                    row[field_name] = val if val else None

                # Skip the end-of-record marker field
                if row["uei"] is None or row["uei"] == "!end":
                    continue

                # Computed fields
                row["name_norm"] = normalize_name(row["legal_name"])
                row["zip5"] = row["phys_zip"][:5] if row["phys_zip"] else None
                row["name_first4"] = row["name_norm"][:4] if row["name_norm"] else None
                row["snapshot_date"] = snapshot
                row["source_file"] = zip_path.name

                rows.append(row)

    return rows


def create_schema(con: duckdb.DuckDBPyConnection):
    """Create the registrations table if it doesn't exist."""
    con.execute("""
        CREATE TABLE IF NOT EXISTS registrations (
            uei              VARCHAR,
            snapshot_date    DATE,
            cage_code        VARCHAR,
            legal_name       VARCHAR,
            dba_name         VARCHAR,
            division_name    VARCHAR,
            division_number  VARCHAR,
            phys_addr1       VARCHAR,
            phys_addr2       VARCHAR,
            phys_city        VARCHAR,
            phys_state       VARCHAR,
            phys_zip         VARCHAR,
            phys_zip4        VARCHAR,
            phys_country     VARCHAR,
            phys_congress    VARCHAR,
            entity_url       VARCHAR,
            entity_structure VARCHAR,
            state_of_incorp  VARCHAR,
            country_of_incorp VARCHAR,
            bus_type_string  VARCHAR,
            primary_naics    VARCHAR,
            naics_string     VARCHAR,
            extract_code     VARCHAR,
            initial_reg_date VARCHAR,
            expiration_date  VARCHAR,
            last_update_date VARCHAR,
            activation_date  VARCHAR,
            gov_poc_first    VARCHAR,
            gov_poc_last     VARCHAR,
            elec_poc_first   VARCHAR,
            elec_poc_last    VARCHAR,
            exclusion_flag   VARCHAR,
            evs_source       VARCHAR,
            name_norm        VARCHAR,
            zip5             VARCHAR,
            name_first4      VARCHAR,
            source_file      VARCHAR,
            PRIMARY KEY (uei, snapshot_date)
        )
    """)


def build_derived_tables(con: duckdb.DuckDBPyConnection):
    """Build registrations_latest and history aggregation tables."""
    print("Building derived tables...")

    con.execute("DROP TABLE IF EXISTS registrations_latest")
    con.execute("""
        CREATE TABLE registrations_latest AS
        SELECT r.* FROM registrations r
        INNER JOIN (
            SELECT uei, MAX(snapshot_date) AS max_date
            FROM registrations GROUP BY uei
        ) latest ON r.uei = latest.uei AND r.snapshot_date = latest.max_date
    """)
    count = con.execute("SELECT COUNT(*) FROM registrations_latest").fetchone()[0]
    print(f"  registrations_latest: {count:,} unique UEIs")

    con.execute("DROP TABLE IF EXISTS uei_names")
    con.execute("""
        CREATE TABLE uei_names AS
        SELECT uei, LIST(DISTINCT name_norm) AS all_names
        FROM registrations
        WHERE name_norm IS NOT NULL
        GROUP BY uei
    """)

    con.execute("DROP TABLE IF EXISTS uei_addresses")
    con.execute("""
        CREATE TABLE uei_addresses AS
        SELECT uei, LIST(DISTINCT phys_city || '|' || zip5) AS all_locations
        FROM registrations
        WHERE zip5 IS NOT NULL
        GROUP BY uei
    """)
    print("  Derived tables built.")


def find_zip_files(all_snapshots: bool = False) -> list[Path]:
    """Find ZIP files to ingest."""
    zips = []
    for folder in ["Public V2", "Public - Historical"]:
        folder_path = DATA_DIR / folder
        if folder_path.exists():
            zips.extend(sorted(folder_path.glob("*.ZIP")))
            zips.extend(sorted(folder_path.glob("*.zip")))

    # Deduplicate (glob might overlap on case-insensitive FS)
    seen = set()
    unique = []
    for z in zips:
        key = z.name.lower()
        if key not in seen:
            seen.add(key)
            unique.append(z)

    if not all_snapshots:
        # Just the latest V2 ASCII file
        v2_files = [z for z in unique if "V2" in z.name and "UTF-8" not in z.name]
        if v2_files:
            return [v2_files[0]]  # most recent (sorted by name)
        return unique[:1]

    # Skip UTF-8 variants (same data, different encoding)
    return [z for z in unique if "UTF-8" not in z.name]


def main():
    parser = argparse.ArgumentParser(description="Ingest SAM.gov data into DuckDB")
    parser.add_argument("--all", action="store_true", help="Load all available snapshots")
    parser.add_argument("--file", type=Path, help="Load a specific ZIP file")
    parser.add_argument("--rebuild", action="store_true", help="Drop and rebuild all tables")
    args = parser.parse_args()

    con = duckdb.connect(str(DB_PATH))

    if args.rebuild:
        con.execute("DROP TABLE IF EXISTS registrations")
        con.execute("DROP TABLE IF EXISTS registrations_latest")
        con.execute("DROP TABLE IF EXISTS uei_names")
        con.execute("DROP TABLE IF EXISTS uei_addresses")
        print("Dropped all tables.")

    create_schema(con)

    # Figure out which files to load
    if args.file:
        zip_files = [args.file]
    else:
        zip_files = find_zip_files(all_snapshots=args.all)

    if not zip_files:
        print("No ZIP files found to ingest.")
        return

    # Check which snapshots are already loaded
    existing = set()
    try:
        rows = con.execute("SELECT DISTINCT source_file FROM registrations").fetchall()
        existing = {r[0] for r in rows}
    except Exception:
        pass

    loaded_any = False
    for zf in zip_files:
        if zf.name in existing:
            print(f"  SKIP {zf.name} (already loaded)")
            continue

        print(f"Loading {zf.name}...")
        rows = parse_dat_from_zip(zf)
        if not rows:
            continue

        # Insert via DuckDB's fast path from list of dicts
        import pandas as pd
        df = pd.DataFrame(rows)
        df["snapshot_date"] = pd.to_datetime(df["snapshot_date"])

        # Handle duplicate (uei, snapshot_date) within a single file
        # Keep last occurrence (most recently updated)
        df = df.drop_duplicates(subset=["uei", "snapshot_date"], keep="last")

        # Ensure column order matches table schema
        table_cols = [desc[0] for desc in con.execute("DESCRIBE registrations").fetchall()]
        df = df[table_cols]

        con.execute("INSERT OR REPLACE INTO registrations SELECT * FROM df")
        print(f"  Loaded {len(df):,} records (snapshot: {rows[0]['snapshot_date']})")
        loaded_any = True

    if loaded_any:
        build_derived_tables(con)

    # Summary stats
    total = con.execute("SELECT COUNT(*) FROM registrations").fetchone()[0]
    snapshots = con.execute("SELECT COUNT(DISTINCT snapshot_date) FROM registrations").fetchone()[0]
    ueis = con.execute("SELECT COUNT(DISTINCT uei) FROM registrations").fetchone()[0]
    print(f"\nDatabase: {DB_PATH}")
    print(f"  Total rows:      {total:,}")
    print(f"  Snapshots:       {snapshots}")
    print(f"  Unique UEIs:     {ueis:,}")

    # Show a few sample records
    print("\nSample records:")
    sample = con.execute("""
        SELECT uei, legal_name, phys_city, phys_state, zip5, snapshot_date
        FROM registrations_latest
        ORDER BY legal_name
        LIMIT 5
    """).fetchdf()
    print(sample.to_string(index=False))

    con.close()


if __name__ == "__main__":
    main()
