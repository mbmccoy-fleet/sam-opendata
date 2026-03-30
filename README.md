# SAM.gov Entity Graph

Discovers corporate relationships (parent/subsidiary/branch) from
[SAM.gov](https://sam.gov) public entity registration data using
probabilistic record linkage.

SAM.gov's public extract does **not** include explicit parent-child
fields (those are FOUO/Sensitive only). This project infers
relationships from name similarity, address proximity, shared URLs,
overlapping NAICS codes, and shared points of contact.

## Architecture

```
SAM .dat files (pipe-delimited, in ZIP)
       │
       ▼
 ┌────────────┐
 │  ingest.py  │   Parse → DuckDB
 └──────┬─────┘
        ▼
 ┌────────────┐
 │  link.py    │   Splink (Fellegi-Sunter + EM), DuckDB backend
 └──────┬─────┘
        ▼
 ┌────────────┐
 │ cluster.py  │   Connected components → entity groups
 └──────┬─────┘
        ▼
 ┌────────────┐
 │  query.py   │   Search, inspect, diagnose
 └────────────┘
```

**Stack**: Python, DuckDB, Splink 4. No external database server needed.

## Quick start

```bash
# Install dependencies
uv sync

# Download data (requires SAM.gov browser login — see below)
uv run python download_sam.py --list       # show available files
uv run python download_sam.py --links      # get browser-downloadable URLs
uv run python download_sam.py --ingest ~/Downloads  # move downloads into data/

# Run the pipeline
uv run python ingest.py                    # load latest snapshot into DuckDB
uv run python link.py                      # probabilistic entity matching (~7s)
uv run python cluster.py                   # group matches into clusters (~10s)

# Explore results
uv run python query.py "Lockheed Martin"   # search by name
uv run python query.py --uei ZFN2JJXBLZT3  # look up a specific entity
uv run python query.py --cluster 3801      # show all members of a cluster
uv run python query.py --diagnose 7        # inspect a large cluster's weak edges
uv run python query.py --stats             # overall cluster statistics
```

## Data

Source: [SAM.gov Data Services — Entity Registration (Public)](https://sam.gov/data-services/Entity%20Registration?privacy=Public)

Two folders:
- **Public - Historical**: 23 biannual snapshots (Nov 2014 – Nov 2025) + readme
- **Public V2**: Current monthly snapshots with UEI identifiers

Each snapshot is a full dump of ~875K active/recently-expired entity
registrations in a pipe-delimited `.dat` file (142 fields per record,
~530 MB uncompressed).

The download API requires a SAM.gov session for actual file retrieval.
Use `download_sam.py --links` to generate browser-clickable URLs, then
`--ingest` to organize the files.

## How it works

### 1. Ingestion (`ingest.py`)

Parses pipe-delimited `.dat` files from ZIP archives. Extracts ~30 fields
relevant to entity linking (identifiers, names, addresses, POCs, NAICS
codes, URLs). Normalizes business names by stripping legal suffixes
(Inc, LLC, Corp, etc.) and computes blocking keys. Stores everything in
a DuckDB database keyed on `(uei, snapshot_date)` — all historical
snapshots are kept for temporal analysis.

### 2. Linking (`link.py`)

Runs [Splink](https://moj-analytical-services.github.io/splink/) with
the Fellegi-Sunter probabilistic model. EM estimates match/non-match
probabilities from the data distribution — no labeled training data
needed.

**Blocking rules** (reduce 380B naive pairs to ~5-8M candidates):
- Same ZIP + name prefix
- Same normalized name
- Same CAGE code
- Same ZIP + same POC last name
- Same entity URL

**Comparison columns**: Jaro-Winkler on names, exact match on
identifiers and location fields, with term-frequency adjustments on
high-cardinality fields.

### 3. Clustering (`cluster.py`)

Builds a graph from above-threshold match pairs and finds connected
components via igraph. Each component becomes an entity cluster.

### 4. Querying (`query.py`)

CLI for exploring the graph. `--diagnose` shows the weakest edges in a
cluster to find false-positive links.

## Results (single snapshot, 865K entities)

| Metric | Value |
|--------|-------|
| Total entities | 865,232 |
| In clusters | 151,171 (17.5%) |
| Singletons | 714,061 (82.5%) |
| Clusters | 50,888 |
| Largest cluster | Sherwin-Williams (2,120 locations) |

Example clusters discovered:
- **Lockheed Martin**: 53 entities (Corp, Aeroparts, Aculight, Canada, etc.)
- **Boeing**: 63 entities (Aerospace Ops, Commercial Satellite, Intelligence & Analytics, etc.)
- **Sherwin-Williams**: 2,120 store locations

## Configuration

Key tuning parameters:

| Parameter | Where | Default | Effect |
|-----------|-------|---------|--------|
| Match weight threshold | `link.py --threshold` | 2.0 | Lower = more match pairs (permissive) |
| Cluster threshold | `cluster.py --threshold` | 5.0 | Higher = tighter, smaller clusters |
| Blocking rules | `link.py build_settings()` | 5 rules | Add/remove to control candidate pairs |

## Scripts

| File | Purpose |
|------|---------|
| `ingest.py` | Parse SAM .dat files → DuckDB |
| `link.py` | Splink entity matching (blocking + EM + scoring) |
| `cluster.py` | Connected components → entity groups |
| `query.py` | CLI for exploring the entity graph |
| `download_sam.py` | SAM.gov file manifest, download URLs, file ingestion |
| `parse_file_list.py` | Parse saved SAM.gov HTML pages for file lists |
| `download_data.sh` | Download data from Google Drive via rclone |
