"""
Query the SAM.gov entity graph.

Usage:
    uv run python query.py "Lockheed Martin"       # search by name
    uv run python query.py --uei C111ATT311C8      # show entity + its cluster
    uv run python query.py --cluster 42            # show all entities in cluster 42
    uv run python query.py --stats                 # cluster size distribution
    uv run python query.py --diagnose 7            # diagnose why a cluster is large
"""

import argparse
import sys
from pathlib import Path

import duckdb

DB_PATH = Path(__file__).parent / "sam.duckdb"


def search_name(con, query: str, limit: int = 25):
    """Search for entities by name."""
    results = con.execute("""
        SELECT
            ec.cluster_id,
            ec.cluster_size,
            ec.uei,
            ec.legal_name,
            ec.phys_city,
            ec.phys_state,
            ec.zip5
        FROM entity_clusters ec
        WHERE ec.name_norm LIKE '%' || LOWER(?) || '%'
           OR ec.legal_name ILIKE '%' || ? || '%'
        ORDER BY ec.cluster_size DESC, ec.legal_name
        LIMIT ?
    """, [query, query, limit]).fetchdf()

    if results.empty:
        # Fall back to registrations_latest (entity might not be in a cluster)
        results = con.execute("""
            SELECT
                NULL AS cluster_id,
                NULL AS cluster_size,
                uei,
                legal_name,
                phys_city,
                phys_state,
                zip5
            FROM registrations_latest
            WHERE name_norm LIKE '%' || LOWER(?) || '%'
               OR legal_name ILIKE '%' || ? || '%'
            ORDER BY legal_name
            LIMIT ?
        """, [query, query, limit]).fetchdf()

        if results.empty:
            print(f"No entities found matching '{query}'")
            return
        print(f"Found {len(results)} entities (none in clusters):")
    else:
        print(f"Found {len(results)} clustered entities matching '{query}':")

    print(results.to_string(index=False))


def show_uei(con, uei: str):
    """Show an entity and its cluster members."""
    # First, get the entity
    entity = con.execute("""
        SELECT * FROM registrations_latest WHERE uei = ?
    """, [uei]).fetchdf()

    if entity.empty:
        print(f"UEI {uei} not found")
        return

    print("=== Entity ===")
    for col in ["uei", "legal_name", "dba_name", "phys_addr1", "phys_city",
                "phys_state", "zip5", "cage_code", "entity_url", "primary_naics",
                "entity_structure", "initial_reg_date", "gov_poc_first", "gov_poc_last"]:
        val = entity[col].iloc[0]
        if val and str(val) != "None" and str(val) != "nan":
            print(f"  {col:>20}: {val}")

    # Check if it's in a cluster
    cluster_info = con.execute("""
        SELECT cluster_id, cluster_size
        FROM entity_clusters WHERE uei = ?
    """, [uei]).fetchdf()

    if cluster_info.empty:
        print("\nNot in any cluster (no matches found above threshold)")
        return

    cluster_id = int(cluster_info["cluster_id"].iloc[0])
    cluster_size = int(cluster_info["cluster_size"].iloc[0])
    print(f"\n=== Cluster {cluster_id} ({cluster_size} members) ===")
    show_cluster(con, cluster_id, limit=30)


def show_cluster(con, cluster_id: int, limit: int = 50):
    """Show all entities in a cluster."""
    members = con.execute("""
        SELECT uei, legal_name, phys_city, phys_state, zip5, entity_url, cage_code
        FROM entity_clusters
        WHERE cluster_id = ?
        ORDER BY legal_name
        LIMIT ?
    """, [cluster_id, limit]).fetchdf()

    if members.empty:
        print(f"Cluster {cluster_id} not found")
        return

    total = con.execute(
        "SELECT cluster_size FROM entity_clusters WHERE cluster_id = ? LIMIT 1",
        [cluster_id],
    ).fetchone()[0]

    print(f"Cluster {cluster_id}: {total} members" +
          (f" (showing first {limit})" if total > limit else ""))
    print(members.to_string(index=False))

    # Show edges within cluster
    edges = con.execute("""
        SELECT uei_l, uei_r, ROUND(match_weight, 1) AS weight
        FROM match_edges
        WHERE uei_l IN (SELECT uei FROM entity_clusters WHERE cluster_id = ?)
          AND uei_r IN (SELECT uei FROM entity_clusters WHERE cluster_id = ?)
        ORDER BY match_weight DESC
        LIMIT 20
    """, [cluster_id, cluster_id]).fetchdf()

    if not edges.empty:
        print(f"\nTop edges:")
        print(edges.to_string(index=False))


def diagnose_cluster(con, cluster_id: int):
    """Diagnose why a cluster is large — find the weakest links."""
    size = con.execute(
        "SELECT cluster_size FROM entity_clusters WHERE cluster_id = ? LIMIT 1",
        [cluster_id],
    ).fetchone()
    if not size:
        print(f"Cluster {cluster_id} not found")
        return

    print(f"Cluster {cluster_id}: {size[0]} members\n")

    # Find the weakest edges (bridges that connect sub-groups)
    print("Weakest edges (potential over-links):")
    weak = con.execute("""
        SELECT
            me.uei_l, me.uei_r,
            ROUND(me.match_weight, 2) AS weight,
            rl.legal_name AS name_l,
            rr.legal_name AS name_r,
            rl.phys_city AS city_l,
            rr.phys_city AS city_r
        FROM match_edges me
        JOIN registrations_latest rl ON me.uei_l = rl.uei
        JOIN registrations_latest rr ON me.uei_r = rr.uei
        WHERE me.uei_l IN (SELECT uei FROM entity_clusters WHERE cluster_id = ?)
          AND me.uei_r IN (SELECT uei FROM entity_clusters WHERE cluster_id = ?)
        ORDER BY me.match_weight ASC
        LIMIT 15
    """, [cluster_id, cluster_id]).fetchdf()
    print(weak.to_string(index=False))

    # Show the name distribution
    print("\nMost common normalized names in cluster:")
    names = con.execute("""
        SELECT name_norm, COUNT(*) AS cnt
        FROM entity_clusters
        WHERE cluster_id = ?
        GROUP BY name_norm
        ORDER BY cnt DESC
        LIMIT 10
    """, [cluster_id]).fetchdf()
    print(names.to_string(index=False))

    # Show city distribution
    print("\nCities represented:")
    cities = con.execute("""
        SELECT phys_city, phys_state, COUNT(*) AS cnt
        FROM entity_clusters
        WHERE cluster_id = ?
        GROUP BY phys_city, phys_state
        ORDER BY cnt DESC
        LIMIT 10
    """, [cluster_id]).fetchdf()
    print(cities.to_string(index=False))


def show_stats(con):
    """Print overall statistics."""
    total_entities = con.execute("SELECT COUNT(*) FROM registrations_latest").fetchone()[0]
    clustered = con.execute("SELECT COUNT(DISTINCT uei) FROM entity_clusters").fetchone()[0]
    n_clusters = con.execute("SELECT COUNT(DISTINCT cluster_id) FROM entity_clusters").fetchone()[0]

    print(f"Total entities:     {total_entities:,}")
    print(f"In clusters:        {clustered:,} ({clustered/total_entities*100:.1f}%)")
    print(f"Singletons:         {total_entities - clustered:,} ({(total_entities-clustered)/total_entities*100:.1f}%)")
    print(f"Number of clusters: {n_clusters:,}")

    print("\nCluster size distribution:")
    dist = con.execute("""
        SELECT
            CASE
                WHEN cluster_size = 2 THEN '2'
                WHEN cluster_size BETWEEN 3 AND 5 THEN '3-5'
                WHEN cluster_size BETWEEN 6 AND 10 THEN '6-10'
                WHEN cluster_size BETWEEN 11 AND 20 THEN '11-20'
                WHEN cluster_size BETWEEN 21 AND 50 THEN '21-50'
                WHEN cluster_size BETWEEN 51 AND 100 THEN '51-100'
                ELSE '100+'
            END AS size_range,
            COUNT(DISTINCT cluster_id) AS n_clusters,
            SUM(1) AS n_entities
        FROM entity_clusters
        GROUP BY 1
        ORDER BY MIN(cluster_size)
    """).fetchdf()
    print(dist.to_string(index=False))


def main():
    parser = argparse.ArgumentParser(description="Query the SAM.gov entity graph")
    parser.add_argument("name", nargs="?", help="Search by entity name")
    parser.add_argument("--uei", help="Look up a specific UEI")
    parser.add_argument("--cluster", type=int, help="Show a specific cluster")
    parser.add_argument("--diagnose", type=int, help="Diagnose a large cluster")
    parser.add_argument("--stats", action="store_true", help="Show statistics")
    parser.add_argument("--limit", type=int, default=25, help="Max results")
    args = parser.parse_args()

    con = duckdb.connect(str(DB_PATH), read_only=True)

    try:
        if args.stats:
            show_stats(con)
        elif args.uei:
            show_uei(con, args.uei)
        elif args.cluster is not None:
            show_cluster(con, args.cluster, limit=args.limit)
        elif args.diagnose is not None:
            diagnose_cluster(con, args.diagnose)
        elif args.name:
            search_name(con, args.name, limit=args.limit)
        else:
            parser.print_help()
    finally:
        con.close()


if __name__ == "__main__":
    main()
