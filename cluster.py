"""
Cluster matched entity pairs into entity groups using connected components.

Reads pairwise match predictions from link.py, applies a configurable
threshold, and finds connected components to produce entity clusters.

Usage:
    uv run python cluster.py                  # cluster with default threshold (5.0)
    uv run python cluster.py --threshold 10   # higher threshold = fewer, tighter clusters
    uv run python cluster.py --stats          # just show cluster statistics
"""

import argparse
from pathlib import Path

import duckdb
from splink import DuckDBAPI, Linker

DB_PATH = Path(__file__).parent / "sam.duckdb"


def cluster(threshold: float = 5.0):
    """Build entity clusters from match predictions."""
    con = duckdb.connect(str(DB_PATH))

    n_predictions = con.execute("SELECT COUNT(*) FROM match_predictions").fetchone()[0]
    if n_predictions == 0:
        print("ERROR: No match predictions found. Run link.py first.")
        return
    print(f"Match predictions: {n_predictions:,}")

    above = con.execute(
        "SELECT COUNT(*) FROM match_predictions WHERE match_weight >= ?",
        [threshold],
    ).fetchone()[0]
    print(f"Above threshold ({threshold}): {above:,}")

    # Use Splink's built-in clustering via graph connected components
    # We need to reconstruct the linker to use cluster_pairwise_predictions
    # But we can also do it directly with DuckDB + igraph

    print("Building graph and finding connected components...")

    # Extract edges above threshold
    edges = con.execute("""
        SELECT uei_l, uei_r, match_weight
        FROM match_predictions
        WHERE match_weight >= ?
    """, [threshold]).fetchdf()

    if edges.empty:
        print("No edges above threshold.")
        return

    # Build graph with igraph
    import igraph as ig

    # Collect all unique nodes
    nodes = sorted(set(edges["uei_l"].tolist() + edges["uei_r"].tolist()))
    node_idx = {uei: i for i, uei in enumerate(nodes)}

    g = ig.Graph(
        n=len(nodes),
        edges=[(node_idx[r["uei_l"]], node_idx[r["uei_r"]]) for _, r in edges.iterrows()],
        directed=False,
    )
    g.es["weight"] = edges["match_weight"].tolist()

    # Find connected components
    components = g.connected_components()
    print(f"Connected components: {len(components)}")

    # Build cluster assignments
    cluster_data = []
    for cluster_id, members in enumerate(components):
        for node_id in members:
            cluster_data.append({
                "cluster_id": cluster_id,
                "uei": nodes[node_id],
            })

    import pandas as pd
    clusters_df = pd.DataFrame(cluster_data)

    # Store in DuckDB, joining with entity info
    con.execute("DROP TABLE IF EXISTS entity_clusters")
    con.execute("""
        CREATE TABLE entity_clusters AS
        SELECT
            c.cluster_id,
            c.uei,
            r.legal_name,
            r.name_norm,
            r.phys_city,
            r.phys_state,
            r.zip5,
            r.entity_url,
            r.cage_code,
            r.primary_naics,
            r.entity_structure,
            r.initial_reg_date,
            cluster_sizes.cluster_size
        FROM clusters_df c
        LEFT JOIN registrations_latest r ON c.uei = r.uei
        LEFT JOIN (
            SELECT cluster_id, COUNT(*) AS cluster_size
            FROM clusters_df
            GROUP BY cluster_id
        ) cluster_sizes ON c.cluster_id = cluster_sizes.cluster_id
    """)

    # Also store the edges for explainability
    con.execute("DROP TABLE IF EXISTS match_edges")
    con.execute("""
        CREATE TABLE match_edges AS
        SELECT
            uei_l, uei_r,
            match_weight,
            match_probability
        FROM match_predictions
        WHERE match_weight >= ?
    """, [threshold])

    show_stats(con)
    con.close()
    print(f"\nResults saved to {DB_PATH}")


def show_stats(con):
    """Print cluster statistics."""
    total = con.execute("SELECT COUNT(DISTINCT uei) FROM entity_clusters").fetchone()[0]
    n_clusters = con.execute("SELECT COUNT(DISTINCT cluster_id) FROM entity_clusters").fetchone()[0]
    print(f"\n=== Cluster Statistics ===")
    print(f"Clustered UEIs:  {total:,}")
    print(f"Clusters:        {n_clusters:,}")

    # Size distribution
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
    print("\nCluster size distribution:")
    print(dist.to_string(index=False))

    # Largest clusters
    print("\nTop 10 largest clusters:")
    top = con.execute("""
        SELECT
            cluster_id,
            cluster_size,
            MIN(legal_name) AS example_name,
            MIN(phys_city) AS example_city,
            MIN(phys_state) AS example_state
        FROM entity_clusters
        GROUP BY cluster_id, cluster_size
        ORDER BY cluster_size DESC
        LIMIT 10
    """).fetchdf()
    print(top.to_string(index=False))

    # Sanity check: no cluster should have >1% of all entities
    max_size = con.execute("SELECT MAX(cluster_size) FROM entity_clusters").fetchone()[0]
    total_entities = con.execute("SELECT COUNT(*) FROM registrations_latest").fetchone()[0]
    pct = max_size / total_entities * 100
    if pct > 1:
        print(f"\n** WARNING: Largest cluster has {max_size} entities ({pct:.1f}% of total)")
        print("   This may indicate over-linking. Consider raising the threshold.")
    else:
        print(f"\n  Largest cluster: {max_size} entities ({pct:.2f}% of total) — looks reasonable.")


def main():
    parser = argparse.ArgumentParser(description="Cluster matched entity pairs")
    parser.add_argument(
        "--threshold", type=float, default=5.0,
        help="Match weight threshold for clustering (default: 5.0)"
    )
    parser.add_argument("--stats", action="store_true", help="Just show stats for existing clusters")
    args = parser.parse_args()

    if args.stats:
        con = duckdb.connect(str(DB_PATH))
        try:
            show_stats(con)
        except Exception as e:
            print(f"No clusters found: {e}")
        con.close()
    else:
        cluster(threshold=args.threshold)


if __name__ == "__main__":
    main()
