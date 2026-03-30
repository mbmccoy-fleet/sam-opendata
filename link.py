"""
Entity linking via Splink probabilistic record linkage.

Runs Fellegi-Sunter matching with EM parameter estimation on the
registrations_latest table in DuckDB. Produces pairwise match
predictions stored in the database.

Usage:
    uv run python link.py                # run linking with default settings
    uv run python link.py --threshold 5  # set match weight threshold
"""

import argparse
from pathlib import Path

import duckdb
from splink import DuckDBAPI, Linker, SettingsCreator, block_on
from splink import comparison_library as cl

DB_PATH = Path(__file__).parent / "sam.duckdb"


def build_settings():
    """Configure Splink settings: blocking rules and comparisons."""

    return SettingsCreator(
        link_type="dedupe_only",
        unique_id_column_name="uei",

        # Blocking rules — a pair is compared if it matches ANY rule.
        # Each rule reduces the comparison space dramatically.
        blocking_rules_to_generate_predictions=[
            # Same ZIP + similar name prefix
            block_on("zip5", "name_first4"),
            # Same normalized name (exact) — catches chains/franchises at different locations
            block_on("name_norm"),
            # Same CAGE code (strong deterministic link)
            block_on("cage_code"),
            # Same ZIP + same government POC last name
            block_on("zip5", "gov_poc_last"),
            # Same entity URL (strong signal)
            block_on("entity_url"),
        ],

        comparisons=[
            # Business name — strongest signal. Jaro-Winkler with multiple thresholds.
            cl.JaroWinklerAtThresholds(
                "name_norm",
                score_threshold_or_thresholds=[0.92, 0.85, 0.7],
            ).configure(term_frequency_adjustments=True),
            # DBA name — moderate confirmation
            cl.JaroWinklerAtThresholds(
                "dba_name",
                score_threshold_or_thresholds=[0.88],
            ),
            # City — moderate
            cl.ExactMatch("phys_city").configure(term_frequency_adjustments=True),
            # ZIP code — moderate
            cl.ExactMatch("zip5").configure(term_frequency_adjustments=True),
            # State — weak (too many per state)
            cl.ExactMatch("phys_state"),
            # Entity URL — strong
            cl.ExactMatch("entity_url"),
            # Primary NAICS — weak (same industry != same company)
            cl.ExactMatch("primary_naics"),
            # Government POC last name — moderate
            cl.ExactMatch("gov_poc_last").configure(term_frequency_adjustments=True),
            # CAGE code — strong deterministic
            cl.ExactMatch("cage_code"),
            # Entity structure — very weak, just confirms type
            cl.ExactMatch("entity_structure"),
        ],

        # EM convergence
        max_iterations=20,
        em_convergence=0.0001,

        retain_intermediate_calculation_columns=True,
        retain_matching_columns=True,
    )


def run_linking(threshold: float = 5.0):
    """Run the full Splink linking pipeline."""

    con = duckdb.connect(str(DB_PATH))

    # Check data is loaded
    count = con.execute("SELECT COUNT(*) FROM registrations_latest").fetchone()[0]
    if count == 0:
        print("ERROR: registrations_latest is empty. Run ingest.py first.")
        return
    print(f"Input: {count:,} entities in registrations_latest")

    # Filter to records with a name (can't match without one)
    con.execute("""
        CREATE OR REPLACE TEMPORARY TABLE link_input AS
        SELECT * FROM registrations_latest
        WHERE name_norm IS NOT NULL
    """)
    link_count = con.execute("SELECT COUNT(*) FROM link_input").fetchone()[0]
    print(f"After filtering nulls: {link_count:,} entities")

    # Set up Splink
    db_api = DuckDBAPI(connection=con)
    settings = build_settings()

    linker = Linker(
        "link_input",
        settings,
        db_api,
    )

    # Estimate u (probability of random agreement) from deterministic rules
    print("\nEstimating u parameters...")
    linker.training.estimate_u_using_random_sampling(max_pairs=5_000_000)

    # Estimate m/u via EM on different blocking passes
    print("Training m parameters (pass 1: name)...")
    linker.training.estimate_parameters_using_expectation_maximisation(
        block_on("name_norm"),
        fix_u_probabilities=True,
    )

    print("Training m parameters (pass 2: zip + city)...")
    linker.training.estimate_parameters_using_expectation_maximisation(
        block_on("zip5", "phys_city"),
        fix_u_probabilities=True,
    )

    # Show learned parameters
    print("\nMatch weights summary:")
    print(linker.visualisations.match_weights_chart().to_dict().get("title", ""))

    # Predict
    print(f"\nPredicting matches (threshold={threshold})...")
    predictions = linker.inference.predict(threshold_match_weight=threshold)

    # Save to DuckDB
    con.execute("DROP TABLE IF EXISTS match_predictions")
    predictions.as_duckdbpyrelation().create("match_predictions")

    n_matches = con.execute("SELECT COUNT(*) FROM match_predictions").fetchone()[0]
    print(f"Match pairs found: {n_matches:,}")

    if n_matches > 0:
        # Show match weight distribution
        dist = con.execute("""
            SELECT
                CASE
                    WHEN match_weight >= 15 THEN '15+'
                    WHEN match_weight >= 10 THEN '10-15'
                    WHEN match_weight >= 6 THEN '6-10'
                    WHEN match_weight >= 2 THEN '2-6'
                    ELSE '<2'
                END AS weight_band,
                COUNT(*) AS n_pairs
            FROM match_predictions
            GROUP BY 1
            ORDER BY 1
        """).fetchdf()
        print("\nMatch weight distribution:")
        print(dist.to_string(index=False))

        # Show top matches
        print("\nTop 10 highest-scoring matches:")
        top = con.execute("""
            SELECT
                uei_l, uei_r,
                name_norm_l, name_norm_r,
                phys_city_l, phys_city_r,
                ROUND(match_weight, 2) as weight
            FROM match_predictions
            ORDER BY match_weight DESC
            LIMIT 10
        """).fetchdf()
        print(top.to_string(index=False))

    con.close()
    print(f"\nResults saved to {DB_PATH}")


def main():
    parser = argparse.ArgumentParser(description="Run entity linking via Splink")
    parser.add_argument(
        "--threshold", type=float, default=2.0,
        help="Match weight threshold (default: 2.0; higher = fewer, more confident matches)"
    )
    args = parser.parse_args()
    run_linking(threshold=args.threshold)


if __name__ == "__main__":
    main()
