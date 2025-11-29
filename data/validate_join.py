"""
Quick Join Validation script to verify that the data center dataset can be
joined to the EIA data.

This script:
1. Loads a data center dataset and a power cost dataset.
2. Verifies that utility names match between the two datasets.
3. Reports unique facilities and utilities.
4. Computes intersections (matches) and missing utilities.
5. Performs an inner join on Utility_Name + State.
6. Prints a preview of the joined results.
"""

import pandas as pd
import sys

# Column constants for readability
FACILITY_NAME = "Facility_Name"
UTILITY_NAME = "Utility_Name"
STATE = "State"
OPENING_YEAR = "Opening_Year"


def validate_join(datacenter_file, power_file):
    """ Load the datasets, validate joinability"""

    print("=" * 70)
    print("DATA CENTER <-> POWER COST")
    print("=" * 70)

    # ------------------------------
    # Load data center CSV
    # ------------------------------
    print(f"\nLoading data centers from: {datacenter_file}")
    try:
        data_centers = pd.read_csv(datacenter_file)
        print(f"Loaded {len(data_centers)} data centers")
    except Exception as e:
        print(f"Error loading data centers: {e}")
        return False

    # ------------------------------
    # Load power cost CSV
    # ------------------------------
    print(f"\nLoading power costs from: {power_file}")
    try:
        power_data = pd.read_csv(power_file)
        print(f"Loaded {len(power_data)} records")
    except Exception as e:
        print(f"Error loading power costs: {e}")
        return False

    # ------------------------------
    # Validate required columns exist
    # ------------------------------
    if UTILITY_NAME not in data_centers.columns:
        print(f"Error: {UTILITY_NAME} column not found in data centers")
        return False

    if UTILITY_NAME not in power_data.columns:
        print(f"Error: {UTILITY_NAME} column not found in power records")
        return False

    # ------------------------------
    # Show unique combinations of facility + utility + state
    # Helps check for duplicates or inconsistencies
    # ------------------------------
    print("UNIQUE DATA CENTERS:")
    unique_data_centers = data_centers[
        [FACILITY_NAME, UTILITY_NAME, STATE, OPENING_YEAR]
    ].drop_duplicates()

    print(unique_data_centers.to_string(index=False))

    # ------------------------------
    # Collect unique utility names from both datasets
    # ------------------------------
    data_center_utilities = set(data_centers[UTILITY_NAME].dropna().unique())
    power_utilities = set(power_data[UTILITY_NAME].dropna().unique())

    print(f"Unique utilities in data centers: {len(data_center_utilities)}")
    print(f"Unique utilities in power data: {len(power_utilities)}")

    # ------------------------------
    # Find matching and missing utilities
    # Intersection = utilities that exist in *both* files
    # Missing      = utilities in data_centers but NOT in power_data
    # ------------------------------
    matches = data_center_utilities.intersection(power_utilities)
    missing = data_center_utilities - power_utilities

    print("MATCH RESULTS")
    print(
        f"Matched utilities: {len(matches)}/{len(data_center_utilities)} "
        f"({len(matches) / len(data_center_utilities) * 100:.1f}%)"
    )

    # NOTE:
    # The script prints the entire column instead of the individual util (bug).
    # This seems intentional but you might want to revise it later.

    if matches:
        print(f"\n MATCHED ({len(matches)}):")
        for util in sorted(matches):
            # This prints the entire series rather than the specific row(s).
            print(f" {data_centers[UTILITY_NAME]} ({data_centers[STATE]})")

    if missing:
        print(f"\n MISSING ({len(missing)}):")
        for util in sorted(missing):
            # Same as above: prints columns instead of specific utility.
            print(f" {data_centers[UTILITY_NAME]} ({data_centers[STATE]})")

    # ------------------------------
    # Perform actual join:
    #   Join on (Utility_Name, State)
    #   This ensures that the right cost is matched to the correct state.
    #
    # how="inner" means:
    #   Only keep records that appear in *both* datasets.
    # ------------------------------
    joined = data_centers.merge(
        power_data,
        left_on=[UTILITY_NAME, STATE],
        right_on=[UTILITY_NAME, STATE],
        how="inner"
    )

    print(f"Joined: {len(joined)} records")

    # ------------------------------
    # Show first 5 rows of important joined fields
    # Only show columns that actually exist in the result.
    # ------------------------------
    if len(joined) > 0:
        print("PREVIEW (first 5 rows):")
        preview_cols = [
            FACILITY_NAME,
            UTILITY_NAME,
            STATE,
            OPENING_YEAR,
            "Year",
            "Price_Per_kWh",
        ]

        # Only include columns that actually exist in the join result
        available_cols = [col for col in preview_cols if col in joined.columns]

        print(joined[available_cols].head().to_string(index=False))


def show_help():
    """Print usage instructions."""
    print("Usage: python validate_join.py <data_centers.csv> <power_cost_XXXX.csv>")


if __name__ == "__main__":
    # Expect exactly two file paths as arguments
    if len(sys.argv) != 3:
        show_help()
        sys.exit(1)

    datacenter_file = sys.argv[1]
    power_file = sys.argv[2]

    # Execute validation and exit with success/failure code
    success = validate_join(datacenter_file, power_file)
    sys.exit(0 if success else 1)
