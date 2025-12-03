#!/usr/bin/env python3
import csv

INPUT_CSV = "kaggle_raw.csv"
OUTPUT_CSV = "kaggle_fixed.csv"

DESIRED_HEADER = [
    "Facility_ID",
    "Facility_Name",
    "Operator",
    "City",
    "State",
    "Utility_Name",
    "Latitude",
    "Longitude",
    "Opening_Year",
    "Opening_Month",
    "Capacity_MW",
    "Verified",
    "Data_Source",
    "Source_URL",
    "Notes",
]

def main():
    with open(INPUT_CSV, newline="", encoding="utf-8") as fin, \
         open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as fout:

        reader = csv.reader(fin)
        writer = csv.writer(fout)

        writer.writerow(DESIRED_HEADER)

        for row in reader:
            # Skip empty lines
            if not row:
                continue

            if len(row) != 10:
                continue

            facility_id, name, operator, city, state, lat, lon, year, cap, utility = row

            new_row = [
                facility_id,          # Facility_ID
                name,                 # Facility_Name
                operator,             # Operator
                city,                 # City
                state,                # State
                utility,              # Utility_Name
                lat,                  # Latitude
                lon,                  # Longitude
                year,                 # Opening_Year
                "",                   # Opening_Month (unknown)
                cap,                  # Capacity_MW
                "",                   # Verified
                "",   # Data_Source
                "",                   # Source_URL
                "",                   # Notes
            ]

            writer.writerow(new_row)

if __name__ == "__main__":
    main()
