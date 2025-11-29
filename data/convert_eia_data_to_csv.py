import pandas as pd
import sys
import os
import re
from pathlib import Path

def process_file(input_file, output_dir='.'):
    """Processes an EIA-861 file"""

    if not os.path.exists(input_file):
        print(f"Error: File not found: {input_file}")
        return False

    print(f"Reading: {input_file}")

    try:
        df = pd.read_excel(input_file, sheet_name=0, header=2)
        print(f"Loaded: {len(df):,} rows, {len(df.columns)} columns")
    except Exception as e:
        print(f"Error reading file: {e}")
        return False

    year = detect_year(df)
    if not year:
        print("Error: Could not detect year from data")
        return False

    print(f"Detected year: {year}")

    df_clean = clean_data(df, year)
    print(f"Cleaned dataset: {len(df_clean):,} rows")

    print("\n Data Summary:")
    if 'State' in df_clean.columns:
        state_counts = df_clean['State'].value_counts()
        print(f"    States: {len(state_counts)}")
        print(f"    Top 5 states: {', '.join(state_counts.head().index)}")

    if 'Price_Per_kWh' in df_clean.columns:
        valid_prices = df_clean['Price_Per_kWh'].dropna()
        if len(valid_prices) > 0:
            print(f"\n Price statistics:")
            print(f"    Mean price: ${valid_prices.mean():.4f}/kWh")
            print(f"    Median price: ${valid_prices.median():.4f}/kWh")
            print(f"    Min price: ${valid_prices.min():.4f}/kWh")
            print(f"    Max price: ${valid_prices.max():.4f}/kWh")

    os.makedirs(output_dir, exist_ok=True)
    output_file = os.path.join(output_dir, f'power_cost_{year}.csv')

    print(f"\nSaving to: {output_file}")
    df_clean.to_csv(output_file, index=False)

    print("Saving complete: Preview:")
    preview_cols = ['Utility_Name', 'State', 'Year', 'Price_Per_kWh']
    available_preview = [col for col in preview_cols if col in df_clean.columns]
    print(df_clean[available_preview].head().to_string(index=False))

    return True


def detect_year(df):
    """Detect year from "Data Year" column"""
    column_name = 'Data Year'

    if column_name in df.columns:
        years = df[column_name].dropna().unique()
        if len(years) == 1:
            return int(years[0])
        else:
            # Return most common value for year
            return int(df[column_name].mode()[0])

    else:
        print("Warning: Year column not detected")
        print(df.columns)
        return None


def clean_data(df, year):
    """Clean and standardize EIA-861 data"""

    revenue_cols = [col for col in df.columns if 'Thousand Dollars' in str(col) or 'Revenue' in str(col)]
    sales_cols = [col for col in df.columns if 'Megawatthours' in str(col) or 'Sales' in str(col) or 'MWh' in str(col)]
    customer_cols = [col for col in df.columns if 'Count' in str(col) or 'Customer' in str(col)]

    column_map = {}

    for col in df.columns:
        if 'Utility Number' in str(col) or 'Utility ID' in str(col):
            column_map[col] = 'Utility_ID'
        elif 'Utility Name' in str(col):
            column_map[col] = 'Utility_Name'
        elif col == 'State' or 'State' in str(col):
            column_map[col] = 'State'

    if revenue_cols:
        total = None
        for col in reversed(revenue_cols):
            if '.4' in str(col) or 'Total' in str(col):
                total = col
                break
        if not total:
            # If there is no 'total' column, use the last column
            total = revenue_cols[-1] 
        column_map[total] = 'Revenue_Thousands'

    if sales_cols:
        total = None
        for col in reversed(sales_cols):
            if '.4' in str(col) or 'Total' in str(col):
                total = col
                break
        if not total:
            # If there is no 'total' column, use the last column
            total = sales_cols[1]
        column_map[total] = 'Sales_MWh'

    if customer_cols:
        total = None
        for col in reversed(customer_cols):
            if '.4' in str(col) or 'Total' in str(col):
                total = col
                break
        if not total:
            # If there is no 'total' column, use the last column
            total = customer_cols[1]
        column_map[total] = 'Customers'

    if not column_map:
        print(f"Warning: Could not find any standard EIA-816 columns from {list(df.columns[:10])}")
        print(f"Available columns: {list(df.columns[:10])}")
        return pd.DataFrame()

    df_clean = df[list(column_map.keys())].copy()
    df_clean = df_clean.rename(columns=column_map)
    df_clean['Year'] = year

    number_cols = ['Revenue_Thousands', 'Sales_MWh', 'Customers']
    for col in number_cols:
        df_clean[col] = pd.to_numeric(df_clean[col], errors='coerce')

    if 'Revenue_Thousands' in df_clean.columns and 'Sales_MWh' in df_clean.columns:
        df_clean['Price_Per_kWh'] = (df_clean['Revenue_Thousands'] * 1000) / (df_clean['Sales_MWh'] * 1000)

    df_clean['Utility_Name'] = df_clean['Utility_Name'].astype(str).str.strip()
    df_clean['State'] = df_clean['State'].astype(str).str.strip().str.upper()

    return df_clean


def show_help():
    """Help menu"""
    print("Usage: Specify the EIA-861 Excel file to be converted to a csv")


def main():
    if len(sys.argv) != 2:
        show_help()
        sys.exit(1)

    input_file = sys.argv[1]
    output_dir = 'cleaned_eia_years'

    success = process_file(input_file, output_dir)
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()
