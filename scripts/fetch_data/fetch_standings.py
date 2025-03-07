import pandas as pd
import numpy as np
from pybaseball import standings
from sqlalchemy import create_engine
from mlb_database_project.config.db_config import get_db_connection_string, get_start_year

# Expected standings column names (adjust as needed)
EXPECTED_STANDINGS = [
    "Tm", "W", "L", "W-L%", "GB"
]

def clean_data(df):
    df = df.dropna(axis=1, how='all')
    return df.where(pd.notna(df), None)

def fetch_standings(year):
    print(f"Fetching standings for {year}")
    data = standings(year)  # This returns a nested structure
    try:
        # Assume data is a list of 2D arrays (or DataFrames)
        # Flatten them into one 2D array.
        flattened = np.concatenate(data, axis=0)
        df = pd.DataFrame(flattened, columns=EXPECTED_STANDINGS)
    except Exception as e:
        print(f"Error flattening standings data for {year}: {e}")
        df = pd.DataFrame()
    df = clean_data(df)
    # Add the Season column to the DataFrame.
    df["Season"] = year
    return df

def store_standings(df, engine, year):
    df.to_sql('standings', con=engine, if_exists='append', index=False)
    print(f"Stored standings for {year}")

if __name__ == "__main__":
    engine = create_engine(get_db_connection_string())
    for year in range(get_start_year(), 2025):
        df = fetch_standings(year)
        if not df.empty:
            store_standings(df, engine, year)
