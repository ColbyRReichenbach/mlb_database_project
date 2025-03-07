import pandas as pd
from pybaseball import amateur_draft_by_team
from sqlalchemy import create_engine
from mlb_database_project.config.db_config import get_db_connection_string, get_start_year, get_draft

# Expected draft schema (adjust if needed)
EXPECTED_DRAFT = [
    "OvPck", "Tm", "Signed", "Bonus", "Name", "Pos", "WAR", "G", "AB",
    "HR", "BA", "OPS", "G.1", "W", "L", "ERA", "WHIP", "SV", "Type", "Drafted Out of"
]


def ensure_schema(df, expected_cols):
    """Ensure that DataFrame df has all expected_cols; add missing columns with None."""
    for col in expected_cols:
        if col not in df.columns:
            df[col] = None
    return df.reindex(columns=expected_cols)


def clean_data(df):
    """Replace NaN values with None for PostgreSQL compatibility."""
    return df.where(pd.notna(df), None)


def fetch_draft(year, team_code, keep_stats=True):
    """
    Fetch draft picks for the given team code and year.

    Args:
        year (int): The draft year.
        team_code (str): The team code from DRAFT_TEAM_CODES.
        keep_stats (bool): Whether to keep player stats (default True).

    Returns:
        DataFrame: The cleaned draft picks DataFrame.
    """
    print(f"Fetching draft picks for team {team_code} in {year}...")
    df = amateur_draft_by_team(team_code, year, keep_stats=keep_stats)
    df = ensure_schema(df, EXPECTED_DRAFT)
    return clean_data(df)


def store_draft(df, engine):
    df.to_sql('amateur_draft_by_team', con=engine, if_exists='append', index=False)
    print("Stored draft picks.")


if __name__ == "__main__":
    engine = create_engine(get_db_connection_string())
    start_year = get_start_year()
    teams = get_draft()
    for year in range(start_year, 2025):
        for team_code in teams:
            try:
                df = fetch_draft(1980, team_code)
                store_draft(df, engine)
            except Exception as e:
                print(f"Error processing draft for team {team_code} in {year}: {e}")