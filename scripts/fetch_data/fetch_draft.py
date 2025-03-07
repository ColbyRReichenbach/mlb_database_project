import pandas as pd
from pybaseball import amateur_draft_by_team
from sqlalchemy import create_engine
from mlb_database_project.config.db_config import get_db_connection_string, get_start_year, get_team_abbreviations

EXPECTED_DRAFT = [
    "OvPck", "Tm", "Signed", "Bonus", "Name", "Pos", "WAR", "G", "AB",
    "HR", "BA", "OPS", "G.1", "W", "L", "ERA", "WHIP", "SV", "Type", "Drafted Out of"
]


def ensure_schema(df, expected_cols):
    for col in expected_cols:
        if col not in df.columns:
            df[col] = None
    return df.reindex(columns=expected_cols)


def clean_data(df):
    return df.where(pd.notna(df), None)


def fetch_draft(year, team):
    print(f"Fetching draft picks for {team} in {year}...")
    df = amateur_draft_by_team(year, team)
    df = ensure_schema(df, EXPECTED_DRAFT)
    return clean_data(df)


def store_draft(df, engine):
    df.to_sql('amateur_draft_by_team', con=engine, if_exists='append', index=False)
    print("Stored draft picks.")


if __name__ == "__main__":
    engine = create_engine(get_db_connection_string())
    teams = get_team_abbreviations()
    for year in range(get_start_year(), 2025):
        for team in teams:
            df = fetch_draft(year, team)
            store_draft(df, engine)
