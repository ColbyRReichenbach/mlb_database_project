import pandas as pd
from pybaseball import amateur_draft_by_team
from sqlalchemy import create_engine
from mlb_database_project.config.db_config import get_db_connection_string, get_start_year, get_team_abbreviations


def clean_data(df):
    df = df.dropna(axis=1, how='all')
    return df.where(pd.notna(df), None)


def fetch_draft(year, team):
    print(f"Fetching draft picks for {team} in {year}...")
    return clean_data(amateur_draft_by_team(year, team))


def store_draft(df, engine):
    df.to_sql('amateur_draft', con=engine, if_exists='append', index=False)
    print(f"Stored draft picks for {team} in {year}")


if __name__ == "__main__":
    engine = create_engine(get_db_connection_string())
    start_year = get_start_year()
    teams = get_team_abbreviations()
    for year in range(start_year, 2025):
        for team in teams:
            df = fetch_draft(year, team)
            store_draft(df, engine)
