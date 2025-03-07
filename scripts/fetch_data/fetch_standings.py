import pandas as pd
from pybaseball import standings
from sqlalchemy import create_engine
from mlb_database_project.config.db_config import get_db_connection_string, get_start_year, get_team_abbreviations


def clean_data(df):
    df = df.dropna(axis=1, how='all')
    return df.where(pd.notna(df), None)


def fetch_standings(year):
    print(f"Fetching standings for {year}")
    return clean_data(standings(year))


def store_standings(df, engine):
    df.to_sql('team_standings', con=engine, if_exists='append', index=False)
    print(f"Stored standings for {year}")


if __name__ == "__main__":
    engine = create_engine(get_db_connection_string())
    start_year = get_start_year()
    teams = get_team_abbreviations()
    for year in range(start_year, 2025):
        df = fetch_standings(year)
        store_standings(df, engine)
