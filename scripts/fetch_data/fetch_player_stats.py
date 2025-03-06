import pandas as pd
from pybaseball import batting_stats, pitching_stats
from sqlalchemy import create_engine
from mlb_database_project.config.db_config import get_db_connection_string, get_start_year


def clean_data(df):
    """Cleans data by removing fully NaN columns and replacing other NaNs with None."""
    df = df.dropna(axis=1, how='all')
    return df.where(pd.notna(df), None)


def fetch_player_stats(year):
    print(f"Fetching player stats for {year}...")
    batting = clean_data(batting_stats(year, ind=1))
    pitching = clean_data(pitching_stats(year, ind=1))
    return batting, pitching


def store_player_stats(batting, pitching, engine):
    batting.to_sql('player_batting_stats', con=engine, if_exists='append', index=False)
    pitching.to_sql('player_pitching_stats', con=engine, if_exists='append', index=False)
    print(f"Stored player stats for year {year}")


if __name__ == "__main__":
    engine = create_engine(get_db_connection_string())
    start_year = get_start_year()
    for year in range(start_year, 2025):
        batting, pitching = fetch_player_stats(year)
        store_player_stats(batting, pitching, engine)
