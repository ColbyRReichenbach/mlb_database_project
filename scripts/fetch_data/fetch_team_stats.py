import pandas as pd
from pybaseball import team_batting, team_pitching, team_fielding
from sqlalchemy import create_engine
from mlb_database_project.config.db_config import get_db_connection_string, get_start_year


def clean_data(df):
    """Drops columns where every row is NaN and replaces other NaNs with None for PostgreSQL compatibility."""
    df = df.dropna(axis=1, how='all')
    return df.where(pd.notna(df), None)


def fetch_team_stats(year):
    print(f"Fetching team stats for {year}...")

    # Fetching data with `ind=1` to ensure team-level yearly records
    batting = clean_data(team_batting(year, ind=1))
    pitching = clean_data(team_pitching(year, ind=1))
    fielding = clean_data(team_fielding(year, ind=1))

    # Adding a 'year' column to ensure we can filter correctly in queries
    batting["year"] = year
    pitching["year"] = year
    fielding["year"] = year

    return batting, pitching, fielding


def store_team_stats(batting, pitching, fielding, engine):
    batting.to_sql('team_batting_stats', con=engine, if_exists='append', index=False)
    pitching.to_sql('team_pitching_stats', con=engine, if_exists='append', index=False)
    fielding.to_sql('team_fielding_stats', con=engine, if_exists='append', index=False)
    print(f"Stored team stats for year {year}")


if __name__ == "__main__":
    engine = create_engine(get_db_connection_string())
    start_year = get_start_year()
    for year in range(start_year, 2025):
        batting, pitching, fielding = fetch_team_stats(year)
        store_team_stats(batting, pitching, fielding, engine)
