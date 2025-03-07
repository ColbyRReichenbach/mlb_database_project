import time
import pandas as pd
from pybaseball import team_game_logs
from sqlalchemy import create_engine
from mlb_database_project.config.db_config import get_db_connection_string, get_team_abbreviations, get_start_year

# Get database connection string & settings
DATABASE_URI = get_db_connection_string()
TEAMS = get_team_abbreviations()
START_YEAR = get_start_year()
END_YEAR = 2024  # Can adjust if needed - or after 2025 season


def clean_data(df):
    """Drops columns where every row is NaN and replaces other NaNs with None for PostgreSQL compatibility."""
    df = df.dropna(axis=1, how='all')
    return df.where(pd.notna(df), None)


# Function to fetch game logs for all teams over multiple seasons
def fetch_game_logs(save_to_db=True):
    """
    Fetches batting and pitching game logs for all teams over multiple seasons.

    Args:
        save_to_db (bool): If True, saves the data to PostgreSQL.

    Returns:
        Tuple of Pandas DataFrames (batting_logs_df, pitching_logs_df)
    """
    batting_logs_list = []
    pitching_logs_list = []

    for year in range(START_YEAR, END_YEAR + 1):
        for team in TEAMS:
            print(f"Fetching Batting Logs: {team} ({year})...")
            try:
                batting_logs = clean_data(team_game_logs(year, team, log_type="batting"))
                batting_logs["season"] = year
                batting_logs["team"] = team
                batting_logs_list.append(batting_logs)
            except Exception as e:
                print(f"Error fetching batting logs for {team} ({year}): {e}")

            print(f"Fetching Pitching Logs: {team} ({year})...")
            try:
                pitching_logs = clean_data(team_game_logs(year, team, log_type="pitching"))
                pitching_logs["season"] = year
                pitching_logs["team"] = team
                pitching_logs_list.append(pitching_logs)
            except Exception as e:
                print(f"Error fetching pitching logs for {team} ({year}): {e}")

            # Prevent API overloading
            time.sleep(1)

    # Convert lists to DataFrames
    batting_logs_df = pd.concat(batting_logs_list, ignore_index=True) if batting_logs_list else pd.DataFrame()
    pitching_logs_df = pd.concat(pitching_logs_list, ignore_index=True) if pitching_logs_list else pd.DataFrame()

    print("Game log data fetching complete.")

    # Save to PostgreSQL if enabled
    if save_to_db:
        save_game_logs_to_db(batting_logs_df, pitching_logs_df)

    return batting_logs_df, pitching_logs_df


# Function to save data to the database
def save_game_logs_to_db(batting_logs_df, pitching_logs_df):
    """
    Saves the game logs DataFrames into PostgreSQL.

    Args:
        batting_logs_df (DataFrame): Batting game logs.
        pitching_logs_df (DataFrame): Pitching game logs.
    """
    engine = create_engine(DATABASE_URI)

    with engine.connect() as conn:
        if not batting_logs_df.empty:
            batting_logs_df.to_sql("batting_game_logs", conn, if_exists="append", index=False)
            print("Batting logs saved to database.")

        if not pitching_logs_df.empty:
            pitching_logs_df.to_sql("pitching_game_logs", conn, if_exists="append", index=False)
            print("Pitching logs saved to database.")


if __name__ == "__main__":
    fetch_game_logs()
