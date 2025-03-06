import pandas as pd
from pybaseball import (
    team_batting, team_pitching, team_fielding,
    standings, schedule_and_record, batting_stats,
    pitching_stats, amateur_draft_by_team, team_game_logs
)
from mlb_database_project.config.db_config import get_team_abbreviations

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)

# Set test year and team
TEST_YEAR = 2023
TEST_TEAM = "NYY"


def fetch_and_inspect(fetch_function, *args):
    """Fetches data and prints column names & data types."""
    try:
        df = fetch_function(*args)
        if isinstance(df, pd.DataFrame):
            print(f"\n=== {fetch_function.__name__} ({args}) ===")
            print(df.dtypes)  # Display column names and data types
            print(df.head(3))  # Display first few rows
        else:
            print(f"\n[WARNING] {fetch_function.__name__} did not return a DataFrame.")
    except Exception as e:
        print(f"\n[ERROR] {fetch_function.__name__} ({args}): {e}")


if __name__ == "__main__":
    print("\n=== Running Data Fetch Tests ===")

    # Team stats (batting, pitching, fielding) - Yearly
    fetch_and_inspect(team_batting, TEST_YEAR)
    fetch_and_inspect(team_pitching, TEST_YEAR)
    fetch_and_inspect(team_fielding, TEST_YEAR)

    # Standings - Yearly
    fetch_and_inspect(standings, TEST_YEAR)

    # Game logs (schedule & record) - Team-based
    fetch_and_inspect(schedule_and_record, TEST_YEAR, TEST_TEAM)

    # Player stats (batting & pitching) - Yearly
    fetch_and_inspect(batting_stats, TEST_YEAR)
    fetch_and_inspect(pitching_stats, TEST_YEAR)

    # Amateur Draft - Team-based
    fetch_and_inspect(amateur_draft_by_team, TEST_YEAR, TEST_TEAM)

    # Team Game Logs - Batting & Pitching
    fetch_and_inspect(team_game_logs, TEST_YEAR, TEST_TEAM, "batting")
    fetch_and_inspect(team_game_logs, TEST_YEAR, TEST_TEAM, "pitching")

    print("\n=== Test Complete ===")
