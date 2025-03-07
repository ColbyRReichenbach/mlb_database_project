import time
import pandas as pd
from pybaseball import team_game_logs
from sqlalchemy import create_engine
from mlb_database_project.config.db_config import get_db_connection_string, get_bref, get_start_year

# Expected column schemas for batting and pitching game logs:
EXPECTED_GAMELOGS_BATTING = [
    "Game", "Date", "Home", "Opp", "Rslt", "PA", "AB", "R", "H", "2B", "3B",
    "HR", "RBI", "BB", "IBB", "SO", "HBP", "SH", "SF", "ROE", "GDP", "SB",
    "CS", "BA", "OBP", "SLG", "OPS", "LOB", "NumPlayers", "Thr", "OppStart"
]

EXPECTED_GAMELOGS_PITCHING = [
    "Game", "Date", "Home", "Opp", "Rslt", "IP", "H", "R", "ER", "UER", "BB",
    "SO", "HR", "HBP", "ERA", "BF", "Pit", "Str", "IR", "IS", "SB", "CS", "AB",
    "2B", "3B", "IBB", "SH", "SF", "ROE", "GDP", "NumPlayers", "Umpire", "PitchersUsed"
]

def ensure_schema(df, expected_cols):
    """Ensure DataFrame df has every column in expected_cols, adding any missing as None."""
    for col in expected_cols:
        if col not in df.columns:
            df[col] = None
    return df.reindex(columns=expected_cols)

def clean_data(df, schema):
    """Clean df by ensuring full schema and replacing NaN with None."""
    df = ensure_schema(df, schema)
    return df.where(pd.notna(df), None)

def fetch_and_store_game_logs():
    engine = create_engine(get_db_connection_string())
    teams = get_bref()
    start_year = get_start_year()

    for year in range(start_year, 2025):
        for team in teams:
            # Process batting logs
            print(f"Fetching Batting Logs: {team} ({year})...")
            try:
                df_b = team_game_logs(year, team, log_type="batting")
                df_b = clean_data(df_b, EXPECTED_GAMELOGS_BATTING)
                df_b["season"] = year
                df_b["team"] = team
                df_b.to_sql("team_game_logs_batting", engine, if_exists="append", index=False)
                print(f"Stored batting logs for {team} ({year}).")
            except Exception as e:
                print(f"Error processing batting logs for {team} ({year}): {e}")

            # Process pitching logs
            print(f"Fetching Pitching Logs: {team} ({year})...")
            try:
                df_p = team_game_logs(year, team, log_type="pitching")
                df_p = clean_data(df_p, EXPECTED_GAMELOGS_PITCHING)
                df_p["season"] = year
                df_p["team"] = team
                df_p.to_sql("team_game_logs_pitching", engine, if_exists="append", index=False)
                print(f"Stored pitching logs for {team} ({year}).")
            except Exception as e:
                print(f"Error processing pitching logs for {team} ({year}): {e}")

            # Avoid overloading the API
            time.sleep(1)

if __name__ == "__main__":
    fetch_and_store_game_logs()
