import time
import pandas as pd
from pybaseball import team_game_logs
from sqlalchemy import create_engine
from mlb_database_project.config.db_config import get_db_connection_string, get_team_abbreviations, get_start_year

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
    for col in expected_cols:
        if col not in df.columns:
            df[col] = None
    return df.reindex(columns=expected_cols)


def clean_data(df, schema):
    df = ensure_schema(df, schema)
    return df.where(pd.notna(df), None)


def fetch_game_logs(save_to_db=True):
    batting_logs_list = []
    pitching_logs_list = []
    TEAMS = get_bref()
    for year in range(get_start_year(), 2025):
        for team in TEAMS:
            print(f"Fetching Batting Logs: {team} ({year})...")
            try:
                df_b = team_game_logs(year, team, log_type="batting")
                df_b = clean_data(df_b, EXPECTED_GAMELOGS_BATTING)
                df_b["season"] = year
                df_b["team"] = team
                batting_logs_list.append(df_b)
            except Exception as e:
                print(f"Error fetching batting logs for {team} ({year}): {e}")
            print(f"Fetching Pitching Logs: {team} ({year})...")
            try:
                df_p = team_game_logs(year, team, log_type="pitching")
                df_p = clean_data(df_p, EXPECTED_GAMELOGS_PITCHING)
                df_p["season"] = year
                df_p["team"] = team
                pitching_logs_list.append(df_p)
            except Exception as e:
                print(f"Error fetching pitching logs for {team} ({year}): {e}")
            time.sleep(1)
    batting_logs_df = pd.concat(batting_logs_list, ignore_index=True) if batting_logs_list else pd.DataFrame()
    pitching_logs_df = pd.concat(pitching_logs_list, ignore_index=True) if pitching_logs_list else pd.DataFrame()
    if save_to_db:
        engine = create_engine(get_db_connection_string())
        with engine.connect() as conn:
            if not batting_logs_df.empty:
                batting_logs_df.to_sql("team_game_logs_batting", conn, if_exists="append", index=False)
                print("Batting logs saved to database.")
            if not pitching_logs_df.empty:
                pitching_logs_df.to_sql("team_game_logs_pitching", conn, if_exists="append", index=False)
                print("Pitching logs saved to database.")
    return batting_logs_df, pitching_logs_df


if __name__ == "__main__":
    fetch_game_logs()
