import pandas as pd
from pybaseball import batting_stats, pitching_stats
from sqlalchemy import create_engine
from mlb_database_project.config.db_config import get_db_connection_string, get_start_year

EXPECTED_PLAYER_BATTING = [
    "IDfg", "Season", "Name", "Team", "Age", "G", "AB", "PA", "H", "1B", "2B",
    "3B", "HR", "R", "RBI", "BB", "IBB", "SO", "HBP", "SF", "SH", "GDP", "SB",
    "CS", "AVG", "GB", "FB", "LD", "IFFB", "Pitches", "Balls", "Strikes", "IFH",
    "BU", "BUH", "BB%", "K%", "BB/K", "OBP", "SLG", "OPS", "ISO", "BABIP", "GB/FB",
    "LD%", "GB%", "FB%", "IFFB%", "HR/FB", "IFH%", "BUH%", "wOBA", "wRAA", "wRC",
    "Bat", "Fld", "Rep", "Pos", "RAR", "WAR", "Dol", "Spd", "wRC+", "WPA",
    "-WPA", "+WPA", "RE24", "REW", "pLI", "phLI", "PH", "WPA/LI", "Clutch",
    "FB% (Pitch)", "FBv", "SL%", "SLv", "CT%", "CTv", "CB%", "CBv", "CH%", "CHv",
    "SF%", "SFv", "KN%", "KNv", "XX%", "PO%", "wFB", "wSL", "wCT", "wCB", "wCH",
    "wSF", "wKN", "wFB/C", "wSL/C", "wCT/C", "wCB/C", "wCH/C", "wSF/C", "wKN/C",
    "O-Swing%", "Z-Swing%", "Swing%", "O-Contact%", "Z-Contact%", "Contact%",
    "Zone%", "F-Strike%", "SwStr%", "BsR", "FA% (sc)", "FT% (sc)", "FC% (sc)",
    "FS% (sc)", "FO% (sc)", "SI% (sc)", "SL% (sc)", "CU% (sc)", "KC% (sc)",
    "EP% (sc)", "CH% (sc)", "SC% (sc)", "KN% (sc)", "UN% (sc)", "vFA (sc)",
    "vFT (sc)", "vFC (sc)", "vFS (sc)", "vFO (sc)", "vSI (sc)", "vSL (sc)",
    "vCU (sc)", "vKC (sc)", "vEP (sc)", "vCH (sc)", "vSC (sc)", "vKN (sc)",
    "FA-X (sc)", "FT-X (sc)", "FC-X (sc)", "FS-X (sc)", "FO-X (sc)", "SI-X (sc)",
    "SL-X (sc)", "CU-X (sc)", "KC-X (sc)", "EP-X (sc)", "CH-X (sc)", "SC-X (sc)",
    "KN-X (sc)", "FA-Z (sc)", "FT-Z (sc)", "FC-Z (sc)", "FS-Z (sc)", "FO-Z (sc)",
    "SI-Z (sc)", "SL-Z (sc)", "CU-Z (sc)", "KC-Z (sc)", "EP-Z (sc)", "CH-Z (sc)",
    "SC-Z (sc)", "KN-Z (sc)"
]

EXPECTED_PLAYER_PITCHING = [
    "IDfg", "Season", "Name", "Team", "Age", "W", "L", "WAR", "ERA", "G", "GS",
    "CG", "ShO", "SV", "BS", "IP", "TBF", "H", "R", "ER", "HR", "BB", "IBB",
    "HBP", "WP", "BK", "SO", "GB", "FB", "LD", "IFFB", "Balls", "Strikes",
    "Pitches", "RS", "IFH", "BU", "BUH", "K/9", "BB/9", "K/BB", "H/9",
    "HR/9", "AVG", "WHIP", "BABIP", "LOB%", "FIP", "GB/FB", "LD%",
    "GB%", "FB%", "IFFB%", "HR/FB", "IFH%", "BUH%", "Starting", "Start-IP",
    "Relieving", "Relief-IP", "RAR", "Dollars", "tERA", "xFIP", "WPA",
    "-WPA", "+WPA", "RE24", "REW", "pLI", "inLI", "gmLI", "exLI", "Pulls",
    "WPA/LI", "Clutch", "FB% 2", "FBv", "SL%", "SLv", "CT%", "CTv", "CB%",
    "CBv", "CH%", "CHv", "SF%", "SFv", "KN%", "KNv", "XX%", "PO%", "wFB",
    "wSL", "wCT", "wCB", "wCH", "wSF", "wKN", "wFB/C", "wSL/C", "wCT/C",
    "wCB/C", "wCH/C", "wSF/C", "wKN/C", "O-Swing%", "Z-Swing%", "Swing%",
    "O-Contact%", "Z-Contact%", "Contact%", "Zone%", "F-Strike%", "SwStr%",
    "HLD", "SD", "MD", "ERA-", "FIP-", "xFIP-", "K%", "BB%", "SIERA", "RS/9",
    "E-F", "FA% (sc)", "FT% (sc)", "FC% (sc)", "FS% (sc)", "FO% (sc)", "SI% (sc)",
    "SL% (sc)", "CU% (sc)", "KC% (sc)", "EP% (sc)", "CH% (sc)", "SC% (sc)",
    "KN% (sc)", "UN% (sc)", "vFA (sc)", "vFT (sc)", "vFC (sc)", "vFS (sc)",
    "vFO (sc)", "vSI (sc)", "vSL (sc)", "vCU (sc)", "vKC (sc)", "vEP (sc)",
    "vCH (sc)", "vSC (sc)", "vKN (sc)", "FA-X (sc)", "FT-X (sc)", "FC-X (sc)",
    "FS-X (sc)", "FO-X (sc)", "SI-X (sc)", "SL-X (sc)", "CU-X (sc)", "KC-X (sc)",
    "EP-X (sc)", "CH-X (sc)", "SC-X (sc)", "KN-X (sc)", "FA-Z (sc)", "FT-Z (sc)",
    "FC-Z (sc)", "FS-Z (sc)", "FO-Z (sc)", "SI-Z (sc)", "SL-Z (sc)", "CU-Z (sc)",
    "KC-Z (sc)", "EP-Z (sc)", "CH-Z (sc)", "SC-Z (sc)", "KN-Z (sc)"
]


def ensure_schema(df, expected_cols):
    for col in expected_cols:
        if col not in df.columns:
            df[col] = None
    return df.reindex(columns=expected_cols)


def clean_data(df):
    return df.where(pd.notna(df), None)


def fetch_player_stats(year):
    print(f"Fetching player stats for {year}...")
    batting_df = batting_stats(year, ind=1)
    pitching_df = pitching_stats(year, ind=1)
    batting = ensure_schema(batting_df, EXPECTED_PLAYER_BATTING)
    batting = clean_data(batting)
    pitching = ensure_schema(pitching_df, EXPECTED_PLAYER_PITCHING)
    pitching = clean_data(pitching)
    return batting, pitching


def store_player_stats(batting, pitching, engine):
    batting.to_sql('player_batting_stats', con=engine, if_exists='append', index=False)
    pitching.to_sql('player_pitching_stats', con=engine, if_exists='append', index=False)
    print("Stored player stats.")


if __name__ == "__main__":
    engine = create_engine(get_db_connection_string())
    for year in range(get_start_year(), 2025):
        batting, pitching = fetch_player_stats(year)
        store_player_stats(batting, pitching, engine)
