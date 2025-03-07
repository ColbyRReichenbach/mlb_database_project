import pandas as pd
from pybaseball import team_batting, team_pitching, team_fielding
from sqlalchemy import create_engine
from mlb_database_project.config.db_config import get_db_connection_string, get_start_year


# Define expected schemas for each dataset
EXPECTED_SCHEMAS = {
    "batting": [
        "teamIDfg", "Season", "Team", "Age", "G", "AB", "PA", "H", "1B", "2B",
        "3B", "HR", "R", "RBI", "BB", "IBB", "SO", "HBP", "SF", "SH", "GDP",
        "SB", "CS", "AVG", "GB", "FB", "LD", "IFFB", "Pitches", "Balls",
        "Strikes", "IFH", "BU", "BUH", "BB%", "K%", "BB/K", "OBP", "SLG",
        "OPS", "ISO", "BABIP", "GB/FB", "LD%", "GB%", "FB%", "IFFB%", "HR/FB",
        "IFH%", "BUH%", "wOBA", "wRAA", "wRC", "Bat", "Fld", "Rep", "Pos",
        "RAR", "WAR", "Dol", "Spd", "wRC+", "WPA", "-WPA", "+WPA", "RE24",
        "REW", "pLI", "phLI", "PH", "WPA/LI", "Clutch", "FB% (Pitch)", "FBv",
        "SL%", "SLv", "CT%", "CTv", "CB%", "CBv", "CH%", "CHv", "SF%", "SFv",
        "KN%", "KNv", "XX%", "PO%", "wFB", "wSL", "wCT", "wCB", "wCH", "wSF",
        "wKN", "wFB/C", "wSL/C", "wCT/C", "wCB/C", "wCH/C", "wSF/C", "wKN/C",
        "O-Swing%", "Z-Swing%", "Swing%", "O-Contact%", "Z-Contact%",
        "Contact%", "Zone%", "F-Strike%", "SwStr%", "BsR", "FA% (sc)",
        "FT% (sc)", "FC% (sc)", "FS% (sc)", "FO% (sc)", "SI% (sc)", "SL% (sc)",
        "CU% (sc)", "KC% (sc)", "EP% (sc)", "CH% (sc)", "SC% (sc)", "KN% (sc)",
        "UN% (sc)", "vFA (sc)", "vFT (sc)", "vFC (sc)", "vFS (sc)", "vFO (sc)",
        "vSI (sc)", "vSL (sc)", "vCU (sc)", "vKC (sc)", "vEP (sc)", "vCH (sc)",
        "vSC (sc)", "vKN (sc)", "FA-X (sc)", "FT-X (sc)", "FC-X (sc)",
        "FS-X (sc)", "FO-X (sc)", "SI-X (sc)", "SL-X (sc)", "CU-X (sc)",
        "KC-X (sc)", "EP-X (sc)", "CH-X (sc)", "SC-X (sc)", "KN-X (sc)",
        "FA-Z (sc)", "FT-Z (sc)", "FC-Z (sc)", "FS-Z (sc)", "FO-Z (sc)",
        "SI-Z (sc)", "SL-Z (sc)", "CU-Z (sc)", "KC-Z (sc)", "EP-Z (sc)",
        "CH-Z (sc)", "SC-Z (sc)", "KN-Z (sc)"
    ],
    "pitching": [
        "teamIDfg", "Season", "Team", "Age", "W", "L", "ERA", "G", "GS", "CG",
        "ShO", "SV", "BS", "IP", "TBF", "H", "R", "ER", "HR", "BB", "IBB",
        "HBP", "WP", "BK", "SO", "GB", "FB", "LD", "IFFB", "Balls", "Strikes",
        "Pitches", "RS", "IFH", "BU", "BUH", "K/9", "BB/9", "K/BB", "H/9",
        "HR/9", "AVG", "WHIP", "BABIP", "LOB%", "FIP", "GB/FB", "LD%",
        "GB%", "FB%", "IFFB%", "HR/FB", "IFH%", "BUH%", "Starting", "Start-IP",
        "Relieving", "Relief-IP", "RAR", "WAR", "Dollars", "tERA", "xFIP",
        "WPA", "-WPA", "+WPA", "RE24", "REW", "pLI", "inLI", "gmLI", "exLI",
        "Pulls", "WPA/LI", "Clutch", "FB% 2", "FBv", "SL%", "SLv", "CT%",
        "CTv", "CB%", "CBv", "CH%", "CHv", "SF%", "SFv", "KN%", "KNv", "XX%",
        "PO%", "wFB", "wSL", "wCT", "wCB", "wCH", "wSF", "wKN", "wFB/C",
        "wSL/C", "wCT/C", "wCB/C", "wCH/C", "wSF/C", "wKN/C", "O-Swing%",
        "Z-Swing%", "Swing%", "O-Contact%", "Z-Contact%", "Contact%", "Zone%",
        "F-Strike%", "SwStr%", "HLD", "SD", "MD", "ERA-", "FIP-", "xFIP-",
        "K%", "BB%", "SIERA", "RS/9", "E-F", "FA% (sc)", "FT% (sc)", "FC% (sc)",
        "FS% (sc)", "FO% (sc)", "SI% (sc)", "SL% (sc)", "CU% (sc)", "KC% (sc)",
        "EP% (sc)", "CH% (sc)", "SC% (sc)", "KN% (sc)", "UN% (sc)", "vFA (sc)",
        "vFT (sc)", "vFC (sc)", "vFS (sc)", "vFO (sc)", "vSI (sc)", "vSL (sc)",
        "vCU (sc)", "vKC (sc)", "vEP (sc)", "vCH (sc)", "vSC (sc)", "vKN (sc)",
        "FA-X (sc)", "FT-X (sc)", "FC-X (sc)", "FS-X (sc)", "FO-X (sc)",
        "SI-X (sc)", "SL-X (sc)", "CU-X (sc)", "KC-X (sc)", "EP-X (sc)",
        "CH-X (sc)", "SC-X (sc)", "KN-X (sc)", "FA-Z (sc)", "FT-Z (sc)",
        "FC-Z (sc)", "FS-Z (sc)", "FO-Z (sc)", "SI-Z (sc)", "SL-Z (sc)",
        "CU-Z (sc)", "KC-Z (sc)", "EP-Z (sc)", "CH-Z (sc)", "SC-Z (sc)",
        "KN-Z (sc)"
    ],
    "fielding": [
        "teamIDfg", "Season", "Team", "G", "GS", "Inn", "PO", "A", "E", "FE", "TE",
        "DP", "DPS", "DPT", "DPF", "Scp", "SB", "CS", "PB", "WP", "FP", "TZ",
        "rSB", "rGDP", "rARM", "rGFP", "rPM", "DRS", "BIZ", "Plays", "RZR", "OOZ",
        "TZL", "FSR", "ARM", "DPR", "RngR", "ErrR", "UZR", "UZR/150", "CPP",
        "RPP", "Def", "0%", "# 0%", "1-10%", "# 1-10%", "10-40%", "# 10-40%",
        "40-60%", "# 40-60%", "60-90%", "# 60-90%", "90-100%", "# 90-100%",
        "rSZ", "rCERA", "rTS", "FRM", "OAA", "Range"
    ]
}


def ensure_schema(df, expected_cols):
    """Ensure that the DataFrame df has all expected_cols; add missing columns with None."""
    for col in expected_cols:
        if col not in df.columns:
            df[col] = None
    return df.reindex(columns=expected_cols)


def clean_data(df, schema_key):
    """
    Replace NaN values with None and ensure the DataFrame has the full expected schema
    for the given schema_key (e.g., "batting", "pitching", or "fielding").
    """
    expected_cols = EXPECTED_SCHEMAS[schema_key]
    df = ensure_schema(df, expected_cols)
    # Replace any remaining NaN with None
    return df.where(pd.notna(df), None)


def fetch_team_stats(year):
    print(f"Fetching team stats for {year}...")

    # Fetch team-level records
    batting_df = team_batting(year, ind=1)
    pitching_df = team_pitching(year, ind=1)
    fielding_df = team_fielding(year, ind=1)

    # Clean and ensure full schema for each DataFrame separately
    batting = clean_data(batting_df, "batting")
    pitching = clean_data(pitching_df, "pitching")
    fielding = clean_data(fielding_df, "fielding")

    return batting, pitching, fielding


def store_team_stats(batting, pitching, fielding, engine):
    batting.to_sql('team_batting', con=engine, if_exists='append', index=False)
    pitching.to_sql('team_pitching', con=engine, if_exists='append', index=False)
    fielding.to_sql('team_fielding', con=engine, if_exists='append', index=False)
    print(f"Stored team stats for year {year}")


if __name__ == "__main__":
    engine = create_engine(get_db_connection_string())
    start_year = get_start_year()
    for year in range(start_year, 2025):
        batting, pitching, fielding = fetch_team_stats(year)
        store_team_stats(batting, pitching, fielding, engine)
