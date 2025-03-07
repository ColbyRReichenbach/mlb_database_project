## [Version 1.1] - 2025-03-06
### Changes:
- Updated `db_config.py` to load settings from 'settings.yaml'
- Added `.gitignore` rule to exclude `settings.yaml
- Removed `settings.yaml` from Git tracking for security
- Verified that database credentials are now securely loaded via environment variables

## [Version 1.2] - 2025-03-06
### Added:
- Created scripts to fetch and store MLB data:
  - Team stats (batting, pitching, fielding)
  - Player stats (batting & pitching)
  - Yearly standings
  - Game logs for each team
  - Amateur draft picks
- Implemented data cleaning (dropping fully empty columns, replacing NaNs with None)
- Ensured database integration with PostgreSQL

### Next Steps:
- Run fetch scripts to analyze column structure
- Define the final database schema based on fetched data

## [Version 1.2] - 2025-03-06 7:00pm est
### Changes:
- reworked db_management subfolder - merged create_tables into create_db - created create_mlb_database.sql for schema.
- updated fetch_game_logs to include clean function

- Next - fetch data and store it.

## [Version 1.3]] - 2025-03-9:44pm est
- Rewrote fetches due to errors with missing columns in data - uses expected columns, filling wiht none if no values
- added a bref abbreviations in settings due to bref having different abbreviations for teams
- Rewrote create_mlb_database to ensure data types and column anmes were correcty

