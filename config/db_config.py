import os
import yaml

from dotenv import load_dotenv
load_dotenv()  # Loads variables from .env in the current directory

# Ensure the script finds settings.yaml
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SETTINGS_PATH = os.path.join(BASE_DIR, "config", "settings.yaml")

# Debugging: Check if the path exists
if not os.path.exists(SETTINGS_PATH):
    raise FileNotFoundError(f"settings.yaml not found at {SETTINGS_PATH}")

# Load YAML settings
with open(SETTINGS_PATH, "r") as file:
    config = yaml.safe_load(file)

# Override sensitive values from environment variables
config["database"]["username"] = os.getenv("MLB_DB_USER", config["database"]["username"])
config["database"]["password"] = os.getenv("MLB_DB_PASSWORD", config["database"]["password"])


# Function to return database connection string
def get_db_connection_string():
    return (
        f"postgresql://{config['database']['username']}:{config['database']['password']}"
        f"@{config['database']['server']}:{config['database']['port']}/{config['database']['name']}"
    )


# Function to return team abbreviations
def get_team_abbreviations():
    return config["settings"]["team_abbreviations"]


# Function to return start year
def get_start_year():
    return config["settings"]["start_year"]
