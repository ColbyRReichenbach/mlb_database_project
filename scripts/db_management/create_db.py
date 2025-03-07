import os
import psycopg2
from psycopg2 import sql
import urllib.parse

from mlb_database_project.config.db_config import get_db_connection_string

# Determine the base directory and SQL file path
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SQL_FILE_PATH = os.path.join(BASE_DIR, "create_mlb_database.sql")


def create_database():
    """
    Connects to the default 'postgres' database to check if our target database exists;
    if not, creates it.
    """
    conn_str = get_db_connection_string()
    # Parse connection string to extract components
    url = urllib.parse.urlparse(conn_str)
    target_db = url.path.lstrip('/')
    # Create a connection string to the default 'postgres' database.
    default_conn_str = f"postgresql://{url.username}:{url.password}@{url.hostname}:{url.port}/postgres"

    try:
        with psycopg2.connect(default_conn_str) as conn:
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (target_db,))
                exists = cur.fetchone()
                if not exists:
                    cur.execute(sql.SQL("CREATE mlb_data {}").format(sql.Identifier(target_db)))
                    print(f"Database '{target_db}' created.")
                else:
                    print(f"Database '{target_db}' already exists.")
    except Exception as e:
        print("Error while creating the database:", e)


def create_tables():
    """
    Connects to the target database and executes the SQL file to create all tables.
    """
    conn_str = get_db_connection_string()
    try:
        with psycopg2.connect(conn_str) as conn:
            with conn.cursor() as cur:
                with open(SQL_FILE_PATH, "r") as file:
                    sql_commands = file.read()
                    cur.execute(sql_commands)
                    conn.commit()
                    print("Tables created successfully.")
    except Exception as e:
        print("Error while creating tables:", e)


if __name__ == "__main__":
    create_database()
    create_tables()
