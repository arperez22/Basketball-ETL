import os
from dotenv import load_dotenv
import psycopg2
from etl import run_pipeline
from etl.migrations import link_players_to_teams

def main():
    load_dotenv()

    host = os.getenv('DB_HOST')
    dbname = os.getenv('DB_NAME')
    user = os.getenv('DB_USER')
    password = os.getenv('DB_PASSWORD')
    port = os.getenv('DB_PORT')

    conn = psycopg2.connect(
        host=host,
        dbname=dbname,
        user=user,
        password=password,
        port=port
    )

    # Extract, Transform, Load
    run_pipeline(conn)

    # Foreign Key Fix
    link_players_to_teams(conn)

    conn.close()


if __name__ == '__main__':
    main()