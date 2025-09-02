import os
from dotenv import load_dotenv
import psycopg2
from pipeline import run_pipeline


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

    run_pipeline(conn)

    conn.close()


if __name__ == '__main__':
    main()