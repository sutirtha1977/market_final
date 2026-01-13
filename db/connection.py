import psycopg2
from psycopg2.extras import RealDictCursor
from config.logger import log
from config.db_table import DB_CONFIG   # expects dict with host, dbname, user, password, port

def get_db_connection():
    try:
        conn = psycopg2.connect(
            host=DB_CONFIG["host"],
            dbname=DB_CONFIG["dbname"],
            user=DB_CONFIG["user"],
            password=DB_CONFIG["password"],
            port=DB_CONFIG.get("port", 5432),
            connect_timeout=30
        )

        # Session-level settings
        with conn.cursor() as cur:
            cur.execute("SET statement_timeout = '5min';")

        return conn

    except Exception as e:
        log(f"DB CONNECTION FAILED: {e}")
        raise


def close_db_connection(conn):
    try:
        if conn:
            conn.close()
    except Exception as e:
        log(f"DB CLOSE FAILED: {e}")