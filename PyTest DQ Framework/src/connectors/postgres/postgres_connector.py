import psycopg2
import pandas as pd

class PostgresConnectorContextManager:
    def __init__(self, db_host: str, db_name: str, db_port: str, db_user: str, db_password: str):
        self.db_host = db_host
        self.db_name = db_name
        self.db_port = db_port
        self.db_user = db_user
        self.db_password = db_password
        self.conn = None

    def __enter__(self):
        self.conn = psycopg2.connect(
            host=self.db_host,
            dbname=self.db_name,
            port=self.db_port,
            user=self.db_user,
            password=self.db_password
        )
        return self

    def __exit__(self, exc_type, exc_value, exc_tb):
        if self.conn:
            try:
                self.conn.close()
                if exc_type:
                    print(f"Exception occurred: {exc_type.__name__}: {exc_value}")
            except Exception as close_exc:
                print(f"Error closing the connection: {close_exc}")
        return False

    def get_data_sql(self, sql: str) -> pd.DataFrame:
        if not self.conn:
            raise ValueError("Connection is not established.")
        try:
            with self.conn.cursor() as cur:
                cur.execute(sql)
                columns = [desc[0] for desc in cur.description]
                data = cur.fetchall()
                return pd.DataFrame(data, columns=columns)
        except Exception as e:
            print(f"Error executing query: {e}")
            raise
