import polars as pl
from contextlib import contextmanager
from sqlalchemy import create_engine

from dagster import IOManager, OutputContext, InputContext


@contextmanager
def connect_mysql(config: dict):
    conn_info = (
        f"mysql+pymysql://{config['user']}:{config['password']}"
        + f"@{config['host']}:{config['port']}"
        + f"/{config['database']}"
    )
    db_conn = create_engine(conn_info)
    try:
        yield db_conn
    except Exception:
        raise


class MySQLIOManager(IOManager):

    def __init__(self, config):
        self._config = config

    def handle_output(self, context: OutputContext, obj: pl.DataFrame):
        pass

    def load_input(self, context: InputContext) -> pl.DataFrame:
        pass

    def extract_data(self, sql: str) -> pl.DataFrame:
        with connect_mysql(self._config) as db_conn:
            pd_data = pl.read_database(query=sql, connection=db_conn)
            return pd_data