from contextlib import contextmanager
from datetime import datetime
from psycopg2 import sql
import psycopg2.extras
import psycopg2

import polars as pl
from dagster import IOManager, OutputContext, InputContext
from sqlalchemy import create_engine


@contextmanager
def connect_psql(config: dict):
    try:
        yield psycopg2.connect(
            host=config["host"],
            port=config["port"],
            database=config["database"],
            user=config["user"],
            password=config["password"],
        )
        
    except Exception as e:
        raise e
    
    
class PostgreSQLIOManager(IOManager):
    
    def __init__(self, config):
        self._config = config
    
    def load_input(self, context: InputContext) -> pl.DataFrame:
        pass

    def handle_output(self, context: OutputContext, obj: pl.DataFrame):
        schema, table = context.asset_key.path[-2], context.asset_key.path[-1]
        tmp_tbl = f"{table}_tmp_{datetime.now().strftime('%Y_%m_%d')}"
        
        with connect_psql(self._config) as db_conn:
            primary_keys = (context.metadata or {}).get("primary_keys", [])
            ls_columns = (context.metadata or {}).get("columns", [])
            
            with db_conn.cursor() as cursor:
                # create temp table
                cursor.execute(
                    f'CREATE TEMP TABLE IF NOT EXISTS "{tmp_tbl}" (LIKE {schema}.{table})'
                )
                cursor.execute(f'SELECT COUNT(*) FROM "{tmp_tbl}"')
                context.log.debug(
                    f"Log for creating temp table: {cursor.fetchall()}"
                )
                # cursor.execute(
                #     sql.SQL("CREATE TEMP TABLE IF NOT EXISTS {} (LIKE {}.{});").format(
                #         sql.Identifier(tmp_tbl),
                #         sql.Identifier(schema),
                #         sql.Identifier(table),
                #     )
                # )
                
                # insert new data
                try:
                    columns = sql.SQL(",").join(
                        sql.Identifier(name.lower()) for name in obj.columns
                    )
                    context.log.info(f"Table {table} with columns: {columns}")
                    values = sql.SQL(",").join(sql.Placeholder() for _ in obj.columns)
                    
                    context.log.debug("Inserting data into temp table")
                    insert_query = sql.SQL('INSERT INTO {} ({}) VALUES({});').format(
                        sql.Identifier(tmp_tbl), columns, values
                    )
                    psycopg2.extras.execute_batch(cursor, insert_query, obj.rows())
                    context.log.info(f"Insert into data for table {table} Success !!!")
                    
                    db_conn.commit()
                    
                except Exception as e:
                    raise e
                
            with db_conn.cursor() as cursor:
                # check data inserted
                cursor.execute(f'SELECT COUNT(*) FROM "{tmp_tbl}"')
                context.log.info(f"Number of rows inserted: {cursor.fetchone()}")
                    
                # upsert data
                if len(primary_keys) > 0:
                    conditions = " AND ".join(
                        [
                            f""" {schema}.{table}."{k.lower()}" = "{tmp_tbl}"."{k.lower()}" """
                            for k in primary_keys
                        ]
                    )
                    command = f"""
                        BEGIN TRANSACTION;
                        DELETE FROM {schema}.{table}
                        USING "{tmp_tbl}"
                        WHERE {conditions};
                        
                        INSERT INTO {schema}.{table}
                        SELECT * FROM "{tmp_tbl}";
                        
                        END TRANSACTION;
                    """
                else:
                    command = f"""
                        BEGIN TRANSACTION;
                        TRUNCATE TABLE {schema}.{table};

                        INSERT INTO {schema}.{table}
                        SELECT * FROM "{tmp_tbl}";
                        
                        END TRANSACTION;
                    """

                cursor.execute(command)
                # drop temp table
                cursor.execute(f'DROP TABLE IF EXISTS "{tmp_tbl}"')
                db_conn.commit()