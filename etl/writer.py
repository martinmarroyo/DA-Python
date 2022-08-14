"""
@Author: Martin Arroyo
@Email: martinm.arroyo7@gmail.com
@Description: A collection of functions to load data for the
              COOP Data Analytics DataLab
"""
from concurrent.futures import ThreadPoolExecutor
from psycopg2.extensions import AsIs
import sql
import utils
from glob import glob
from sqlalchemy.sql import text
import pandas as pd
import psycopg2
from psycopg2 import OperationalError


def execute_sql(conn, queries: list):
    """Executes the given queries on the provided connection"""
    for query in queries:
        conn.execute(query)


def load_data_sync(data: dict, conf: dict, engine, key:str = "data"):
    """Synchronously loads data into the database based on the provided conf"""
    for df in data:
        data[df].to_sql(
            conf[key][df]["tablename"],
            engine,
            schema=conf[key][df]["schema"],
            if_exists="replace",
            method="multi",
            chunksize=1000,
            index=False,
        )


def load_data_single(data: tuple, engine, conf: dict, key: str = "data"):
    """Helper function to unpack and load data into database"""
    # unpack data
    table, df = data
    tablename = conf[key][table]["tablename"]
    schema = conf[table]["schema"]
    df.to_sql(
        tablename,
        engine,
        schema=schema,
        if_exists="replace",
        method="multi",
        chunksize=1000,
        index=False,
    )


def load_data_concurrent(conf: dict, data: dict, engine):
    """Concurrently loads data into the database based on provided conf"""
    with ThreadPoolExecutor(max_workers=5) as executor:
        executor.map(lambda p: load_data_single(p, engine, conf), data.items())


def create_schemas(conf:dict, cur) -> None:
    """
    Executes a CREATE SCHEMA statement for 
    each schema in the given configuration using
    the given Database connection. 
    This completely drops any schema with
    the same name and then recreates it.
    Use with caution.
    """
    cmd_template = sql.CREATE_SCHEMA_STUB
    schemas = utils.extract_schema(conf)
    for schema in schemas:
        cur.execute(cmd_template, (AsIs(schema), AsIs(schema),))
    return None


def write_metadata(conf:dict, cur, key:str = "metadata") -> None:
    """Uses the query in the provided metadata"""
    data = conf[key]
    for metadata in data:
        with open(metadata, "r", encoding="utf-8", errors="ignore") as file:
            query = file.read()
            cur.execute(query)


def write_views(conf:dict, cur, key:str = "views") -> None:
    """Writes the views found at the location in the configuration to the database"""
    data = conf[key]
    for view in data:
        with open(view, "r", encoding="utf-8", errors="ignore") as file:
            query = file.read()
            cur.execute(query)


def analyze_column_stats(conf:dict, cur, key:str = "data") -> None:
    """Analyzes the column statistics for all tables in the given configuration"""
    tables = utils.extract_table(conf)
    query = ""
    for table in tables:
        query += f"ANALYZE {table}; "
    cur.execute(text(query))

def initialize_db(conf:dict, cur) -> None:
    """
    Creates the initial database schemas (based on configuration file),
    the date dimension table, and enables the crosstab function
    """
    # Create schema
    create_schemas(conf, cur)
    # Create date dimension
    cur.execute(sql.CREATE_DIM_DAY)
    # Enable crosstab
    cur.execute(sql.ENABLE_CROSSTAB)


def finalize_db(conf:dict, cur) -> None:
    """
    Adds pre-defined column metadata, analyzes our tables,
    and creates views 
    """
    write_views(conf, cur)
    write_metadata(conf, cur)
    analyze_column_stats(conf, cur)
    

def pg_connect(user: str, password: str, host: str, port: int, dbname: str):
    """
    Returns a Connection object for the
    specified server
    """
    try:
        connection = psycopg2.connect(
            user=user, password=password, host=host, port=port, dbname=dbname
        )
        return connection
    except (KeyError, OperationalError):
        print("One of your input parameters is incorrect.")
        print("Please try again.")
        raise


def pg_connection(user: str, password: str, host: str, port: int, dbname: str):
    return pg_connect(user, password, host, port, dbname)