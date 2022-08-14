"""
@Author: Martin Arroyo
@Email: martinm.arroyo7@gmail.com
@Description: An ETL process to fill the COOP-DA-Database with initial sample
data from the MyAnimeList API. This process is meant to be invoked once,
but it is idempotent and can be run multiple times while still yielding
the same data and structure.
"""
import sys
import logging
import ingestion
import writer
import yaml
import psycopg2
from yaml.loader import SafeLoader
from DBToolBox.DBConnector import DataConnector
from dotenv import dotenv_values


def main():
    """The main/driver method for the ETL process"""
    # Load our configuration parameters and table definitions
    with open(r"tables.yml", "r", encoding="utf-8") as file:
        config = yaml.load(file, Loader=SafeLoader)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s:%(levelname)s:%(message)s",
        handlers=[
            logging.FileHandler("coop-da-etl.log"),
            logging.StreamHandler(sys.stdout),
        ],
    )
    pg_config = dotenv_values(".env")
    logging.info("Beginning ETL Process...")
    logging.info("Cleaning and transforming raw data")
    # Get raw data & clean it
    cleaned_data = ingestion.ingest_data(config)
    logging.info("Data cleaned! Preparing to upload to database...")
    # Load to database
    try:
        dc = DataConnector()
        engine = dc.engine
        connection = writer.pg_connection(
            pg_config["DBC_USER"], pg_config["DBC_PWD"], pg_config["DBC_SERVER"], pg_config["DBC_PORT"], pg_config["DBC_DB"]
        )
        logging.info("Initializing database and creating schemas...")
        with connection.cursor() as cur:
            writer.initialize_db(config, cur)
            # Insert raw data
            logging.info("Inserting data into database...")
            writer.load_data_sync(cleaned_data, config, engine)
            # Add metadata and analyze column statistics
            logging.info("Creating views, analyzing column statistics, and adding metadata...")
            writer.finalize_db(config, cur)
        logging.info("Process complete! Data has been successfully loaded to database!")
    except psycopg2.DatabaseError:
        logging.exception("A database error occurred")
        raise
    finally:
        engine.dispose()  # Close any remaining connections


if __name__ == "__main__":
    main()
