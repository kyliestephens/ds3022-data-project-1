import duckdb
import os
import logging

logging.basicConfig(
    level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
    filename='load.log'
)
logger = logging.getLogger(__name__)

def load_parquet_files():

    con = None

    try:
        # Connect to local DuckDB instance
        con = duckdb.connect(database='emissions.duckdb', read_only=False)

        logger.info("Connected to DuckDB instance")
        con.execute("DROP TABLE IF EXISTS yellow_tripdata;")
        con.execute("DROP TABLE IF EXISTS green_tripdata;")
        con.execute("DROP TABLE IF EXISTS vehicle_emissions;")
        logger.info("Dropped table if exists")

        con.execute(f"""
            -- Loading in yellow taxi data
            CREATE TABLE yellow_trip_data AS
            SELECT *
            FROM read_parquet('https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-*.parquet');
        """)
        print("Imported yellow parquet file to DuckDB table")
        logger.info("Yellow data imported")

        con.execute(f"""
            -- Loading in green taxi data
            CREATE TABLE green_trip_data AS
            SELECT *
            FROM read_parquet('https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2024-*.parquet');
        """)
        print("Imported green parquet file to DuckDB table")
        logger.info("Green data imported")

        count = con.execute(f"""
            SELECT COUNT(*) FROM yellow_trip_data;
        """)
        print(f"Number of records in yellow cab table: {count.fetchone()[0]}")

        counts = con.execute(f"""
            SELECT COUNT(*) FROM green_trip_data;
        """)
        print(f"Number of records in green cab table: {counts.fetchone()[0]}")

        con.execute(f"""
            CREATE TABLE vehicle_emissions AS
            SELECT *
            FROM read_csv_auto('vehicle_emissions.csv');
        """)
        print("Imported emissions csv file to DuckDB table")
        logger.info("Vehicle emissions data imported")

        counted = con.execute(f"""
            SELECT COUNT(*) FROM vehicle_emissions;
        """)
        print(f"Number of records in vehicle emissions table: {counted.fetchone()[0]}")

    except Exception as e:
        print(f"An error occurred: {e}")
        logger.error(f"An error occurred: {e}")

if __name__ == "__main__":
    load_parquet_files()