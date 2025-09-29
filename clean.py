import duckdb
import logging

logging.basicConfig(
    level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
    filename='clean.log'
)
logger = logging.getLogger(__name__)

def cleaning_trips():

    con = None

    try:
        # Connect to local DuckDB instance
        con = duckdb.connect(database='emissions.duckdb', read_only=False)

        logger.info("Connected to DuckDB instance")

        con.execute("""
            ALTER TABLE yellow_tripdata RENAME COLUMN pickup_datetime TO tpep_pickup_datetime;
        """)
        con.execute("""
            ALTER TABLE yellow_tripdata RENAME COLUMN dropoff_datetime TO tpep_dropoff_datetime;
        """)

        # Standardize Green columns
        con.execute("""
            ALTER TABLE green_tripdata RENAME COLUMN pickup_datetime TO lpep_pickup_datetime;
        """)
        con.execute("""
            ALTER TABLE green_tripdata RENAME COLUMN dropoff_datetime TO lpep_dropoff_datetime;
        """)

        con.execute("DROP TABLE IF EXISTS yellow_tripdata_clean;")
        con.execute("DROP TABLE IF EXISTS green_tripdata_clean;")

        #Removing Duplicates
        con.execute(f"""
            CREATE TABLE yellow_tripdata_clean AS
            SELECT DISTINCT * FROM yellow_tripdata;
        """)
        logger.info("yellow_tripdata: duplicates removed")

        #Removing Duplicates
        con.execute(f"""
            -- Create a new table with unique rows
            CREATE TABLE green_tripdata_clean AS 
            SELECT DISTINCT * FROM green_tripdata;
        """)
        #logger.info("Green duplicates removed")

        #result = con.execute(f"""
            #SELECT strftime('%Y', tpep_pickup_datetime) 
                #AS year, COUNT(*) AS trips
            #FROM yellow_tripdata
            #GROUP BY year
            #ORDER BY year;
        #""").fetchall()

        # Print nicely
        #for year, trips in result:
            #print(f"Year: {year}, Trips: {trips}")

        #Remove Trips with 0 Passengers
        con.execute(f"""
            -- Deleting passengers = 0
            DELETE FROM yellow_tripdata_clean
            WHERE passenger_count = 0;
        """)
        logger.info("Deleting yellow trips with 0 passengers")

        #Remove Trips with 0 Passengers
        con.execute(f"""
            -- Deleting passengers = 0
            DELETE FROM green_tripdata_clean
            WHERE passenger_count = 0;
        """)
        logger.info("Deleting green trips with 0 passengers")

        #Deleting Trips with No Length
        con.execute(f"""
            -- Deleting trips 0 miles in length
            DELETE FROM yellow_tripdata_clean
            WHERE trip_distance <= 0;
        """)
        logger.info("Deleting yellow trips 0 miles in length")

        con.execute(f"""
            -- Deleting trips 0 miles in length
            DELETE FROM green_tripdata_clean
            WHERE trip_distance <= 0;
        """)
        logger.info("Deleting green trips 0 miles in length")

        #Deleting Trips with Long Lengths
        con.execute(f"""
            -- Deleting trips greater than 100 miles in length
            DELETE FROM yellow_tripdata_clean
            WHERE trip_distance > 100;
        """)
        logger.info("Deleting yellow trips greater than 100 miles in length")

        con.execute(f"""
            -- Deleting passengers = 0
            DELETE FROM green_tripdata_clean
            WHERE trip_distance > 100;
        """)
        logger.info("Deleting green trips greater than 100 miles in length")
        
        #Delete trips longer than 24 hours 
        con.execute("""
                DELETE FROM yellow_tripdata_clean
                WHERE tpep_pickup_datetime IS NULL 
                   OR tpep_dropoff_datetime IS NULL
                   OR tpep_pickup_datetime >= tpep_dropoff_datetime
                   OR DATEDIFF('second', tpep_pickup_datetime, tpep_dropoff_datetime) <= 0
                   OR DATEDIFF('second', tpep_pickup_datetime, tpep_dropoff_datetime) > 86400
                   OR EXTRACT(YEAR FROM tpep_pickup_datetime) NOT BETWEEN 2015 AND 2024
                   OR total_amount < 0;
            """)
        
        con.execute("""
                DELETE FROM green_tripdata_clean
                WHERE lpep_pickup_datetime IS NULL 
                   OR lpep_dropoff_datetime IS NULL
                   OR lpep_pickup_datetime >= lpep_dropoff_datetime
                   OR DATEDIFF('second', lpep_pickup_datetime, lpep_dropoff_datetime) <= 0
                   OR DATEDIFF('second', lpep_pickup_datetime, lpep_dropoff_datetime) > 86400
                   OR EXTRACT(YEAR FROM lpep_pickup_datetime) NOT BETWEEN 2015 AND 2024
                   OR total_amount < 0;
            """)

        #Duplicates Check
        yellow_dupe = con.execute("""
            SELECT COUNT(*) - COUNT(DISTINCT COLUMNS(*)) AS duplicate_row_count
            FROM yellow_tripdata_clean;
        """).fetchone()[0]
        logger.info("counting duplicates in yellow")
        print(f"Yellow Dupes: {yellow_dupe}")

        green_dupe = con.execute("""
            SELECT COUNT(*) - COUNT(DISTINCT COLUMNS(*)) AS duplicate_row_count
            FROM green_tripdata_clean;
        """).fetchone()[0]
        logger.info("counting duplicates in green")
        print(f"Green Dupes: {green_dupe}")

        #Zero Passenger Check
        yellow_passenger = con.execute("SELECT COUNT(*) FROM yellow_tripdata_clean WHERE passenger_count = 0;").fetchone()[0]
        green_passenger = con.execute("SELECT COUNT(*) FROM green_tripdata_clean WHERE passenger_count = 0;").fetchone()[0]
        print(f"Yellow trips with 0 passengers: {yellow_passenger}")
        print(f"Green trips with 0 passengers: {green_passenger}")
        logger.info("No Passenger Check")

        #Invalid Trip Length Check
        yellow_bad_length = con.execute("SELECT COUNT(*) FROM yellow_tripdata_clean WHERE trip_distance <= 0 OR trip_distance > 100;").fetchone()[0]
        green_bad_length = con.execute("SELECT COUNT(*) FROM green_tripdata_clean WHERE trip_distance <= 0 OR trip_distance > 100;").fetchone()[0]
        print(f"Yellow trips with invalid distances: {yellow_bad_length}")
        print(f"Green trips with invalid distances: {green_bad_length}")
        logger.info("Trip Length Check")

        #24 hr time Check
        yellow_day = con.execute("""
            SELECT COUNT(*) FROM yellow_tripdata_clean
            WHERE DATEDIFF('second', tpep_pickup_datetime, tpep_dropoff_datetime) > 86400;
        """).fetchone()[0]

        green_day = con.execute("""
            SELECT COUNT(*) FROM green_tripdata_clean
            WHERE DATEDIFF('second', lpep_pickup_datetime, lpep_dropoff_datetime) > 86400;
        """).fetchone()[0]
        print(f"Yellow trips longer than 24 hr: {yellow_day}")
        print(f"Green trips longer than 24 hr: {green_day}")
        logger.info("24 Hour Check")

    except Exception as e:
        print(f"An error occurred: {e}")
        logger.error(f"An error occurred: {e}")

if __name__ == "__main__":
    cleaning_trips()
