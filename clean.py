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

        #Standardize Yellow columns
        con.execute("""
            #ALTER TABLE yellow_tripdata RENAME COLUMN pickup_datetime TO tpep_pickup_datetime;
        #""")
        con.execute("""
            ALTER TABLE yellow_tripdata RENAME COLUMN dropoff_datetime TO tpep_dropoff_datetime;
        """)
        logger.info("Yellow columns standardized")

        # Standardize Green columns
        con.execute("""
            ALTER TABLE green_tripdata RENAME COLUMN pickup_datetime TO lpep_pickup_datetime;
        """)
        con.execute("""
            ALTER TABLE green_tripdata RENAME COLUMN dropoff_datetime TO lpep_dropoff_datetime;
        """)
        logger.info("Green columns standardized")

        con.execute("DROP TABLE IF EXISTS yellow_tripdata_clean;")
        con.execute("DROP TABLE IF EXISTS green_tripdata_clean;")
        logger.info("Existing tables dropped.")

        #Initial Data counts
        try:
            yellow_preclean = con.execute("SELECT COUNT(*) FROM yellow_tripdata").fetchone()[0]
            logger.info(f" Yellow taxis: {yellow_preclean:,} trips")
        except:
            yellow_preclean = 0
            logger.info(" Yellow taxis: No data found")
            
        try:
            green_preclean = con.execute("SELECT COUNT(*) FROM green_tripdata").fetchone()[0] 
            logger.info(f"   Green taxis:  {green_preclean:,} trips")
        except:
            green_preclean = 0
            logger.info("   Green taxis: No data found")
        
        total_pc = yellow_preclean + green_preclean
        logger.info(f"   Total before cleaning: {total_pc:,} trips")
        
        if total_pc== 0:
            logger.error("No data to clean! Run load.py first.")
            return None
            
        logger.info(f"Initial counts - Yellow: {yellow_preclean:,}, Green: {green_preclean:,}")


        #Removing Duplicates
        con.execute(f"""
            CREATE TABLE yellow_tripdata_clean AS
            SELECT DISTINCT * FROM yellow_tripdata;
        """)
        logger.info("Yellow duplicates removed")

        #Removing Duplicates
        con.execute(f"""
            -- Create a new table with unique rows
            CREATE TABLE green_tripdata_clean AS 
            SELECT DISTINCT * FROM green_tripdata;
        """)
        logger.info("Green duplicates removed")

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
        logger.info("Performing remaining cleaning activities on yellow data.")
        
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
        logger.info("Performing remaining cleaning activities on green data.")


        #Zero Passenger Check
        yellow_passenger = con.execute("SELECT COUNT(*) FROM yellow_tripdata_clean WHERE passenger_count = 0;").fetchone()[0]
        green_passenger = con.execute("SELECT COUNT(*) FROM green_tripdata_clean WHERE passenger_count = 0;").fetchone()[0]
        print(f"Yellow trips with 0 passengers: {yellow_passenger}")
        print(f"Green trips with 0 passengers: {green_passenger}")
        logger.info(f"Yellow trips with 0 passengers: {yellow_passenger}")
        logger.info(f"Green trips with 0 passengers: {green_passenger}")

        #Invalid Trip Length Check
        yellow_bad_length = con.execute("SELECT COUNT(*) FROM yellow_tripdata_clean WHERE trip_distance <= 0 OR trip_distance > 100;").fetchone()[0]
        green_bad_length = con.execute("SELECT COUNT(*) FROM green_tripdata_clean WHERE trip_distance <= 0 OR trip_distance > 100;").fetchone()[0]
        print(f"Yellow trips with invalid distances: {yellow_bad_length}")
        print(f"Green trips with invalid distances: {green_bad_length}")
        logger.info(f"Yellow trips with invalid distances: {yellow_bad_length}")
        logger.info(f"Green trips with invalid distances: {green_bad_length}")

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
        logger.info(f"Yellow trips longer than 24 hr: {yellow_day}")
        logger.info(f"Green trips longer than 24 hr: {green_day}")

        #Yellow Post Cleaning Numbers Summary
        yellow_postclean = con.execute("SELECT COUNT(*) FROM yellow_tripdata_clean").fetchone()[0]
        yellow_removed = yellow_preclean - yellow_postclean
        yellow_pct = (yellow_removed / yellow_preclean * 100) if yellow_preclean > 0 else 0
            
        logger.info(f"Yellow taxi results: {yellow_postclean:,} trips remaining ({yellow_removed:,} removed)")
            
        #replace original table
        logger.info("Step 4: Replacing original yellow data table with cleaned version")
        con.execute("DROP TABLE yellow_tripdata; ALTER TABLE yellow_tripdata_clean RENAME TO yellow_tripdata;")
            
        logger.info("Yellow cleaning complete:")
        logger.info(f"      Before: {yellow_preclean:,} trips")
        logger.info(f"      After:  {yellow_postclean:,} trips") 
        logger.info(f"      Removed: {yellow_removed:,} trips ({yellow_pct:.1f}%)")
            
        logger.info(f"Yellow cleaned: {yellow_preclean:,} → {yellow_postclean:,} (removed {yellow_removed:,}, {yellow_pct:.1f}%)")
        print(f"Yellow cleaned: {yellow_preclean:,} → {yellow_postclean:,} (removed {yellow_removed:,}, {yellow_pct:.1f}%)")

        #Green Post Cleaning Numbers Summary
        green_postclean = con.execute("SELECT COUNT(*) FROM green_tripdata_clean").fetchone()[0]
        green_removed = green_preclean - green_postclean
        green_pct = (green_removed / green_preclean * 100) if green_preclean > 0 else 0
            
        logger.info(f"Green taxi results: {green_postclean:,} trips remaining ({green_removed:,} removed)")
            
        #replace original table
        logger.info("Step 4: Replacing original green data table with cleaned version")
        con.execute("DROP TABLE green_tripdata; ALTER TABLE green_tripdata_clean RENAME TO green_tripdata;")
            
        logger.info("Green cleaning complete:")
        logger.info(f"      Before: {green_preclean:,} trips")
        logger.info(f"      After:  {green_postclean:,} trips") 
        logger.info(f"      Removed: {green_removed:,} trips ({green_pct:.1f}%)")
            
        logger.info(f"Green cleaned: {green_preclean:,} → {green_postclean:,} (removed {green_removed:,}, {green_pct:.1f}%)")
        print(f"Green cleaned: {green_preclean:,} → {green_postclean:,} (removed {green_removed:,}, {green_pct:.1f}%)")

    except Exception as e:
        print(f"An error occurred: {e}")
        logger.error(f"An error occurred: {e}")

if __name__ == "__main__":
    cleaning_trips()
