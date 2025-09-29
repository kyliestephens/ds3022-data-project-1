import duckdb
import logging
import time 

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='load.log'
)
logger = logging.getLogger(__name__)

def create_urls(start_year=2024, end_year=2024):
    base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data"
    urls = {'yellow': [], 'green': []}

    for year in range(start_year, end_year + 1):
        for month in range(1, 13):
            month_str = f"{month:02d}"
            urls['yellow'].append(f"{base_url}/yellow_tripdata_{year}-{month_str}.parquet")
            urls['green'].append(f"{base_url}/green_tripdata_{year}-{month_str}.parquet")

    logger.info(
        f"Generated {len(urls['yellow'])} yellow and {len(urls['green'])} green cab URLs "
        f"for {start_year}-{end_year}"
    )
    return urls


def load_parquet_files(start_year=2024, end_year=2024):
    urls = create_urls(start_year, end_year)
    con = duckdb.connect(database='emissions.duckdb', read_only=False)
    logger.info("Connected to DuckDB")

    # Drop existing tables if they exist
    con.execute("DROP TABLE IF EXISTS yellow_tripdata;")
    con.execute("DROP TABLE IF EXISTS green_tripdata;")
    con.execute("DROP TABLE IF EXISTS vehicle_emissions;")
    logger.info("Dropped old tables if existed")

    # === Load YELLOW cabs ===
    logger.info("Loading YELLOW cab data...")
    first = True
    yellow_rows = 0
    for url in urls['yellow']:
        ym = url.split("_")[-1].replace(".parquet", "")
        try:
            if first:
                con.execute(f"""
                    CREATE TABLE yellow_tripdata AS
                    SELECT 
                        VendorID,
                        tpep_pickup_datetime AS pickup_datetime,
                        tpep_dropoff_datetime AS dropoff_datetime,
                        passenger_count,
                        trip_distance,
                        total_amount
                    FROM read_parquet('{url}');
                """)
                first = False
            else:
                con.execute(f"""
                    INSERT INTO yellow_tripdata
                    SELECT 
                        VendorID,
                        tpep_pickup_datetime AS pickup_datetime,
                        tpep_dropoff_datetime AS dropoff_datetime,
                        passenger_count,
                        trip_distance,
                        total_amount
                    FROM read_parquet('{url}');
                """)
            yellow_rows = con.execute("SELECT COUNT(*) FROM yellow_tripdata").fetchone()[0]
            logger.info(f"{ym}: Loaded yellow cab data, total rows: {yellow_rows:,}")
            time.sleep(30)
        except Exception as e:
            logger.warning(f"Skipped yellow {ym} due to error: {e}")
            continue

    # === Load GREEN cabs ===
    logger.info("Loading GREEN cab data...")
    first = True
    green_rows = 0
    for url in urls['green']:
        ym = url.split("_")[-1].replace(".parquet", "")
        try:
            if first:
                con.execute(f"""
                    CREATE TABLE green_tripdata AS
                    SELECT 
                        VendorID,
                        lpep_pickup_datetime AS pickup_datetime,
                        lpep_dropoff_datetime AS dropoff_datetime,
                        passenger_count,
                        trip_distance,
                        total_amount
                    FROM read_parquet('{url}');
                """)
                first = False
            else:
                con.execute(f"""
                    INSERT INTO green_tripdata
                    SELECT 
                        VendorID,
                        lpep_pickup_datetime AS pickup_datetime,
                        lpep_dropoff_datetime AS dropoff_datetime,
                        passenger_count,
                        trip_distance,
                        total_amount
                    FROM read_parquet('{url}');
                """)
            green_rows = con.execute("SELECT COUNT(*) FROM green_tripdata").fetchone()[0]
            logger.info(f"{ym}: Loaded green cab data, total rows: {green_rows:,}")
            time.sleep(60)
        except Exception as e:
            logger.warning(f"Skipped green {ym} due to error: {e}")
            continue

    logger.info(f"Finished loading YELLOW: {yellow_rows:,} rows, GREEN: {green_rows:,} rows")

    # === Load vehicle emissions data ===
    try:
        con.execute(f"""
            CREATE TABLE vehicle_emissions AS
            SELECT *
            FROM read_csv_auto('data/vehicle_emissions.csv');
        """)
        logger.info("Loaded vehicle emissions table")
    except Exception as e:
        logger.error(f"Failed to load vehicle emissions data: {e}")

    return con


if __name__ == "__main__":
    con = load_parquet_files(2024, 2024)
    if con:
        logger.info("All data loaded successfully")
        con.close()

