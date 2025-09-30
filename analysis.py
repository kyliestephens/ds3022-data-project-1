import duckdb
import matplotlib.pyplot as plt
import logging
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='analysis.log'
)
logger = logging.getLogger(__name__)

def analyzing_cleandata():
    try:
        con = duckdb.connect(database='emissions.duckdb', read_only=False)
        logger.info("Connected to DuckDB")

        logger.info("1. SINGLE LARGEST CARBON TRIPS (Yellow & Green)")
        logger.info("-" * 30)

        cab_types = ['yellow', 'green']

        for cab in cab_types:
            largest_trip = con.execute(f"""
                SELECT 
                    cab_type,
                    VendorID,
                    pickup_datetime,
                    dropoff_datetime,
                    trip_distance,
                    total_amount,
                    co2_emissions_kg
                FROM all_data_transformed
                WHERE cab_type = '{cab}'
                ORDER BY co2_emissions_kg DESC
                LIMIT 1;
            """).fetchdf()

            logger.info(f"Largest {cab.upper()} trip:\n{largest_trip.to_string(index=False)}")

        logger.info("2. HOURLY CO2 PATTERNS (1-24 hours by cab type)")
        logger.info("-" * 30)

        for cab in cab_types:
            logger.info(f"--- {cab.upper()} Taxi ---")

            hourly_analysis = con.execute(f"""
                SELECT
                hour_of_day + 1 AS hour_of_day,  -- Shift 0-23 to 1-24
                ROUND(SUM(co2_emissions_kg), 2) AS total_co2_kg,
                COUNT(*) AS trips,
                ROUND(AVG(co2_emissions_kg), 4) AS avg_co2_per_trip
                FROM all_data_transformed
                WHERE cab_type = '{cab}'
                GROUP BY hour_of_day
                ORDER BY hour_of_day;
            """).fetchall()

    # Find lightest and heaviest average CO2 hours
            min_hour = min(hourly_analysis, key=lambda x: x[3])
            max_hour = max(hourly_analysis, key=lambda x: x[3])

            for hour, co2_kg, trips, avg_co2 in hourly_analysis:
                co2_tons = co2_kg / 1000
                time_str = f"{hour:02d}:00"
                logger.info(f"   {time_str}: {trips:>12,} trips, {co2_tons:>10,.1f} metric tons")

            logger.info(f"Most carbon-heavy hour: {max_hour[0]:02d}:00 with avg {max_hour[3]:.4f} kg CO2")
            logger.info(f"Least carbon-heavy hour: {min_hour[0]:02d}:00 with avg {min_hour[3]:.4f} kg CO2")
    
        logger.info("3. WEEKLY CO2 PATTERNS (Sun-Sat by cab type)")
        logger.info("-" * 30)

        days = ['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday']

        for cab in cab_types:
            logger.info(f"--- {cab.upper()} Taxi ---")

            weekly_analysis = con.execute(f"""
                SELECT
                day_of_week,
                ROUND(SUM(co2_emissions_kg), 2) AS total_co2_kg,
                COUNT(*) AS trips,
                ROUND(AVG(co2_emissions_kg), 4) AS avg_co2_per_trip
                FROM all_data_transformed
                WHERE cab_type = '{cab}'
                GROUP BY day_of_week
                ORDER BY day_of_week;
            """).fetchall()

    # Find lightest and heaviest average CO2 days
            min_day = min(weekly_analysis, key=lambda x: x[3])
            max_day = max(weekly_analysis, key=lambda x: x[3])

            for dow, co2_kg, trips, avg_co2 in weekly_analysis:
                co2_tons = co2_kg / 1000
                day_name = days[dow]
                logger.info(f"   {day_name}: {trips:>12,} trips, {co2_tons:>10,.1f} metric tons")

            logger.info(f"Most carbon-heavy day: {days[max_day[0]]} with avg {max_day[3]:.4f} kg CO2")
            logger.info(f"Least carbon-heavy day: {days[min_day[0]]} with avg {min_day[3]:.4f} kg CO2")

            logger.info("4. WEEKLY CO2 PATTERNS (Weeks 1-52 by cab type)")
        logger.info("-" * 30)

        for cab in cab_types:
            logger.info(f"--- {cab.upper()} Taxi ---")

            weekly_analysis = con.execute(f"""
                SELECT
                week_of_year,
                ROUND(SUM(co2_emissions_kg), 2) AS total_co2_kg,
                COUNT(*) AS trips,
                ROUND(AVG(co2_emissions_kg), 4) AS avg_co2_per_trip
                FROM all_data_transformed
                WHERE cab_type = '{cab}'
                GROUP BY week_of_year
                ORDER BY week_of_year;
            """).fetchall()

    # Find the lightest and heaviest average CO2 weeks
            min_week = min(weekly_analysis, key=lambda x: x[3])
            max_week = max(weekly_analysis, key=lambda x: x[3])

            for week, co2_kg, trips, avg_co2 in weekly_analysis:
                co2_tons = co2_kg / 1000
                logger.info(f"   Week {week:02d}: {trips:>12,} trips, {co2_tons:>10,.1f} metric tons")

            logger.info(f"Most carbon-heavy week: Week {max_week[0]} with avg {max_week[3]:.4f} kg CO2")
            logger.info(f"Least carbon-heavy week: Week {min_week[0]} with avg {min_week[3]:.4f} kg CO2")

        logger.info("5. MONTHLY CO2 PATTERNS (Jan-Dec by cab type)")
        logger.info("-" * 30)

        months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
          'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']

        for cab in cab_types:
            logger.info(f"--- {cab.upper()} Taxi ---")

            monthly_analysis = con.execute(f"""
                SELECT
                month_of_year,
                ROUND(SUM(co2_emissions_kg), 2) AS total_co2_kg,
                COUNT(*) AS trips,
                ROUND(AVG(co2_emissions_kg), 4) AS avg_co2_per_trip
                FROM all_data_transformed
                WHERE cab_type = '{cab}'
                GROUP BY month_of_year
                ORDER BY month_of_year;
            """).fetchall()

    # Find the lightest and heaviest average CO2 months
            min_month = min(monthly_analysis, key=lambda x: x[3])
            max_month = max(monthly_analysis, key=lambda x: x[3])

            for month, co2_kg, trips, avg_co2 in monthly_analysis:
                co2_tons = co2_kg / 1000
                month_name = months[month - 1]  # Convert 1-12 to Jan-Dec
                logger.info(f"   {month_name}: {trips:>12,} trips, {co2_tons:>10,.1f} metric tons")

            logger.info(f"Most carbon-heavy month: {months[max_month[0]-1]} with avg {max_month[3]:.4f} kg CO2")
            logger.info(f"Least carbon-heavy month: {months[min_month[0]-1]} with avg {min_month[3]:.4f} kg CO2")

        return con

    except Exception as e:
        error_msg = f"Analysis failed: {e}"
        logger.error(error_msg)
        logger.error(error_msg)
        return False

def plot_monthly_co2(con):
    try:
        cab_types = ['yellow', 'green']
        monthly_data = {}

        # Fetch monthly CO2 totals for each cab type
        for cab in cab_types:
            monthly_analysis = con.execute(f"""
                SELECT
                    CAST(month_of_year AS INTEGER) AS month_num,
                    SUM(co2_emissions_kg) AS total_co2_kg
                FROM all_data_transformed
                WHERE cab_type = '{cab}'
                GROUP BY month_num
                ORDER BY month_num;
            """).fetchall()

            if not monthly_analysis:
                # Default to zero if no data found
                monthly_data[cab] = [0]*12
            else:
                # Fill in CO2 totals per month (metric tons)
                co2_by_month = [0]*12
                for row in monthly_analysis:
                    month_idx = row[0] - 1  # convert 1–12 to 0–11 index
                    co2_by_month[month_idx] = row[1] / 1000  # kg → metric tons
                monthly_data[cab] = co2_by_month

        # Plotting
        plt.figure(figsize=(12, 6))
        x = range(1, 13)  # 1–12 for months
        plt.plot(x, monthly_data['yellow'], marker='o', linewidth=2, color='gold', label='YELLOW')
        plt.plot(x, monthly_data['green'], marker='s', linewidth=2, color='green', label='GREEN')

        plt.xticks(x, ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'])
        plt.title('Monthly CO₂ Emissions by Taxi Type', fontsize=16, fontweight='bold')
        plt.xlabel('Month', fontsize=12)
        plt.ylabel('CO₂ Emissions (Metric Tons)', fontsize=12)
        plt.grid(alpha=0.3)
        plt.legend()
        plt.tight_layout()
        plt.savefig('monthly_co2_yellow_green.png', dpi=300)
        plt.show()

        return con

    except Exception as e:
        logger.error(f"Monthly CO2 plotting failed: {e}")
        print(f"Error plotting monthly CO2: {e}")

# -----------------------------
if __name__ == "__main__":
    con = analyzing_cleandata()
    if con:
        plot_monthly_co2(con)
