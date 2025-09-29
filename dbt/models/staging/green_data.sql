SELECT 
    'green' as cab_type,
    VendorID,
    lpep_pickup_datetime as pickup_datetime,
    lpep_dropoff_datetime as dropoff_datetime,
    passenger_count,
    trip_distance,
    total_amount,
    
    -- Extract time features
    EXTRACT(hour FROM lpep_pickup_datetime) as hour_of_day,
    EXTRACT(dow FROM lpep_pickup_datetime) as day_of_week,
    EXTRACT(week FROM lpep_pickup_datetime) as week_of_year,
    EXTRACT(month FROM lpep_pickup_datetime) as month_of_year,
    EXTRACT(year FROM lpep_pickup_datetime) as pickup_year,
    
    -- Calculate trip duration in minutes
    ROUND(EXTRACT(epoch FROM (lpep_dropoff_datetime - lpep_pickup_datetime)) / 60.0, 2) as trip_duration_minutes,
    
    -- Calculate average speed in mph
    CASE 
      WHEN EXTRACT(epoch FROM (dropoff_datetime - pickup_datetime)) > 0
      THEN ROUND(trip_distance / (EXTRACT(epoch FROM (dropoff_datetime - pickup_datetime)) / 3600.0), 2)
      ELSE 0
    END as avg_mph,

    -- Calculate CO2 emissions (kg CO2 per mile for taxi = 0.404)
    ROUND(trip_distance * (
      SELECT co2_grams_per_mile / 1000.0 
      FROM {{ source('raw_data', 'vehicle_emissions') }} 
      WHERE vehicle_type = 'green_taxi'
    ), 4) as co2_emissions_kg


FROM {{ source('raw_data', 'green_tripdata') }}
WHERE lpep_pickup_datetime IS NOT NULL
  AND lpep_dropoff_datetime IS NOT NULL
  AND trip_distance > 0
  AND total_amount > 0