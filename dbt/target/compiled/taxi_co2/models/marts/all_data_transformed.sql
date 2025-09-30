 -- Materialize this model as a table (not a view) for efficiency in downstream queries

SELECT -- Select relevant, cleaned, and engineered columns
    cab_type,
    VendorID,
    pickup_datetime,
    dropoff_datetime,
    passenger_count,
    trip_distance,
    total_amount,
    pickup_year,
    hour_of_day,        
    day_of_week,      
    week_of_year,      
    month_of_year,     
    trip_duration_minutes,
    avg_mph,      
    co2_emissions_kg  

FROM "emissions"."main"."yellow_data" -- reference the cleaned yellow taxi dataset

-- Combine the yellow taxi data with the green taxi data
UNION ALL

SELECT -- Select relevant, cleaned, and engineered columns
    cab_type,
    VendorID,
    pickup_datetime,
    dropoff_datetime,
    passenger_count,
    trip_distance,
    total_amount,
    pickup_year,
    hour_of_day,        
    day_of_week,      
    week_of_year,      
    month_of_year,     
    trip_duration_minutes,
    avg_mph,      
    co2_emissions_kg

FROM "emissions"."main"."green_data" -- reference the cleaned green taxi dataset