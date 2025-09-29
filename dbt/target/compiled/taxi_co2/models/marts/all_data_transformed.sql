

SELECT 
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

FROM "emissions"."main"."yellow_data"

UNION ALL

SELECT 
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

FROM "emissions"."main"."green_data"