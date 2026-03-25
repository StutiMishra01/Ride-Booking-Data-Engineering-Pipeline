CREATE OR REFRESH STREAMING TABLE silver_obt
AS 
    SELECT         
            st_rides.ride_id, st_rides.confirmation_number, st_rides.passenger_id, st_rides.driver_id, st_rides.vehicle_id, st_rides.pickup_location_id, st_rides.dropoff_location_id, st_rides.vehicle_type_id, st_rides.vehicle_make_id, st_rides.payment_method_id, st_rides.ride_status_id, st_rides.pickup_city_id, st_rides.dropoff_city_id, st_rides.cancellation_reason_id, st_rides.passenger_name, st_rides.passenger_email, st_rides.passenger_phone, st_rides.driver_name, st_rides.driver_rating, st_rides.driver_phone, st_rides.driver_license, st_rides.vehicle_model, st_rides.vehicle_color, st_rides.license_plate, st_rides.pickup_address, st_rides.pickup_latitude, st_rides.pickup_longitude, st_rides.dropoff_address, st_rides.dropoff_latitude, st_rides.dropoff_longitude, st_rides.distance_miles, st_rides.duration_minutes, st_rides.booking_timestamp, st_rides.pickup_timestamp, st_rides.dropoff_timestamp, st_rides.base_fare, st_rides.distance_fare, st_rides.time_fare, st_rides.surge_multiplier, st_rides.subtotal, st_rides.tip_amount, st_rides.total_fare, st_rides.rating 
                
                    ,
                
        
            map_vehicle_makes.vehicle_make 
                
                    ,
                
        
            map_vehicle_types.vehicle_type,map_vehicle_types.description,map_vehicle_types.base_rate,map_vehicle_types.per_mile,map_vehicle_types.per_minute 
                
                    ,
                
        
            map_ride_statuses.ride_status 
                
                    ,
                
        
            map_payment_methods.payment_method, map_payment_methods.is_card, map_payment_methods.requires_auth 
                
                    ,
                
        
            map_cities.city as pickup_city, map_cities.state, map_cities.region, map_cities.updated_at as city_updated_at 
                
                    ,
                
        
            map_cancellation_reasons.cancellation_reason 
                
        
    FROM 
        
            
                STREAM (uber.bronze.st_rides) 
                WATERMARK booking_timestamp DELAY OF INTERVAL 3 MINUTES st_rides
            
            
                LEFT JOIN uber.bronze.map_vehicle_makes map_vehicle_makes ON st_rides.vehicle_make_id = map_vehicle_makes.vehicle_make_id
            
        
            
                LEFT JOIN uber.bronze.map_vehicle_types map_vehicle_types ON st_rides.vehicle_type_id = map_vehicle_types.vehicle_type_id
            
        
            
                LEFT JOIN uber.bronze.map_ride_statuses map_ride_statuses ON st_rides.ride_status_id = map_ride_statuses.ride_status_id
            
        
            
                LEFT JOIN uber.bronze.map_payment_methods map_payment_methods ON st_rides.payment_method_id = map_payment_methods.payment_method_id
            
        
            
                LEFT JOIN uber.bronze.map_cities map_cities ON st_rides.pickup_city_id = map_cities.city_id
            
        
            
                LEFT JOIN uber.bronze.map_cancellation_reasons map_cancellation_reasons ON st_rides.cancellation_reason_id = map_cancellation_reasons.cancellation_reason_id
            
        
   