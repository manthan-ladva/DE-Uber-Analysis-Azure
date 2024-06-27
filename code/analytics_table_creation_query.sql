IF OBJECT_ID('analytics', 'U') IS NOT NULL 
	DROP TABLE analytics;
	

select u.vendorid,
CONVERT(VARCHAR(23), CAST(dd.tpep_pickup_datetime AS DATETIME), 121) as tpep_pickup_datetime, CONVERT(VARCHAR(23), CAST(dd.tpep_dropoff_datetime AS DATETIME), 121) as tpep_dropoff_datetime,
dpc.passenger_count, 
dt.trip_distance, 
dr.[_name] as rate_code_name,
dp.pickup_latitude, dp.pickup_longitude,
ddl.dropoff_latitude, ddl.dropoff_longitude,
dpt.payment_type,
u.fare_amount, u.extra, u.mta_tax, u.tip_amount, u.tolls_amount, u.improvement_surcharge, u.total_amount
into analytics
from uber_fact_table u
inner join dim_datetimes dd on dd.datetime_id = u.datetime_id 
inner join dim_passenger_counts dpc on dpc.passenger_count_id = u.passenger_count_id 
inner join dim_trip_distances dt on dt.trip_distance_id = u.trip_distance_id 
inner join dim_rate_codes dr on dr.rate_code_id = u.rate_code_id 
inner join dim_pickup_locations dp on dp.pickup_location_id = u.pickup_location_id 
inner join dim_drop_locations ddl on ddl.drop_location_id = u.drop_location_id
inner join dim_payment_types dpt on dpt.payment_type_id = u.payment_type_id;
