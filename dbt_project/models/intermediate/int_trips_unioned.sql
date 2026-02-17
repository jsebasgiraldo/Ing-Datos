with green_tripdata as (
    select * from {{ref('stg_green_tripdata')}}
),
yellow_tripdata as (
    select * from {{ref('stg_yellow_tripdata')}}
),

trips_unioned as (
    select * from green_tripdata
    union all
    select * from yellow_tripdata
)

select
    {{ dbt_utils.generate_surrogate_key([
        'vendor_id',
        'pickup_datetime',
        'dropoff_datetime',
        'pickup_location_id',
        'dropoff_location_id'
    ]) }} as trip_id,
    *
from trips_unioned