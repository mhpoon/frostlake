select
    customer_id,
    trim(first_name)                    as first_name,
    trim(last_name)                     as last_name,
    lower(trim(email))                     as email,
    date_of_birth,
    trim(trim(address))                 as address,
    trim(trim(city))                    as city,
    upper(trim(state))                     as state,
    trim(zip_code)                         as zip_code,
    created_at,

    -- derived
    date_diff('year', date_of_birth, current_date)  as age,
    current_timestamp                               as dbt_updated_at

from iceberg_scan('warehouse/bronze/customers')
where customer_id is not null
    and email is not null
