select
    customer_id,
    first_name,
    last_name,
    first_name || ' ' || last_name  as full_name,
    email,
    date_of_birth,
    age,
    address,
    city,
    state,
    zip_code,
    created_at,
    dbt_updated_at

from {{ ref('stg_customers') }}