

select
    mortgage_id,
    customer_id,
    property_value,
    loan_amount,
    interest_rate,
    term_years,
    start_date,
    lower(trim(status))                                     as status,
    monthly_payment,
    created_at,

    -- derived
    round(loan_amount / nullif(property_value, 0), 4)      as loan_to_value_ratio,
    start_date + to_years(term_years)  as maturity_date,
    current_timestamp                                       as dbt_updated_at

from iceberg_scan('warehouse/bronze/mortgages')
where mortgage_id  is not null
    and customer_id  is not null
    and loan_amount  > 0
    and property_value > 0
    and status in ('active', 'closed', 'defaulted')
