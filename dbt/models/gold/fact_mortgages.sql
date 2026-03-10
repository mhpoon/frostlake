with mortgages as (
    select * from {{ ref('stg_mortgages') }}
),

dim_date as (
    select * from {{ ref('dim_date') }}
)

select
    -- keys
    m.mortgage_id,
    m.customer_id,
    d.date_key                                      as start_date_key,

    -- measures
    m.property_value,
    m.loan_amount,
    m.interest_rate,
    m.monthly_payment,
    m.loan_to_value_ratio,

    -- derived measures
    round(m.loan_amount * (m.interest_rate / 100), 2)   as estimated_annual_interest,
    round(m.monthly_payment * m.term_years * 12, 2)     as total_repayment_amount,
    round(
        (m.monthly_payment * m.term_years * 12) - m.loan_amount, 2
    )                                               as total_interest_paid,

    m.dbt_updated_at

from mortgages m
left join dim_date d on m.start_date = d.date_day