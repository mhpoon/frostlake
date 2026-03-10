select
    -- keys
    l.loan_id,
    l.customer_id,
    d.date_key                                      as issue_date_key,

    -- measures
    l.amount,
    l.interest_rate,
    l.term_months,
    l.monthly_payment,

    -- derived measures
    l.estimated_annual_interest,
    round(l.monthly_payment * l.term_months, 2)     as total_repayment_amount,
    round(
        (l.monthly_payment * l.term_months) - l.amount, 2
    )                                               as total_interest_paid,

    l.dbt_updated_at

from {{ ref('stg_loans') }} l
left join {{ ref('dim_date') }} d on l.issue_date = d.date_day