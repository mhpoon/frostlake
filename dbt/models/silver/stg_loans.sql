
select
    loan_id,
    customer_id,
    lower(trim(loan_type))                                  as loan_type,
    amount,
    interest_rate,
    term_months,
    issue_date,
    lower(trim(status))                                     as status,
    monthly_payment,
    created_at,

    -- derived
    issue_date + to_months(term_months)    as maturity_date,
    round(amount * (interest_rate / 100), 2)               as estimated_annual_interest,
    current_timestamp                                       as dbt_updated_at

from iceberg_scan('warehouse/bronze/loans')
where loan_id     is not null
    and customer_id is not null
    and amount      > 0
    and loan_type   in ('personal', 'auto', 'student')
    and status      in ('active', 'closed', 'defaulted')
