select
    loan_id,
    loan_type,
    status,
    term_months,
    maturity_date,

    -- categorise term into bands
    case
        when term_months <= 24  then 'short'
        when term_months <= 48  then 'medium'
        else 'long'
    end                                             as term_band,

    -- categorise interest rate into bands
    case
        when interest_rate < 5.0  then 'low'
        when interest_rate < 10.0 then 'medium'
        when interest_rate < 15.0 then 'high'
        else 'very_high'
    end                                             as rate_band,

    -- categorise loan amount into bands
    case
        when amount < 10_000  then 'small'
        when amount < 30_000  then 'medium'
        else 'large'
    end                                             as amount_band,

    dbt_updated_at

from {{ ref('stg_loans') }}