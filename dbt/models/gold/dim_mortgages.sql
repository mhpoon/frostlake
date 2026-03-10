select
    mortgage_id,
    status,
    term_years,
    maturity_date,

    -- categorise loan-to-value ratio into bands
    case
        when loan_to_value_ratio <= 0.60 then 'low'
        when loan_to_value_ratio <= 0.80 then 'medium'
        when loan_to_value_ratio <= 0.95 then 'high'
        else 'very_high'
    end                                             as ltv_band,

    -- categorise interest rate into bands
    case
        when interest_rate < 3.0  then 'very_low'
        when interest_rate < 5.0  then 'low'
        when interest_rate < 7.0  then 'medium'
        else 'high'
    end                                             as rate_band,

    -- categorise term
    case
        when term_years <= 15 then 'short'
        when term_years <= 20 then 'medium'
        else 'long'
    end                                             as term_band,

    dbt_updated_at

from {{ ref('stg_mortgages') }}