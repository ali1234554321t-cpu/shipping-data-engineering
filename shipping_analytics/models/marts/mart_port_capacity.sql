
{{ config(materialized='table') }}

SELECT
    year,
    month,
    summary_type,
    indicator_category,
    indicator,
    units,
    measure1,
    measure1_description,
    measure2,
    measure2_description,
    record_count,
    observation_days,
    min_value,
    max_value,
    avg_value
FROM {{ source('shipping_source', 'gold_port_capacity_summary') }}
WHERE record_count IS NOT NULL
  AND observation_days IS NOT NULL
  AND indicator IS NOT NULL
  AND summary_type IN ('monthly', 'annual')
