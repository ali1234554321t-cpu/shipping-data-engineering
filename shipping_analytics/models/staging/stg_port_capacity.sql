{{ config(materialized='view') }}

SELECT
id AS port_capacity_id,
source_id,
observation_date,
year,
TRIM(indicator) AS indicator,
value,
TRIM(units) AS units,
note,
source
FROM {{ source('shipping_source', 'port_capacity') }}
