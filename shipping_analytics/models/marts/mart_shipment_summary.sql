

{{ config(materialized='table') }}

SELECT
    summary_type,
    status,
    location,
    shipment_count
FROM {{ source('shipping_source', 'gold_shipment_summary') }}
WHERE summary_type IS NOT NULL
  AND status IS NOT NULL
  AND shipment_count IS NOT NULL
