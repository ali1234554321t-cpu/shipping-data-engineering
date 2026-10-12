{{ config(materialized='view') }}

SELECT
id AS event_id,
shipment_id,
TRIM(status) AS status,
TRIM(location) AS location,
event_time
FROM {{ source('shipping_source', 'shipment_events') }}
