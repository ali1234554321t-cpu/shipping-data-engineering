CREATE OR REPLACE VIEW shipping_indicators AS
SELECT
    source_id,
    observation_date,
    indicator,
    value,
    units,
    note,
    CASE
        WHEN indicator IN (
            'Capacity of Containerships Calling at U.S. Ports (in TEUs)',
            'Containerized Exports at U.S. Ports in TEUs',
            'Containerized Imports at U.S. Ports in TEUs'
        ) THEN 'Container Volume'
        WHEN indicator IN (
            'Number of Containerships Awaiting Berths at all U.S. Ports',
            'Number of Containerships Anchored off U.S. Ports'
        ) THEN 'Port Congestion'
        ELSE 'Other Logistics'
    END AS indicator_category
FROM port_capacity;
