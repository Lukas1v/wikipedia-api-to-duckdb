{{ config(materialized='view') }}
SELECT *
FROM {{ ref('gold_most_active_timeslots') }}
ORDER BY change_count DESC
LIMIT 1