{{ config(materialized='view') }}
WITH time_slots AS (
    -- Creating 30-minute slots that shift every 15 minutes
    SELECT
        -- ROUND to the nearest 15-minute interval
        DATE_TRUNC('minute', timestamp) AS original_timestamp,
        -- Generate the start of the 30-minute window by truncating to 15-minute intervals
        -- This will give you a sliding window based on the timestamp
        DATE_TRUNC('minute', timestamp) - INTERVAL '1 minute' * (EXTRACT(minute FROM timestamp) % 15) AS shifted_slot_start,
        -- Generate the end of the 30-minute window
        DATE_TRUNC('minute', timestamp) - INTERVAL '1 minute' * (EXTRACT(minute FROM timestamp) % 15) + INTERVAL '30 minute' AS shifted_slot_end
    FROM 
        {{ ref('silver_recent_changes') }}  -- or your table name where timestamp is available
),
change_counts AS (
    -- Counting the changes in each 30-minute slot
    SELECT
        shifted_slot_start,
        shifted_slot_end,
        COUNT(*) AS change_count
    FROM time_slots
    GROUP BY shifted_slot_start, shifted_slot_end
),
-- Identifying the most active 30-minute window per shift
ranked_changes AS (
    SELECT 
        shifted_slot_start,
        shifted_slot_end,
        change_count,
        ROW_NUMBER() OVER (PARTITION BY shifted_slot_start ORDER BY change_count DESC) AS rank
    FROM change_counts
)
-- Selecting the top ranked (most changes) slot for each 15-minute shift
SELECT 
    shifted_slot_start,
    shifted_slot_end,
    change_count
FROM ranked_changes
WHERE rank = 1
ORDER BY shifted_slot_start
