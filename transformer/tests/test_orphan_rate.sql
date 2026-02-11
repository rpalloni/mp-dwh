{{ config(severity='error') }}

-- no matching user in events (only user_id: 9999)

WITH orphans AS (
  SELECT 
    COUNT(*) AS total_count
    , SUM(CASE 
            WHEN u.user_id IS null 
            AND e.event_type in ('booking_requested', 'booking_confirmed') 
        THEN 1 ELSE 0 END) AS orphan_count
  FROM {{ ref('stg_events') }}  e
  LEFT JOIN {{ ref('stg_users') }} u 
  ON e.user_id = u.user_id
)
SELECT *
FROM orphans
WHERE (orphan_count / total_count) * 100 > 5.0