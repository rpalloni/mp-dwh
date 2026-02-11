{{ config(severity='warn') }}

-- users requesting and cancelling without sessions

SELECT DISTINCT user_id
FROM {{ ref('stg_events') }} e
WHERE NOT EXISTS (
    SELECT 1 FROM {{ ref('stg_events') }} s 
    WHERE s.user_id = e.user_id 
    AND s.event_type = 'session_started'
)