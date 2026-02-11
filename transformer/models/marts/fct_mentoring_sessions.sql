{{ config(materialized='table') }}

WITH 
session_starts AS (
    SELECT 
      user_id
      , mentor_id
      , session_id
      , event_date AS session_date
      , event_time AS started_at
    FROM {{ ref('stg_events') }}
    WHERE event_type = 'session_started'
),
session_ends AS (
    SELECT 
      user_id
      , mentor_id
      , session_id
      , event_date AS session_date
      , event_time AS ended_at
    FROM {{ ref('stg_events') }}
    WHERE event_type = 'session_ended'
)

SELECT
    s.session_id
    , s.user_id
    , s.mentor_id
    , s.session_date
    , s.started_at
    , COALESCE(e.ended_at, s.started_at + INTERVAL '30 minutes') AS ended_at
    , CASE WHEN e.ended_at IS NOT NULL THEN TRUE ELSE FALSE END AS has_end_event
    , EXTRACT(EPOCH FROM (COALESCE(e.ended_at, s.started_at + INTERVAL '30 minutes') - s.started_at))/60 AS duration_minutes -- duration (apart fixed) is always 30 mins: standard or sys bug?
    , ROW_NUMBER() OVER (PARTITION BY s.user_id ORDER BY s.started_at) AS session_sequence
FROM session_starts s
LEFT JOIN session_ends e 
    ON s.session_id = e.session_id
ORDER BY s.user_id, session_sequence