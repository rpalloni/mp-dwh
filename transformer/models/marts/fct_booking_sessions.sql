{{ config(materialized='table') }}

WITH 
booking_requested AS (
    SELECT 
      user_id
      , mentor_id
      , session_id
      , event_date AS booking_requested_date
      , event_time AS booking_requested_ts
    FROM {{ ref('stg_events') }}
    WHERE event_type = 'booking_requested'
),
booking_cancelled AS (
    SELECT 
      user_id
      , mentor_id
      , session_id
      , event_date AS booking_cancelled_date
      , event_time AS booking_cancelled_ts
    FROM {{ ref('stg_events') }}
    WHERE event_type = 'booking_cancelled'
),
booking_confirmed AS (
    SELECT 
      user_id
      , mentor_id
      , session_id
      , event_date AS booking_confirmed_date
      , event_time AS booking_confirmed_ts
    FROM {{ ref('stg_events') }}
    WHERE event_type = 'booking_confirmed'
),
booking_sessions AS (
    SELECT
        r.session_id
        , r.user_id
        , r.mentor_id
        , r.booking_requested_date
        , x.booking_cancelled_date
        , c.booking_confirmed_date
        , CASE WHEN x.booking_cancelled_ts IS NOT NULL THEN TRUE ELSE FALSE END AS is_cancelled
        , ROW_NUMBER() OVER (PARTITION BY r.user_id ORDER BY c.booking_confirmed_date) AS session_sequence
    FROM booking_requested r
    LEFT JOIN booking_cancelled x 
        ON r.session_id = x.session_id
    LEFT JOIN booking_confirmed c 
        ON r.session_id = c.session_id
    ORDER BY r.user_id, session_sequence
)
-- dedupe requested-cancelled and requested-confirmed same day
SELECT *
FROM booking_sessions
QUALIFY  ROW_NUMBER() OVER (
    PARTITION BY session_id, user_id, mentor_id, booking_requested_date, booking_cancelled_date, booking_confirmed_date, is_cancelled
    ORDER BY session_sequence ASC) = 1