{{ config(materialized='table') }}

WITH 
first_sessions AS (
  SELECT 
    user_id
    , mentor_id
    , MIN(booking_requested_date) AS first_session_date
  FROM {{ ref('fct_booking_sessions') }}
  WHERE is_cancelled = FALSE
  GROUP BY 1, 2
),
user_sessions AS (
  SELECT 
    f.user_id
    , f.mentor_id
    , COUNT(s.*) FILTER (
        WHERE s.session_sequence > 1 
        AND s.booking_requested_date <= f.first_session_date + INTERVAL '30 days') > 0 AS rebooked_30d
  FROM first_sessions f
  LEFT JOIN {{ ref('fct_booking_sessions') }} s 
    ON f.user_id = s.user_id
  WHERE is_cancelled = FALSE
  GROUP BY 1, 2
)
SELECT 
  m.tier
  , m.tier_category
  , COUNT(*) AS total_users_w_sessions
  , COUNT(*) FILTER (WHERE rebooked_30d) AS rebooked_count
  , ROUND(COUNT(*) FILTER (WHERE rebooked_30d)::FLOAT / COUNT(*) * 100, 1) AS rebooking_rate_pct
FROM user_sessions u
LEFT JOIN {{ ref('dim_mentors') }} m
ON u.mentor_id = m.mentor_id
GROUP BY 1, 2
ORDER BY rebooking_rate_pct DESC