{{ config(severity='warn') }}

-- users in db but without events

SELECT *
FROM {{ ref('stg_users') }}  u
LEFT JOIN {{ ref('stg_events') }} e
ON e.user_id = u.user_id
WHERE e.user_id IS NULL
AND u.status = 'active'