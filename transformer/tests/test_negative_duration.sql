{{ config(severity='error') }}

-- mentoring sessions with negative duration

SELECT *
FROM {{ ref('fct_mentoring_sessions') }}
WHERE duration_minutes < 0