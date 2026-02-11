{{ config(severity='error') }}

SELECT *
FROM {{ ref('fct_mentoring_sessions') }}
WHERE session_sequence IS NULL