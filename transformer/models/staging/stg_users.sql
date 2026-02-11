{{ config(materialized='table') }}

SELECT 
  CAST(user_id AS TEXT) AS user_id,
  company_id,
  CAST(signup_date AS DATE) AS signup_date,
  status
FROM {{ source('raw', 'users') }}
-- dedupe users
-- QUALIFY  ROW_NUMBER() OVER (PARTITION BY CAST(user_id AS TEXT) ORDER BY signup_date ASC NULLS LAST) = 1