{{ config(materialized='table') }}

SELECT 
  mentor_id,
  tier,
  CAST(hourly_rate AS DECIMAL(10,2)) AS hourly_rate
FROM {{ source('raw', 'mentors') }}