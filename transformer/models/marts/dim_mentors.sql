{{ config(materialized='table') }}

SELECT 
  mentor_id
  , tier
  , hourly_rate
  , CASE 
      WHEN tier = 'Gold' THEN 'top_tier'
      ELSE 'standard_tier'
  END as tier_category
FROM {{ ref('stg_mentors') }}