{{ config(materialized='table') }}

SELECT 
  user_id
  , company_id
  , signup_date
  , status
  , CASE WHEN status = 'active' THEN TRUE ELSE FALSE END as is_active
FROM {{ ref('stg_users') }}