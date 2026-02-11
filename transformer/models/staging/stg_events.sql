{{ config(materialized='table') }}

/* DISCLAIMER
the workaround surrogate key adopted is prone to errors in some edge cases namely:
- "across midnight" sessions, as event_date changes between start and end
- "repeated" sessions,  in case more then one sessions happens the same day
*/

SELECT
    event_id
    , event_type
    , timestamp::TIMESTAMP AS event_time
    , DATE_TRUNC('day', event_time) AS event_date
    , user_id
    , mentor_id
    , md5(concat(event_date, '|', user_id, '|', mentor_id)) AS session_id
FROM {{ source('raw', 'events') }}


/* options:
this can also be materialized as incremental with offset (3/5 days) if the share of late events is minor

this can also be modeled with a table for each event_type if the number of events goes to billions

this can also be modeled with a 12/24 months shifting window if many late events are present and the focus is on more recent data
*/