

{{
    config(
        materialization = 'table'
    )
}}

select
    session_key,
    user_key,
    pagepath_level_1,
    page_location,
    page_referrer,
    page_title,
    page_query_string,
    exits,
    value,
    entrances,
    case when pagepath_level_1 = '/job' then 1 else 0 end as is_jobs_page
from {{ ref('stg_ga4__event_page_view') }}