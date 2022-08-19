-- cohort_period: "week"|"month" weeks begin on Sundays
-- cohort_table: the table you want to join on user_key against dim_ga4__users to create a cohort
-- cohort_column_date: the datetime column from the cohort_table that you want to create a cohort on
-- first_cohort_date: cohort start including this day
-- num_cohorts: the number of cohorts that you want
-- variables override using dbt_project.yml file 
-- var:
--   ga4:
--     models:
--       marts:
--         library:
--           anl_cohort:
--             first_cohort_date: '20210101'
--             num_cohorts:

-- or override using the vars flag with dbt run
-- dbt compile --select cohort  --vars '{"cohort_period":"week", "cohort_table":"fct_ga4__purchases", "cohort_column_date":"event_date_dt", "first_cohort_date":"2021-01-01","num_cohorts":12}'

with initial as (
    select
        user_key,
        first_seen_dt,
        concat( extract( year from  first_seen_dt )
            {% if var('cohort_period', 'month') == 'month'  %}
                , "-" ,extract( month from  first_seen_dt ) 
            {% elif var('cohort_period', 'month') == 'week'  %}
                , "-" , extract( week from  first_seen_dt ) 
            {% endif %} 
        ) as initial_cohort_{{var('cohort_period', 'month')}}
    from  {{ ref('dim_ga4__users') }}
    where cast( format_date(  '%Y%m%d', first_seen_dt ) as int64 )  >= {{ var( 'first_cohort_date', 20220101 ) }}
),
cohorts as (
    select   -- adding user_key here and removing the sum and changing the THEN to then +=1 would create a more permanent table that you could sum in your final reports
        initial.initial_cohort_{{var('cohort_period', 'month')}}
        {% for num in range( var( 'num_cohorts', 6) ) %}
            ,sum(case when date_diff( {{ var('cohort_column_date','session_start_date')  }},initial.first_seen_dt, {{ var('cohort_period','month') }}  ) = {{ num }} then 1 end) as {{ var('cohort_period','month') }}_{{num}}
        {% endfor %}
    from  {{ ref( var( 'cohort_table', 'fct_ga4__sessions')  ) }}
    right join initial using(user_key)
    group by initial.initial_cohort_{{var('cohort_period', 'month')}}
    order by initial.initial_cohort_{{var('cohort_period', 'month')}}
)
select
    *
from cohorts