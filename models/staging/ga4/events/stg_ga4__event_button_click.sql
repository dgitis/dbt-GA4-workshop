 with button_click_with_params as (
   select *,
      {{ ga4.unnest_key('event_params', 'button_type', 'string_value') }}
      {% if var("button_click_custom_parameters", "none") != "none" %}
        {{ ga4.stage_custom_parameters( var("button_click_custom_parameters") )}}
      {% endif %}
 from {{ref('stg_ga4__events')}}    
 where event_name = 'button_click'
)

select * from button_click_with_params