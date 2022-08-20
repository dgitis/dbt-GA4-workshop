#https://www.machinelearningplus.com/time-series/arima-model-time-series-forecasting-python/#:~:text=ARIMA%2C%20short%20for%20'AutoRegressive%20Integrated,to%20predict%20the%20future%20values.
#standardSQL
CREATE OR REPLACE MODEL dbt_dgudaitis.ga_pageviews_forecast
OPTIONS
  (model_type = 'ARIMA_PLUS',
   time_series_timestamp_col = 'session_start_date',
   time_series_data_col = 'page_views',
   auto_arima = TRUE,
   data_frequency = 'AUTO_FREQUENCY',
   decompose_time_series = TRUE
  ) AS
SELECT
  session_start_date,
  sum(count_page_views) as page_views
FROM
  `daa-workshop.dbt_dgudaitis.fct_ga4__sessions`
GROUP BY session_start_date


#standardSQL
SELECT
 *
FROM
 ML.FORECAST(MODEL dbt_dgudaitis.ga_pageviews_forecast,
             STRUCT(30 AS horizon, 0.8 AS confidence_level))