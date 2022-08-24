# https://www.machinelearningplus.com/time-series/arima-model-time-series-forecasting-python/#:~:text=ARIMA%2C%20short%20for%20'AutoRegressive%20Integrated,to%20predict%20the%20future%20values.
# https://cloud.google.com/bigquery-ml/docs/reference/standard-sql/bigqueryml-syntax-create-time-series
#standardSQL
CREATE OR REPLACE MODEL dbt_dgudaitis.ga_pageviews_forecast
OPTIONS
  (model_type = 'ARIMA_PLUS',
   time_series_timestamp_col = 'session_start_date',
   time_series_data_col = 'page_views',
   # time_series_id_col = '',  #segment; can be a list
   horizon = 60,
   auto_arima = TRUE,
   data_frequency = 'AUTO_FREQUENCY',
   holiday_region = 'NA',
   decompose_time_series = TRUE
  ) AS
SELECT
  session_start_date,
  sum(count_page_views) as page_views
FROM
  `daa-workshop.dbt_dgudaitis_analytics.fct_ga4__sessions`
GROUP BY session_start_date


#standardSQL
SELECT
 *
FROM
 ML.FORECAST(MODEL dbt_dgudaitis.ga_pageviews_forecast,
             STRUCT(30 AS horizon, 0.8 AS confidence_level))