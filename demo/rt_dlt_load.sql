-- Databricks notebook source
CREATE TEMPORARY STREAMING LIVE VIEW raw_fpds_json_data AS
SELECT *, 'FPDataSolutions' AS twitter_query FROM cloud_files("dbfs:/mnt/data/twitter_real_time/#FPDataSolutions/data/","json",map("cloudFiles.inferColumnTypes", "true"))

-- COMMAND ----------

CREATE STREAMING LIVE VIEW vw_fpds_json_data AS
SELECT 
  twitter_query,
  author_id,
  conversation_id,
  created_at,
  `id`,
  lang,
  possibly_sensitive,
  public_metrics.impression_count,
  public_metrics.like_count,
  public_metrics.quote_count,
  public_metrics.reply_count,
  public_metrics.retweet_count,
  reply_settings,
  text,
  array_min(edit_history_tweet_ids) as pk_id
FROM STREAM(LIVE.raw_fpds_json_data)

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE fpds_json_data

-- COMMAND ----------

APPLY CHANGES INTO LIVE.fpds_json_data FROM STREAM(LIVE.vw_fpds_json_data)
KEYS(pk_id) SEQUENCE BY `id` 
STORED AS SCD TYPE 1
