-- Databricks notebook source
CREATE OR REFRESH STREAMING LIVE TABLE fpds_prediction COMMENT "Tweets with class label and score generated by the models. General labels come from transformers classifier, while the other come from the fine-tuned openai classifier" AS
SELECT
  twitter_query,
  author_id,
  conversation_id,
  created_at,
  id,
  lang,
  possibly_sensitive,
  impression_count,
  like_count,
  quote_count,
  reply_count,
  retweet_count,
  reply_settings,
  text,
  pk_id,
  general_classification(text) as class_general_label,
  openai_classification_bricks(text) AS class_openai
FROM
  STREAM(live.fpds_json_data)
WHERE 
    lang='en'

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE fpds_keywords COMMENT "Tweets with extracted keywords score generated by the KeyBert" AS
WITH stacked_data AS (
  SELECT 
    twitter_query,
    id,
    pk_id,
    text,
    explode(split(general_keywords_extraction(TEXT),"\},")) AS keywords
  FROM STREAM(live.fpds_json_data)
  WHERE
    lang='en'
)
SELECT 
  twitter_query,
  id,
  pk_id,
  text,
  from_json(concat(keywords,'}'), 'entity STRING, score FLOAT').entity AS entity, 
  from_json(concat(keywords,'}'), 'entity STRING, score FLOAT').score AS score
FROM stacked_data
