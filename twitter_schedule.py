# Databricks notebook source
import json
import requests

from datetime import datetime
import ipywidgets as widgets
import pandas as pd
import tweepy

# COMMAND ----------

# MAGIC %run ./config

# COMMAND ----------

dbutils.widgets.dropdown(
    'Twitter search query',
    'sqlday',
    ['sqlday','delta-live-tables','snowflakedb','databricks']
)

# COMMAND ----------

scope='kv-sqlday2023'
bearerToken = dbutils.secrets.get(scope,'TwitterSQLDay2023BearerToken')
client = tweepy.Client(bearerToken, return_type = requests.Response)

tweet_fields_all = 'attachments,author_id,context_annotations,conversation_id,created_at,edit_controls,entities,geo,id,in_reply_to_user_id,lang,public_metrics,possibly_sensitive,referenced_tweets,reply_settings,source,text,withheld'
user_fields_all = 'created_at,description,entities,id,location,name,pinned_tweet_id,profile_image_url,protected,public_metrics,url,username,verified,verified_type,withheld'
place_fields_all = 'contained_within,country,country_code,full_name,geo,id,name,place_type'

df_accepted_keys = ['id','text','created_at','lang','public_metrics']

# COMMAND ----------

query_dict = {'sqlday':'sqlday','delta-live-tables':'\"delta live tables\"','snowflakedb':'#snowflakedb OR snowflake data','databricks':'databricks'}
query = dbutils.widgets.get("Twitter search query")
query_tw = query_dict[query]
next_token_exists = True
next_token = None

json_data = []
json_includes = []
response_df = []

while next_token_exists:
    response = client.search_recent_tweets(query_tw + " -is:retweet",
                                           tweet_fields = tweet_fields_all, 
                                           user_fields = user_fields_all, 
                                           place_fields = place_fields_all, 
                                           expansions = 'geo.place_id,author_id', 
                                           max_results = 100,
                                           next_token = next_token)
    json_data = json_data + response.json()['data']
    json_includes = json_includes + [response.json()['includes']]
    response_df = response_df + [{key: post[key] for key in df_accepted_keys} for post in response.json()['data']]
    
    if 'next_token' in response.json()['meta'].keys():
        next_token = response.json()['meta']['next_token']
    else:
        next_token_exists = False
        
response_df = pd.DataFrame(response_df)
response_df = pd.concat([response_df,response_df['public_metrics'].apply(pd.Series)],axis=1)
response_df.drop('public_metrics', axis=1, inplace=True)

response_df = spark.createDataFrame(response_df)

# COMMAND ----------

mount('data')

# COMMAND ----------

response_df.write.csv(f"dbfs:/mnt/data/twitter/{query}/{datetime.today().strftime('%Y%m%d')}_response_df", header=True)

dbutils.fs.put(f"dbfs:/mnt/data/twitter/{query}/data/{datetime.today().strftime('%Y%m%d')}.json",str(json.dumps(json_data)), True)
dbutils.fs.put(f"dbfs:/mnt/data/twitter/{query}/includes/{datetime.today().strftime('%Y%m%d')}.json",str(json.dumps(json_includes)), True)
