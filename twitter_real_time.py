# Databricks notebook source
import requests

from datetime import datetime
import ipywidgets as widgets
import json
import pandas as pd
import os
from os.path import exists
import tweepy

# COMMAND ----------

# MAGIC %run ./config

# COMMAND ----------

dbutils.widgets.text('Twitter search query','Tucker')

# COMMAND ----------

scope='kv-sqlday2023'
bearerToken = dbutils.secrets.get(scope,'TwitterSQLDay2023BearerToken')
client = tweepy.Client(bearerToken, return_type = requests.Response)

tweet_fields_all = 'attachments,author_id,context_annotations,conversation_id,created_at,edit_controls,entities,geo,id,in_reply_to_user_id,lang,public_metrics,possibly_sensitive,referenced_tweets,reply_settings,source,text,withheld'
user_fields_all = 'created_at,description,entities,id,location,name,pinned_tweet_id,profile_image_url,protected,public_metrics,url,username,verified,verified_type,withheld'
place_fields_all = 'contained_within,country,country_code,full_name,geo,id,name,place_type'

# COMMAND ----------

query = dbutils.widgets.get("Twitter search query")
query_tw = '#snowflakedb OR snowflake data' if query=='snowflakedb' else query
next_token_exists = True
next_token = None
max_id_path = f"/dbfs/mnt/data/twitter_real_time/{query}/_max_id.txt"
if exists(max_id_path):
    f = open(max_id_path,"r")
    max_id = f.read()
else:
    max_id = None

json_data = []
json_includes = []

# COMMAND ----------

while next_token_exists:
    response = client.search_recent_tweets(query_tw + " -is:retweet",
                                           tweet_fields = tweet_fields_all, 
                                           user_fields = user_fields_all, 
                                           place_fields = place_fields_all, 
                                           expansions = 'geo.place_id,author_id', 
                                           max_results = 100,
                                           next_token = next_token,
                                           since_id = max_id)
    if response.json()['meta']['result_count'] != 0:
        json_data = json_data + response.json()['data']
        json_includes = json_includes + [response.json()['includes']]
    
    if 'next_token' in response.json()['meta'].keys():
        next_token = response.json()['meta']['next_token']
    else:
        next_token_exists = False

# COMMAND ----------

if not os.path.exists(f"/dbfs/mnt/data/twitter_real_time/{query}"):
    os.makedirs(f"/dbfs/mnt/data/twitter_real_time/{query}")

if len(json_data) != 0:
    f = open(f"/dbfs/mnt/data/twitter_real_time/{query}/_max_id.txt","w")
    f.write(json_data[0]['id']) # search_recent_tweets by default sorts tweets by recency from the newest returend as first
    f.close()

    f = open(f"/dbfs/mnt/data/twitter_real_time/{query}/status.txt","w")
    f.write(f"Updated at {datetime.today().strftime('%Y%m%d_%H%M%S')}")
    f.close()

    dbutils.fs.put(f"dbfs:/mnt/data/twitter_real_time/{query}/data/{datetime.today().strftime('%Y%m%d_%H%M%S')}.json",str(json.dumps(json_data)), True)
    dbutils.fs.put(f"dbfs:/mnt/data/twitter_real_time/{query}/includes/{datetime.today().strftime('%Y%m%d_%H%M%S')}.json",str(json.dumps(json_includes)), True)
else:
    f = open(f"/dbfs/mnt/data/twitter_real_time/{query}/status.txt","w")
    f.write(f"No update at {datetime.today().strftime('%Y%m%d_%H%M%S')}")
    f.close()
