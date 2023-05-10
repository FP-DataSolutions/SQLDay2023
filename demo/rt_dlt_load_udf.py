# Databricks notebook source
# MAGIC %pip install mlflow --upgrade

# COMMAND ----------

# MAGIC %pip install torch

# COMMAND ----------

# MAGIC %pip install transformers

# COMMAND ----------

# MAGIC %pip install keybert

# COMMAND ----------

# MAGIC %pip install openai

# COMMAND ----------

import mlflow
import openai
import os
import pandas as pd
import re

from keybert import KeyBERT
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import StringType

# COMMAND ----------

#Load transformers classification model
classifier_udf_reg = mlflow.pyfunc.spark_udf(spark, model_uri="models:/transformers_general_classification/Production", result_type=StringType())
spark.udf.register("general_classification", classifier_udf_reg)

# COMMAND ----------

#Load the keywords extractor
pyfunc_keywords_udf =  mlflow.pyfunc.spark_udf(spark, model_uri="models:/pyfunc_keyword_extraction/Staging", result_type=StringType())
spark.udf.register("general_keywords_extraction", pyfunc_keywords_udf)

# COMMAND ----------

#Load fine-tuned openai classifier
openai_datarbicks_udf = mlflow.pyfunc.spark_udf(spark, model_uri="models:/openai_ft_databricks_classification/Production", result_type=StringType())
spark.udf.register("openai_classification_bricks", openai_datarbicks_udf)

# COMMAND ----------

# # DLT feature-store not implemented in public preview
# @dlt.create_table(
#     comment = 'Enriched date features for tweets'
# )
# def twitter_time_features():
#     df = dlt.read_stream('fpds_json_data')
#     df = df.withColumn("day_of_week", dayofweek("created_at").cast("integer"))
#     df = df.withColumn("is_weekend", when((df["day_of_week"] == 7) | (df["day_of_week"] == 1), 1).otherwise(0))
#     df = df.select(['pk_id','created_at','day_of_week','is_weekend'])
#     return df

# dlt.create_feature_table(
#     name = 'fsdb_time_features',
#     comment = 'Streaming table of tweet enriched time features.',
#     primary_keys = 'pk_id',
#     timestamp_key = 'created_at'
# )

# dlt.apply_changes(
#     target = 'fsdb_time_features',
#     source = 'twitter_time_features',
#     keys= ['pk_id'],
#     sequence_by = 'created_at',
#     stored_as_scd_type = "2"
# )
