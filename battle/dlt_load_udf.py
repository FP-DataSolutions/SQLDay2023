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
pyfunc_keywords_udf =  mlflow.pyfunc.spark_udf(spark, model_uri="models:/pyfunc_keyword_extraction/Production", result_type=StringType())
spark.udf.register("general_keywords_extraction", pyfunc_keywords_udf)

# COMMAND ----------

#Load the fine-tuned databricks openai classification model
openai_datarbicks_udf = mlflow.pyfunc.spark_udf(spark, model_uri="models:/openai_ft_databricks_classification/Production", result_type=StringType())
spark.udf.register("openai_classification_bricks", openai_datarbicks_udf)

openai_snflk_udf = mlflow.pyfunc.spark_udf(spark, model_uri="models:/openai_ft_snowflake_classification/Production", result_type=StringType())
spark.udf.register("openai_classification_snflk", openai_snflk_udf)
