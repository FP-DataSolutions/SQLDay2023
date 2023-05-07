# Databricks notebook source
# MAGIC %pip install keybert

# COMMAND ----------

import mlflow
import pandas as pd
import re

from keybert import KeyBERT
from pyspark.sql.types import StringType

# COMMAND ----------

# Read data for tests
df = spark.read.table('tucker.tucker_json_data')
df_small= df.limit(10)

# COMMAND ----------

# PyFunc used to log MLFlow model
class KeywordExtractionModel(mlflow.pyfunc.PythonModel):

    def predict(self, context, model_input):
        return self.keyword_extraction(model_input)

    def keyword_extraction(self, model_input):
        kw_model = KeyBERT()
        model_input = model_input.iloc[:,0]
        entities = kw_model.extract_keywords(model_input.to_list(), keyphrase_ngram_range=(1,2))
        keys = ['entity', 'score']
        entities_struct = [[dict(zip(keys,values)) for values in records] for records in entities]
        entities_str = [re.sub(r"[\[\]]+",'',str(tweet)) for tweet in entities_struct]
        return pd.Series(entities_str)

# COMMAND ----------

# Log model to mlflow
with mlflow.start_run() as run:
    model_info = mlflow.pyfunc.log_model(artifact_path="pyfunc_keyword_extraction", python_model=KeywordExtractionModel())
loaded_model = mlflow.pyfunc.spark_udf(spark, model_uri=model_info.model_uri, result_type=StringType())

# Register model as SQL function
spark.udf.register("keywords_extraction", loaded_model) 

# COMMAND ----------

# Test the model registered as spark udf
display(df_small.withColumn('keywords',loaded_model(df_small.text)))

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Test the model registered as SQL function
# MAGIC WITH stacked_data AS (
# MAGIC   SELECT 
# MAGIC     twitter_query,
# MAGIC     text,
# MAGIC     explode(split(keywords_extraction(TEXT),"\},")) AS keywords
# MAGIC   FROM fpds.fpds_json_data
# MAGIC   WHERE
# MAGIC     lang='en'
# MAGIC )
# MAGIC SELECT 
# MAGIC   twitter_query,
# MAGIC   text,
# MAGIC   from_json(concat(keywords,'}'), 'entity STRING, score FLOAT').entity AS entity, 
# MAGIC   from_json(concat(keywords,'}'), 'entity STRING, score FLOAT').score AS score
# MAGIC FROM stacked_data

# COMMAND ----------

#Register model
model_details = mlflow.register_model(f"runs:/{run.info.run_id}/pyfunc_keyword_extraction","pyfunc_keyword_extraction")

#Add descriptions to the registered model
client = mlflow.MlflowClient()
client.update_registered_model(
    name=model_details.name,
    description="This model extracts keywords from tweets, using KeyBERT model. More info can be found at https://maartengr.github.io/KeyBERT/")

client.update_model_version(
    name=model_details.name,
    version=model_details.version,
    description="This is a tested ona a small set and first working version of the keyword extraction model"
)

client.set_tag(run.info.run_id, key="db_table", value="fpds.fpds_json_data")

#Move the model to production
client.transition_model_version_stage(
    name=model_details.name,
    version=model_details.version,
    stage='production'
)