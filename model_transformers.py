# Databricks notebook source
# MAGIC %pip install mlflow --upgrade

# COMMAND ----------

# MAGIC %pip install transformers --upgrade 

# COMMAND ----------

import transformers
import mlflow

from pyspark.sql.types import StringType

# COMMAND ----------

# Read data for tests
df = spark.read.table('tucker.tucker_json_data')
df_small= df.limit(10)

# COMMAND ----------

# Transformers flavour used to log MLFlow model
architecture = "cardiffnlp/tweet-topic-21-multi"
classifier_pipeline = transformers.pipeline(task="text-classification", model=architecture)

with mlflow.start_run():
  model_info = mlflow.transformers.log_model(
    transformers_model=classifier_pipeline,
    artifact_path="transformers_model_classification",
    input_example="Databricks recently released mlflow veriosn 2.3, where you can easily integrate with openai and transformers models."
  )
loaded_model = mlflow.pyfunc.spark_udf(spark, model_uri=model_info.model_uri, result_type=StringType()) #mlflow.transformers.load_model(model_info.model_uri)

# Register model as SQL function
spark.udf.register("general_classification", loaded_model) 

# COMMAND ----------

# Test the model registered as spark udf
display(df_small.withColumn('class',loaded_model(df_small.text)))

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Test the model registered as SQL function
# MAGIC SELECT
# MAGIC   twitter_query,
# MAGIC   text,
# MAGIC   general_classification(text) AS class_general
# MAGIC FROM
# MAGIC   tucker.tucker_json_data
# MAGIC LIMIT 10

# COMMAND ----------

#Register model
model_details = mlflow.register_model(model_info.model_uri,"transformers_general_classification")

#Add descriptions to the registered model
client = mlflow.MlflowClient()
client.update_registered_model(
    name=model_details.name,
    description="This model classifies tweets to predefined general categories. Model used is a pretrained huggingface language model trained on ~124M tweets gathered from 2018 to 2021. More details at https://huggingface.co/cardiffnlp/tweet-topic-21-multi")

client.update_model_version(
    name=model_details.name,
    version=model_details.version,
    description="This is a tested ona a small set and working version of the pretrained model"
)

client.set_tag(run.info.run_id, key="db_table", value="fpds.fpds_json_data")

#Move the model to production
client.transition_model_version_stage(
    name=model_details.name,
    version=model_details.version,
    stage='production'
)
