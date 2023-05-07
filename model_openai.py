# Databricks notebook source
import mlflow
import numpy as np
import openai
import pandas as pd

from pyspark.sql.types import StringType

# COMMAND ----------

# Read data for tests
df = spark.read.table('tucker.tucker_json_data')
df_small= df.limit(10)

# COMMAND ----------

# PyFunc used to log MLFlow model
class OpenaiClassificationModel(mlflow.pyfunc.PythonModel):

    def predict(self, context, model_input):
        return self.openai_classification(model_input)

    def openai_classification(self, model_input):
        ft_model = 'ada:ft-personal-2023-04-25-18-14-48'
        model_input = model_input.iloc[:,0]
        tweet_lst = [tweet+' ->' for tweet in model_input.to_list()]
        if len(tweet_lst)>2047: #openai prompt length limit 2049
            batches = (len(tweet_lst)//2047)+1
            model_labels = []
            for i in range(batches):
                res = openai.Completion.create(model=ft_model, prompt=tweet_lst[i*2047:min((i+1)*2047,len(tweet_lst))], max_tokens=1, temperature=0)
                model_labels_tmp = [res['text'] for res in res['choices']]
                model_labels = model_labels+model_labels_tmp
        else:        
            res = openai.Completion.create(model=ft_model, prompt=tweet_lst, max_tokens=1, temperature=0)
            model_labels = [res['text'] for res in res['choices']]
        return pd.Series(model_labels)

# COMMAND ----------

# Log model to mlflow
with mlflow.start_run() as run:
    model_info = mlflow.pyfunc.log_model(artifact_path="pyfunc_openai_snflk_classification", python_model=OpenaiClassificationModel())

loaded_model = mlflow.pyfunc.spark_udf(spark, model_uri=model_info.model_uri, result_type=StringType())
spark.udf.register("openai_snflk_classification", loaded_model)

# COMMAND ----------

# Test the model registered as spark udf
display(df_small.withColumn('class',loaded_model(df_small.text)))

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Test the model registered as SQL function
# MAGIC SELECT
# MAGIC   twitter_query,
# MAGIC   text,
# MAGIC   openai_snflk_classification(text) AS class_openai_bricks
# MAGIC FROM
# MAGIC   tucker.tucker_json_data
# MAGIC LIMIT 10

# COMMAND ----------

#Register model
model_details = mlflow.register_model(f"runs:/{run.info.run_id}/pyfunc_openai_snflk_classification","openai_ft_snowflake_classification")

#Add descriptions to the registered model
client = mlflow.MlflowClient()
client.update_registered_model(
    name=model_details.name,
    description="This model is a fine-tuned classifier of tweets, with 11 labels. The model was fine-tuned with openai API on 250+ examples.")

client.update_model_version(
    name=model_details.name,
    version=model_details.version,
    description="Added prompt size limit workaround by batch inference."
)

client.set_tag(run.info.run_id, key="db_table", value="fpds.fpds_json_data")

#Move the model to production
client.transition_model_version_stage(
    name=model_details.name,
    version=model_details.version,
    stage='production'
)
