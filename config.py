# Databricks notebook source
scope='kv-sqlday2023'
principalId = dbutils.secrets.get(scope,'DataBrickStorageServicePrincipalId')
principalKey = dbutils.secrets.get(scope,'DataBrickStorageServicePrincipalKey')
endpoint = dbutils.secrets.get(scope,'DataBrickStorageEndpoint')

configs = {
    "fs.azure.account.auth.type": "OAuth",
    "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
    "fs.azure.account.oauth2.client.id":principalId ,
    "fs.azure.account.oauth2.client.secret":principalKey,
    "fs.azure.account.oauth2.client.endpoint":endpoint
}

# COMMAND ----------

def mount(container):
    try:
        dbutils.fs.unmount(f"/mnt/{container}")
    except:
        pass
    
    dbutils.fs.mount(
        source = f"abfss://{container}@blsqlday2023.dfs.core.windows.net/",
        mount_point = f"/mnt/{container}",
        extra_configs = configs)
    print(f"The container \'{container}\' was properly mounted")
