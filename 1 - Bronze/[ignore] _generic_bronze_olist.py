# Databricks notebook source
# MAGIC %run ../_utils

# COMMAND ----------

import datetime

# COMMAND ----------

# widgets para debugar
# e parametros para receber do 'orquestrador' da camada bronze
table_name = dbutils.widgets.get("table_name")
dataset_location = dbutils.widgets.get("dataset_location")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Camada bronze
# MAGIC
# MAGIC Na camada bronze, nenhuma limpeza ou regra de negócio devem ser aplicadas aos dados.
# MAGIC
# MAGIC só vamos ler em parquet e salvar em delta.
# MAGIC
# MAGIC Vamos também utilizar da tabela de controle para termos o milestone da ultima execução (aqui nao será utilizado de fato, mas é interessante justamenete para o UPSERT)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 - leitura dos dados

# COMMAND ----------

parquet_location = f"/FileStore/parquet/brazilian_ecommerce/{dataset_location}"
target_location = f"/FilesStore/delta/brazilian_ecommerce/{dataset_location}/bronze"

df = spark.read.parquet(parquet_location)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Save dataframe

# COMMAND ----------

updated_timestamp = None
try:
    save_dataframe(df, format_mode="delta", target_location=target_location)
    updated_timestamp = datetime.datetime.now()
    
except Exception as e:
    dbutils.notebook.exit(None)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Create delta table

# COMMAND ----------

tb_name = f"olist_bronze.{table_name}"
print(f"table full name: {tb_name}")

# COMMAND ----------

create_table(tb_name, target_location)

# COMMAND ----------

# para sair do notebook
# tudo que rodar após isso será ignorado quando o notebook for executado via workflow

# retornar o timestamp de atualização
dbutils.notebook.exit(updated_timestamp)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select count(*) from olist_bronze.customers

# COMMAND ----------


