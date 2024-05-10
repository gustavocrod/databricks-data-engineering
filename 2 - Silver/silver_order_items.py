# Databricks notebook source
# MAGIC %run ../_utils

# COMMAND ----------

from pyspark.sql.functions import when, to_date, col, dayofweek
from pyspark.sql.types import StringType, BooleanType

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Camada Silver
# MAGIC
# MAGIC Na camada silver, limpezas e ajustes em dados devem ser aplicados
# MAGIC
# MAGIC Caso seja possível, enriquecer os dados e extrair dados também deve acontecer nessa camada (minha definição)

# COMMAND ----------

tb_name = "olist_silver.order_items"
dataset_location = "olist_order_items_dataset"
target_location = f"dbfs:/FileStore/delta/brazilian_ecommerce/{dataset_location}/silver"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 - Data ingestion

# COMMAND ----------

df = spark.read.table("olist_bronze.order_items") # leituira da delta table

# COMMAND ----------

df.printSchema()

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 2 - Data Munging
# MAGIC
# MAGIC Aqui nessa table os dados já estão limpos e tratados. Aqui temos a ciencia de que é uma tablea intermediária gerada a partir de um n:n

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Saving data

# COMMAND ----------

save_dataframe(df, format_mode="delta", table_name=tb_name, target_location=target_location)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## create delta table
# MAGIC
# MAGIC TODO: implementar UPSERT
# MAGIC
# MAGIC o upsert serve para não precisar reescrever todos os dados, mas aproveitar do Delta para fazer um MERGE, caso um registro antigo tenha uma nova versão e INSERT para os dados que são novos

# COMMAND ----------

create_table(tb_name, target_location)

# COMMAND ----------

# exit para fechar a execução
dbutils.notebook.exit("OK")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from olist_silver.order_items

# COMMAND ----------


