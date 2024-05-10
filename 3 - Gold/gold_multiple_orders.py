# Databricks notebook source
# MAGIC %run ../_utils

# COMMAND ----------

from pyspark.sql.functions import col, count, when

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Camada GOLD
# MAGIC
# MAGIC Na camada gold, as limpezas e ajustes já foram feitos, então essa camada é responsável por aplicar regras de negócio, agregações e junções de dados que convirjam para analises.
# MAGIC
# MAGIC Essa é uma tabela sumarizada analítica.
# MAGIC
# MAGIC O objetivo dela é informar os meses em que tiveram mais pedidos

# COMMAND ----------

tb_name = "olist_gold.multiple_orders"
dataset_location = "olist_multiple_orders_dataset"
target_location = f"dbfs:/FileStore/delta/brazilian_ecommerce/{dataset_location}/gold"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 - Data ingestion
# MAGIC

# COMMAND ----------

df = spark.read.table("olist_gold.customer_orders") # leituira da delta table central, orders (ja da propria gold)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 2 - preparation

# COMMAND ----------

df = df.groupBy("customer_unique_id").agg(
    count("order_id").alias("total_orders"),
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # 2.1 multiple_orders?
# MAGIC
# MAGIC informar se o cliente comprou mais de uma vez

# COMMAND ----------

df = df.withColumn("multiple_order", when(col("total_orders") > 1, True).otherwise(False))

# COMMAND ----------

display(df)

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

create_table(table_name=tb_name, target_location=target_location)

# COMMAND ----------

# exit para fechar a execução
dbutils.notebook.exit("OK")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from olist_gold.multiple_orders

# COMMAND ----------


