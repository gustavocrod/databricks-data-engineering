# Databricks notebook source
# MAGIC %run ../_utils

# COMMAND ----------

from pyspark.sql.functions import col, count

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

tb_name = "olist_gold.total_orders_month_year"
dataset_location = "olist_total_orders_dataset"
target_location = f"dbfs:/FileStore/delta/brazilian_ecommerce/{dataset_location}/gold"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 - Data ingestion
# MAGIC

# COMMAND ----------

df_orders = spark.read.table("olist_gold.orders") # leituira da delta table central, orders (ja da propria gold)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 2 - preparation

# COMMAND ----------

df = df_orders.groupBy("order_purchase_year", "order_purchase_month").agg(
    count("order_id").alias("total_orders_by_month_year"),
).orderBy("order_purchase_year", "order_purchase_month")

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
# MAGIC select * from olist_gold.orders

# COMMAND ----------


