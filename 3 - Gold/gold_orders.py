# Databricks notebook source
# MAGIC %run ../_utils

# COMMAND ----------

from pyspark.sql.functions import when, to_date, col, dayofweek, max, collect_set, sum
from pyspark.sql.types import StringType, BooleanType

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Camada GOLD
# MAGIC
# MAGIC Na camada gold, as limpezas e ajustes já foram feitos, então essa camada é responsável por aplicar regras de negócio, agregações e junções de dados que convirjam para analises.

# COMMAND ----------

tb_name = "olist_gold.orders"
dataset_location = "olist_orders_dataset"
target_location = f"dbfs:/FileStore/delta/brazilian_ecommerce/{dataset_location}/gold"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 - Data ingestion
# MAGIC
# MAGIC Conforme o schema disponibilizado, iremos agregar os dados em uma big table que permitirá ~quase~ todas as analises subsequentes
# MAGIC
# MAGIC Apenas para fins de teste, iremos agregar apenas reviews e payments à table "fact" orders;
# MAGIC Portanto, iremos carregar essas tabelas
# MAGIC

# COMMAND ----------

df_orders = spark.read.table("olist_silver.orders") # leituira da delta table central, orders
df_order_reviews = spark.read.table("olist_silver.order_reviews") # leituira da delta table "dim" reviews
df_order_payments = spark.read.table("olist_silver.order_payments") # leituira da delta table "dim" payments
#df_order_items = spark.sql("select * from olist_silver.order_items") # leitura de outra maneira, da delta table "dim" items

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 2 - preparation

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### 2.1 order_payments
# MAGIC
# MAGIC uma order_id pode ter várias formas de pagamento (geralmente vouchs).  cada pagamento gera um registro
# MAGIC
# MAGIC Então iremos agregar, somando em valor de pagamento e pegando o max payment_sequential

# COMMAND ----------

df_order_payments = df_order_payments.groupBy("order_id").agg(
    max("payment_sequential").alias("total_payment_sequential"),
    sum("payment_value").alias("total_payment_value"),
    collect_set("payment_type").alias("payment_types"),
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### 2.2 order_reviews
# MAGIC
# MAGIC podemos perceber que existem casos onde existe mais de um review para um mesmo order_id

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select count(*), order_id from olist_silver.order_reviews group by order_id having count(*) > 1

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from olist_silver.order_reviews where order_id = 'f63a31c3349b87273468ff7e66852056'

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 2 - Data Join

# COMMAND ----------

print(f"Total de registros ANTES da agregação {df_orders.count()}")

# COMMAND ----------

df = (df_orders
      .join(df_order_payments, on=['order_id'], how='left')
      .join(df_order_reviews, on=['order_id'], how='left'))

# COMMAND ----------

print(f"Total de registros DEPOIS da agregação {df.count()}")

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


