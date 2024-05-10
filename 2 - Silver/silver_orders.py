# Databricks notebook source
# MAGIC %run ../_utils

# COMMAND ----------

from pyspark.sql.functions import when, to_date, col, dayofweek, date_format, month, year
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

tb_name = "olist_silver.orders"
dataset_location = "olist_orders_dataset"
target_location = f"dbfs:/FileStore/delta/brazilian_ecommerce/{dataset_location}/silver"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 - Data ingestion

# COMMAND ----------

df = spark.read.table("olist_bronze.orders") # leituira da delta table

# COMMAND ----------

df.printSchema()

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 2 - Data Munging
# MAGIC
# MAGIC Processo de limpeza e normalizações necessárias
# MAGIC
# MAGIC Como esses dados já são bem tratados, vamos apenas atentar para extração ou "enriquecimento" de dados.
# MAGIC
# MAGIC Inicialmente iremos carregar dados para agregações:
# MAGIC  - Campo mes/ano para calcular vendas mensais, trimestrais e etc
# MAGIC
# MAGIC Podemos nos focar no tempo decorrido de cada etapa, por exemplo:
# MAGIC  - tempo até a aprovação (em minutos ou segundos)
# MAGIC  - tempo de entrega (em dias)
# MAGIC  - tempo total da compra até a entrega (em dias)
# MAGIC  - atraso (divergencia entre tempo estimado e o entregue)
# MAGIC
# MAGIC  Além disso, podemos trazer dados que auxiliem na analise do padrão de compra por data
# MAGIC   - dia da semana
# MAGIC   - é fim de semana?
# MAGIC
# MAGIC aqui poderia estressar e ir até para coisas do tipo, mas daí nao daria tempo no projeto:
# MAGIC pandas_market_calendars
# MAGIC  - é feriado?
# MAGIC  - qual feriado
# MAGIC  - dias até o próximo feriado - para entender padrões de compra próximo a feriados

# COMMAND ----------

# MAGIC %md
# MAGIC ### month_year
# MAGIC
# MAGIC mes/ano da venda
# MAGIC
# MAGIC Campos desse tipo ajudam em calculos vendas mensais

# COMMAND ----------

df = df.withColumns(
    {
        "month_year": date_format("order_purchase_timestamp", "MM-yyy"),
        "order_purchase_month": month("order_purchase_timestamp"),
        "order_purchase_year": year("order_purchase_timestamp"),
    }
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### minutes_to_approve
# MAGIC
# MAGIC Tempo decorrido até a aprovação do pedido
# MAGIC
# MAGIC time delta entre ``order_purchase_timestamp`` e ``order_approved_at``

# COMMAND ----------

df = get_diff_between_dates(
    df,
    col_1="order_approved_at",
    col_2="order_purchase_timestamp",
    delta_type="minutes",
    col_name="minutes_to_approve",
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### days_to_deliver
# MAGIC #### hours e days
# MAGIC
# MAGIC Tempo total da transportadora até chegar ao destinatário
# MAGIC
# MAGIC time delta entre ``order_delivered_carrier_date`` e ``order_delivered_customer_date``

# COMMAND ----------

df = get_diff_between_dates(
    df,
    col_1="order_delivered_customer_date",
    col_2="order_delivered_carrier_date",
    delta_type="days",
    col_name="days_to_deliver",
)
df = get_diff_between_dates(
    df,
    col_1="order_delivered_customer_date",
    col_2="order_delivered_carrier_date",
    delta_type="hours",
    col_name="hours_to_deliver",
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### total_elapsed_time
# MAGIC
# MAGIC Tempo total decorrido em que o cliente solicitou a compra e recebeu em sua casa

# COMMAND ----------

# extrair o tempo até a entrega (em dias)
df = get_diff_between_dates(
    df,
    col_1="order_delivered_customer_date",
    col_2="order_purchase_timestamp",
    delta_type="days",
    col_name="total_elapsed_days",
)
# extrair o tempo até a entrega (em horas)
df = get_diff_between_dates(
    df,
    col_1="order_delivered_customer_date",
    col_2="order_purchase_timestamp",
    delta_type="hours",
    col_name="total_elapsed_hours",
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### delay_time and overdue?
# MAGIC
# MAGIC ``overdue``>: True or false
# MAGIC
# MAGIC aqui como é uma base estática, não faz sentido adicionar o atrasado com sentido de "entrega está atrasada?", apenas para o sentido de se a "entrega ocorreu com atraso". 
# MAGIC E por isso talvez fizesse sentido adicionar teste no "order_status" para garantir comportamento ideal.[]
# MAGIC
# MAGIC
# MAGIC ``delay_time``>: estimado x real
# MAGIC
# MAGIC

# COMMAND ----------

df = df.withColumn(
    "overdue",
    when(
        col("order_estimated_delivery_date") > col("order_delivered_customer_date"),
        True,
    ).otherwise(False),
)

# pegar os atrasados e informar o tempo de atraso
df = df.withColumn(
    "delay_time",
    when(
        col("overdue"),
        col("order_estimated_delivery_date").cast("long")
        - col("order_delivered_customer_date").cast("long"),
    ).otherwise(0),
)

# converter para dias e para horas
df = df.withColumn("delay_hours", round(col("delay_time") / 3600, 2))
df = df.withColumn("delay_days", round(col("delay_time") / (24 * 3600), 2)).drop(
    "delay_time"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### day_of_week

# COMMAND ----------

df = df.withColumn("number_day_of_week", dayofweek(to_date(col("order_purchase_timestamp"))))

# Traduzir os números do dia da semana para o português
df = df.withColumn(
    "day_of_week",
    when(df["number_day_of_week"] == 1, "Domingo")
    .when(df["number_day_of_week"] == 2, "Segunda-feira")
    .when(df["number_day_of_week"] == 3, "Terça-feira")
    .when(df["number_day_of_week"] == 4, "Quarta-feira")
    .when(df["number_day_of_week"] == 5, "Quinta-feira")
    .when(df["number_day_of_week"] == 6, "Sexta-feira")
    .when(df["number_day_of_week"] == 7, "Sábado"),
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### weekend?
# MAGIC
# MAGIC criar um campo para informar se a data de compra é em um fim de semana (ajuda também para verificar a logística)

# COMMAND ----------

df = df.withColumn(
    "weekend",
    when(col("day_of_week").isin(["Sábado", "Domingo"]), True).otherwise(False),
)

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC belezura!
# MAGIC
# MAGIC Limpamos um cadim (nada), enriquecemos e transformamos algumas coisas e temos nosso dataset preparado para analises (até preditiva e prescritiva -> mas aí tem que fazer um pouco mais de cleanings e etc)

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
# MAGIC select * from olist_silver.orders

# COMMAND ----------


