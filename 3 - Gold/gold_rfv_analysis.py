# Databricks notebook source
# MAGIC %run ../_utils

# COMMAND ----------

from pyspark.sql.functions import (
    col,
    count,
    date_diff,
    lit,
    min,
    count,
    sum,
    sqrt,
    when,
    concat
)
from datetime import date

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # RFV
# MAGIC
# MAGIC RFV, ou Recency, Frequency, and Value, é uma técnica de análise de dados frequentemente usada em marketing e gerenciamento de clientes para segmentar clientes com base em seu comportamento de compra.
# MAGIC
# MAGIC Essa abordagem analisa três aspectos principais do comportamento do cliente:
# MAGIC
# MAGIC - **Recency (Recência):** Refere-se à última vez que um cliente fez uma compra. Geralmente, clientes que fizeram compras recentes são mais propensos a fazer compras futuras do que aqueles que não compraram há muito tempo.
# MAGIC - **Frequency (Frequência):** Refere-se à frequência com que um cliente faz compras durante um determinado período de tempo. Clientes que compram com frequência podem ser considerados mais leais e valiosos para a empresa.
# MAGIC - **Value (Valor):** Refere-se ao valor monetário total das compras feitas por um cliente durante um determinado período de tempo. Clientes que gastam mais têm um valor de vida do cliente mais alto e podem ser alvos de estratégias de marketing mais agressivas.
# MAGIC
# MAGIC Ao analisar esses três aspectos juntos, as empresas podem segmentar seus clientes em diferentes grupos com base em seu comportamento de compra e adaptar suas estratégias de marketing e relacionamento com o cliente de acordo. Por exemplo, clientes com alta recência, frequência e valor podem ser segmentados como clientes VIP e receber ofertas exclusivas, enquanto clientes com baixa recência, frequência e valor podem ser alvos de campanhas de reativação.
# MAGIC
# MAGIC ----
# MAGIC
# MAGIC ## Implementação
# MAGIC
# MAGIC  - **1 Calcular a Recency (Recência):** Determinar a última data de compra para cada cliente.
# MAGIC  - **2 Calcular a Frequency (Frequência):** Contar o número de transações que cada cliente realizou durante um determinado período de tempo.
# MAGIC  - **3 Calcular o Value (Valor):** Calcular o valor total gasto por cada cliente durante o mesmo período de tempo.
# MAGIC  - **4 Atribuir Pontuações:** Atribuir pontuações a cada cliente com base nos três aspectos (Recency, Frequency, Value).
# MAGIC - **5 Segmentação de Clientes:** Segmentar os clientes com base nas pontuações atribuídas.
# MAGIC

# COMMAND ----------

tb_name = "olist_gold.customers_rfv"
dataset_location = "olist_customers_rfv_dataset"
target_location = f"dbfs:/FileStore/delta/brazilian_ecommerce/{dataset_location}/gold"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 - Data ingestion
# MAGIC

# COMMAND ----------

df_customers = spark.sql("SELECT customer_id, customer_unique_id, customer_state FROM olist_silver.customers")

df_orders = spark.sql("SELECT order_id, customer_id, DATE(order_purchase_timestamp) as order_date FROM olist_silver.orders")

# dados de order items cruzados com products para trazermos a categoria do produto
df_order_items =spark.sql("""SELECT 
                          order_id, l.product_id, product_category_name  
                          FROM olist_silver.order_items l 
                          INNER JOIN olist_silver.products r
                          ON l.product_id = r.product_id
                          """)

df_order_payments = spark.sql("SELECT order_id, payment_value FROM olist_silver.order_payments")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 2 - preparation

# COMMAND ----------

date_pd = df_orders.select("order_date").toPandas()

MIN_DATE = date_pd.min()[0]
MAX_DATE = date_pd.max()[0]

print(f'''
Analysis init date: {MIN_DATE}
 Analysis end date: {MAX_DATE}
  Days of analysis: {(MAX_DATE - MIN_DATE).days}
''')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Joins

# COMMAND ----------

df = (
    df_orders
    .join(df_order_payments, on="order_id", how="left")
    .join(df_customers, on="customer_id", how="left")
    .filter(col("payment_value").isNotNull() & col("customer_state").isNotNull())
    .withColumn("time_since_last_order", date_diff(lit(MAX_DATE), col("order_date")))
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### RFV

# COMMAND ----------

df = (
    df
    .groupBy("customer_unique_id")
    .agg(
        min("time_since_last_order").alias("recency"), # tempo da ultima compra
        count("order_id").alias("frequency"), # frequencia de compra
        sum("payment_value").alias("value") # valor total de gastos
    )
)

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Ranking
# MAGIC
# MAGIC  - **Classificação dos dados de acordo com quartis:** As colunas recency, frequency e value são classificadas de acordo com os quartis 25%, 50% e 75%, respectivamente. Isso significa que estamos dividindo os dados em quatro partes iguais com base em cada métrica.
# MAGIC  - **Classificação dos tiers com base no score: Os tiers (Bronze, Silver, Gold)** são atribuídos com base nos quartis calculados.
# MAGIC
# MAGIC  O uso de quartis permite uma distribuição equitativa dos clientes em cada tier com base na distribuição dos dados.
# MAGIC ____
# MAGIC  **Para melhoria:** O calculo de score deveria ser revisto. Por exemplo, clientes com alta frequência e alto valor são extremamente valiosos, enquanto aqueles com alta recência, mas baixa frequência e valor, podem precisar de mais atenção para aumentar seu engajamento. 

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### quartiles
# MAGIC
# MAGIC **Calcular os quartis das métricas RFV:**
# MAGIC
# MAGIC Utilizamos a função approxQuantile para calcular os quartis das métricas "recency" (tempo desde a última compra), "frequency" (frequência de compras) e "value" (valor total gasto).
# MAGIC
# MAGIC Os quartis são calculados nos percentis 25%, 50% e 75%, com uma precisão de 0.01.
# MAGIC
# MAGIC **Extrair os quartis para cada métrica:**
# MAGIC Extraimos os quartis calculados para cada métrica (recency, frequency e value) e criamos colunas separados (recency_quartiles, frequency_quartiles e value_quartiles).

# COMMAND ----------

# Calcular os quartis das métricas RFV
quartiles = df.approxQuantile(
    ["recency", "frequency", "value"], [0.25, 0.5, 0.75], 0.01
)

# Extrair os quartis para cada métrica
recency_quartiles = quartiles[0]
frequency_quartiles = quartiles[1]
value_quartiles = quartiles[2]

# Adicionar os quartis como colunas no DataFrame
df_rfv = df.withColumn("recency_quartile",
                       when(col("recency") <= recency_quartiles[0], "Q1")
                       .when((col("recency") > recency_quartiles[0]) & (col("recency") <= recency_quartiles[1]), "Q2")
                       .when((col("recency") > recency_quartiles[1]) & (col("recency") <= recency_quartiles[2]), "Q3")
                       .otherwise("Q4"))

df_rfv = df_rfv.withColumn("frequency_quartile",
                            when(col("frequency") <= frequency_quartiles[0], "Q1")
                            .when((col("frequency") > frequency_quartiles[0]) & (col("frequency") <= frequency_quartiles[1]), "Q2")
                            .when((col("frequency") > frequency_quartiles[1]) & (col("frequency") <= frequency_quartiles[2]), "Q3")
                            .otherwise("Q4"))

df_rfv = df_rfv.withColumn("value_quartile",
                            when(col("value") <= value_quartiles[0], "Q1")
                            .when((col("value") > value_quartiles[0]) & (col("value") <= value_quartiles[1]), "Q2")
                            .when((col("value") > value_quartiles[1]) & (col("value") <= value_quartiles[2]), "Q3")
                            .otherwise("Q4"))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Tier
# MAGIC
# MAGIC **Classificar os tiers com bas enas frequencias:**
# MAGIC
# MAGIC - Se a recency for Q1 (melhor), a frequency for Q4 (melhor) e a value for Q4 (melhor), o cliente recebe o tier Gold, indicando o melhor desempenho possível.
# MAGIC - Se a recency for Q2 (segunda-melhor), a frequency for Q4 (melhor) e a value for Q3 (segundo-melhor), o cliente também recebe o tier Gold, desepenho medio
# MAGIC
# MAGIC a frequencia está sendo utilizada como se tivesse maior peso para um tier maior.
# MAGIC
# MAGIC Para qualquer outro caso, o cliente recebe o tier Bronze, indicando um desempenho abaixo dos níveis de prata e ouro.
# MAGIC
# MAGIC **Disclaimer** esse cálculo é fictício e não deve ser levado em consideração "ao pé da letra". Apenas serve de teste para mostrar a possibilidade de segmentação de clientes para campanhas de marketing, por exemplo.

# COMMAND ----------

# Classificar os tiers
df = df_rfv.withColumn("tier",
    when((col("recency_quartile") == "Q1") & (col("frequency_quartile") == "Q4") & (col("value_quartile") == "Q4"), "Gold")
    .when((col("recency_quartile") == "Q2") & (col("frequency_quartile") == "Q4") & (col("value_quartile") == "Q3"), "Gold")
    .when((col("recency_quartile") == "Q2") & (col("frequency_quartile") == "Q3") & (col("value_quartile") == "Q3"), "Silver")
    .when((col("recency_quartile") == "Q3") & (col("frequency_quartile") == "Q3") & (col("value_quartile") == "Q2"), "Silver")
    .otherwise("Bronze")
)

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Saving data

# COMMAND ----------

save_dataframe(df, format_mode="delta", table_name=tb_name, target_location=target_location, mode="overwrite")

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
# MAGIC select * from olist_gold.customers_rfv

# COMMAND ----------


