# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # 🗃️ Brazilian ecommerce olist
# MAGIC
# MAGIC Este é um conjunto de dados públicos de comércio eletrônico brasileiro das compras feitas na loja Olist. O conjunto de dados contém informações de 100 mil pedidos de 2016 a 2018 feitos em vários marketplaces no Brasil. Suas características permitem visualizar um pedido em várias dimensões: desde o status do pedido, preço, pagamento e desempenho de frete até a localização do cliente, atributos do produto e, finalmente, avaliações escritas pelos clientes. Também disponibilizamos um conjunto de dados de geolocalização que relaciona os códigos postais brasileiros às coordenadas lat/long.
# MAGIC
# MAGIC Estes são dados comerciais reais, foram anonimizados, e as referências às empresas e parceiros no texto de revisão foram substituídas pelos nomes das grandes casas de Game of Thrones.

# COMMAND ----------

# MAGIC %run ./_utils

# COMMAND ----------

# MAGIC %run ./_create_database

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## dependency

# COMMAND ----------

!pip install opendatasets

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## download dataset from kaggle
# MAGIC
# MAGIC O dataset escolhido foi o [Brazilian E-Commerce Public Dataset by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerces)
# MAGIC
# MAGIC Como é um dataset estático (ou quase 100%), não faz sentido adicionar upsert e tampouco streaming.
# MAGIC
# MAGIC Mas aqui poderíamos utilizar do AutoLoader, ou até mesmo de alguma ferramenta com CDC, como airbyte
# MAGIC
# MAGIC Nossa staging não precisaria existir (apenas caso fossem dados vindos por airbyte, por exemplo). Mas vou criar aqui apenas para exemplificar, pois irei salvar a staging em parquet. Depois disso, todas as camadas serão em Delta

# COMMAND ----------

import opendatasets as od
od.download("https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce", "dataset/")

# COMMAND ----------

# MAGIC %ls dbfs:/FileStore/dataset/brazilian-ecommerce/

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC os arquivos foram salvos em `/FileStore/dataset/brazilian-ecommerce/olist_*.csv``

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Carregando o arquivo .csv para spark dataframe
# MAGIC
# MAGIC aqui o .od fez eu usar o workspace files. [mais detalhes aqui](https://docs.databricks.com/en/files/index.html)

# COMMAND ----------

df = (
    spark.read.format("csv")
    .options(header="true", inferSchema=True)
    .load("dataset/brazilian-ecommerce/olist_customers_dataset.csv")
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 🔎 First exploratory data analysis (mini descriptive)

# COMMAND ----------

display(df)

# COMMAND ----------

print(f"Total de linhas: {df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Data set central. Deve ser usado com o uma especie de "tabela fato"
# MAGIC ### schema
# MAGIC image

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Saving data
# MAGIC
# MAGIC salvando todos os .csv no dbf em arquivo parquet
# MAGIC
# MAGIC Nossa staging poderia ser um S3 (aws) ou blob storage, mas vai ser diretamente no dbfs mesmo

# COMMAND ----------

files = [
    "olist_customers_dataset",
    "olist_orders_dataset",
    "olist_geolocation_dataset",
    "olist_products_dataset",
    "olist_order_items_dataset",
    "olist_sellers_dataset",
    "olist_order_payments_dataset",
    "product_category_name_translation",
    "olist_order_reviews_dataset",
]

# COMMAND ----------

for file_name in files:
    # multiline é preciso por conta principalmente dos reviews. pois tem  \n
    df = (
        spark.read.format("csv")
        .options(header="true", inferSchema=True, multiLine=True, quote='"', escape='"')
        .load(f"dbfs:/FileStore/dataset/brazilian-ecommerce/{file_name}.csv")
    )

    save_dataframe(
        df,
        format_mode="parquet",
        target_location=f"dbfs:/FileStore/parquet/brazilian_ecommerce/{file_name}",
    )

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Aqui utilizamos de algumas options para leitura do csv
# MAGIC
# MAGIC ```
# MAGIC .option("quote", "\"") # para que o delimitador de string seja "
# MAGIC .option("header", True) # para que a primeira linha do csv seja lida como header
# MAGIC .option("inferSchema", True) # para que infira o schema automaticamente
# MAGIC .option("multiLine", True) # para casos onde tem quebra de linha no texto
# MAGIC .option("escape", "\"") # para ignorar aspas duplas dentro de uma string (principalmente nos reviews)
# MAGIC
# MAGIC ```

# COMMAND ----------

# MAGIC %fs ls '/FileStore/parquet/brazilian_ecommerce/'

# COMMAND ----------

dbutils.notebook.exit("OK")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### test

# COMMAND ----------

display(spark.read.parquet('/FileStore/parquet/brazilian_ecommerce/olist_order_reviews_dataset/'))

# COMMAND ----------


