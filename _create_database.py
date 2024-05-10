# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # 🥉 Bronze
# MAGIC
# MAGIC **Camada inicial, dados _as is_**
# MAGIC
# MAGIC Muito importante que dados nessa camada reflitam o banco ou fonte dos dados
# MAGIC
# MAGIC aqui podemos ter duplicidade em versões de dados que devem ser tratados nas camadas posteriores.
# MAGIC Costumo chamar essa camada de "lake"

# COMMAND ----------

spark.sql("""
CREATE DATABASE IF NOT EXISTS
    olist_bronze
COMMENT "Olist bronze layer"
          """)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # 🥈 Silver
# MAGIC
# MAGIC **camada de limpeza, normalização e enriquecimento de dados.**
# MAGIC
# MAGIC e.g., 
# MAGIC  - uppercase
# MAGIC  - data textual para tipo date
# MAGIC  - dias em atraso (diferença entre data atual e data de envimento)
# MAGIC
# MAGIC p.s. embora tenha visto várias implementações distintas em projetos que atuei, prefiro desconsiderar as regras de negócio nessa camada (deixamos para aplicar na camada gold)
# MAGIC

# COMMAND ----------

spark.sql("""
CREATE DATABASE IF NOT EXISTS
    olist_silver
COMMENT "Olist silver layer"
          """)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # 🥇 Gold
# MAGIC
# MAGIC **camada para aplicação de regras de negócio**
# MAGIC
# MAGIC e.g.,
# MAGIC  - junção/união de tabelas
# MAGIC  - filtro de dados

# COMMAND ----------

spark.sql("""
CREATE DATABASE IF NOT EXISTS
    olist_gold
COMMENT "Olist gold layer"
          """)
