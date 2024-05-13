# Databricks notebook source
!pip install unidecode

# COMMAND ----------

from pyspark.sql.functions import col, regexp_replace, trim
import unidecode
import re

# COMMAND ----------

def save_dataframe(df, format_mode, target_location):
    """
    save a df into location in parquet or delta mode
    df: Spark dataframe
    format_mode: should be "delta" or "parquet"
    target_location: target file location to write files :D
    """

    if format_mode not in ["delta", "parquet"]:
        print(f"[LOG] Not recognized mode {format_mode}. Should be 'delta' or 'parquet'")
    else:
        print(f"[LOG] Saving 'data' {format_mode} file on {target_location}", end="... ")
        try:
            (df
             .write
             .format(format_mode)
             .mode("overwrite")
             .option("overwriteSchema", "true")
             .save(target_location))
            print("OK!")
        except Exception as e:
            dbutils.notebook.exit(f"[LOG] ERROR writing files: \n{e}")

# COMMAND ----------

def create_table(tb_name, target_location):
    try:
        print(f"Creating delta table {tb_name} on {target_location}... ", end="")
        spark.sql(f"CREATE TABLE IF NOT EXISTS {tb_name} USING DELTA LOCATION '{target_location}'")
        print("OK!")
    except Exception as e:
        dbutils.notebook.exit(f"[LOG] ERROR: Creating table {tb_name} \n {e}")


# COMMAND ----------


# alguns metodos que utilizo no dia a dia
def remove_special_characters(s):
  """
  Remove caracteres especiais de uma string
  """
  s = s.replace("\n", "")
  return re.sub(r'[^\w\s]', '', s)

def remove_accents(s):
  """
  remove os acentos de uma string, substiuindo pelo caractere sem acento
  """
  return unidecode.unidecode(s)

def fix_df_column_names(df):
  """
  renomeia colunas, deixando em caixa baixa, removendo espaços em branco do inicio e do fim
  removendo acentos e caracteres especiais
  trocando espaços em branco por underline
  """
  for old_col_name in df.columns:
    new_col_name = remove_accents(old_col_name)
    new_col_name = remove_special_characters(new_col_name)
    new_col_name = new_col_name.lower().strip().replace(" ", "_").replace("", "") # caixa baixa -> sem espaço no final -> espaços por _
    df = df.withColumnRenamed(old_col_name, new_col_name)
  return df
