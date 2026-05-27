# Databricks notebook source
# MAGIC %md
# MAGIC # Gold — Dimensão Perfil | SRAG DATASUS
# MAGIC **Tabela:** `certificacao_indicium.poc_srag_datasus.gold_dim_perfil_poc_srag_datasus`  
# MAGIC **Grão:** 1 linha por combinação única de atributos demográficos agregados  
# MAGIC **Estratégia:** OVERWRITE  
# MAGIC **Dependência:** `silver_poc_srag_datasus`

# COMMAND ----------

# MAGIC %md
# MAGIC ## [Célula 1] Setup

# COMMAND ----------

import uuid
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

RUN_ID      = str(uuid.uuid4())
CATALOGO    = "certificacao_indicium"
SCHEMA      = "poc_srag_datasus"
FULL_SILVER = f"{CATALOGO}.{SCHEMA}.silver_poc_srag_datasus"
FULL_DIM    = f"{CATALOGO}.{SCHEMA}.gold_dim_perfil_poc_srag_datasus"

print(f" Destino : {FULL_DIM}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## [Célula 2] Decodificadores de domínio

# COMMAND ----------

# cs_sexo
sexo_desc = (
    F.when(F.col("cs_sexo") == "M", F.lit("Masculino"))
     .when(F.col("cs_sexo") == "F", F.lit("Feminino"))
     .when(F.col("cs_sexo") == "I", F.lit("Ignorado"))
     .otherwise(F.lit("Não informado"))
)

# cs_raca
raca_desc = (
    F.when(F.col("cs_raca") == 1, F.lit("Branca"))
     .when(F.col("cs_raca") == 2, F.lit("Preta"))
     .when(F.col("cs_raca") == 3, F.lit("Amarela"))
     .when(F.col("cs_raca") == 4, F.lit("Parda"))
     .when(F.col("cs_raca") == 5, F.lit("Indígena"))
     .when(F.col("cs_raca") == 9, F.lit("Ignorado"))
     .otherwise(F.lit("Não informado"))
)

# cs_gestant
gestant_desc = (
    F.when(F.col("cs_gestant") == 1, F.lit("1º Trimestre"))
     .when(F.col("cs_gestant") == 2, F.lit("2º Trimestre"))
     .when(F.col("cs_gestant") == 3, F.lit("3º Trimestre"))
     .when(F.col("cs_gestant") == 4, F.lit("Idade Gestacional Ignorada"))
     .when(F.col("cs_gestant") == 5, F.lit("Não"))
     .when(F.col("cs_gestant") == 6, F.lit("Não se aplica"))
     .when(F.col("cs_gestant") == 9, F.lit("Ignorado"))
     .otherwise(F.lit("Não informado"))
)

# cs_escol_n
escol_desc = (
    F.when(F.col("cs_escol_n") == 0, F.lit("Sem escolaridade/Analfabeto"))
     .when(F.col("cs_escol_n") == 1, F.lit("Fundamental 1º ciclo"))
     .when(F.col("cs_escol_n") == 2, F.lit("Fundamental 2º ciclo"))
     .when(F.col("cs_escol_n") == 3, F.lit("Médio"))
     .when(F.col("cs_escol_n") == 4, F.lit("Superior"))
     .when(F.col("cs_escol_n") == 5, F.lit("Não se aplica"))
     .when(F.col("cs_escol_n") == 9, F.lit("Ignorado"))
     .otherwise(F.lit("Não informado"))
)

print(" Decodificadores definidos.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## [Célula 3] Extrair combinações únicas e construir dimensão

# COMMAND ----------

df_silver = (
    spark.table(FULL_SILVER)
    .filter(F.col("registro_mais_atual") == True)
)

df_dim_perfil = (
    df_silver
    .select(
        F.coalesce(F.col("cs_sexo"),    F.lit("n/a")).alias("cs_sexo"),
        F.coalesce(F.col("faixa_etaria"), F.lit("Ignorado")).alias("faixa_etaria"),
        F.coalesce(F.col("cs_raca").cast(StringType()), F.lit("n/a")).alias("cs_raca_str"),
        F.col("cs_raca"),
        F.coalesce(F.col("cs_gestant").cast(StringType()), F.lit("n/a")).alias("cs_gestant_str"),
        F.col("cs_gestant"),
        F.coalesce(F.col("cs_escol_n").cast(StringType()), F.lit("n/a")).alias("cs_escol_n_str"),
        F.col("cs_escol_n"),
    )
    .distinct()
    # SK baseada nas colunas coalesced para evitar hash de NULL
    .withColumn(
        "sk_perfil",
        F.sha2(
            F.concat_ws("||",
                F.col("cs_sexo"),
                F.col("faixa_etaria"),
                F.col("cs_raca_str"),
                F.col("cs_gestant_str"),
                F.col("cs_escol_n_str"),
            ), 256
        )
    )
    .withColumn("descricao_sexo",     sexo_desc)
    .withColumn("descricao_raca",     raca_desc)
    .withColumn("descricao_gestant",  gestant_desc)
    .withColumn("descricao_escol",    escol_desc)
    .withColumn("dh_ingestao_gold",   F.current_timestamp())
    .withColumn("run_id", F.lit(RUN_ID))
    .select(
        "sk_perfil", "cs_sexo", "descricao_sexo",
        "faixa_etaria", "cs_raca", "descricao_raca",
        "cs_gestant", "descricao_gestant",
        "cs_escol_n", "descricao_escol",
        "dh_ingestao_gold",
    )
    .dropDuplicates(["sk_perfil"])
)

total = df_dim_perfil.count()
print(f" Dimensão construída: {total:,} linhas | {len(df_dim_perfil.columns)} colunas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## [Célula 4] Gravação — OVERWRITE

# COMMAND ----------

(
    df_dim_perfil
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(FULL_DIM)
)

print(f" {FULL_DIM} gravada com sucesso.")
spark.sql(f"OPTIMIZE {FULL_DIM}")
dbutils.notebook.exit(f"SUCESSO|run_id={RUN_ID}|dim_perfil|linhas={total}")
