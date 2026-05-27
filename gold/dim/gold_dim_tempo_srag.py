# Databricks notebook source
# MAGIC %md
# MAGIC # Gold — Dimensão Tempo | SRAG DATASUS
# MAGIC **Tabela:** `certificacao_indicium.poc_srag_datasus.gold_dim_tempo_poc_srag_datasus`  
# MAGIC **Grão:** 1 linha por data única de `dt_sin_pri`  
# MAGIC **Estratégia:** OVERWRITE — dimensão pequena, reconstruída integralmente  
# MAGIC **Dependência:** `silver_poc_srag_datasus`

# COMMAND ----------

# MAGIC %md
# MAGIC ## [Célula 1] Setup

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType
import uuid
from datetime import datetime

RUN_ID       = str(uuid.uuid4())
CATALOGO     = "certificacao_indicium"
SCHEMA       = "poc_srag_datasus"
FULL_SILVER  = f"{CATALOGO}.{SCHEMA}.silver_poc_srag_datasus"
FULL_DIM     = f"{CATALOGO}.{SCHEMA}.gold_dim_tempo_poc_srag_datasus"

MESES_PT = {
    1: "Janeiro", 2: "Fevereiro", 3: "Março",    4: "Abril",
    5: "Maio",    6: "Junho",     7: "Julho",     8: "Agosto",
    9: "Setembro",10: "Outubro",  11: "Novembro", 12: "Dezembro"
}

print(f" Destino : {FULL_DIM}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## [Célula 2] Extrair datas únicas da Silver

# COMMAND ----------

df_datas = (
    spark.table(FULL_SILVER)
    .filter(
        (F.col("registro_mais_atual") == True) &
        (F.col("dt_sin_pri").isNotNull())
    )
    .select(F.col("dt_sin_pri").alias("data"))
    .distinct()
)

total_datas = df_datas.count()
DATA_REF = df_datas.agg(F.max("data").alias("max_data")).first()["max_data"]
print(f" Datas únicas encontradas: {total_datas:,}")
print(f" Data referência dataset: {DATA_REF}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## [Célula 3] Construir dimensão Tempo

# COMMAND ----------

# Mapeamento de número de mês para nome em português via SQL CASE
nome_mes_expr = (
    F.when(F.month("data") == 1,  F.lit("Janeiro"))
     .when(F.month("data") == 2,  F.lit("Fevereiro"))
     .when(F.month("data") == 3,  F.lit("Março"))
     .when(F.month("data") == 4,  F.lit("Abril"))
     .when(F.month("data") == 5,  F.lit("Maio"))
     .when(F.month("data") == 6,  F.lit("Junho"))
     .when(F.month("data") == 7,  F.lit("Julho"))
     .when(F.month("data") == 8,  F.lit("Agosto"))
     .when(F.month("data") == 9,  F.lit("Setembro"))
     .when(F.month("data") == 10, F.lit("Outubro"))
     .when(F.month("data") == 11, F.lit("Novembro"))
     .otherwise(F.lit("Dezembro"))
)

df_dim_tempo = (
    df_datas
    .withColumn("sk_tempo",
        F.sha2(F.col("data").cast("string"), 256))
    .withColumn("ano",              F.year("data").cast(IntegerType()))
    .withColumn("mes",              F.month("data").cast(IntegerType()))
    .withColumn("ano_mes",          F.date_format("data", "yyyy-MM"))
    .withColumn("dia",              F.dayofmonth("data").cast(IntegerType()))
    .withColumn("semana_epidemiologica",       F.weekofyear("data").cast(IntegerType()))
    .withColumn("trimestre",        F.quarter("data").cast(IntegerType()))
    .withColumn("nome_mes",         nome_mes_expr)
    .withColumn("eh_ultimos_30_dias",
        F.col("data") >= F.date_sub(F.lit(DATA_REF), 29))
    .withColumn("eh_ultimos_12_meses",
        F.col("data") >= F.add_months(F.lit(DATA_REF), -11))
    .withColumn("dh_ingestao_gold", F.current_timestamp())
    .withColumn("run_id", F.lit(RUN_ID))
    .select(
        "sk_tempo", "data", "ano", "mes", "ano_mes", "dia",
        "semana_epidemiologica", "trimestre", "nome_mes",
        "eh_ultimos_30_dias", "eh_ultimos_12_meses", "dh_ingestao_gold"
    )
)

print(f" Dimensão construída: {df_dim_tempo.count():,} linhas | {len(df_dim_tempo.columns)} colunas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## [Célula 4] Gravação — OVERWRITE

# COMMAND ----------

(
    df_dim_tempo
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(FULL_DIM)
)

print(f" {FULL_DIM} gravada com sucesso.")
spark.sql(f"OPTIMIZE {FULL_DIM}")
print(f" OPTIMIZE concluído.")
dbutils.notebook.exit(f"SUCESSO|run_id={RUN_ID}|dim_tempo|linhas={df_dim_tempo.count()}")
