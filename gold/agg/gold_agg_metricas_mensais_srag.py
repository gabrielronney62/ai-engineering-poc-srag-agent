# Databricks notebook source
# MAGIC %md
# MAGIC # Gold — Agregada: Métricas Mensais | SRAG DATASUS
# MAGIC **Tabela:** `certificacao_indicium.poc_srag_datasus.gold_agg_poc_srag_metricas_mensais`  
# MAGIC **Grão:** 1 linha por ano-mês de primeiros sintomas  
# MAGIC **Estratégia:** OVERWRITE  
# MAGIC **Dependência:** `gold_fact_poc_srag_datasus`

# COMMAND ----------

# MAGIC %md
# MAGIC ## [Célula 1] Setup

# COMMAND ----------

import uuid
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DoubleType

RUN_ID     = str(uuid.uuid4())
CATALOGO   = "certificacao_indicium"
SCHEMA     = "poc_srag_datasus"
FULL_FACT  = f"{CATALOGO}.{SCHEMA}.gold_fact_poc_srag_datasus"
FULL_AGG   = f"{CATALOGO}.{SCHEMA}.gold_agg_poc_srag_metricas_mensais"

print(f" Destino : {FULL_AGG}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## [Célula 2] Agregar por ano-mês

# COMMAND ----------

df_fact = spark.table(FULL_FACT).filter(F.col("dt_sin_pri").isNotNull())
DATA_REF = df_fact.agg(F.max("dt_sin_pri").alias("max_dt")).first()["max_dt"]

df_mensal = (
    df_fact
    .withColumn("ano",     F.year("dt_sin_pri").cast(IntegerType()))
    .withColumn("mes",     F.month("dt_sin_pri").cast(IntegerType()))
    .withColumn("ano_mes", F.date_format("dt_sin_pri", "yyyy-MM"))
    .groupBy("ano", "mes", "ano_mes")
    .agg(
        F.sum("flag_caso_srag").alias("total_casos"),
        F.sum("flag_obito_srag").alias("total_obitos"),
        F.sum("flag_evolucao_informada").alias("total_casos_com_evolucao"),
        F.sum("flag_hospitalizado").alias("total_hospitalizados"),
        F.sum("flag_uti").alias("total_uti"),
        F.sum("flag_vacinado_gripe").alias("total_vacinados_gripe"),
        F.sum("flag_info_vacina_gripe").alias("total_info_vacina_gripe"),
        F.sum("flag_vacinado_covid").alias("total_vacinados_covid"),
        F.sum("flag_info_vacina_covid").alias("total_info_vacina_covid"),
        F.sum("flag_covid_classificacao_final").alias("total_covid"),
        F.sum("flag_influenza_classificacao_final").alias("total_influenza"),
        F.sum("flag_outro_virus_classificacao_final").alias("total_outro_virus"),
        F.sum("flag_srag_nao_especificado").alias("total_srag_nao_especificado"),
        F.avg("dias_uti").alias("media_dias_uti"),
    )
    .orderBy("ano", "mes")
)

# ── Taxas mensais ─────────────────────────────────────────────────────────────
df_mensal = (
    df_mensal
    .withColumn("taxa_mortalidade",
        F.when(F.col("total_casos_com_evolucao") > 0,
            F.round(F.col("total_obitos") / F.col("total_casos_com_evolucao"), 4)
        ).otherwise(F.lit(None).cast(DoubleType()))
    )
    .withColumn("taxa_uso_uti",
        F.when(F.col("total_hospitalizados") > 0,
            F.round(F.col("total_uti") / F.col("total_hospitalizados"), 4)
        ).otherwise(F.lit(None).cast(DoubleType()))
    )
    .withColumn("taxa_vacinacao_gripe_registrada",
        F.when(F.col("total_info_vacina_gripe") > 0,
            F.round(F.col("total_vacinados_gripe") / F.col("total_info_vacina_gripe"), 4)
        ).otherwise(F.lit(None).cast(DoubleType()))
    )
    .withColumn("taxa_vacinacao_covid_registrada",
        F.when(F.col("total_info_vacina_covid") > 0,
            F.round(F.col("total_vacinados_covid") / F.col("total_info_vacina_covid"), 4)
        ).otherwise(F.lit(None).cast(DoubleType()))
    )
    .withColumn("media_dias_uti", F.round(F.col("media_dias_uti"), 1))
    .withColumn("dh_ingestao_gold", F.current_timestamp())
    .withColumn("run_id", F.lit(RUN_ID))
)

total = df_mensal.count()
print(f"✅ Agregação mensal construída: {total:,} meses")

# COMMAND ----------

# MAGIC %md
# MAGIC ## [Célula 3] Amostra últimos 12 meses

# COMMAND ----------

print(" Amostra — últimos 12 meses:")
df_mensal.filter(
    F.col("ano_mes") >= F.date_format(F.add_months(F.lit(DATA_REF), -11), "yyyy-MM")
).select(
    "ano_mes", "total_casos", "total_obitos", "total_uti",
    "taxa_mortalidade", "taxa_uso_uti",
    "taxa_vacinacao_gripe_registrada", "taxa_vacinacao_covid_registrada"
).orderBy(F.col("ano_mes").desc()).show(12, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## [Célula 4] Gravação — OVERWRITE

# COMMAND ----------

(
    df_mensal
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(FULL_AGG)
)

print(f"✅ {FULL_AGG} gravada com sucesso.")
spark.sql(f"OPTIMIZE {FULL_AGG} ZORDER BY (ano_mes)")
print(f"✅ OPTIMIZE + Z-ORDER concluídos.")
dbutils.notebook.exit(f"SUCESSO|run_id={RUN_ID}|agg_mensal|linhas={total}")
