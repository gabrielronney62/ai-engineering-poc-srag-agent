# Databricks notebook source
# MAGIC %md
# MAGIC # Gold — Fato SRAG | SRAG DATASUS
# MAGIC **Tabela:** `certificacao_indicium.poc_srag_datasus.gold_fact_poc_srag_datasus`  
# MAGIC **Grão:** 1 linha por caso SRAG pseudoanonimizado (hash_caso único, registro ativo)  
# MAGIC **Estratégia:** OVERWRITE (volume desta PoC comporta reprocessamento integral)  
# MAGIC **Dependência:** `silver_poc_srag_datasus` + todas as 4 dimensões Gold

# COMMAND ----------

# MAGIC %md
# MAGIC ## [Célula 1] Setup

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, TimestampType
import uuid
from datetime import datetime

RUN_ID           = str(uuid.uuid4())
CATALOGO         = "certificacao_indicium"
SCHEMA           = "poc_srag_datasus"
FULL_SILVER      = f"{CATALOGO}.{SCHEMA}.silver_poc_srag_datasus"
FULL_DIM_TEMPO   = f"{CATALOGO}.{SCHEMA}.gold_dim_tempo_poc_srag_datasus"
FULL_DIM_LOCAL   = f"{CATALOGO}.{SCHEMA}.gold_dim_localidade_poc_srag_datasus"
FULL_DIM_PERFIL  = f"{CATALOGO}.{SCHEMA}.gold_dim_perfil_poc_srag_datasus"
FULL_DIM_CLASSI  = f"{CATALOGO}.{SCHEMA}.gold_dim_classificacao_poc_srag_datasus"
FULL_FACT        = f"{CATALOGO}.{SCHEMA}.gold_fact_poc_srag_datasus"

print(f" Destino : {FULL_FACT}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## [Célula 2] Leitura da Silver — apenas registros ativos

# COMMAND ----------

df_silver = (
    spark.table(FULL_SILVER)
    .filter(F.col("registro_mais_atual") == True)
)

total_silver = df_silver.count()
print(f" Registros ativos na Silver: {total_silver:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## [Célula 3] Gerar chaves surrogate para o Join com dimensões
# MAGIC
# MAGIC Recalculamos os mesmos hashes usados para gerar as SKs nas dimensões,
# MAGIC garantindo que os joins resolvam corretamente sem depender de ID sequencial.

# COMMAND ----------

from pyspark.sql.types import StringType

# ── SK Tempo (baseado em dt_sin_pri) ──────────────────────────────────────────
df_fact = df_silver.withColumn(
    "sk_tempo",
    F.when(
        F.col("dt_sin_pri").isNotNull(),
        F.sha2(F.col("dt_sin_pri").cast("string"), 256)
    ).otherwise(F.lit(None))
)

# ── SK Localidade ─────────────────────────────────────────────────────────────
df_fact = df_fact.withColumn(
    "sk_localidade",
    F.sha2(
        F.concat_ws("||",
            F.coalesce(F.col("sg_uf_residencia"),  F.lit("n/a")),
            F.coalesce(F.col("co_mun_res"), F.lit("n/a")),
        ), 256
    )
)

# ── SK Perfil ─────────────────────────────────────────────────────────────────
df_fact = df_fact.withColumn(
    "sk_perfil",
    F.sha2(
        F.concat_ws("||",
            F.coalesce(F.col("cs_sexo"),                              F.lit("n/a")),
            F.coalesce(F.col("faixa_etaria"),                         F.lit("Ignorado")),
            F.coalesce(F.col("cs_raca").cast(StringType()),           F.lit("n/a")),
            F.coalesce(F.col("cs_gestant").cast(StringType()),        F.lit("n/a")),
            F.coalesce(F.col("cs_escol_n").cast(StringType()),        F.lit("n/a")),
        ), 256
    )
)

# ── SK Classificação ──────────────────────────────────────────────────────────
df_fact = df_fact.withColumn(
    "sk_classificacao",
    F.sha2(
        F.concat_ws("||",
            F.coalesce(F.col("classi_fin").cast(StringType()), F.lit("n/a")),
            F.coalesce(F.col("criterio").cast(StringType()),   F.lit("n/a")),
            F.coalesce(F.col("evolucao").cast(StringType()),   F.lit("n/a")),
        ), 256
    )
)

# ── SK Caso (PK do Fato) ──────────────────────────────────────────────────────
df_fact = df_fact.withColumn(
    "sk_caso",
    F.sha2(F.coalesce(F.col("hash_caso"), F.lit("n/a")), 256)
)

print(" Surrogate Keys geradas.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## [Célula 4] Selecionar colunas finais do Fato

# COMMAND ----------

df_fact_final = (
    df_fact
    .withColumn("dh_ingestao_gold", F.current_timestamp())
    .select(
        # ── PKs e FKs ─────────────────────────────────────────────────────────
        "sk_caso",
        "hash_caso",
        "sk_tempo",
        "sk_localidade",
        "sk_perfil",
        "sk_classificacao",
        # ── Datas de referência (mantidas para análise direta sem join) ────────
        "dt_sin_pri",
        "dt_notific",
        "dt_interna",
        "dt_entuti",
        "dt_saiduti",
        "dt_evoluca",
        # ── Métricas numéricas ────────────────────────────────────────────────
        "dias_uti",
        "qtd_doses_covid_registradas",
        # ── Flags epidemiológicas (17) ────────────────────────────────────────
        "flag_caso_srag",
        "flag_obito_srag",
        "flag_obito_outras_causas",
        "flag_cura",
        "flag_evolucao_informada",
        "flag_hospitalizado",
        "flag_uti",
        "flag_uso_suporte_ventilatorio",
        "flag_vacinado_gripe",
        "flag_info_vacina_gripe",
        "flag_vacinado_covid",
        "flag_info_vacina_covid",
        "flag_covid_classificacao_final",
        "flag_influenza_classificacao_final",
        "flag_outro_virus_classificacao_final",
        "flag_srag_nao_especificado",
        # ── Auditoria ─────────────────────────────────────────────────────────
        "dh_ultima_atualizacao",
        "dh_ingestao_gold",
        # run_id adicionado via withColumn após select
    )
    # Deduplicar por sk_caso (garante 1 linha por caso no fato)
    .withColumn("run_id", F.lit(RUN_ID))
    .dropDuplicates(["sk_caso"])
)

total_fact = df_fact_final.count()
print(f" Fato construído: {total_fact:,} linhas | {len(df_fact_final.columns)} colunas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## [Célula 5] Validação de integridade referencial

# COMMAND ----------

print(" Validação de integridade referencial:")

# Verificar FKs não resolvidas (SKs que não encontram match nas dimensões)
dim_tempo    = spark.table(FULL_DIM_TEMPO).select("sk_tempo").distinct()
dim_local    = spark.table(FULL_DIM_LOCAL).select("sk_localidade").distinct()
dim_perfil   = spark.table(FULL_DIM_PERFIL).select("sk_perfil").distinct()
dim_classi   = spark.table(FULL_DIM_CLASSI).select("sk_classificacao").distinct()

orphaos_tempo  = df_fact_final.join(dim_tempo,  "sk_tempo",         "left_anti") \
                               .filter(F.col("sk_tempo").isNotNull()).count()
orphaos_local  = df_fact_final.join(dim_local,  "sk_localidade",    "left_anti").count()
orphaos_perfil = df_fact_final.join(dim_perfil, "sk_perfil",        "left_anti").count()
orphaos_classi = df_fact_final.join(dim_classi, "sk_classificacao", "left_anti").count()

for dim, orfaos in [
    ("dim_tempo",          orphaos_tempo),
    ("dim_localidade",     orphaos_local),
    ("dim_perfil",         orphaos_perfil),
    ("dim_classificacao",  orphaos_classi),
]:
    status = "✅" if orfaos == 0 else "⚠️ "
    print(f"   {status} FK {dim:20s} — órfãos: {orfaos:,}")

if orphaos_tempo + orphaos_local + orphaos_perfil + orphaos_classi > 0:
    print("\n⚠️  Atenção: existem FKs sem match nas dimensões.")
    print("   Verifique se as dimensões foram geradas antes do fato.")
else:
    print("\n✅ Integridade referencial 100% validada.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## [Célula 6] Gravação — OVERWRITE

# COMMAND ----------

(
    df_fact_final
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(FULL_FACT)
)

print(f" {FULL_FACT} gravada com sucesso.")

spark.sql(f"""
    OPTIMIZE {FULL_FACT}
    ZORDER BY (dt_sin_pri, sk_classificacao, sk_localidade)
""")
print(f" OPTIMIZE + Z-ORDER concluídos.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## [Célula 7] Resumo do Fato

# COMMAND ----------

print("\n Resumo do Fato SRAG:")
df_fact_final.agg(
    F.count("*").alias("total_casos"),
    F.sum("flag_obito_srag").alias("total_obitos"),
    F.sum("flag_uti").alias("total_uti"),
    F.sum("flag_hospitalizado").alias("total_hospitalizados"),
    F.sum("flag_vacinado_covid").alias("total_vacinados_covid"),
    F.min("dt_sin_pri").alias("data_minima"),
    F.max("dt_sin_pri").alias("data_maxima"),
).show(truncate=False)

dbutils.notebook.exit(f"SUCESSO|run_id={RUN_ID}|gold_fact|linhas={total_fact}")
