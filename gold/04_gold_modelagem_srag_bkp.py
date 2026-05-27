# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · Modelagem Gold — SRAG DATASUS
# MAGIC
# MAGIC **Camada:** Gold  
# MAGIC **Modelo:** Star Schema (Fato + 4 Dimensões + 3 Agregadas)  
# MAGIC **Estratégia:** Full Load (OVERWRITE) para dimensões e agregadas  
# MAGIC **Fonte:** silver_poc_srag_datasus (registro_mais_atual = True)

# COMMAND ----------

# [Célula 1] Setup
from datetime import datetime, date, timedelta
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import *
import uuid

# COMMAND ----------

# [Célula 2] Parâmetros

RUN_ID   = str(uuid.uuid4())
CATALOGO = "certificacao_indicium"
SCHEMA   = "poc_srag_datasus"

SILVER   = f"{CATALOGO}.{SCHEMA}.silver_poc_srag_datasus"
GOLD_FACT        = f"{CATALOGO}.{SCHEMA}.gold_fact_poc_srag_datasus"
GOLD_DIM_TEMPO   = f"{CATALOGO}.{SCHEMA}.gold_dim_tempo_poc_srag_datasus"
GOLD_DIM_LOC     = f"{CATALOGO}.{SCHEMA}.gold_dim_localidade_poc_srag_datasus"
GOLD_DIM_PERF    = f"{CATALOGO}.{SCHEMA}.gold_dim_perfil_poc_srag_datasus"
GOLD_DIM_CLASS   = f"{CATALOGO}.{SCHEMA}.gold_dim_classificacao_poc_srag_datasus"
GOLD_AGG_DIARIA  = f"{CATALOGO}.{SCHEMA}.gold_agg_poc_srag_metricas_diarias"
GOLD_AGG_MENSAL  = f"{CATALOGO}.{SCHEMA}.gold_agg_poc_srag_metricas_mensais"
GOLD_AGG_IND     = f"{CATALOGO}.{SCHEMA}.gold_agg_poc_srag_indicadores_relatorio"

DH_INGESTAO_GOLD = datetime.utcnow()

# COMMAND ----------

# [Célula 3] Leitura da Silver — apenas registros atuais

df_silver = (
    spark.table(SILVER)
    .filter(F.col("registro_mais_atual") == True)
)

TOTAL_SILVER = df_silver.count()
print(f"Registros Silver (atuais): {TOTAL_SILVER:,}")

# COMMAND ----------

# [Célula 4] dim_tempo — Geração programática

# Extrair range de datas do dataset
data_min_row = df_silver.select(F.min("dt_sin_pri")).collect()[0][0]
data_max_row = df_silver.select(F.max("dt_sin_pri")).collect()[0][0]
hoje = date.today()

data_inicio = data_min_row if data_min_row else date(2019, 1, 1)
data_fim    = max(data_max_row, hoje) if data_max_row else hoje

# Gerar sequência de datas via Spark
df_datas = spark.sql(f"""
    SELECT sequence(
        to_date('{data_inicio}'),
        to_date('{data_fim}'),
        interval 1 day
    ) AS datas
""").withColumn("data", F.explode(F.col("datas"))).drop("datas")

# Data de referência para flags de janela
hoje_lit = F.lit(str(hoje))

MESES_PT = {
    1: "Janeiro", 2: "Fevereiro", 3: "Março", 4: "Abril",
    5: "Maio", 6: "Junho", 7: "Julho", 8: "Agosto",
    9: "Setembro", 10: "Outubro", 11: "Novembro", 12: "Dezembro"
}

df_dim_tempo = (
    df_datas
    .withColumn("sk_tempo",  F.sha2(F.col("data").cast("string"), 256))
    .withColumn("ano",       F.year("data"))
    .withColumn("mes",       F.month("data"))
    .withColumn("dia",       F.dayofmonth("data"))
    .withColumn("ano_mes",   F.date_format("data", "yyyy-MM"))
    .withColumn("semana_epidemiologica", F.concat(
        F.year("data").cast("string"), F.lit("-"),
        F.lpad(F.weekofyear("data").cast("string"), 2, "0")
    ))
    .withColumn("trimestre", F.quarter("data"))
    .withColumn("nome_mes",  F.create_map(
        *[val for pair in [(F.lit(k), F.lit(v)) for k, v in MESES_PT.items()] for val in pair]
    ).getItem(F.month("data")))
    .withColumn("eh_ultimos_30_dias",   F.col("data") >= F.date_sub(F.to_date(hoje_lit), 30))
    .withColumn("eh_ultimos_12_meses",  F.col("data") >= F.add_months(F.to_date(hoje_lit), -12))
    .withColumn("dh_ingestao_gold", F.lit(DH_INGESTAO_GOLD).cast("timestamp"))
)

(
    df_dim_tempo
    .write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(GOLD_DIM_TEMPO)
)

print(f"gold_dim_tempo gravada: {df_dim_tempo.count():,} datas")

# COMMAND ----------

# [Célula 5] dim_localidade

df_dim_loc = (
    df_silver
    .select(
        F.coalesce(F.col(F.col("sg_uf_residencia").alias("sg_uf")), F.lit("n/a")).alias("sg_uf"),
        F.coalesce(F.col("co_mun_residencia"), F.lit("n/a")).alias("co_mun_residencia"),
        F.col("id_municip_residencia"),
        F.col("sg_uf_internacao"),
        F.col("co_mun_internacao"),
        F.col("id_mn_internacao"),
    )
    .distinct()
    .withColumn(
        "sk_localidade",
        F.sha2(
            F.concat_ws("||",
                F.coalesce(F.col("sg_uf"), F.lit("n/a")),
                F.coalesce(F.col("co_mun_residencia"), F.lit("n/a"))
            ), 256
        )
    )
    .withColumn("dh_ingestao_gold", F.lit(DH_INGESTAO_GOLD).cast("timestamp"))
)

(
    df_dim_loc
    .write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(GOLD_DIM_LOC)
)

print(f"gold_dim_localidade gravada: {df_dim_loc.count():,} localidades")

# COMMAND ----------

# [Célula 6] dim_perfil

df_dim_perf = (
    df_silver
    .select(
        F.coalesce(F.col("cs_sexo"), F.lit("I")).alias("cs_sexo"),
        F.coalesce(F.col("faixa_etaria"), F.lit("Ignorado")).alias("faixa_etaria"),
        F.col("cs_raca"),
        F.col("cs_gestant"),
        F.col("cs_escol_n"),
    )
    .distinct()
    .withColumn(
        "sk_perfil",
        F.sha2(
            F.concat_ws("||",
                F.coalesce(F.col("cs_sexo"), F.lit("n/a")),
                F.coalesce(F.col("faixa_etaria"), F.lit("n/a")),
                F.coalesce(F.col("cs_raca").cast("string"), F.lit("n/a")),
                F.coalesce(F.col("cs_gestant").cast("string"), F.lit("n/a")),
                F.coalesce(F.col("cs_escol_n").cast("string"), F.lit("n/a")),
            ), 256
        )
    )
    .withColumn("dh_ingestao_gold", F.lit(DH_INGESTAO_GOLD).cast("timestamp"))
)

(
    df_dim_perf
    .write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(GOLD_DIM_PERF)
)

print(f"gold_dim_perfil gravada: {df_dim_perf.count():,} perfis")

# COMMAND ----------

# [Célula 7] dim_classificacao — com descrições decodificadas

MAPA_CLASSI = {
    1: "SRAG por Influenza",
    2: "SRAG por outro vírus respiratório",
    3: "SRAG por outro agente etiológico",
    4: "SRAG não especificado",
    5: "SRAG por COVID-19",
}

MAPA_CRITERIO = {
    1: "Laboratorial",
    2: "Clínico Epidemiológico",
    3: "Clínico",
    4: "Clínico Imagem",
}

MAPA_EVOLUCAO = {
    1: "Cura",
    2: "Óbito por SRAG",
    3: "Óbito por outras causas",
    9: "Ignorado",
}

def criar_mapa_spark(mapa_dict):
    """Cria expressão MapType do Spark a partir de dict Python."""
    return F.create_map(
        *[val for pair in [(F.lit(k), F.lit(v)) for k, v in mapa_dict.items()] for val in pair]
    )

df_dim_class = (
    df_silver
    .select(
        F.col("classi_fin"),
        F.col("criterio"),
        F.col("evolucao"),
    )
    .distinct()
    .withColumn("descricao_classificacao_final",
        criar_mapa_spark(MAPA_CLASSI).getItem(F.col("classi_fin"))
    )
    .withColumn("descricao_criterio",
        criar_mapa_spark(MAPA_CRITERIO).getItem(F.col("criterio"))
    )
    .withColumn("descricao_evolucao",
        criar_mapa_spark(MAPA_EVOLUCAO).getItem(F.col("evolucao"))
    )
    .withColumn(
        "sk_classificacao",
        F.sha2(
            F.concat_ws("||",
                F.coalesce(F.col("classi_fin").cast("string"), F.lit("n/a")),
                F.coalesce(F.col("criterio").cast("string"), F.lit("n/a")),
                F.coalesce(F.col("evolucao").cast("string"), F.lit("n/a")),
            ), 256
        )
    )
    .withColumn("dh_ingestao_gold", F.lit(DH_INGESTAO_GOLD).cast("timestamp"))
)

(
    df_dim_class
    .write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(GOLD_DIM_CLASS)
)

print(f"gold_dim_classificacao gravada: {df_dim_class.count():,} classificações")

# COMMAND ----------

# [Célula 8] gold_fact — Fato SRAG com todas as FKs

df_dim_tempo_lookup = spark.table(GOLD_DIM_TEMPO).select("sk_tempo", "data")
df_dim_loc_lookup   = spark.table(GOLD_DIM_LOC).select("sk_localidade", "sg_uf", "co_mun_residencia")
df_dim_perf_lookup  = spark.table(GOLD_DIM_PERF).select("sk_perfil", "cs_sexo", "faixa_etaria", "cs_raca", "cs_gestant", "cs_escol_n")
df_dim_class_lookup = spark.table(GOLD_DIM_CLASS).select("sk_classificacao", "classi_fin", "criterio", "evolucao")

df_fact = (
    df_silver
    # Join dim_tempo (dt_sin_pri como data principal)
    .join(
        df_dim_tempo_lookup.withColumnRenamed("data", "dt_sin_pri_dim").withColumnRenamed("sk_tempo", "sk_tempo"),
        df_silver["dt_sin_pri"] == F.col("dt_sin_pri_dim"),
        "left"
    )
    # Join dim_localidade
    .join(
        df_dim_loc_lookup,
        [
            F.coalesce(df_silver[F.col("sg_uf_residencia").alias("sg_uf")], F.lit("n/a")) == F.coalesce(df_dim_loc_lookup["sg_uf"], F.lit("n/a")),
            F.coalesce(df_silver["co_mun_residencia"], F.lit("n/a")) == F.coalesce(df_dim_loc_lookup["co_mun_residencia"], F.lit("n/a"))
        ],
        "left"
    )
    # Join dim_perfil
    .join(
        df_dim_perf_lookup,
        [
            F.coalesce(df_silver["cs_sexo"], F.lit("I")) == F.coalesce(df_dim_perf_lookup["cs_sexo"], F.lit("I")),
            F.coalesce(df_silver["faixa_etaria"], F.lit("Ignorado")) == F.coalesce(df_dim_perf_lookup["faixa_etaria"], F.lit("Ignorado")),
        ],
        "left"
    )
    # Join dim_classificacao
    .join(
        df_dim_class_lookup,
        [
            df_silver["classi_fin"] == df_dim_class_lookup["classi_fin"],
            df_silver["criterio"] == df_dim_class_lookup["criterio"],
            df_silver["evolucao"] == df_dim_class_lookup["evolucao"],
        ],
        "left"
    )
    .select(
        # SK do caso
        F.sha2(F.coalesce(df_silver["hash_caso"], F.lit("n/a")), 256).alias("sk_caso"),
        df_silver["hash_caso"],
        # Foreign Keys
        F.col("sk_tempo"),
        F.col("sk_localidade"),
        F.col("sk_perfil"),
        F.col("sk_classificacao"),
        # Datas do caso
        df_silver["dt_sin_pri"],
        df_silver["dt_notific"],
        df_silver["dt_interna"],
        df_silver["dt_entuti"],
        df_silver["dt_saiduti"],
        df_silver["dt_evoluca"],
        # Todas as flags
        *[F.col(c) for c in df_silver.columns if c.startswith("flag_")],
        # Métricas numéricas
        df_silver["dias_uti"],
        df_silver["qtd_doses_covid_registradas"],
        # Auditoria
        df_silver["dh_ultima_atualizacao"],
        F.lit(DH_INGESTAO_GOLD).cast("timestamp").alias("dh_ingestao_gold"),
    )
)

(
    df_fact
    .write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(GOLD_FACT)
)

print(f"gold_fact gravada: {df_fact.count():,} casos")

# COMMAND ----------

# [Célula 9] Agregada Diária — com média móvel 7 dias

w_movel = Window.orderBy("data_referencia").rowsBetween(-6, 0)

df_agg_diaria = (
    df_silver
    .filter(F.col("dt_sin_pri").isNotNull())
    .groupBy(F.col("dt_sin_pri").alias("data_referencia"))
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
    )
    .withColumn(
        "taxa_mortalidade",
        F.round(
            F.col("total_obitos") / F.nullif(F.col("total_casos_com_evolucao"), F.lit(0)), 4
        )
    )
    .withColumn(
        "taxa_uso_uti",
        F.round(
            F.col("total_uti") / F.nullif(F.col("total_hospitalizados"), F.lit(0)), 4
        )
    )
    .withColumn(
        "taxa_vacinacao_gripe_registrada",
        F.round(
            F.col("total_vacinados_gripe") / F.nullif(F.col("total_info_vacina_gripe"), F.lit(0)), 4
        )
    )
    .withColumn(
        "taxa_vacinacao_covid_registrada",
        F.round(
            F.col("total_vacinados_covid") / F.nullif(F.col("total_info_vacina_covid"), F.lit(0)), 4
        )
    )
    .withColumn("media_movel_7d_casos", F.round(F.avg("total_casos").over(w_movel), 2))
    .withColumn("dh_ingestao_gold", F.lit(DH_INGESTAO_GOLD).cast("timestamp"))
    .orderBy("data_referencia")
)

(
    df_agg_diaria
    .write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(GOLD_AGG_DIARIA)
)

print(f"gold_agg_metricas_diarias gravada: {df_agg_diaria.count():,} datas")

# COMMAND ----------

# [Célula 10] Agregada Mensal

df_agg_mensal = (
    df_silver
    .filter(F.col("dt_sin_pri").isNotNull())
    .withColumn("ano",    F.year("dt_sin_pri"))
    .withColumn("mes",    F.month("dt_sin_pri"))
    .withColumn("ano_mes", F.date_format("dt_sin_pri", "yyyy-MM"))
    .groupBy("ano", "mes", "ano_mes")
    .agg(
        F.sum("flag_caso_srag").alias("total_casos"),
        F.sum("flag_obito_srag").alias("total_obitos"),
        F.sum("flag_uti").alias("total_uti"),
        F.sum("flag_hospitalizado").alias("total_hospitalizados"),
        F.sum("flag_info_vacina_gripe").alias("total_info_vacina_gripe"),
        F.sum("flag_vacinado_gripe").alias("total_vacinados_gripe"),
        F.sum("flag_info_vacina_covid").alias("total_info_vacina_covid"),
        F.sum("flag_vacinado_covid").alias("total_vacinados_covid"),
        F.sum("flag_evolucao_informada").alias("total_casos_com_evolucao"),
    )
    .withColumn("taxa_mortalidade",
        F.round(F.col("total_obitos") / F.nullif(F.col("total_casos_com_evolucao"), F.lit(0)), 4)
    )
    .withColumn("taxa_uso_uti",
        F.round(F.col("total_uti") / F.nullif(F.col("total_hospitalizados"), F.lit(0)), 4)
    )
    .withColumn("taxa_vacinacao_gripe_registrada",
        F.round(F.col("total_vacinados_gripe") / F.nullif(F.col("total_info_vacina_gripe"), F.lit(0)), 4)
    )
    .withColumn("taxa_vacinacao_covid_registrada",
        F.round(F.col("total_vacinados_covid") / F.nullif(F.col("total_info_vacina_covid"), F.lit(0)), 4)
    )
    .withColumn("dh_ingestao_gold", F.lit(DH_INGESTAO_GOLD).cast("timestamp"))
    .orderBy("ano", "mes")
)

(
    df_agg_mensal
    .write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(GOLD_AGG_MENSAL)
)

print(f"gold_agg_metricas_mensais gravada: {df_agg_mensal.count():,} meses")

# COMMAND ----------

# [Célula 11] Indicadores do Relatório — KPIs do Agente

data_maxima = df_silver.filter(F.col("dt_sin_pri").isNotNull()) \
    .select(F.max("dt_sin_pri")).collect()[0][0]

hoje_str = str(hoje)

# Janelas temporais
ultimos_7_dias      = (F.col("data_referencia") >= F.date_sub(F.to_date(F.lit(hoje_str)), 7))
sete_dias_anteriores = (
    (F.col("data_referencia") >= F.date_sub(F.to_date(F.lit(hoje_str)), 14)) &
    (F.col("data_referencia") < F.date_sub(F.to_date(F.lit(hoje_str)), 7))
)
ultimos_30_dias = (F.col("data_referencia") >= F.date_sub(F.to_date(F.lit(hoje_str)), 30))

df_agg_diaria_cached = spark.table(GOLD_AGG_DIARIA)

casos_7d   = df_agg_diaria_cached.filter(ultimos_7_dias).agg(F.sum("total_casos")).collect()[0][0] or 0
casos_7d_ant = df_agg_diaria_cached.filter(sete_dias_anteriores).agg(F.sum("total_casos")).collect()[0][0] or 0

row_30d = df_agg_diaria_cached.filter(ultimos_30_dias).agg(
    F.sum("total_casos").alias("casos_30d"),
    F.sum("total_obitos").alias("obitos_30d"),
    F.sum("total_casos_com_evolucao").alias("evolucao_30d"),
    F.sum("total_uti").alias("uti_30d"),
    F.sum("total_hospitalizados").alias("hosp_30d"),
    F.sum("total_info_vacina_gripe").alias("info_gripe_30d"),
    F.sum("total_vacinados_gripe").alias("vac_gripe_30d"),
    F.sum("total_info_vacina_covid").alias("info_covid_30d"),
    F.sum("total_vacinados_covid").alias("vac_covid_30d"),
).collect()[0]

# Taxa de aumento 7 dias
taxa_aumento_7d = (
    (casos_7d - casos_7d_ant) / casos_7d_ant
    if casos_7d_ant > 0 else None
)

# Observação de confiabilidade
defasagem_dias_max = (hoje - data_maxima).days if data_maxima else 999
obs_conf = (
    "ATENÇÃO: Data máxima do dataset está a mais de 30 dias. Métricas recentes podem estar subestimadas por atraso de digitação."
    if defasagem_dias_max > 30
    else f"Dataset com data máxima a {defasagem_dias_max} dias. Métricas recentes podem ter atraso de digitação típico do SIVEP-Gripe."
)

df_indicadores = spark.createDataFrame([{
    "data_referencia":              str(hoje),
    "casos_ultimos_7_dias":         int(casos_7d),
    "casos_7_dias_anteriores":      int(casos_7d_ant),
    "taxa_aumento_casos_7d":        float(taxa_aumento_7d) if taxa_aumento_7d is not None else None,
    "casos_ultimos_30_dias":        int(row_30d["casos_30d"] or 0),
    "obitos_ultimos_30_dias":       int(row_30d["obitos_30d"] or 0),
    "taxa_mortalidade_30d":         float((row_30d["obitos_30d"] or 0) / max(row_30d["evolucao_30d"] or 1, 1)),
    "taxa_uso_uti_30d":             float((row_30d["uti_30d"] or 0) / max(row_30d["hosp_30d"] or 1, 1)),
    "taxa_vacinacao_gripe_30d":     float((row_30d["vac_gripe_30d"] or 0) / max(row_30d["info_gripe_30d"] or 1, 1)),
    "taxa_vacinacao_covid_30d":     float((row_30d["vac_covid_30d"] or 0) / max(row_30d["info_covid_30d"] or 1, 1)),
    "data_maxima_dataset":          str(data_maxima) if data_maxima else "Não disponível",
    "data_execucao":                str(DH_INGESTAO_GOLD),
    "observacao_confiabilidade":    obs_conf,
    "dh_ingestao_gold":             DH_INGESTAO_GOLD,
}])

(
    df_indicadores
    .write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(GOLD_AGG_IND)
)

print("gold_agg_indicadores_relatorio gravada.")
print(f"  Taxa aumento 7d  : {taxa_aumento_7d:.2%}" if taxa_aumento_7d else "  Taxa aumento 7d: N/A (sem período anterior)")
print(f"  Casos últimos 7d : {casos_7d:,}")
print(f"  Data máxima DS   : {data_maxima}")
print("=" * 60)
print("GOLD COMPLETA — SRAG DATASUS")
print("=" * 60)

dbutils.notebook.exit(f"SUCESSO|run_id={RUN_ID}|gold_modelagem")
