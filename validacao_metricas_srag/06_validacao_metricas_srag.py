# Databricks notebook source
# MAGIC %md
# MAGIC # 06 · Validação de Métricas — SRAG DATASUS
# MAGIC
# MAGIC **Objetivo:** Validar as métricas calculadas e exibir resumo analítico  
# MAGIC **Uso:** Conferência pós-pipeline antes de disponibilizar ao agente

# COMMAND ----------

import uuid
from pyspark.sql import functions as F
import matplotlib.pyplot as plt
import pandas as pd
from datetime import date, timedelta

RUN_ID   = str(uuid.uuid4())
CATALOGO = "certificacao_indicium"
SCHEMA   = "poc_srag_datasus"

GOLD_AGG_DIARIA  = f"{CATALOGO}.{SCHEMA}.gold_agg_poc_srag_metricas_diarias"
GOLD_AGG_MENSAL  = f"{CATALOGO}.{SCHEMA}.gold_agg_poc_srag_metricas_mensais"
GOLD_AGG_IND     = f"{CATALOGO}.{SCHEMA}.gold_agg_poc_srag_indicadores_relatorio"
GOLD_FACT        = f"{CATALOGO}.{SCHEMA}.gold_fact_poc_srag_datasus"
GOLD_QUALITY     = f"{CATALOGO}.{SCHEMA}.gold_quality_poc_srag_datasus"

# COMMAND ----------

# MAGIC %md
# MAGIC ##  Indicadores Consolidados (Agente)

# COMMAND ----------

df_ind = spark.table(GOLD_AGG_IND)
display(df_ind)

# COMMAND ----------

# MAGIC %md
# MAGIC ##  Série Diária — Últimos 30 Dias

# COMMAND ----------

hoje = date.today()
data_30d = hoje - timedelta(days=30)

df_diaria = (
    spark.table(GOLD_AGG_DIARIA)
    .filter(F.col("data_referencia") >= F.lit(str(data_30d)))
    .orderBy("data_referencia")
    .toPandas()
)

if not df_diaria.empty:
    fig, axes = plt.subplots(2, 2, figsize=(16, 10))
    fig.suptitle("SRAG — Série Diária (Últimos 30 dias)", fontsize=16, fontweight="bold")

    # Gráfico 1 — Total de Casos e Média Móvel
    ax1 = axes[0, 0]
    ax1.bar(df_diaria["data_referencia"], df_diaria["total_casos"], alpha=0.6, label="Casos", color="#4A90D9")
    ax1.plot(df_diaria["data_referencia"], df_diaria["media_movel_7d_casos"], color="red", linewidth=2, label="Média Móvel 7d")
    ax1.set_title("Casos Diários + Média Móvel 7d")
    ax1.set_xlabel("Data")
    ax1.set_ylabel("Casos")
    ax1.legend()
    ax1.tick_params(axis="x", rotation=45)

    # Gráfico 2 — Óbitos
    ax2 = axes[0, 1]
    ax2.bar(df_diaria["data_referencia"], df_diaria["total_obitos"], alpha=0.8, color="#E74C3C", label="Óbitos")
    ax2.set_title("Óbitos Diários")
    ax2.set_xlabel("Data")
    ax2.set_ylabel("Óbitos")
    ax2.tick_params(axis="x", rotation=45)

    # Gráfico 3 — Taxa de Mortalidade
    ax3 = axes[1, 0]
    ax3.plot(df_diaria["data_referencia"], df_diaria["taxa_mortalidade"] * 100, color="#F39C12", linewidth=2)
    ax3.fill_between(df_diaria["data_referencia"], df_diaria["taxa_mortalidade"] * 100, alpha=0.2, color="#F39C12")
    ax3.set_title("Taxa de Mortalidade (%) — casos com evolução informada")
    ax3.set_xlabel("Data")
    ax3.set_ylabel("Taxa (%)")
    ax3.tick_params(axis="x", rotation=45)

    # Gráfico 4 — Taxa de UTI
    ax4 = axes[1, 1]
    ax4.plot(df_diaria["data_referencia"], df_diaria["taxa_uso_uti"] * 100, color="#8E44AD", linewidth=2)
    ax4.fill_between(df_diaria["data_referencia"], df_diaria["taxa_uso_uti"] * 100, alpha=0.2, color="#8E44AD")
    ax4.set_title("Taxa de Uso de UTI (%) — entre hospitalizados")
    ax4.set_xlabel("Data")
    ax4.set_ylabel("Taxa (%)")
    ax4.tick_params(axis="x", rotation=45)

    plt.tight_layout()
    display(fig)
    plt.close()
else:
    print("Sem dados na série diária dos últimos 30 dias.")

# COMMAND ----------

# MAGIC %md
# MAGIC ##  Série Mensal — Últimos 12 Meses

# COMMAND ----------

from datetime import datetime
mes_12 = (datetime.today() - pd.DateOffset(months=12)).strftime("%Y-%m")

df_mensal = (
    spark.table(GOLD_AGG_MENSAL)
    .filter(F.col("ano_mes") >= F.lit(mes_12))
    .orderBy("ano", "mes")
    .toPandas()
)

if not df_mensal.empty:
    fig2, ax = plt.subplots(figsize=(14, 6))
    bars = ax.bar(df_mensal["ano_mes"], df_mensal["total_casos"], color="#4A90D9", alpha=0.8, label="Casos")
    ax2b = ax.twinx()
    ax2b.plot(df_mensal["ano_mes"], df_mensal["total_obitos"], color="red", linewidth=2, marker="o", label="Óbitos")
    ax.set_title("SRAG — Casos e Óbitos Mensais (Últimos 12 Meses)", fontsize=14, fontweight="bold")
    ax.set_xlabel("Mês/Ano")
    ax.set_ylabel("Total de Casos", color="#4A90D9")
    ax2b.set_ylabel("Total de Óbitos", color="red")
    ax.tick_params(axis="x", rotation=45)
    ax.legend(loc="upper left")
    ax2b.legend(loc="upper right")
    plt.tight_layout()
    display(fig2)
    plt.close()

# COMMAND ----------

# MAGIC %md
# MAGIC ##  Qualidade Consolidada

# COMMAND ----------

df_q = spark.table(GOLD_QUALITY).orderBy("severidade", "percentual_invalidade", ascending=[True, False])
display(df_q.filter(F.col("registros_invalidos") > 0))

# COMMAND ----------

# MAGIC %md
# MAGIC ##  Checklist de Validação

# COMMAND ----------

ind = spark.table(GOLD_AGG_IND).collect()[0]
print("=" * 70)
print("VALIDAÇÃO DE MÉTRICAS — SRAG DATASUS")
print("=" * 70)
print(f"Data execução           : {ind['data_execucao']}")
print(f"Data máxima do dataset  : {ind['data_maxima_dataset']}")
print()
print(f"Casos últimos 7 dias    : {ind['casos_ultimos_7_dias']:,}")
print(f"Casos 7 dias anteriores : {ind['casos_7_dias_anteriores']:,}")
taxa_7d = ind['taxa_aumento_casos_7d']
print(f"Taxa aumento 7d         : {f'{taxa_7d:.2%}' if taxa_7d is not None else 'N/A'}")
print()
print(f"Casos últimos 30 dias   : {ind['casos_ultimos_30_dias']:,}")
print(f"Óbitos últimos 30 dias  : {ind['obitos_ultimos_30_dias']:,}")
print(f"Taxa mortalidade 30d    : {ind['taxa_mortalidade_30d']:.2%}")
print(f"Taxa uso UTI 30d        : {ind['taxa_uso_uti_30d']:.2%}")
print(f"Taxa vac. gripe 30d     : {ind['taxa_vacinacao_gripe_30d']:.2%}")
print(f"Taxa vac. COVID 30d     : {ind['taxa_vacinacao_covid_30d']:.2%}")
print()
print(f"Observação confiab.     : {ind['observacao_confiabilidade']}")
print("=" * 70)
print()
print("DISCLAIMER OBRIGATÓRIO:")
print("• Taxa de mortalidade: calculada sobre casos com evolução informada (pode subestimar).")
print("• Taxa de UTI: proporção de internados com registro UTI (não reflete capacidade de leitos).")
print("• Taxas de vacinação: proporção entre casos SRAG (não representa cobertura populacional).")
print("• Dataset batch: não é streaming em tempo real. Atrasos de digitação são esperados.")
