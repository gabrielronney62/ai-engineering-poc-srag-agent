# Databricks notebook source
# MAGIC %md
# MAGIC # 08 — Dashboard: Visualizações Epidemiológicas SRAG
# MAGIC
# MAGIC **PoC:** HealthCare Indicium — Monitoramento de SRAG
# MAGIC **Objetivo:** Gráficos interativos das métricas epidemiológicas, 100% Databricks-native
# MAGIC
# MAGIC ---
# MAGIC ### Gráficos gerados
# MAGIC
# MAGIC | # | Gráfico | Fonte | Requisito |
# MAGIC |---|---|---|---|
# MAGIC | 1 | Casos diários + Média Móvel 7d (últimos 30 dias) | `gold_agg_poc_srag_metricas_diarias` | ✅ Obrigatório |
# MAGIC | 2 | Casos mensais + Óbitos (últimos 12 meses) | `gold_agg_poc_srag_metricas_mensais` | ✅ Obrigatório |
# MAGIC | 3 | Painel de KPIs — 4 métricas consolidadas | `gold_agg_poc_srag_indicadores_relatorio` | ✅ Obrigatório |
# MAGIC | 4 | Taxa de mortalidade diária | `gold_agg_poc_srag_metricas_diarias` | Extra |
# MAGIC | 5 | Distribuição por agente etiológico | `gold_agg_poc_srag_metricas_mensais` | Extra |
# MAGIC | 6 | Heatmap de casos por UF e mês | `gold_fact_poc_srag_datasus` | Extra |
# MAGIC
# MAGIC ---
# MAGIC ### Como usar
# MAGIC - Execute **Run All** para gerar todos os gráficos de uma vez
# MAGIC - Os gráficos são gerados com `matplotlib` + `seaborn` — nativos no Databricks
# MAGIC - Para adicionar ao Databricks Dashboard: Workspace → seu notebook → (+) Add to Dashboard

# COMMAND ----------

# MAGIC %md
# MAGIC ## [Célula 1] Setup e Dependências

# COMMAND ----------

import matplotlib
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import matplotlib.gridspec as gridspec
from matplotlib.ticker import FuncFormatter, MaxNLocator
import matplotlib.dates as mdates
import seaborn as sns
import pandas as pd
import numpy as np
from pyspark.sql import functions as F
from datetime import date, timedelta
from dateutil.relativedelta import relativedelta
import warnings
warnings.filterwarnings("ignore")

# ── Paleta de cores HealthCare Gabriel Ronney ──────────────────────────────────
# Cores clínicas com acessibilidade (WCAG AA)
COR_PRIMARIA     = "#1A6B8A"   # Azul-teal médico
COR_PERIGO       = "#D64B4B"   # Vermelho-coral (óbitos, alertas)
COR_ALERTA       = "#E8952C"   # Âmbar (atenção, UTI)
COR_SUCESSO      = "#2E8B57"   # Verde-floresta (vacinação, cura)
COR_ROXO         = "#6B4E9B"   # Roxo (classificações)
COR_CINZA        = "#7F8C8D"   # Cinza neutro
COR_FUNDO        = "#F8FAFB"   # Fundo quase branco
COR_FUNDO_CARD   = "#FFFFFF"
COR_GRID         = "#E8EDF0"
COR_TEXTO        = "#1C2B36"
COR_TEXTO_LEVE   = "#546E7A"
COR_MM7          = "#E84393"   # Rosa vibrante para média móvel

# ── Estilo global ──────────────────────────────────────────────────────────────
plt.rcParams.update({
    "figure.facecolor":    COR_FUNDO,
    "axes.facecolor":      COR_FUNDO_CARD,
    "axes.edgecolor":      COR_GRID,
    "axes.labelcolor":     COR_TEXTO_LEVE,
    "axes.titlecolor":     COR_TEXTO,
    "axes.titlesize":      12,
    "axes.titleweight":    "bold",
    "axes.labelsize":      9,
    "axes.grid":           True,
    "grid.color":          COR_GRID,
    "grid.linewidth":      0.7,
    "grid.alpha":          0.8,
    "xtick.color":         COR_TEXTO_LEVE,
    "ytick.color":         COR_TEXTO_LEVE,
    "xtick.labelsize":     8,
    "ytick.labelsize":     8,
    "legend.fontsize":     8,
    "legend.framealpha":   0.9,
    "legend.edgecolor":    COR_GRID,
    "font.family":         "DejaVu Sans",
    "figure.dpi":          120,
})

# ── Catálogo e tabelas ─────────────────────────────────────────────────────────
CATALOGO = "certificacao_indicium"
SCHEMA   = "poc_srag_datasus"
T_DIARIA = f"{CATALOGO}.{SCHEMA}.gold_agg_poc_srag_metricas_diarias"
T_MENSAL = f"{CATALOGO}.{SCHEMA}.gold_agg_poc_srag_metricas_mensais"
T_IND    = f"{CATALOGO}.{SCHEMA}.gold_agg_poc_srag_indicadores_relatorio"
T_FACT   = f"{CATALOGO}.{SCHEMA}.gold_fact_poc_srag_datasus"
T_DIM_L  = f"{CATALOGO}.{SCHEMA}.gold_dim_localidade_poc_srag_datasus"

# ── Formatadores ──────────────────────────────────────────────────────────────
fmt_mil   = FuncFormatter(lambda x, _: f"{x:,.0f}")
fmt_pct   = FuncFormatter(lambda x, _: f"{x:.1f}%")
fmt_k     = FuncFormatter(lambda x, _: f"{x/1000:.1f}k" if x >= 1000 else f"{x:.0f}")

print("✅ Setup concluído — paleta HealthCare Gabriel Ronney carregada.")
print(f"   Matplotlib: {matplotlib.__version__} | Seaborn: {sns.__version__}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## [Célula 2] Carregar dados das tabelas Gold

# COMMAND ----------

# ── Indicadores consolidados ───────────────────────────────────────────────────
row_ind = spark.table(T_IND).orderBy(F.col("data_execucao").desc()).limit(1).first()

DATA_REF        = row_ind["data_maxima_dataset"]
DEFASAGEM       = row_ind["defasagem_dias"]
CASOS_7D        = row_ind["casos_ultimos_7_dias"]
CASOS_7D_ANT    = row_ind["casos_7_dias_anteriores"]
TAXA_AUMENTO    = row_ind["taxa_aumento_casos_7d"]
CASOS_30D       = row_ind["casos_ultimos_30_dias"]
OBITOS_30D      = row_ind["obitos_ultimos_30_dias"]
TAXA_MORT       = row_ind["taxa_mortalidade_30d"]
TAXA_UTI        = row_ind["taxa_uso_uti_30d"]
TAXA_VAC_GRIPE  = row_ind["taxa_vacinacao_gripe_30d"]
TAXA_VAC_COVID  = row_ind["taxa_vacinacao_covid_30d"]
OBS_CONF        = row_ind["observacao_confiabilidade"]

# ── Série diária — últimos 30 dias ────────────────────────────────────────────
df_diaria = (
    spark.table(T_DIARIA)
    .filter(
        (F.col("data_referencia") >= F.date_sub(F.lit(DATA_REF), 29)) &
        (F.col("data_referencia") <= F.lit(DATA_REF))
    )
    .orderBy("data_referencia")
    .toPandas()
)
df_diaria["data_referencia"] = pd.to_datetime(df_diaria["data_referencia"])

# ── Série mensal — últimos 12 meses ───────────────────────────────────────────
data_12m = str(DATA_REF)[:7]   # YYYY-MM
# Calcular 12 meses antes
from dateutil.relativedelta import relativedelta as rd
data_12m_inicio = (pd.to_datetime(str(DATA_REF)) - rd(months=11)).strftime("%Y-%m")

df_mensal = (
    spark.table(T_MENSAL)
    .filter(F.col("ano_mes") >= F.lit(data_12m_inicio))
    .orderBy("ano", "mes")
    .toPandas()
)

# ── Distribuição por agente etiológico (soma acumulada) ───────────────────────
row_agente = spark.table(T_MENSAL).filter(
    F.col("ano_mes") >= F.lit(data_12m_inicio)
).agg(
    F.sum("total_covid").alias("covid"),
    F.sum("total_influenza").alias("influenza"),
    F.sum("total_outro_virus").alias("outro_virus"),
    F.sum("total_srag_nao_especificado").alias("srag_nao_esp"),
).first()

# ── Heatmap: casos por UF + mês ───────────────────────────────────────────────
df_uf_mes = (
    spark.table(T_FACT)
    .filter(F.col("dt_sin_pri").isNotNull())
    .withColumn("ano_mes", F.date_format("dt_sin_pri", "yyyy-MM"))
    .join(
        spark.table(T_DIM_L).select("sk_localidade", "sg_uf"),
        "sk_localidade", "left"
    )
    .filter(F.col("sg_uf").isNotNull())
    .filter(F.col("ano_mes") >= F.lit(data_12m_inicio))
    .groupBy("sg_uf", "ano_mes")
    .agg(F.sum("flag_caso_srag").alias("casos"))
    .toPandas()
)

print(f"✅ Dados carregados:")
print(f"   Série diária  : {len(df_diaria):,} dias")
print(f"   Série mensal  : {len(df_mensal):,} meses")
print(f"   Data referência: {DATA_REF} (defasagem: {DEFASAGEM} dias)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## [Célula 3] Gráfico 1 — Painel de KPIs (4 métricas obrigatórias)

# COMMAND ----------

fig_kpi, ax = plt.subplots(figsize=(16, 3.5))
ax.set_axis_off()
fig_kpi.patch.set_facecolor(COR_FUNDO)

# Título do painel
fig_kpi.text(0.5, 0.96, "SRAG — Painel de Indicadores Epidemiológicos",
             ha="center", va="top", fontsize=16, fontweight="bold",
             color=COR_TEXTO)
fig_kpi.text(0.5, 0.84,
             f"Data de referência: {DATA_REF} | Defasagem: {DEFASAGEM} dias | {OBS_CONF}",
             ha="center", va="top", fontsize=8, color=COR_TEXTO_LEVE)

# ── 4 cards de KPI ────────────────────────────────────────────────────────────
CARDS = [
    {
        "titulo":    "TAXA DE AUMENTO DE CASOS",
        "subtitulo": "Variação 7 dias vs 7 dias anteriores",
        "valor":     f"{TAXA_AUMENTO:+.1%}" if TAXA_AUMENTO is not None else "N/A",
        "detalhe":   f"{CASOS_7D:,} casos (7d) vs {CASOS_7D_ANT:,} (7d ant.)",
        "cor_borda": COR_PERIGO if (TAXA_AUMENTO or 0) > 0.10 else COR_ALERTA if (TAXA_AUMENTO or 0) > 0 else COR_SUCESSO,
        "icone":     "📈" if (TAXA_AUMENTO or 0) > 0 else "📉",
    },
    {
        "titulo":    "TAXA DE MORTALIDADE",
        "subtitulo": "Óbitos / casos com evolução informada (30d)",
        "valor":     f"{TAXA_MORT:.1%}" if TAXA_MORT is not None else "N/A",
        "detalhe":   f"{OBITOS_30D:,} óbitos | {CASOS_30D:,} casos (30d)",
        "cor_borda": COR_PERIGO,
        "icone":     "🏥",
    },
    {
        "titulo":    "TAXA DE USO DE UTI",
        "subtitulo": "Hospitalizados SRAG que usaram UTI (30d)",
        "valor":     f"{TAXA_UTI:.1%}" if TAXA_UTI is not None else "N/A",
        "detalhe":   "⚠️ Não representa ocupação real de leitos",
        "cor_borda": COR_ALERTA,
        "icone":     "🫁",
    },
    {
        "titulo":    "VACINAÇÃO REGISTRADA",
        "subtitulo": "Casos SRAG vacinados contra gripe (30d)",
        "valor":     f"{TAXA_VAC_GRIPE:.1%}" if TAXA_VAC_GRIPE is not None else "N/A",
        "detalhe":   f"COVID: {TAXA_VAC_COVID:.1%}" if TAXA_VAC_COVID else "COVID: N/A",
        "cor_borda": COR_SUCESSO,
        "icone":     "💉",
    },
]

for i, card in enumerate(CARDS):
    x_left  = 0.02 + i * 0.245
    x_right = x_left + 0.235
    # Fundo do card
    rect = mpatches.FancyBboxPatch(
        (x_left, 0.05), x_right - x_left, 0.68,
        boxstyle="round,pad=0.01",
        linewidth=2.5,
        edgecolor=card["cor_borda"],
        facecolor=COR_FUNDO_CARD,
        transform=fig_kpi.transFigure,
        figure=fig_kpi,
    )
    fig_kpi.add_artist(rect)
    # Barra colorida superior
    barra = mpatches.FancyBboxPatch(
        (x_left, 0.73), x_right - x_left, 0.055,
        boxstyle="round,pad=0.005",
        facecolor=card["cor_borda"], edgecolor="none",
        transform=fig_kpi.transFigure, figure=fig_kpi,
    )
    fig_kpi.add_artist(barra)
    cx = (x_left + x_right) / 2
    fig_kpi.text(cx, 0.76, f"{card['icone']} {card['titulo']}",
                 ha="center", va="center", fontsize=7.5,
                 fontweight="bold", color="white",
                 transform=fig_kpi.transFigure)
    fig_kpi.text(cx, 0.59, card["valor"],
                 ha="center", va="center", fontsize=26,
                 fontweight="bold", color=card["cor_borda"],
                 transform=fig_kpi.transFigure)
    fig_kpi.text(cx, 0.42, card["subtitulo"],
                 ha="center", va="center", fontsize=7,
                 color=COR_TEXTO_LEVE, style="italic",
                 transform=fig_kpi.transFigure)
    fig_kpi.text(cx, 0.14, card["detalhe"],
                 ha="center", va="center", fontsize=7,
                 color=COR_TEXTO_LEVE,
                 transform=fig_kpi.transFigure)

plt.tight_layout(pad=0)
display(fig_kpi)
plt.close()
print("✅ Painel de KPIs gerado.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## [Célula 4] Gráfico 2 — Casos Diários + Média Móvel 7d (OBRIGATÓRIO)

# COMMAND ----------

fig_diario, (ax_casos, ax_taxa) = plt.subplots(
    2, 1, figsize=(16, 9), sharex=True,
    gridspec_kw={"height_ratios": [3, 1.2], "hspace": 0.08}
)
fig_diario.patch.set_facecolor(COR_FUNDO)

datas  = df_diaria["data_referencia"]
casos  = df_diaria["total_casos"].fillna(0)
obitos = df_diaria["total_obitos"].fillna(0)
mm7    = df_diaria["media_movel_7d_casos"].fillna(0)
tx_m   = df_diaria["taxa_mortalidade"].fillna(0) * 100
tx_uti = df_diaria["taxa_uso_uti"].fillna(0) * 100

# ── Subplot superior: barras de casos + média móvel ───────────────────────────
bars_casos = ax_casos.bar(
    datas, casos,
    color=COR_PRIMARIA, alpha=0.65, width=0.85,
    label="Casos diários", zorder=2
)
ax_casos.bar(
    datas, obitos,
    color=COR_PERIGO, alpha=0.85, width=0.85,
    label="Óbitos diários", zorder=3
)
ax_casos.plot(
    datas, mm7,
    color=COR_MM7, linewidth=2.5, zorder=5,
    label="Média móvel 7d", marker="o",
    markersize=3.5, markerfacecolor=COR_MM7,
    markeredgecolor="white", markeredgewidth=0.8
)

# Anotação do pico
idx_pico = casos.idxmax()
ax_casos.annotate(
    f"Pico: {int(casos[idx_pico]):,}",
    xy=(datas[idx_pico], casos[idx_pico]),
    xytext=(0, 14), textcoords="offset points",
    ha="center", fontsize=7.5, fontweight="bold",
    color=COR_PRIMARIA,
    arrowprops=dict(arrowstyle="-", color=COR_PRIMARIA, lw=1),
)

ax_casos.set_title("Número Diário de Casos SRAG — Últimos 30 Dias",
                   fontsize=13, fontweight="bold", pad=12, color=COR_TEXTO)
ax_casos.set_ylabel("Casos", fontsize=9, color=COR_TEXTO_LEVE)
ax_casos.yaxis.set_major_formatter(fmt_mil)
ax_casos.legend(loc="upper left", framealpha=0.95)
ax_casos.set_facecolor(COR_FUNDO_CARD)
ax_casos.spines[["top","right"]].set_visible(False)

# Texto de disclaimer
ax_casos.text(
    0.99, 0.97,
    f"Ref.: {DATA_REF} | Defasagem: {DEFASAGEM}d",
    transform=ax_casos.transAxes,
    ha="right", va="top", fontsize=7, color=COR_TEXTO_LEVE,
    style="italic"
)

# ── Subplot inferior: taxa de mortalidade e UTI ────────────────────────────────
ax_taxa.plot(datas, tx_m,  color=COR_PERIGO, linewidth=1.8,
             label="Mortalidade (%)", alpha=0.9)
ax_taxa.fill_between(datas, tx_m, alpha=0.12, color=COR_PERIGO)
ax_taxa.plot(datas, tx_uti, color=COR_ALERTA, linewidth=1.8,
             label="Uso UTI (%)", linestyle="--", alpha=0.9)

ax_taxa.set_ylabel("Taxa (%)", fontsize=9, color=COR_TEXTO_LEVE)
ax_taxa.yaxis.set_major_formatter(fmt_pct)
ax_taxa.legend(loc="upper left", framealpha=0.95, fontsize=7.5)
ax_taxa.set_facecolor(COR_FUNDO_CARD)
ax_taxa.spines[["top","right"]].set_visible(False)

# Formatação do eixo X
ax_taxa.xaxis.set_major_formatter(mdates.DateFormatter("%d/%m"))
ax_taxa.xaxis.set_major_locator(mdates.DayLocator(interval=3))
plt.setp(ax_taxa.xaxis.get_majorticklabels(), rotation=45, ha="right")
ax_taxa.set_xlabel("Data dos primeiros sintomas", fontsize=9, color=COR_TEXTO_LEVE)

# Rodapé com limitações
fig_diario.text(
    0.5, -0.02,
    "⚠️  Taxa de mortalidade calculada sobre casos com evolução informada | "
    "Taxa UTI: uso entre hospitalizados SRAG (≠ ocupação real de leitos)",
    ha="center", fontsize=7, color=COR_TEXTO_LEVE, style="italic"
)

plt.tight_layout()
display(fig_diario)
plt.close()
print("✅ Gráfico diário gerado.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## [Célula 5] Gráfico 3 — Casos e Óbitos Mensais (OBRIGATÓRIO)

# COMMAND ----------

fig_mensal, ax_m = plt.subplots(figsize=(16, 7))
fig_mensal.patch.set_facecolor(COR_FUNDO)
ax_m.set_facecolor(COR_FUNDO_CARD)

meses      = df_mensal["ano_mes"]
x_pos      = np.arange(len(meses))
casos_m    = df_mensal["total_casos"].fillna(0)
obitos_m   = df_mensal["total_obitos"].fillna(0)
hosp_m     = df_mensal["total_hospitalizados"].fillna(0)

bar_width = 0.55

# Barras empilhadas: hospitalizados no fundo, não-hospitalizados em cima
bars_hosp = ax_m.bar(
    x_pos, hosp_m,
    width=bar_width, color=COR_PRIMARIA, alpha=0.85,
    label="Hospitalizados", zorder=2
)
nao_hosp = casos_m - hosp_m
ax_m.bar(
    x_pos, nao_hosp, bottom=hosp_m,
    width=bar_width, color="#A8D4E8", alpha=0.75,
    label="Não hospitalizados", zorder=2
)

# Linha de óbitos no eixo secundário
ax_m2 = ax_m.twinx()
ax_m2.plot(
    x_pos, obitos_m,
    color=COR_PERIGO, linewidth=2.5, zorder=5,
    marker="D", markersize=5,
    markerfacecolor=COR_PERIGO, markeredgecolor="white",
    markeredgewidth=1, label="Óbitos"
)
ax_m2.fill_between(x_pos, obitos_m, alpha=0.08, color=COR_PERIGO)
ax_m2.set_ylabel("Óbitos", fontsize=9, color=COR_PERIGO)
ax_m2.tick_params(axis="y", labelcolor=COR_PERIGO)
ax_m2.yaxis.set_major_formatter(fmt_mil)
ax_m2.spines["right"].set_edgecolor(COR_PERIGO)

# Anotação de valores nas barras (apenas nos meses com dados expressivos)
for i, (v, o) in enumerate(zip(casos_m, obitos_m)):
    if v > 0:
        ax_m.text(i, v + casos_m.max() * 0.012,
                  f"{int(v):,}", ha="center", fontsize=6.5,
                  color=COR_TEXTO_LEVE, fontweight="bold")

ax_m.set_title("Casos e Óbitos Mensais por SRAG — Últimos 12 Meses",
               fontsize=13, fontweight="bold", pad=12, color=COR_TEXTO)
ax_m.set_xlabel("Mês/Ano dos primeiros sintomas", fontsize=9, color=COR_TEXTO_LEVE)
ax_m.set_ylabel("Total de Casos", fontsize=9, color=COR_PRIMARIA)
ax_m.set_xticks(x_pos)
ax_m.set_xticklabels(meses, rotation=45, ha="right", fontsize=8)
ax_m.yaxis.set_major_formatter(fmt_mil)
ax_m.tick_params(axis="y", labelcolor=COR_PRIMARIA)
ax_m.spines[["top"]].set_visible(False)
ax_m.spines["left"].set_edgecolor(COR_PRIMARIA)

# Legendas unificadas
h1, l1 = ax_m.get_legend_handles_labels()
h2, l2 = ax_m2.get_legend_handles_labels()
ax_m.legend(h1 + h2, l1 + l2, loc="upper left", framealpha=0.95)

fig_mensal.text(
    0.5, -0.03,
    "⚠️  Dados batch — atualização periódica via DATASUS. "
    "Atraso de digitação de 7-30 dias esperado para meses recentes.",
    ha="center", fontsize=7, color=COR_TEXTO_LEVE, style="italic"
)

plt.tight_layout()
display(fig_mensal)
plt.close()
print("✅ Gráfico mensal gerado.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## [Célula 6] Gráfico 4 — Distribuição por Agente Etiológico (12 meses)

# COMMAND ----------

fig_agente, (ax_pie, ax_bar_ag) = plt.subplots(1, 2, figsize=(16, 6))
fig_agente.patch.set_facecolor(COR_FUNDO)

# Dados por agente
agentes = {
    "COVID-19":              int(row_agente["covid"]      or 0),
    "Influenza":             int(row_agente["influenza"]  or 0),
    "Outro Vírus":           int(row_agente["outro_virus"] or 0),
    "SRAG Não Especificado": int(row_agente["srag_nao_esp"] or 0),
}
agentes = {k: v for k, v in agentes.items() if v > 0}

cores_agentes = [COR_PERIGO, COR_PRIMARIA, COR_ALERTA, COR_CINZA][:len(agentes)]

# ── Donut ────────────────────────────────────────────────────────────────────
wedges, texts, autotexts = ax_pie.pie(
    list(agentes.values()),
    labels=None,
    colors=cores_agentes,
    autopct=lambda p: f"{p:.1f}%" if p > 3 else "",
    startangle=90,
    pctdistance=0.78,
    wedgeprops=dict(width=0.55, edgecolor="white", linewidth=2),
)
for at in autotexts:
    at.set_fontsize(9)
    at.set_fontweight("bold")
    at.set_color("white")

# Círculo interno — centro do donut
total_agentes = sum(agentes.values())
circulo = plt.Circle((0, 0), 0.45, color=COR_FUNDO_CARD)
ax_pie.add_artist(circulo)
ax_pie.text(0, 0.08, f"{total_agentes:,}", ha="center", va="center",
            fontsize=16, fontweight="bold", color=COR_TEXTO)
ax_pie.text(0, -0.18, "casos", ha="center", va="center",
            fontsize=9, color=COR_TEXTO_LEVE)

ax_pie.legend(
    wedges, list(agentes.keys()),
    loc="lower center", bbox_to_anchor=(0.5, -0.12),
    ncol=2, fontsize=8, framealpha=0.9
)
ax_pie.set_title("Distribuição por Agente Etiológico\n(Últimos 12 meses)",
                 fontsize=12, fontweight="bold", color=COR_TEXTO, pad=10)
ax_pie.set_facecolor(COR_FUNDO_CARD)

# ── Barras horizontais com taxa de mortalidade por agente ────────────────────
ag_data = (
    spark.table(f"{CATALOGO}.{SCHEMA}.gold_agg_poc_srag_metricas_mensais")
    .filter(F.col("ano_mes") >= F.lit(data_12m_inicio))
    .agg(
        F.sum("total_obitos").alias("obitos"),
        F.sum("total_casos_com_evolucao").alias("evolucao"),
        F.sum("total_casos").alias("total_casos"),
        F.sum("total_uti").alias("uti"),
        F.sum("total_hospitalizados").alias("hosp"),
    ).first()
)
taxa_mort_geral = round((ag_data["obitos"] or 0) / (ag_data["evolucao"] or 1) * 100, 2)
taxa_uti_geral  = round((ag_data["uti"] or 0) / (ag_data["hosp"] or 1) * 100, 2)
taxa_vac_gripe  = round((TAXA_VAC_GRIPE or 0) * 100, 2)
taxa_vac_covid  = round((TAXA_VAC_COVID or 0) * 100, 2)

metricas_bar = [
    ("Mortalidade (%)",        taxa_mort_geral,  COR_PERIGO),
    ("Uso de UTI (%)",         taxa_uti_geral,   COR_ALERTA),
    ("Vac. Gripe Registrada (%)", taxa_vac_gripe,   COR_SUCESSO),
    ("Vac. COVID Registrada (%)", taxa_vac_covid,   COR_ROXO),
]
nomes_m  = [m[0] for m in metricas_bar]
valores_m = [m[1] for m in metricas_bar]
cores_m  = [m[2] for m in metricas_bar]

bars_h = ax_bar_ag.barh(
    nomes_m, valores_m,
    color=cores_m, alpha=0.85,
    height=0.55, edgecolor="white", linewidth=0.5
)
for bar, val in zip(bars_h, valores_m):
    ax_bar_ag.text(
        val + max(valores_m) * 0.015,
        bar.get_y() + bar.get_height() / 2,
        f"{val:.1f}%", va="center", fontsize=9,
        fontweight="bold", color=COR_TEXTO
    )

ax_bar_ag.set_title("Indicadores Consolidados\n(Últimos 12 meses)",
                    fontsize=12, fontweight="bold", color=COR_TEXTO, pad=10)
ax_bar_ag.set_xlabel("Percentual (%)", fontsize=9, color=COR_TEXTO_LEVE)
ax_bar_ag.set_facecolor(COR_FUNDO_CARD)
ax_bar_ag.spines[["top","right"]].set_visible(False)
ax_bar_ag.set_xlim(0, max(valores_m) * 1.25)

plt.tight_layout()
display(fig_agente)
plt.close()
print("✅ Gráfico de agente etiológico gerado.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## [Célula 7] Gráfico 5 — Heatmap Casos por UF × Mês

# COMMAND ----------

if not df_uf_mes.empty:
    pivot = df_uf_mes.pivot_table(
        index="sg_uf", columns="ano_mes",
        values="casos", aggfunc="sum", fill_value=0
    )
    # Ordenar UFs pelo total de casos (maior para menor)
    pivot = pivot.loc[pivot.sum(axis=1).sort_values(ascending=False).index]
    # Limitar a 27 UFs e 12 meses
    pivot = pivot.head(27)

    fig_heat, ax_heat = plt.subplots(
        figsize=(max(14, len(pivot.columns) * 1.2),
                 max(8,  len(pivot.index) * 0.45))
    )
    fig_heat.patch.set_facecolor(COR_FUNDO)

    # Paleta: fundo claro → azul-teal profundo
    cmap_heat = sns.light_palette(COR_PRIMARIA, as_cmap=True)

    sns.heatmap(
        pivot,
        ax=ax_heat,
        cmap=cmap_heat,
        linewidths=0.4,
        linecolor=COR_FUNDO,
        annot=(len(pivot.columns) <= 14),   # Anotar apenas se ≤ 14 colunas
        fmt=".0f",
        annot_kws={"size": 7, "color": COR_TEXTO},
        cbar_kws={
            "label": "Total de Casos",
            "shrink": 0.6,
            "pad": 0.02,
        },
        yticklabels=True,
        xticklabels=True,
    )

    ax_heat.set_title(
        "Heatmap de Casos SRAG por UF e Mês — Últimos 12 Meses",
        fontsize=13, fontweight="bold", pad=14, color=COR_TEXTO
    )
    ax_heat.set_xlabel("Mês/Ano", fontsize=9, color=COR_TEXTO_LEVE)
    ax_heat.set_ylabel("UF de Residência", fontsize=9, color=COR_TEXTO_LEVE)
    plt.setp(ax_heat.xaxis.get_majorticklabels(), rotation=45, ha="right", fontsize=8)
    plt.setp(ax_heat.yaxis.get_majorticklabels(), fontsize=8)

    # Colorbar customizado
    cbar = ax_heat.collections[0].colorbar
    cbar.ax.yaxis.label.set_color(COR_TEXTO_LEVE)
    cbar.ax.tick_params(labelcolor=COR_TEXTO_LEVE, labelsize=7)

    fig_heat.text(
        0.5, -0.02,
        "⚠️  Baseado em UF de residência. Casos sem UF informada foram excluídos.",
        ha="center", fontsize=7, color=COR_TEXTO_LEVE, style="italic"
    )

    plt.tight_layout()
    display(fig_heat)
    plt.close()
    print(f"✅ Heatmap gerado: {len(pivot.index)} UFs × {len(pivot.columns)} meses")
else:
    print("⚠️  Dados insuficientes para o heatmap.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## [Célula 8] Gráfico 6 — Taxa de Vacinação + Evolução Temporal

# COMMAND ----------

fig_vac, axes_vac = plt.subplots(1, 2, figsize=(16, 6))
fig_vac.patch.set_facecolor(COR_FUNDO)

# ── Esquerda: evolução temporal da vacinação (mensal) ─────────────────────────
ax_vl = axes_vac[0]
ax_vl.set_facecolor(COR_FUNDO_CARD)
ax_vl.spines[["top","right"]].set_visible(False)

vac_gripe_m = df_mensal["taxa_vacinacao_gripe_registrada"].fillna(0) * 100
vac_covid_m = df_mensal["taxa_vacinacao_covid_registrada"].fillna(0) * 100
x_m = np.arange(len(df_mensal))

ax_vl.plot(x_m, vac_gripe_m,
           color=COR_SUCESSO, linewidth=2.2,
           marker="o", markersize=5,
           markerfacecolor=COR_SUCESSO, markeredgecolor="white",
           label="Gripe (registrada)")
ax_vl.fill_between(x_m, vac_gripe_m, alpha=0.12, color=COR_SUCESSO)

ax_vl.plot(x_m, vac_covid_m,
           color=COR_ROXO, linewidth=2.2,
           marker="s", markersize=5,
           markerfacecolor=COR_ROXO, markeredgecolor="white",
           linestyle="--", label="COVID-19 (registrada)")
ax_vl.fill_between(x_m, vac_covid_m, alpha=0.10, color=COR_ROXO)

ax_vl.set_title("Vacinação Registrada entre Casos SRAG\n(por mês — últimos 12 meses)",
                fontsize=11, fontweight="bold", color=COR_TEXTO)
ax_vl.set_ylabel("% vacinados (entre casos com info.)", fontsize=9)
ax_vl.set_xticks(x_m)
ax_vl.set_xticklabels(df_mensal["ano_mes"], rotation=45, ha="right", fontsize=7.5)
ax_vl.yaxis.set_major_formatter(fmt_pct)
ax_vl.legend(fontsize=8, framealpha=0.95)
ax_vl.set_ylim(0, 105)

# Disclaimer obrigatório
ax_vl.text(0.02, 0.04,
           "⚠️ Não representa cobertura vacinal populacional\n"
           "Calculado entre casos SRAG com info. de vacinação",
           transform=ax_vl.transAxes,
           fontsize=6.5, color=COR_TEXTO_LEVE, style="italic", va="bottom")

# ── Direita: comparação período atual vs anterior ─────────────────────────────
ax_vr = axes_vac[1]
ax_vr.set_facecolor(COR_FUNDO_CARD)
ax_vr.spines[["top","right"]].set_visible(False)

# Comparação 7d atual vs 7d anterior
comparacao = {
    "Casos\n7d atual":  CASOS_7D,
    "Casos\n7d anterior": CASOS_7D_ANT,
}
cores_comp = [COR_PRIMARIA, "#A8D4E8"]
bars_comp = ax_vr.bar(
    list(comparacao.keys()), list(comparacao.values()),
    color=cores_comp, width=0.45,
    edgecolor="white", linewidth=0.8,
    alpha=0.9
)
for bar, val in zip(bars_comp, comparacao.values()):
    ax_vr.text(
        bar.get_x() + bar.get_width() / 2,
        bar.get_height() + max(comparacao.values()) * 0.02,
        f"{val:,}", ha="center", va="bottom",
        fontsize=13, fontweight="bold", color=COR_TEXTO
    )

# Seta de variação
variacao_str = f"{TAXA_AUMENTO:+.1%}" if TAXA_AUMENTO is not None else "N/A"
variacao_cor  = COR_PERIGO if (TAXA_AUMENTO or 0) > 0 else COR_SUCESSO
ax_vr.text(
    0.5, 0.88, f"Variação: {variacao_str}",
    transform=ax_vr.transAxes,
    ha="center", fontsize=16, fontweight="bold",
    color=variacao_cor
)

ax_vr.set_title("Taxa de Aumento de Casos\n(7 dias vs 7 dias anteriores)",
                fontsize=11, fontweight="bold", color=COR_TEXTO)
ax_vr.set_ylabel("Número de casos", fontsize=9, color=COR_TEXTO_LEVE)
ax_vr.yaxis.set_major_formatter(fmt_mil)

plt.tight_layout()
display(fig_vac)
plt.close()
print("✅ Gráfico de vacinação e comparação gerado.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## [Célula 9] Resumo Final — Todos os Disclaimers Obrigatórios

# COMMAND ----------

print("=" * 72)
print("  DASHBOARD SRAG — RESUMO ANALÍTICO E DISCLAIMERS")
print("=" * 72)
print(f"\n  Data de referência do dataset : {DATA_REF}")
print(f"  Defasagem                     : {DEFASAGEM} dias")
print(f"  Confiabilidade                : {OBS_CONF}")
print()
print("  4 MÉTRICAS OBRIGATÓRIAS:")
print(f"  ├─ Taxa de aumento de casos : {f'{TAXA_AUMENTO:+.2%}' if TAXA_AUMENTO is not None else 'N/A'}")
print(f"  │    (7d: {CASOS_7D:,} casos vs 7d ant.: {CASOS_7D_ANT:,})")
print(f"  ├─ Taxa de mortalidade (30d): {f'{TAXA_MORT:.2%}' if TAXA_MORT else 'N/A'}")
print(f"  │    ({OBITOS_30D:,} óbitos / {CASOS_30D:,} casos com evolução)")
print(f"  ├─ Taxa de uso de UTI (30d) : {f'{TAXA_UTI:.2%}' if TAXA_UTI else 'N/A'}")
print(f"  └─ Vacinação gripe (30d)    : {f'{TAXA_VAC_GRIPE:.2%}' if TAXA_VAC_GRIPE else 'N/A'}")
print()
print("  DISCLAIMERS OBRIGATÓRIOS:")
print("  1. Taxa de mortalidade: sobre casos com evolução informada (pode subestimar).")
print("  2. Taxa de UTI: uso registrado entre SRAG — não representa ocupação real de leitos.")
print("  3. Taxa de vacinação: entre casos SRAG — não é cobertura vacinal populacional.")
print("  4. Dataset batch: atraso de digitação de 7-30 dias esperado no SIVEP-Gripe.")
print("  5. Leitos UTI: capacidade via CNES — UTIs privadas não cadastradas não incluídas.")
print()
print("  COMO ADICIONAR AO DATABRICKS DASHBOARD:")
print("  Workspace → este notebook → ⋮ → Add to Dashboard → criar novo")
print("=" * 72)

dbutils.notebook.exit("SUCESSO|dashboard_srag|6_graficos_gerados")
