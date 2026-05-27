# Databricks notebook source
# MAGIC %md
# MAGIC # 09 — Dashboard: Qualidade de Dados DMBOK — SRAG DATASUS
# MAGIC
# MAGIC **PoC:** HealthCare Indicium — Monitoramento de SRAG
# MAGIC **Objetivo:** Visualizar métricas de qualidade DMBOK das camadas Bronze e Gold
# MAGIC
# MAGIC ---
# MAGIC ### Gráficos gerados
# MAGIC
# MAGIC | # | Gráfico | Fonte |
# MAGIC |---|---|---|
# MAGIC | 1 | Semáforo de severidade — visão geral Bronze + Gold | Ambas |
# MAGIC | 2 | Completude por coluna obrigatória (Bronze) | bronze_quality |
# MAGIC | 3 | Validade por coluna de domínio (Bronze) | bronze_quality |
# MAGIC | 4 | Consistência temporal entre datas | bronze_quality |
# MAGIC | 5 | Radar DMBOK — 5 dimensões Bronze + Gold | Ambas |
# MAGIC | 6 | Evolução histórica da qualidade por execução | Ambas |
# MAGIC | 7 | Acurácia das métricas Gold (sanidade) | gold_quality |
# MAGIC
# MAGIC ---
# MAGIC > **Agrupamento por `run_id`:** cada execução é identificada pelo UUID gerado
# MAGIC > no início do notebook de qualidade, garantindo que todas as dimensões de
# MAGIC > uma mesma execução sejam tratadas em conjunto — independente do timestamp.

# COMMAND ----------

# MAGIC %md
# MAGIC ## [Célula 1] Setup

# COMMAND ----------

import matplotlib
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import matplotlib.gridspec as gridspec
from matplotlib.ticker import FuncFormatter, PercentFormatter
import matplotlib.patheffects as pe
import seaborn as sns
import pandas as pd
import numpy as np
from pyspark.sql import functions as F
import warnings
warnings.filterwarnings("ignore")

# ── Paleta clínica ─────────────────────────────────────────────────────────────
COR_CRITICO   = "#C0392B"
COR_ALTO      = "#E67E22"
COR_MEDIO     = "#E8B84B"
COR_BAIXO     = "#3498DB"
COR_ACEITAVEL = "#27AE60"
COR_BRONZE    = "#1A6B8A"   # azul-teal — Bronze
COR_GOLD      = "#E8952C"   # âmbar — Gold
COR_FUNDO     = "#F8FAFB"
COR_CARD      = "#FFFFFF"
COR_GRID      = "#E8EDF0"
COR_TEXTO     = "#1C2B36"
COR_LEVE      = "#546E7A"

SEV_CORES  = {"CRÍTICO": COR_CRITICO, "ALTO": COR_ALTO,
               "MÉDIO": COR_MEDIO, "BAIXO": COR_BAIXO, "ACEITÁVEL": COR_ACEITAVEL}
SEV_ORDEM  = ["CRÍTICO", "ALTO", "MÉDIO", "BAIXO", "ACEITÁVEL"]

plt.rcParams.update({
    "figure.facecolor": COR_FUNDO, "axes.facecolor": COR_CARD,
    "axes.edgecolor": COR_GRID, "axes.labelcolor": COR_LEVE,
    "axes.titlecolor": COR_TEXTO, "axes.titlesize": 11,
    "axes.titleweight": "bold", "axes.labelsize": 9,
    "axes.grid": True, "grid.color": COR_GRID, "grid.linewidth": 0.6,
    "xtick.labelsize": 8, "ytick.labelsize": 8,
    "legend.fontsize": 8, "legend.framealpha": 0.9,
    "font.family": "DejaVu Sans", "figure.dpi": 120,
})

CATALOGO = "certificacao_indicium"
SCHEMA   = "poc_srag_datasus"
T_BRZ_Q  = f"{CATALOGO}.{SCHEMA}.bronze_quality_poc_srag_datasus"
T_GOLD_Q = f"{CATALOGO}.{SCHEMA}.gold_quality_poc_srag_datasus"

print("✅ Setup concluído.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## [Célula 2] Carregar dados — agrupamento por run_id
# MAGIC
# MAGIC **Por que run_id e não dh_execucao?**
# MAGIC O notebook de qualidade avalia cada dimensão em uma célula separada.
# MAGIC O `dh_execucao` é gerado por `current_timestamp()` no momento de cada célula —
# MAGIC portanto as 5 dimensões de uma mesma execução têm timestamps diferentes
# MAGIC (spread de 2-5 minutos). Agrupar por timestamp pega apenas uma dimensão por vez.
# MAGIC O `run_id` é gerado UMA VEZ no início e compartilhado por todas as 40 regras.

# COMMAND ----------

# ── Bronze Quality ─────────────────────────────────────────────────────────────
df_brz_all = spark.table(T_BRZ_Q).toPandas()
df_brz_all["dh_execucao"] = pd.to_datetime(
    df_brz_all["dh_execucao"], utc=True, errors="coerce"
)
for col in ["percentual_validade", "percentual_invalidade",
            "total_registros", "registros_validos", "registros_invalidos"]:
    if col in df_brz_all.columns:
        df_brz_all[col] = pd.to_numeric(df_brz_all[col], errors="coerce")

# Seleciona o run_id mais recente que tem todas as 5 dimensões DMBOK
_stats_brz = (
    df_brz_all
    .groupby("run_id")
    .agg(
        n_dims=("dimensao_dmbok", "nunique"),
        max_ts=("dh_execucao",    "max"),
    )
    .reset_index()
)
_completos_brz = _stats_brz[_stats_brz["n_dims"] >= 5]
if _completos_brz.empty:
    _completos_brz = _stats_brz
_run_brz   = _completos_brz.sort_values("max_ts", ascending=False).iloc[0]["run_id"]
df_brz     = df_brz_all[df_brz_all["run_id"] == _run_brz].copy()
dh_max_brz = df_brz["dh_execucao"].max()

# Normalizar coluna de observação (Bronze usa "observacao")
if "observacao" in df_brz.columns and "impacto_analitico" not in df_brz.columns:
    df_brz["impacto_analitico"] = df_brz["observacao"]

# ── Gold Quality ───────────────────────────────────────────────────────────────
df_gld_all = spark.table(T_GOLD_Q).toPandas()
df_gld_all["dh_execucao"] = pd.to_datetime(
    df_gld_all["dh_execucao"], utc=True, errors="coerce"
)
for col in ["percentual_validade", "percentual_invalidade",
            "total_registros", "registros_validos", "registros_invalidos"]:
    if col in df_gld_all.columns:
        df_gld_all[col] = pd.to_numeric(df_gld_all[col], errors="coerce")

_stats_gld = (
    df_gld_all
    .groupby("run_id")
    .agg(
        n_dims=("dimensao_dmbok", "nunique"),
        max_ts=("dh_execucao",   "max"),
    )
    .reset_index()
)
_completos_gld = _stats_gld[_stats_gld["n_dims"] >= _stats_gld["n_dims"].max()]
_run_gld   = _completos_gld.sort_values("max_ts", ascending=False).iloc[0]["run_id"]
df_gld     = df_gld_all[df_gld_all["run_id"] == _run_gld].copy()
dh_max_gld = df_gld["dh_execucao"].max()

if "impacto_analitico" not in df_gld.columns:
    df_gld["impacto_analitico"] = df_gld.get("observacao", "—")

# ── Histórico por run_id ───────────────────────────────────────────────────────
df_hist_brz = df_brz_all.copy()
df_hist_gld = df_gld_all.copy()

criticos_brz = (df_brz["severidade"] == "CRÍTICO").sum()
altos_brz    = (df_brz["severidade"] == "ALTO").sum()
criticos_gld = (df_gld["severidade"] == "CRÍTICO").sum()
altos_gld    = (df_gld["severidade"] == "ALTO").sum()

print(f"✅ Dados carregados:")
print(f"   Bronze Quality  : {len(df_brz)} regras | CRÍTICO={criticos_brz} | ALTO={altos_brz}")
print(f"   Gold Quality    : {len(df_gld)} regras | CRÍTICO={criticos_gld} | ALTO={altos_gld}")
print(f"   run_id Bronze   : {_run_brz}")
print(f"   run_id Gold     : {_run_gld}")
print(f"   Execuções únicas Bronze: {df_brz_all['run_id'].nunique()}")
print(f"   Execuções únicas Gold  : {df_gld_all['run_id'].nunique()}")
print()
print("   Dimensões na Bronze:")
print(df_brz["dimensao_dmbok"].value_counts().to_string())

# COMMAND ----------

# MAGIC %md
# MAGIC ## [Célula 3] Gráfico 1 — Semáforo de Qualidade Geral

# COMMAND ----------

fig_sem, axes_sem = plt.subplots(1, 2, figsize=(16, 5))
fig_sem.patch.set_facecolor(COR_FUNDO)
fig_sem.suptitle("Semáforo de Qualidade DMBOK — Execução Mais Completa",
                 fontsize=14, fontweight="bold", color=COR_TEXTO, y=1.01)

for ax, df_q, titulo, dh_exec, cor_layer in [
    (axes_sem[0], df_brz, " Bronze Quality", dh_max_brz, COR_BRONZE),
    (axes_sem[1], df_gld, " Gold Quality",   dh_max_gld, COR_GOLD),
]:
    ax.set_facecolor(COR_CARD)
    ax.spines[["top","right","bottom","left"]].set_visible(False)
    ax.set_xticks([]); ax.set_yticks([])

    contagem = df_q["severidade"].value_counts().reindex(SEV_ORDEM, fill_value=0)
    total    = contagem.sum()

    # Barra empilhada de severidades
    x_start = 0.05
    bar_y, bar_h = 0.68, 0.14
    for sev in SEV_ORDEM:
        n   = contagem[sev]
        pct = n / total if total > 0 else 0
        w   = pct * 0.90
        if w > 0:
            rect = mpatches.FancyBboxPatch(
                (x_start, bar_y), w, bar_h,
                boxstyle="round,pad=0.005",
                facecolor=SEV_CORES[sev], edgecolor="white", linewidth=1.5,
                transform=ax.transAxes, figure=fig_sem
            )
            ax.add_patch(rect)
            if pct > 0.08:
                ax.text(x_start + w/2, bar_y + bar_h/2, f"{n}",
                        ha="center", va="center", fontsize=11,
                        fontweight="bold", color="white",
                        transform=ax.transAxes)
        x_start += w

    # Contagem por severidade
    for i, sev in enumerate(SEV_ORDEM):
        n = contagem[sev]
        ax.text(0.05 + i*0.185, 0.52, f"■ {sev}",
                color=SEV_CORES[sev], transform=ax.transAxes,
                fontsize=7.5, fontweight="bold")
        ax.text(0.05 + i*0.185, 0.43, f"{n} regra{'s' if n!=1 else ''}",
                color=COR_LEVE, transform=ax.transAxes, fontsize=7)

    # Círculo central com nível geral
    nivel = ("CRÍTICO"   if contagem["CRÍTICO"] > 0 else
             "ALTO"      if contagem["ALTO"] > 0    else
             "MÉDIO"     if contagem["MÉDIO"] > 0   else
             "ACEITÁVEL")
    cor_nivel = SEV_CORES[nivel]
    circ = plt.Circle((0.5, 0.21), 0.13, color=cor_nivel,
                       transform=ax.transAxes, zorder=5)
    ax.add_patch(circ)
    ax.text(0.5, 0.24, nivel, ha="center", va="center",
            transform=ax.transAxes, fontsize=8,
            fontweight="bold", color="white", zorder=6)
    ax.text(0.5, 0.16, "Nível Geral", ha="center", va="center",
            transform=ax.transAxes, fontsize=6.5,
            color="white", zorder=6)

    dh_str = dh_exec.strftime("%Y-%m-%d %H:%M") if pd.notna(dh_exec) else "—"
    ax.set_title(f"{titulo}\n{total} regras avaliadas | {dh_str}",
                 fontsize=10, fontweight="bold", color=COR_TEXTO, pad=8)
    ax.set_xlim(0, 1); ax.set_ylim(0, 1)

plt.tight_layout()
display(fig_sem)
plt.close()
print("✅ Semáforo gerado.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## [Célula 4] Gráfico 2 — Completude por Coluna (Bronze)

# COMMAND ----------

df_comp = df_brz[df_brz["dimensao_dmbok"] == "Completude"].copy()
df_comp = df_comp.sort_values("percentual_validade", ascending=True)

if not df_comp.empty:
    fig_c, ax_c = plt.subplots(figsize=(14, max(4, len(df_comp) * 0.55)))
    fig_c.patch.set_facecolor(COR_FUNDO)
    ax_c.set_facecolor(COR_CARD)
    ax_c.spines[["top","right"]].set_visible(False)

    valores  = df_comp["percentual_validade"].fillna(0)
    colunas  = df_comp["coluna_avaliada"].fillna("—")
    severid  = df_comp["severidade"]
    cores_c  = [SEV_CORES.get(s, COR_ACEITAVEL) for s in severid]

    ax_c.barh(colunas, valores, color=cores_c, height=0.6,
              edgecolor="white", linewidth=0.5)

    for thr, lbl, ls, cor in [
        (0.99, "ACEITÁVEL (99%)", "--", COR_ACEITAVEL),
        (0.90, "ALTO (90%)",      ":",  COR_ALTO),
        (0.80, "CRÍTICO (80%)",   "-.", COR_CRITICO),
    ]:
        ax_c.axvline(thr, color=cor, linestyle=ls, linewidth=1.2,
                     alpha=0.7, label=lbl)

    for i, (val, sev) in enumerate(zip(valores, severid)):
        ax_c.text(min(val - 0.005, 0.995), i,
                  f"{val:.1%}", va="center", ha="right",
                  fontsize=8, fontweight="bold",
                  color="white" if val < 0.5 else COR_TEXTO)
        if sev in ("CRÍTICO", "ALTO"):
            ax_c.text(val + 0.003, i, f"⚠ {sev}", va="center",
                      fontsize=7, color=SEV_CORES[sev], fontweight="bold")

    ax_c.set_xlim(0, 1.15)
    ax_c.set_title("Completude por Coluna — Camada Bronze\n(% de registros não-nulos)",
                   fontsize=12, fontweight="bold", color=COR_TEXTO)
    ax_c.set_xlabel("Percentual de validade", fontsize=9)
    ax_c.xaxis.set_major_formatter(FuncFormatter(lambda x,_: f"{x:.0%}"))
    ax_c.legend(loc="lower right", fontsize=7.5, framealpha=0.95)
    plt.tight_layout()
    display(fig_c)
    plt.close()
    print(f"✅ Completude: {len(df_comp)} colunas avaliadas.")
else:
    print("⚠️  Nenhuma regra de Completude encontrada na Bronze Quality.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## [Célula 5] Gráfico 3 — Validade por Coluna (Bronze)

# COMMAND ----------

df_val = df_brz[df_brz["dimensao_dmbok"] == "Validade"].copy()
# Deduplicar se houver registros duplicados por coluna
df_val = df_val.drop_duplicates(subset=["coluna_avaliada"]) \
               .sort_values("percentual_invalidade", ascending=False)

if not df_val.empty:
    fig_v, ax_v = plt.subplots(figsize=(14, max(4, len(df_val) * 0.45)))
    fig_v.patch.set_facecolor(COR_FUNDO)
    ax_v.set_facecolor(COR_CARD)
    ax_v.spines[["top","right"]].set_visible(False)

    invalidos  = df_val["percentual_invalidade"].fillna(0)
    validos    = 1 - invalidos
    colunas_v  = df_val["coluna_avaliada"].fillna("—")
    severid_v  = df_val["severidade"]
    x_pos      = np.arange(len(df_val))

    ax_v.barh(x_pos, validos, color="#A8D8B9", alpha=0.85,
              height=0.6, label="Válidos", edgecolor="white", linewidth=0.3)
    # Cor inválidos: sempre laranja-vermelho independente da severidade
    # (quando severidade=ACEITÁVEL a cor seria verde igual aos válidos — invisível)
    cores_inv = [
        COR_CRITICO if s == "CRÍTICO" else
        COR_ALTO    if s == "ALTO"    else
        COR_MEDIO   if s == "MÉDIO"   else
        "#E8A87C"   # laranja suave para BAIXO/ACEITÁVEL
        for s in severid_v
    ]
    ax_v.barh(x_pos, invalidos, left=validos,
              color=cores_inv, alpha=0.95, height=0.6, label="Inválidos",
              edgecolor="white", linewidth=0.3)

    for i, (inv, sev) in enumerate(zip(invalidos, severid_v)):
        if inv > 0.001:
            ax_v.text(1 + 0.005, x_pos[i], f"{inv:.1%} inválidos",
                      va="center", fontsize=7.5,
                      color=SEV_CORES.get(sev, COR_ACEITAVEL),
                      fontweight="bold" if sev in ("CRÍTICO","ALTO") else "normal")

    ax_v.set_yticks(x_pos)
    ax_v.set_yticklabels(colunas_v, fontsize=8)
    ax_v.set_xlim(0, 1.22)
    ax_v.set_title(
        "Validade de Domínio por Coluna — Camada Bronze\n"
        "(% registros fora do domínio SIVEP-Gripe)",
        fontsize=12, fontweight="bold", color=COR_TEXTO
    )
    ax_v.set_xlabel("Proporção", fontsize=9)
    ax_v.xaxis.set_major_formatter(FuncFormatter(lambda x,_: f"{x:.0%}"))
    ax_v.legend(loc="lower right", fontsize=8, framealpha=0.95)
    plt.tight_layout()
    display(fig_v)
    plt.close()
    print(f"✅ Validade: {len(df_val)} colunas avaliadas.")
else:
    print("⚠️  Nenhuma regra de Validade encontrada.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## [Célula 6] Gráfico 4 — Consistência Temporal

# COMMAND ----------

df_cons = df_brz[df_brz["dimensao_dmbok"] == "Consistência"].copy()

if not df_cons.empty:
    df_cons = df_cons.sort_values("percentual_validade", ascending=True)
    regras   = df_cons["regra_qualidade"].str.replace(
        r"Consistência [Tt]emporal\s*[-–]\s*", "", regex=True).fillna("—")
    pct_val  = df_cons["percentual_validade"].fillna(0)
    pct_inv  = df_cons["percentual_invalidade"].fillna(0)
    severid  = df_cons["severidade"]
    x_pos    = np.arange(len(df_cons))

    fig_cs, ax_cs = plt.subplots(figsize=(14, max(3.5, len(df_cons) * 0.7)))
    fig_cs.patch.set_facecolor(COR_FUNDO)
    ax_cs.set_facecolor(COR_CARD)
    ax_cs.spines[["top","right"]].set_visible(False)

    ax_cs.barh(x_pos, pct_val, color="#A8D8B9", alpha=0.85,
               height=0.55, label="Consistentes", edgecolor="white")
    cores_inc = [
        COR_CRITICO if s == "CRÍTICO" else
        COR_ALTO    if s == "ALTO"    else
        COR_MEDIO   if s == "MÉDIO"   else
        "#E8A87C"
        for s in severid
    ]
    ax_cs.barh(x_pos, pct_inv, left=pct_val,
               color=cores_inc, alpha=0.95, height=0.55,
               label="Inconsistentes", edgecolor="white")

    for i, (pv, pi, sev) in enumerate(zip(pct_val, pct_inv, severid)):
        if pi > 0.001:
            ax_cs.text(1+0.005, x_pos[i], f"{pi:.2%} ⚠",
                       va="center", fontsize=7.5,
                       color=SEV_CORES.get(sev, COR_ALTO),
                       fontweight="bold")

    ax_cs.set_yticks(x_pos)
    ax_cs.set_yticklabels(regras, fontsize=8)
    ax_cs.set_xlim(0, 1.22)
    ax_cs.set_title(
        "Consistência Temporal entre Datas — Camada Bronze\n"
        "(ex: dt_sin_pri ≤ dt_notific, dt_entuti ≤ dt_saiduti)",
        fontsize=12, fontweight="bold", color=COR_TEXTO)
    ax_cs.set_xlabel("Proporção de registros", fontsize=9)
    ax_cs.xaxis.set_major_formatter(FuncFormatter(lambda x,_: f"{x:.0%}"))
    ax_cs.legend(loc="lower right", fontsize=8)
    plt.tight_layout()
    display(fig_cs)
    plt.close()
    print(f"✅ Consistência: {len(df_cons)} relações avaliadas.")
else:
    print("⚠️  Nenhuma regra de Consistência encontrada.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## [Célula 7] Gráfico 5 — Radar DMBOK (Bronze vs Gold)

# COMMAND ----------

def score_dim(df, dim):
    sub = df[df["dimensao_dmbok"] == dim]
    return float(sub["percentual_validade"].mean()) if len(sub) > 0 else None

DIMS_RADAR = ["Completude", "Validade", "Consistência", "Unicidade", "Atualidade", "Acurácia"]

# Scores reais — None se dimensão não avaliada
sb = {d: score_dim(df_brz, d) for d in DIMS_RADAR}
sg = {d: score_dim(df_gld, d) for d in DIMS_RADAR}

# N/A (dimensão não avaliada) → eixo mínimo (0.70) para não colapsar o polígono
# Valor 0 real (bug histórico em Atualidade) → mantido como 0.70 também
# pois reflete dado histórico incorreto, não o estado atual
RADAR_MIN = 0.70  # mesmo valor do ylim inferior

def safe_radar(v):
    if v is None or (isinstance(v, float) and np.isnan(v)):
        return None  # não avaliado
    return max(v, 0.0)  # valor real

vals_brz_raw = [safe_radar(sb[d]) for d in DIMS_RADAR]
vals_gld_raw = [safe_radar(sg[d]) for d in DIMS_RADAR]

# Para plotar: None → RADAR_MIN (mínimo do eixo), valor real → posição correta
def para_plot(v): return RADAR_MIN if v is None else max(v, RADAR_MIN)

N       = len(DIMS_RADAR)
angulos = np.linspace(0, 2*np.pi, N, endpoint=False).tolist()
angulos += angulos[:1]
vals_brz = [para_plot(v) for v in vals_brz_raw] + [para_plot(vals_brz_raw[0])]
vals_gld = [para_plot(v) for v in vals_gld_raw] + [para_plot(vals_gld_raw[0])]

fig_rad, ax_r = plt.subplots(figsize=(9, 9), subplot_kw=dict(polar=True))
fig_rad.patch.set_facecolor(COR_FUNDO)
ax_r.set_facecolor(COR_CARD)

# Zonas de referência
theta_fill = np.linspace(0, 2*np.pi, 200)
ax_r.fill_between(theta_fill, 0, 0.90,  color=COR_CRITICO,   alpha=0.04, zorder=1)
ax_r.fill_between(theta_fill, 0.90, 0.99, color=COR_ALTO,   alpha=0.04, zorder=1)

for r_val in [0.80, 0.90, 0.95, 0.99]:
    ax_r.plot([0, 2*np.pi], [r_val]*2, "--",
              color=COR_GRID, linewidth=0.8, alpha=0.7)
    ax_r.text(angulos[0], r_val+0.008, f"{r_val:.0%}",
              ha="center", va="bottom", fontsize=7, color=COR_LEVE)

# Bronze — azul sólido com marcadores
ax_r.plot(angulos, vals_brz, color=COR_BRONZE, linewidth=2.8,
          zorder=4, label="Bronze", marker="o", markersize=6,
          markerfacecolor=COR_BRONZE, markeredgecolor="white",
          markeredgewidth=1.2)
ax_r.fill(angulos, vals_brz, alpha=0.20, color=COR_BRONZE, zorder=2)

# Gold — âmbar tracejado com marcadores quadrados
ax_r.plot(angulos, vals_gld, color=COR_GOLD, linewidth=2.8,
          linestyle="--", zorder=5, label="Gold",
          marker="s", markersize=6,
          markerfacecolor=COR_GOLD, markeredgecolor="white",
          markeredgewidth=1.2)
ax_r.fill(angulos, vals_gld, alpha=0.15, color=COR_GOLD, zorder=3)

# Labels com score
labels_radar = []
for d, vb, vg in zip(DIMS_RADAR, vals_brz_raw, vals_gld_raw):
    vb_str = f"{vb:.1%}" if vb is not None else "não avaliado"
    vg_str = f"{vg:.1%}" if vg is not None else "não avaliado"
    labels_radar.append(f"{d}\nBronze: {vb_str}\nGold: {vg_str}")

ax_r.set_thetagrids(np.degrees(angulos[:-1]), labels=labels_radar,
                    fontsize=8, color=COR_TEXTO)
ax_r.set_ylim(0.70, 1.03)
ax_r.set_yticks([])
ax_r.spines["polar"].set_color(COR_GRID)
ax_r.set_title("Radar de Qualidade DMBOK\n(Bronze vs Gold — 6 dimensões)",
               fontsize=13, fontweight="bold", color=COR_TEXTO, pad=25)
ax_r.legend(loc="upper right", bbox_to_anchor=(1.35, 1.12),
            fontsize=9, framealpha=0.95)

plt.tight_layout()
display(fig_rad)
plt.close()
print("✅ Radar DMBOK gerado.")
print()
print("Scores por dimensão:")
for d in DIMS_RADAR:
    vb = f"{sb[d]:.1%}" if sb[d] is not None else "não avaliado"
    vg = f"{sg[d]:.1%}" if sg[d] is not None else "não avaliado"
    print(f"  {d:<15} Bronze: {vb:<10} Gold: {vg}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## [Célula 8] Gráfico 6 — Evolução Histórica por Execução (run_id)

# COMMAND ----------

def agregar_por_run(df, camada):
    """Agrupa por run_id e calcula métricas de qualidade por execução."""
    if "run_id" not in df.columns or df.empty:
        return pd.DataFrame()

    agg = (
        df.groupby("run_id")
        .apply(lambda g: pd.Series({
            "dh_execucao":  g["dh_execucao"].max(),
            "criticos":     (g["severidade"] == "CRÍTICO").sum(),
            "altos":        (g["severidade"] == "ALTO").sum(),
            "medios":       (g["severidade"] == "MÉDIO").sum(),
            "aceitaveis":   (g["severidade"] == "ACEITÁVEL").sum(),
            "score_medio":  g["percentual_validade"].mean(),
            "total_regras": len(g),
        }))
        .reset_index()
        .sort_values("dh_execucao")
    )
    # Manter apenas execuções com ao menos 3 regras (descartar parciais)
    agg = agg[agg["total_regras"] >= 3].copy()
    agg["camada"] = camada
    agg["label_x"] = agg["dh_execucao"].dt.strftime("%d/%m\n%H:%M")
    return agg

hist_brz = agregar_por_run(df_hist_brz, "Bronze")
hist_gld = agregar_por_run(df_hist_gld, "Gold")

fig_hist, (ax_h1, ax_h2) = plt.subplots(1, 2, figsize=(16, 6))
fig_hist.patch.set_facecolor(COR_FUNDO)
fig_hist.suptitle("Evolução Histórica da Qualidade por Execução (run_id)",
                  fontsize=13, fontweight="bold", color=COR_TEXTO)

for ax_h, hist, titulo, cor in [
    (ax_h1, hist_brz, f" Bronze ({len(hist_brz)} execuções completas)", COR_BRONZE),
    (ax_h2, hist_gld, f" Gold ({len(hist_gld)} execuções completas)",   COR_GOLD),
]:
    ax_h.set_facecolor(COR_CARD)
    ax_h.spines[["top","right"]].set_visible(False)

    if hist.empty:
        ax_h.text(0.5, 0.5, "Sem execuções completas",
                  ha="center", va="center", transform=ax_h.transAxes,
                  fontsize=11, color=COR_LEVE)
        ax_h.set_title(titulo, fontsize=10, fontweight="bold")
        continue

    x = np.arange(len(hist))

    # Eixo secundário — score médio
    ax_sec = ax_h.twinx()
    ax_sec.plot(x, hist["score_medio"] * 100, color=cor, linewidth=2.5,
                marker="o", markersize=5,
                markerfacecolor=cor, markeredgecolor="white",
                label="Score médio (%)", zorder=5)
    ax_sec.set_ylabel("Score médio (%)", fontsize=8, color=cor)
    ax_sec.tick_params(axis="y", labelcolor=cor, labelsize=7)
    ax_sec.set_ylim(75, 102)
    ax_sec.spines["right"].set_edgecolor(cor)

    # Barras de alertas empilhadas
    bottom = np.zeros(len(hist))
    for sev_col, sev_label, sev_cor in [
        ("criticos", "CRÍTICO", COR_CRITICO),
        ("altos",    "ALTO",    COR_ALTO),
        ("medios",   "MÉDIO",   COR_MEDIO),
    ]:
        vals = hist[sev_col].fillna(0).values
        if vals.sum() > 0:
            ax_h.bar(x, vals, bottom=bottom, color=sev_cor,
                     alpha=0.85, label=sev_label, zorder=3)
            bottom += vals

    ax_h.set_xticks(x)
    ax_h.set_xticklabels(hist["label_x"], fontsize=7, rotation=0)
    ax_h.set_ylabel("Nº de alertas", fontsize=8, color=COR_LEVE)
    ax_h.set_title(titulo, fontsize=10, fontweight="bold", color=COR_TEXTO)
    ax_h.yaxis.set_major_locator(plt.MaxNLocator(integer=True))

    h1, l1 = ax_h.get_legend_handles_labels()
    h2, l2 = ax_sec.get_legend_handles_labels()
    ax_h.legend(h1+h2, l1+l2, loc="upper left", fontsize=7.5, framealpha=0.95)

plt.tight_layout()
display(fig_hist)
plt.close()
print("✅ Histórico de qualidade gerado.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## [Célula 9] Gráfico 7 — Acurácia e Atualidade das Métricas Gold

# COMMAND ----------

df_acur = df_gld[df_gld["dimensao_dmbok"].isin(["Acurácia", "Atualidade"])].copy()

if not df_acur.empty:
    n_rows = len(df_acur)
    fig_ac, ax_ac = plt.subplots(figsize=(22, max(3.5, n_rows * 1.3 + 2.0)))
    fig_ac.patch.set_facecolor(COR_FUNDO)
    ax_ac.set_facecolor(COR_FUNDO)
    ax_ac.set_axis_off()

    fig_ac.text(0.5, 0.97,
                "Acurácia e Atualidade das Métricas Gold — Validação de Sanidade",
                ha="center", va="top", fontsize=13,
                fontweight="bold", color=COR_TEXTO)

    # ── Cabeçalho da tabela ───────────────────────────────────────────────────
    # Distribuição: Regra(largo) | Dimensão | Resultado | Severidade | Impacto(largo)
    COL_X     = [0.01, 0.35, 0.50, 0.61, 0.72]
    COL_NOMES = ["Regra", "Dimensão", "Resultado", "Severidade", "Impacto"]

    y_cab = 0.88
    rect_h = mpatches.FancyBboxPatch(
        (0.01, y_cab - 0.015), 0.98, 0.065,
        boxstyle="square,pad=0", facecolor=COR_BRONZE,
        edgecolor="none", transform=fig_ac.transFigure, figure=fig_ac
    )
    fig_ac.add_artist(rect_h)
    for cx, nm in zip(COL_X, COL_NOMES):
        fig_ac.text(cx + 0.01, y_cab + 0.015, nm,
                    ha="left", va="center", fontsize=8.5,
                    fontweight="bold", color="white",
                    transform=fig_ac.transFigure, zorder=5)

    # ── Linhas da tabela ──────────────────────────────────────────────────────
    altura_linha = min(0.12, 0.75 / max(n_rows, 1))
    y = y_cab - 0.015 - altura_linha

    for i, (_, row) in enumerate(df_acur.iterrows()):
        cor_bg  = "#EAF4FB" if i % 2 == 0 else COR_CARD
        rect_r  = mpatches.FancyBboxPatch(
            (0.01, y), 0.98, altura_linha,
            boxstyle="square,pad=0",
            facecolor=cor_bg, edgecolor=COR_GRID, linewidth=0.3,
            transform=fig_ac.transFigure, figure=fig_ac
        )
        fig_ac.add_artist(rect_r)

        sev    = row.get("severidade", "ACEITÁVEL")
        pct    = row.get("percentual_validade", 1)
        # Sem emoji: matplotlib não suporta emoji coloridos (renderiza □)
        # Usar prefixo textual + cor via fontweight
        prefix = {"CRÍTICO":"[!] ","ALTO":"[!] ","MÉDIO":"[~] "}.get(sev, "[ok] ")
        regra  = str(row.get("regra_qualidade","—"))[:60]
        dim    = str(row.get("dimensao_dmbok","—"))
        pct_s  = f"{float(pct):.1%}" if pct is not None else "N/A"
        sev_s  = f"{prefix}{sev}"
        imp    = str(row.get("impacto_analitico", row.get("observacao","—")))[:90]

        dados = [regra, dim, pct_s, sev_s, imp]
        for cx, txt in zip(COL_X, dados):
            cor_txt = SEV_CORES.get(sev, COR_ACEITAVEL) if cx == COL_X[3] else COR_TEXTO
            fig_ac.text(
                cx + 0.01, y + altura_linha * 0.45, txt,
                ha="left", va="center", fontsize=8,
                color=cor_txt,
                fontweight="bold" if cx == COL_X[3] else "normal",
                transform=fig_ac.transFigure, zorder=5,
                clip_on=False
            )
        y -= altura_linha

    plt.tight_layout(pad=0.5)
    display(fig_ac)
    plt.close()
    print(f"✅ Acurácia Gold: {n_rows} regras visualizadas.")
else:
    print("⚠️  Nenhuma regra de Acurácia/Atualidade encontrada na Gold Quality.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## [Célula 10] Resumo Final

# COMMAND ----------

print("=" * 72)
print("  RESUMO DE QUALIDADE DMBOK — SRAG DATASUS")
print("=" * 72)
print()
print(f"  {'CAMADA':<12} {'REGRAS':>8} {'CRÍTICO':>9} {'ALTO':>6} {'ACEITÁVEL':>11} {'SCORE':>8}")
print(f"  {'-'*60}")

for df_q, label, run_id in [
    (df_brz, "Bronze", _run_brz),
    (df_gld, "Gold",   _run_gld),
]:
    if df_q.empty:
        print(f"  {label:<12} {'SEM DADOS':>8}")
        continue
    n_total  = len(df_q)
    n_crit   = (df_q["severidade"] == "CRÍTICO").sum()
    n_alto   = (df_q["severidade"] == "ALTO").sum()
    n_aceit  = (df_q["severidade"] == "ACEITÁVEL").sum()
    score    = df_q["percentual_validade"].mean()
    nivel    = "🔴 CRÍTICO" if n_crit else "🟠 ALTO" if n_alto else "🟢 ACEITÁVEL"
    print(f"  {label:<12} {n_total:>8} {n_crit:>9} {n_alto:>6} {n_aceit:>11} {score:.1%}")
    print(f"  {'':12} Nível geral: {nivel}")
    print(f"  {'':12} run_id: {run_id[:12]}...")
    print()

print("  THRESHOLDS DMBOK:")
print("  ├─ CRÍTICO  : < 80%  → métrica comprometida")
print("  ├─ ALTO     : 80–90% → limitação obrigatória")
print("  ├─ MÉDIO    : 90–95% → monitorar")
print("  ├─ BAIXO    : 95–99% → impacto pequeno")
print("  └─ ACEITÁVEL: ≥ 99%  → nenhuma ação")
print()
print("  COLUNAS COM ALERTAS ALTO/MÉDIO (Bronze):")
alertas = df_brz[df_brz["severidade"].isin(["CRÍTICO","ALTO","MÉDIO"])]
for _, r in alertas.iterrows():
    print(f"  ⚠ {r['dimensao_dmbok']:<15} {r['coluna_avaliada']:<20} "
          f"{r['severidade']:<10} {r['percentual_validade']:.1%}")
print()
print("  Como adicionar ao Databricks Dashboard:")
print("  Workspace → notebook → ⋮ → Add to Dashboard")
print("=" * 72)

dbutils.notebook.exit("SUCESSO|dashboard_qualidade|7_graficos_gerados")
