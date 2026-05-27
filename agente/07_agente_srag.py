# Databricks notebook source
# MAGIC %md
# MAGIC # 07 — Agente Epidemiológico SRAG | Databricks App
# MAGIC
# MAGIC **PoC:** HealthCare Indicium — Monitoramento de SRAG  
# MAGIC **LLM:** Databricks Model Serving (Foundation Model — sem chave externa)  
# MAGIC **Interface:** Databricks App (Gradio hospedado no workspace)  
# MAGIC **Dependências:** Todas as tabelas Gold devem estar gravadas antes de rodar.
# MAGIC
# MAGIC ---
# MAGIC ### Como funciona
# MAGIC - O LLM roda via **Databricks Model Serving** — sem API key
# MAGIC - A interface web é servida como **Databricks App** dentro do workspace
# MAGIC - As queries nas tabelas Gold usam **Spark diretamente** — sem conector externo
# MAGIC - Tudo dentro do ecossistema Databricks

# COMMAND ----------

# MAGIC %md
# MAGIC ## [Célula 1] Instalar dependências

# COMMAND ----------

# MAGIC %pip install gradio mlflow databricks-sdk feedparser requests --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## [Célula 2] Setup e Configuração

# COMMAND ----------

import uuid
import json
import re
import hashlib
import requests
import feedparser
import mlflow.deployments
from datetime import datetime, date, timezone
from pyspark.sql import functions as F

# ── Configuração ───────────────────────────────────────────────────────────────
CATALOGO   = "certificacao_indicium"
SCHEMA     = "poc_srag_datasus"

FULL_INDICADORES  = f"{CATALOGO}.{SCHEMA}.gold_agg_poc_srag_indicadores_relatorio"
FULL_AGG_DIARIA   = f"{CATALOGO}.{SCHEMA}.gold_agg_poc_srag_metricas_diarias"
FULL_AGG_MENSAL   = f"{CATALOGO}.{SCHEMA}.gold_agg_poc_srag_metricas_mensais"
FULL_QUALITY      = f"{CATALOGO}.{SCHEMA}.gold_quality_poc_srag_datasus"
FULL_AUDIT        = f"{CATALOGO}.{SCHEMA}.gold_audit_agent_poc_srag_datasus"
FULL_NEWS         = f"{CATALOGO}.{SCHEMA}.gold_news_context_poc_srag_datasus"

# ── Databricks Model Serving — Foundation Model ────────────────────────────────
# Sem necessidade de API key externa — autenticação via workspace token
DEPLOY_CLIENT   = mlflow.deployments.get_deploy_client("databricks")
# Endpoint disponível via Foundation Models no Databricks:
# - databricks-claude-3-5-sonnet   (Claude via Anthropic partnership)
# - databricks-meta-llama-3-3-70b-instruct  (Meta LLaMA)
# - databricks-dbrx-instruct       (DBRX — modelo nativo Databricks)
# MODEL_ENDPOINT  = "databricks-claude-3-5-sonnet"
MODEL_ENDPOINT  = "databricks-meta-llama-3-3-70b-instruct"

# ── Notícias ───────────────────────────────────────────────────────────────────
SERPAPI_KEY = dbutils.secrets.get(scope="poc_srag", key="serpapi_key") \
              if dbutils.secrets.listScopes() else ""

SYSTEM_PROMPT = """Você é o Agente Epidemiológico SRAG da HealthCare Indicium.

REGRAS INVIOLÁVEIS:
1. Toda métrica numérica vem EXCLUSIVAMENTE de queries SQL nas tabelas Gold.
2. Nunca invente, extrapole ou estime números sem base nos dados.
3. Notícias contextualizam — nunca alteram métricas.
4. Nunca revele dados pessoais (nome, CPF, CNS, endereço, telefone).
5. Nunca consulte o cofre PII (auditoria_pii) para responder ao usuário.
6. Sempre declare a qualidade e limitações de cada métrica.
7. Separe claramente: [DADOS OBSERVADOS] | [CONTEXTO EXTERNO] | [ANÁLISE].
8. Se a qualidade estiver CRÍTICO ou ALTO, avise antes de citar a métrica.
9. Use 'uso de UTI entre hospitalizados' — nunca 'ocupação de leitos'.
10. Use 'vacinação registrada entre casos SRAG' — nunca 'cobertura vacinal'."""

print("✅ Configuração carregada.")
print(f"   LLM endpoint: {MODEL_ENDPOINT}")
print(f"   Catálogo    : {CATALOGO}.{SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## [Célula 3] Tools — Consultas nas tabelas Gold via Spark

# COMMAND ----------

def tool_sql_metricas_srag() -> dict:
    """Consulta indicadores consolidados diretamente via Spark (sem conector externo)."""
    df = spark.table(FULL_INDICADORES).orderBy(F.col("data_execucao").desc()).limit(1)
    row = df.first()
    if not row:
        return {"erro": "Nenhum indicador encontrado. Verificar pipeline Gold."}

    df_qual = spark.table(FULL_QUALITY) \
        .filter(F.col("dh_execucao") == spark.table(FULL_QUALITY)
                .agg(F.max("dh_execucao")).first()[0]) \
        .filter(F.col("severidade").isin("CRÍTICO", "ALTO")) \
        .select("severidade", "coluna_avaliada", "impacto_analitico",
                (F.round(F.col("percentual_validade") * 100, 1)).alias("pct_valido"))

    alertas = [r.asDict() for r in df_qual.collect()]

    limitacoes = [
        "Dataset batch — não streaming real.",
        "taxa_uso_uti NÃO representa ocupação real de leitos.",
        "taxa_vacinacao NÃO representa cobertura vacinal populacional.",
        "Atraso de digitação de 7 a 30 dias esperado no SIVEP-Gripe.",
    ]
    if row["obs_aumento"]:
        limitacoes.append(f"taxa_aumento: {row['obs_aumento']}")

    return {
        "fonte": FULL_INDICADORES,
        "data_referencia": str(row["data_referencia"]),
        "data_maxima_dataset": str(row["data_maxima_dataset"]),
        "defasagem_dias": row["defasagem_dias"],
        "indicadores": {
            "casos_ultimos_7_dias":         row["casos_ultimos_7_dias"],
            "casos_7_dias_anteriores":      row["casos_7_dias_anteriores"],
            "casos_ultimos_30_dias":        row["casos_ultimos_30_dias"],
            "obitos_ultimos_30_dias":       row["obitos_ultimos_30_dias"],
            "taxa_aumento_casos_7d_pct":    round((row["taxa_aumento_casos_7d"] or 0) * 100, 2),
            "taxa_mortalidade_30d_pct":     round((row["taxa_mortalidade_30d"] or 0) * 100, 2),
            "taxa_uso_uti_30d_pct":         round((row["taxa_uso_uti_30d"] or 0) * 100, 2),
            "taxa_vacinacao_gripe_30d_pct": round((row["taxa_vacinacao_gripe_30d"] or 0) * 100, 2),
            "taxa_vacinacao_covid_30d_pct": round((row["taxa_vacinacao_covid_30d"] or 0) * 100, 2),
        },
        "confiabilidade": row["observacao_confiabilidade"],
        "alertas_qualidade": alertas,
        "limitacoes": limitacoes,
    }


def tool_series_temporais_srag() -> dict:
    """Consulta séries diária e mensal via Spark."""
    serie_diaria = spark.table(FULL_AGG_DIARIA) \
        .filter(F.col("data_referencia") >= F.date_sub(F.current_date(), 30)) \
        .select("data_referencia", "total_casos", "total_obitos", "total_uti",
                F.round(F.col("taxa_mortalidade") * 100, 2).alias("taxa_mortalidade_pct"),
                F.round(F.col("taxa_uso_uti") * 100, 2).alias("taxa_uso_uti_pct"),
                F.round(F.col("media_movel_7d_casos"), 1).alias("media_movel_7d")) \
        .orderBy("data_referencia") \
        .collect()

    serie_mensal = spark.table(FULL_AGG_MENSAL) \
        .filter(F.col("ano_mes") >= F.date_format(F.add_months(F.current_date(), -12), "yyyy-MM")) \
        .select("ano_mes", "total_casos", "total_obitos", "total_uti", "total_hospitalizados",
                F.round(F.col("taxa_mortalidade") * 100, 2).alias("taxa_mortalidade_pct"),
                F.round(F.col("taxa_uso_uti") * 100, 2).alias("taxa_uso_uti_pct")) \
        .orderBy("ano_mes") \
        .collect()

    return {
        "serie_diaria_30d": [r.asDict() for r in serie_diaria],
        "serie_mensal_12m": [r.asDict() for r in serie_mensal],
        "notas": ["Eixo temporal baseado em dt_sin_pri.", "Dados recentes podem estar subestimados."],
    }


def tool_data_quality_srag() -> dict:
    """Consulta alertas de qualidade via Spark."""
    dh_max = spark.table(FULL_QUALITY).agg(F.max("dh_execucao")).first()[0]
    df = spark.table(FULL_QUALITY) \
        .filter(F.col("dh_execucao") == dh_max) \
        .filter(F.col("severidade").isin("CRÍTICO", "ALTO"))

    alertas = [r.asDict() for r in df.collect()]
    criticos = sum(1 for a in alertas if a["severidade"] == "CRÍTICO")
    altos    = sum(1 for a in alertas if a["severidade"] == "ALTO")

    return {
        "nivel_geral": "CRÍTICO" if criticos > 0 else ("ALTO" if altos > 0 else "ACEITÁVEL"),
        "criticos": criticos,
        "altos":    altos,
        "alertas":  alertas,
    }


CONFIABILIDADE_DOMINIOS = {
    "gov.br": "Alta", "paho.org": "Alta", "who.int": "Alta",
    "fiocruz.br": "Alta", "g1.globo.com": "Média-Alta",
}

TERMOS_BUSCA = [
    "SRAG síndrome respiratória aguda grave 2025",
    "influenza gripe surto Brasil 2025",
    "COVID-19 surto respiratório Brasil 2025",
    "alerta epidemiológico Ministério Saúde",
]

def _inferir_conf(url: str) -> str:
    for d, n in CONFIABILIDADE_DOMINIOS.items():
        if d in url: return n
    return "Média"

def tool_news_srag(run_id: str) -> list[dict]:
    """Busca notícias e grava em gold_news_context."""
    noticias = []

    # RSS Ministério da Saúde
    try:
        feed = feedparser.parse("https://www.gov.br/saude/pt-br/assuntos/saude-de-a-a-z/feed")
        for e in feed.entries[:5]:
            noticias.append({
                "titulo": e.get("title",""), "fonte": "Ministério da Saúde",
                "data_publicacao": e.get("published",""), "url": e.get("link",""),
                "resumo": e.get("summary","")[:300], "confiabilidade": "Alta",
                "termo_busca": "RSS Ministério da Saúde",
            })
    except Exception:
        pass

    # SerpAPI (se configurado)
    if SERPAPI_KEY:
        for termo in TERMOS_BUSCA[:3]:
            try:
                r = requests.get("https://serpapi.com/search",
                    params={"q": termo, "tbm": "nws", "num": 3, "tbs": "qdr:w",
                            "api_key": SERPAPI_KEY}, timeout=10)
                if r.status_code == 200:
                    for n in r.json().get("news_results", []):
                        noticias.append({
                            "titulo": n.get("title",""), "fonte": n.get("source",""),
                            "data_publicacao": n.get("date",""), "url": n.get("link",""),
                            "resumo": n.get("snippet",""), "termo_busca": termo,
                            "confiabilidade": _inferir_conf(n.get("link","")),
                        })
            except Exception:
                pass

    # Deduplicar por URL
    vistas, unicas = set(), []
    for n in noticias:
        if n["url"] not in vistas:
            vistas.add(n["url"])
            unicas.append(n)

    # Gravar em gold_news_context via Spark
    if unicas:
        rows = [(
            hashlib.md5(n["url"].encode()).hexdigest(), run_id,
            n.get("termo_busca",""), n.get("titulo","")[:200],
            n.get("fonte","")[:100], n.get("data_publicacao","")[:50],
            n.get("url","")[:500], n.get("resumo","")[:500],
            n.get("confiabilidade","Média"), True,
            datetime.now(timezone.utc).isoformat(),
        ) for n in unicas[:20]]

        spark.createDataFrame(rows, schema=[
            "news_id","run_id","termo_busca","titulo","fonte",
            "data_publicacao","url","resumo","confiabilidade_fonte",
            "usado_no_relatorio","dh_consulta"
        ]).write.format("delta").mode("append").saveAsTable(FULL_NEWS)

    return unicas[:15]


# Guardrails
TERMOS_BLOQUEADOS = [
    "quem é o paciente","qual é o nome","cpf do paciente","cns do paciente",
    "nome completo","mostre os nomes","identifique o paciente","dados do paciente",
    "auditoria_pii","mapa_identificacao","mapa pii",
]
TERMOS_ENGANOSOS = {
    "ocupação de leitos": "uso de UTI registrado entre casos SRAG hospitalizados",
    "ocupação de UTI": "uso de UTI registrado entre casos SRAG hospitalizados",
    "cobertura vacinal": "vacinação registrada entre casos SRAG",
    "cobertura de vacinação": "vacinação registrada entre casos SRAG",
}
PII_PATTERNS = [
    re.compile(r"\b\d{3}[\.\-]?\d{3}[\.\-]?\d{3}[\.\-]?\d{2}\b"),
    re.compile(r"\b\d{15}\b"),
]

def tool_guardrails_lgpd(texto: str, modo: str = "input") -> dict:
    t = texto.lower()
    if modo == "input":
        for termo in TERMOS_BLOQUEADOS:
            if termo in t:
                return {"aprovado": False, "motivo": f"Bloqueado por LGPD: '{termo}' detectado.", "texto": None}
        return {"aprovado": True, "motivo": None, "texto": texto}
    if modo == "output":
        for p in PII_PATTERNS:
            if p.search(texto):
                return {"aprovado": False, "motivo": "PII detectado na resposta.", "texto": None}
        corrigido = texto
        for errado, correto in TERMOS_ENGANOSOS.items():
            corrigido = re.sub(re.escape(errado), correto, corrigido, flags=re.IGNORECASE)
        return {"aprovado": True, "motivo": None, "texto": corrigido}
    return {"aprovado": True, "motivo": None, "texto": texto}


def tool_audit_logger(run_id, usuario, pergunta, tools, queries, noticias, resposta, guardrails, status):
    """Grava auditoria via Spark."""
    row = [(
        str(uuid.uuid4()), run_id, usuario,
        pergunta[:500], json.dumps(tools)[:1000], json.dumps(queries)[:2000],
        json.dumps(noticias)[:1000], resposta[:500],
        json.dumps(guardrails)[:500], status,
        datetime.now(timezone.utc).isoformat(),
    )]
    spark.createDataFrame(row, schema=[
        "audit_id","run_id","usuario","pergunta","tools_acionadas",
        "queries_executadas","fontes_noticias_consultadas","resposta_resumida",
        "guardrails_acionados","status_execucao","dh_execucao"
    ]).write.format("delta").mode("append").saveAsTable(FULL_AUDIT)


print("✅ Todas as tools carregadas (via Spark + Databricks Model Serving).")

# COMMAND ----------

# MAGIC %md
# MAGIC ## [Célula 4] LLM via Databricks Model Serving

# COMMAND ----------

def chamar_llm(prompt_usuario: str, contexto: dict) -> str:
    """
    Chama o LLM via Databricks Model Serving.
    Sem API key externa — autenticação via workspace token.
    """
    ind  = contexto.get("indicadores", {}).get("indicadores", {})
    qual = contexto.get("qualidade", {})
    news = contexto.get("noticias", [])[:5]

    resumo_metricas = (
        f"Casos últimos 7 dias: {ind.get('casos_ultimos_7_dias')} | "
        f"Taxa de aumento 7d: {ind.get('taxa_aumento_casos_7d_pct')}% | "
        f"Taxa mortalidade 30d: {ind.get('taxa_mortalidade_30d_pct')}% | "
        f"Taxa uso UTI 30d: {ind.get('taxa_uso_uti_30d_pct')}%"
    )
    resumo_news = "\n".join(
        f"- [{n.get('fonte')}] {n.get('titulo')} ({n.get('data_publicacao')})"
        for n in news
    ) or "Nenhuma notícia encontrada."
    resumo_qual = (
        f"Nível: {qual.get('nivel_geral')} | "
        f"Críticos: {qual.get('criticos')} | Altos: {qual.get('altos')}"
    )

    prompt_completo = f"""
Pergunta do usuário: {prompt_usuario}

[DADOS OBSERVADOS — fonte: tabelas Gold SRAG]
{resumo_metricas}

[QUALIDADE DOS DADOS]
{resumo_qual}

[CONTEXTO EXTERNO — notícias recentes]
{resumo_news}

Gere um comentário analítico epidemiológico em 3 parágrafos:
1. O que os dados mostram (métricas + tendência).
2. O que o contexto externo (notícias) sugere.
3. Conclusão com limitações declaradas.
"""

    resp = DEPLOY_CLIENT.predict(
        endpoint=MODEL_ENDPOINT,
        inputs={
            "messages": [
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user",   "content": prompt_completo},
            ],
            "max_tokens": 1024,
            "temperature": 0.1,
        }
    )
    return resp["choices"][0]["message"]["content"]


print(f"✅ LLM configurado via Databricks Model Serving: {MODEL_ENDPOINT}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## [Célula 5] Orquestrador — Fluxo de 10 etapas

# COMMAND ----------

def executar_agente(pergunta: str, usuario: str = "anonimo") -> str:
    """
    Orquestra o fluxo completo do agente epidemiológico.
    Tudo roda dentro do Databricks — sem dependência externa além das notícias.
    """
    run_id     = str(uuid.uuid4())
    tools_usadas  = []
    queries_usadas = []
    guardrails_acionados = []

    # 1. Guardrail input
    guard_in = tool_guardrails_lgpd(pergunta, modo="input")
    if not guard_in["aprovado"]:
        guardrails_acionados.append("G1_input_bloqueado")
        relatorio = f"# Solicitação Bloqueada\n\n{guard_in['motivo']}"
        tool_audit_logger(run_id, usuario, pergunta, [], [], [], relatorio,
                          guardrails_acionados, "BLOQUEADO")
        return relatorio

    # 2. Buscar métricas
    indicadores = tool_sql_metricas_srag()
    tools_usadas.append("tool_sql_metricas_srag")
    queries_usadas.append(FULL_INDICADORES)

    # 3. Buscar séries temporais
    series = tool_series_temporais_srag()
    tools_usadas.append("tool_series_temporais_srag")

    # 4. Buscar qualidade
    qualidade = tool_data_quality_srag()
    tools_usadas.append("tool_data_quality_srag")
    queries_usadas.append(FULL_QUALITY)

    # 5. Buscar notícias
    noticias = tool_news_srag(run_id)
    tools_usadas.append("tool_news_srag")
    fontes_news = list({n.get("fonte","") for n in noticias[:5]})

    # 6. LLM sintetiza
    contexto = {"indicadores": indicadores, "qualidade": qualidade, "noticias": noticias}
    comentario = chamar_llm(pergunta, contexto)

    # 7. Guardrail output
    guard_out = tool_guardrails_lgpd(comentario, modo="output")
    if not guard_out["aprovado"]:
        guardrails_acionados.append("G2_output_bloqueado")
        comentario = "⚠️ Resposta bloqueada pelos guardrails de privacidade."
    else:
        comentario = guard_out["texto"]

    # 8. Gerar relatório
    relatorio = gerar_relatorio(indicadores, series, qualidade, noticias,
                                pergunta, comentario)

    # 9. Gravar auditoria
    tool_audit_logger(run_id, usuario, pergunta, tools_usadas, queries_usadas,
                      fontes_news, relatorio, guardrails_acionados, "SUCESSO")

    return relatorio


def gerar_relatorio(indicadores, series, qualidade, noticias, pergunta, comentario) -> str:
    ind    = indicadores.get("indicadores", {})
    hoje   = datetime.utcnow().strftime("%d/%m/%Y %H:%M UTC")
    nivel_qual = qualidade.get("nivel_geral", "ACEITÁVEL")

    aviso_qual = ""
    if nivel_qual in ("CRÍTICO", "ALTO"):
        aviso_qual = f"\n> ⚠️ **Aviso de Qualidade ({nivel_qual}):** Algumas métricas podem estar afetadas.\n"

    noticias_md = ""
    for n in noticias[:5]:
        noticias_md += (
            f"- **{n.get('titulo','')}**\n"
            f"  Fonte: {n.get('fonte','')} | {n.get('data_publicacao','')}\n"
            f"  {n.get('resumo','')[:200]}\n"
            f"  [Ver notícia]({n.get('url','')})\n\n"
        )

    def fmt(v):
        return f"{v:.2f}%" if v is not None else "N/D"

    return f"""# Relatório Epidemiológico SRAG — Brasil
## {hoje}
{aviso_qual}

---

## 1. Identificação

| Campo | Valor |
|---|---|
| **Fonte** | Open DATASUS / SIVEP-Gripe |
| **Versão** | INFLUD25_DATASUS-Versao26-06-2025 |
| **Data máxima dos registros** | {indicadores.get('data_maxima_dataset', 'N/D')} |
| **Defasagem** | {indicadores.get('defasagem_dias', 'N/D')} dias |
| **Gerado em** | {hoje} |

---

## 2. Resumo Executivo

{comentario}

---

## 3. Métricas Principais (Janela 30 dias)

| Indicador | Valor |
|---|---|
| Casos últimos 7 dias | {ind.get('casos_ultimos_7_dias', 'N/D'):,} |
| Casos 7 dias anteriores | {ind.get('casos_7_dias_anteriores', 'N/D'):,} |
| **Taxa de aumento de casos (7d)** | {fmt(ind.get('taxa_aumento_casos_7d_pct'))} |
| Casos últimos 30 dias | {ind.get('casos_ultimos_30_dias', 'N/D'):,} |
| Óbitos últimos 30 dias | {ind.get('obitos_ultimos_30_dias', 'N/D'):,} |
| **Taxa de mortalidade (30d)** | {fmt(ind.get('taxa_mortalidade_30d_pct'))} |
| **Taxa de uso de UTI entre hospitalizados (30d)** ⚠️ | {fmt(ind.get('taxa_uso_uti_30d_pct'))} |
| **Taxa de vacinação gripe registrada (30d)** ⚠️ | {fmt(ind.get('taxa_vacinacao_gripe_30d_pct'))} |
| **Taxa de vacinação COVID registrada (30d)** ⚠️ | {fmt(ind.get('taxa_vacinacao_covid_30d_pct'))} |

> ⚠️ Taxas marcadas com ⚠️ possuem limitações — ver seção Limitações.

---

## 4. [CONTEXTO EXTERNO] Notícias Recentes

{noticias_md if noticias_md else "_Nenhuma notícia encontrada._"}

---

## 5. Qualidade dos Dados

**Nível geral:** `{nivel_qual}`

| Severidade | Alertas |
|---|---|
| CRÍTICO | {qualidade.get('criticos', 0)} |
| ALTO | {qualidade.get('altos', 0)} |

---

## 6. Limitações

1. Dataset batch — não streaming real.
2. `taxa_uso_uti` ≠ ocupação real de leitos de UTI.
3. `taxa_vacinacao` ≠ cobertura vacinal populacional.
4. Atraso de digitação de 7-30 dias esperado no SIVEP-Gripe.
5. Dados ausentes podem afetar indicadores.
6. Notícias são contexto externo, não fonte de métricas.

---

## 7. Confiabilidade

{indicadores.get('confiabilidade', 'N/D')}

---
*Relatório gerado pelo Agente Epidemiológico SRAG via Databricks Model Serving.*  
*Dados: Open DATASUS (domínio público). Nenhum dado pessoal foi exposto.*
"""

print("✅ Orquestrador carregado.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## [Célula 6] Interface Gradio (Databricks App)

# COMMAND ----------

import gradio as gr

def responder(pergunta: str, usuario: str) -> str:
    if not pergunta or len(pergunta.strip()) < 5:
        return "⚠️ Pergunta muito curta. Por favor, detalhe sua consulta."
    if len(pergunta) > 1000:
        return "⚠️ Pergunta muito longa (máx 1000 caracteres)."
    try:
        return executar_agente(pergunta, usuario or "anonimo")
    except Exception as e:
        return f"❌ Erro no agente: {str(e)}"

def gerar_relatorio_completo(usuario: str) -> str:
    pergunta = (
        "Qual é a situação atual do SRAG no Brasil? "
        "Inclua métricas de casos, mortalidade, UTI e vacinação, "
        "além de notícias recentes relevantes."
    )
    return responder(pergunta, usuario)

# Gradio 6.0+: theme movido para launch()
with gr.Blocks(title="Agente Epidemiológico SRAG") as app:
    gr.Markdown("""
    # 🏥 Agente Epidemiológico SRAG
    ### HealthCare Indicium — Monitoramento em Tempo Quase Real
    **Fonte:** Open DATASUS / SIVEP-Gripe | **LLM:** Databricks Model Serving
    """)

    with gr.Tab("💬 Consulta Livre"):
        usuario_1 = gr.Textbox(label="Seu nome (para auditoria)", value="analista", scale=1)
        pergunta  = gr.Textbox(
            label="Pergunta epidemiológica",
            placeholder="Ex: Qual a taxa de mortalidade por SRAG nos últimos 30 dias?",
            lines=3,
        )
        btn_ask = gr.Button("🔍 Consultar", variant="primary")
        resposta = gr.Markdown(label="Relatório do Agente")
        btn_ask.click(fn=responder, inputs=[pergunta, usuario_1], outputs=resposta)

    with gr.Tab("📊 Relatório Completo"):
        usuario_2 = gr.Textbox(label="Seu nome (para auditoria)", value="analista", scale=1)
        btn_report = gr.Button("📋 Gerar Relatório Epidemiológico", variant="primary")
        relatorio  = gr.Markdown(label="Relatório")
        btn_report.click(fn=gerar_relatorio_completo, inputs=[usuario_2], outputs=relatorio)

    gr.Markdown("""
    ---
    ⚠️ **Guardrails ativos:** Identificação de pacientes bloqueada | PII protegida | Métricas apenas de dados reais
    """)

# ── Para rodar como Databricks App (RECOMENDADO): ──────────────────────────────
# 1. No workspace: Apps → Create App → selecionar este notebook
# 2. O Databricks gerencia o servidor automaticamente
# 3. A variável 'app' é exposta automaticamente — NÃO chamar app.launch()
#
# ── Para testar no notebook diretamente (OPCIONAL): ────────────────────────────
# Descomente as linhas abaixo SOMENTE para teste local no notebook:
#
# app.launch(
#     server_name="0.0.0.0",
#     server_port=7860,
#     share=False,
#     prevent_thread_lock=True
# )

print("✅ Interface Gradio configurada.")
print("   Para Databricks App: Apps → Create App → selecionar este notebook")
print("   A variável 'app' está pronta para ser servida pelo Databricks.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Como transformar em Databricks App
# MAGIC
# MAGIC 1. No Databricks workspace: **Apps → Create App**
# MAGIC 2. Selecionar **"From Notebook"**
# MAGIC 3. Escolher este notebook (`07_agente_srag.py`)
# MAGIC 4. O Databricks gera uma URL pública dentro do workspace
# MAGIC 5. Compartilhar a URL com a banca avaliadora
# MAGIC
# MAGIC ### Requisitos no workspace
# MAGIC - Cluster com DBR 14.x+
# MAGIC - Foundation Model `databricks-meta-llama-3-3-70b-instruct` habilitado
# MAGIC - Secret scope `poc_srag` com chave `serpapi_key` (opcional — notícias)
