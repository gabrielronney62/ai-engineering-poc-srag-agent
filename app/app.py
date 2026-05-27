import os
import uuid
import json
import re
import base64
import requests
import feedparser
from datetime import datetime, timezone


# ── Spark via Serverless (obrigatório em Databricks Apps) ─────────────────────
from databricks.connect import DatabricksSession
from pyspark.sql import functions as F

spark = DatabricksSession.builder.serverless(True).getOrCreate()

# ── Databricks SDK para Model Serving ─────────────────────────────────────────
from databricks.sdk import WorkspaceClient
w = WorkspaceClient()

# ── Gradio ────────────────────────────────────────────────────────────────────
import gradio as gr

# ── Parâmetros ────────────────────────────────────────────────────────────────
CATALOGO         = "certificacao_indicium"
SCHEMA           = "poc_srag_datasus"
FULL_INDICADORES = f"{CATALOGO}.{SCHEMA}.gold_agg_poc_srag_indicadores_relatorio"
FULL_AGG_DIARIA  = f"{CATALOGO}.{SCHEMA}.gold_agg_poc_srag_metricas_diarias"
FULL_AGG_MENSAL  = f"{CATALOGO}.{SCHEMA}.gold_agg_poc_srag_metricas_mensais"
FULL_QUALITY     = f"{CATALOGO}.{SCHEMA}.gold_quality_poc_srag_datasus"
FULL_AUDIT       = f"{CATALOGO}.{SCHEMA}.gold_audit_agent_poc_srag_datasus"
FULL_NEWS        = f"{CATALOGO}.{SCHEMA}.gold_news_context_poc_srag_datasus"

MODEL_ENDPOINT   = "databricks-meta-llama-3-3-70b-instruct"

# SerpAPI via variável de ambiente (definida nas configurações do App)
# Ler API KEY via Databricks SDK
# SerpAPI via Databricks Secrets
# SerpAPI via Databricks Secrets
try:
    secret_b64 = w.secrets.get_secret(
        scope="poc_srag", key="serpapi_key"
    ).value
    SERPAPI_KEY = base64.b64decode(secret_b64).decode("utf-8")
    print(f"✅ SerpAPI key carregada: {SERPAPI_KEY[:8]}...")
except Exception as e:
    SERPAPI_KEY = ""
    print(f"⚠️ SerpAPI key não encontrada: {e}")

TERMOS_BUSCA = [
    "SRAG síndrome respiratória aguda grave 2025",
    "influenza gripe surto Brasil 2025",
    "COVID-19 surto respiratório Brasil 2025",
]
TERMOS_BLOQUEADOS = [
    "nome do paciente", "cpf", "rg", "endereço", "telefone",
    "identific", "quem é", "dados pessoais", "prontuário",
]

SYSTEM_PROMPT = """Você é um agente epidemiológico especializado em SRAG no Brasil.
Suas respostas são baseadas EXCLUSIVAMENTE nos dados do SIVEP-Gripe (Open DATASUS).

REGRAS:
1. Nunca identifique pacientes individuais — dados são agregados
2. Sempre cite a fonte: Open DATASUS / SIVEP-Gripe
3. Declare limitações: atraso de digitação de 7-30 dias é esperado
4. Separe: [DADOS OBSERVADOS] / [CONTEXTO EXTERNO] / [ANÁLISE]
5. Taxas calculadas apenas sobre casos com desfecho informado

Responda sempre em português brasileiro."""

# ── Tools ─────────────────────────────────────────────────────────────────────
def tool_metricas() -> str:
    try:
        row = spark.table(FULL_INDICADORES).orderBy(
            F.col("data_execucao").desc()
        ).limit(1).first()
        if not row:
            return "Sem dados disponíveis."
        return (
            f"[DADOS OBSERVADOS — Open DATASUS]\n"
            f"Data referência     : {row['data_maxima_dataset']}\n"
            f"Defasagem           : {row['defasagem_dias']} dias\n"
            f"Casos últimos 7d    : {int(row['casos_ultimos_7_dias'] or 0):,}\n"
            f"Casos 7d anteriores : {int(row['casos_7_dias_anteriores'] or 0):,}\n"
            f"Taxa aumento 7d     : {row['taxa_aumento_casos_7d']:.1%}\n"
            f"Casos 30d           : {int(row['casos_ultimos_30_dias'] or 0):,}\n"
            f"Óbitos 30d          : {int(row['obitos_ultimos_30_dias'] or 0):,}\n"
            f"Taxa mortalidade 30d: {row['taxa_mortalidade_30d']:.1%}\n"
            f"Taxa UTI 30d        : {row['taxa_uso_uti_30d']:.1%}\n"
            f"Vac. gripe 30d      : {row['taxa_vacinacao_gripe_30d']:.1%}\n"
            f"Vac. COVID 30d      : {row['taxa_vacinacao_covid_30d']:.1%}\n"
            f"Confiabilidade      : {row['observacao_confiabilidade']}\n"
        )
    except Exception as e:
        return f"Erro ao buscar métricas: {e}"


def tool_noticias() -> str:
    noticias  = []
    registros = []
    dh_busca  = datetime.now(timezone.utc)
    run_id_news = str(uuid.uuid4())

    # RSS Ministério da Saúde
    try:
        feed = feedparser.parse("https://www.gov.br/saude/pt-br/rss.xml")
        for entry in feed.entries[:5]:
            titulo = entry.get("title", "")
            if any(t in titulo.lower() for t in
                   ["srag","gripe","influenza","surto","epidemia","síndrome respiratória"]):
                url  = entry.get("link", "")
                data = entry.get("published", "")[:50]
                noticias.append(f"• {titulo} ({data[:10]})")
                registros.append((
                    __import__('hashlib').md5(url.encode()).hexdigest(),
                    run_id_news, "RSS Ministério da Saúde",
                    titulo[:200], "Ministério da Saúde",
                    data[:50], url[:500], "", "Alta", True,
                    dh_busca.isoformat(),
                ))
    except Exception:
        pass

    # SerpAPI
    if SERPAPI_KEY:
        for termo in TERMOS_BUSCA[:2]:
            try:
                resp = requests.get(
                    "https://serpapi.com/search",
                    params={"q": termo, "tbm": "nws", "num": 3,
                            "api_key": SERPAPI_KEY, "hl": "pt"},
                    timeout=8
                )
                for r in resp.json().get("news_results", []):
                    titulo = r.get("title", "")
                    url    = r.get("link", "")
                    data   = r.get("date", "")
                    noticias.append(f"• {titulo} ({data})")
                    registros.append((
                        __import__('hashlib').md5(url.encode()).hexdigest(),
                        run_id_news, termo[:200],
                        titulo[:200], r.get("source","")[:100],
                        data[:50], url[:500],
                        r.get("snippet","")[:500], "Média", True,
                        dh_busca.isoformat(),
                    ))
            except Exception:
                pass

    # Gravar em Delta
    if registros:
        try:
            spark.createDataFrame(registros, schema=[
                "news_id","run_id","termo_busca","titulo","fonte",
                "data_publicacao","url","resumo","confiabilidade_fonte",
                "usado_no_relatorio","dh_consulta"
            ]).write.format("delta").mode("append").saveAsTable(FULL_NEWS)
        except Exception:
            pass

    if not noticias:
        return "[CONTEXTO EXTERNO]\nSem notícias recentes disponíveis."
    return "[CONTEXTO EXTERNO — RSS/Google News]\n" + "\n".join(noticias[:8])


def guardrail_lgpd(pergunta: str) -> str | None:
    pq = pergunta.lower()
    for termo in TERMOS_BLOQUEADOS:
        if termo in pq:
            return (
                "⚠️ Esta consulta envolve identificação de pacientes individuais. "
                "Por conformidade com a LGPD, só trabalho com dados agregados."
            )
    if re.search(r'\d{3}[\.\-]?\d{3}[\.\-]?\d{3}[\.\-]?\d{2}', pergunta):
        return "⚠️ CPF detectado. Não processo consultas com dados pessoais."
    return None


def chamar_llm(mensagens: list) -> str:
    try:
        # w.api_client.do() usa OAuth do App automaticamente
        # É o cliente HTTP interno do SDK — sem tokens manuais
        result = w.api_client.do(
            "POST",
            f"/serving-endpoints/{MODEL_ENDPOINT}/invocations",
            body={
                "messages":    mensagens,
                "max_tokens":  1200,
                "temperature": 0.1,
            }
        )
        return result["choices"][0]["message"]["content"]
    except Exception as e:
        return f"❌ Erro no LLM: {e}"


def executar_agente(pergunta: str, usuario: str = "anonimo") -> str:
    bloqueio = guardrail_lgpd(pergunta)
    if bloqueio:
        return bloqueio

    metricas = tool_metricas()
    noticias = tool_noticias()

    msgs = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user",   "content": (
            f"Contexto epidemiológico:\n{metricas}\n\n{noticias}\n\n"
            f"Pergunta: {pergunta}"
        )},
    ]

    resposta = chamar_llm(msgs)

    # Log de auditoria
    try:
        from pyspark.sql.types import StructType, StructField, StringType, TimestampType
        schema = StructType([
            StructField("run_id",      StringType(),    True),
            StructField("usuario",     StringType(),    True),
            StructField("pergunta",    StringType(),    True),
            StructField("resposta",    StringType(),    True),
            StructField("dh_consulta", TimestampType(), True),
        ])
        audit = spark.createDataFrame([{
            "run_id":      str(uuid.uuid4()),
            "usuario":     usuario[:100],
            "pergunta":    pergunta[:500],
            "resposta":    resposta[:1000],
            "dh_consulta": datetime.now(timezone.utc),
        }], schema=schema)
        audit.write.format("delta").mode("append").saveAsTable(FULL_AUDIT)
    except Exception:
        pass  # Log não crítico

    return resposta


# ── Interface Gradio ──────────────────────────────────────────────────────────
def responder(pergunta: str, usuario: str) -> str:
    if not pergunta or len(pergunta.strip()) < 5:
        return "⚠️ Pergunta muito curta. Por favor, detalhe sua consulta."
    if len(pergunta) > 1000:
        return "⚠️ Pergunta muito longa (máx 1000 caracteres)."
    try:
        return executar_agente(pergunta, usuario or "anonimo")
    except Exception as e:
        return f"❌ Erro no agente: {str(e)}"


def gerar_relatorio(usuario: str) -> str:
    return responder(
        "Qual é a situação atual do SRAG no Brasil? "
        "Inclua métricas de casos, mortalidade, UTI e vacinação, "
        "e notícias recentes relevantes.",
        usuario
    )


with gr.Blocks(title="Agente Epidemiológico SRAG") as app:
    gr.Markdown("""
    # 🏥 Agente Epidemiológico SRAG
    ### HealthCare Indicium — Monitoramento em Tempo Quase Real
    **Fonte:** Open DATASUS / SIVEP-Gripe | **LLM:** Databricks Model Serving
    """)

    with gr.Tab("💬 Consulta Livre"):
        usuario_1 = gr.Textbox(label="Seu nome (auditoria)", value="analista")
        pergunta  = gr.Textbox(
            label="Pergunta epidemiológica",
            placeholder="Ex: Qual a taxa de mortalidade nos últimos 30 dias?",
            lines=3,
        )
        btn_ask  = gr.Button("🔍 Consultar", variant="primary")
        resposta = gr.Markdown()
        btn_ask.click(fn=responder, inputs=[pergunta, usuario_1], outputs=resposta)

    with gr.Tab("📋 Relatório Completo"):
        usuario_2  = gr.Textbox(label="Seu nome (auditoria)", value="analista")
        btn_report = gr.Button("📊 Gerar Relatório", variant="primary")
        relatorio  = gr.Markdown()
        btn_report.click(fn=gerar_relatorio, inputs=[usuario_2], outputs=relatorio)

    gr.Markdown("""
    ---
    ⚠️ **Guardrails ativos:** Identificação de pacientes bloqueada | PII protegida
    """)

# Entry point — Databricks App detecta o objeto 'app' e serve automaticamente
port = int(os.environ.get("DATABRICKS_APP_PORT", 8000))
app.launch(server_name="0.0.0.0", server_port=port)