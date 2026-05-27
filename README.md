# PoC SRAG DATASUS — Agente de IA Epidemiológico

**Projeto:** PoC SRAG — HealthCare Indicium  
**Autor:** Gabriel Ronney da Silva  
**Plataforma:** Databricks Unity Catalog  
**Última atualização:** Maio/2026

---

## O que é este projeto?

Esta PoC valida a viabilidade técnica de uma solução de monitoramento epidemiológico
que combina um pipeline de dados completo com um agente de inteligência artificial
capaz de responder perguntas em linguagem natural sobre **Síndrome Respiratória Aguda
Grave (SRAG)** no Brasil.

Os dados são reais, públicos e vêm diretamente do **SIVEP-Gripe (Open DATASUS)**
— o sistema oficial de vigilância epidemiológica do Ministério da Saúde. A ideia
é que um profissional de saúde ou gestor possa perguntar, por exemplo,
*"Qual a taxa de mortalidade por SRAG nos últimos 30 dias?"* e receber uma resposta
fundamentada, com fonte declarada e limitações explícitas.

---

## Visão geral da arquitetura

```
Open DATASUS (CSV)
    ↓ Auto Loader (Notebook 01)
Bronze — dados brutos, imutáveis, 194 colunas STRING
    ↓ DMBOK Profiling (Notebook 02)
    ↓ SCD2 + Pseudoanonimização PII (Notebook 03)
Silver — dados limpos, tipados, ~80 colunas, ~440k casos únicos
    ↓ Star Schema (Gold dims + fact)
    ↓ Agregações (KPIs, séries 30d e 12m)
Gold — pronto para consumo analítico
    ↓
Agente Epidemiológico (Notebook 07 / app.py)
    → Interface Gradio via Databricks App
    → LLM: Meta Llama 3.3 70B (Databricks Model Serving)
    → Notícias em tempo real: RSS Ministério da Saúde + SerpAPI
```

O diagrama conceitual completo está em `documentacoes/arquitetura_conceitual.mmd`.

---

## Datasets utilizados

| Arquivo | Fonte | Registros | Período |
|---|---|---|---|
| `INFLUD25_DATASUS-Versao26-06-2025.csv` | SIVEP-Gripe | 165.397 | Dez/2024–Jun/2025 |
| `INFLUD25-2025.csv` | SIVEP-Gripe | 336.239 | Dez/2024–Jan/2026 |
| `INFLUD26-2026.csv` | SIVEP-Gripe | 101.425 | Jan–Mai/2026 |
| `Leitos_2025.csv` | CNES/DATASUS | 86.147 | Jan–Dez/2025 |
| `Leitos_2026.csv` | CNES/DATASUS | 28.783 | Jan–Abr/2026 |

**Total após SCD2 (deduplicação Silver):** ~440.674 registros únicos.

Os dois formatos de data do SIVEP-Gripe (`YYYY-MM-DD` e `YYYY-MM-DDTHH:MM:SS.mmmZ`)
são preservados como STRING na Bronze e normalizados para `DateType` na Silver via
`try_to_date(substring(col, 1, 10))`.

---

## Estrutura do repositório

```
certificacao_ai_engineering_poc_srag/
│
├── bronze/
│   ├── 01_bronze_ingestao_srag.py        # Auto Loader — 3 CSVs SRAG
│   └── 02_bronze_quality_srag.py         # DMBOK profiling — 40 regras, 5 dimensões
│
├── silver/
│   ├── 03_silver_tratamento_srag.py      # SCD2 + pseudoanonimização PII
│   └── 03b_silver_cnes_leitos_uti.py     # Leitos UTI por UF/mês (CNES)
│
├── gold/
│   ├── dim/
│   │   ├── gold_dim_tempo_srag.py
│   │   ├── gold_dim_localidade_srag.py
│   │   ├── gold_dim_perfil_srag.py
│   │   └── gold_dim_classificacao_srag.py
│   ├── fact/
│   │   └── gold_fact_srag.py
│   ├── agg/
│   │   ├── gold_agg_metricas_diarias_srag.py
│   │   ├── gold_agg_metricas_mensais_srag.py
│   │   └── gold_agg_indicadores_relatorio_srag.py
│   └── gold_quality_srag.py              # Qualidade consolidada Gold
│
├── validacao_metricas_srag/
│   └── 06_validacao_metricas_srag.py     # Testes unitários das métricas
│
├── agente/
│   └── 07_agente_srag.py                 # Agente orquestrador (dev/staging)
│
├── app/
│   ├── app.py                            # Databricks App — produção
│   └── requirements.txt
│
├── dashboard/
│   ├── 08_dashboard_visualizacoes_srag.py  # 6 gráficos epidemiológicos
│   └── 09_dashboard_qualidade_dmbok.py     # 7 gráficos de qualidade DMBOK
│
├── documentacoes/
│   ├── README.md                           # este arquivo
│   ├── arquitetura_conceitual.mmd          # diagrama Mermaid completo
│   ├── mapeamento_bronze_silver.md
│   ├── dicionario_colunas_silver.md
│   ├── qualidade_dados_dmbok.md
│   ├── governanca_lgpd_poc.md
│   ├── limitacoes_poc.md
│   ├── decisao_arquitetural_pii_poc.md
│   └── perguntas_agente_srag.md
│
├── databricks.yml                          # DABs — bundle de deploy
└── .gitignore
```

---

## Tabelas no Unity Catalog — `certificacao_indicium`

### Schema `poc_srag_datasus`

| Tabela | Camada | Estratégia | Descrição |
|---|---|---|---|
| `bronze_poc_srag_datasus` | Bronze | APPEND | Dados brutos — 194 colunas STRING |
| `bronze_quality_poc_srag_datasus` | Bronze | APPEND | Métricas DMBOK por execução (run_id) |
| `bronze_dados_leitos_2024_2026` | Bronze | criado pelo usuário | Leitos CNES brutos |
| `silver_poc_srag_datasus` | Silver | MERGE SCD2 | ~440k casos únicos, tipados, com hash PII |
| `silver_cnes_leitos_uti` | Silver | OVERWRITE | Por estabelecimento CNES |
| `silver_cnes_leitos_uti_por_uf` | Silver | OVERWRITE | Denominador real de ocupação por UF/mês |
| `silver_ibge_estados` | Silver | criado pelo usuário | Lookup de estados |
| `silver_ibge_municipios` | Silver | criado pelo usuário | Lookup de municípios |
| `gold_fact_poc_srag_datasus` | Gold | OVERWRITE | Fato principal — 1 linha por caso |
| `gold_dim_tempo_poc_srag_datasus` | Gold | OVERWRITE | Dimensão tempo |
| `gold_dim_localidade_poc_srag_datasus` | Gold | OVERWRITE | Dimensão localidade |
| `gold_dim_perfil_poc_srag_datasus` | Gold | OVERWRITE | Dimensão perfil do paciente |
| `gold_dim_classificacao_poc_srag_datasus` | Gold | OVERWRITE | Dimensão classificação SRAG |
| `gold_agg_poc_srag_metricas_diarias` | Gold | OVERWRITE | Série temporal 30 dias + MM7d |
| `gold_agg_poc_srag_metricas_mensais` | Gold | OVERWRITE | Série temporal 12 meses |
| `gold_agg_poc_srag_indicadores_relatorio` | Gold | OVERWRITE | KPIs prontos — entrada do agente |
| `gold_quality_poc_srag_datasus` | Gold | APPEND | Qualidade consolidada |
| `gold_audit_agent_poc_srag_datasus` | Gold | APPEND | Auditoria de consultas do agente |
| `gold_news_context_poc_srag_datasus` | Gold | APPEND | Histórico de notícias consultadas |

### Schema `auditoria_pii`

| Tabela | Estratégia | Descrição |
|---|---|---|
| `mapa_identificacao_dados_srag_datasus` | OVERWRITE | Mapa SHA-256 ↔ dado real (CPF, CNS, nome) |

> ⚠️ O schema `auditoria_pii` tem acesso restrito. O agente não consegue
> consultá-lo — esse bloqueio é um dos guardrails LGPD ativos.

---

## Ordem de execução

```
PRÉ-REQUISITOS (tabelas criadas manualmente pelo usuário):
  bronze_dados_leitos_2024_2026
  silver_ibge_estados
  silver_ibge_municipios

PIPELINE PRINCIPAL:
  01  bronze/01_bronze_ingestao_srag.py
  02  bronze/02_bronze_quality_srag.py
  03  silver/03_silver_tratamento_srag.py
  03b silver/03b_silver_cnes_leitos_uti.py     ← pode rodar junto com 03
      gold/dim/gold_dim_*.py                   ← aguardar 03 e 03b
      gold/fact/gold_fact_srag.py
      gold/agg/gold_agg_*.py
      gold/gold_quality_srag.py
  06  validacao_metricas_srag/06_validacao_metricas_srag.py

DASHBOARDS (independentes, podem rodar a qualquer momento após Gold):
      dashboard/08_dashboard_visualizacoes_srag.py
      dashboard/09_dashboard_qualidade_dmbok.py

AGENTE (dev/staging — rodar após Gold):
  07  agente/07_agente_srag.py

PRODUÇÃO:
      app/app.py → deploy via Databricks Apps
```

---

## Métricas e fórmulas

| Métrica | Fórmula | Observação |
|---|---|---|
| Taxa de aumento de casos | `(casos_7d − casos_7d_ant) / casos_7d_ant` | NULL se período anterior = 0 |
| Taxa de mortalidade | `óbitos / casos_com_evolucao` | `evolucao IN (1, 2, 3)` — exclui sem desfecho |
| Taxa de uso de UTI | `uti_srag / uti_total_cnes_por_uf_mes` | Usa capacidade CNES — UTIs privadas sem cadastro não entram |
| Taxa de vacinação gripe | `vacinados / com_info_vacina` | Não é cobertura populacional |
| Taxa de vacinação COVID | `vacinados_covid / com_info_vacina_covid` | Não é cobertura populacional |
| Média móvel 7d | `avg(casos) sobre janela de −6 a 0 dias` | Window Function — suaviza sazonalidade |

---

## Agente epidemiológico

O agente é uma interface de linguagem natural conectada diretamente às tabelas Gold.
Funciona via **Databricks App** com Gradio e usa o modelo
**Meta Llama 3.3 70B Instruct** via Databricks Model Serving.

A cada consulta o agente:

1. Verifica guardrails LGPD na pergunta (bloqueia se detectar PII)
2. Busca KPIs consolidados na `gold_agg_poc_srag_indicadores_relatorio`
3. Busca notícias recentes (RSS gov.br + SerpAPI)
4. Chama o LLM com o contexto completo
5. Verifica guardrails na resposta gerada
6. Grava o log na `gold_audit_agent_poc_srag_datasus`
7. Grava as notícias na `gold_news_context_poc_srag_datasus`

Toda resposta segue a estrutura:
```
[DADOS OBSERVADOS — Open DATASUS]  ← métricas reais
[CONTEXTO EXTERNO — RSS/Google News] ← notícias
[ANÁLISE] ← interpretação do LLM com disclaimers
```

---

## Guardrails LGPD

O agente tem guardrails em dois momentos: na entrada (pergunta) e na saída (resposta gerada).

| Guardrail | Momento | O que faz |
|---|---|---|
| G1 — Identificação individual | Entrada | Bloqueia perguntas com termos como "nome do paciente", "prontuário", "dados pessoais" |
| G2 — CPF/CNS na pergunta | Entrada | Regex detecta padrão de CPF (999.999.999-99) e bloqueia |
| G2 — PII na resposta | Saída | Bloqueia resposta se o LLM gerar identificadores pessoais |
| G4 — Cofre PII | Tool | O agente não tem acesso ao schema `auditoria_pii` |
| G6 — Estrutura obrigatória | Saída | Força separação [DADOS] / [CONTEXTO] / [ANÁLISE] |
| G8 — Métricas apenas do banco | Sistema | System prompt instrui: só citar métricas que vieram das tools |

Guardrails acionados ficam registrados em `guardrails_acionados` na tabela de auditoria.

---

## Qualidade de dados — DMBOK

O notebook `02_bronze_quality_srag.py` avalia 40 regras a cada execução, agrupadas
em 5 dimensões. Os resultados são gravados em APPEND com `run_id` único por execução.

| Dimensão | Regras | O que avalia |
|---|---|---|
| Completude | 11 | Campos obrigatórios e essenciais não nulos |
| Validade | 18 | Valores dentro dos domínios SIVEP-Gripe |
| Consistência | 6 | Relações temporais entre datas (ex: `dt_sin_pri ≤ dt_notific`) |
| Unicidade | 2 | Duplicatas por `nu_notific` simples e composta |
| Atualidade | 3 | Defasagem entre data máxima e execução (limite: 30 dias) |

O dashboard `09_dashboard_qualidade_dmbok.py` visualiza essas métricas com
semáforo de severidade (ACEITÁVEL / MÉDIO / ALTO / CRÍTICO) e Radar DMBOK
comparando Bronze vs Gold.

---

## Tecnologias

| Componente | Tecnologia |
|---|---|
| Plataforma | Databricks (Serverless) |
| Formato de dados | Delta Lake |
| Governança | Unity Catalog |
| Ingestão | Auto Loader (`cloudFiles`) |
| LLM | Meta Llama 3.3 70B Instruct — Databricks Model Serving |
| Interface | Databricks App + Gradio |
| Notícias externas | RSS Ministério da Saúde + SerpAPI |
| Secrets | Databricks Secret Scope (`poc_srag`) |
| Versionamento | Git + Databricks Asset Bundles (DABs) |
| Dados | Open DATASUS / CNES (domínio público) |

---

## Privacidade e LGPD

Campos pessoais identificáveis (CPF, CNS, nome completo, nome da mãe) são
pseudoanonimizados via SHA-256 antes de chegar à Silver produtiva. O mapeamento
hash ↔ dado real fica isolado no schema `auditoria_pii`, sem acesso pelo agente.

Para os detalhes da decisão arquitetural, ver `documentacoes/decisao_arquitetural_pii_poc.md`.

> Em ambiente de produção real recomenda-se: catálogo dedicado para PII,
> salt dinâmico via Azure Key Vault e auditoria formal de acessos.

---

## Limitações conhecidas

1. **Latência dos dados:** atualização batch — não é streaming contínuo.
2. **Atraso de digitação:** 7 a 30 dias esperado no SIVEP-Gripe.
3. **Mortalidade:** calculada sobre casos com evolução informada — pode subestimar.
4. **UTI:** usa capacidade cadastrada no CNES — UTIs privadas sem cadastro não entram.
5. **Vacinação:** mede entre casos SRAG — não é cobertura vacinal da população.
6. **Notícias:** são contexto externo — não alteram as métricas calculadas.
7. **PII:** salt fixo nesta PoC — em produção usar Azure Key Vault.
8. **Cofre PII:** mesmo catálogo nesta PoC — em produção, catálogo dedicado.

---

## Acesso para avaliação

| Recurso | Acesso |
|---|---|
| 🤖 Agente Epidemiológico | credenciais por canal privado |
| 📊 Dashboard Monitoramento SRAG | credenciais por canal privado |
| 📋 Dashboard Qualidade DMBOK | credenciais por canal privado |
| 🗂️ Workspace Databricks | credenciais por canal privado |
| 📁 Tabelas Silver | Somente leitura — schema `poc_srag_datasus` |
| 📁 Tabelas Gold | Somente leitura — schema `poc_srag_datasus` |
| 🔐 Cofre PII | Bloqueado — schema `auditoria_pii` (LGPD) |

---

---

*PoC desenvolvida para certificação em AI Engineering — Indicium.*  
*Dados públicos — Open DATASUS. Não usar em ambiente clínico sem validação.*
