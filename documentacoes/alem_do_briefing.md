# Relatório — O Que Foi Além do Briefing

**Projeto:** PoC SRAG — HealthCare Indicium  
**Autor:** Gabriel Ronney da Silva  
**Atualizado:** 2026-05-27

---

## O que o briefing solicitava (resumido)

> Pipeline de dados com CSV do Open DATASUS → 4 métricas + 2 gráficos → Agente em linguagem natural com busca de notícias → Repositório GitHub com PDF da arquitetura.

Tudo isso foi entregue. O que está neste documento é o que **não estava no briefing** e foi implementado por decisão técnica.

---

## 1. Arquitetura Medallion (Bronze → Silver → Gold)

**O briefing pediu:** "banco de dados" — sem especificar arquitetura.

**O que foi feito:** Arquitetura Medallion completa com três camadas fisicamente separadas no Databricks Unity Catalog.

| Camada | Responsabilidade | Estratégia de escrita |
|---|---|---|
| Bronze | Ingestão imutável — dado exatamente como veio da fonte | APPEND exclusivo |
| Silver | Limpeza, tipagem, deduplicação, pseudoanonimização | MERGE SCD Tipo 2 |
| Gold | Modelagem dimensional, agregações e KPIs | OVERWRITE por tabela |

**Por que foi além:** o briefing poderia ter sido atendido com uma única tabela. A escolha da Medallion garante rastreabilidade histórica completa, capacidade de reprocessamento e separação clara de responsabilidades entre camadas — padrão de produção, não de PoC.

---

## 2. SCD Tipo 2 na Camada Silver

**O briefing pediu:** tratamento de dados.

**O que foi feito:** Slowly Changing Dimension Tipo 2 com MERGE Delta Lake. Quando um caso SRAG é atualizado na fonte (nova notificação com mesmo `nu_notific`), o sistema não sobrescreve — fecha o registro anterior e abre um novo.

Colunas de controle adicionadas:

| Coluna | Função |
|---|---|
| `hash_registros` | SHA-256 do conteúdo — detecta se o registro mudou |
| `hash_caso` | Identificador pseudoanonimizado do caso |
| `registro_mais_atual` | `true` = versão vigente |
| `dh_inicio_vigencia` | Quando esta versão ficou ativa |
| `dh_fim_vigencia` | Quando foi substituída (`null` = ainda vigente) |
| `run_id` | UUID da execução — rastreia qual pipeline gerou o registro |

**Por que foi além:** o briefing não pediu histórico de versões. O SCD2 garante que se a classificação de um caso mudar (ex: COVID-19 → Influenza), a versão anterior é preservada para auditoria.

---

## 3. Star Schema na Camada Gold

**O briefing pediu:** métricas calculadas.

**O que foi feito:** Modelagem dimensional completa com Fato + 4 Dimensões + 3 tabelas de Agregação.

**Dimensões:**
- `gold_dim_tempo` — calendário epidemiológico por data
- `gold_dim_localidade` — UF, município, região
- `gold_dim_perfil` — sexo, faixa etária, raça, escolaridade, gestação
- `gold_dim_classificacao` — agente etiológico, critério, desfecho

**Fato:**
- `gold_fact_poc_srag_datasus` — 1 linha por caso, com surrogate keys (SHA-256)

**Agregações:**
- `gold_agg_metricas_diarias` — série temporal 30 dias + média móvel 7d
- `gold_agg_metricas_mensais` — série temporal 12 meses
- `gold_agg_indicadores_relatorio` — KPIs consolidados, entrada principal do agente

**Surrogate Keys via Hash:**
Chaves primárias geradas com SHA-256 (`sk_tempo`, `sk_localidade`, `sk_perfil`, `sk_classificacao`, `sk_caso`) — idempotentes e geráveis em paralelo, sem sequencial numérico.

**Por que foi além:** o briefing precisava apenas de números. O Star Schema torna as métricas extensíveis para análises multidimensionais futuras (por UF, faixa etária, agente) sem reescrever o pipeline.

---

## 4. Qualidade de Dados — 5 Dimensões DMBOK, 40 Regras

**O briefing pediu:** tratamento de dados — "aplicar os tratamentos que achar necessários".

**O que foi feito:** Framework completo de qualidade baseado no DMBOK (Data Management Body of Knowledge), com 40 regras avaliadas por execução e rastreadas por `run_id`.

| Dimensão | Regras | O que avalia |
|---|---|---|
| Completude | 11 | Campos obrigatórios/essenciais preenchidos |
| Validade | 18 | Valores dentro dos domínios definidos pelo SIVEP-Gripe |
| Consistência | 6 | Relações temporais entre datas coerentes |
| Unicidade | 2 | Duplicatas por chave simples e composta |
| Atualidade | 3 | Defasagem entre data máxima do dataset e data de execução |

**Thresholds de severidade:** CRÍTICO (< 80%), ALTO (80-89%), MÉDIO (90-94%), BAIXO (95-98%), ACEITÁVEL (≥ 99%).

**Rastreamento por run_id:** cada execução gera um UUID único. Todas as 40 regras são agrupadas por `run_id` — não por timestamp, que varia célula a célula.

**Por que foi além:** o briefing mencionou "problemas de preenchimento incorreto e dados ausentes" mas não pediu monitoramento sistemático. O DMBOK cria uma série histórica de qualidade que permite detectar degradação ao longo do tempo.

---

## 5. Qualidade da Camada Gold

**O briefing pediu:** métricas corretas.

**O que foi feito:** notebook `gold_quality_srag.py` com validação de sanidade das métricas calculadas (dimensão Acurácia) — independente das 40 regras da Bronze.

| Regra | O que valida |
|---|---|
| Acurácia da taxa de mortalidade | Entre 0% e 60% — acima disso é erro de cálculo |
| Acurácia da taxa de UTI | Entre 0% e 100% — UTI ≤ hospitalizados |
| Acurácia da taxa de vacinação gripe | Entre 0% e 100% |
| Acurácia da taxa de vacinação COVID | Entre 0% e 100% |
| Atualidade do dataset Gold | Defasagem ≤ 30 dias |

**Por que foi além:** não basta que o dado esteja limpo na Bronze — o resultado final pode estar errado por bug na lógica de cálculo. A validação da Gold fecha o ciclo de qualidade.

---

## 6. Dashboard de Qualidade DMBOK

**O briefing pediu:** 2 gráficos epidemiológicos.

**O que foi feito:** um dashboard dedicado exclusivamente à qualidade dos dados (`09_dashboard_qualidade_dmbok.py`), com 7 gráficos, separado do dashboard epidemiológico.

| Gráfico | O que mostra |
|---|---|
| Semáforo Bronze | Distribuição de regras por severidade + nível geral |
| Semáforo Gold | Idem para a camada Gold |
| Completude por coluna | % de preenchimento das 11 colunas críticas |
| Validade por coluna | % de registros dentro do domínio por coluna |
| Consistência temporal | % de registros com datas coerentes |
| Radar DMBOK | 6 dimensões Bronze vs Gold lado a lado |
| Evolução histórica | Como a qualidade evoluiu entre execuções |

**Por que foi além:** transparência sobre a confiabilidade dos dados é tão importante quanto os dados em si. O dashboard permite à banca e à equipe avaliar o quanto as métricas são confiáveis antes de interpretá-las.

---

## 7. Pseudoanonimização LGPD com Cofre PII

**O briefing pediu:** "tratamento de dados sensíveis de acordo com a LGPD".

**O que foi feito:** estratégia completa de desacoplamento de identidade.

**Identificação e classificação:**
- 15 campos de PII direto (CNS, nome, endereço, telefone, etc.)
- 1 quasi-identificador de alto risco: `dt_nasc` (combinado com UF e sexo = ~87% re-identificação)

**Pseudoanonimização:**
- SHA-256 determinístico com salt (`SALT_PII` via Databricks Secrets)
- Captura de PII **antes** de qualquer seleção de colunas — lição aprendida: se feito depois, o cofre fica vazio

**Cofre PII:**
- Tabela `auditoria_pii.mapa_identificacao_dados_srag_datasus`
- Único local onde hash e valor original coexistem
- OVERWRITE: permite exclusão de um titular pela LGPD simplesmente reprocessando o pipeline sem o registro

**Silver produtiva:**
- Zero PII em texto claro
- `dt_nasc_hash` no lugar de `dt_nasc`
- Substitutos analíticos: `faixa_etaria` + `idade_anos_calculada`

**Por que foi além:** o briefing mencionou LGPD como critério de avaliação. A implementação vai além de "remover colunas sensíveis" — cria um sistema de re-identificação controlada, auditável e reversível.

---

## 8. 9 Guardrails no Agente

**O briefing pediu:** "guardrails" — sem especificar quais.

**O que foi feito:** 9 guardrails implementados em duas camadas (entrada e saída).

| Guardrail | Camada | O que faz |
|---|---|---|
| G1 — Identificação individual | Entrada | Bloqueia "nome do paciente", "prontuário", "dados pessoais" |
| G2 — Dados pessoais | Entrada e Saída | Regex detecta padrão de CNS e bloqueia |
| G3 — Comprimento mínimo | Entrada | Pergunta muito curta (< 5 chars) é rejeitada |
| G4 — Comprimento máximo | Entrada | Pergunta > 1000 chars é rejeitada |
| G5 — Cofre PII | Tool | Agente sem acesso ao schema `auditoria_pii` |
| G6 — Alerta de qualidade | Saída | Aviso obrigatório se DMBOK CRÍTICO ou ALTO |
| G7 — Estrutura obrigatória | Saída | Força [DADOS OBSERVADOS] / [CONTEXTO EXTERNO] / [ANÁLISE] |
| G8 — Correção de linguagem | Saída | "cobertura vacinal" → "vacinação registrada entre casos SRAG" |
| G9 — Métricas apenas do banco | Sistema | System prompt instrui LLM a não inventar métricas |

**Por que foi além:** guardrails de entrada e saída com correção ativa de linguagem técnica é um nível de sofisticação além do que normalmente se espera numa PoC.

---

## 9. Audit Log Automático

**O briefing pediu:** "mecanismos de auditoria e registro de decisões dos agentes".

**O que foi feito:** toda interação com o agente é gravada automaticamente em `gold_audit_agent_poc_srag_datasus` com APPEND.

Campos gravados por interação:

| Campo | Conteúdo |
|---|---|
| `usuario` | Nome informado na interface |
| `pergunta` | Texto original da pergunta |
| `resposta_resumo` | Primeiros 500 chars da resposta |
| `tools_utilizadas` | Quais tools foram acionadas |
| `guardrails_acionados` | Se G1, G2, G8, etc. foram disparados |
| `dh_execucao` | Timestamp da interação |
| `run_id` | UUID da sessão |

**Notícias também são gravadas:** `gold_news_context_poc_srag_datasus` mantém histórico de todas as notícias consumidas pelo agente, com fonte e timestamp.

**Por que foi além:** o briefing pedia "registro de decisões" de forma genérica. O audit log por coluna — incluindo qual guardrail foi acionado e quais tools foram usadas — permite rastrear exatamente o raciocínio do agente em cada interação.

---

## 10. Integração com Leitos CNES para Denominador Real de UTI

**O briefing pediu:** taxa de ocupação de UTI.

**O que foi feito:** o denominador da taxa não foi estimado — foi calculado com dados reais do CNES (Cadastro Nacional de Estabelecimentos de Saúde), cruzando capacidade de UTI por UF e mês.

Pipeline adicional criado (`03b_silver_cnes_leitos_uti.py`):
- Ingestão de 2 arquivos CSV de leitos (2025 e 2026) — 86k + 28k registros
- Normalização e tipagem na Silver
- Agregação por UF/mês em `silver_cnes_leitos_uti_por_uf`
- Join com os casos SRAG no pipeline Gold para calcular a taxa real

**Por que foi além:** sem o CNES, a taxa de UTI seria calculada como "casos SRAG em UTI / total de casos SRAG" — uma métrica sem denominador de capacidade, que não responde à pergunta real: "o sistema está sobrecarregado?"

---

## 11. 17 Flags Epidemiológicas Derivadas na Silver

**O briefing pediu:** métricas na entrega final.

**O que foi feito:** 17 colunas binárias (0/1) derivadas na Silver, que pré-computam as condições necessárias para todas as métricas Gold.

| Flag | Regra de derivação |
|---|---|
| `flag_caso_srag` | Literal `1` — toda linha é um caso |
| `flag_obito_srag` | `evolucao = 2` |
| `flag_evolucao_informada` | `evolucao IN (1,2,3)` — denominador da mortalidade |
| `flag_hospitalizado` | `hospital=1 OR dt_interna IS NOT NULL` |
| `flag_uti` | `uti = 1` — numerador da taxa de UTI |
| `flag_vacinado_gripe` | `vacina_gripe = 1` |
| `flag_info_vacina_gripe` | `vacina_gripe IN (1,2)` — denominador vacinação |
| `flag_vacinado_covid` | `vacina_covid = 1` |
| `flag_info_vacina_covid` | `vacina_covid IN (1,2)` — denominador vacinação |
| `flag_covid_classificacao_final` | `classi_fin = 5` |
| `flag_influenza_classificacao_final` | `classi_fin = 1` |
| ... | (e mais 6 flags de desfecho e agente) |

**Por que foi além:** as flags tornam as queries Gold simples somas — sem lógica condicional embutida nas agregações. São auditáveis, testáveis e reutilizáveis em qualquer análise futura.

---

## 12. Taxa de Vacinação Dividida em Duas

**O briefing pediu:** "taxa de vacinação da população" (singular).

**O que foi feito:** duas taxas separadas:
- `taxa_vacinacao_gripe_30d` — vacinação contra Influenza entre casos SRAG
- `taxa_vacinacao_covid_30d` — vacinação contra COVID-19 entre casos SRAG

**Por que foi além:** agregar gripe e COVID numa única taxa de vacinação seria epidemiologicamente sem sentido — são doenças diferentes, campanhas diferentes, públicos-alvo diferentes e impactos clínicos distintos.

---

## 13. Média Móvel de 7 Dias no Gráfico Diário

**O briefing pediu:** gráfico do número diário de casos dos últimos 30 dias.

**O que foi feito:** barras diárias + linha de média móvel de 7 dias (MM7d) sobreposta.

A MM7d suaviza a sazonalidade semanal do SIVEP-Gripe (digitações se acumulam em dias úteis e caem nos finais de semana), revelando a tendência real sem o ruído do dia a dia.

---

## 14. Documentação Técnica Completa (11 documentos)

**O briefing pediu:** README com explicações e PDF da arquitetura.

**O que foi feito:** 11 documentos técnicos além do README.

| Documento | Conteúdo |
|---|---|
| `README.md` | Visão geral, acesso e estrutura do repositório |
| `arquitetura_conceitual.pdf` | PDF do diagrama conceitual (requisito) |
| `caso_de_uso_poc.md` | Contexto, briefing e decisões arquiteturais |
| `dashboards_poc.md` | Documentação dos 13 gráficos dos 2 dashboards |
| `governanca_lgpd_poc.md` | Classificação de PII, algoritmo, fluxo e guardrails |
| `qualidade_dados_dmbok.md` | 5 dimensões, thresholds e como o agente usa a qualidade |
| `limitacoes_poc.md` | 10 limitações documentadas com solução em produção |
| `decisao_arquitetural_pii_poc.md` | ADR-001 e ADR-002 — registros formais de decisão |
| `mapeamento_bronze_silver.md` | Mapeamento campo a campo das 194 colunas |
| `dicionario_colunas_silver.md` | Dicionário completo da camada Silver |
| `perguntas_agente_srag.md` | Guia de interações + roteiro de demonstração |

**Por que foi além:** o briefing pedia documentação no README. A separação em documentos especializados facilita manutenção, onboarding de novos membros e serve como referência técnica autônoma.

---

## Resumo Executivo

| Dimensão | Briefing | PoC Entregue |
|---|---|---|
| Pipeline | "banco de dados" | Medallion 3 camadas + Unity Catalog |
| Histórico | Não pedido | SCD Tipo 2 na Silver |
| Modelagem | Não pedido | Star Schema + 4 dims + 3 aggs |
| Qualidade | "tratar dados" | DMBOK 5 dims + 40 regras + dashboard |
| LGPD | "conforme LGPD" | Cofre PII + SHA-256 + 15 campos + ADRs |
| Guardrails | "guardrails" | 9 guardrails em 2 camadas + correção de linguagem |
| Auditoria | "mecanismos de auditoria" | Audit log por coluna + histórico de notícias |
| Gráficos | 2 gráficos | 2 gráficos (briefing) + MM7d + 7 gráficos de qualidade |
| Vacinação | 1 taxa | 2 taxas (gripe + COVID separadas) |
| UTI | taxa de ocupação | Denominador real via CNES por UF/mês |
| Documentação | README + PDF | README + PDF + 11 documentos técnicos |
| Infraestrutura | repositório | Repositório + DABs (deploy automatizado) |

---

*PoC: Certificação AI Engineering — Indicium | Maio/2026*
