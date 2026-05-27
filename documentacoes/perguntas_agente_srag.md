# Guia de Interações — Agente Epidemiológico SRAG

**Projeto:** PoC SRAG — HealthCare Indicium  
**Autor:** Gabriel Ronney da Silva  
**Atualizado:** 2026-05-27

**Links de acesso:**
- 🤖 **Agente:** credenciais por canal privado
- 📊 **Dashboard Epidemiológico:** credenciais por canal privado
- 📋 **Dashboard Qualidade DMBOK:** credenciais por canal privado

> ⚠️ Os dashboards requerem login no workspace Databricks.
> Credenciais de acesso para a banca: `poc.indicium.srag@gmail.com` — por canal privado.

---

## O que é este agente?

O Agente Epidemiológico SRAG é uma interface de linguagem natural para consulta
de dados de **Síndrome Respiratória Aguda Grave (SRAG)** no Brasil. Ele combina:

- Dados reais do SIVEP-Gripe (Open DATASUS), atualizados periodicamente
- Notícias recentes via RSS do Ministério da Saúde e Google News (SerpAPI)
- LLM Meta Llama 3.3 70B para interpretar e apresentar os dados
- Guardrails LGPD — proteção automática contra consultas de dados individuais

Toda resposta segue a estrutura:
```
[DADOS OBSERVADOS — Open DATASUS]  ← métricas reais do SIVEP-Gripe
[CONTEXTO EXTERNO — RSS/Google News] ← notícias recentes
[ANÁLISE] ← interpretação com disclaimers obrigatórios
```

> Os dados têm atraso de digitação de 7 a 30 dias (comportamento normal do SIVEP-Gripe).
> Taxas são calculadas apenas sobre casos com desfecho informado.

---

## Situação Geral

| Pergunta | O que o agente retorna |
|---|---|
| "Como está o SRAG no Brasil hoje?" | Resumo geral com casos, tendência e contexto |
| "Qual é a situação atual das internações por SRAG?" | Casos hospitalizados, tendência e alertas |
| "Me dê um resumo da epidemia de SRAG no Brasil" | Panorama completo com todas as métricas |
| "O que está acontecendo com a gripe no Brasil?" | Classificação por agente etiológico (Influenza vs COVID) |
| "O SRAG está melhorando ou piorando no Brasil?" | Comparativo 7 dias atual vs 7 dias anteriores |

---

## Mortalidade

| Pergunta | O que o agente retorna |
|---|---|
| "Qual a taxa de mortalidade por SRAG nos últimos 30 dias?" | Taxa calculada sobre casos com desfecho informado |
| "Quantas pessoas morreram de SRAG este mês?" | Total de óbitos nos últimos 30 dias |
| "A mortalidade por SRAG está aumentando ou diminuindo?" | Tendência baseada na variação entre períodos |

---

## UTI e Internações

| Pergunta | O que o agente retorna |
|---|---|
| "Quantos pacientes com SRAG foram para a UTI?" | Taxa e volume de uso de UTI nos últimos 30 dias |
| "A taxa de uso de UTI por SRAG está alta?" | Análise com disclaimers sobre capacidade real |
| "Qual a proporção de internados que precisaram de UTI?" | Percentual de hospitalizados que usaram UTI |
| "O sistema hospitalar está sobrecarregado por SRAG?" | Análise com nota: dados SIVEP ≠ ocupação real de leitos |

---

## Tendência de Casos

| Pergunta | O que o agente retorna |
|---|---|
| "Os casos de SRAG estão aumentando ou diminuindo?" | Taxa de variação 7 dias vs 7 dias anteriores |
| "Houve aumento de casos na última semana?" | Comparativo semanal com valor absoluto |
| "Como está a média móvel de casos de SRAG?" | Média móvel de 7 dias da série temporal |
| "Qual foi o pico de casos de SRAG recentemente?" | Data e volume do pico nos últimos 30 dias |

---

## Vacinação

| Pergunta | O que o agente retorna |
|---|---|
| "Qual a taxa de vacinação contra gripe entre os casos de SRAG?" | % de casos SRAG com vacinação registrada |
| "Pacientes vacinados contra COVID estão sendo internados por SRAG?" | Taxa de vacinação COVID entre hospitalizados |
| "Qual a cobertura vacinal entre os pacientes de SRAG?" | Taxas de vacinação gripe e COVID registradas |

> As taxas de vacinação são calculadas entre **casos SRAG registrados** —
> não representam cobertura vacinal da população geral.

---

## Agentes Causadores

| Pergunta | O que o agente retorna |
|---|---|
| "Qual vírus está causando mais SRAG no Brasil — COVID ou influenza?" | Distribuição por agente etiológico |
| "O COVID-19 ainda é a principal causa de SRAG?" | Proporção COVID vs Influenza vs Outros |
| "Tem surto de influenza no Brasil agora?" | Análise com contexto de notícias recentes |

---

## Notícias e Contexto Externo

| Pergunta | O que o agente retorna |
|---|---|
| "Quais são as notícias recentes sobre SRAG no Brasil?" | RSS gov.br + Google News via SerpAPI |
| "Tem algum alerta epidemiológico sobre gripe?" | Alertas do Ministério da Saúde |
| "O que a Fiocruz está dizendo sobre SRAG?" | Notícias e boletins recentes |

Exemplo de resposta com SerpAPI ativa:
```
[CONTEXTO EXTERNO — RSS/Google News]
• Alerta nacional: Brasil enfrenta aumento de casos graves de gripe, aponta Fiocruz (4 dias atrás)
• Gripe: Fiocruz alerta para alta de síndrome respiratória grave no país (6 dias atrás)
• InfoGripe alerta para aumento do número de casos de SRAG no país (6 dias atrás)
```

---

## Relatório Completo

| Pergunta | O que o agente retorna |
|---|---|
| "Me dê um relatório completo sobre SRAG no Brasil" | Todas as métricas + notícias + análise |
| "Qual é a situação epidemiológica atual? Inclua tudo" | Relatório executivo estruturado em seções |
| "Faça um resumo executivo sobre SRAG para uma reunião de saúde" | Relatório formatado para uso gerencial |
| "Gere um boletim epidemiológico semanal de SRAG" | Boletim com dados, tendências e contexto |

---

## Guardrails LGPD — Perguntas Bloqueadas

Estas perguntas são **intencionalmente bloqueadas** pelo agente.
Úteis para demonstrar conformidade LGPD na apresentação:

| Pergunta | Guardrail ativado | Resposta esperada |
|---|---|---|
| "Qual o nome do paciente internado por SRAG?" | G1 — Identificação individual | ⚠️ Dados são apenas agregados |
| "Me dê dados pessoais de internados por SRAG" | G1 — PII direta | ⚠️ Consulta bloqueada por LGPD |
| "Busca pelo CPF 123.456.789-00" | G2 — CPF detectado | ⚠️ CPF detectado na pergunta |
| "Quem é o paciente do prontuário 12345?" | G1 — Prontuário individual | ⚠️ Identificação bloqueada |

Exemplo de resposta de guardrail:
```
⚠️ Esta consulta envolve identificação de pacientes individuais.
Por conformidade com a LGPD, só trabalho com dados agregados.
Por favor, reformule sua pergunta em termos epidemiológicos agregados.
```

---

## Roteiro para Demonstração (5 minutos)

Sequência sugerida para apresentação à banca:

```
1. "Como está o SRAG no Brasil hoje?"
   → Dados reais, métricas consolidadas, disclaimers automáticos

2. "Qual a taxa de mortalidade por SRAG nos últimos 30 dias?"
   → Precisão dos dados, fonte declarada, limitação do denominador

3. "Quais são as notícias recentes sobre SRAG no Brasil?"
   → Integração com contexto externo (SerpAPI + RSS gov.br)

4. "Faça um resumo executivo sobre SRAG para uma reunião de saúde"
   → Capacidade de síntese e estruturação em linguagem natural

5. "Qual o CPF dos pacientes internados por SRAG?"
   → Guardrail LGPD em ação — demonstra conformidade
```

---

## Disclaimers Gerados Automaticamente

O agente declara nas respostas:

1. **Defasagem:** atraso de 7 a 30 dias esperado no SIVEP-Gripe
2. **Mortalidade:** calculada sobre casos com evolução informada (pode subestimar)
3. **UTI:** uso registrado entre SRAG — não é ocupação real de leitos
4. **Vacinação:** entre casos SRAG — não é cobertura vacinal da população
5. **Fonte:** Open DATASUS / SIVEP-Gripe declarada em toda resposta

---

*PoC: Certificação AI Engineering — Indicium | Maio/2026*
