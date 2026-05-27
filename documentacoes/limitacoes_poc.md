# Limitações da PoC

**Projeto:** PoC SRAG — HealthCare Indicium  
**Autor:** Gabriel Ronney da Silva  
**Atualizado:** 2026-05-27

---

Documentar as limitações de um sistema é tão importante quanto documentar o que ele faz.
Estas limitações não são falhas — são escolhas conscientes de escopo para uma PoC.
Em produção, cada uma delas tem um caminho de evolução documentado.

---

## 1. Dataset Batch — Não é Streaming Real

O SIVEP-Gripe publica arquivos CSV periodicamente. O Auto Loader detecta novos
arquivos automaticamente quando carregados no Volume, mas a atualização depende
de nova publicação do DATASUS.

**Impacto:** O agente não reflete casos notificados nas últimas horas.
O termo correto na interface é "tempo quase real", não "tempo real".

**Em produção:** Integração via API hospitalar (FHIR) ou Event Hub para streaming contínuo.

---

## 2. Atraso de Digitação no SIVEP-Gripe (7 a 30 dias)

Profissionais de saúde têm prazo para registrar casos no sistema. O atraso
típico é de 7 a 30 dias para notificações recentes.

**Impacto:** Métricas dos últimos 7 a 14 dias estão subnotificadas. O campo
`defasagem_dias` na `gold_agg_poc_srag_indicadores_relatorio` indica quantos
dias o dataset está atrasado em relação ao dia de execução.

**Mitigação:** O agente declara `observacao_confiabilidade` em todas as respostas.
Métricas de 30 dias são mais estáveis que métricas de 7 dias.

---

## 3. Taxa de Mortalidade — Denominador Parcial

**Fórmula:** `SUM(flag_obito_srag) / SUM(flag_evolucao_informada)`

Casos com `evolucao = 9` (Ignorado) ou nulo são excluídos do denominador.

**Impacto:** Se os casos mais graves têm maior chance de ter evolução registrada,
a taxa pode estar superestimada. Se houver viés na direção oposta, pode estar
subestimada. O agente declara essa limitação em toda resposta sobre mortalidade.

---

## 4. Taxa de Uso de UTI — Denominador CNES

**Fórmula:** `casos_srag_em_uti / capacidade_uti_cnes_por_uf_mes`

A capacidade vem do CNES (Cadastro Nacional de Estabelecimentos de Saúde).

**Limitações:**
- Inclui apenas estabelecimentos cadastrados no CNES — UTIs privadas sem cadastro
  não entram no denominador.
- O numerador são casos SRAG que usaram UTI — não todos os pacientes de UTI do período.
- Não deve ser lida como "ocupação real de leitos de UTI".

**Leitura correta:** "Dos casos SRAG registrados neste estado/mês, que proporção
representa em relação à capacidade total de UTI cadastrada no CNES?"

---

## 5. Taxa de Vacinação — Não é Cobertura Populacional

**Fórmula:** `vacinados / casos_srag_com_info_de_vacina`

O denominador são **casos SRAG que informaram status vacinal**, não a população total.

**Não chamar de:** "cobertura vacinal" ou "vacinação da população".  
**Chamar de:** "vacinação registrada entre casos SRAG" — o guardrail G7 aplica
essa correção automaticamente nas respostas do agente.

---

## 6. Sobreposição entre Arquivos CSV

O arquivo `INFLUD25_DATASUS-Versao26-06-2025.csv` contém registros que também
aparecem no `INFLUD25-2025.csv` — são versões diferentes do mesmo dataset.

**Como é tratado:** O SCD2 (MERGE) na Silver usa `hash_caso` como chave.
Registros idênticos são ignorados. Registros atualizados (mesmo caso, conteúdo
diferente) fecham a versão anterior e abrem uma nova.

**Resultado:** ~440.674 registros únicos na Silver após processar os 3 arquivos.

---

## 7. Dois Formatos de Data nos CSVs

O arquivo original usa `YYYY-MM-DD`. Os arquivos 2025 e 2026 usam
`YYYY-MM-DDTHH:MM:SS.mmmZ` (ISO 8601 UTC).

**Como é tratado:** Bronze preserva como STRING. Silver normaliza via
`try_to_date(substring(col, 1, 10), 'yyyy-MM-dd')` — extrai apenas os
primeiros 10 caracteres, funcionando para ambos os formatos.

---

## 8. Notícias — Contexto Externo, Não Fonte de Métricas

O agente busca notícias via RSS (Ministério da Saúde) e SerpAPI (Google News).
As notícias enriquecem o contexto, mas não alteram as métricas calculadas.

O guardrail G6 exige separação explícita: `[DADOS OBSERVADOS]` (apenas SIVEP-Gripe)
vs `[CONTEXTO EXTERNO]` (notícias).

---

## 9. Salt PII Fixo

O SHA-256 usa salt fixo (`srag_poc_salt_v1`) — simplificação de PoC.

**Em produção:** Salt armazenado no Azure Key Vault, único por campo, com rotação periódica.

---

## 10. Cofre PII no Mesmo Catálogo

O mapa PII está em `certificacao_indicium.auditoria_pii` — mesmo catálogo das
tabelas analíticas. Sem isolamento físico de catálogo.

**Em produção:** Catálogo dedicado com ACLs restritas e monitoramento de acesso formal.

Para a justificativa completa dessa decisão: `documentacoes/decisao_arquitetural_pii_poc.md`.
