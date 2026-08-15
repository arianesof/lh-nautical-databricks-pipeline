# 🛥️ LH Nautical — Pipeline de Dados (Databricks / Arquitetura Medallion)

## 📌 Objetivo

Estruturar o pipeline de dados da **LH Nautical**, empresa fictícia de varejo náutico, aplicando técnicas de engenharia de dados, análise exploratória, modelagem preditiva e sistemas de recomendação sobre uma base relacional de 22 tabelas (2020–2026), cobrindo catálogo de produtos, pedidos, pagamentos, notas fiscais, compras de fornecedores, estoque e devoluções.

O projeto foi desenvolvido como parte de um desafio técnico avaliando engenharia de dados, SQL, ciência de dados e comunicação de insights, seguindo o contexto e as premissas descritas pelo Tech Lead fictício (Gabriel Santos).

---

## 🏗️ Arquitetura

```
CSV (22 tabelas) → Bronze (dados brutos + timestamp de ingestão)
                  → Silver (tratamento, tipagem e filtros de qualidade)
                  → Gold (agregações e regras de negócio)
```

- **Bronze**: ingestão 1:1 dos 22 CSVs via PySpark, sem tratamento, com coluna `_ingested_at` para rastreabilidade.
- **Silver**: conversão de tipos, filtro de pedidos válidos (`paid`/`completed`/`shipped`) e devoluções concluídas.
- **Gold**: tabelas agregadas para consumo de negócio — clientes fiéis, vendas médias por dia da semana, previsão de demanda e recomendação de produtos.

Paralelamente à camada Bronze/Silver/Gold em Delta Lake, o desafio também exigiu demonstrar:
- Geração de schema relacional (`schema.sql`, dialeto PostgreSQL) a partir dos CSVs, usando **Python puro** (sem pandas/Spark).
- Carregamento bruto em um banco relacional real via **SQLite**, como alternativa pragmática à ausência de uma instância PostgreSQL disponível no ambiente do desafio.

---

## 📋 Questões respondidas

| Questão | Descrição | Ferramenta exigida | Status |
|---|---|---|---|
| 1 | EDA restrita à tabela `orders` (volumetria, intervalo de datas, min/max/média) | SQL | ✅ |
| 2 | Geração de `schema.sql` (PostgreSQL) a partir dos CSVs | Python 3 puro (sem libs externas) | ✅ |
| 3 | Carregamento bruto de todas as tabelas + validação de contagem | Python 3 | ✅ (SQLite, ver nota abaixo) |
| 4 | Clientes fiéis — ticket médio, diversidade de categorias, categoria mais comprada | SQL/PySpark | ✅ |
| 5 | Dimensão de calendário — vendas médias por dia da semana (lojas físicas) | SQL/PySpark | ✅ |
| 6 | Previsão de demanda — baseline de média móvel + MAE, comparado a SARIMAX | Python | ✅ |
| 7 | Sistema de recomendação — similaridade de cosseno, top 5 produtos | Python (pandas/sklearn) | ✅ |

---

## 🔍 Principais achados (qualidade de dados)

- **`orders.salesperson_id`** possui ~49% de valores nulos — quase metade dos pedidos não tem vendedor associado, limitando análises de performance por vendedor.
- **`fiscal_invoices.nfe_access_key`** foi inferida como `double`, mas deveria ser `string` — a chave de acesso da NF-e tem 44 dígitos e provavelmente perde precisão nesse tipo.
- **`stock_levels.reorder_point`** foi inferida como `string` em vez de numérica, indicando possível inconsistência de formatação no CSV de origem.
- **`tax_id`** aparece com tipos diferentes entre `customers` (long) e `suppliers` (string), mesmo representando o mesmo conceito (CPF/CNPJ).
- Nulos em `customers.trade_name` e `state_registration` são esperados para clientes Pessoa Física, que não possuem razão social nem inscrição estadual.
- O catálogo de produtos (`products`) contém pelo menos um registro de dado de teste (nome "asdf"), que chegou a aparecer no ranking de recomendação da Questão 7 — evidência de que a qualidade do dado de entrada afeta diretamente a qualidade da recomendação.

---

## 🧮 Metodologia — Questão 6 (Previsão de Demanda)

- **Baseline**: média móvel dos últimos 3 meses, projetada mês a mês sem uso de dados futuros (evitando data leakage — cada previsão usa apenas meses já observados até aquele ponto).
- **Comparação**: modelo SARIMAX(1,1,1)(1,1,0,12) como alternativa mais sofisticada.
- **Validação**: MAE (Mean Absolute Error) calculado contra os valores reais do 1º trimestre de 2026 para ambos os modelos.

---

## 🎯 Metodologia — Questão 7 (Sistema de Recomendação)

- Matriz de interação **Usuário × Produto** (binária: comprou ou não comprou, quantidade ignorada).
- **Similaridade de cosseno** entre vetores de produtos, calculada com `sklearn.metrics.pairwise.cosine_similarity`.
- Ranking dos **5 produtos mais similares** ao item de referência ("Motor de Popa 1949"), excluindo o próprio item.

---

## 📝 Nota sobre a Questão 3 (carregamento)

O `schema.sql` da Questão 2 foi gerado no dialeto **PostgreSQL**, conforme exigido. Para a etapa de carregamento (Questão 3), diante do prazo do desafio e da ausência de uma instância PostgreSQL provisionada no ambiente, optou-se por demonstrar o carregamento usando **SQLite** — banco relacional embutido no Python — aplicando a mesma lógica de carga sem tratamento (sem remoção de nulos, sem conversão de tipos, todas as colunas como `TEXT`). A migração para PostgreSQL seguiria a mesma lógica de script, trocando o driver de conexão para `psycopg2` e aplicando o `schema.sql` já gerado.

---

## 🛠️ Stack utilizada

- **Databricks** (PySpark + Delta Lake + Spark SQL)
- **Python**: pandas, scikit-learn, statsmodels, sqlite3, csv (biblioteca padrão)
- **SQL** (Spark SQL / SQLite)

---

## 📂 Estrutura do repositório

```
├── LH_Nautical_Pipeline.ipynb   # Notebook completo (Bronze → Silver → Gold + questões 1-7)
├── LH_Nautical_Pipeline.py      # Versão .py do mesmo pipeline (formato Databricks)
├── schema.sql                   # Schema PostgreSQL gerado pela Questão 2
└── README.md
```
