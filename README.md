[README_lh_nautical.md](https://github.com/user-attachments/files/31093312/README_lh_nautical.md)
# 🛥️ LH Nautical — Pipeline de Dados (Databricks / Arquitetura Medallion)

## 📌 Objetivo

Estruturar o pipeline de dados da **LH Nautical** no Databricks utilizando a **Arquitetura Medallion** (Bronze, Silver e Gold), e aplicar técnicas de análise exploratória, modelagem preditiva e sistemas de recomendação para resolver problemas operacionais e estratégicos da empresa.

O projeto parte de 22 tabelas brutas (CSV) de um ERP relacional e evolui até entregas de negócio: identificação de clientes fiéis, correção de uma métrica de vendas por dia da semana, previsão de demanda e um motor de recomendação de produtos.

---

## 🏗️ Arquitetura

```
CSV (22 tabelas) → Bronze (dados brutos + timestamp de ingestão)
                  → Silver (tratamento, tipagem e filtros de qualidade)
                  → Gold (agregações e regras de negócio)
```

- **Bronze**: ingestão 1:1 dos 22 CSVs, sem tratamento, com coluna `_ingested_at` para rastreabilidade.
- **Silver**: conversão de tipos, filtros de status válidos (ex: apenas pedidos `paid`/`completed`/`shipped`) e padronização de datas.
- **Gold**: tabelas agregadas prontas para consumo — clientes fiéis, vendas médias por dia da semana, previsão de demanda e recomendação de produtos.

---

## 📋 Questões respondidas

| Questão | Descrição | Status |
|---|---|---|
| 1 | EDA — volumetria e nulos das 22 tabelas Bronze | ✅ |
| 2 | Schema das tabelas Bronze | ✅ |
| 3 | Carregamento do banco bruto + validação de contagem de linhas | ✅ |
| 4 | Clientes fiéis (ticket médio, diversidade de categorias) | ✅ |
| 5 | Dimensão de calendário — média de vendas por dia da semana (PT-BR) | ✅ |
| 6 | Previsão de demanda — baseline (média móvel) + comparação com SARIMAX | ✅ |
| 7 | Sistema de recomendação — similaridade de cosseno | ✅ |

---

## 🔍 Principais achados

- **`orders.salesperson_id`** possui ~49% de valores nulos — quase metade dos pedidos não tem vendedor associado, o que limita análises futuras por performance de vendedor.
- **`fiscal_invoices.nfe_access_key`** foi inferida como `double`, mas deveria ser `string` — a chave de acesso da NF-e tem 44 dígitos e provavelmente perde precisão nesse tipo.
- **`stock_levels.reorder_point`** foi inferida como `string` em vez de numérica, indicando possível inconsistência de formatação no CSV de origem.
- **`tax_id`** aparece com tipos diferentes entre `customers` (long) e `suppliers` (string), mesmo representando o mesmo conceito (CPF/CNPJ).
- Nulos em `customers.trade_name` e `state_registration` são esperados para clientes Pessoa Física, que não possuem razão social nem inscrição estadual.

---

## 🧮 Metodologia — Questão 6 (Previsão de Demanda)

- **Baseline**: média móvel dos últimos 3 meses, projetada mês a mês sem uso de dados futuros (evitando data leakage).
- **Comparação**: modelo SARIMAX(1,1,1)(1,1,0,12) como alternativa mais sofisticada.
- **Validação**: MAE (Mean Absolute Error) calculado contra os valores reais do 1º trimestre de 2026 para ambos os modelos.

---

## 🎯 Metodologia — Questão 7 (Sistema de Recomendação)

- Matriz de interação **Usuário × Produto** (binária: comprou ou não comprou).
- **Similaridade de cosseno** entre vetores de produtos para identificar itens com padrão de compra semelhante.
- Ranking gerado para recomendar o produto mais similar ao item-alvo ("Motor de Popa 1949").

---

## 🛠️ Stack utilizada

- **Databricks** (PySpark + Delta Lake)
- **Python**: pandas, scikit-learn, statsmodels
- **SQL** (via Spark SQL)

---

## 📂 Estrutura do repositório

```
├── LH_Nautical_Pipeline.ipynb   # Notebook completo (Bronze → Silver → Gold + questões)
└── README.md
```
