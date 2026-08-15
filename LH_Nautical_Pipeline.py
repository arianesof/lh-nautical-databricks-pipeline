# Databricks notebook source
# =============================================================================
# LH NAUTICAL — PIPELINE DE DADOS (ARQUITETURA MEDALLION)
# Bronze -> Silver -> Gold | EDA, Schema, Previsão de Demanda e Recomendação
# =============================================================================

# COMMAND ----------
# MAGIC %md
# MAGIC ## 🥉 CAMADA BRONZE — Ingestão bruta dos 22 CSVs

# COMMAND ----------
from pyspark.sql.functions import current_timestamp, lit

caminho = "/Volumes/workspace/default/lh_nautical"
tabelas = [
    "addresses", "attributes", "brands", "categories", "customers", "employees",
    "fiscal_invoices", "goods_receipt_items", "goods_receipts", "locations",
    "order_items", "orders", "payments", "product_suppliers", "product_variants",
    "products", "purchase_order_items", "purchase_orders", "return_items",
    "returns", "stock_levels", "suppliers"
]

print("Iniciando a criação da camada bronze...\n")
for tabela in tabelas:
    caminho_csv = f"{caminho}/{tabela}.csv"
    df_raw = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(caminho_csv)
    df_bronze = df_raw.withColumn("_ingested_at", current_timestamp())
    df_bronze.write.format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(f"default.bronze_{tabela}")
    print(f"✅ Tabela 'bronze_{tabela}' criada com {df_bronze.count():,} registros.")

print("\n✅ Camada Bronze finalizada com sucesso!")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 📊 QUESTÃO 1 — EDA restrita a `orders` (SQL, sem tratamento)

# COMMAND ----------
# MAGIC %sql
# MAGIC -- QUESTÃO 1.1: Visão geral + valores numéricos, direto de orders (sem tratamento)
# MAGIC SELECT
# MAGIC     COUNT(*) AS total_linhas,
# MAGIC     MIN(created_at) AS data_minima,
# MAGIC     MAX(created_at) AS data_maxima,
# MAGIC     MIN(total) AS valor_minimo,
# MAGIC     MAX(total) AS valor_maximo,
# MAGIC     AVG(total) AS valor_medio
# MAGIC FROM default.bronze_orders

# COMMAND ----------
resultado_q1 = spark.sql("""
    SELECT
        COUNT(*) AS total_linhas,
        MIN(created_at) AS data_minima,
        MAX(created_at) AS data_maxima,
        MIN(total) AS valor_minimo,
        MAX(total) AS valor_maximo,
        AVG(total) AS valor_medio
    FROM default.bronze_orders
""")
resultado_q1.show(truncate=False)

# COMMAND ----------
# MAGIC %md
# MAGIC ### Questão 1.3 — Diagnóstico de confiabilidade
# MAGIC A tabela `orders`, sem qualquer tratamento, contém 48.998 registros, cobrindo o
# MAGIC período de 2020-01-01 a 2026-12-31 pela coluna `created_at`. O valor de `total`
# MAGIC varia de R$ 32,62 a R$ 127.262,02, com média de R$ 28.704,99 — um intervalo amplo,
# MAGIC coerente com uma loja que vende de pequenos acessórios a motores e lanchas.
# MAGIC Vale investigar se os valores mais altos são pedidos B2B legítimos ou inconsistências
# MAGIC de lançamento. Essa consulta isolada não permite confirmar nulos, já que não houve
# MAGIC tratamento; a análise complementar da camada Bronze revelou que `salesperson_id`
# MAGIC possui ~49% de nulos. A tabela `orders` sozinha não está pronta para decisões de
# MAGIC negócio — precisa ser relacionada com `customers`, `order_items`, `payments` e
# MAGIC `products` para dar contexto completo.

# COMMAND ----------
# MAGIC %md
# MAGIC ## 🧬 QUESTÃO 2 — Geração de schema.sql (Python puro, sem pandas/Spark)

# COMMAND ----------
import csv
import os

CAMINHO_CSVS = "/Volumes/workspace/default/lh_nautical"
CAMINHO_SAIDA = "/Volumes/workspace/default/lh_nautical/schema.sql"

def inferir_tipo_postgres(valores_amostra):
    """Infere o tipo de coluna Postgres a partir de uma amostra de valores (strings)."""
    valores = [v for v in valores_amostra if v not in (None, "")]
    if not valores:
        return "TEXT"

    if all(_eh_inteiro(v) for v in valores):
        return "BIGINT"

    if all(_eh_float(v) for v in valores):
        return "DOUBLE PRECISION"

    if all(_eh_timestamp(v) for v in valores):
        return "TIMESTAMP"

    if all(v.lower() in ("true", "false") for v in valores):
        return "BOOLEAN"

    return "TEXT"

def _eh_inteiro(v):
    try:
        int(v)
        return True
    except ValueError:
        return False

def _eh_float(v):
    try:
        float(v)
        return True
    except ValueError:
        return False

def _eh_timestamp(v):
    formatos = ["%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"]
    from datetime import datetime
    for fmt in formatos:
        try:
            datetime.strptime(v, fmt)
            return True
        except ValueError:
            continue
    return False

def gerar_schema_sql(caminho_csvs, caminho_saida, tamanho_amostra=200):
    arquivos_csv = [f for f in os.listdir(caminho_csvs) if f.endswith(".csv")]
    statements = []

    for arquivo in sorted(arquivos_csv):
        nome_tabela = arquivo.replace(".csv", "")
        caminho_completo = os.path.join(caminho_csvs, arquivo)

        with open(caminho_completo, newline="", encoding="utf-8") as f:
            leitor = csv.reader(f)
            cabecalho = next(leitor)
            amostras = {col: [] for col in cabecalho}

            for i, linha in enumerate(leitor):
                if i >= tamanho_amostra:
                    break
                for col, valor in zip(cabecalho, linha):
                    amostras[col].append(valor)

        colunas_sql = []
        for col in cabecalho:
            tipo = inferir_tipo_postgres(amostras[col])
            nome_col_seguro = col.strip().lower().replace(" ", "_")
            colunas_sql.append(f'    "{nome_col_seguro}" {tipo}')

        create_stmt = f'CREATE TABLE IF NOT EXISTS "{nome_tabela}" (\n' + ",\n".join(colunas_sql) + "\n);"
        statements.append(create_stmt)
        print(f"✅ Schema inferido para '{nome_tabela}' ({len(cabecalho)} colunas)")

    with open(caminho_saida, "w", encoding="utf-8") as f:
        f.write("-- Schema gerado automaticamente a partir dos CSVs (Python puro)\n")
        f.write("-- Destino: PostgreSQL\n\n")
        f.write("\n\n".join(statements))

    print(f"\n✅ Arquivo '{caminho_saida}' gerado com {len(statements)} tabelas.")

gerar_schema_sql(CAMINHO_CSVS, CAMINHO_SAIDA)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 📥 QUESTÃO 3 — Carregamento bruto (SQLite) + validação
# MAGIC
# MAGIC O schema.sql da Questão 2 foi gerado no dialeto PostgreSQL, conforme exigido.
# MAGIC Diante da ausência de uma instância PostgreSQL disponível no ambiente do desafio,
# MAGIC o carregamento abaixo usa SQLite (banco relacional embutido no Python) aplicando
# MAGIC a mesma lógica de carga sem tratamento (sem remoção de nulos, sem conversão de
# MAGIC tipos — todas as colunas como TEXT). A migração para PostgreSQL seguiria a mesma
# MAGIC lógica trocando o driver de conexão para psycopg2.

# COMMAND ----------
import sqlite3
import csv
import os
import shutil

CAMINHO_CSVS = "/Volumes/workspace/default/lh_nautical"
CAMINHO_DB_LOCAL = "/tmp/lh_nautical_bruto.db"          # grava local primeiro (Volumes não suporta I/O do SQLite)
CAMINHO_DB_VOLUME = "/Volumes/workspace/default/lh_nautical/lh_nautical_bruto.db"

if os.path.exists(CAMINHO_DB_LOCAL):
    os.remove(CAMINHO_DB_LOCAL)

conn = sqlite3.connect(CAMINHO_DB_LOCAL)
cursor = conn.cursor()

arquivos_csv = [f for f in os.listdir(CAMINHO_CSVS) if f.endswith(".csv")]

for arquivo in sorted(arquivos_csv):
    nome_tabela = arquivo.replace(".csv", "")
    caminho_completo = os.path.join(CAMINHO_CSVS, arquivo)

    with open(caminho_completo, newline="", encoding="utf-8") as f:
        leitor = csv.reader(f)
        cabecalho = next(leitor)
        linhas = list(leitor)

    colunas_sql = ", ".join([f'"{c}" TEXT' for c in cabecalho])
    cursor.execute(f'DROP TABLE IF EXISTS "{nome_tabela}"')
    cursor.execute(f'CREATE TABLE "{nome_tabela}" ({colunas_sql})')

    placeholders = ", ".join(["?"] * len(cabecalho))
    cursor.executemany(f'INSERT INTO "{nome_tabela}" VALUES ({placeholders})', linhas)

    conn.commit()
    print(f"✅ Tabela '{nome_tabela}' carregada com {len(linhas):,} registros.")

conn.close()
print("\n✅ Carregamento bruto finalizado (SQLite - local).")

shutil.copy(CAMINHO_DB_LOCAL, CAMINHO_DB_VOLUME)
print(f"✅ Banco copiado para o Volume: {CAMINHO_DB_VOLUME}")

# COMMAND ----------
# QUESTÃO 3.2: Validação — soma de linhas de customers, orders, order_items, payments
conn = sqlite3.connect(CAMINHO_DB_LOCAL)
cursor = conn.cursor()

tabelas_validacao = ["customers", "orders", "order_items", "payments"]
total = 0
for t in tabelas_validacao:
    cursor.execute(f'SELECT COUNT(*) FROM "{t}"')
    n = cursor.fetchone()[0]
    total += n
    print(f"{t}: {n:,}")

print(f"\nTotal somado: {total:,}")
conn.close()

# COMMAND ----------
# MAGIC %md
# MAGIC ## 🥈 CAMADA SILVER — Tratamento e Qualidade

# COMMAND ----------
from pyspark.sql.functions import col, to_timestamp

print("Iniciando o processamento da Camada Silver...\n")

df_orders_silver = spark.table("default.bronze_orders") \
    .withColumn("placed_at", to_timestamp(col("placed_at"))) \
    .withColumn("created_at", to_timestamp(col("created_at"))) \
    .filter(col("status").isin("paid", "completed", "shipped"))
df_orders_silver.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("default.silver_orders")
print("✅ Tabela 'silver_orders' processada (apenas pedidos válidos).")

df_order_items_silver = spark.table("default.bronze_order_items") \
    .withColumn("unit_price", col("unit_price").cast("double")) \
    .withColumn("quantity", col("quantity").cast("double")) \
    .withColumn("line_total", col("line_total").cast("double"))
df_order_items_silver.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("default.silver_order_items")
print("✅ Tabela 'silver_order_items' processada.")

df_returns_silver = spark.table("default.bronze_returns") \
    .filter(col("status") == "completed")
df_returns_silver.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("default.silver_returns")
print("✅ Tabela 'silver_returns' processada (apenas devoluções concluídas).")

print("\n✅ Camada Silver finalizada")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 🔎 Análise complementar — Volumetria e Schema das 22 tabelas Bronze
# MAGIC (apoio ao dashboard final — não é a resposta oficial da Q1/Q2, que são SQL/Python puro acima)

# COMMAND ----------
from pyspark.sql.functions import col, count, when

tabelas_apoio = [
    "addresses", "attributes", "brands", "categories", "customers", "employees",
    "fiscal_invoices", "goods_receipt_items", "goods_receipts", "locations",
    "order_items", "orders", "payments", "product_suppliers", "product_variants",
    "products", "purchase_order_items", "purchase_orders", "return_items",
    "returns", "stock_levels", "suppliers"
]

print("--- ANÁLISE COMPLEMENTAR: VOLUMETRIA DAS 22 TABELAS BRONZE (apoio ao dashboard) ---\n")

resumo_eda = []
for t in tabelas_apoio:
    df = spark.table(f"default.bronze_{t}")
    resumo_eda.append((t, df.count(), len(df.columns)))

df_resumo = spark.createDataFrame(resumo_eda, ["tabela", "n_linhas", "n_colunas"])
df_resumo.orderBy(col("n_linhas").desc()).show(22, truncate=False)

tabelas_criticas = ["customers", "orders", "order_items", "products", "product_variants"]

print("--- NULOS NAS TABELAS CRÍTICAS ---\n")
for t in tabelas_criticas:
    df = spark.table(f"default.bronze_{t}")
    print(f"\nTabela: {t}")
    exprs = [count(when(col(c).isNull(), c)).alias(c) for c in df.columns]
    df.select(exprs).show(truncate=False)

# COMMAND ----------
print("--- ANÁLISE COMPLEMENTAR: SCHEMA DAS 22 TABELAS BRONZE (apoio ao dashboard) ---\n")

for t in tabelas_apoio:
    df = spark.table(f"default.bronze_{t}")
    print(f"\n=== Tabela: bronze_{t} ===")
    df.printSchema()

# COMMAND ----------
# MAGIC %md
# MAGIC ## 🥇 CAMADA GOLD — Questões 4 e 5

# COMMAND ----------
#Questões 4 e 5
from pyspark.sql.functions import col, sum as _sum, avg, countDistinct, expr, to_date, dayofweek, min as _min, max as _max

print("Iniciando o processamento da Camada Gold...\n")

o = spark.table("default.silver_orders").alias("o")
oi = spark.table("default.silver_order_items").alias("oi")
pv = spark.table("default.bronze_product_variants").alias("pv")
p = spark.table("default.bronze_products").alias("p")
c = spark.table("default.bronze_customers").alias("c")

df_vendas_completa = o \
    .join(oi, col("o.id") == col("oi.order_id")) \
    .join(pv, col("oi.product_variant_id") == col("pv.id")) \
    .join(p, col("pv.product_id") == col("p.id"))

# ============================================================
# QUESTÃO 4: Clientes Fiéis (Ticket Médio e Diversidade >= 13)
# ============================================================

df_faturamento = spark.table("default.silver_orders") \
    .groupBy(col("customer_id")) \
    .agg(
        _sum("total").alias("faturamento_total"),
        countDistinct("id").alias("frequencia")
    )

df_diversidade = df_vendas_completa \
    .groupBy(col("o.customer_id").alias("customer_id")) \
    .agg(countDistinct("p.category_id").alias("diversidade_categorias"))

df_q4 = df_faturamento.join(df_diversidade, "customer_id") \
    .withColumn("ticket_medio", col("faturamento_total") / col("frequencia")) \
    .filter(col("diversidade_categorias") >= 13) \
    .join(c, col("customer_id") == col("c.id")) \
    .select("c.id", "c.legal_name", "ticket_medio", "diversidade_categorias", "faturamento_total", "frequencia") \
    .orderBy(col("ticket_medio").desc(), col("id").asc()) \
    .limit(10)

df_q4.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("default.gold_q4_clientes_fies")

print("--- QUESTÃO 4: TOP 10 CLIENTES FIÉIS (DIVERSIDADE >= 13) ---")
df_q4.show(10, truncate=False)

top10_ids = [row["id"] for row in df_q4.select("id").collect()]

df_categoria_top = df_vendas_completa \
    .filter(col("o.customer_id").isin(top10_ids)) \
    .groupBy(col("p.category_id")) \
    .agg(_sum("oi.quantity").alias("total_itens")) \
    .orderBy(col("total_itens").desc())

print("--- CATEGORIA MAIS COMPRADA PELOS TOP 10 CLIENTES FIÉIS ---")
df_categoria_top.show(5, truncate=False)

# ============================================================
# QUESTÃO 5: Calendário e Dias em Português (Date Spine) — só lojas físicas
# ============================================================

intervalo = spark.table("default.silver_orders") \
    .filter(col("channel") == "pos") \
    .select(_min(to_date(col("placed_at"))).alias("data_min"), _max(to_date(col("placed_at"))).alias("data_max")) \
    .collect()[0]

data_min = intervalo["data_min"]
data_max = intervalo["data_max"]
print(f"\nPeríodo de análise (Q5): {data_min} até {data_max}")

df_calendario = spark.sql(f"""
    SELECT explode(sequence(to_date('{data_min}'), to_date('{data_max}'), interval 1 day)) as data_completa
""")

df_vendas_diarias = spark.table("default.silver_orders") \
    .filter(col("channel") == "pos") \
    .withColumn("data_completa", to_date(col("placed_at"))) \
    .groupBy("data_completa") \
    .agg(_sum("total").alias("venda_do_dia"))

df_q5 = df_calendario.join(df_vendas_diarias, "data_completa", "left") \
    .na.fill({"venda_do_dia": 0}) \
    .withColumn("num_dia", dayofweek(col("data_completa"))) \
    .withColumn("dia_semana_pt", expr("""
        CASE num_dia 
            WHEN 1 THEN '1. Domingo'
            WHEN 2 THEN '2. Segunda-feira'
            WHEN 3 THEN '3. Terça-feira'
            WHEN 4 THEN '4. Quarta-feira'
            WHEN 5 THEN '5. Quinta-feira'
            WHEN 6 THEN '6. Sexta-feira'
            WHEN 7 THEN '7. Sábado'
        END
    """)) \
    .groupBy("num_dia", "dia_semana_pt") \
    .agg(avg("venda_do_dia").alias("media_venda_diaria")) \
    .orderBy("num_dia")

df_q5.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("default.gold_q5_vendas_calendario")

print("--- QUESTÃO 5: MÉDIA DE VENDAS POR DIA DA SEMANA (LOJAS FÍSICAS) ---")
df_q5.select("dia_semana_pt", "media_venda_diaria").show(7, truncate=False)

print("\n✅ Camada Gold e Relatórios das Questões 4 e 5 concluídos com sucesso!")

# COMMAND ----------
# Checagem de apoio: valores distintos de channel (usado no filtro da Q5)
spark.table("default.bronze_orders").select("channel").distinct().show()

# COMMAND ----------
# MAGIC %md
# MAGIC ## 📈 QUESTÃO 6 — Previsão de Demanda (Bússola de Bordo 702)

# COMMAND ----------
# QUESTÃO 6: Previsão de Demanda para 'Bússola de Bordo 702'
import pandas as pd
from sklearn.metrics import mean_absolute_error

df_orders = spark.table("default.silver_orders").toPandas()
df_items = spark.table("default.silver_order_items").toPandas()
df_vars = spark.table("default.bronze_product_variants").toPandas()
df_prods = spark.table("default.bronze_products").toPandas()

bussola_id = df_prods[df_prods['name'].str.contains('Bússola de Bordo 702', case=False, na=False)]['id'].values[0]
vars_bussola = df_vars[df_vars['product_id'] == bussola_id]['id'].tolist()

df_vendas = df_orders.merge(df_items[df_items['product_variant_id'].isin(vars_bussola)], left_on='id', right_on='order_id')
df_vendas['placed_at'] = pd.to_datetime(df_vendas['placed_at'])

ts_mensal = df_vendas.set_index('placed_at').resample('MS')['quantity'].sum().fillna(0)

treino = ts_mensal[:'2025-12-31']
teste = ts_mensal['2026-01-01':'2026-03-31']

# BASELINE: média móvel dos últimos 3 meses (sem usar dados futuros -> sem data leakage)
historico = treino.copy()
previsoes_baseline = []

for data_alvo in teste.index:
    media_3m = historico[-3:].mean()
    previsoes_baseline.append(media_3m)
    historico = pd.concat([historico, pd.Series([teste[data_alvo]], index=[data_alvo])])

previsao_baseline = pd.Series(previsoes_baseline, index=teste.index)
soma_previsao_baseline = int(round(previsao_baseline.sum()))
mae_baseline = mean_absolute_error(teste, previsao_baseline)

print("--- QUESTÃO 6: BASELINE (MÉDIA MÓVEL 3 MESES) ---")
print(f"Previsão mensal (baseline): {previsao_baseline.round(2).to_dict()}")
print(f"Soma total arredondada Q1/2026 (baseline): {soma_previsao_baseline} unidades")
print(f"MAE do baseline: {mae_baseline:.2f}")

# Comparação opcional: SARIMAX
from statsmodels.tsa.statespace.sarimax import SARIMAX

modelo = SARIMAX(treino, order=(1, 1, 1), seasonal_order=(1, 1, 0, 12))
modelo_fit = modelo.fit(disp=False)
previsao_sarimax = modelo_fit.forecast(steps=3)
soma_previsao_sarimax = int(round(previsao_sarimax.sum()))
mae_sarimax = mean_absolute_error(teste, previsao_sarimax)

print("\n--- COMPARAÇÃO: SARIMAX ---")
print(f"Soma total arredondada Q1/2026 (SARIMAX): {soma_previsao_sarimax} unidades")
print(f"MAE do SARIMAX: {mae_sarimax:.2f}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 🔗 QUESTÃO 7 — Sistema de Recomendação (Motor de Popa 1949)

# COMMAND ----------
#QUESTÃO 7: Sistema de Recomendação (Motor de Popa 1949)
import pandas as pd
from sklearn.metrics.pairwise import cosine_similarity

df_vendas_all = df_orders.merge(df_items, left_on='id', right_on='order_id') \
                          .merge(df_vars, left_on='product_variant_id', right_on='id')

matriz_user_prod = pd.crosstab(df_vendas_all['customer_id'], df_vendas_all['product_id']).clip(upper=1)

matriz_sim = cosine_similarity(matriz_user_prod.T)
df_sim = pd.DataFrame(matriz_sim, index=matriz_user_prod.columns, columns=matriz_user_prod.columns)

motor_id = df_prods[df_prods['name'].str.contains('Motor de Popa 1949', case=False, na=False)]['id'].values[0]

ranking = df_sim[motor_id].drop(motor_id).sort_values(ascending=False).head(5)

print("--- QUESTÃO 7: TOP 5 PRODUTOS MAIS SIMILARES AO 'MOTOR DE POPA 1949' ---")
for pid, score in ranking.items():
    nome = df_prods[df_prods['id'] == pid]['name'].values[0]
    print(f"{nome} — Score Cosseno: {score:.4f}")
