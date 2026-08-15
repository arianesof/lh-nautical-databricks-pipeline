# LH NAUTICAL — PIPELINE DE DADOS (ARQUITETURA MEDALLION)
# Bronze -> Silver -> Gold | EDA, Schema, Previsão de Demanda e Recomendação

#CAMADA BRONZE — Ingestão bruta dos 22 CSVs
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
    # adiciona timestamp de ingestão
    df_bronze = df_raw.withColumn("_ingested_at", current_timestamp())
    # escreve no delta
    df_bronze.write.format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(f"default.bronze_{tabela}")
    print(f"✅ Tabela 'bronze_{tabela}' criada com {df_bronze.count():,} registros.")

print("\n✅ Camada Bronze finalizada com sucesso!")

#QUESTÃO 1 — EDA (Análise Exploratória)

from pyspark.sql.functions import col, count, when

print("--- QUESTÃO 1: EDA - VOLUMETRIA DAS TABELAS BRONZE ---\n")

resumo_eda = []
for t in tabelas:
    df = spark.table(f"default.bronze_{t}")
    resumo_eda.append((t, df.count(), len(df.columns)))

df_resumo = spark.createDataFrame(resumo_eda, ["tabela", "n_linhas", "n_colunas"])
df_resumo.orderBy(col("n_linhas").desc()).show(22, truncate=False)

# Checagem de nulos nas tabelas centrais
tabelas_criticas = ["customers", "orders", "order_items", "products", "product_variants"]

print("--- NULOS NAS TABELAS CRÍTICAS ---\n")
for t in tabelas_criticas:
    df = spark.table(f"default.bronze_{t}")
    print(f"\nTabela: {t}")
    exprs = [count(when(col(c).isNull(), c)).alias(c) for c in df.columns]
    df.select(exprs).show(truncate=False)

# QUESTÃO 2 — Schema das Tabelas Bronze
print("--- QUESTÃO 2: SCHEMA DAS TABELAS BRONZE ---\n")
for t in tabelas:
    df = spark.table(f"default.bronze_{t}")
    print(f"\n=== Tabela: bronze_{t} ===")
    df.printSchema()

#AMADA SILVER — Tratamento e Qualidade
from pyspark.sql.functions import col, to_timestamp

print("Iniciando o processamento da Camada Silver...\n")

df_orders = spark.table("default.bronze_orders") \
    .withColumn("placed_at", to_timestamp(col("placed_at"))) \
    .withColumn("created_at", to_timestamp(col("created_at"))) \
    .filter(col("status").isin("paid", "completed", "shipped"))
df_orders.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("default.silver_orders")
print("✅ Tabela 'silver_orders' processada (apenas pedidos válidos).")

#Tratamento de Itens de Pedidos
df_order_items = spark.table("default.bronze_order_items") \
    .withColumn("unit_price", col("unit_price").cast("double")) \
    .withColumn("quantity", col("quantity").cast("double")) \
    .withColumn("line_total", col("line_total").cast("double"))
df_order_items.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("default.silver_order_items")
print("✅ Tabela 'silver_order_items' processada.")

#Tratamento de Devoluções
df_returns = spark.table("default.bronze_returns") \
    .filter(col("status") == "completed")
df_returns.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("default.silver_returns")
print("✅ Tabela 'silver_returns' processada (apenas devoluções concluídas).")

print("\n✅ Camada Silver finalizada")

#CAMADA GOLD — Questões 4 e 5

from pyspark.sql.functions import col, sum as _sum, avg, countDistinct, expr, to_date, dayofweek

print("Iniciando o processamento da Camada Gold...\n")

# Aliases para facilitar os joins
o = spark.table("default.silver_orders").alias("o")
oi = spark.table("default.silver_order_items").alias("oi")
pv = spark.table("default.bronze_product_variants").alias("pv")
p = spark.table("default.bronze_products").alias("p")
c = spark.table("default.bronze_customers").alias("c")

# Base de vendas
df_vendas_completa = o \
    .join(oi, col("o.id") == col("oi.order_id")) \
    .join(pv, col("oi.product_variant_id") == col("pv.id")) \
    .join(p, col("pv.product_id") == col("p.id"))

# ---------------------------------------------------------------------------
# QUESTÃO 4: Clientes Fiéis (Ticket Médio e Diversidade >= 13)
# Faturamento e frequência calculados direto de 'orders' (nível de pedido),
# evitando duplicação por múltiplos itens no mesmo pedido.
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
    .orderBy(col("ticket_medio").desc())

df_q4.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("default.gold_q4_clientes_fies")

print("--- QUESTÃO 4: TOP 10 CLIENTES FIÉIS (DIVERSIDADE >= 13) ---")
df_q4.show(10, truncate=False)


# QUESTÃO 5: Calendário e Dias em Português
df_calendario = spark.sql("SELECT explode(sequence(to_date('2020-01-01'), to_date('2026-12-31'), interval 1 day)) as data_completa")

df_vendas_diarias = spark.table("default.silver_orders") \
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

print("--- QUESTÃO 5: MÉDIA DE VENDAS POR DIA DA SEMANA (EM PORTUGUÊS) ---")
df_q5.select("dia_semana_pt", "media_venda_diaria").show(7, truncate=False)

print("\n✅ Camada Gold e Relatórios das Questões concluídos com sucesso!")

#QUESTÃO 3.2 — Validação de Volumetria
tabelas_validacao = ["customers", "orders", "order_items", "payments"]
total_linhas = sum([spark.table(f"default.bronze_{t}").count() for t in tabelas_validacao])

print("--- QUESTÃO 3.2: TOTAL DE LINHAS ---")
print(f"Soma total das linhas (customers + orders + order_items + payments): {total_linhas:,}")

#QUESTÃO 6 — Previsão de Demanda (Bússola de Bordo 702)
import pandas as pd
from sklearn.metrics import mean_absolute_error

# 1. Carregar tabelas
df_orders_pd = spark.table("default.silver_orders").toPandas()
df_items_pd = spark.table("default.silver_order_items").toPandas()
df_vars_pd = spark.table("default.bronze_product_variants").toPandas()
df_prods_pd = spark.table("default.bronze_products").toPandas()

# 2. Filtrar o produto 'Bússola de Bordo 702'
bussola_id = df_prods_pd[df_prods_pd['name'].str.contains('Bússola de Bordo 702', case=False, na=False)]['id'].values[0]
vars_bussola = df_vars_pd[df_vars_pd['product_id'] == bussola_id]['id'].tolist()

df_vendas = df_orders_pd.merge(df_items_pd[df_items_pd['product_variant_id'].isin(vars_bussola)], left_on='id', right_on='order_id')
df_vendas['placed_at'] = pd.to_datetime(df_vendas['placed_at'])

# 3. Agrupamento mensal (Série Temporal)
ts_mensal = df_vendas.set_index('placed_at').resample('MS')['quantity'].sum().fillna(0)

# 4. Split treino/teste conforme premissas do enunciado
treino = ts_mensal[:'2025-12-31']
teste = ts_mensal['2026-01-01':'2026-03-31']

# 5. BASELINE: média móvel dos últimos 3 meses (sem usar dados futuros -> sem data leakage)
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

# 6. Comparação opcional: SARIMAX
from statsmodels.tsa.statespace.sarimax import SARIMAX

modelo = SARIMAX(treino, order=(1, 1, 1), seasonal_order=(1, 1, 0, 12))
modelo_fit = modelo.fit(disp=False)
previsao_sarimax = modelo_fit.forecast(steps=3)
soma_previsao_sarimax = int(round(previsao_sarimax.sum()))
mae_sarimax = mean_absolute_error(teste, previsao_sarimax)

print("\n--- COMPARAÇÃO: SARIMAX ---")
print(f"Soma total arredondada Q1/2026 (SARIMAX): {soma_previsao_sarimax} unidades")
print(f"MAE do SARIMAX: {mae_sarimax:.2f}")

#QUESTÃO 7 — Sistema de Recomendação (Motor de Popa 1949)
from sklearn.metrics.pairwise import cosine_similarity

# 1. Unir vendas com produtos
df_vendas_all = df_orders_pd.merge(df_items_pd, left_on='id', right_on='order_id').merge(df_vars_pd, left_on='product_variant_id', right_on='id')

# 2. Matriz Usuário x Produto (1 comprou, 0 não comprou)
matriz_user_prod = pd.crosstab(df_vendas_all['customer_id'], df_vendas_all['product_id']).clip(upper=1)

# 3. Similaridade de Cosseno entre Produtos
matriz_sim = cosine_similarity(matriz_user_prod.T)
df_sim = pd.DataFrame(matriz_sim, index=matriz_user_prod.columns, columns=matriz_user_prod.columns)

# 4. Produto alvo: 'Motor de Popa 1949'
motor_id = df_prods_pd[df_prods_pd['name'].str.contains('Motor de Popa 1949', case=False, na=False)]['id'].values[0]

# 5. Ranking de similaridade
rec_id = df_sim[motor_id].drop(motor_id).idxmax()
rec_nome = df_prods_pd[df_prods_pd['id'] == rec_id]['name'].values[0]
score = df_sim.loc[motor_id, rec_id]

print("--- QUESTÃO 7: RECOMENDAÇÃO DE PRODUTO ---")
print(f"Produto mais similar ao 'Motor de Popa 1949': {rec_nome} (Score Cosseno: {score:.4f})")
