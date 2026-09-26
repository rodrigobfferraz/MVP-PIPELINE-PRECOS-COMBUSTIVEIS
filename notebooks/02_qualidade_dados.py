# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Análise de qualidade dos dados (sobre a Bronze)
# MAGIC
# MAGIC Conforme o enunciado, a qualidade é verificada **no dado como capturado**, antes de qualquer tratamento. Os problemas encontrados aqui **justificam as transformações** do notebook 03 (Silver).
# MAGIC
# MAGIC | Dimensão | Pergunta | Como é verificada |
# MAGIC |---|---|---|
# MAGIC | **Completude** | Há nulos ou vazios? Em que proporção? | % de nulos/vazios por atributo |
# MAGIC | **Consistência** | Os valores seguem o padrão esperado? | Expressão regular por atributo e coerência produto × unidade |
# MAGIC | **Unicidade** | Há duplicatas onde não deveria? | Linhas idênticas e chave de negócio repetida (posto + produto + data) |
# MAGIC | **Acurácia** | Os valores fazem sentido? | Preços fora da faixa plausível; datas fora do recorte |
# MAGIC | **Outliers** | Há valores extremos? | Regra IQR (Q1 − 1,5·IQR; Q3 + 1,5·IQR) por produto |
# MAGIC
# MAGIC Todos os resultados são persistidos na tabela `silver.relatorio_qualidade`.

# COMMAND ----------

# MAGIC %run ./00_setup

# COMMAND ----------

from pyspark.sql import functions as F

bronze = spark.table(TB["bronze"])
TOTAL = bronze.count()
ATRIBUTOS = [
    "regiao_sigla", "estado_sigla", "municipio", "revenda", "cnpj_revenda", "nome_rua", "numero_rua",
    "complemento", "bairro", "cep", "produto", "data_coleta", "valor_venda", "valor_compra",
    "unidade_medida", "bandeira",
]
relatorio = []  # (dimensao, atributo, verificacao, valor, observacao)
print(f"Total de registros na Bronze: {TOTAL:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Completude e consistência por atributo

# COMMAND ----------

REGRAS_CONSISTENCIA = {
    "regiao_sigla": (r"^(N|NE|CO|SE|S)$", "sigla de regiao valida"),
    "estado_sigla": (r"^[A-Z]{2}$", "2 letras maiusculas"),
    "cnpj_revenda": (r"^(\d{2}\.\d{3}\.\d{3}/\d{4}-\d{2}|\d{14})$", "CNPJ com 14 digitos"),
    "cep": (r"^\d{5}-?\d{3}$", "CEP com 8 digitos"),
    "data_coleta": (r"^\d{2}/\d{2}/\d{4}$", "data dd/mm/aaaa"),
    "valor_venda": (r"^\d+(,\d+)?$", "numero com virgula decimal"),
    "valor_compra": (r"^\d+(,\d+)?$", "numero com virgula decimal"),
}

exprs = []
for c in ATRIBUTOS:
    vazio = F.col(c).isNull() | (F.trim(F.col(c)) == "")
    exprs.append(F.sum(F.when(vazio, 1).otherwise(0)).alias(f"{c}__vazios"))
    exprs.append(F.approx_count_distinct(c).alias(f"{c}__distintos"))
    if c in REGRAS_CONSISTENCIA:
        padrao = REGRAS_CONSISTENCIA[c][0]
        exprs.append(
            F.sum(F.when(~vazio & F.trim(F.col(c)).rlike(padrao), 1).otherwise(0)).alias(f"{c}__consistentes")
        )
m = bronze.agg(*exprs).first().asDict()

linhas = []
for c in ATRIBUTOS:
    vazios = m[f"{c}__vazios"]
    preenchidos = TOTAL - vazios
    regra = REGRAS_CONSISTENCIA.get(c, (None, "-"))[1]
    consistentes = m.get(f"{c}__consistentes")
    pct_consist = round(100 * consistentes / preenchidos, 2) if consistentes is not None and preenchidos else None
    linhas.append((c, vazios, round(100 * preenchidos / TOTAL, 2), m[f"{c}__distintos"], regra, pct_consist))
    relatorio.append(("Completude", c, "% preenchido", float(round(100 * preenchidos / TOTAL, 2)), f"{vazios} nulos/vazios"))
    if pct_consist is not None:
        relatorio.append(("Consistencia", c, f"% no padrao ({regra})", float(pct_consist), f"{preenchidos - consistentes} fora do padrao"))

perfil = spark.createDataFrame(
    linhas,
    "atributo string, qtd_nulos_vazios long, pct_completude double, qtd_distintos_aprox long, regra_consistencia string, pct_consistente double",
)
display(perfil)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Domínios dos atributos categóricos
# MAGIC Verifica se as categorias observadas correspondem ao domínio esperado e se cada produto tem uma única unidade de medida.

# COMMAND ----------

display(bronze.groupBy("regiao_sigla").count().orderBy("regiao_sigla"))

# COMMAND ----------

display(bronze.groupBy("estado_sigla").count().orderBy("estado_sigla"))
qtd_ufs = bronze.select("estado_sigla").distinct().count()
relatorio.append(("Consistencia", "estado_sigla", "qtd de UFs distintas (esperado 27)", float(qtd_ufs), ""))

# COMMAND ----------

produto_unidade = bronze.groupBy("produto", "unidade_medida").count().orderBy("produto")
display(produto_unidade)
for r in produto_unidade.collect():
    relatorio.append(("Consistencia", "produto x unidade_medida", f"{r['produto']} | {r['unidade_medida']}", float(r["count"]), "registros"))

# COMMAND ----------

display(bronze.groupBy("bandeira").count().orderBy(F.desc("count")).limit(30))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Unicidade

# COMMAND ----------

distintos_linha = bronze.select(*ATRIBUTOS).distinct().count()
dup_exatas = TOTAL - distintos_linha

chave = ["cnpj_revenda", "produto", "data_coleta"]
por_chave = bronze.groupBy(*chave).agg(F.count("*").alias("n"), F.countDistinct("valor_venda").alias("precos"))
chaves_repetidas = por_chave.filter("n > 1").count()
chaves_conflitantes = por_chave.filter("precos > 1").count()

relatorio += [
    ("Unicidade", "linha inteira", "duplicatas exatas", float(dup_exatas), "linhas identicas em todos os atributos"),
    ("Unicidade", "cnpj+produto+data", "chaves repetidas", float(chaves_repetidas), "mesmo posto, produto e data em mais de uma linha"),
    ("Unicidade", "cnpj+produto+data", "chaves com precos diferentes", float(chaves_conflitantes), "conflito de preco para a mesma chave"),
]
print(f"Duplicatas exatas: {dup_exatas:,} | chaves repetidas: {chaves_repetidas:,} | chaves conflitantes: {chaves_conflitantes:,}")
display(por_chave.filter("n > 1").orderBy(F.desc("n")).limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Acurácia (faixas plausíveis e recorte temporal)
# MAGIC Converte-se o texto para número e data **apenas para medir**; nada é gravado aqui. Faixa plausível adotada para o preço: **R$ 0,50 a R$ 20,00** por litro ou m³.

# COMMAND ----------

tipado = (
    bronze.withColumn("preco", F.expr("try_cast(replace(trim(valor_venda), ',', '.') AS DECIMAL(10,3))"))
    .withColumn("data", F.expr("CAST(try_to_timestamp(trim(data_coleta), 'dd/MM/yyyy') AS DATE)"))
)

acc = tipado.agg(
    F.sum(F.when(F.col("preco").isNull() & F.col("valor_venda").isNotNull(), 1).otherwise(0)).alias("preco_nao_conversivel"),
    F.sum(F.when(F.col("preco") <= 0, 1).otherwise(0)).alias("preco_nao_positivo"),
    F.sum(F.when((F.col("preco") < 0.5) | (F.col("preco") > 20), 1).otherwise(0)).alias("preco_fora_faixa"),
    F.sum(F.when(F.col("data").isNull() & F.col("data_coleta").isNotNull(), 1).otherwise(0)).alias("data_nao_conversivel"),
    F.sum(F.when((F.col("data") < "2025-01-01") | (F.col("data") > "2026-06-30"), 1).otherwise(0)).alias("data_fora_recorte"),
    F.min("data").alias("data_min"),
    F.max("data").alias("data_max"),
).first()

for campo in ["preco_nao_conversivel", "preco_nao_positivo", "preco_fora_faixa", "data_nao_conversivel", "data_fora_recorte"]:
    relatorio.append(("Acuracia", "valor_venda" if "preco" in campo else "data_coleta", campo, float(acc[campo] or 0), ""))
relatorio.append(("Acuracia", "data_coleta", "intervalo observado", None, f"{acc['data_min']} a {acc['data_max']}"))
print(acc)

display(
    tipado.groupBy("produto").agg(
        F.count("preco").alias("qtd"),
        F.min("preco").alias("min"),
        F.expr("percentile_approx(preco, 0.5)").alias("mediana"),
        F.round(F.avg("preco"), 3).alias("media"),
        F.max("preco").alias("max"),
    ).orderBy("produto")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Outliers (regra IQR por produto)

# COMMAND ----------

limites = (
    tipado.groupBy("produto")
    .agg(F.expr("percentile_approx(preco, array(0.25, 0.75))").alias("q"))
    .select(
        "produto",
        (F.col("q")[0] - 1.5 * (F.col("q")[1] - F.col("q")[0])).alias("lim_inf"),
        (F.col("q")[1] + 1.5 * (F.col("q")[1] - F.col("q")[0])).alias("lim_sup"),
    )
)
outliers = (
    tipado.join(limites, "produto")
    .groupBy("produto", "lim_inf", "lim_sup")
    .agg(
        F.count("preco").alias("qtd"),
        F.sum(F.when((F.col("preco") < F.col("lim_inf")) | (F.col("preco") > F.col("lim_sup")), 1).otherwise(0)).alias("qtd_outliers"),
    )
    .withColumn("pct_outliers", F.round(100 * F.col("qtd_outliers") / F.col("qtd"), 3))
    .orderBy("produto")
)
display(outliers)
for r in outliers.collect():
    relatorio.append(("Outliers", "valor_venda", f"IQR {r['produto']}", float(r["pct_outliers"] or 0), f"{r['qtd_outliers']} de {r['qtd']} registros"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Persistência do relatório de qualidade

# COMMAND ----------

df_relatorio = spark.createDataFrame(
    relatorio, "dimensao string, atributo string, verificacao string, valor double, observacao string"
).withColumn("data_execucao", F.current_timestamp())
df_relatorio.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(TB["qualidade"])

documentar_tabela(
    TB["qualidade"],
    "Relatorio de qualidade dos dados medido sobre a camada Bronze (dado como capturado). "
    "Cada linha e uma verificacao de uma dimensao de qualidade para um atributo.",
    {
        "dimensao": "Dimensao de qualidade. Dominio: Completude, Consistencia, Unicidade, Acuracia, Outliers.",
        "atributo": "Atributo (coluna) da Bronze avaliado, ou combinacao de atributos.",
        "verificacao": "Descricao da verificacao realizada.",
        "valor": "Resultado numerico (percentual ou contagem, conforme a verificacao).",
        "observacao": "Detalhe complementar do resultado.",
        "data_execucao": "Data e hora da execucao da verificacao.",
    },
)
display(spark.table(TB["qualidade"]).orderBy("dimensao", "atributo"))
