# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · Modelagem dimensional na camada Gold (esquema estrela)
# MAGIC
# MAGIC **Por que esquema estrela?** Existe um evento central e mensurável, *um preço coletado*, descrito por perguntas do tipo **quando** (tempo), **onde** (localidade), **o quê** (produto) e **quem** (posto e sua bandeira). É o caso clássico de fato cercado de dimensões, e as perguntas de negócio viram agregações simples (`GROUP BY`) sobre ele.
# MAGIC
# MAGIC ```
# MAGIC                 dim_tempo
# MAGIC                     |
# MAGIC  dim_localidade — fato_preco_revenda — dim_produto
# MAGIC                     |
# MAGIC                 dim_posto
# MAGIC ```
# MAGIC
# MAGIC | Tabela | Grão | Chave |
# MAGIC |---|---|---|
# MAGIC | `fato_preco_revenda` | 1 preço de 1 produto em 1 posto em 1 data de coleta | sk_tempo + sk_posto + sk_produto |
# MAGIC | `dim_tempo` | 1 data de coleta | `sk_tempo` (aaaammdd) |
# MAGIC | `dim_localidade` | 1 município | `sk_localidade` |
# MAGIC | `dim_produto` | 1 produto | `sk_produto` |
# MAGIC | `dim_posto` | 1 posto (CNPJ) | `sk_posto` |
# MAGIC | `agg_preco_mensal_uf` | 1 produto × 1 UF × 1 mês (sem outliers) | ano_mes + uf_sigla + produto_analise |
# MAGIC
# MAGIC **Decisões de modelagem:**
# MAGIC - **Chaves substitutas** (`sk_*`) desacoplam o modelo das chaves de origem.
# MAGIC - **`produto_analise`** padroniza os rótulos da ANP em categorias estáveis (por exemplo, "DIESEL S10"), para que as consultas não dependam de variações de grafia.
# MAGIC - **`dim_posto`** guarda os atributos mais recentes do posto (SCD tipo 1). Postos que trocaram de bandeira no período são sinalizados em `qtd_bandeiras_periodo`. Isso é uma simplificação consciente do MVP.
# MAGIC - **Restrições PK/FK informativas** no Unity Catalog permitem que o Catalog Explorer exiba o diagrama entidade-relacionamento.

# COMMAND ----------

# MAGIC %run ./00_setup

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

silver = spark.table(TB["silver"])


def salvar(df, chave: str) -> None:
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(TB[chave])


# Remove restrições antigas antes de sobrescrever as tabelas (execuções repetidas).
for tabela, restricao in [
    ("fato", "fk_fato_tempo"), ("fato", "fk_fato_localidade"), ("fato", "fk_fato_produto"), ("fato", "fk_fato_posto"),
    ("dim_tempo", "pk_dim_tempo"), ("dim_localidade", "pk_dim_localidade"),
    ("dim_produto", "pk_dim_produto"), ("dim_posto", "pk_dim_posto"),
]:
    try:
        spark.sql(f"ALTER TABLE {TB[tabela]} DROP CONSTRAINT IF EXISTS {restricao}")
    except Exception:
        pass  # a tabela ainda não existe na primeira execução

# COMMAND ----------

# MAGIC %md
# MAGIC ## dim_tempo

# COMMAND ----------

MESES = ["", "Janeiro", "Fevereiro", "Marco", "Abril", "Maio", "Junho", "Julho", "Agosto", "Setembro", "Outubro", "Novembro", "Dezembro"]
mapa_meses = F.create_map(*[x for i, nome in enumerate(MESES) if i for x in (F.lit(i), F.lit(nome))])

dim_tempo = (
    silver.select("data_coleta").distinct()
    .withColumnRenamed("data_coleta", "data")
    .withColumn("sk_tempo", F.date_format("data", "yyyyMMdd").cast("int"))
    .withColumn("ano", F.year("data"))
    .withColumn("mes", F.month("data"))
    .withColumn("nome_mes", mapa_meses[F.month("data")])
    .withColumn("ano_mes", F.date_format("data", "yyyy-MM"))
    .withColumn("trimestre", F.quarter("data"))
    .withColumn("semestre", F.when(F.month("data") <= 6, 1).otherwise(2))
    .withColumn("semana_ano", F.weekofyear("data"))
    .select("sk_tempo", "data", "ano", "mes", "nome_mes", "ano_mes", "trimestre", "semestre", "semana_ano")
)
salvar(dim_tempo, "dim_tempo")

# COMMAND ----------

# MAGIC %md
# MAGIC ## dim_localidade

# COMMAND ----------

REGIOES = {"N": "Norte", "NE": "Nordeste", "CO": "Centro-Oeste", "SE": "Sudeste", "S": "Sul"}
UFS = {
    "AC": ("Acre", "RIO BRANCO"), "AL": ("Alagoas", "MACEIO"), "AP": ("Amapa", "MACAPA"),
    "AM": ("Amazonas", "MANAUS"), "BA": ("Bahia", "SALVADOR"), "CE": ("Ceara", "FORTALEZA"),
    "DF": ("Distrito Federal", "BRASILIA"), "ES": ("Espirito Santo", "VITORIA"), "GO": ("Goias", "GOIANIA"),
    "MA": ("Maranhao", "SAO LUIS"), "MT": ("Mato Grosso", "CUIABA"), "MS": ("Mato Grosso do Sul", "CAMPO GRANDE"),
    "MG": ("Minas Gerais", "BELO HORIZONTE"), "PA": ("Para", "BELEM"), "PB": ("Paraiba", "JOAO PESSOA"),
    "PR": ("Parana", "CURITIBA"), "PE": ("Pernambuco", "RECIFE"), "PI": ("Piaui", "TERESINA"),
    "RJ": ("Rio de Janeiro", "RIO DE JANEIRO"), "RN": ("Rio Grande do Norte", "NATAL"),
    "RS": ("Rio Grande do Sul", "PORTO ALEGRE"), "RO": ("Rondonia", "PORTO VELHO"), "RR": ("Roraima", "BOA VISTA"),
    "SC": ("Santa Catarina", "FLORIANOPOLIS"), "SP": ("Sao Paulo", "SAO PAULO"), "SE": ("Sergipe", "ARACAJU"),
    "TO": ("Tocantins", "PALMAS"),
}
mapa_regiao = F.create_map(*[x for k, v in REGIOES.items() for x in (F.lit(k), F.lit(v))])
mapa_uf = F.create_map(*[x for k, (nome, _) in UFS.items() for x in (F.lit(k), F.lit(nome))])
mapa_capital = F.create_map(*[x for k, (_, cap) in UFS.items() for x in (F.lit(k), F.lit(cap))])

dim_localidade = (
    silver.groupBy("uf_sigla", "municipio").agg(F.max("regiao_sigla").alias("regiao_sigla"))
    .withColumn("regiao_nome", mapa_regiao[F.col("regiao_sigla")])
    .withColumn("uf_nome", mapa_uf[F.col("uf_sigla")])
    .withColumn("fl_capital", F.col("municipio") == mapa_capital[F.col("uf_sigla")])
    .withColumn("sk_localidade", F.row_number().over(Window.orderBy("uf_sigla", "municipio")))
    .select("sk_localidade", "regiao_sigla", "regiao_nome", "uf_sigla", "uf_nome", "municipio", "fl_capital")
)
salvar(dim_localidade, "dim_localidade")

# COMMAND ----------

# MAGIC %md
# MAGIC ## dim_produto

# COMMAND ----------

produto_analise = (
    F.when(F.col("produto").contains("GASOLINA") & F.col("produto").contains("ADITIVADA"), "GASOLINA ADITIVADA")
    .when(F.col("produto").contains("GASOLINA"), "GASOLINA COMUM")
    .when(F.col("produto").contains("ETANOL"), "ETANOL")
    .when(F.col("produto").contains("DIESEL") & F.col("produto").rlike("S-?10"), "DIESEL S10")
    .when(F.col("produto").contains("DIESEL"), "DIESEL S500")
    .when(F.col("produto").contains("GNV"), "GNV")
    .otherwise("OUTROS")
)
grupo_produto = (
    F.when(F.col("produto_analise").startswith("GASOLINA"), "Gasolina")
    .when(F.col("produto_analise") == "ETANOL", "Etanol")
    .when(F.col("produto_analise").startswith("DIESEL"), "Diesel")
    .when(F.col("produto_analise") == "GNV", "GNV")
    .otherwise("Outros")
)
dim_produto = (
    silver.groupBy("produto").agg(F.first("unidade_medida", ignorenulls=True).alias("unidade_medida"))
    .withColumn("produto_analise", produto_analise)
    .withColumn("grupo_produto", grupo_produto)
    .withColumn("sk_produto", F.row_number().over(Window.orderBy("produto")))
    .select("sk_produto", "produto", "produto_analise", "grupo_produto", "unidade_medida")
)
salvar(dim_produto, "dim_produto")
display(spark.table(TB["dim_produto"]))

# COMMAND ----------

# MAGIC %md
# MAGIC ## dim_posto

# COMMAND ----------

ultimo_registro = Window.partitionBy("cnpj_revenda").orderBy(F.desc("data_coleta"), F.desc("_data_ingestao"))
atributos_recentes = (
    silver.withColumn("_rn", F.row_number().over(ultimo_registro)).filter("_rn = 1")
    .select("cnpj_revenda", "revenda", "bandeira", "bairro", "cep")
)
qtd_bandeiras = silver.groupBy("cnpj_revenda").agg(F.countDistinct("bandeira").alias("qtd_bandeiras_periodo"))

dim_posto = (
    atributos_recentes.join(qtd_bandeiras, "cnpj_revenda")
    .withColumn("tipo_bandeira", F.when(F.col("bandeira") == "BRANCA", "BANDEIRA BRANCA").otherwise("BANDEIRADO"))
    .withColumn("sk_posto", F.row_number().over(Window.orderBy("cnpj_revenda")))
    .select("sk_posto", "cnpj_revenda", "revenda", "bandeira", "tipo_bandeira", "qtd_bandeiras_periodo", "bairro", "cep")
)
salvar(dim_posto, "dim_posto")

# COMMAND ----------

# MAGIC %md
# MAGIC ## fato_preco_revenda
# MAGIC Junção da Silver com as quatro dimensões pelas chaves naturais, substituídas pelas chaves substitutas.

# COMMAND ----------

d_tempo = spark.table(TB["dim_tempo"]).select("sk_tempo", F.col("data").alias("data_coleta"))
d_local = spark.table(TB["dim_localidade"]).select("sk_localidade", "uf_sigla", "municipio")
d_prod = spark.table(TB["dim_produto"]).select("sk_produto", "produto")
d_posto = spark.table(TB["dim_posto"]).select("sk_posto", "cnpj_revenda")

fato = (
    silver.join(d_tempo, "data_coleta")
    .join(d_local, ["uf_sigla", "municipio"])
    .join(d_prod, "produto")
    .join(d_posto, "cnpj_revenda")
    .select(
        "sk_tempo", "sk_localidade", "sk_produto", "sk_posto",
        "valor_venda", "fl_outlier_iqr", F.col("_arquivo_origem").alias("arquivo_origem"),
    )
)
salvar(fato, "fato")

qtd_silver, qtd_fato = silver.count(), spark.table(TB["fato"]).count()
print(f"Silver: {qtd_silver:,} | Fato: {qtd_fato:,} | Diferença (deve ser 0): {qtd_silver - qtd_fato:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## agg_preco_mensal_uf (métricas pré-calculadas, sem outliers)

# COMMAND ----------

agg = (
    spark.table(TB["fato"]).filter("NOT fl_outlier_iqr")
    .join(spark.table(TB["dim_tempo"]), "sk_tempo")
    .join(spark.table(TB["dim_localidade"]), "sk_localidade")
    .join(spark.table(TB["dim_produto"]), "sk_produto")
    .groupBy("ano_mes", "regiao_sigla", "uf_sigla", "produto_analise", "grupo_produto")
    .agg(
        F.count("*").alias("qtd_coletas"),
        F.countDistinct("sk_posto").alias("qtd_postos"),
        F.round(F.avg("valor_venda"), 3).cast("decimal(10,3)").alias("preco_medio"),
        F.expr("percentile_approx(valor_venda, 0.5)").cast("decimal(10,3)").alias("preco_mediano"),
        F.min("valor_venda").alias("preco_min"),
        F.max("valor_venda").alias("preco_max"),
    )
)
salvar(agg, "agg")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Catálogo de dados da Gold (Unity Catalog)

# COMMAND ----------

documentar_tabela(TB["dim_tempo"], "Dimensao de tempo: uma linha por data de coleta presente nos dados (jan/2025 a jun/2026). Origem: silver.precos_revenda.data_coleta.", {
    "sk_tempo": "Chave substituta no formato aaaammdd. INT. PK. Ex.: 20250106.",
    "data": "Data da coleta. DATE. Dominio: 2025-01-01 a 2026-06-30. Linhagem: silver.data_coleta.",
    "ano": "Ano. INT. Dominio: 2025 a 2026. Derivado de data.",
    "mes": "Mes. INT. Dominio: 1 a 12. Derivado de data.",
    "nome_mes": "Nome do mes em portugues. STRING. Dominio: Janeiro a Dezembro.",
    "ano_mes": "Ano e mes no formato aaaa-mm. STRING. Usado para agregacoes mensais.",
    "trimestre": "Trimestre. INT. Dominio: 1 a 4.",
    "semestre": "Semestre. INT. Dominio: 1 ou 2.",
    "semana_ano": "Semana do ano (ISO). INT. Dominio: 1 a 53.",
})
documentar_tabela(TB["dim_localidade"], "Dimensao geografica: uma linha por municipio pesquisado pela ANP. Origem: silver.precos_revenda, enriquecida com nomes de regiao, UF e indicador de capital.", {
    "sk_localidade": "Chave substituta. INT. PK.",
    "regiao_sigla": "Sigla da regiao. STRING. Dominio: N, NE, CO, SE, S. Linhagem: silver.regiao_sigla.",
    "regiao_nome": "Nome da regiao. STRING. Dominio: Norte, Nordeste, Centro-Oeste, Sudeste, Sul. Enriquecimento por tabela de referencia.",
    "uf_sigla": "Sigla da UF. STRING. Dominio: 27 UFs. Linhagem: silver.uf_sigla.",
    "uf_nome": "Nome da UF (sem acentos). STRING. Enriquecimento por tabela de referencia.",
    "municipio": "Municipio (maiusculas, sem acentos). STRING. Linhagem: silver.municipio.",
    "fl_capital": "Indica se o municipio e a capital da UF. BOOLEAN. Derivado por comparacao com a lista de capitais.",
})
documentar_tabela(TB["dim_produto"], "Dimensao de produto: uma linha por combustivel conforme rotulo da ANP, com categoria padronizada para analise. Origem: silver.precos_revenda.produto.", {
    "sk_produto": "Chave substituta. INT. PK.",
    "produto": "Rotulo original do combustivel (padronizado na Silver). STRING. Linhagem: silver.produto.",
    "produto_analise": "Categoria padronizada. STRING. Dominio: GASOLINA COMUM, GASOLINA ADITIVADA, ETANOL, DIESEL S10, DIESEL S500, GNV, OUTROS.",
    "grupo_produto": "Familia do combustivel. STRING. Dominio: Gasolina, Etanol, Diesel, GNV, Outros.",
    "unidade_medida": "Unidade do preco. STRING. Dominio: R$ / litro; R$ / m3 (GNV). Linhagem: silver.unidade_medida.",
})
documentar_tabela(TB["dim_posto"], "Dimensao de posto revendedor: uma linha por CNPJ, com os atributos da coleta mais recente (SCD tipo 1). Origem: silver.precos_revenda.", {
    "sk_posto": "Chave substituta. INT. PK.",
    "cnpj_revenda": "CNPJ do posto, 14 digitos. STRING. Chave natural. Linhagem: silver.cnpj_revenda.",
    "revenda": "Razao social mais recente. STRING. Linhagem: silver.revenda.",
    "bandeira": "Bandeira mais recente (distribuidora); BRANCA = sem bandeira. STRING. Linhagem: silver.bandeira.",
    "tipo_bandeira": "Classificacao. STRING. Dominio: BANDEIRADO, BANDEIRA BRANCA. Derivado de bandeira.",
    "qtd_bandeiras_periodo": "Quantidade de bandeiras distintas do posto no periodo. BIGINT. Dominio: 1 ou mais; maior que 1 indica troca de bandeira.",
    "bairro": "Bairro mais recente. STRING. Linhagem: silver.bairro.",
    "cep": "CEP mais recente, 8 digitos. STRING. Linhagem: silver.cep.",
})
documentar_tabela(TB["fato"], "Fato de precos de revenda. Grao: um preco de um produto em um posto em uma data de coleta. Origem: silver.precos_revenda com juncao das quatro dimensoes pelas chaves naturais.", {
    "sk_tempo": "FK para dim_tempo. INT.",
    "sk_localidade": "FK para dim_localidade. INT. Obtida por uf_sigla + municipio.",
    "sk_produto": "FK para dim_produto. INT. Obtida por produto.",
    "sk_posto": "FK para dim_posto. INT. Obtida por cnpj_revenda.",
    "valor_venda": "Preco ao consumidor em R$ por litro (ou m3 para GNV). DECIMAL(10,3). Dominio: 0,50 a 20,00. Linhagem: silver.valor_venda.",
    "fl_outlier_iqr": "Indica preco atipico pela regra IQR (produto x mes). BOOLEAN. Linhagem: silver.fl_outlier_iqr.",
    "arquivo_origem": "Arquivo CSV de origem, para rastreabilidade. STRING. Linhagem: bronze._arquivo_origem.",
})
documentar_tabela(TB["agg"], "Agregado mensal de precos por UF e produto, excluindo outliers. Grao: ano_mes x uf_sigla x produto_analise. Origem: fato_preco_revenda com dim_tempo, dim_localidade e dim_produto.", {
    "ano_mes": "Mes de referencia, aaaa-mm. STRING. Linhagem: dim_tempo.ano_mes.",
    "regiao_sigla": "Sigla da regiao. STRING. Linhagem: dim_localidade.",
    "uf_sigla": "Sigla da UF. STRING. Linhagem: dim_localidade.",
    "produto_analise": "Categoria padronizada do produto. STRING. Linhagem: dim_produto.",
    "grupo_produto": "Familia do produto. STRING. Linhagem: dim_produto.",
    "qtd_coletas": "Quantidade de precos coletados. BIGINT. Minimo 1.",
    "qtd_postos": "Quantidade de postos distintos. BIGINT. Minimo 1.",
    "preco_medio": "Media simples do preco de venda. DECIMAL(10,3). Faixa 0,50 a 20,00.",
    "preco_mediano": "Mediana aproximada do preco de venda. DECIMAL(10,3).",
    "preco_min": "Menor preco observado. DECIMAL(10,3).",
    "preco_max": "Maior preco observado. DECIMAL(10,3).",
})

# COMMAND ----------

# MAGIC %md
# MAGIC ## Restrições PK/FK (informativas) para o diagrama do Catalog Explorer

# COMMAND ----------

def executar(sql: str) -> None:
    try:
        spark.sql(sql)
    except Exception as erro:
        print(f"[aviso] {sql[:70]}... -> {str(erro)[:150]}")


for chave, coluna, nome in [
    ("dim_tempo", "sk_tempo", "pk_dim_tempo"), ("dim_localidade", "sk_localidade", "pk_dim_localidade"),
    ("dim_produto", "sk_produto", "pk_dim_produto"), ("dim_posto", "sk_posto", "pk_dim_posto"),
]:
    executar(f"ALTER TABLE {TB[chave]} ALTER COLUMN {coluna} SET NOT NULL")
    executar(f"ALTER TABLE {TB[chave]} ADD CONSTRAINT {nome} PRIMARY KEY ({coluna})")

for coluna, dim, nome in [
    ("sk_tempo", "dim_tempo", "fk_fato_tempo"), ("sk_localidade", "dim_localidade", "fk_fato_localidade"),
    ("sk_produto", "dim_produto", "fk_fato_produto"), ("sk_posto", "dim_posto", "fk_fato_posto"),
]:
    executar(f"ALTER TABLE {TB['fato']} ADD CONSTRAINT {nome} FOREIGN KEY ({coluna}) REFERENCES {TB[dim]} ({coluna})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Evidências: tabelas persistidas na Gold

# COMMAND ----------

display(spark.sql(f"SHOW TABLES IN {CATALOGO}.gold"))
