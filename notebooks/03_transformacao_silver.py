# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Transformação para a camada Silver (limpeza e padronização)
# MAGIC
# MAGIC Cada transformação abaixo responde a um problema medido no notebook 02. Formato de documentação: **o que foi feito → por quê → impacto nos dados**.
# MAGIC
# MAGIC | # | Transformação | Por quê | Impacto |
# MAGIC |---|---|---|---|
# MAGIC | T1 | `trim` + maiúsculas + remoção de acentos nos textos | Evitar que "São Paulo" e "SAO PAULO" virem categorias diferentes (inclusive por arquivos com codificações distintas) | Categorias únicas e comparáveis |
# MAGIC | T1b | Unidade `R$ / m³` → `R$ / m3` | A Bronze tem duas grafias para o GNV (8.732 × 17.945 registros) | Uma única unidade por produto |
# MAGIC | T2 | CNPJ e CEP apenas com dígitos | Padronizar a chave do posto e o CEP | CNPJ com 14 dígitos; CEP com 8 |
# MAGIC | T3 | `data_coleta` de texto `dd/mm/aaaa` para DATE (`try_to_timestamp`) | Permitir filtros e agregações temporais | Datas inválidas viram nulo e são rejeitadas |
# MAGIC | T4 | `valor_venda` e `valor_compra`: vírgula → ponto e `try_cast` para DECIMAL(10,3) | O CSV usa vírgula decimal; DECIMAL evita erro de arredondamento de ponto flutuante | Preços numéricos e somáveis |
# MAGIC | T5 | Regras de validade (campos obrigatórios, preço entre R$ 0,50 e R$ 20,00) | Registros sem data, preço ou posto não respondem às perguntas; preços fora da faixa são erro de digitação | Registros inválidos vão para `silver.precos_revenda_rejeitados`, com o motivo |
# MAGIC | T6 | Deduplicação pela chave de negócio (posto + produto + data) | Um posto tem um único preço por produto em cada coleta | Remove repetições, mantendo a última ingestão |
# MAGIC | T7 | Sinalização de outliers (IQR por produto, UF e mês): `fl_outlier_iqr` | Valores extremos distorcem médias; **sinalizar em vez de apagar** preserva a rastreabilidade | As análises podem excluí-los com um filtro |
# MAGIC | T8 | Remoção de colunas sem uso analítico (`nome_rua`, `numero_rua`, `complemento`) | Minimização de dados: não respondem a nenhuma pergunta | Tabela mais enxuta |

# COMMAND ----------

# MAGIC %run ./00_setup

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

bronze = spark.table(TB["bronze"])
qtd_bronze = bronze.count()

COM_ACENTO = "ÁÀÂÃÄÉÈÊËÍÌÎÏÓÒÔÕÖÚÙÛÜÇÑ"
SEM_ACENTO = "AAAAAEEEEIIIIOOOOOUUUUCN"


def texto_padrao(coluna: str):
    """T1: remove espaços nas pontas, converte para maiúsculas e remove acentos."""
    return F.translate(F.upper(F.trim(F.col(coluna))), COM_ACENTO, SEM_ACENTO)


def numero_br(coluna: str):
    """T4: converte texto com vírgula decimal para DECIMAL(10,3); valor inválido vira nulo."""
    return F.expr(f"try_cast(replace(trim({coluna}), ',', '.') AS DECIMAL(10,3))")


padronizado = bronze.select(
    texto_padrao("regiao_sigla").alias("regiao_sigla"),
    texto_padrao("estado_sigla").alias("uf_sigla"),
    texto_padrao("municipio").alias("municipio"),
    texto_padrao("revenda").alias("revenda"),
    F.regexp_replace("cnpj_revenda", r"\D", "").alias("cnpj_revenda"),       # T2
    texto_padrao("bairro").alias("bairro"),
    F.regexp_replace("cep", r"\D", "").alias("cep"),                          # T2
    F.regexp_replace(texto_padrao("produto"), r"\s+", " ").alias("produto"),
    F.regexp_replace(F.trim("unidade_medida"), "m³", "m3").alias("unidade_medida"),  # T1b: unifica R$ / m³ e R$ / m3
    texto_padrao("bandeira").alias("bandeira"),
    F.expr("CAST(try_to_timestamp(trim(data_coleta), 'dd/MM/yyyy') AS DATE)").alias("data_coleta"),  # T3
    numero_br("valor_venda").alias("valor_venda"),                            # T4
    numero_br("valor_compra").alias("valor_compra"),                          # T4
    "_arquivo_origem",
    "_data_ingestao",
)                                                                             # T8: endereço detalhado não é selecionado

# COMMAND ----------

# MAGIC %md
# MAGIC ## T5 · Regras de validade e separação dos rejeitados

# COMMAND ----------

regras = {
    "data_coleta_invalida": F.col("data_coleta").isNull(),
    "preco_ausente_ou_invalido": F.col("valor_venda").isNull(),
    "preco_fora_da_faixa": (F.col("valor_venda") < 0.5) | (F.col("valor_venda") > 20),
    "cnpj_invalido": F.col("cnpj_revenda").isNull() | (F.length("cnpj_revenda") != 14),
    "uf_invalida": F.col("uf_sigla").isNull() | (F.length("uf_sigla") != 2),
    "produto_ausente": F.col("produto").isNull() | (F.col("produto") == ""),
}
motivo = F.concat_ws(", ", *[F.when(cond, F.lit(nome)) for nome, cond in regras.items()])
avaliado = padronizado.withColumn("motivo_rejeicao", motivo)

rejeitados = avaliado.filter(F.col("motivo_rejeicao") != "")
validos = avaliado.filter(F.col("motivo_rejeicao") == "").drop("motivo_rejeicao")

rejeitados.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(TB["rejeitados"])
display(spark.table(TB["rejeitados"]).groupBy("motivo_rejeicao").count().orderBy(F.desc("count")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## T6 · Deduplicação pela chave de negócio

# COMMAND ----------

janela_chave = Window.partitionBy("cnpj_revenda", "produto", "data_coleta").orderBy(
    F.desc("_data_ingestao"), F.desc("valor_venda")
)
deduplicado = (
    validos.withColumn("_rn", F.row_number().over(janela_chave))
    .filter("_rn = 1")
    .drop("_rn")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## T7 · Sinalização de outliers (IQR por produto, UF e mês)
# MAGIC O IQR é calculado **dentro de cada UF**: um preço alto no Acre é comparado com outros postos do Acre, e não com a média nacional. Assim, preços legitimamente altos de regiões remotas não são marcados como outliers, o que evita distorcer a P2.

# COMMAND ----------

com_mes = deduplicado.withColumn("_ano_mes", F.date_format("data_coleta", "yyyy-MM"))
limites = (
    com_mes.groupBy("produto", "uf_sigla", "_ano_mes")
    .agg(F.expr("percentile_approx(valor_venda, array(0.25, 0.75))").alias("q"))
    .select(
        "produto",
        "uf_sigla",
        "_ano_mes",
        (F.col("q")[0] - 1.5 * (F.col("q")[1] - F.col("q")[0])).alias("_lim_inf"),
        (F.col("q")[1] + 1.5 * (F.col("q")[1] - F.col("q")[0])).alias("_lim_sup"),
    )
)
silver = (
    com_mes.join(limites, ["produto", "uf_sigla", "_ano_mes"], "left")
    .withColumn(
        "fl_outlier_iqr",
        (F.col("valor_venda") < F.col("_lim_inf")) | (F.col("valor_venda") > F.col("_lim_sup")),
    )
    .select(
        "regiao_sigla", "uf_sigla", "municipio", "revenda", "cnpj_revenda", "bairro", "cep",
        "produto", "unidade_medida", "bandeira", "data_coleta", "valor_venda", "valor_compra",
        "fl_outlier_iqr", "_arquivo_origem", "_data_ingestao",
    )
)

(
    silver.write.mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TB["silver"])
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Funil Bronze → Silver (impacto das transformações)

# COMMAND ----------

qtd_rejeitados = spark.table(TB["rejeitados"]).count()
qtd_validos = qtd_bronze - qtd_rejeitados
qtd_silver = spark.table(TB["silver"]).count()
qtd_outliers = spark.table(TB["silver"]).filter("fl_outlier_iqr").count()

funil = spark.createDataFrame(
    [
        ("1. Registros na Bronze", qtd_bronze),
        ("2. Rejeitados por regras de validade (T5)", qtd_rejeitados),
        ("3. Válidos", qtd_validos),
        ("4. Removidos na deduplicação (T6)", qtd_validos - qtd_silver),
        ("5. Registros na Silver", qtd_silver),
        ("6. Sinalizados como outlier (T7, mantidos)", qtd_outliers),
    ],
    "etapa string, registros long",
)
display(funil)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Catálogo de dados da Silver

# COMMAND ----------

COLUNAS_SILVER = {
    "regiao_sigla": "Sigla da regiao. STRING. Dominio: N, NE, CO, SE, S. Linhagem: bronze.regiao_sigla (trim, maiusculas).",
    "uf_sigla": "Sigla da UF. STRING. Dominio: 27 UFs. Linhagem: bronze.estado_sigla (trim, maiusculas).",
    "municipio": "Municipio do posto. STRING, maiusculas e sem acentos. Linhagem: bronze.municipio (T1).",
    "revenda": "Razao social do posto. STRING, maiusculas e sem acentos. Linhagem: bronze.revenda (T1).",
    "cnpj_revenda": "CNPJ do posto, somente digitos. STRING de 14 caracteres. Linhagem: bronze.cnpj_revenda (T2).",
    "bairro": "Bairro do posto. STRING. Linhagem: bronze.bairro (T1).",
    "cep": "CEP, somente digitos. STRING de 8 caracteres. Linhagem: bronze.cep (T2).",
    "produto": "Combustivel. STRING. Dominio: GASOLINA, GASOLINA ADITIVADA, ETANOL, DIESEL, DIESEL S10, GNV. Linhagem: bronze.produto (T1).",
    "unidade_medida": "Unidade do preco. STRING. Dominio: R$ / litro; R$ / m3 (GNV). Linhagem: bronze.unidade_medida (trim).",
    "bandeira": "Bandeira (distribuidora) exibida pelo posto; BRANCA = sem bandeira. STRING. Linhagem: bronze.bandeira (T1).",
    "data_coleta": "Data da coleta do preco. DATE. Dominio: 2025-01-01 a 2026-06-30. Linhagem: bronze.data_coleta dd/mm/aaaa convertida (T3).",
    "valor_venda": "Preco ao consumidor em R$. DECIMAL(10,3). Dominio: 0,50 a 20,00. Linhagem: bronze.valor_venda, virgula para ponto e try_cast (T4, T5).",
    "valor_compra": "Preco de distribuicao em R$. DECIMAL(10,3). Esperado nulo: serie descontinuada pela ANP em ago/2020. Linhagem: bronze.valor_compra (T4).",
    "fl_outlier_iqr": "Indica preco fora de Q1-1,5*IQR e Q3+1,5*IQR do produto na UF e no mes. BOOLEAN. Calculado na Silver (T7).",
    "_arquivo_origem": "Arquivo CSV de origem. STRING. Linhagem: bronze._arquivo_origem.",
    "_data_ingestao": "Data e hora da ingestao na Bronze. TIMESTAMP. Linhagem: bronze._data_ingestao.",
}
documentar_tabela(
    TB["silver"],
    "Precos de revenda limpos, tipados, padronizados e deduplicados. Grao: um preco por posto (CNPJ), "
    "produto e data de coleta. Origem: bronze.precos_revenda_raw, transformacoes T1 a T8 do notebook 03.",
    COLUNAS_SILVER,
)
documentar_tabela(
    TB["rejeitados"],
    "Registros da Bronze que nao passaram nas regras de validade (T5), mantidos para auditoria com o motivo da rejeicao.",
    {**{k: v for k, v in COLUNAS_SILVER.items() if k != "fl_outlier_iqr"},
     "motivo_rejeicao": "Lista de regras violadas. STRING. Dominio: data_coleta_invalida, preco_ausente_ou_invalido, preco_fora_da_faixa, cnpj_invalido, uf_invalida, produto_ausente."},
)

# COMMAND ----------

display(spark.table(TB["silver"]).limit(10))
