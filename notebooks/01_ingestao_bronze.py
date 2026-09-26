# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Coleta e ingestão na camada Bronze
# MAGIC
# MAGIC **Fonte:** ANP, Série Histórica de Preços de Combustíveis e de GLP (Levantamento de Preços de Combustíveis, LPC), dados semanais agrupados por semestre, arquivos de *combustíveis automotivos*.
# MAGIC
# MAGIC **Recorte:** 1º semestre de 2025, 2º semestre de 2025 e 1º semestre de 2026 (jan/2025 a jun/2026).
# MAGIC
# MAGIC **Processo (padrão ELT):**
# MAGIC 1. Os arquivos `.zip` (ou os `.csv` já extraídos) são colocados no Volume `bronze.arquivos_anp`, por upload manual ou por download automático (célula opcional abaixo).
# MAGIC 2. Arquivos `.zip` presentes no Volume são extraídos automaticamente.
# MAGIC 3. Cada CSV é lido **como texto (STRING)**, sem inferência de tipos, com detecção de codificação (UTF-8 ou ISO-8859-1).
# MAGIC 4. São adicionados **metadados de controle**: arquivo de origem, codificação detectada, fonte e data/hora da ingestão.
# MAGIC 5. O resultado é gravado na tabela Delta `bronze.precos_revenda_raw`.
# MAGIC
# MAGIC **Princípio da Bronze:** o **conteúdo** não é alterado. A única intervenção é técnica: os nomes das colunas originais (com espaços, acentos e hífens) são convertidos para *snake_case*, porque tabelas Delta não aceitam espaços em nomes de colunas por padrão. O mapeamento está documentado no catálogo.

# COMMAND ----------

# MAGIC %run ./00_setup

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Obtenção dos arquivos (download automático opcional)
# MAGIC
# MAGIC Se o ambiente não tiver acesso à internet, esta célula apenas registra um aviso. Nesse caso, baixe os três `.zip` pelos links abaixo e faça o upload no Volume pela interface: **Catalog → `bronze` → Volumes → `arquivos_anp` → Upload to this volume**.

# COMMAND ----------

import io
import os
import zipfile
import urllib.request

BAIXAR_AUTOMATICAMENTE = True

ARQUIVOS_FONTE = {
    "ca-2025-01.zip": "https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/arquivos/shpc/dsas/ca/ca-2025-01.zip",
    "ca-2025-02.zip": "https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/arquivos/shpc/dsas/ca/ca-2025-02.zip",
    "ca-2026-01.zip": "https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/arquivos/shpc/dsas/ca/ca-2026-01.zip",
}


def listar(extensao: str) -> list:
    return sorted(f for f in os.listdir(VOLUME) if f.lower().endswith(extensao))


def baixar(nome: str, url: str) -> None:
    requisicao = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(requisicao, timeout=180) as resposta:
        conteudo = resposta.read()
    with open(f"{VOLUME}/{nome}", "wb") as destino:
        destino.write(conteudo)
    print(f"baixado: {nome} ({len(conteudo)/1e6:.1f} MB)")


if BAIXAR_AUTOMATICAMENTE:
    for nome, url in ARQUIVOS_FONTE.items():
        if nome in listar(".zip"):
            print(f"já existe no Volume: {nome}")
            continue
        try:
            baixar(nome, url)
        except Exception as erro:
            print(f"[aviso] download automático indisponível para {nome}: {erro}. Faça o upload manual.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Extração dos arquivos `.zip` no Volume

# COMMAND ----------

for arquivo_zip in listar(".zip"):
    with zipfile.ZipFile(f"{VOLUME}/{arquivo_zip}") as z:
        for membro in z.namelist():
            if not membro.lower().endswith(".csv"):
                continue
            destino = f"{VOLUME}/{os.path.basename(membro)}"
            if os.path.exists(destino):
                print(f"já extraído: {os.path.basename(membro)}")
                continue
            with z.open(membro) as origem, open(destino, "wb") as saida:
                saida.write(origem.read())
            print(f"extraído: {arquivo_zip} -> {os.path.basename(membro)}")

ARQUIVOS_CSV = listar(".csv")
assert ARQUIVOS_CSV, f"Nenhum CSV encontrado em {VOLUME}. Faça o upload dos arquivos antes de continuar."
print("CSVs que serão ingeridos:", ARQUIVOS_CSV)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Leitura como texto, detecção de codificação e metadados de controle
# MAGIC
# MAGIC - **Separador `;`**, cabeçalho na primeira linha, **sem inferência de tipos**: todos os campos entram como STRING, preservando o dado original (por exemplo, `6,29` com vírgula decimal).
# MAGIC - **Codificação:** a leitura é feita em UTF-8. Se aparecer o caractere de substituição `�` (sinal de arquivo em Latin-1), o arquivo é relido em ISO-8859-1.

# COMMAND ----------

import re
import unicodedata
from functools import reduce
from pyspark.sql import functions as F

FONTE = "ANP - Serie Historica de Precos de Combustiveis (LPC) - combustiveis automotivos"

# nome original normalizado -> nome da coluna na Bronze
MAPA_COLUNAS = {
    "regiao_sigla": "regiao_sigla",
    "estado_sigla": "estado_sigla",
    "municipio": "municipio",
    "revenda": "revenda",
    "cnpj_da_revenda": "cnpj_revenda",
    "nome_da_rua": "nome_rua",
    "numero_rua": "numero_rua",
    "complemento": "complemento",
    "bairro": "bairro",
    "cep": "cep",
    "produto": "produto",
    "data_da_coleta": "data_coleta",
    "valor_de_venda": "valor_venda",
    "valor_de_compra": "valor_compra",
    "unidade_de_medida": "unidade_medida",
    "bandeira": "bandeira",
}


def normalizar_nome(coluna: str) -> str:
    coluna = coluna.replace("﻿", "").strip()
    coluna = unicodedata.normalize("NFKD", coluna).encode("ascii", "ignore").decode()
    return re.sub(r"[^0-9a-zA-Z]+", "_", coluna).strip("_").lower()


def ler_csv(caminho: str, encoding: str):
    df = (
        spark.read.option("header", True)
        .option("sep", ";")
        .option("encoding", encoding)
        .option("inferSchema", False)
        .csv(caminho)
    )
    return df, df.toDF(*[normalizar_nome(c) for c in df.columns])


def tem_caractere_invalido(df_original, df_normalizado) -> bool:
    if any("�" in c for c in df_original.columns):
        return True
    colunas_texto = [c for c in ("municipio", "revenda", "bairro") if c in df_normalizado.columns]
    if not colunas_texto:
        return False
    condicao = reduce(lambda a, b: a | b, [F.col(c).contains("�") for c in colunas_texto])
    return df_normalizado.filter(condicao).limit(1).count() > 0


partes, log_ingestao = [], []
for arquivo in ARQUIVOS_CSV:
    caminho = f"{VOLUME}/{arquivo}"
    encoding = "UTF-8"
    df_original, df = ler_csv(caminho, encoding)
    if tem_caractere_invalido(df_original, df):
        encoding = "ISO-8859-1"
        df_original, df = ler_csv(caminho, encoding)

    faltantes = [c for c in MAPA_COLUNAS if c not in df.columns]
    extras = [c for c in df.columns if c not in MAPA_COLUNAS]
    for c in faltantes:
        df = df.withColumn(c, F.lit(None).cast("string"))

    df = (
        df.select([F.col(origem).alias(destino) for origem, destino in MAPA_COLUNAS.items()])
        .withColumn("_arquivo_origem", F.lit(arquivo))
        .withColumn("_encoding_detectado", F.lit(encoding))
        .withColumn("_fonte", F.lit(FONTE))
        .withColumn("_data_ingestao", F.current_timestamp())
    )
    partes.append(df)
    log_ingestao.append((arquivo, encoding, ", ".join(faltantes) or "-", ", ".join(extras) or "-"))

display(spark.createDataFrame(log_ingestao, "arquivo string, encoding string, colunas_faltantes string, colunas_extras string"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Gravação da tabela Bronze (Delta)
# MAGIC
# MAGIC Modo `overwrite`: o pipeline é **reprodutível e idempotente**. Executá-lo novamente recria a Bronze a partir dos arquivos do Volume, sem duplicar registros.

# COMMAND ----------

bronze = reduce(lambda a, b: a.unionByName(b), partes)
(
    bronze.write.mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TB["bronze"])
)

display(
    spark.table(TB["bronze"])
    .groupBy("_arquivo_origem", "_encoding_detectado")
    .count()
    .orderBy("_arquivo_origem")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Catálogo de dados da Bronze (Unity Catalog)

# COMMAND ----------

documentar_tabela(
    TB["bronze"],
    "Precos de revenda de combustiveis automotivos coletados semanalmente pela ANP (LPC), "
    "exatamente como publicados (todos os campos em texto). Uma linha = um preco de um produto "
    "em um posto em uma data de coleta. Fonte: gov.br/anp, dados abertos. Recorte: jan/2025 a jun/2026.",
    {
        "regiao_sigla": "Original: Regiao - Sigla. Sigla da regiao do posto. Dominio esperado: N, NE, CO, SE, S. Texto bruto.",
        "estado_sigla": "Original: Estado - Sigla. Sigla da UF do posto. Dominio esperado: 27 UFs. Texto bruto.",
        "municipio": "Original: Municipio. Nome do municipio do posto. Texto bruto.",
        "revenda": "Original: Revenda. Razao social do posto revendedor (pessoa juridica). Texto bruto.",
        "cnpj_revenda": "Original: CNPJ da Revenda. CNPJ do posto (formato com mascara). Texto bruto.",
        "nome_rua": "Original: Nome da Rua. Logradouro do posto. Texto bruto.",
        "numero_rua": "Original: Numero Rua. Numero do logradouro. Texto bruto.",
        "complemento": "Original: Complemento. Complemento do endereco. Texto bruto, frequentemente vazio.",
        "bairro": "Original: Bairro. Bairro do posto. Texto bruto.",
        "cep": "Original: Cep. CEP do posto (esperado 8 digitos, com ou sem hifen). Texto bruto.",
        "produto": "Original: Produto. Combustivel pesquisado. Dominio esperado: GASOLINA, GASOLINA ADITIVADA, ETANOL, DIESEL, DIESEL S10, GNV. Texto bruto.",
        "data_coleta": "Original: Data da Coleta. Data da coleta do preco, formato dd/mm/aaaa. Texto bruto.",
        "valor_venda": "Original: Valor de Venda. Preco ao consumidor com virgula decimal (ex.: 6,29). Texto bruto.",
        "valor_compra": "Original: Valor de Compra. Preco de distribuicao; segundo a ANP, serie disponivel apenas ate ago/2020 (esperado vazio). Texto bruto.",
        "unidade_medida": "Original: Unidade de Medida. Ex.: R$ / litro, R$ / m3 (GNV). Texto bruto.",
        "bandeira": "Original: Bandeira. Distribuidora cuja marca o posto exibe; BRANCA = sem marca. Texto bruto.",
        "_arquivo_origem": "Metadado de controle: nome do arquivo CSV de origem no Volume bronze.arquivos_anp.",
        "_encoding_detectado": "Metadado de controle: codificacao usada na leitura (UTF-8 ou ISO-8859-1).",
        "_fonte": "Metadado de controle: identificacao da fonte dos dados.",
        "_data_ingestao": "Metadado de controle: data e hora (UTC) da ingestao na Bronze.",
    },
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Evidências
# MAGIC - Amostra dos dados brutos e histórico transacional da tabela Delta (*time travel*).

# COMMAND ----------

display(spark.table(TB["bronze"]).limit(10))

# COMMAND ----------

display(spark.sql(f"DESCRIBE HISTORY {TB['bronze']}"))
