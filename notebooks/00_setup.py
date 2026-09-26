# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Configuração do ambiente
# MAGIC
# MAGIC **Objetivo:** preparar a estrutura do Lakehouse no Unity Catalog para o MVP.
# MAGIC
# MAGIC | Item | Decisão | Por quê |
# MAGIC |---|---|---|
# MAGIC | Catálogo | `mvp_combustiveis` (com *fallback* para `workspace`) | Isola o projeto. Se a Free Edition não permitir criar catálogo, usa o catálogo padrão sem quebrar o pipeline |
# MAGIC | Schemas | `bronze`, `silver`, `gold` | Arquitetura Medalhão: cada camada com uma responsabilidade |
# MAGIC | Volume | `bronze.arquivos_anp` | Área de *landing* dos arquivos brutos (CSV/ZIP) dentro do Unity Catalog |
# MAGIC
# MAGIC Este notebook é chamado por `%run` em todos os demais, garantindo que todos usem os mesmos nomes de tabelas.

# COMMAND ----------

CATALOGO_PREFERIDO = "mvp_combustiveis"


def definir_catalogo(preferido: str = CATALOGO_PREFERIDO) -> str:
    """Tenta criar/usar um catálogo dedicado; se a conta não permitir, usa o catálogo padrão 'workspace'."""
    try:
        spark.sql(f"CREATE CATALOG IF NOT EXISTS {preferido} COMMENT 'MVP Engenharia de Dados - precos de combustiveis ANP'")
        spark.sql(f"USE CATALOG {preferido}")
        return preferido
    except Exception as erro:
        print(f"[aviso] Catálogo '{preferido}' indisponível ({str(erro)[:150]}). Usando o catálogo 'workspace'.")
        spark.sql("USE CATALOG workspace")
        return "workspace"


CATALOGO = definir_catalogo()

DESCRICAO_SCHEMAS = {
    "bronze": "Camada Bronze: dados brutos da ANP exatamente como recebidos, acrescidos de metadados de controle.",
    "silver": "Camada Silver: dados limpos, tipados, padronizados e deduplicados; relatorio de qualidade.",
    "gold": "Camada Gold: modelo dimensional (esquema estrela) e agregados prontos para responder as perguntas de negocio.",
}
for schema, descricao in DESCRICAO_SCHEMAS.items():
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOGO}.{schema} COMMENT '{descricao}'")

spark.sql(
    f"CREATE VOLUME IF NOT EXISTS {CATALOGO}.bronze.arquivos_anp "
    "COMMENT 'Landing zone dos arquivos brutos (CSV/ZIP) da Serie Historica de Precos de Combustiveis da ANP.'"
)
VOLUME = f"/Volumes/{CATALOGO}/bronze/arquivos_anp"

TB = {
    "bronze": f"{CATALOGO}.bronze.precos_revenda_raw",
    "qualidade": f"{CATALOGO}.silver.relatorio_qualidade",
    "silver": f"{CATALOGO}.silver.precos_revenda",
    "rejeitados": f"{CATALOGO}.silver.precos_revenda_rejeitados",
    "dim_tempo": f"{CATALOGO}.gold.dim_tempo",
    "dim_localidade": f"{CATALOGO}.gold.dim_localidade",
    "dim_produto": f"{CATALOGO}.gold.dim_produto",
    "dim_posto": f"{CATALOGO}.gold.dim_posto",
    "fato": f"{CATALOGO}.gold.fato_preco_revenda",
    "agg": f"{CATALOGO}.gold.agg_preco_mensal_uf",
}


def _esc(texto: str) -> str:
    return texto.replace("\\", "\\\\").replace("'", "\\'")


def documentar_tabela(tabela: str, descricao: str, colunas: dict) -> None:
    """Grava o catálogo de dados no Unity Catalog (comentário da tabela e de cada coluna)."""
    spark.sql(f"COMMENT ON TABLE {tabela} IS '{_esc(descricao)}'")
    for coluna, texto in colunas.items():
        spark.sql(f"ALTER TABLE {tabela} ALTER COLUMN {coluna} COMMENT '{_esc(texto)}'")
    print(f"Catálogo aplicado em {tabela} ({len(colunas)} colunas documentadas).")


print(f"Catálogo em uso : {CATALOGO}")
print(f"Volume de landing: {VOLUME}")
for k, v in TB.items():
    print(f"  {k:<15} -> {v}")
