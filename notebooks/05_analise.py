# Databricks notebook source
# MAGIC %md
# MAGIC # 05 · Análise: respondendo às perguntas de negócio
# MAGIC
# MAGIC **Problema:** entender como os preços de revenda dos principais combustíveis variam no Brasil **ao longo do tempo, entre estados e entre tipos de posto**, e em que situações o **etanol compensa** em relação à gasolina, com dados da ANP de jan/2025 a jun/2026.
# MAGIC
# MAGIC | # | Pergunta | Tipo |
# MAGIC |---|---|---|
# MAGIC | P1 | Como evoluiu o preço médio mensal da gasolina comum, do etanol e do diesel S10 no período? | Núcleo |
# MAGIC | P2 | Quais UFs têm os maiores e os menores preços médios de gasolina comum e diesel S10, e qual a diferença entre elas? | Núcleo |
# MAGIC | P3 | Em quais UFs o etanol é economicamente vantajoso (razão etanol/gasolina ≤ 0,70) e com que frequência? | Núcleo |
# MAGIC | P4 | Postos bandeirados cobram mais que postos de bandeira branca? Quanto, por produto? | Núcleo |
# MAGIC | P5 | Em quais capitais a dispersão de preços da gasolina entre postos é maior? | Fronteira |
# MAGIC | P6 | É possível estimar a margem bruta da revenda (venda − compra)? | Fronteira |
# MAGIC | P7 | Quais UFs sofreram a maior alta no choque de mar-abr/2026? | Adicionada na análise (a partir da P1) |
# MAGIC
# MAGIC As consultas usam a camada **Gold**. Registros sinalizados como outlier (`fl_outlier_iqr`) são excluídos. Nos gráficos, use o botão **+ → Visualization** sob cada resultado.

# COMMAND ----------

# MAGIC %run ./00_setup

# COMMAND ----------

# MAGIC %md
# MAGIC ### Conferência: categorias de produto disponíveis

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT p.produto_analise, p.produto, p.unidade_medida, COUNT(*) AS qtd_coletas
# MAGIC FROM gold.fato_preco_revenda f
# MAGIC JOIN gold.dim_produto p USING (sk_produto)
# MAGIC GROUP BY ALL
# MAGIC ORDER BY qtd_coletas DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ## P1 · Evolução mensal do preço médio (Brasil)
# MAGIC Média nacional ponderada pela quantidade de coletas de cada UF. Sugestão de gráfico: **linha**, eixo X `ano_mes`, série `produto_analise`.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT ano_mes,
# MAGIC        produto_analise,
# MAGIC        ROUND(SUM(preco_medio * qtd_coletas) / SUM(qtd_coletas), 3) AS preco_medio_brasil,
# MAGIC        SUM(qtd_coletas) AS qtd_coletas
# MAGIC FROM gold.agg_preco_mensal_uf
# MAGIC WHERE produto_analise IN ('GASOLINA COMUM', 'ETANOL', 'DIESEL S10')
# MAGIC GROUP BY ano_mes, produto_analise
# MAGIC ORDER BY ano_mes, produto_analise

# COMMAND ----------

# MAGIC %sql
# MAGIC -- P1b: variação entre o primeiro e o último mês, e amplitude no período
# MAGIC WITH mensal AS (
# MAGIC   SELECT ano_mes, produto_analise,
# MAGIC          SUM(preco_medio * qtd_coletas) / SUM(qtd_coletas) AS preco
# MAGIC   FROM gold.agg_preco_mensal_uf
# MAGIC   WHERE produto_analise IN ('GASOLINA COMUM', 'ETANOL', 'DIESEL S10')
# MAGIC   GROUP BY ano_mes, produto_analise
# MAGIC )
# MAGIC SELECT produto_analise,
# MAGIC        MIN(ano_mes) AS mes_inicial,
# MAGIC        MAX(ano_mes) AS mes_final,
# MAGIC        ROUND(MIN_BY(preco, ano_mes), 3) AS preco_inicial,
# MAGIC        ROUND(MAX_BY(preco, ano_mes), 3) AS preco_final,
# MAGIC        ROUND((MAX_BY(preco, ano_mes) / MIN_BY(preco, ano_mes) - 1) * 100, 2) AS variacao_pct,
# MAGIC        ROUND(MIN(preco), 3) AS menor_media_mensal,
# MAGIC        ROUND(MAX(preco), 3) AS maior_media_mensal
# MAGIC FROM mensal
# MAGIC GROUP BY produto_analise

# COMMAND ----------

# MAGIC %md
# MAGIC ## P2 · Preço médio por UF (gasolina comum e diesel S10)
# MAGIC Sugestão de gráfico: **barras**, eixo X `uf_sigla`, valor `preco_medio`, agrupado por `produto_analise`.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT produto_analise, regiao_sigla, uf_sigla,
# MAGIC        ROUND(SUM(preco_medio * qtd_coletas) / SUM(qtd_coletas), 3) AS preco_medio,
# MAGIC        SUM(qtd_coletas) AS qtd_coletas
# MAGIC FROM gold.agg_preco_mensal_uf
# MAGIC WHERE produto_analise IN ('GASOLINA COMUM', 'DIESEL S10')
# MAGIC GROUP BY produto_analise, regiao_sigla, uf_sigla
# MAGIC ORDER BY produto_analise, preco_medio DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- P2b: UF mais cara, UF mais barata e diferença entre elas
# MAGIC WITH por_uf AS (
# MAGIC   SELECT produto_analise, uf_sigla,
# MAGIC          SUM(preco_medio * qtd_coletas) / SUM(qtd_coletas) AS preco
# MAGIC   FROM gold.agg_preco_mensal_uf
# MAGIC   WHERE produto_analise IN ('GASOLINA COMUM', 'DIESEL S10')
# MAGIC   GROUP BY produto_analise, uf_sigla
# MAGIC )
# MAGIC SELECT produto_analise,
# MAGIC        MAX_BY(uf_sigla, preco) AS uf_mais_cara,
# MAGIC        ROUND(MAX(preco), 3) AS preco_mais_caro,
# MAGIC        MIN_BY(uf_sigla, preco) AS uf_mais_barata,
# MAGIC        ROUND(MIN(preco), 3) AS preco_mais_barato,
# MAGIC        ROUND(MAX(preco) - MIN(preco), 3) AS diferenca_rs,
# MAGIC        ROUND((MAX(preco) / MIN(preco) - 1) * 100, 1) AS diferenca_pct
# MAGIC FROM por_uf
# MAGIC GROUP BY produto_analise

# COMMAND ----------

# MAGIC %md
# MAGIC ## P3 · Quando o etanol compensa?
# MAGIC Regra de mercado: o etanol é vantajoso quando custa **até 70%** do preço da gasolina, por causa do seu menor poder calorífico. Sugestão de gráfico: **barras** com `razao_media` por UF.

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH mensal_uf AS (
# MAGIC   SELECT ano_mes, uf_sigla,
# MAGIC          SUM(CASE WHEN produto_analise = 'ETANOL' THEN preco_medio * qtd_coletas END)
# MAGIC            / SUM(CASE WHEN produto_analise = 'ETANOL' THEN qtd_coletas END) AS etanol,
# MAGIC          SUM(CASE WHEN produto_analise = 'GASOLINA COMUM' THEN preco_medio * qtd_coletas END)
# MAGIC            / SUM(CASE WHEN produto_analise = 'GASOLINA COMUM' THEN qtd_coletas END) AS gasolina
# MAGIC   FROM gold.agg_preco_mensal_uf
# MAGIC   GROUP BY ano_mes, uf_sigla
# MAGIC )
# MAGIC SELECT uf_sigla,
# MAGIC        ROUND(AVG(etanol / gasolina), 3) AS razao_media,
# MAGIC        SUM(CASE WHEN etanol / gasolina <= 0.70 THEN 1 ELSE 0 END) AS meses_etanol_vantajoso,
# MAGIC        COUNT(*) AS meses_avaliados,
# MAGIC        ROUND(100 * SUM(CASE WHEN etanol / gasolina <= 0.70 THEN 1 ELSE 0 END) / COUNT(*), 1) AS pct_meses_vantajoso
# MAGIC FROM mensal_uf
# MAGIC WHERE etanol IS NOT NULL AND gasolina IS NOT NULL
# MAGIC GROUP BY uf_sigla
# MAGIC ORDER BY razao_media

# COMMAND ----------

# MAGIC %sql
# MAGIC -- P3b: quantas UFs tinham etanol vantajoso em cada mês
# MAGIC WITH mensal_uf AS (
# MAGIC   SELECT ano_mes, uf_sigla,
# MAGIC          SUM(CASE WHEN produto_analise = 'ETANOL' THEN preco_medio * qtd_coletas END)
# MAGIC            / SUM(CASE WHEN produto_analise = 'ETANOL' THEN qtd_coletas END) AS etanol,
# MAGIC          SUM(CASE WHEN produto_analise = 'GASOLINA COMUM' THEN preco_medio * qtd_coletas END)
# MAGIC            / SUM(CASE WHEN produto_analise = 'GASOLINA COMUM' THEN qtd_coletas END) AS gasolina
# MAGIC   FROM gold.agg_preco_mensal_uf
# MAGIC   GROUP BY ano_mes, uf_sigla
# MAGIC )
# MAGIC SELECT ano_mes,
# MAGIC        SUM(CASE WHEN etanol / gasolina <= 0.70 THEN 1 ELSE 0 END) AS ufs_etanol_vantajoso,
# MAGIC        COUNT(*) AS ufs_avaliadas,
# MAGIC        ROUND(AVG(etanol / gasolina), 3) AS razao_media_entre_ufs
# MAGIC FROM mensal_uf
# MAGIC WHERE etanol IS NOT NULL AND gasolina IS NOT NULL
# MAGIC GROUP BY ano_mes
# MAGIC ORDER BY ano_mes

# COMMAND ----------

# MAGIC %md
# MAGIC ## P4 · Bandeirado × bandeira branca
# MAGIC **P4a** compara as médias gerais. Como a composição geográfica dos dois grupos é diferente, **P4b** faz uma comparação mais justa: calcula a diferença **dentro do mesmo município e do mesmo mês** e depois tira a média dessas diferenças.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- P4a: comparação simples
# MAGIC SELECT p.produto_analise, po.tipo_bandeira,
# MAGIC        ROUND(AVG(f.valor_venda), 3) AS preco_medio,
# MAGIC        COUNT(*) AS qtd_coletas,
# MAGIC        COUNT(DISTINCT f.sk_posto) AS qtd_postos
# MAGIC FROM gold.fato_preco_revenda f
# MAGIC JOIN gold.dim_produto p USING (sk_produto)
# MAGIC JOIN gold.dim_posto po USING (sk_posto)
# MAGIC WHERE NOT f.fl_outlier_iqr
# MAGIC   AND p.produto_analise IN ('GASOLINA COMUM', 'ETANOL', 'DIESEL S10')
# MAGIC GROUP BY p.produto_analise, po.tipo_bandeira
# MAGIC ORDER BY p.produto_analise, po.tipo_bandeira

# COMMAND ----------

# MAGIC %sql
# MAGIC -- P4b: diferença controlada por município e mês
# MAGIC WITH grupos AS (
# MAGIC   SELECT p.produto_analise, f.sk_localidade, t.ano_mes,
# MAGIC          AVG(CASE WHEN po.tipo_bandeira = 'BANDEIRADO' THEN f.valor_venda END) AS bandeirado,
# MAGIC          AVG(CASE WHEN po.tipo_bandeira = 'BANDEIRA BRANCA' THEN f.valor_venda END) AS branca
# MAGIC   FROM gold.fato_preco_revenda f
# MAGIC   JOIN gold.dim_produto p USING (sk_produto)
# MAGIC   JOIN gold.dim_posto po USING (sk_posto)
# MAGIC   JOIN gold.dim_tempo t USING (sk_tempo)
# MAGIC   WHERE NOT f.fl_outlier_iqr
# MAGIC     AND p.produto_analise IN ('GASOLINA COMUM', 'ETANOL', 'DIESEL S10')
# MAGIC   GROUP BY p.produto_analise, f.sk_localidade, t.ano_mes
# MAGIC )
# MAGIC SELECT produto_analise,
# MAGIC        COUNT(*) AS pares_municipio_mes,
# MAGIC        ROUND(AVG(bandeirado - branca), 3) AS diferenca_media_rs,
# MAGIC        ROUND(AVG((bandeirado / branca - 1) * 100), 2) AS diferenca_media_pct,
# MAGIC        ROUND(100 * AVG(CASE WHEN bandeirado > branca THEN 1 ELSE 0 END), 1) AS pct_casos_bandeirado_mais_caro
# MAGIC FROM grupos
# MAGIC WHERE bandeirado IS NOT NULL AND branca IS NOT NULL
# MAGIC GROUP BY produto_analise
# MAGIC ORDER BY produto_analise

# COMMAND ----------

# MAGIC %sql
# MAGIC -- P4c: preço médio da gasolina comum pelas 8 bandeiras com mais postos
# MAGIC WITH top AS (
# MAGIC   SELECT bandeira FROM gold.dim_posto GROUP BY bandeira ORDER BY COUNT(*) DESC LIMIT 8
# MAGIC )
# MAGIC SELECT po.bandeira,
# MAGIC        ROUND(AVG(f.valor_venda), 3) AS preco_medio_gasolina,
# MAGIC        COUNT(DISTINCT f.sk_posto) AS qtd_postos
# MAGIC FROM gold.fato_preco_revenda f
# MAGIC JOIN gold.dim_produto p USING (sk_produto)
# MAGIC JOIN gold.dim_posto po USING (sk_posto)
# MAGIC WHERE NOT f.fl_outlier_iqr AND p.produto_analise = 'GASOLINA COMUM'
# MAGIC   AND po.bandeira IN (SELECT bandeira FROM top)
# MAGIC GROUP BY po.bandeira
# MAGIC ORDER BY preco_medio_gasolina DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ## P5 · Dispersão de preços da gasolina comum nas capitais (pergunta de fronteira)
# MAGIC Para cada capital e mês, calcula-se a diferença entre o posto mais caro e o mais barato e o **coeficiente de variação** (desvio-padrão ÷ média). Em seguida, tira-se a média dos meses. Quanto maior a dispersão, maior o ganho potencial de o consumidor pesquisar preços.

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH mensal AS (
# MAGIC   SELECT l.uf_sigla, l.municipio, t.ano_mes,
# MAGIC          MAX(f.valor_venda) - MIN(f.valor_venda) AS amplitude,
# MAGIC          STDDEV(f.valor_venda) / AVG(f.valor_venda) AS coef_variacao,
# MAGIC          COUNT(DISTINCT f.sk_posto) AS postos
# MAGIC   FROM gold.fato_preco_revenda f
# MAGIC   JOIN gold.dim_localidade l USING (sk_localidade)
# MAGIC   JOIN gold.dim_produto p USING (sk_produto)
# MAGIC   JOIN gold.dim_tempo t USING (sk_tempo)
# MAGIC   WHERE l.fl_capital AND NOT f.fl_outlier_iqr AND p.produto_analise = 'GASOLINA COMUM'
# MAGIC   GROUP BY l.uf_sigla, l.municipio, t.ano_mes
# MAGIC )
# MAGIC SELECT uf_sigla, municipio,
# MAGIC        ROUND(AVG(amplitude), 3) AS amplitude_media_rs,
# MAGIC        ROUND(AVG(coef_variacao) * 100, 2) AS coef_variacao_medio_pct,
# MAGIC        ROUND(AVG(postos), 0) AS postos_pesquisados_por_mes
# MAGIC FROM mensal
# MAGIC GROUP BY uf_sigla, municipio
# MAGIC ORDER BY coef_variacao_medio_pct DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ## P6 · Margem bruta da revenda (pergunta de fronteira)
# MAGIC A margem exigiria o preço de compra (distribuição). A consulta abaixo verifica se esse dado existe no período.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS registros,
# MAGIC        COUNT(valor_compra) AS registros_com_valor_compra,
# MAGIC        ROUND(100 * COUNT(valor_compra) / COUNT(*), 2) AS pct_preenchido
# MAGIC FROM silver.precos_revenda

# COMMAND ----------

# MAGIC %md
# MAGIC ## P7 · Alta de preços por UF no choque de mar-abr/2026 (pergunta adicionada na análise)
# MAGIC Surgiu do achado da P1. Compara o preço médio de fev/2026 (antes do choque) com abr/2026 (pico). Usa o nome completo da tabela (catálogo.schema.tabela) para funcionar mesmo após o reinício da sessão.

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH m AS (
# MAGIC   SELECT produto_analise, regiao_sigla, uf_sigla, ano_mes,
# MAGIC          SUM(preco_medio * qtd_coletas) / SUM(qtd_coletas) AS p
# MAGIC   FROM mvp_combustiveis.gold.agg_preco_mensal_uf
# MAGIC   WHERE produto_analise IN ('DIESEL S10', 'GASOLINA COMUM')
# MAGIC     AND ano_mes IN ('2026-02', '2026-04')
# MAGIC   GROUP BY ALL
# MAGIC )
# MAGIC SELECT produto_analise, regiao_sigla, uf_sigla,
# MAGIC        ROUND(MAX(CASE WHEN ano_mes = '2026-02' THEN p END), 3) AS preco_fev26,
# MAGIC        ROUND(MAX(CASE WHEN ano_mes = '2026-04' THEN p END), 3) AS preco_abr26,
# MAGIC        ROUND((MAX(CASE WHEN ano_mes = '2026-04' THEN p END)
# MAGIC             / MAX(CASE WHEN ano_mes = '2026-02' THEN p END) - 1) * 100, 1) AS alta_pct
# MAGIC FROM m
# MAGIC GROUP BY produto_analise, regiao_sigla, uf_sigla
# MAGIC ORDER BY produto_analise, alta_pct DESC
