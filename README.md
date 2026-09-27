# MVP · Pipeline de dados de preços de combustíveis no Brasil (ANP)

**Autor:** Rodrigo Ferraz · **Plataforma:** Databricks Free Edition (Lakehouse, Delta Lake, Unity Catalog) · **Linguagens:** PySpark e SQL

> Pipeline de ponta a ponta que coleta a série histórica de preços de revenda de combustíveis publicada pela ANP, organiza os dados na **Arquitetura Medalhão** (Bronze → Silver → Gold), modela um **esquema estrela** documentado no Unity Catalog e responde a perguntas de negócio sobre preços, regiões, bandeiras e a competitividade do etanol.

> ⏳ **Marcadores deste documento:** `📸` indica um screenshot a inserir (salvo na pasta `imagens/`) e `⏳` indica um número ou texto a preencher após a execução dos notebooks.

---

## Sumário

1. [Contexto de Negócios e Perguntas (Etapa 2 e 4.1)](#1-contexto-de-negócios-e-perguntas-etapa-2-e-41)
2. [Carga dos Dados (Etapa 4.2)](#2-carga-dos-dados-etapa-42)
3. [Modelagem e Catálogo de Dados (Etapa 4.3)](#3-modelagem-e-catálogo-de-dados-etapa-43)
4. [Pipeline de Dados (Etapa 4.4)](#4-pipeline-de-dados-etapa-44)
5. [Qualidade de Dados (Etapa 4.5)](#5-qualidade-de-dados-etapa-45)
6. [Análise de Dados (Etapa 4.5)](#6-análise-de-dados-etapa-45)
7. [Autoavaliação](#7-autoavaliação)
8. [Como reproduzir](#8-como-reproduzir)

---

## 1. Contexto de Negócios e Perguntas (Etapa 2 e 4.1)

### 1.1 Problema

O preço dos combustíveis afeta diretamente o orçamento das famílias, o custo do frete e a inflação. No Brasil, os preços são livres na revenda e variam bastante entre estados, municípios e postos. O consumidor ainda precisa decidir entre **etanol e gasolina**, e essa escolha depende da relação entre os dois preços.

**Objetivo:** entender como os preços de revenda dos principais combustíveis variam no Brasil **ao longo do tempo, entre estados e entre tipos de posto**, e em que situações o **etanol compensa** em relação à gasolina, usando os dados oficiais da ANP de **janeiro de 2025 a junho de 2026**.

### 1.2 Perguntas de negócio

*Objetivo registrado antes da coleta e mantido intacto até o fim do trabalho, conforme a orientação do enunciado.*

| # | Pergunta | Tipo | Decisão técnica que ela orienta |
|---|---|---|---|
| **P1** | Como evoluiu o preço médio mensal da gasolina comum, do etanol e do diesel S10 no período? | Núcleo | Granularidade temporal mensal; dimensão de tempo |
| **P2** | Quais UFs têm os maiores e os menores preços médios de gasolina comum e diesel S10, e qual a diferença entre elas? | Núcleo | Dimensão de localidade (região, UF, município) |
| **P3** | Em quais UFs o etanol é economicamente vantajoso (razão etanol/gasolina ≤ 0,70) e com que frequência? | Núcleo | Padronização de produto; agregado mensal por UF |
| **P4** | Postos bandeirados cobram mais que postos de bandeira branca? Quanto, por produto? | Núcleo | Dimensão de posto com tipo de bandeira |
| **P5** | Em quais capitais a dispersão de preços da gasolina entre postos é maior? | Fronteira | Grão do fato no nível do posto; indicador de capital |
| **P6** | É possível estimar a margem bruta da revenda (preço de venda − preço de compra)? | Fronteira | Manter o campo `valor_compra` e medir sua completude |

As perguntas de **fronteira** foram incluídas de propósito: elas testam os limites da base e alimentam a autoavaliação.

**Matriz de rastreabilidade (pergunta → dados → modelo):**

| Pergunta | Campos da fonte | Tabelas Gold usadas |
|---|---|---|
| P1 | Data da Coleta, Produto, Valor de Venda | `agg_preco_mensal_uf` |
| P2 | Estado - Sigla, Produto, Valor de Venda | `agg_preco_mensal_uf` |
| P3 | Estado - Sigla, Produto, Valor de Venda, Data da Coleta | `agg_preco_mensal_uf` |
| P4 | Bandeira, Municipio, Produto, Valor de Venda | `fato_preco_revenda`, `dim_posto`, `dim_produto`, `dim_tempo` |
| P5 | Municipio, CNPJ da Revenda, Valor de Venda | `fato_preco_revenda`, `dim_localidade` |
| P6 | Valor de Compra | `silver.precos_revenda` |

### 1.3 Fonte dos dados

| Item | Descrição |
|---|---|
| Conjunto | **Série Histórica de Preços de Combustíveis e de GLP**, ANP (Agência Nacional do Petróleo, Gás Natural e Biocombustíveis) |
| Página oficial | https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/serie-historica-de-precos-de-combustiveis |
| Origem | Levantamento de Preços de Combustíveis (LPC): pesquisa **semanal** realizada por empresa contratada pela ANP, em cumprimento ao art. 8º da Lei nº 9.478/1997 (Lei do Petróleo) |
| Responsável | ANP/SDC (Superintendência de Defesa da Concorrência), contato sdc@anp.gov.br |
| Arquivos usados | Combustíveis automotivos, dados semanais agrupados por semestre: `ca-2025-01.zip`, `ca-2025-02.zip`, `ca-2026-01.zip` |
| Formato | CSV, separador `;`, vírgula decimal, datas em `dd/mm/aaaa` |
| Produtos | Gasolina C comum, gasolina aditivada, etanol hidratado, diesel S500, diesel S10 e GNV |
| Volume | ⏳ registros brutos, ⏳ postos, ⏳ municípios, 27 UFs |

**Estrutura dos dados brutos** (uma tabela; uma linha = o preço de um produto em um posto em uma data de coleta), conforme o dicionário oficial da ANP:

| Coluna original | Descrição (metadados ANP) | Tipo (ANP) |
|---|---|---|
| Regiao - Sigla | Sigla da região da revenda | alfanumérico |
| Estado - Sigla | Sigla da UF da revenda | alfanumérico |
| Municipio | Nome do município da revenda | alfanumérico |
| Revenda | Nome da revenda | alfanumérico |
| CNPJ da Revenda | CNPJ da revenda | numérico |
| Nome da Rua | Logradouro | alfanumérico |
| Numero Rua | Número do logradouro | alfanumérico |
| Complemento | Complemento do endereço | alfanumérico |
| Bairro | Bairro | alfanumérico |
| Cep | CEP do logradouro | alfanumérico |
| Produto | Combustível pesquisado | alfanumérico |
| Data da Coleta | Data da coleta do preço | data |
| Valor de Venda | Preço ao consumidor final na data da coleta | numérico |
| Valor de Compra | Preço de distribuição (**série disponível até agosto de 2020**) | numérico |
| Unidade de Medida | Unidade do preço | alfanumérico |
| Bandeira | Marca da distribuidora exibida pelo posto; "BRANCA" = sem marca | alfanumérico |

### 1.4 Licença e uso dos dados

- Os dados são **dados abertos governamentais**, publicados pela ANP no âmbito da Política de Dados Abertos do Poder Executivo Federal (**Decreto nº 8.777/2016**), com Plano de Dados Abertos próprio, e também listados no Portal Brasileiro de Dados Abertos (dados.gov.br).
- Dados abertos governamentais podem ser **livremente acessados, utilizados, modificados e compartilhados**, inclusive para fins acadêmicos e comerciais, desde que **citada a fonte**. A fonte é citada neste documento e nos metadados das tabelas (`_fonte`).
- **Licença específica:** o portal dados.gov.br não exibe uma licença específica para este conjunto (consulta em 27/09/2026). O uso segue a Política de Dados Abertos do Poder Executivo Federal (Decreto nº 8.777/2016), que prevê a livre utilização dos dados abertos, com citação da fonte.
- **LGPD:** os registros identificam **pessoas jurídicas** (postos revendedores, por CNPJ e razão social), e não pessoas físicas. Mesmo assim, por minimização, os campos de endereço detalhado (rua, número, complemento) foram descartados a partir da Silver.

---

## 2. Carga dos Dados (Etapa 4.2)

**Estratégia:** padrão **ELT**. O dado é carregado bruto no Lakehouse e transformado dentro da própria plataforma.

1. **Aquisição:** os três arquivos semestrais (`.zip`) são obtidos no portal da ANP. O notebook tenta o **download automático**. Se o ambiente não tiver acesso à internet, é feito o **upload manual** dos `.zip` no Volume.
2. **Landing zone:** Volume do Unity Catalog `bronze.arquivos_anp`. O notebook extrai automaticamente os CSVs dos `.zip`.
3. **Leitura:** cada CSV é lido como **texto** (sem inferência de tipos), com detecção automática de codificação (UTF-8 ou ISO-8859-1).
4. **Metadados de controle:** `_arquivo_origem`, `_encoding_detectado`, `_fonte` e `_data_ingestao`.
5. **Gravação:** tabela Delta `bronze.precos_revenda_raw`, em modo `overwrite` (reprodutível e idempotente).

⏳ Método efetivamente usado nesta execução: download automático ou upload manual.

**Script:** [`notebooks/01_ingestao_bronze.py`](notebooks/01_ingestao_bronze.py)

📸 `imagens/01_volume_arquivos.png`: Volume `arquivos_anp` com os arquivos carregados.
📸 `imagens/02_bronze_contagem.png`: contagem de registros por arquivo de origem.

---

## 3. Modelagem e Catálogo de Dados (Etapa 4.3)

### 3.1 Organização no Lakehouse

| Camada | Schema | Tabelas | Papel |
|---|---|---|---|
| Bronze | `bronze` | `precos_revenda_raw` + Volume `arquivos_anp` | Dado como veio, com metadados de controle |
| Silver | `silver` | `precos_revenda`, `precos_revenda_rejeitados`, `relatorio_qualidade` | Dado limpo, tipado, deduplicado; auditoria de rejeitados; qualidade |
| Gold | `gold` | `fato_preco_revenda`, `dim_tempo`, `dim_localidade`, `dim_produto`, `dim_posto`, `agg_preco_mensal_uf` | Esquema estrela e agregado para as perguntas |

Catálogo: `mvp_combustiveis` (o notebook `00_setup` usa o catálogo `workspace` como alternativa caso a conta não permita criar catálogos). ⏳ Catálogo usado: ___

### 3.2 Esquema estrela

```
                    dim_tempo
                        │
 dim_localidade ── fato_preco_revenda ── dim_produto
                        │
                    dim_posto
```

**Por que estrela:** há um evento central e mensurável (um preço coletado), descrito por **quando** (tempo), **onde** (localidade), **o quê** (produto) e **quem** (posto e bandeira). As perguntas viram agregações simples sobre o fato. Não se optou por *snowflake* porque as dimensões são pequenas e a normalização só tornaria as consultas mais complexas.

**Grão do fato:** um preço de um produto em um posto em uma data de coleta. Esse é o nível mais detalhado disponível, e é ele que viabiliza a P5 (dispersão entre postos) e a P4b (comparação dentro do mesmo município).

**Decisões de modelagem:**
- **Chaves substitutas** (`sk_*`), com restrições **PK/FK informativas** no Unity Catalog, o que gera o diagrama entidade-relacionamento no Catalog Explorer.
- **`produto_analise`**: categoria padronizada (por exemplo, "DIESEL S10"), para que as análises não dependam de variações de rótulo da fonte.
- **`dim_posto` como SCD tipo 1**: guarda os atributos mais recentes do posto. Trocas de bandeira no período ficam sinalizadas em `qtd_bandeiras_periodo`. É uma simplificação consciente do MVP.
- **Agregado `agg_preco_mensal_uf`**: métrica pré-calculada (média, mediana, mínimo e máximo por UF, produto e mês), **sem outliers**, que atende P1 a P3.

📸 `imagens/03_diagrama_er.png`: diagrama entidade-relacionamento (Catalog Explorer → `gold.fato_preco_revenda` → aba de relacionamentos).

### 3.3 Catálogo de dados

O catálogo foi **gravado no próprio Unity Catalog** (comentários de tabela e de coluna via `COMMENT ON TABLE` e `ALTER COLUMN ... COMMENT`) e está transcrito abaixo.

📸 `imagens/04_catalogo_fato.png`: aba *Overview* de `gold.fato_preco_revenda` mostrando as descrições das colunas.
📸 `imagens/05_linhagem.png`: aba *Lineage* mostrando o fluxo Bronze → Silver → Gold.

#### `bronze.precos_revenda_raw`
Preços de revenda exatamente como publicados pela ANP (todos os campos em texto). Uma linha = um preço de um produto em um posto em uma data.

| Coluna | Tipo | Descrição e domínio | Linhagem |
|---|---|---|---|
| regiao_sigla | STRING | Sigla da região. Domínio esperado: N, NE, CO, SE, S | CSV: Regiao - Sigla |
| estado_sigla | STRING | Sigla da UF. Domínio: 27 UFs | CSV: Estado - Sigla |
| municipio | STRING | Município do posto | CSV: Municipio |
| revenda | STRING | Razão social do posto | CSV: Revenda |
| cnpj_revenda | STRING | CNPJ com máscara | CSV: CNPJ da Revenda |
| nome_rua | STRING | Logradouro | CSV: Nome da Rua |
| numero_rua | STRING | Número | CSV: Numero Rua |
| complemento | STRING | Complemento (frequentemente vazio) | CSV: Complemento |
| bairro | STRING | Bairro | CSV: Bairro |
| cep | STRING | CEP (8 dígitos, com ou sem hífen) | CSV: Cep |
| produto | STRING | Combustível. Domínio: GASOLINA, GASOLINA ADITIVADA, ETANOL, DIESEL, DIESEL S10, GNV | CSV: Produto |
| data_coleta | STRING | Data `dd/mm/aaaa` | CSV: Data da Coleta |
| valor_venda | STRING | Preço com vírgula decimal (ex.: 6,29) | CSV: Valor de Venda |
| valor_compra | STRING | Preço de distribuição; esperado vazio (série encerrada em ago/2020) | CSV: Valor de Compra |
| unidade_medida | STRING | R$ / litro; R$ / m³ (GNV) | CSV: Unidade de Medida |
| bandeira | STRING | Distribuidora; BRANCA = sem bandeira | CSV: Bandeira |
| _arquivo_origem | STRING | Arquivo CSV de origem | Metadado de ingestão |
| _encoding_detectado | STRING | UTF-8 ou ISO-8859-1 | Metadado de ingestão |
| _fonte | STRING | Identificação da fonte | Metadado de ingestão |
| _data_ingestao | TIMESTAMP | Data e hora da carga | Metadado de ingestão |

#### `silver.precos_revenda`
Dados limpos, tipados, padronizados e deduplicados. Grão: posto (CNPJ) × produto × data de coleta.

| Coluna | Tipo | Descrição e domínio | Linhagem |
|---|---|---|---|
| regiao_sigla | STRING | N, NE, CO, SE, S | bronze.regiao_sigla (trim, maiúsculas) |
| uf_sigla | STRING | 27 UFs | bronze.estado_sigla (trim, maiúsculas) |
| municipio | STRING | Maiúsculas, sem acentos | bronze.municipio (T1) |
| revenda | STRING | Razão social padronizada | bronze.revenda (T1) |
| cnpj_revenda | STRING | 14 dígitos | bronze.cnpj_revenda (T2) |
| bairro | STRING | Padronizado | bronze.bairro (T1) |
| cep | STRING | 8 dígitos | bronze.cep (T2) |
| produto | STRING | GASOLINA, GASOLINA ADITIVADA, ETANOL, DIESEL, DIESEL S10, GNV | bronze.produto (T1) |
| unidade_medida | STRING | R$ / litro; R$ / m³ | bronze.unidade_medida |
| bandeira | STRING | Distribuidora; BRANCA = sem bandeira | bronze.bandeira (T1) |
| data_coleta | DATE | 2025-01-01 a 2026-06-30 | bronze.data_coleta convertida (T3) |
| valor_venda | DECIMAL(10,3) | R$ 0,50 a R$ 20,00 | bronze.valor_venda (T4, T5) |
| valor_compra | DECIMAL(10,3) | Esperado nulo | bronze.valor_compra (T4) |
| fl_outlier_iqr | BOOLEAN | Preço fora de Q1−1,5·IQR e Q3+1,5·IQR (produto × mês) | Calculado (T7) |
| _arquivo_origem | STRING | Arquivo de origem | bronze._arquivo_origem |
| _data_ingestao | TIMESTAMP | Data e hora da carga | bronze._data_ingestao |

#### `silver.precos_revenda_rejeitados`
Registros que violaram as regras de validade (T5), mantidos para auditoria. Mesmas colunas da Silver (exceto `fl_outlier_iqr`), mais:

| Coluna | Tipo | Descrição e domínio |
|---|---|---|
| motivo_rejeicao | STRING | Regras violadas: data_coleta_invalida, preco_ausente_ou_invalido, preco_fora_da_faixa, cnpj_invalido, uf_invalida, produto_ausente |

#### `silver.relatorio_qualidade`
Resultados das verificações de qualidade medidas sobre a Bronze.

| Coluna | Tipo | Descrição e domínio |
|---|---|---|
| dimensao | STRING | Completude, Consistencia, Unicidade, Acuracia, Outliers |
| atributo | STRING | Coluna (ou combinação) avaliada |
| verificacao | STRING | Verificação realizada |
| valor | DOUBLE | Resultado (percentual ou contagem) |
| observacao | STRING | Detalhe do resultado |
| data_execucao | TIMESTAMP | Momento da verificação |

#### `gold.fato_preco_revenda`
Fato de preços. Grão: um preço de um produto em um posto em uma data de coleta.

| Coluna | Tipo | Descrição e domínio | Linhagem |
|---|---|---|---|
| sk_tempo | INT | FK → dim_tempo | Junção por data_coleta |
| sk_localidade | INT | FK → dim_localidade | Junção por uf_sigla + municipio |
| sk_produto | INT | FK → dim_produto | Junção por produto |
| sk_posto | INT | FK → dim_posto | Junção por cnpj_revenda |
| valor_venda | DECIMAL(10,3) | Preço em R$/litro (R$/m³ para GNV). 0,50 a 20,00 | silver.valor_venda |
| fl_outlier_iqr | BOOLEAN | Preço atípico (IQR) | silver.fl_outlier_iqr |
| arquivo_origem | STRING | Rastreabilidade até o arquivo | bronze._arquivo_origem |

#### `gold.dim_tempo`
| Coluna | Tipo | Descrição e domínio |
|---|---|---|
| sk_tempo | INT | PK, formato aaaammdd |
| data | DATE | 2025-01-01 a 2026-06-30 |
| ano | INT | 2025 a 2026 |
| mes | INT | 1 a 12 |
| nome_mes | STRING | Janeiro a Dezembro |
| ano_mes | STRING | aaaa-mm |
| trimestre | INT | 1 a 4 |
| semestre | INT | 1 ou 2 |
| semana_ano | INT | 1 a 53 |

#### `gold.dim_localidade`
| Coluna | Tipo | Descrição e domínio | Linhagem |
|---|---|---|---|
| sk_localidade | INT | PK | Gerada |
| regiao_sigla | STRING | N, NE, CO, SE, S | silver |
| regiao_nome | STRING | Norte, Nordeste, Centro-Oeste, Sudeste, Sul | Tabela de referência |
| uf_sigla | STRING | 27 UFs | silver |
| uf_nome | STRING | Nome da UF | Tabela de referência |
| municipio | STRING | Maiúsculas, sem acentos | silver |
| fl_capital | BOOLEAN | Município é capital da UF | Lista de capitais |

#### `gold.dim_produto`
| Coluna | Tipo | Descrição e domínio | Linhagem |
|---|---|---|---|
| sk_produto | INT | PK | Gerada |
| produto | STRING | Rótulo da ANP padronizado | silver.produto |
| produto_analise | STRING | GASOLINA COMUM, GASOLINA ADITIVADA, ETANOL, DIESEL S10, DIESEL S500, GNV, OUTROS | Regra sobre produto |
| grupo_produto | STRING | Gasolina, Etanol, Diesel, GNV, Outros | Regra sobre produto_analise |
| unidade_medida | STRING | R$ / litro; R$ / m³ | silver.unidade_medida |

#### `gold.dim_posto`
| Coluna | Tipo | Descrição e domínio | Linhagem |
|---|---|---|---|
| sk_posto | INT | PK | Gerada |
| cnpj_revenda | STRING | 14 dígitos (chave natural) | silver |
| revenda | STRING | Razão social mais recente | silver |
| bandeira | STRING | Bandeira mais recente | silver |
| tipo_bandeira | STRING | BANDEIRADO, BANDEIRA BRANCA | Derivado de bandeira |
| qtd_bandeiras_periodo | BIGINT | ≥ 1; > 1 indica troca de bandeira | Contagem na silver |
| bairro | STRING | Bairro mais recente | silver |
| cep | STRING | 8 dígitos | silver |

#### `gold.agg_preco_mensal_uf`
Agregado sem outliers. Grão: mês × UF × produto_analise.

| Coluna | Tipo | Descrição e domínio |
|---|---|---|
| ano_mes | STRING | aaaa-mm |
| regiao_sigla | STRING | N, NE, CO, SE, S |
| uf_sigla | STRING | 27 UFs |
| produto_analise | STRING | Categoria padronizada |
| grupo_produto | STRING | Família do produto |
| qtd_coletas | BIGINT | ≥ 1 |
| qtd_postos | BIGINT | ≥ 1 |
| preco_medio | DECIMAL(10,3) | Média do preço de venda |
| preco_mediano | DECIMAL(10,3) | Mediana aproximada |
| preco_min | DECIMAL(10,3) | Menor preço |
| preco_max | DECIMAL(10,3) | Maior preço |

---

## 4. Pipeline de Dados (Etapa 4.4)

O pipeline foi **ramificado em um notebook por etapa**. Cada notebook lê de uma camada e grava na seguinte, o que torna o fluxo legível e permite reexecutar só a etapa necessária. Todos chamam `00_setup` via `%run`, para garantir nomes de tabelas consistentes.

| Ordem | Notebook | Lê de | Grava em | Função |
|---|---|---|---|---|
| 0 | [`00_setup.py`](notebooks/00_setup.py) | — | catálogo, schemas, Volume | Estrutura do Lakehouse e função de catalogação |
| 1 | [`01_ingestao_bronze.py`](notebooks/01_ingestao_bronze.py) | Volume `arquivos_anp` | `bronze.precos_revenda_raw` | Extração e carga do dado bruto |
| 2 | [`02_qualidade_dados.py`](notebooks/02_qualidade_dados.py) | Bronze | `silver.relatorio_qualidade` | Diagnóstico de qualidade do dado capturado |
| 3 | [`03_transformacao_silver.py`](notebooks/03_transformacao_silver.py) | Bronze | `silver.precos_revenda`, `silver.precos_revenda_rejeitados` | Limpeza e padronização (T1 a T8) |
| 4 | [`04_modelagem_gold.py`](notebooks/04_modelagem_gold.py) | Silver | 4 dimensões, fato e agregado | Modelo dimensional e catálogo |
| 5 | [`05_analise.py`](notebooks/05_analise.py) | Gold | — | Respostas às perguntas |

**Transformações e seus motivos** (detalhadas no notebook 03):

| # | O que foi feito | Por quê | Impacto |
|---|---|---|---|
| T1 | `trim`, maiúsculas e remoção de acentos nos textos | Unificar categorias ("São Paulo" = "SAO PAULO") | Agrupamentos corretos |
| T2 | CNPJ e CEP só com dígitos | Padronizar a chave do posto | CNPJ com 14 dígitos |
| T3 | Texto `dd/mm/aaaa` → DATE | Permitir análises temporais | Datas inválidas viram nulo e são rejeitadas |
| T4 | Vírgula → ponto e conversão para DECIMAL(10,3) | O CSV usa vírgula decimal | Preços somáveis, sem erro de ponto flutuante |
| T5 | Regras de validade e tabela de rejeitados | Registros sem data, preço ou posto não respondem às perguntas | ⏳ registros rejeitados, com motivo |
| T6 | Deduplicação por posto + produto + data | Um preço por chave de negócio | ⏳ duplicatas removidas |
| T7 | Sinalização de outliers (IQR por produto e mês) | Evitar distorção sem perder rastreabilidade | ⏳ registros sinalizados |
| T8 | Descarte de rua, número e complemento | Minimização de dados | Tabela mais enxuta |
| G1 | JOIN da Silver com as 4 dimensões pelas chaves naturais | Substituir chaves naturais por chaves substitutas no fato | Fato com ⏳ registros (igual à Silver) |
| G2 | Agregação mensal por UF e produto, sem outliers | Pré-calcular as métricas de P1 a P3 | Consultas simples e rápidas |

**Funil de registros** (saída do notebook 03):

| Etapa | Registros |
|---|---|
| Bronze | ⏳ |
| Rejeitados (T5) | ⏳ |
| Removidos na deduplicação (T6) | ⏳ |
| Silver | ⏳ |
| Sinalizados como outlier (T7, mantidos) | ⏳ |

📸 `imagens/06_tabelas_persistidas.png`: Catalog Explorer com os schemas `bronze`, `silver` e `gold` e suas tabelas.
📸 `imagens/07_funil.png`: resultado do funil Bronze → Silver.
📸 `imagens/08_historico_delta.png`: `DESCRIBE HISTORY` (versões da tabela Delta).

---

## 5. Qualidade de Dados (Etapa 4.5)

A verificação foi feita **sobre a Bronze** (dado como capturado), em todos os atributos, nas cinco dimensões pedidas. Os resultados estão persistidos em `silver.relatorio_qualidade` (notebook 02).

### 5.1 Resultados por atributo

⏳ Transcrever a tabela de perfil do notebook 02 (completude, distintos e consistência por atributo).

📸 `imagens/09_perfil_qualidade.png`

### 5.2 Problemas detectados e tratamento

| # | Problema | Dimensão | Evidência | Tratamento | Onde |
|---|---|---|---|---|---|
| Q1 | Preços armazenados como texto com vírgula decimal | Consistência | Todos os campos em texto na fonte | Conversão para DECIMAL(10,3) | T4 |
| Q2 | Datas como texto `dd/mm/aaaa` | Consistência | ⏳ % no padrão | Conversão para DATE; inválidas rejeitadas | T3, T5 |
| Q3 | `valor_compra` vazio | Completude | ⏳ % preenchido | Mantido e documentado; inviabiliza a P6 | P6 |
| Q4 | `complemento` com muitos vazios | Completude | ⏳ % preenchido | Coluna descartada (sem uso analítico) | T8 |
| Q5 | Máscara no CNPJ e no CEP | Consistência | ⏳ % no padrão | Manter só dígitos | T2 |
| Q6 | Possível variação de codificação e acentuação entre arquivos | Consistência | ⏳ encodings detectados | Detecção de encoding e remoção de acentos | Notebook 01, T1 |
| Q7 | Chaves posto + produto + data repetidas | Unicidade | ⏳ chaves repetidas / ⏳ conflitantes | Deduplicação | T6 |
| Q8 | Preços fora da faixa plausível | Acurácia | ⏳ registros | Rejeição com motivo | T5 |
| Q9 | Preços extremos dentro da faixa | Outliers | ⏳ % por produto (IQR) | Sinalização e exclusão nas análises | T7 |

⏳ Ajustar esta tabela aos números reais. Se algum problema **não** ocorrer, manter a linha e registrar "verificado, sem ocorrências", o que também é evidência exigida pelo enunciado.

---

## 6. Análise de Dados (Etapa 4.5)

Todas as consultas estão em [`notebooks/05_analise.py`](notebooks/05_analise.py), em SQL sobre a camada Gold, excluindo outliers.

### P1 · Evolução mensal do preço médio
📸 `imagens/10_p1_evolucao.png` (gráfico de linha)
**Resultado:** ⏳
**Discussão:** ⏳

### P2 · UFs mais caras e mais baratas
📸 `imagens/11_p2_ufs.png`
**Resultado:** ⏳
**Discussão:** ⏳

### P3 · Quando o etanol compensa
📸 `imagens/12_p3_etanol.png`
**Resultado:** ⏳
**Discussão:** ⏳

### P4 · Bandeirado × bandeira branca
📸 `imagens/13_p4_bandeira.png`
**Resultado:** ⏳
**Discussão:** ⏳

### P5 · Dispersão de preços nas capitais
📸 `imagens/14_p5_dispersao.png`
**Resultado:** ⏳
**Discussão:** ⏳

### P6 · Margem bruta da revenda
📸 `imagens/15_p6_margem.png`
**Resultado:** ⏳
**Discussão:** ⏳

### Discussão geral
⏳

---

## 7. Autoavaliação

⏳ A ser redigida após a análise, cobrindo:
- **Objetivos atingidos:** quais perguntas foram respondidas integralmente, parcialmente ou não respondidas, e por quê.
- **Dificuldades:** ⏳
- **Limitações conhecidas:** a análise é descritiva, e não causal; a pesquisa da ANP é amostral (não cobre todos os postos); `dim_posto` simplifica trocas de bandeira (SCD tipo 1); médias simples não ponderam pelo volume vendido, que não existe na base.
- **Trabalhos futuros:** incorporar a série de volumes de venda da ANP para ponderar preços; cruzar com a cotação do Brent e do câmbio para relacionar preço de revenda e preço internacional; incluir os demais anos da série; orquestrar o pipeline com *Jobs* do Databricks para carga mensal automática; implementar SCD tipo 2 em `dim_posto`.

---

## 8. Como reproduzir

1. Criar uma conta no **Databricks Free Edition**.
2. **Workspace → Create → Git folder**, informando a URL deste repositório.
3. Executar `notebooks/01_ingestao_bronze.py`. Se o download automático falhar, baixar `ca-2025-01.zip`, `ca-2025-02.zip` e `ca-2026-01.zip` na [página da ANP](https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/serie-historica-de-precos-de-combustiveis), fazer o upload no Volume `bronze.arquivos_anp` e reexecutar o notebook.
4. Executar, em ordem, os notebooks 02, 03, 04 e 05.

Os dados não estão incluídos no repositório, conforme permitido pelo enunciado.
