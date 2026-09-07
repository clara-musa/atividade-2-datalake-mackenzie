Atividade 2 — Pipeline de Data Lake com Amazon S3 e Amazon Athena
MBA Mackenzie — Data Lakes, Lakehouses e Data Meshes
Professor: Yuri Menezes
Objetivo
Construir um pipeline completo de ingestão, validação de qualidade (Data Quality), segregação de anomalias em quarentena, transformação em camadas analíticas (Arquitetura Medallion — Raw, Silver e Gold) e auditoria de metadados/consistência, utilizando Amazon S3 e Amazon Athena.
Ambiente utilizado
AWS Academy Learner Lab (S3 e Athena)
Google Colab (execução dos scripts Python, via `boto3`)
Bibliotecas: `boto3`, `pandas`, `pyarrow`
Arquitetura do pipeline
```
Geração de dados (com anomalias propositais)
        ↓
   Raw (S3, CSV, particionado por ingest_date)
        ↓
   Data Quality (validação de regras de negócio)
        ↓
  ┌─────┴─────┐
  ↓           ↓
Quarentena   Silver (JOIN + valor_total, Parquet)
(JSON)          ↓
              Gold (agregações analíticas, Parquet)
        ↓
   Amazon Athena (tabelas externas + auditoria)
```
Estrutura de dados
Tema dos dados simulados: papelaria e artigos criativos.
Tabela	Registros	Observação
clientes	300	`cliente_id` 1–300, com `cidade` e `uf`
produtos	30	`product_id` 1–30, com `categoria` e `preco`
pedidos	1500	~15% de anomalias propositais
Anomalias geradas nos pedidos
Tipo de anomalia	Quantidade encontrada
Quantidade inválida (≤ 0)	80
`cliente_id` inexistente	73
`product_id` inexistente	75
Total de anomalias	228
Pedidos válidos	1272
Estrutura final no S3
```
s3://<bucket>/
├── raw/
│   ├── clientes/ingest_date=YYYY-MM-DD/clientes.csv
│   ├── produtos/ingest_date=YYYY-MM-DD/produtos.csv
│   └── pedidos/ingest_date=YYYY-MM-DD/pedidos.csv
├── quarantine/
│   └── pedidos_rejeitados/data=YYYY-MM-DD/rejeitados.json
├── processed/
│   └── fato_vendas/ingest_date=YYYY-MM-DD/fato_vendas.parquet
├── gold/
│   ├── vendas_categoria/ingest_date=YYYY-MM-DD/vendas_categoria.parquet
│   ├── vendas_uf/ingest_date=YYYY-MM-DD/vendas_uf.parquet
│   └── vendas_cidade/ingest_date=YYYY-MM-DD/vendas_cidade.parquet  (extra)
└── athena-results/
```
Regras de Data Quality aplicadas
Um pedido é enviado para a quarentena (com o motivo da rejeição) quando:
`quantidade <= 0`, e/ou
`cliente_id` não existe na tabela `clientes`, e/ou
`product_id` não existe na tabela `produtos`.
Pedidos que não se enquadram em nenhuma dessas regras seguem para a camada Silver, onde são enriquecidos via `JOIN` com `clientes` e `produtos`, e recebem a coluna calculada `valor_total = quantidade * preco`.
Camada Gold
Agregações analíticas construídas a partir da Silver:
`gold_vendas_categoria`: métricas de negócio (total de pedidos, quantidade total, valor total vendido, ticket médio) agrupadas por categoria de produto.
`gold_vendas_uf`: as mesmas métricas agrupadas por UF do cliente.
Tabelas criadas no Amazon Athena
Tabela	Formato	Origem
`raw_clientes`	CSV	`raw/clientes/`
`raw_produtos`	CSV	`raw/produtos/`
`raw_pedidos`	CSV	`raw/pedidos/`
`quarentena_pedidos`	JSON (JSON Lines)	`quarantine/pedidos_rejeitados/`
`silver_fato_vendas`	Parquet/Snappy	`processed/fato_vendas/`
`gold_vendas_categoria`	Parquet/Snappy	`gold/vendas_categoria/`
`gold_vendas_uf`	Parquet/Snappy	`gold/vendas_uf/`
Todas as tabelas são particionadas e tiveram suas partições registradas via `MSCK REPAIR TABLE`.
Instruções de execução
Iniciar o AWS Academy Learner Lab e copiar as credenciais temporárias (`AWS Details`).
Abrir o notebook `atividade2_datalake.ipynb` no Google Colab.
Colar as credenciais na célula de configuração da sessão `boto3` (região `us-east-1`).
Executar as células na ordem, de cima para baixo:
Criação do bucket S3
Geração dos dados simulados (clientes, produtos, pedidos com anomalias)
Ingestão particionada na camada Raw
Aplicação das regras de Data Quality, gravação da Quarentena e construção da Silver
Construção da camada Gold (por categoria e por UF)
Criação do banco e das tabelas externas no Athena, com `MSCK REPAIR TABLE`
As duas queries de auditoria (metadados e conciliação de integridade) foram executadas diretamente no console web do Amazon Athena — ver evidências abaixo.
> Observação: as credenciais da AWS são temporárias (Learner Lab) e nunca foram commitadas neste repositório.
Queries de auditoria (Athena)
1 — Metadados dos arquivos (`$path`, `$file_size`):
```sql
SELECT
    element_at(split("$path", '/'), -1) AS nome_arquivo,
    "$file_size" AS tamanho_bytes,
    ROUND("$file_size" / 1024.0, 2) AS tamanho_kb,
    "$path" AS caminho_completo
FROM raw_pedidos
GROUP BY "$path", "$file_size"
ORDER BY tamanho_bytes DESC;
```
Evidência: `athena_metadados.png`
2 — Conciliação de integridade (total ingerido = válidos + rejeitados):
```sql
SELECT
    (SELECT COUNT(*) FROM raw_pedidos) AS total_raw,
    (SELECT COUNT(*) FROM silver_fato_vendas) AS total_silver,
    (SELECT COUNT(*) FROM quarentena_pedidos) AS total_quarentena,
    (SELECT COUNT(*) FROM silver_fato_vendas) + (SELECT COUNT(*) FROM quarentena_pedidos) AS soma_silver_quarentena,
    CASE
        WHEN (SELECT COUNT(*) FROM raw_pedidos) = (SELECT COUNT(*) FROM silver_fato_vendas) + (SELECT COUNT(*) FROM quarentena_pedidos)
        THEN 'OK'
        ELSE 'DIVERGENTE'
    END AS integridade;
```
Resultado obtido: `total_raw = 1500`, `total_silver = 1272`, `total_quarentena = 228`, `soma_silver_quarentena = 1500`, integridade = OK.
Evidência: `athena_conciliacao.png`
Estrutura do repositório
```
.
├── README.md
├── Atividade_2_DataLake_Clara.ipynb
├── athena_metadados.png
└── athena_conciliacao.png
```
