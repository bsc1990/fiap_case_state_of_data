# Tech Challenge FIAP — State of Data 2023–2025

Projeto de engenharia e análise de dados desenvolvido para o Tech Challenge da Fase 3 da Pós-Tech em Data Analytics da FIAP.

O projeto consolida as pesquisas State of Data de 2023, 2024 e 2025 em uma arquitetura de dados na AWS, organiza os dados nas camadas Bronze, Silver e Gold e transforma as tabelas analíticas em visualizações e conclusões executivas.

## Objetivo

Construir um pipeline reproduzível para:

1. armazenar os arquivos originais no Amazon S3;
2. padronizar schemas e variáveis com AWS Glue e PySpark;
3. disponibilizar tabelas analíticas particionadas em Parquet;
4. catalogar e consultar os dados com Glue Data Catalog e Athena;
5. produzir análises e gráficos em Python no Google Colab.

## Arquitetura

```text
CSV State of Data
       ↓
Amazon S3 — Bronze
       ↓
AWS Glue/PySpark — Bronze para Silver
       ↓
Amazon S3 — Silver, Parquet particionado por ano_pesquisa
       ↓
AWS Glue/PySpark — Silver para Gold
       ↓
Amazon S3 — 12 tabelas Gold em Parquet
       ↓
Glue Crawler e Glue Data Catalog
       ↓
Amazon Athena
       ↓
CSV Gold exportado
       ↓
Google Colab — análises e DataViz
```

O diagrama visual da arquitetura será disponibilizado em `arquitetura/`.

## Estrutura do repositório

```text
fiap-state-of-data/
├── README.md
├── scripts/
│   ├── 01_bronze_to_silver.py
│   ├── 02_silver_to_gold.py
│   └── 03_validacoes_athena.sql
├── notebooks/
│   └── 04_analise_gold.ipynb
├── outputs/
│   └── graficos_gold/
├── arquitetura/
└── evidencias/
```

## Pipeline AWS

### Bronze para Silver

O script `scripts/01_bronze_to_silver.py`:

- lê os três arquivos CSV armazenados na Bronze;
- valida os schemas de origem;
- seleciona e padroniza as colunas equivalentes entre as pesquisas;
- harmoniza tecnologias, satisfação, IA e modelo de trabalho;
- cria uma estimativa de salário representativo a partir das faixas salariais;
- verifica anos, idades e IDs duplicados;
- grava a Silver em Parquet particionado por `ano_pesquisa`.

### Silver para Gold

O script `scripts/02_silver_to_gold.py` cria 12 tabelas analíticas:

- `gold_mercado`;
- `gold_diversidade`;
- `gold_remuneracao`;
- `gold_tecnologias`;
- `gold_ia`;
- `gold_geografia`;
- `gold_formacao`;
- `gold_satisfacao`;
- `gold_modelo_trabalho`;
- `gold_experiencia`;
- `gold_setores`;
- `gold_indicadores_anuais`.

As tabelas são gravadas em Parquet, particionadas por `ano_pesquisa`, e catalogadas para consulta no Athena.

## Análise no Google Colab

O notebook `notebooks/04_analise_gold.ipynb` começa exclusivamente nos 12 CSVs exportados das tabelas Gold. Ele não reexecuta as transformações AWS.

O notebook contém:

- validação de volumes, schemas, anos e duplicidades;
- reconciliação dos 14.002 respondentes;
- análise de tecnologias e IA;
- remuneração por senioridade;
- composição dos modelos de trabalho;
- satisfação e remuneração;
- diversidade salarial;
- cargos mais frequentes;
- exportação de oito gráficos em PNG.

Para executá-lo no Colab, envie os 12 arquivos CSV Gold para `/content` ou armazene-os em:

```text
/content/drive/MyDrive/Tech_Challenge_Fase_3/data/gold
```

Depois, selecione **Ambiente de execução → Executar tudo**.

## Principais resultados

- O salário representativo médio aumentou de aproximadamente R$ 10,5 mil em 2023 para R$ 13,2 mil em 2025.
- A satisfação passou de 72,0% em 2023 para 69,0% em 2025.
- Python avançou de 74,9% para 92,0% das respostas válidas.
- AWS avançou de 30,6% para 48,3% das respostas válidas.
- O uso de IA para produtividade superou 90% entre respostas válidas em 2024 e 2025.
- Trabalho remoto apresentou percentuais de satisfação superiores aos do presencial nas três edições.
- Analista de Dados permaneceu como o cargo mais frequente.

Esses resultados descrevem as amostras das pesquisas e não demonstram relações causais nem representam um censo do mercado brasileiro.

## Observações metodológicas

- As pesquisas possuem amostras independentes e tamanhos diferentes em cada ano.
- Os percentuais de tecnologias e IA utilizam apenas respostas válidas como denominador.
- O salário representativo é uma estimativa calculada a partir das faixas salariais informadas.
- Comparações de satisfação, gênero e modelo de trabalho são associações descritivas.
- A amostra de 2025 possui menos respondentes que as amostras de 2023 e 2024.

## Tecnologias

- Amazon S3
- AWS Glue
- PySpark
- AWS Glue Data Catalog
- Amazon Athena
- Python
- Pandas
- Matplotlib
- Seaborn
- Google Colab

