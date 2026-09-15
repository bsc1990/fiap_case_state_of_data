# ============================================================
# TECH CHALLENGE FIAP — FASE 3
# JOB: SILVER → GOLD
# ============================================================


# ------------------------------------------------------------
# 1. IMPORTAÇÃO DAS BIBLIOTECAS
# ------------------------------------------------------------

import sys

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.context import SparkContext
from pyspark.sql import functions as F


# ------------------------------------------------------------
# 2. INICIALIZAÇÃO DO AWS GLUE E DO SPARK
# ------------------------------------------------------------

args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME"]
)

spark_context = SparkContext.getOrCreate()

glue_context = GlueContext(
    spark_context
)

spark = glue_context.spark_session

job = Job(
    glue_context
)

job.init(
    args["JOB_NAME"],
    args
)


# ------------------------------------------------------------
# 3. DEFINIÇÃO DOS CAMINHOS DO AMAZON S3
# ------------------------------------------------------------

BUCKET = "fiap-tc3-state-of-data-bruno-20260915"

SILVER_PATH = (
    f"s3://{BUCKET}/silver/survey_profissionais/"
)

GOLD_PATH = (
    f"s3://{BUCKET}/gold"
)


# ------------------------------------------------------------
# 4. LEITURA DA CAMADA SILVER EM PARQUET
# ------------------------------------------------------------

silver = (
    spark.read
    .parquet(SILVER_PATH)
)


# ------------------------------------------------------------
# 5. VALIDAÇÃO DA LEITURA DA SILVER
# ------------------------------------------------------------

silver_count = silver.count()

silver_columns_count = len(
    silver.columns
)

print(
    f"SILVER LIDA COM SUCESSO: "
    f"{silver_count} linhas | "
    f"{silver_columns_count} colunas"
)


if silver_count == 0:

    raise ValueError(
        "A camada Silver está vazia."
    )


required_columns = [
    "ano_pesquisa",
    "id_respondente",
    "cargo",
    "senioridade",
    "salario_representativo",
    "usa_sql",
    "usa_python",
    "usa_aws",
    "usa_power_bi"
]


missing_columns = [
    column
    for column in required_columns
    if column not in silver.columns
]


if missing_columns:

    raise ValueError(
        f"Colunas obrigatórias ausentes na Silver: "
        f"{missing_columns}"
    )


print(
    "PASS — estrutura da Silver validada."
)


# ------------------------------------------------------------
# 6. CRIAÇÃO DA GOLD DE MERCADO
# ------------------------------------------------------------

gold_market = silver.select(
    "ano_pesquisa",
    "id_respondente",
    "situacao_trabalho",
    "setor",
    "cargo",
    "senioridade",
    "experiencia_dados",
    "escolaridade",
    "uf",
    "regiao",
    "modelo_trabalho_resumo",
    "satisfeito"
)


# ------------------------------------------------------------
# 7. CRIAÇÃO DA GOLD DE DIVERSIDADE
# ------------------------------------------------------------

gold_diversity = silver.select(
    "ano_pesquisa",
    "id_respondente",
    "idade",
    "faixa_idade",
    "genero",
    "raca_etnia",
    "uf",
    "regiao",
    "cargo",
    "senioridade",
    "faixa_salarial",
    "salario_representativo"
)


# ------------------------------------------------------------
# 8. CRIAÇÃO DA GOLD DE REMUNERAÇÃO
# ------------------------------------------------------------

gold_compensation = (
    silver
    .select(
        "ano_pesquisa",
        "id_respondente",
        "genero",
        "raca_etnia",
        "faixa_idade",
        "uf",
        "regiao",
        "escolaridade",
        "setor",
        "cargo",
        "senioridade",
        "experiencia_dados",
        "modelo_trabalho_resumo",
        "satisfeito",
        "faixa_salarial",
        "salario_representativo"
    )
    .filter(
        F.col("salario_representativo").isNotNull()
    )
)


# ------------------------------------------------------------
# 9. CRIAÇÃO DA GOLD DE TECNOLOGIAS
# ------------------------------------------------------------

gold_technology = silver.select(
    "ano_pesquisa",
    "id_respondente",
    "escolaridade",
    "setor",
    "cargo",
    "senioridade",
    "experiencia_dados",
    "uf",
    "regiao",
    "usa_sql",
    "usa_python",
    "usa_aws",
    "usa_power_bi"
)


# ------------------------------------------------------------
# 10. CRIAÇÃO DA GOLD DE INTELIGÊNCIA ARTIFICIAL
# ------------------------------------------------------------

gold_ai = silver.select(
    "ano_pesquisa",
    "id_respondente",
    "setor",
    "cargo",
    "senioridade",
    "experiencia_dados",
    "uf",
    "regiao",
    "modelo_trabalho_resumo",
    "satisfeito",
    "ia_prioridade",
    "uso_ia_produtividade",
    "usa_ia_produtividade"
)


# ------------------------------------------------------------
# 11. CRIAÇÃO DA GOLD DE GEOGRAFIA
# ------------------------------------------------------------

gold_geography = silver.select(
    "ano_pesquisa",
    "id_respondente",
    "uf",
    "regiao",
    "situacao_trabalho",
    "setor",
    "cargo",
    "senioridade",
    "modelo_trabalho_resumo",
    "faixa_salarial",
    "salario_representativo"
)


# ------------------------------------------------------------
# 12. CRIAÇÃO DA GOLD DE FORMAÇÃO
# ------------------------------------------------------------

gold_education = silver.select(
    "ano_pesquisa",
    "id_respondente",
    "escolaridade",
    "cargo",
    "senioridade",
    "experiencia_dados",
    "setor",
    "regiao",
    "faixa_salarial",
    "salario_representativo",
    "usa_sql",
    "usa_python",
    "usa_aws",
    "usa_power_bi"
)


# ------------------------------------------------------------
# 13. CRIAÇÃO DA GOLD DE SATISFAÇÃO
# ------------------------------------------------------------

gold_satisfaction = silver.select(
    "ano_pesquisa",
    "id_respondente",
    "satisfeito",
    "cargo",
    "senioridade",
    "setor",
    "experiencia_dados",
    "uf",
    "regiao",
    "modelo_trabalho_resumo",
    "faixa_salarial",
    "salario_representativo"
)


# ------------------------------------------------------------
# 14. CRIAÇÃO DA GOLD DE MODELO DE TRABALHO
# ------------------------------------------------------------

gold_work_model = silver.select(
    "ano_pesquisa",
    "id_respondente",
    "modelo_trabalho",
    "modelo_trabalho_resumo",
    "uf",
    "regiao",
    "setor",
    "cargo",
    "senioridade",
    "experiencia_dados",
    "satisfeito",
    "faixa_salarial",
    "salario_representativo"
)


# ------------------------------------------------------------
# 15. CRIAÇÃO DA GOLD DE EXPERIÊNCIA
# ------------------------------------------------------------

gold_experience = silver.select(
    "ano_pesquisa",
    "id_respondente",
    "experiencia_dados",
    "escolaridade",
    "cargo",
    "senioridade",
    "setor",
    "regiao",
    "faixa_salarial",
    "salario_representativo",
    "usa_sql",
    "usa_python",
    "usa_aws",
    "usa_power_bi",
    "usa_ia_produtividade"
)


# ------------------------------------------------------------
# 16. CRIAÇÃO DA GOLD DE SETORES
# ------------------------------------------------------------

gold_sectors = silver.select(
    "ano_pesquisa",
    "id_respondente",
    "setor",
    "cargo",
    "senioridade",
    "experiencia_dados",
    "escolaridade",
    "uf",
    "regiao",
    "modelo_trabalho_resumo",
    "faixa_salarial",
    "salario_representativo",
    "usa_sql",
    "usa_python",
    "usa_aws",
    "usa_power_bi",
    "usa_ia_produtividade"
)


# ------------------------------------------------------------
# 17. CRIAÇÃO DA GOLD DE INDICADORES ANUAIS
# ------------------------------------------------------------

# Define quais registros possuem um modelo de trabalho válido
modelo_trabalho_valido = (
    F.col("modelo_trabalho_resumo")
    .isin(
        "Remoto",
        "Híbrido",
        "Presencial"
    )
)


# Consolida os principais KPIs por ano da pesquisa
gold_annual_indicators = (
    silver
    .groupBy("ano_pesquisa")
    .agg(
        # Quantidade total de respondentes
        F.count("*").alias(
            "total_respondentes"
        ),

        # Quantidade de respondentes com cargo informado
        F.count("cargo").alias(
            "cargos_informados"
        ),

        # Quantidade de respondentes com salário calculado
        F.count("salario_representativo").alias(
            "salarios_validos"
        ),

        # Remuneração representativa média
        F.round(
            F.avg("salario_representativo"),
            2
        ).alias(
            "salario_representativo_medio"
        ),

        # Percentual de respondentes satisfeitos
        F.round(
            100 * F.avg("satisfeito"),
            1
        ).alias(
            "pct_satisfeitos"
        ),

        # Percentual de adoção de SQL
        F.round(
            100 * F.avg("usa_sql"),
            1
        ).alias(
            "pct_usa_sql"
        ),

        # Percentual de adoção de Python
        F.round(
            100 * F.avg("usa_python"),
            1
        ).alias(
            "pct_usa_python"
        ),

        # Percentual de adoção de AWS
        F.round(
            100 * F.avg("usa_aws"),
            1
        ).alias(
            "pct_usa_aws"
        ),

        # Percentual de adoção de Power BI
        F.round(
            100 * F.avg("usa_power_bi"),
            1
        ).alias(
            "pct_usa_power_bi"
        ),

        # Percentual de utilização de IA
        F.round(
            100 * F.avg("usa_ia_produtividade"),
            1
        ).alias(
            "pct_usa_ia_produtividade"
        ),

        # Percentual de trabalho remoto entre modelos válidos
        F.round(
            100 * F.avg(
                F.when(
                    ~modelo_trabalho_valido,
                    None
                )
                .when(
                    F.col("modelo_trabalho_resumo") == "Remoto",
                    1
                )
                .otherwise(0)
            ),
            1
        ).alias(
            "pct_trabalho_remoto"
        ),

        # Percentual de trabalho híbrido entre modelos válidos
        F.round(
            100 * F.avg(
                F.when(
                    ~modelo_trabalho_valido,
                    None
                )
                .when(
                    F.col("modelo_trabalho_resumo") == "Híbrido",
                    1
                )
                .otherwise(0)
            ),
            1
        ).alias(
            "pct_trabalho_hibrido"
        ),

        # Percentual de trabalho presencial entre modelos válidos
        F.round(
            100 * F.avg(
                F.when(
                    ~modelo_trabalho_valido,
                    None
                )
                .when(
                    F.col("modelo_trabalho_resumo") == "Presencial",
                    1
                )
                .otherwise(0)
            ),
            1
        ).alias(
            "pct_trabalho_presencial"
        )
    )
    .orderBy("ano_pesquisa")
)


# ------------------------------------------------------------
# 18. ORGANIZAÇÃO DAS 12 TABELAS GOLD
# ------------------------------------------------------------

GOLD_TABLES = {
    "mercado": gold_market,
    "diversidade": gold_diversity,
    "remuneracao": gold_compensation,
    "tecnologias": gold_technology,
    "ia": gold_ai,
    "geografia": gold_geography,
    "formacao": gold_education,
    "satisfacao": gold_satisfaction,
    "modelo_trabalho": gold_work_model,
    "experiencia": gold_experience,
    "setores": gold_sectors,
    "indicadores_anuais": gold_annual_indicators
}


# ------------------------------------------------------------
# 19. VALIDAÇÃO DOS VOLUMES DAS TABELAS GOLD
# ------------------------------------------------------------

gold_counts = {}


for table_name, dataframe in GOLD_TABLES.items():

    table_count = dataframe.count()

    gold_counts[table_name] = table_count

    print(
        f"GOLD {table_name.upper()}: "
        f"{table_count} linhas | "
        f"{len(dataframe.columns)} colunas"
    )

    if table_count == 0:

        raise ValueError(
            f"A tabela Gold {table_name} está vazia."
        )


# ------------------------------------------------------------
# 20. VALIDAÇÃO DAS TABELAS DE MESMO GRÃO DA SILVER
# ------------------------------------------------------------

tables_expected_full_volume = [
    "mercado",
    "diversidade",
    "tecnologias",
    "ia",
    "geografia",
    "formacao",
    "satisfacao",
    "modelo_trabalho",
    "experiencia",
    "setores"
]


for table_name in tables_expected_full_volume:

    if gold_counts[table_name] != silver_count:

        raise ValueError(
            f"A Gold {table_name} deveria possuir "
            f"{silver_count} linhas, mas possui "
            f"{gold_counts[table_name]}."
        )


# ------------------------------------------------------------
# 21. VALIDAÇÃO DA GOLD DE INDICADORES ANUAIS
# ------------------------------------------------------------

expected_years = (
    silver
    .select("ano_pesquisa")
    .distinct()
    .count()
)


if gold_counts["indicadores_anuais"] != expected_years:

    raise ValueError(
        "A Gold indicadores_anuais deveria possuir "
        f"{expected_years} registros, mas possui "
        f"{gold_counts['indicadores_anuais']}."
    )


print(
    "PASS — volumes das 12 tabelas Gold validados."
)


# ------------------------------------------------------------
# 22. EXIBIÇÃO DOS INDICADORES ANUAIS NOS LOGS
# ------------------------------------------------------------

print(
    "GOLD INDICADORES ANUAIS"
)

gold_annual_indicators.show(
    truncate=False
)


# ------------------------------------------------------------
# 23. EXIBIÇÃO DE AMOSTRAS DAS OUTRAS TABELAS
# ------------------------------------------------------------

for table_name, dataframe in GOLD_TABLES.items():

    if table_name != "indicadores_anuais":

        print(
            f"AMOSTRA DA GOLD {table_name.upper()}"
        )

        dataframe.show(
            3,
            truncate=False
        )


# ------------------------------------------------------------
# 24. GRAVAÇÃO DAS 12 TABELAS GOLD NO AMAZON S3
# ------------------------------------------------------------

for table_name, dataframe in GOLD_TABLES.items():

    output_path = (
        f"{GOLD_PATH}/{table_name}/"
    )

    (
        dataframe
        .write
        .mode("overwrite")
        .partitionBy("ano_pesquisa")
        .parquet(output_path)
    )

    print(
        f"SUCCESS — Gold {table_name} gravada em: "
        f"{output_path}"
    )


# ------------------------------------------------------------
# 25. VALIDAÇÃO FINAL DA CAMADA GOLD
# ------------------------------------------------------------

print(
    "CAMADA GOLD CRIADA COM SUCESSO."
)

print(
    f"TOTAL DE TABELAS GOLD: "
    f"{len(GOLD_TABLES)}"
)

print(
    f"TABELAS CRIADAS: "
    f"{list(GOLD_TABLES.keys())}"
)


# ------------------------------------------------------------
# 26. FINALIZAÇÃO DO AWS GLUE JOB
# ------------------------------------------------------------

job.commit()