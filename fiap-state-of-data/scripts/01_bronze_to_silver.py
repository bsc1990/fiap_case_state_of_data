# ============================================================
# TECH CHALLENGE FIAP — FASE 3
# JOB: BRONZE → SILVER
# ============================================================


# ------------------------------------------------------------
# 1. IMPORTAÇÃO DAS BIBLIOTECAS
# ------------------------------------------------------------

import sys

from functools import reduce

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

BRONZE_PATH = f"s3://{BUCKET}/bronze"

SILVER_PATH = (
    f"s3://{BUCKET}/silver/survey_profissionais/"
)

FILES = {
    2023: (
        f"{BRONZE_PATH}/"
        "dataset_state_of_data_2023.csv"
    ),

    2024: (
        f"{BRONZE_PATH}/"
        "dataset_state_of_data_2024.csv"
    ),

    2025: (
        f"{BRONZE_PATH}/"
        "dataset_state_of_data_2025.csv"
    )
}


# ------------------------------------------------------------
# 4. MAPEAMENTO DAS COLUNAS BRONZE → SILVER
# ------------------------------------------------------------

COLUMN_MAP = {
    2023: {
        "id_respondente": "('P0', 'id')",
        "idade": "('P1_a ', 'Idade')",
        "faixa_idade": "('P1_a_1 ', 'Faixa idade')",
        "genero": "('P1_b ', 'Genero')",
        "raca_etnia": "('P1_c ', 'Cor/raca/etnia')",
        "uf": "('P1_i_1 ', 'uf onde mora')",
        "regiao": "('P1_i_2 ', 'Regiao onde mora')",
        "escolaridade": "('P1_l ', 'Nivel de Ensino')",
        "situacao_trabalho": (
            "('P2_a ', 'Qual sua situação atual de trabalho?')"
        ),
        "setor": "('P2_b ', 'Setor')",
        "cargo": "('P2_f ', 'Cargo Atual')",
        "senioridade": "('P2_g ', 'Nivel')",
        "faixa_salarial": "('P2_h ', 'Faixa salarial')",
        "experiencia_dados": (
            "('P2_i ', 'Quanto tempo de experiência "
            "na área de dados você tem?')"
        ),
        "satisfeito": (
            "('P2_k ', 'Você está satisfeito "
            "na sua empresa atual?')"
        ),
        "modelo_trabalho": (
            "('P2_r ', 'Atualmente qual a sua forma de trabalho?')"
        ),
        "ia_prioridade": (
            "('P3_e ', 'AI Generativa é uma "
            "prioridade em sua empresa?')"
        ),
        "usa_sql": "('P4_d_1 ', 'SQL')",
        "usa_python": "('P4_d_3 ', 'Python')",
        "usa_aws": (
            "('P4_h_2 ', 'Amazon Web Services (AWS)')"
        ),
        "usa_power_bi": (
            "('P4_j_1 ', 'Microsoft PowerBI')"
        )
    },

    2024: {
        "id_respondente": "0.a_token",
        "idade": "1.a_idade",
        "faixa_idade": "1.a.1_faixa_idade",
        "genero": "1.b_genero",
        "raca_etnia": "1.c_cor/raca/etnia",
        "uf": "1.i.1_uf_onde_mora",
        "regiao": "1.i.2_regiao_onde_mora",
        "escolaridade": "1.l_nivel_de_ensino",
        "situacao_trabalho": "2.a_situação_de_trabalho",
        "setor": "2.b_setor",
        "cargo": "2.f_cargo_atual",
        "senioridade": "2.g_nivel",
        "faixa_salarial": "2.h_faixa_salarial",
        "experiencia_dados": (
            "2.i_tempo_de_experiencia_em_dados"
        ),
        "satisfeito": "2.k_satisfeito_atualmente",
        "modelo_trabalho": (
            "2.r_modelo_de_trabalho_atual"
        ),
        "ia_prioridade": (
            "3.e_ai_generativa_e_llm_é_uma_prioridade?"
        ),
        "usa_sql": "4.d.1_SQL",
        "usa_python": "4.d.3_Python",
        "usa_aws": (
            "4.h.1_Amazon Web Services (AWS)"
        ),
        "usa_power_bi": (
            "4.j.1_Microsoft PowerBI"
        ),
        "uso_ia_produtividade": (
            "4.m_usa_chatgpt_ou_copilot_no_trabalho?"
        )
    },

    2025: {
        "id_respondente": "0.a_token",
        "idade": "1.a_idade",
        "faixa_idade": "1.a.1_faixa_idade",
        "genero": "1.b_genero",
        "raca_etnia": "1.c_cor/raca/etnia",
        "uf": "1.i.1_uf_onde_mora",
        "regiao": "1.i.2_regiao_onde_mora",
        "escolaridade": "1.l_nivel_de_ensino",
        "situacao_trabalho": "2.a_situação_de_trabalho",
        "setor": "2.b_setor",
        "cargo": "2.f_cargo_atual",
        "senioridade": "2.g_nivel",
        "faixa_salarial": "2.h_faixa_salarial",
        "experiencia_dados": (
            "2.i_tempo_de_experiencia_em_dados"
        ),
        "satisfeito": "2.k_satisfeito_atualmente",
        "modelo_trabalho": (
            "2.q_modelo_de_trabalho_atual"
        ),
        "ia_prioridade": (
            "3.e_ai_generativa_e_llm_é_uma_prioridade?"
        ),
        "usa_sql": "4.c.1_SQL",
        "usa_python": "4.c.3_Python",
        "usa_aws": (
            "4.e.1_Amazon Web Services (AWS)"
        ),
        "usa_power_bi": (
            "4.g.1_Microsoft PowerBI"
        ),
        "uso_ia_produtividade": (
            "4.j_usa_chatgpt_ou_copilot_no_trabalho?"
        )
    }
}


# ------------------------------------------------------------
# 5. FUNÇÃO PARA LEITURA DOS ARQUIVOS CSV
# ------------------------------------------------------------

def read_bronze_csv(path):

    dataframe = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .option("multiLine", True)
        .option("escape", '"')
        .csv(path)
    )

    return dataframe


# ------------------------------------------------------------
# 6. LEITURA DAS TRÊS PESQUISAS DA BRONZE
# ------------------------------------------------------------

df_2023 = read_bronze_csv(
    FILES[2023]
)

df_2024 = read_bronze_csv(
    FILES[2024]
)

df_2025 = read_bronze_csv(
    FILES[2025]
)

bronze = {
    2023: df_2023,
    2024: df_2024,
    2025: df_2025
}


# ------------------------------------------------------------
# 7. VALIDAÇÃO DA LEITURA DOS ARQUIVOS
# ------------------------------------------------------------

for year, dataframe in bronze.items():

    print(
        f"BRONZE {year}: "
        f"{dataframe.count()} linhas | "
        f"{len(dataframe.columns)} colunas"
    )


# ------------------------------------------------------------
# 8. VALIDAÇÃO DO MAPEAMENTO DE COLUNAS
# ------------------------------------------------------------

mapping_errors = []

for year, mapping in COLUMN_MAP.items():

    available_columns = set(
        bronze[year].columns
    )

    for target_column, source_column in mapping.items():

        if source_column not in available_columns:

            mapping_errors.append(
                (
                    year,
                    target_column,
                    source_column
                )
            )


if mapping_errors:

    raise ValueError(
        f"Colunas mapeadas não encontradas: {mapping_errors}"
    )


print(
    "PASS — todas as colunas mapeadas "
    "foram encontradas na Bronze."
)


# ------------------------------------------------------------
# 9. DEFINIÇÃO DO SCHEMA PADRONIZADO DA SILVER
# ------------------------------------------------------------

SILVER_COLUMNS = sorted(
    set().union(
        *[
            set(mapping.keys())
            for mapping in COLUMN_MAP.values()
        ]
    )
)


# ------------------------------------------------------------
# 10. FUNÇÃO PARA SELECIONAR E RENOMEAR AS COLUNAS
# ------------------------------------------------------------

def select_silver(dataframe, year):

    mapping = COLUMN_MAP[year]

    expressions = [
        F.lit(year)
        .cast("int")
        .alias("ano_pesquisa")
    ]

    for target_column in SILVER_COLUMNS:

        if target_column in mapping:

            source_column = mapping[
                target_column
            ]

            safe_source_column = (
                source_column.replace(
                    "`",
                    "``"
                )
            )

            expression = (
                F.col(
                    f"`{safe_source_column}`"
                )
                .cast("string")
                .alias(target_column)
            )

        else:

            expression = (
                F.lit(None)
                .cast("string")
                .alias(target_column)
            )

        expressions.append(
            expression
        )

    return dataframe.select(
        *expressions
    )


# ------------------------------------------------------------
# 11. PADRONIZAÇÃO INDIVIDUAL DOS TRÊS ANOS
# ------------------------------------------------------------

silver_2023 = select_silver(
    bronze[2023],
    2023
)

silver_2024 = select_silver(
    bronze[2024],
    2024
)

silver_2025 = select_silver(
    bronze[2025],
    2025
)


# ------------------------------------------------------------
# 12. UNIÃO DAS TRÊS PESQUISAS
# ------------------------------------------------------------

silver_parts = [
    silver_2023,
    silver_2024,
    silver_2025
]

silver = reduce(
    lambda left, right: left.unionByName(
        right,
        allowMissingColumns=True
    ),
    silver_parts
)


# ------------------------------------------------------------
# 13. PADRONIZAÇÃO DAS VARIÁVEIS DE TECNOLOGIA
# ------------------------------------------------------------

binary_columns = [
    "usa_sql",
    "usa_python",
    "usa_aws",
    "usa_power_bi"
]

for column in binary_columns:

    normalized_value = F.lower(
        F.trim(
            F.col(column).cast("string")
        )
    )

    silver = silver.withColumn(
        column,

        F.when(
            normalized_value.isin(
                "1",
                "1.0",
                "true",
                "sim"
            ),
            1
        )
        .when(
            normalized_value.isin(
                "0",
                "0.0",
                "false",
                "não",
                "nao"
            ),
            0
        )
        .otherwise(None)
        .cast("int")
    )


# ------------------------------------------------------------
# 14. PADRONIZAÇÃO DAS DEMAIS VARIÁVEIS
# ------------------------------------------------------------

silver = (
    silver

    # Converte idade para número inteiro
    .withColumn(
        "idade",
        F.col("idade").cast("int")
    )

    # Converte satisfação para indicador binário
    .withColumn(
        "satisfeito",

        F.when(
            F.lower(
                F.col("satisfeito").cast("string")
            ).isin(
                "1",
                "1.0",
                "true",
                "sim"
            ),
            1
        )
        .when(
            F.lower(
                F.col("satisfeito").cast("string")
            ).isin(
                "0",
                "0.0",
                "false",
                "não",
                "nao"
            ),
            0
        )
        .otherwise(None)
        .cast("int")
    )

    # Identifica utilização de IA para produtividade
    .withColumn(
        "usa_ia_produtividade",

        F.when(
            F.col(
                "uso_ia_produtividade"
            ).isNull(),
            None
        )
        .when(
            F.lower(
                F.col(
                    "uso_ia_produtividade"
                )
            ).contains(
                "não uso"
            ),
            0
        )
        .when(
            F.lower(
                F.col(
                    "uso_ia_produtividade"
                )
            ).contains(
                "não utilizo"
            ),
            0
        )
        .otherwise(1)
        .cast("int")
    )

    # Resume os modelos de trabalho
    .withColumn(
        "modelo_trabalho_resumo",

        F.when(
            F.lower(
                F.col("modelo_trabalho")
            ).contains(
                "100% remoto"
            ),
            "Remoto"
        )
        .when(
            F.lower(
                F.col("modelo_trabalho")
            ).contains(
                "híbrido"
            ),
            "Híbrido"
        )
        .when(
            F.lower(
                F.col("modelo_trabalho")
            ).contains(
                "presencial"
            ),
            "Presencial"
        )
        .otherwise(
            "Não informado"
        )
    )

    # Remove IDs duplicados dentro de cada pesquisa
    .dropDuplicates(
        [
            "ano_pesquisa",
            "id_respondente"
        ]
    )
)


# ------------------------------------------------------------
# 15. CRIAÇÃO DO SALÁRIO REPRESENTATIVO
# ------------------------------------------------------------

salary_midpoints = {
    "Menos de R$ 1.000/mês": 500,
    "de R$ 1.001/mês a R$ 2.000/mês": 1500,
    "de R$ 2.001/mês a R$ 3.000/mês": 2500,
    "de R$ 3.001/mês a R$ 4.000/mês": 3500,
    "de R$ 4.001/mês a R$ 6.000/mês": 5000,
    "de R$ 6.001/mês a R$ 8.000/mês": 7000,
    "de R$ 8.001/mês a R$ 12.000/mês": 10000,
    "de R$ 12.001/mês a R$ 16.000/mês": 14000,
    "de R$ 16.001/mês a R$ 20.000/mês": 18000,
    "de R$ 20.001/mês a R$ 25.000/mês": 22500,
    "de R$ 25.001/mês a R$ 30.000/mês": 27500,

    # Correção de aparente erro textual na edição 2025
    "de R$ 25.001/mês a R$ 3000/mês": 27500,

    "de R$ 30.001/mês a R$ 40.000/mês": 35000,
    "Acima de R$ 40.001/mês": 45000
}


salary_map_items = []

for salary_range, midpoint in salary_midpoints.items():

    salary_map_items.extend(
        [
            F.lit(salary_range),
            F.lit(midpoint)
        ]
    )


salary_map = F.create_map(
    *salary_map_items
)


silver = silver.withColumn(
    "salario_representativo",

    salary_map[
        F.col("faixa_salarial")
    ].cast("double")
)


# ------------------------------------------------------------
# 16. VALIDAÇÕES DE QUALIDADE DA SILVER
# ------------------------------------------------------------

silver.cache()

silver_count = silver.count()

silver_columns_count = len(
    silver.columns
)

print(
    f"SILVER: {silver_count} linhas | "
    f"{silver_columns_count} colunas"
)


if "salario_representativo" not in silver.columns:

    raise ValueError(
        "A coluna salario_representativo "
        "não foi criada."
    )


invalid_years = (
    silver
    .filter(
        ~F.col("ano_pesquisa").isin(
            2023,
            2024,
            2025
        )
    )
    .count()
)


if invalid_years > 0:

    raise ValueError(
        f"Foram encontrados {invalid_years} "
        "registros com ano inválido."
    )


invalid_ages = (
    silver
    .filter(
        (F.col("idade") < 14)
        |
        (F.col("idade") > 100)
    )
    .count()
)


if invalid_ages > 0:

    raise ValueError(
        f"Foram encontradas {invalid_ages} "
        "idades fora do intervalo esperado."
    )


duplicates = (
    silver
    .groupBy(
        "ano_pesquisa",
        "id_respondente"
    )
    .count()
    .filter(
        F.col("count") > 1
    )
    .count()
)


if duplicates > 0:

    raise ValueError(
        f"Foram encontrados {duplicates} "
        "IDs duplicados."
    )


print(
    "PASS — validações da camada Silver concluídas."
)


# ------------------------------------------------------------
# 17. EXIBIÇÃO DE UMA AMOSTRA DA SILVER NOS LOGS
# ------------------------------------------------------------

silver.select(
    "ano_pesquisa",
    "genero",
    "cargo",
    "senioridade",
    "faixa_salarial",
    "salario_representativo",
    "usa_python"
).show(
    10,
    truncate=False
)


# ------------------------------------------------------------
# 18. GRAVAÇÃO DA CAMADA SILVER EM PARQUET
# ------------------------------------------------------------

(
    silver
    .write
    .mode("overwrite")
    .partitionBy("ano_pesquisa")
    .parquet(SILVER_PATH)
)


print(
    f"SUCCESS — camada Silver gravada em: "
    f"{SILVER_PATH}"
)


# ------------------------------------------------------------
# 19. FINALIZAÇÃO DO AWS GLUE JOB
# ------------------------------------------------------------

job.commit()