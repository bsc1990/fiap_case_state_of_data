-- ============================================================
-- TECH CHALLENGE FIAP — FASE 3
-- CONSULTAS DE VALIDAÇÃO E ANÁLISE DAS TABELAS GOLD
-- Banco de dados: fiap_state_of_data
-- ============================================================


-- 1. Visualiza uma amostra da Gold de mercado
SELECT *
FROM fiap_state_of_data.gold_mercado
LIMIT 10;


-- 2. Valida a quantidade de registros e as partições anuais
SELECT
    CAST(ano_pesquisa AS INTEGER) AS ano_pesquisa,
    COUNT(*) AS total_registros
FROM fiap_state_of_data.gold_mercado
GROUP BY ano_pesquisa
ORDER BY ano_pesquisa;


-- 3. Consulta os KPIs anuais consolidados
SELECT *
FROM fiap_state_of_data.gold_indicadores_anuais
ORDER BY CAST(ano_pesquisa AS INTEGER);


-- 4. Calcula a adoção percentual das tecnologias por ano
SELECT
    CAST(ano_pesquisa AS INTEGER) AS ano_pesquisa,
    ROUND(100.0 * AVG(CAST(usa_sql AS DOUBLE)), 1) AS pct_sql,
    ROUND(100.0 * AVG(CAST(usa_python AS DOUBLE)), 1) AS pct_python,
    ROUND(100.0 * AVG(CAST(usa_aws AS DOUBLE)), 1) AS pct_aws,
    ROUND(100.0 * AVG(CAST(usa_power_bi AS DOUBLE)), 1) AS pct_power_bi
FROM fiap_state_of_data.gold_tecnologias
GROUP BY ano_pesquisa
ORDER BY ano_pesquisa;


-- 5. Calcula a adoção de IA para produtividade entre respostas válidas
SELECT
    CAST(ano_pesquisa AS INTEGER) AS ano_pesquisa,
    COUNT(usa_ia_produtividade) AS respostas_validas,
    ROUND(
        100.0 * AVG(CAST(usa_ia_produtividade AS DOUBLE)),
        1
    ) AS pct_usa_ia_produtividade
FROM fiap_state_of_data.gold_ia
WHERE usa_ia_produtividade IS NOT NULL
GROUP BY ano_pesquisa
ORDER BY ano_pesquisa;


-- 6. Analisa a remuneração média por senioridade e ano
SELECT
    CAST(ano_pesquisa AS INTEGER) AS ano_pesquisa,
    senioridade,
    COUNT(*) AS profissionais,
    ROUND(AVG(salario_representativo), 2) AS salario_medio
FROM fiap_state_of_data.gold_remuneracao
WHERE salario_representativo IS NOT NULL
  AND senioridade IS NOT NULL
GROUP BY ano_pesquisa, senioridade
ORDER BY ano_pesquisa, salario_medio DESC;


-- 7. Calcula a distribuição dos modelos de trabalho válidos
SELECT
    CAST(ano_pesquisa AS INTEGER) AS ano_pesquisa,
    modelo_trabalho_resumo,
    COUNT(*) AS profissionais,
    ROUND(
        100.0 * COUNT(*) /
        SUM(COUNT(*)) OVER (PARTITION BY ano_pesquisa),
        1
    ) AS percentual
FROM fiap_state_of_data.gold_modelo_trabalho
WHERE modelo_trabalho_resumo IN ('Remoto', 'Híbrido', 'Presencial')
GROUP BY ano_pesquisa, modelo_trabalho_resumo
ORDER BY ano_pesquisa, percentual DESC;


-- 8. Compara remuneração e participação por gênero
SELECT
    CAST(ano_pesquisa AS INTEGER) AS ano_pesquisa,
    genero,
    COUNT(*) AS profissionais,
    ROUND(AVG(salario_representativo), 2) AS salario_medio
FROM fiap_state_of_data.gold_diversidade
WHERE genero IS NOT NULL
  AND salario_representativo IS NOT NULL
GROUP BY ano_pesquisa, genero
ORDER BY ano_pesquisa, profissionais DESC;


-- 9. Compara remuneração por gênero e senioridade
SELECT
    CAST(ano_pesquisa AS INTEGER) AS ano_pesquisa,
    senioridade,
    genero,
    COUNT(*) AS profissionais,
    ROUND(AVG(salario_representativo), 2) AS salario_medio
FROM fiap_state_of_data.gold_remuneracao
WHERE genero IN ('Feminino', 'Masculino')
  AND senioridade IN ('Júnior', 'Pleno', 'Sênior')
  AND salario_representativo IS NOT NULL
GROUP BY ano_pesquisa, senioridade, genero
ORDER BY ano_pesquisa, senioridade, genero;


-- 10. Compara satisfação e remuneração por ano
SELECT
    CAST(ano_pesquisa AS INTEGER) AS ano_pesquisa,
    satisfeito,
    COUNT(*) AS profissionais,
    ROUND(AVG(salario_representativo), 2) AS salario_medio
FROM fiap_state_of_data.gold_satisfacao
WHERE satisfeito IS NOT NULL
  AND salario_representativo IS NOT NULL
GROUP BY ano_pesquisa, satisfeito
ORDER BY ano_pesquisa, satisfeito;


-- 11. Calcula a satisfação por modelo de trabalho
SELECT
    CAST(ano_pesquisa AS INTEGER) AS ano_pesquisa,
    modelo_trabalho_resumo,
    COUNT(satisfeito) AS respostas_validas,
    ROUND(100.0 * AVG(CAST(satisfeito AS DOUBLE)), 1) AS pct_satisfeitos
FROM fiap_state_of_data.gold_satisfacao
WHERE satisfeito IS NOT NULL
  AND modelo_trabalho_resumo IN ('Remoto', 'Híbrido', 'Presencial')
GROUP BY ano_pesquisa, modelo_trabalho_resumo
ORDER BY ano_pesquisa, modelo_trabalho_resumo;


-- 12. Identifica os cinco cargos mais frequentes em cada ano
WITH cargos AS (
    SELECT
        CAST(ano_pesquisa AS INTEGER) AS ano_pesquisa,
        cargo,
        COUNT(*) AS profissionais
    FROM fiap_state_of_data.gold_mercado
    WHERE cargo IS NOT NULL
    GROUP BY ano_pesquisa, cargo
),
ranking AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY ano_pesquisa
            ORDER BY profissionais DESC, cargo
        ) AS posicao
    FROM cargos
)
SELECT
    ano_pesquisa,
    posicao,
    cargo,
    profissionais
FROM ranking
WHERE posicao <= 5
ORDER BY ano_pesquisa, posicao;

