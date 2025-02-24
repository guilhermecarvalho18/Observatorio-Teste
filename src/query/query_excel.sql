WITH AtracacoesFiltradas AS (
    SELECT
        id_atracacao,
        data_inicio_operacao,
        tempo_espera_atracacao,
        tempo_atracado,
        CASE 
            WHEN uf = 'CE' THEN 'Ceará'
            WHEN regiao_geografica = 'Nordeste' THEN 'Nordeste'
            ELSE 'Brasil'
        END AS localidade,
        YEAR(data_inicio_operacao) AS ano,
        MONTH(data_inicio_operacao) AS mes
    FROM atracacao_fato
    WHERE YEAR(data_inicio_operacao) IN (2021, 2023)
)
SELECT
    localidade,
    ano,
    mes,
    COUNT(*) AS numero_atracacoes,
    AVG(tempo_espera_atracacao) AS tempo_espera_medio,
    AVG(tempo_atracado) AS tempo_atracado_medio
FROM AtracacoesFiltradas
GROUP BY
    localidade,
    ano,
    mes
ORDER BY
    ano,
    mes;
