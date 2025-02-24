# Documentação Completa do Pipeline ETL

Este documento descreve o pipeline ETL completo que integra a etapa de web scraping, extração/descompactação, transformação e carregamento dos dados no SQL Server. O pipeline trabalha com dados de operações portuárias, divididos em duas partes principais:
- **Atracação** (atracacao_fato)
- **Carga** (carga_fato)

Cada etapa é implementada em módulos separados e orquestradas por um script principal que chama as funções de cada módulo.

---

## Sumário

- [Visão Geral do Pipeline ETL](#visão-geral-do-pipeline-etl)
- [Estrutura do Projeto](#estrutura-do-projeto)
- [Módulo web_scraping.py](#módulo-web_scrapingpy)
  - [Função: parse_anos](#função-parse_anos)
  - [Função: baixar_arquivos_por_ano](#função-baixar_arquivos_por_ano)
- [Módulo data_extract_decompress.py](#módulo-data_extract_decompresspy)
  - [Função: decompress_zip_files](#função-decompress_zip_files)
- [Módulo data_transform.py](#módulo-data_transformpy)
  - [Função: transformar_atracacao_fato](#função-transformar_atracacao_fato)
  - [Função: transformar_carga_fato](#função-transformar_carga_fato)
- [Módulo data_loading.py](#módulo-data_loadingpy)
  - [Função: carregar_atracacao_sql](#função-carregar_atracacao_sql)
  - [Função: carregar_carga_sql](#função-carregar_carga_sql)
- [Pipeline ETL Principal](#pipeline-etl-principal)
- [Requisitos](#requisitos)
- [Execução](#execução)
- [Considerações de Memória e Performance](#considerações-de-memória-e-performance)
- [Conclusão](#conclusão)

---

## Visão Geral do Pipeline ETL

O pipeline realiza as seguintes etapas:
1. **(Opcional) Web Scraping**:  
   Caso os arquivos ZIP ainda não tenham sido baixados, o módulo de web scraping pode automatizar o download para os anos desejados.
2. **Extração e Descompactação**:  
   Os arquivos ZIP baixados são descompactados para um diretório de trabalho.
3. **Transformação dos Dados**:
   - **Atracação**: Leitura dos arquivos CSV de atracação e dos tempos, filtragem por ano, conversão e formatação das datas, extração de informações derivadas (como ano e mês de início da operação) e união dos dados.
   - **Carga**: Leitura dos arquivos CSV de carga e carga conteinerizada, união com os dados de atracação processados, criação de colunas derivadas (ex.: cálculo do peso líquido e ajuste do código da mercadoria) e renomeação das colunas para padronização.
4. **Carregamento no SQL Server**:  
   Os datasets processados (atracacao_fato e carga_fato) são carregados em tabelas do SQL Server utilizando o conector JDBC.


