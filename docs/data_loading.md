# Script de Carregamento de Dados para SQL Server

Este script utiliza Apache Spark para ler datasets processados (em formato Parquet) referentes às operações de atracação e carga, e carregá-los em tabelas do SQL Server utilizando o conector JDBC.

---

## Sumário

- [Visão Geral](#visão-geral)
- [Requisitos](#requisitos)
- [Funções Principais](#funções-principais)
  - [carregar_atracacao_sql](#carregar_atracacao_sql)
  - [carregar_carga_sql](#carregar_carga_sql)
- [Configuração e Execução](#configuração-e-execução)
- [Considerações Finais](#considerações-finais)

---

## Visão Geral

O script possui duas funções principais:

1. **carregar_atracacao_sql**:  
   Lê os dados processados do dataset `atracacao_fato` (armazenados em formato Parquet) e os carrega em uma tabela do SQL Server via JDBC.

2. **carregar_carga_sql**:  
   Lê os dados processados do dataset `carga_fato` (também em formato Parquet) e os carrega em uma tabela do SQL Server, utilizando o conector JDBC.

Após o carregamento, o SparkContext é encerrado e uma mensagem é exibida para indicar que os dados foram carregados com sucesso.

---

## Requisitos

- **Apache Spark**: O script utiliza Spark para o processamento dos dados.
- **Conector JDBC para SQL Server**:  
  O script configura o SparkSession para incluir o pacote `com.microsoft.sqlserver:mssql-jdbc:11.2.3.jre17`.
- **Dados Processados**:  
  Os arquivos de atracação e carga devem estar previamente processados e armazenados em formato Parquet nos diretórios:
  - `datalake/processed/atracacao`
  - `datalake/processed/carga`

---

## Funções Principais

### carregar_atracacao_sql

**Descrição**:  
Lê os dados processados de `atracacao_fato` (em formato Parquet) e os carrega na tabela do SQL Server.

**Argumentos**:
- `connection_properties` (dict): Propriedades de conexão (usuário, senha, driver, etc.).
- `jdbc_url` (str): URL JDBC de conexão com o SQL Server.
- `input_path` (str, padrão: `"datalake/processed/atracacao"`): Caminho do diretório contendo os dados processados.
- `table_name` (str, padrão: `"atracacao_fato"`): Nome da tabela de destino no SQL Server.

**Processo**:
1. Cria um SparkSession configurado com o conector JDBC para SQL Server.
2. Lê os dados de atracação processados (Parquet) do diretório especificado.
3. Utiliza o método `write.jdbc` para carregar os dados na tabela SQL, utilizando o modo `append`.
4. Encerra o SparkSession e imprime uma mensagem de sucesso.

---

### carregar_carga_sql

**Descrição**:  
Lê os dados processados de `carga_fato` (em formato Parquet) e os carrega na tabela do SQL Server.

**Argumentos**:
- `connection_properties` (dict): Propriedades de conexão (usuário, senha, driver, etc.).
- `jdbc_url` (str): URL JDBC de conexão com o SQL Server.
- `input_path` (str, padrão: `"datalake/processed/carga"`): Caminho do diretório contendo os dados processados.
- `table_name` (str, padrão: `"carga_fato"`): Nome da tabela de destino no SQL Server.

**Processo**:
1. Cria um SparkSession configurado com o conector JDBC para SQL Server.
2. Lê os dados de carga processados (Parquet) do diretório especificado.
3. Utiliza o método `write.jdbc` para carregar os dados na tabela SQL, utilizando o modo `append`.
4. Encerra o SparkSession e imprime uma mensagem de sucesso.

---

## Configuração e Execução

1. **Definir Diretórios de Entrada**:  
   Os dados processados devem estar localizados nos seguintes diretórios:
   - Atracação: `datalake/processed/atracacao`
   - Carga: `datalake/processed/carga`

2. **Configurar a Conexão JDBC**:  
   No script, a URL JDBC e as propriedades de conexão estão configuradas, por exemplo:
   ```python
   jdbc_url = "jdbc:sqlserver://sqlserver:1433;databaseName=antaq_db;encrypt=true;trustServerCertificate=true"
   connection_properties = {
       "user": "guilhermesilva18",
       "password": "AntaqFiec@2025",
       "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
   }

3. **Execução**  
   Execute o script via linha de comando:
   ```bash
   python src/etl/data_loading.py
   ```
