# Pipeline ETL com Spark, SQL Server e Airflow

Esta documentação descreve o pipeline ETL que integra a execução de jobs Spark (para transformar dados de atracação e carga) com o carregamento dos dados no SQL Server, orquestrado via Airflow e executado em contêineres Docker.

---

## Sumário

- [Visão Geral do Pipeline](#visão-geral-do-pipeline)
- [Arquitetura e Componentes](#arquitetura-e-componentes)
  - [Docker Compose](#docker-compose)
  - [Dockerfile para Spark ETL](#dockerfile-para-spark-etl)
  - [Airflow DAG](#airflow-dag)
- [Execução do Pipeline ETL](#execução-do-pipeline-etl)
- [Considerações Finais](#considerações-finais)

---

## Visão Geral do Pipeline

O pipeline ETL realiza as seguintes etapas:

1. **Transformação com Spark**  
   São executados dois jobs Spark, um para processar os dados de atracação (`atracacao_fato`) e outro para processar os dados de carga (`carga_fato`).  
   - Os jobs transformam os dados brutos (CSV) e os salvam em formato Parquet.  
   - Durante o processamento, os jobs realizam operações de leitura, transformação e escrita, integrando informações de diferentes fontes.

2. **Carregamento no SQL Server**  
   Os dados processados são posteriormente carregados em tabelas do SQL Server utilizando o conector JDBC.

3. **Orquestração via Airflow**  
   A execução dos jobs Spark é orquestrada por um DAG no Airflow, que utiliza o DockerOperator para disparar os jobs em contêineres.

---

## Arquitetura e Componentes

### Docker Compose

O arquivo `docker-compose.yml` define os serviços que compõem a arquitetura do pipeline:

- **sqlserver**:  
  - Utiliza a imagem oficial do SQL Server (2019-latest).  
  - Define variáveis de ambiente (EULA e senha) e expõe a porta 1433.
  - Armazena os dados em um volume `sql_data`.

- **airflow**:  
  - Utiliza a imagem do Apache Airflow (versão 2.5.1).  
  - Depende do serviço `sqlserver` e monta volumes para DAGs, logs, plugins e dados (datalake).  
  - Expõe a porta 8080 e executa o comando `webserver`.

- **spark-etl**:  
  - Constrói uma imagem customizada (definida no Dockerfile) que inclui o Spark e o driver JDBC para SQL Server.  
  - Depende do serviço `sqlserver` e monta o diretório `./datalake` para acesso aos dados.
  - Recebe variáveis de ambiente para configuração de conexão JDBC.

### Dockerfile para Spark ETL

O Dockerfile (baseado na imagem Bitnami Spark) configura o ambiente para a execução dos jobs ETL:

- Utiliza a imagem `bitnami/spark:3`.
- Torna-se usuário root para copiar o driver JDBC (`mssql-jdbc-11.2.3.jre17.jar`) para o diretório `/opt/spark/jars/`.
- Retorna ao usuário não-root (por exemplo, `USER 1001`).
- Define o diretório de trabalho como `/app` e copia os arquivos do projeto para o contêiner.

### Airflow DAG

O DAG do Airflow (definido, por exemplo, em `src/dags/antaq_etl_pipeline.py`) orquestra a execução dos jobs ETL:

- **Definição do DAG**:  
  Configurado com `start_date`, `schedule_interval` (no exemplo, `@monthly`), e parâmetros de retry.
  
- **Tarefas do DAG**:
  - **etl_atracacao**:  
    Utiliza o `DockerOperator` para executar o job Spark de transformação dos dados de atracação.  
    O comando executado é algo como:
    ```bash
    spark-submit --master local --jars /opt/spark/jars/mssql-jdbc-11.2.3.jre17.jar src/etl/etl_atracacao.py
    ```
  - **etl_carga**:  
    Também utiliza o `DockerOperator` para executar o job Spark de transformação dos dados de carga.  
    O comando executado é:
    ```bash
    spark-submit --master local --jars /opt/spark/jars/mssql-jdbc-11.2.3.jre17.jar src/etl/etl_carga.py
    ```

- **Orquestração**:  
  O DAG define que o job de atracação (`etl_atracacao`) deve ser executado antes do job de carga (`etl_carga`).

---

## Execução do Pipeline ETL

Para executar o pipeline:
1. **Suba os Contêineres**  
   Utilize o `docker-compose up -d` para iniciar os serviços (SQL Server, Airflow e Spark ETL).

2. **Verifique a Interface do Airflow**  
   Acesse o Airflow pela porta 8080 para monitorar a execução do DAG `antaq_etl_pipeline`.

3. **Agendamento e Execução**  
   O DAG está configurado para rodar mensalmente, mas pode ser acionado manualmente através da interface do Airflow.

4. **Logs e Monitoramento**  
   Verifique os logs nos diretórios montados (por exemplo, `./logs` para Airflow e `datalake/logs` se configurado) para monitorar a execução do pipeline.

---

## Considerações Finais

- **Configurações de Memória e Performance**:  
  - Se ocorrerem erros de "Java heap space", ajuste as configurações de memória no Spark (por exemplo, aumentando `spark.driver.memory` e `spark.executor.memory`).
  - Considere reparticionar os DataFrames para melhorar a performance com grandes volumes de dados.
  - A opção `parquet.block.size` está definida para 128 MB, mas pode ser ajustada conforme o ambiente.

- **Logs e Avisos**:  
  Durante a execução, é comum visualizar avisos relacionados à resolução de hostname, binding de porta para o SparkUI, escalonamento de blocos de memória pelo MemoryManager e truncamento do plano de execução. Tais avisos podem ser ignorados se o processamento ocorrer corretamente.

- **Orquestração com Airflow**:  
  O Airflow permite monitorar e gerenciar a execução do pipeline, oferecendo recursos para reexecução de tarefas, monitoramento de falhas e alertas por e-mail em caso de problemas.

Este pipeline ETL integra de forma automatizada as etapas de extração, transformação e carregamento dos dados de operações portuárias, garantindo que os dados processados estejam disponíveis tanto para análises locais (via arquivos Parquet) quanto para sistemas de BI e relatórios (via SQL Server).

---
