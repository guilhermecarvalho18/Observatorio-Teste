# Prova Prática – Especialista em Engenharia de Dados

## Visão Geral

O projeto consiste na criação de um pipeline ETL que extrai os dados do Anuário Estatístico da ANTAQ, transforma os dados para atender aos requisitos de duas tabelas (**atracacao_fato** e **carga_fato**) e os carrega tanto em um data lake quanto no SQL Server. Além disso, o processo é automatizado utilizando Apache Airflow orquestrado em Docker, e uma consulta SQL otimizada é criada para que os economistas possam usar o Excel para análise.

---

## 1. Autoavaliação de Habilidades

Avalio meu nível de domínio nas técnicas e ferramentas solicitadas da seguinte forma:

- **Ferramentas de visualização de dados (Power BI, Qlik Sense e outros):** 6 – Conheço e já utilizei para análises e dashboards.
- **Manipulação e tratamento de dados com Python:** 6 – Domino a linguagem, com experiência em diversos projetos.
- **Manipulação e tratamento de dados com PySpark:** 6 – Já desenvolvi vários pipelines ETL utilizando PySpark.
- **Desenvolvimento de data workflows em Ambiente Azure com Databricks:** 4 – Tenho conhecimento teórico e alguma experiência prática.
- **Desenvolvimento de data workflows com Airflow:** 4 – Tenho conhecimento teórico e alguma experiência prática com DAGs.
- **Manipulação de bases de dados NoSQL:** 3 – Conheço e utilizei MongoDB em projetos pequenos, embora com menos profundidade que SQL.
- **Web crawling e web scraping para mineração de dados:** 5 – Já implementei scripts utilizando Selenium e requests.
- **Construção de APIs: REST, SOAP e Microservices:** 5 – Conheço os padrões e já desenvolvi APIs RESTful, mas tenho menos experiência com SOAP.

---

## 2. Desenvolvimento do Pipeline ETL

### a) Estrutura de Armazenamento dos Dados

**Orientação:**

- **Data Lake:**  
  Recomendo armazenar os dados brutos e processados em um data lake, pois ele permite guardar dados em seu formato original (por exemplo, arquivos CSV, JSON, ZIP) de forma escalável e com baixo custo.

- **SQL Server:**  
  Para disponibilizar os dados transformados para análise e relatórios (como, por exemplo, para a ferramenta Power BI do cliente), os dados processados são carregados em tabelas no SQL Server.

- **NoSQL (MongoDB):**  
  Embora possa ser utilizado para armazenar dados sem uma estrutura rígida, para este projeto a escolha foi:
  - **Data Lake:** para os dados brutos.
  - **SQL Server:** para os dados estruturados.

---

## 3. Documentação e Scripts

Todos os scripts desenvolvidos para o pipeline ETL, incluindo os scripts de extração, transformação e carregamento (ETL), estão devidamente documentados. Detalhes específicos e instruções de uso encontram-se na pasta `docs` do repositório. Nessa pasta há documentos que descrevem:

- **Script de Web Scraping e Extração:**  
  Detalha a função de baixar os arquivos do site da ANTAQ e a lógica de parsing dos anos.

- **Script de Descompactação:**  
  Explica como os arquivos ZIP são descompactados para a estrutura de diretórios do data lake.

- **Script de Transformação (PySpark):**  
  Documenta a lógica de transformação dos dados para as tabelas `atracacao_fato` e `carga_fato`, incluindo conversão de datas, filtragem e renomeação de colunas.

- **Script de Carregamento:**  
  Detalha como os dados processados são carregados no SQL Server via JDBC.

- **Configuração do Ambiente Docker + Airflow:**  
  Explica a configuração do `docker-compose.yaml` para orquestração dos serviços (SQL Server, Airflow e job Spark ETL) e como os containers se comunicam na mesma rede.

- **Consulta SQL Otimizada:**  
  Inclui a query otimizada para os economistas, com dados agregados por localidade, mês e ano, com métricas de atracações e tempos.

---

## 4. Execução do Ambiente e Acesso ao SQL Server

### Executando o Ambiente com Docker Compose

1. **Suba os Containers:**

   No diretório do projeto, execute:
   ```bash
   docker-compose up --build
   ```
    Isso levantará os serviços do SQL Server, Airflow e o job Spark ETL, todos na mesma rede, permitindo que o Spark se conecte ao SQL Server usando o hostname `sqlserver`.

2. **Monitorando o Pipeline pelo Airflow:**

    Acesse a interface web do Airflow em [http://localhost:8080](http://localhost:8080) para monitorar a execução das DAGs e verificar o status das tasks.

**Observação:**

Devido a problemas encontrados durante o desenvolvimento, nem todas as funcionalidades planejadas foram concluídas integralmente. No entanto, a solução apresentada demonstra a abordagem adotada, o conhecimento e a criatividade na implementação do pipeline ETL e na integração com as ferramentas de orquestração e containerização. Detalhes e instruções adicionais podem ser encontrados na pasta `docs` deste repositório.
