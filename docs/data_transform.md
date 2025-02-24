# Documentação do Script de Transformação de Dados

Este script Python utiliza Apache Spark para transformar dados brutos (em formato CSV) de operações portuárias (atracação e carga) e gerar datasets processados em formato Parquet. Ele contém duas funções principais: uma para processar os dados de atracação e outra para processar os dados de carga, unindo informações de containerização e dados de atracação.

---

## Sumário

- [Visão Geral](#visão-geral)
- [Requisitos e Configuração do Ambiente](#requisitos-e-configuração-do-ambiente)
- [Função: transformar_atracacao_fato](#função-transformar_atracacao_fato)
  - [Descrição](#descrição)
  - [Argumentos](#argumentos)
  - [Fluxo de Processamento](#fluxo-de-processamento-atracacao)
- [Função: transformar_carga_fato](#função-transformar_carga_fato)
  - [Descrição](#descrição-1)
  - [Argumentos](#argumentos-1)
  - [Fluxo de Processamento](#fluxo-de-processamento-carga)
- [Execução do Script](#execução-do-script)
- [Considerações de Memória e Performance](#considerações-de-memória-e-performance)
- [Logs e Avisos](#logs-e-avisos)


---

## Visão Geral

O script tem como objetivo:
- **Transformar dados de atracação**: Leitura de arquivos CSV, filtragem por ano (2021 a 2023), conversão e formatação de datas, extração de informações (como ano e mês da operação) e unificação dos dados de atracação e dos tempos de atracação.
- **Transformar dados de carga**: Leitura dos arquivos CSV de carga, leitura dos dados de atracação já processados (em formato Parquet), e dos dados de carga conteinerizada. Realiza joins para associar os dados de carga às informações de atracação e containerização, e cria colunas derivadas para definir o peso líquido da carga e ajustar o código da mercadoria conforme a condição de containerização.

---

## Requisitos e Configuração do Ambiente

- **Apache Spark**: O script utiliza o Spark para processamento distribuído dos dados. Certifique-se de que o Spark está instalado e configurado.
- **Python 3.x**: O script é escrito em Python.
- **Configurações de Memória**:  
  - A função `transformar_carga_fato` utiliza configurações para aumentar a memória do driver e dos executores:
    ```python
    .config("spark.driver.memory", "8g")
    .config("spark.executor.memory", "8g")
    ```
  - Ajuste esses valores conforme a capacidade do seu ambiente ou cluster.
- **Formato dos Dados**:  
  - Arquivos de atracação:  
    - Padrão: `[0-9][0-9][0-9][0-9]Atracacao.txt`
    - Tempos: `[0-9][0-9][0-9][0-9]TemposAtracacao.txt`
  - Arquivos de carga:  
    - Padrão: `[0-9][0-9][0-9][0-9]Carga.txt`
    - Carga conteinerizada: `[0-9][0-9][0-9][0-9]Carga_Conteinerizada.txt`

---

## Função: transformar_atracacao_fato

### Descrição

Esta função lê os arquivos CSV de atracação e de tempos de atracação, filtra os dados para os anos de 2021 a 2023, converte e formata as datas para o padrão `"dd-MM-yyyy HH:mm:ss"`, extrai informações de ano e mês da data de início da operação e realiza um join (usando um left join) para combinar as informações de tempos de atracação com os dados principais de atracação. Em seguida, renomeia as colunas para um padrão consistente e salva o resultado em formato Parquet.

### Argumentos

- `input_dir` (str, padrão: `"datalake/raw/unzipped"`): Diretório contendo os arquivos CSV brutos.
- `output_dir` (str, padrão: `"datalake/processed/atracacao"`): Diretório onde o dataset processado (atracacao_fato) será salvo.

### Fluxo de Processamento (Atracação)

1. **Leitura dos Arquivos**:  
   Utiliza glob patterns para ler:
   - Arquivos de atracação: `[0-9][0-9][0-9][0-9]Atracacao.txt`
   - Arquivos de tempos: `[0-9][0-9][0-9][0-9]TemposAtracacao.txt`
2. **Filtragem**:  
   Filtra os registros com ano entre 2021 e 2023 (supondo a existência de uma coluna `Ano`).
3. **Conversão de Datas**:  
   Converte as colunas de data (por exemplo, `"Data Atracação"`) para o formato `"dd-MM-yyyy HH:mm:ss"`.
4. **Extração de Ano e Mês**:  
   Cria colunas derivadas: `ano_data_inicio_operacao` e `mes_data_inicio_operacao` a partir da coluna `"Data Início Operação"`.
5. **Join com Tempos**:  
   Une os DataFrames de atracação e tempos de atracação utilizando um left join sobre a chave `IDAtracacao`.
6. **Renomeação e Seleção de Colunas**:  
   Renomeia as colunas para nomes padronizados (por exemplo, `"IDAtracacao"` para `"id_atracacao"`) e seleciona apenas as colunas relevantes.
7. **Gravação**:  
   Salva o DataFrame resultante no diretório de saída em formato Parquet.

### Exemplo de Uso (Atracação)

```bash
python src/etl/data_transform.py
```
--- 
## Função: transformar_carga_fato

### Descrição
Esta função processa os arquivos CSV de carga e de carga conteinerizada, integrando-os com os dados de atracação previamente processados para criar o dataset final denominado `carga_fato`. Durante o processamento, a função realiza as seguintes operações:
- Leitura dos arquivos de carga e de carga conteinerizada do diretório de dados brutos.
- Leitura do dataset de atracação (gerado pela função `transformar_atracacao_fato`) a partir do diretório de entrada.
- Normalização e preparação dos DataFrames (por exemplo, renomeando e convertendo as chaves de join).
- Realização de um join entre o DataFrame de carga e o de atracação para agregar informações adicionais (como ano e mês da operação, porto de atracação, etc.).
- Realização de um segundo join com os dados de carga conteinerizada, para identificar quais cargas foram conteinerizadas.
- Criação de colunas derivadas que definem:
  - **Peso líquido da carga**: se a carga for conteinerizada (identificada pelo campo `ConteinerEstado` não nulo e diferente de "Vazio"), o peso líquido será determinado pelo valor em `VLPesoCargaConteinerizada`; caso contrário, será utilizado `VLPesoCargaBruta`.
  - **Código da mercadoria**: para cargas conteinerizadas, o valor de `CDMercadoria` será substituído pelo de `CDMercadoriaConteinerizada`.
- Renomeação e seleção das colunas finais, de modo a padronizar a estrutura do dataset.
- Escrita do resultado final em formato Parquet, com configuração para otimização do tamanho dos blocos.

### Argumentos
- `input_dir` (str, padrão: `"datalake/raw/unzipped"`): Diretório que contém os arquivos CSV brutos de carga e de carga conteinerizada.
- `input_atrac_dir` (str, padrão: `"datalake/processed/atracacao"`): Diretório onde está salvo o dataset `atracacao_fato` (em formato Parquet), que será utilizado para integrar informações da atracação.
- `output_dir` (str, padrão: `"datalake/processed/carga"`): Diretório onde o dataset processado `carga_fato` será salvo em formato Parquet.

### Fluxo de Processamento (Carga)
1. **Leitura dos Arquivos**  
   - O DataFrame de carga é lido a partir dos arquivos que seguem o padrão `[0-9][0-9][0-9][0-9]Carga.txt`.
   - O DataFrame de carga conteinerizada é lido a partir dos arquivos que seguem o padrão `[0-9][0-9][0-9][0-9]Carga_Conteinerizada.txt`.
   - O dataset de atracação, já processado e salvo em Parquet, é lido do diretório definido em `input_atrac_dir`.

2. **Preparação dos DataFrames**  
   - As colunas de chave são normalizadas: por exemplo, a coluna `IDAtracacao` é convertida para `id_atracacao` (e seu tipo, se necessário) e `IDCarga` para `id_carga`.
   - São criados aliases para facilitar os joins (por exemplo, alias "c" para o DataFrame de carga e alias "a" para o de atracação).

3. **Join com Atracação**  
   - É realizado um left join entre o DataFrame de carga e o dataset de atracação usando a chave `id_atracacao`.  
   - Após o join, são selecionadas colunas adicionais da atracação, como `ano_data_inicio_operacao`, `mes_data_inicio_operacao`, `porto_atracacao` e `sguf`.

4. **Join com Carga Conteinerizada**  
   - O DataFrame resultante é unido (left join) com o DataFrame de carga conteinerizada (alias "cc") usando a chave `id_carga`.
   
5. **Criação de Colunas Derivadas**  
   - **Peso líquido da carga**:  
     É criada a coluna `peso_liquido_carga` usando uma expressão condicional:
     ```python
     when(
         (col("ConteinerEstado").isNotNull()) & (col("ConteinerEstado") != "Vazio"),
         col("VLPesoCargaConteinerizada")
     ).otherwise(col("VLPesoCargaBruta"))
     ```
   - **Código da mercadoria**:  
     A coluna `cd_mercadoria` é atualizada para, quando a carga for conteinerizada, utilizar o valor de `CDMercadoriaConteinerizada`; caso contrário, manter o valor original de `CDMercadoria`.

6. **Renomeação e Seleção Final de Colunas**  
   - As colunas são renomeadas para um padrão consistente (por exemplo, "Origem" para "origem", "Destino" para "berco", etc.).
   - São selecionadas somente as colunas relevantes para compor o dataset `carga_fato`.

7. **Gravação**  
   - O DataFrame final é escrito em formato Parquet no diretório definido por `output_dir`.
   - A opção `parquet.block.size` é configurada para 134217728 bytes (128 MB) para otimizar a escrita.

---

## Execução do Script

Para executar o script, siga estes passos:
1. **Preparação dos Dados**  
   - Certifique-se de que os arquivos CSV brutos estejam localizados no diretório `datalake/raw/unzipped`.
   - Garanta que o dataset `atracacao_fato` esteja disponível no diretório `datalake/processed/atracacao` (gerado pela função `transformar_atracacao_fato`).
   - Verifique se os diretórios de saída para os datasets processados (`atracacao` e `carga`) estão corretamente especificados.

2. **Execução**  
   Execute o script via linha de comando:
   ```bash
   python src/etl/data_transform.py
   ```
## Considerações de Memória e Performance

- **Alocação de Memória**:  
  Se ocorrerem erros de "Java heap space", aumente as configurações de memória do driver e dos executores (como mostrado na função `transformar_carga_fato`).

- **Particionamento**:  
  Considere reparticionar o DataFrame se estiver lidando com grandes volumes de dados para melhorar a performance e reduzir o consumo de memória.

- **Configurações do Parquet**:  
  A opção `parquet.block.size` está definida para 134217728 bytes (128 MB), mas pode ser ajustada conforme o ambiente.

## Logs e Warnings

Durante a execução, você pode observar mensagens de WARN sobre:

- Resolução de hostname para um endereço de loopback.
- Binding de porta para o SparkUI.
- Mensagens do MemoryManager indicando escalonamento de tamanho de blocos.
- Avisos sobre truncamento da representação do plano de execução.

Esses avisos podem ser ignorados se o processamento estiver ocorrendo corretamente. Contudo, se houver problemas de performance, considere ajustar as configurações conforme necessário.

