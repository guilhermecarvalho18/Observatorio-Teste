# Web Scraper para Download de Arquivos da ANTAQ

Este script acessa a página de download do site da ANTAQ e baixa os arquivos correspondentes aos anos informados. Ele utiliza Selenium para automatizar a navegação e a seleção dos anos, e a biblioteca `requests` para realizar o download dos arquivos.

---
## Sumário

- [Visão Geral](#visão-geral)
- [Requisitos](#requisitos)
- [Funções Principais](#funções-principais)
  - [parse_anos](#parse_anos)
  - [baixar_arquivos_por_ano](#baixar_arquivos_por_ano)
- [Uso e Execução](#uso-e-execução)
- [Estrutura de Diretórios](#estrutura-de-diretórios)
---

## Visão Geral

O script tem como objetivo automatizar o processo de download dos arquivos disponibilizados na página da ANTAQ, de acordo com os anos informados pelo usuário. Ele realiza as seguintes etapas:
1. Inicia o navegador Chrome utilizando Selenium.
2. Acessa a página de download da ANTAQ.
3. Para cada ano informado:
   - Seleciona o ano desejado no dropdown (identificado pelo ID `anotxt`).
   - Aguarda a atualização da página.
   - Localiza todos os links de download (elementos contendo o texto "Clique aqui").
   - Faz o download de cada arquivo encontrado, salvando-o na pasta apropriada com um nome que inclua o ano.
4. Encerra o navegador ao final do processo.

---

## Requisitos

- **Python 3.x**
- **Selenium**: Utilizado para a automação do navegador.
- **Requests**: Utilizado para efetuar o download dos arquivos.
- **Chromedriver**: Certifique-se de ter o Chromedriver compatível com a versão do Google Chrome instalada.
- **Argparse** e **ast**: Bibliotecas padrão para o parsing de argumentos e avaliação segura de strings.

---

## Funções Principais

### parse_anos

**Descrição**:  
Converte o argumento fornecido para o parâmetro de anos em uma lista de inteiros. Aceita tanto um único número (ex.: `2023`) quanto uma lista de anos no formato `[2021, 2022, 2023]`.

**Retorno**:  
Uma lista de inteiros.

**Exceções**:  
Levanta um `argparse.ArgumentTypeError` se o argumento não puder ser convertido em inteiro ou se a lista contiver elementos que não sejam inteiros.

**Exemplo**:
- Entrada: `"2023"` → Saída: `[2023]`
- Entrada: `"[2021,2022,2023]"` → Saída: `[2021, 2022, 2023]`

---

### baixar_arquivos_por_ano

**Descrição**:  
Acessa a página de download da ANTAQ e baixa os arquivos correspondentes aos anos especificados.  
Para cada ano informado, a função:
- Seleciona o ano no dropdown da página.
- Aguarda a atualização da página (utilizando `time.sleep(10)`).
- Localiza todos os links de download na tabela (elementos que contêm o texto "Clique aqui").
- Extrai a URL de cada link e realiza o download utilizando a biblioteca `requests`.
- Salva os arquivos na pasta `datalake/raw`, mantendo os nomes originais extraídos da URL.

**Parâmetros**:
- `anos` (list): Lista de anos (inteiros) para os quais os arquivos serão baixados.

**Processo**:
1. Inicia o driver do Chrome.
2. Cria os diretórios necessários:
   - `datalake/raw/zipped`
   - `datalake/raw/unzipped`
   - `datalake/processed/atracacao`
   - `datalake/processed/carga`
   - `datalake/logs`
3. Para cada ano:
   - Seleciona o valor no dropdown com ID `anotxt`.
   - Aguarda 10 segundos para a página atualizar.
   - Localiza todos os links contendo o texto "Clique aqui".
   - Para cada link, extrai o atributo `href`, determina o nome do arquivo e realiza o download.
4. Finaliza o navegador após o processamento.

---

## Uso e Execução

Para executar o script, abra um terminal e execute o seguinte comando, passando o argumento `--anos` (ou `-a`) com um único ano ou uma lista de anos:

```bash
# Exemplo com um único ano:
python src/etl/data_transform.py --anos 2023
```
```bash
# Exemplo com uma lista de anos:
python src/etl/data_transform.py --anos [2021,2022,2023]
```

## Estrutura de Diretórios

O script cria e utiliza a seguinte estrutura de diretórios:

- **datalake/raw/**
  - **zipped/**: Pasta para armazenar os arquivos compactados baixados (se aplicável).
  - **unzipped/**: Pasta para armazenar os arquivos baixados em formato descompactado.

- **datalake/processed/**
  - **atracacao/**: Diretório onde os arquivos de atracação processados serão salvos.
  - **carga/**: Diretório onde os arquivos de carga processados serão salvos.

- **datalake/logs/**: Diretório para armazenar logs, se necessário.

> Caso os diretórios não existam, o script os criará automaticamente utilizando `os.makedirs` com a opção `exist_ok=True`.

