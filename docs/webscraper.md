# Web Scraper para Download de Arquivos da ANTAQ

## Visão Geral

Este script automatiza o download dos arquivos disponíveis na página de download do anuário da ANTAQ. Ele permite que o usuário informe um ano ou uma lista de anos, acessa a página utilizando Selenium para selecionar o ano desejado, extrai os links de download (usando XPath para localizar os elementos que contenham o texto "Clique aqui") e, por fim, baixa os arquivos via `requests`.

Os arquivos baixados são armazenados em uma estrutura de diretórios que simula um data lake local, com as pastas:
- **datalake/raw**: para os dados brutos extraídos.
- **datalake/processed**: para os dados transformados (não utilizado diretamente nesse script, mas parte do fluxo ETL).
- **datalake/logs**: para armazenar logs (ainda a ser implementado, se necessário).

## Estrutura do Código

O script está organizado em duas partes principais:

1. **Função `parse_anos`**  
   Responsável por converter o argumento fornecido pelo usuário (via linha de comando) em uma lista de inteiros.  
   - Aceita um valor único (ex.: `"2023"`) ou uma lista no formato (ex.: `"[2021,2022,2023]"`).
   - Utiliza `ast.literal_eval` para interpretar o valor e valida se todos os elementos são inteiros.
   
2. **Função `baixar_arquivos_por_ano`**  
   Essa função utiliza o Selenium para:
   - Abrir o navegador (Chrome) e acessar a página de download da ANTAQ.
   - Encontrar o elemento `<select>` pelo `ID="anotxt"` e selecionar o ano desejado.
   - Aguardar alguns segundos para que a página atualize a lista de arquivos.
   - Localizar os links de download (elementos `<a>` que contenham o texto "Clique aqui").
   - Para cada link, extrair a URL, construir o nome do arquivo e fazer o download usando a biblioteca `requests`.
   - Salvar os arquivos na pasta `datalake/raw`.

## Requisitos

- **Python 3.12**
- **Selenium**: para interação automatizada com a página.
- **Requests**: para realizar o download dos arquivos.
- **ChromeDriver**: O driver para o navegador Chrome deve estar instalado e configurado no PATH.

### Instalação das Dependências

Execute o seguinte comando para instalar as bibliotecas necessárias:

```bash
pip install selenium 
