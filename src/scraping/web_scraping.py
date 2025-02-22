import time
import requests
import os
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select
import argparse
import ast

def parse_anos(value):
    """
    Converte o argumento de ano para uma lista de inteiros.
    Aceita um único número (ex.: "2023") ou uma lista no formato "[2021, 2022, 2023]".
    """
    try:
        # Tenta avaliar o valor como uma expressão Python
        result = ast.literal_eval(value)
        if isinstance(result, int):
            return [result]
        elif isinstance(result, list):
            # Verifica se todos os elementos são inteiros
            if all(isinstance(item, int) for item in result):
                return result
            else:
                raise argparse.ArgumentTypeError("A lista deve conter apenas inteiros.")
        else:
            raise argparse.ArgumentTypeError("Formato inválido para o argumento ano.")
    except Exception:
        # Se não for possível avaliar, tenta converter para inteiro
        try:
            return [int(value)]
        except Exception:
            raise argparse.ArgumentTypeError("Informe um número inteiro ou uma lista de inteiros.")


def baixar_arquivos_por_ano(anos):

    driver = webdriver.Chrome()
    try:
        driver.get("https://web3.antaq.gov.br/ea/sense/download.html#pt")

        # Cria pasta local para armazenar os downloads
        os.makedirs("datalake/raw", exist_ok=True)
        os.makedirs("datalake/processed", exist_ok=True)
        os.makedirs("datalake/logs", exist_ok=True)

        for ano in anos:
            # Encontra o elemento select para escolher o ano
            select_element = driver.find_element(By.ID, "anotxt")
            
            # Seleciona o ano desejado
            select_ano = Select(select_element)

            select_ano.select_by_value(str(ano))  

            # Aguarda alguns segundos para a página recarregar e atualizar a lista de arquivos
            time.sleep(10)

            # Agora localiza a tabela com os links de download

            links = driver.find_elements(By.XPATH, "//table//a[contains(text(), 'Clique aqui')]")
            
            # Extrai o atributo href de cada link
            for link_element in links:
                url_download = link_element.get_attribute("href")
                # Nome do arquivo baseando-se na URL ou no texto
                nome_arquivo = url_download.split("/")[-1]
                destino = os.path.join("datalake/raw", f"{nome_arquivo}")

                # Faz o download via requests (mais simples para salvar em disco)
                print(f"Baixando {url_download} para {destino}")
                r = requests.get(url_download)
                if r.status_code == 200:
                    with open(destino, "wb") as f:
                        f.write(r.content)
                else:
                    print(f"Falha ao baixar {url_download}")

    finally:
        # Encerra o navegador
        driver.quit()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Baixar arquivos do site da ANTAQ para o ano especificado.")
    parser.add_argument("-a", "--anos", type=parse_anos, required=True,
                        help="Informe um ano ou uma lista de anos, ex.: 2023 ou [2021,2022,2023]")
    args = parser.parse_args()

    baixar_arquivos_por_ano(args.anos)