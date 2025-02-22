import time
import requests
import os
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select
from urllib.parse import urljoin

driver = webdriver.Chrome()

try:
    driver.get("https://web3.antaq.gov.br/ea/sense/download.html#pt")

    # Lista de anos que você quer baixar
    anos = [2020, 2021, 2022, 2023, 2024]

    # Cria pasta local para armazenar os downloads
    os.makedirs("downloads_antaq", exist_ok=True)

    for ano in anos:
        # Encontra o elemento select para escolher o ano
        # (Você precisa inspecionar o HTML para descobrir o id ou name do dropdown)
        select_element = driver.find_element(By.ID, "anotxt")
        
        # Seleciona o ano desejado
        select_ano = Select(select_element)

        select_ano.select_by_value(str(ano))  
        # Aguarda alguns segundos para a página recarregar e atualizar a lista de arquivos
        time.sleep(10)

        # Agora localiza a tabela com os links de download
        # Supondo que cada link "Clique aqui" seja um <a> dentro de uma tabela
        links = driver.find_elements(By.XPATH, "//table//a[contains(text(), 'Clique aqui')]")
        
        # Extrai o atributo href de cada link
        for link_element in links:
            url_download = link_element.get_attribute("href")
            # Nome do arquivo baseando-se na URL ou no texto
            nome_arquivo = url_download.split("/")[-1]
            destino = os.path.join("downloads_antaq", f"{nome_arquivo}")

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
        