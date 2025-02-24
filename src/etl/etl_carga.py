import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from src.scraping.web_scraping import parse_anos, baixar_arquivos_por_ano
from src.etl.data_extract_decompress import decompress_zip_files
from src.etl.data_transform import transformar_carga_fato
from src.etl.data_loading import carregar_carga_sql

def run_etl():
    # Defina os diretórios:
    # Diretório onde os arquivos ZIP baixados estão localizados
    raw_zipped_dir = "datalake/raw/zipped"
    # Diretório onde os arquivos serão descompactados
    raw_unzipped_dir = "datalake/raw/unzipped"
    # Diretórios de saída dos dados processados
    output_atracacao = "datalake/processed/atracacao"
    output_carga = "datalake/processed/carga"
    
    # Opcional: Baixar os arquivos via web scraping, se necessário
    # Exemplo: baixar arquivos para os anos 2021, 2022 e 2023
    # Uncomment se desejar executar essa etapa:
    # anos = [2021, 2022, 2023]
    # baixar_arquivos_por_ano(anos)
    
    # 1. Descompactação
    print("Descompactando arquivos ZIP...")
    decompress_zip_files(source_dir=raw_zipped_dir, dest_dir=raw_unzipped_dir)
    
    # 2. Transformação da tabela carga_fato
    print("Transformando dados de carga_fato...")
    transformar_carga_fato(input_dir=raw_unzipped_dir, input_atrac_dir=output_atracacao, output_dir=output_carga)
    
    # 3. Carregamento dos Dados no SQL Server
    jdbc_url = "jdbc:sqlserver://sqlserver:1433;databaseName=antaq_db"
    connection_properties = {
        "user": "guilhermesilva18",
        "password": "AntaqFiec@2025",
        "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    }
    
    print("Carregando dados de carga_fato no SQL Server...")
    carregar_carga_sql(jdbc_url=jdbc_url, input_path=output_carga, table_name="carga_fato", connection_properties=connection_properties)
    
    print("Processo de ETL concluído com sucesso.")

if __name__ == "__main__":
    run_etl()

