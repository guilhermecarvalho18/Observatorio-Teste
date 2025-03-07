from pyspark.sql import SparkSession

def carregar_atracacao_sql(connection_properties, jdbc_url, input_path = "datalake/processed/atracacao", table_name = "atracacao_fato"):
    """
    Lê os dados processados de atracacao_fato (formato Parquet)
    e os carrega na tabela do SQL Server utilizando o conector JDBC.
    
    Args:
        input_path (str): Caminho do diretório com os dados processados.
        jdbc_url (str): URL JDBC de conexão com o SQL Server.
        table_name (str): Nome da tabela de destino no SQL Server.
        connection_properties (dict): Propriedades de conexão (usuário, senha, driver, etc.).
    """
    spark = SparkSession.builder.appName("LoadAtracacao") \
        .config("spark.jars.packages", "com.microsoft.sqlserver:mssql-jdbc:11.2.3.jre17") \
        .getOrCreate()

    df = spark.read.parquet(input_path)
    df.write.jdbc(url=jdbc_url, table=table_name, mode="append", properties=connection_properties)
    spark.stop()
    print(f"Dados de atracacao_fato carregados na tabela {table_name}.")


def carregar_carga_sql(connection_properties, jdbc_url, input_path = "datalake/processed/carga", table_name = "carga_fato"):
    """
    Lê os dados processados de carga_fato (formato Parquet)
    e os carrega na tabela do SQL Server utilizando o conector JDBC.
    
    Args:
        input_path (str): Caminho do diretório com os dados processados.
        jdbc_url (str): URL JDBC de conexão com o SQL Server.
        table_name (str): Nome da tabela de destino no SQL Server.
        connection_properties (dict): Propriedades de conexão (usuário, senha, driver, etc.).
    """
    spark = SparkSession.builder.appName("LoadCarga") \
        .config("spark.jars.packages", "com.microsoft.sqlserver:mssql-jdbc:11.2.3.jre17") \
        .getOrCreate()
    df = spark.read.parquet(input_path)
    df.write.jdbc(url=jdbc_url, table=table_name, mode="append", properties=connection_properties)
    spark.stop()
    print(f"Dados de carga_fato carregados na tabela {table_name}.")


if __name__ == "__main__":
    # Defina os diretórios de entrada (dados processados) para cada tabela.
    input_path_atracacao = "datalake/processed/atracacao"  
    input_path_carga = "datalake/processed/carga"          

    # URL de conexão JDBC para o SQL Server.
    jdbc_url = "jdbc:sqlserver://sqlserver:1433;databaseName=antaq_db;encrypt=true;trustServerCertificate=true"


    # Propriedades de conexão.
    connection_properties = {
        "user": "guilhermesilva18",
        "password": "AntaqFiec@2025",
        "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    }

    # Carrega os dados de atracacao_fato no SQL Server.
    print("Carregando dados de atracacao_fato...")
    carregar_atracacao_sql(connection_properties, jdbc_url)

    # Carrega os dados de carga_fato no SQL Server.
    print("Carregando dados de carga_fato...")
    carregar_carga_sql(connection_properties, jdbc_url)

    print("Processo de loading concluído com sucesso.")
