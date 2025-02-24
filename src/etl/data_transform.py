from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, year, month, when, lit, to_timestamp, date_format, unix_timestamp, round


def transformar_atracacao_fato(input_dir, output_dir):
    """
    Lê arquivos CSV de atracação, filtra e transforma 
    para o formato atracacao_fato.
    
    Args:
        input_dir (str): Caminho da pasta onde estão os CSVs descompactados (ex.: datalake/raw/unzipped).
        output_dir (str): Caminho onde serão salvos os dados processados (ex.: datalake/processed/atracacao_fato).
    """
    spark = SparkSession.builder.appName("TransformAtracacao").getOrCreate()
    
    # Lê todos os CSVs de atracação (ex.: atracacao_2021.csv, atracacao_2022.csv, etc.)
    # Ajuste o padrão de arquivo conforme sua convenção de nome.
    df = spark.read.option("sep", ";").csv(f"{input_dir}/*Atracacao.txt", header=True, inferSchema=True)
    
    # Filtra somente os anos 2021 a 2023. Supondo que exista uma coluna "Ano" ou similar.
    df = df.filter((col("Ano") >= 2021) & (col("Ano") <= 2023))
    
    # Conversão de datas 
    df = df.withColumn("data_atracacao", date_format(to_timestamp("Data Atracação", "dd/MM/yyyy HH:mm:ss"), "dd-MM-yyyy HH:mm:ss"))
    df = df.withColumn("data_chegada", date_format(to_timestamp("Data Chegada", "dd/MM/yyyy HH:mm:ss"), "dd-MM-yyyy HH:mm:ss"))
    df = df.withColumn("data_desatracacao", date_format(to_timestamp("Data Desatracação", "dd/MM/yyyy HH:mm:ss"), "dd-MM-yyyy HH:mm:ss"))
    df = df.withColumn("data_inicio_operacao", date_format(to_timestamp("Data Início Operação", "dd/MM/yyyy HH:mm:ss"), "dd-MM-yyyy HH:mm:ss"))
    df = df.withColumn("data_termino_operacao", date_format(to_timestamp("Data Término Operação", "dd/MM/yyyy HH:mm:ss"), "dd-MM-yyyy HH:mm:ss"))
    
    # Cria colunas de Ano e Mês da data de início da operação
    df = df.withColumn("ano_data_inicio_operacao", year(to_timestamp("Data Início Operação", "dd/MM/yyyy HH:mm:ss")))
    df = df.withColumn("mes_data_inicio_operacao", month(to_timestamp("Data Início Operação", "dd/MM/yyyy HH:mm:ss")))
    
    # Renomeia colunas
    df = df.withColumnRenamed("IDAtracacao", "id_atracacao") \
                            .withColumnRenamed("CDTUP", "cdtup") \
                            .withColumnRenamed("IDBerco", "id_berco") \
                            .withColumnRenamed("Berço", "berco") \
                            .withColumnRenamed("Porto Atracação", "porto_atracacao") \
                            .withColumnRenamed("Apelido Instalação Portuária", "apelido_instalacao_portuaria") \
                            .withColumnRenamed("Complexo Portuário", "complexo_portuario") \
                            .withColumnRenamed("Tipo da Autoridade Portuária", "tipo_autoridade_portuaria") \
                            .withColumnRenamed("Tipo de Operação", "tipo_operacao") \
                            .withColumnRenamed("Tipo de Navegação da Atracação", "tipo_navegacao_atracao") \
                            .withColumnRenamed("Nacionalidade do Armador", "nacionalidade_armador") \
                            .withColumnRenamed("FlagMCOperacaoAtracacao", "flag_mc_operacao_atracacao") \
                            .withColumnRenamed("Terminal", "terminal") \
                            .withColumnRenamed("Município", "municipio") \
                            .withColumnRenamed("UF", "uf") \
                            .withColumnRenamed("SGUF", "sguf") \
                            .withColumnRenamed("Região Geográfica", "regiao_geografica") \
                            .withColumnRenamed("Nº da Capitania", "numero_capitania") \
                            .withColumnRenamed("Nº do IMO", "numero_imo") 


    df = df.withColumn("tempo_espera_atracacao", 
                            round((unix_timestamp(col("data_atracacao"), "dd-MM-yyyy HH:mm:ss") - unix_timestamp(col("data_chegada"), "dd-MM-yyyy HH:mm:ss")) / 3600.0, 2)) \
                            .withColumn("tempo_espera_inicio_operacao", 
                            round((unix_timestamp(col("data_inicio_operacao"), "dd-MM-yyyy HH:mm:ss") - unix_timestamp(col("data_atracacao"), "dd-MM-yyyy HH:mm:ss")) / 3600.0, 2)) \
                            .withColumn("tempo_operacao", 
                            round((unix_timestamp(col("data_termino_operacao"), "dd-MM-yyyy HH:mm:ss") - unix_timestamp(col("data_inicio_operacao"), "dd-MM-yyyy HH:mm:ss")) / 3600.0, 2)) \
                            .withColumn("tempo_espera_desatracacao", 
                            round((unix_timestamp(col("data_desatracacao"), "dd-MM-yyyy HH:mm:ss") - unix_timestamp(col("data_termino_operacao"), "dd-MM-yyyy HH:mm:ss")) / 3600.0, 2)) 
    
    df = df.withColumn("tempo_atracado",
                         round(col("tempo_espera_inicio_operacao") + col("tempo_operacao") + col("tempo_espera_desatracacao"), 2))   
    
    df = df.withColumn("tempo_estadia",
                         round(col("tempo_espera_atracacao") + col("tempo_espera_inicio_operacao") + col("tempo_operacao") + col("tempo_espera_desatracacao"), 2))   
    
    # Seleciona somente as colunas relevantes para a tabela atracacao_fato
    df = df.select(
            col("id_atracacao"),
            col("cdtup"),
            col("id_berco"),
            col("berco"),
            col("porto_atracacao"),
            col("apelido_instalacao_portuaria"),
            col("complexo_portuario"),
            col("tipo_autoridade_portuaria"),
            col("data_atracacao"),
            col("data_chegada"),
            col("data_desatracacao"),
            col("data_inicio_operacao"),
            col("data_termino_operacao"),
            col("ano_data_inicio_operacao"),
            col("mes_data_inicio_operacao"),
            col("tipo_operacao"),
            col("tipo_navegacao_atracao"),
            col("nacionalidade_armador"),
            col("flag_mc_operacao_atracacao"),
            col("terminal"),
            col("municipio"),
            col("uf"),
            col("sguf"),
            col("regiao_geografica"),
            col("numero_capitania"),
            col("numero_imo"),     
            col("tempo_espera_atracacao"),
            col("tempo_espera_inicio_operacao"),
            col("tempo_operacao"),
            col("tempo_espera_desatracacao"),
            col("tempo_atracado"),
            col("tempo_estadia")
    )
    
    # Salva em formato Parquet
    df.write.mode("overwrite").parquet(output_dir)
    
    spark.stop()
    print(f"Atracacao_fato processado e salvo em {output_dir}")



def transformar_carga_fato(input_dir, input_sec_dir, output_dir):
    """
    Lê arquivos CSV de carga, filtra e transforma 
    para o formato carga_fato.
    
    Args:
        input_dir (str): Caminho da pasta onde estão os CSVs descompactados.
        output_dir (str): Caminho onde serão salvos os dados processados (ex.: datalake/processed/carga_fato).
    """
    spark = SparkSession.builder.appName("TransformCarga").getOrCreate()
    
    # Lê todos os CSVs de carga (ex.: carga_2021.csv, carga_2022.csv, etc.)
    df = spark.read.option("sep", ";").csv(f"{input_dir}/*Carga.txt", header=True, inferSchema=True)
    df_atracacao = spark.read.parquet(f"{input_sec_dir}/atracacao")

    # Filtra somente os anos 2021 a 2023 (ajuste conforme o seu layout)
    df_atracacao_alias = df_atracacao.alias("a").select("IDAtracacao")
    df_carga_alias = df.alias("c")
    
    df_join = df_carga_alias.join(
        df_atracacao_alias,
        on=[col("c.IDAtracacao") == col("a.IDAtracacao")],
        how="inner"
        )
    
    df_carga = df_join.select("c.*")

    # Exemplo de renomear colunas e criar colunas extras
    df = df.withColumnRenamed("IDCarga", "id_carga") \
           .withColumnRenamed("IDAtracacao", "id_atracacao") \
           .withColumnRenamed("Origem", "origem") \
           .withColumnRenamed("Destino", "destino") \
           .withColumnRenamed("CDMercadoria", "cd_mercadoria") \
           .withColumnRenamed("STSH2", "stsh2") \
           .withColumnRenamed("Tipo Operacao da Carga", "tipo_operacao_carga") \
           # etc...
    
    # Exemplo de colunas derivadas (ano e mês da data de início da operação da atracação)
    # Se essa info estiver em outra tabela, você pode precisar de join. Ajuste conforme a necessidade.
    
    # Seleciona as colunas relevantes para carga_fato
    df = df.select(
        col("id_carga"),
        col("id_atracacao"),
        col("origem"),
        col("destino"),
        col("cd_mercadoria"),
        col("stsh2"),
        col("tipo_operacao_carga"),
        # ... e demais colunas do PDF ...
    )
    
    # Salva em formato Parquet
    df.write.mode("overwrite").parquet(output_dir)
    
    spark.stop()
    print(f"Carga_fato processado e salvo em {output_dir}")

if __name__ == "__main__":
    input_atrac = "datalake/raw/unzipped"
    output_atrac = "datalake/processed/atracacao"
    transformar_atracacao_fato(input_atrac, output_atrac)

    input_carga = "datalake/raw/unzipped"
    output_carga = "datalake/processed/carga"
    input_atrac_dir = "datalake/processed/atracacao" 
    transformar_carga_fato(input_atrac, input_atrac_dir, output_atrac)