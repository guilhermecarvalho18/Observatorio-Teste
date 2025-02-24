from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, year, month, when, lit, to_timestamp, date_format, unix_timestamp, round


def transformar_atracacao_fato(input_dir = "datalake/raw/unzipped", output_dir = "datalake/processed/atracacao"):
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
    df_atracacao = spark.read.option("sep", ";").csv(f"{input_dir}/[0-9][0-9][0-9][0-9]Atracacao.txt", header=True, inferSchema=True)
    df_tempos_atracacao = spark.read.option("sep", ";").csv(f"{input_dir}/[0-9][0-9][0-9][0-9]TemposAtracacao.txt", header=True, inferSchema=True)

    # Filtra somente os anos 2021 a 2023. Supondo que exista uma coluna "Ano" ou similar.
    df_atracacao = df_atracacao.filter((col("Ano") >= 2021) & (col("Ano") <= 2023))
    
    # Conversão de datas 
    df_atracacao = df_atracacao.withColumn("data_atracacao", date_format(to_timestamp("Data Atracação", "dd/MM/yyyy HH:mm:ss"), "dd-MM-yyyy HH:mm:ss"))
    df_atracacao = df_atracacao.withColumn("data_chegada", date_format(to_timestamp("Data Chegada", "dd/MM/yyyy HH:mm:ss"), "dd-MM-yyyy HH:mm:ss"))
    df_atracacao = df_atracacao.withColumn("data_desatracacao", date_format(to_timestamp("Data Desatracação", "dd/MM/yyyy HH:mm:ss"), "dd-MM-yyyy HH:mm:ss"))
    df_atracacao = df_atracacao.withColumn("data_inicio_operacao", date_format(to_timestamp("Data Início Operação", "dd/MM/yyyy HH:mm:ss"), "dd-MM-yyyy HH:mm:ss"))
    df_atracacao = df_atracacao.withColumn("data_termino_operacao", date_format(to_timestamp("Data Término Operação", "dd/MM/yyyy HH:mm:ss"), "dd-MM-yyyy HH:mm:ss"))
    
    # Cria colunas de Ano e Mês da data de início da operação
    df_atracacao = df_atracacao.withColumn("ano_data_inicio_operacao", year(to_timestamp("Data Início Operação", "dd/MM/yyyy HH:mm:ss")))
    df_atracacao = df_atracacao.withColumn("mes_data_inicio_operacao", month(to_timestamp("Data Início Operação", "dd/MM/yyyy HH:mm:ss")))
    
    df_atracacao_alias = df_atracacao.alias("a")
    df_tempos_atracacao_alias = df_tempos_atracacao.alias("ta")

    df_join = df_atracacao_alias.join(
        df_tempos_atracacao_alias,
        on=[col("a.IDAtracacao") == col("ta.IDAtracacao")],
        how="left"
        )
    df_join = df_join.select("a.*","ta.TEsperaAtracacao", "ta.TEsperaInicioOp", 
                                "ta.TOperacao", "ta.TEsperaDesatracacao", 
                                "ta.TAtracado", "ta.TEstadia")

    # Renomeia colunas
    df_atracacao = df_join.withColumnRenamed("IDAtracacao", "id_atracacao") \
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
                          .withColumnRenamed("Nº do IMO", "numero_imo") \
                          .withColumnRenamed("TEsperaAtracacao", "tempo_espera_atracacao") \
                          .withColumnRenamed("TEsperaInicioOp", "tempo_espera_inicio_operacao") \
                          .withColumnRenamed("TOperacao", "tempo_operacao") \
                          .withColumnRenamed("TEsperaDesatracacao", "tempo_espera_desatracacao") \
                          .withColumnRenamed("TAtracado", "tempo_atracado") \
                          .withColumnRenamed("TEstadia", "tempo_estadia") 


    # Seleciona somente as colunas relevantes para a tabela atracacao_fato
    df_atracacao = df_atracacao.select(
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
    df_atracacao.write.mode("overwrite").parquet(output_dir)
    
    spark.stop()
    print(f"Atracacao_fato processado e salvo em {output_dir}")



def transformar_carga_fato(input_dir = "datalake/raw/unzipped", input_atrac_dir = "datalake/processed/atracacao", output_dir = "datalake/processed/carga"):
    """
    Lê arquivos CSV de carga, filtra e transforma 
    para o formato carga_fato.
    
    Args:
        input_dir (str): Caminho da pasta onde estão os CSVs descompactados.
        output_dir (str): Caminho onde serão salvos os dados processados (ex.: datalake/processed/carga_fato).
    """
    spark = SparkSession.builder.appName("TransformCarga").config("spark.driver.memory", "8g").config("spark.executor.memory", "8g").getOrCreate()

    # Lê todos os CSVs de carga (ex.: carga_2021.csv, carga_2022.csv, etc.)
    df_carga = spark.read.option("sep", ";").csv(f"{input_dir}/[0-9][0-9][0-9][0-9]Carga.txt", header=True, inferSchema=True)
    df_atracacao = spark.read.parquet(f"{input_atrac_dir}")
    df_carga_container = spark.read.option("sep", ";").csv(f"{input_dir}/[0-9][0-9][0-9][0-9]Carga_Conteinerizada.txt", header=True, inferSchema=True)

    # Filtra somente os anos 2021 a 2023 (ajuste conforme o seu layout)
    df_carga_alias = df_carga.withColumn("id_atracacao", col("IDAtracacao").cast("int"))
    df_carga_alias = df_carga_alias.withColumnRenamed("IDCarga", "id_carga")
    df_carga_alias = df_carga_alias.alias("c")
    df_atracacao_alias = df_atracacao.alias("a")
    
    df_join = df_carga_alias.join(
        df_atracacao_alias,
        on=[col("c.id_atracacao") == col("a.id_atracacao")],
        how= "left"
        )
    
    df_carga = df_join.select("c.*", "a.ano_data_inicio_operacao", "a.mes_data_inicio_operacao", 
                                "a.porto_atracacao", "a.sguf")

    df_join_2 = df_carga.join(
        df_carga_container.alias("cc"),
        on=[col("c.id_carga") == col("cc.IDCarga")],
        how= "left"
    )

    df_join_2 = df_join_2.withColumn(
    "peso_liquido_carga",
    when(
        (col("ConteinerEstado").isNotNull()) & (col("ConteinerEstado") != "Vazio"),
        col("VLPesoCargaConteinerizada")
    ).otherwise(col("VLPesoCargaBruta"))
    )

    df_join_2 = df_join_2.withColumn(
    "cd_mercadoria",
    when(
        (col("ConteinerEstado").isNotNull()) & (col("ConteinerEstado") != "Vazio"),
        col("CDMercadoriaConteinerizada")
    ).otherwise(col("CDMercadoria"))
    )
    # Exemplo de renomear colunas e criar colunas extras

    df_carga = df_join_2.withColumnRenamed("Origem", "origem") \
                         .withColumnRenamed("Destino", "berco") \
                         .withColumnRenamed("Tipo Operação da Carga", "tipo_operacao_carga") \
                         .withColumnRenamed("Carga Geral Acondicionamento", "carga_geral_acondicionamento") \
                         .withColumnRenamed("ConteinerEstado", "conteiner_estado") \
                         .withColumnRenamed("Tipo Navegação", "tipo_navegacao") \
                         .withColumnRenamed("FlagAutorizacao", "flag_autorizacao") \
                         .withColumnRenamed("FlagCabotagem", "flag_cabotagem") \
                         .withColumnRenamed("FlagCabotagemMovimentacao", "flag_cabotagem_movimentacao") \
                         .withColumnRenamed("FlagConteinerTamanho", "flag_container_tamanho") \
                         .withColumnRenamed("FlagLongoCurso", "flag_longo_curso") \
                         .withColumnRenamed("FlagMCOperacaoCarga", "flag_mc_operacao_carga") \
                         .withColumnRenamed("FlagOffshore", "flag_offshore") \
                         .withColumnRenamed("FlagTransporteViaInterioir", "flag_transporte_via_interioir") \
                         .withColumnRenamed("Percurso Transporte em vias Interiores", "percurso_transporte_via_interiores") \
                         .withColumnRenamed("Percurso Transporte Interiores", "percurso_transporte_interiores") \
                         .withColumnRenamed("STNaturezaCarga", "st_natureza_carga") \
                         .withColumnRenamed("STSH2", "stsh2") \
                         .withColumnRenamed("STSH4", "stsh4") \
                         .withColumnRenamed("Natureza da Carga", "natureza_carga") \
                         .withColumnRenamed("Sentido", "sentido") \
                         .withColumnRenamed("TEU", "teu") \
                         .withColumnRenamed("QTCarga", "qt_carga") \
                         .withColumnRenamed("VLPesoCargaBruta", "vl_peso_carga_bruta") 

    
    # Seleciona as colunas relevantes para carga_fato
    df_carga = df_carga.select(
        col("id_carga"),
        col("IDAtracacao"),
        col("origem"),
        col("berco"),
        col("cd_mercadoria"),
        col("tipo_operacao_carga"),
        col("carga_geral_acondicionamento"),
        col("conteiner_estado"),
        col("tipo_navegacao"),
        col("flag_autorizacao"),
        col("flag_cabotagem"),
        col("flag_cabotagem_movimentacao"),
        col("flag_container_tamanho"),
        col("flag_longo_curso"),
        col("flag_mc_operacao_carga"),
        col("flag_offshore"),
        col("flag_transporte_via_interioir"),
        col("percurso_transporte_via_interiores"),
        col("percurso_transporte_interiores"),
        col("st_natureza_carga"),
        col("stsh2"),
        col("stsh4"),
        col("natureza_carga"),
        col("sentido"),
        col("teu"),
        col("qt_carga"),
        col("vl_peso_carga_bruta"),
        col("ano_data_inicio_operacao"),
        col("mes_data_inicio_operacao"),
        col("porto_atracacao"),
        col("sguf"),
        col("peso_liquido_carga")
    )

    df_carga = df_carga.withColumnRenamed("IDAtracacao", "id_atracacao") 
    
    # Salva em formato Parquet
    df_carga.write.option("parquet.block.size", 134217728).mode("overwrite").parquet(output_dir)
    
    spark.stop()
    print(f"Carga_fato processado e salvo em {output_dir}")

if __name__ == "__main__":

    input_atrac = "datalake/raw/unzipped"
    output_atrac = "datalake/processed/atracacao"
    transformar_atracacao_fato(input_atrac, output_atrac)

    input_carga = "datalake/raw/unzipped"
    output_carga = "datalake/processed/carga"
    input_atrac_dir = "datalake/processed/atracacao" 
    transformar_carga_fato(input_carga, input_atrac_dir, output_carga)