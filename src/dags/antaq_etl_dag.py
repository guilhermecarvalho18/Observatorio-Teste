from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator


default_args = {
    'owner': 'guilherme_silva',
    'depends_on_past': False,
    'start_date': datetime(2025, 2, 24),
    'email_on_failure': True,
    'email': ['guilhermecarvalho181201@gmail.com'],
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'antaq_etl_pipeline',
    default_args=default_args,
    description='Pipeline ETL para a base ANTAQ, com Spark, SQL Server e Airflow',
    schedule_interval='@monthly',
    catchup=False
)

# Task para executar o job Spark ETL para atracacao_fato
etl_atracacao = DockerOperator(
    task_id='etl_atracacao',
    image='etl-spark',  # Nome da imagem construída a partir do seu Dockerfile
    api_version='auto',
    auto_remove=True,
    command="spark-submit --master local --jars /opt/spark/jars/mssql-jdbc-11.2.3.jre17.jar src/etl/etl_atracacao.py",
    docker_url="unix://var/run/docker.sock",
    network_mode="bridge",  
    dag=dag
)

# Task para executar o job Spark ETL para carga_fato
etl_carga = DockerOperator(
    task_id='etl_carga',
    image='etl-spark',
    api_version='auto',
    auto_remove=True,
    command="spark-submit --master local --jars /opt/spark/jars/mssql-jdbc-11.2.3.jre17.jar src/etl/etl_carga.py",
    docker_url="unix://var/run/docker.sock",
    network_mode="bridge",
    dag=dag
)


# Orquestração: Primeiro executa o job de atracacao, depois o de carga, e por fim envia o e-mail.
etl_atracacao >> etl_carga 
