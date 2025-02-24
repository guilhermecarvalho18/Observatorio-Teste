# Aqui usamos a imagem do Bitnami, que já possui Spark configurado.
FROM bitnami/spark:3

# Torne-se root para copiar arquivos
USER root

# Crie o diretório para os JARs, se necessário
RUN mkdir -p /opt/spark/jars

# Copie o driver JDBC para dentro da imagem
COPY mssql-jdbc-11.2.3.jre17.jar /opt/spark/jars/

# Retorne ao usuário não-root (por exemplo, 1001, que é padrão em muitas imagens Bitnami)
USER 1001

# Defina o diretório de trabalho
WORKDIR /app

# Copie os arquivos do projeto para dentro do container
COPY . /app

