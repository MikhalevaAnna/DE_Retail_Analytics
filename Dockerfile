FROM apache/airflow:2.9.2-python3.11

USER root

# Установка Java
RUN apt-get update && \
    apt-get install -y default-jdk wget && \
    apt-get autoremove -yqq --purge && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Установка Spark
RUN wget https://archive.apache.org/dist/spark/spark-3.4.2/spark-3.4.2-bin-hadoop3.tgz && \
    mkdir -p /opt/spark && \
    tar -xzf spark-3.4.2-bin-hadoop3.tgz -C /opt/spark && \
    rm spark-3.4.2-bin-hadoop3.tgz

# Установка JAR для S3
RUN wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar -P /opt/spark/spark-3.4.2-bin-hadoop3/jars/
RUN wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar -P /opt/spark/spark-3.4.2-bin-hadoop3/jars/

# Установка JDBC драйвера для ClickHouse
RUN wget https://repo1.maven.org/maven2/com/clickhouse/clickhouse-jdbc/0.6.3/clickhouse-jdbc-0.6.3-all.jar && \
    mv clickhouse-jdbc-0.6.3-all.jar /opt/spark/spark-3.4.2-bin-hadoop3/jars/

# Настройка переменных окружения
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV SPARK_HOME=/opt/spark/spark-3.4.2-bin-hadoop3
ENV PATH=$PATH:$JAVA_HOME/bin:$SPARK_HOME/bin
ENV PYTHONPATH=$SPARK_HOME/python/lib/py4j-0.10.9.7-src.zip

# Python зависимости
COPY requirements.txt /requirements.txt
RUN chmod 644 /requirements.txt

USER airflow

# Установка зависимостей с constraint-файлом Airflow
ARG AIRFLOW_VERSION=2.9.2
ARG PYTHON_VERSION=3.11
ARG CONSTRAINT_URL=https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt

RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r /requirements.txt --constraint ${CONSTRAINT_URL}


