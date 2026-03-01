"""
DAG для переноса данных из MongoDB в Kafka, Kafka в
ClickHouse и Spark ETL в S3.
"""

from datetime import datetime, timedelta
from confluent_kafka import Producer, Consumer
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from airflow.providers.mongo.hooks.mongo import MongoHook
from clickhouse_driver import Client

# 👇 Импортируем конфиг
from config.config import (
    CLICKHOUSE_HOST,
    CLICKHOUSE_NATIVE_PORT,
    CLICKHOUSE_USER,
    CLICKHOUSE_PASSWORD,
    CLICKHOUSE_DB,
    MONGO_HOST,
    MONGO_PORT,
    MONGO_DATABASE,
)

from utils.mongo.mongo_tasks import load_mongo_data, check_mongo_data
from utils.kafka.mongo_kafka_transfer import MongoKafkaTransfer
from utils.kafka.consumer_kafka_clickhouse import transfer
from utils.spark.pyspark_etl import run_etl

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2024, 1, 1),
}

TOPICS = ["products", "stores", "customers", "purchases"]
SENSITIVE_TOPICS = ["stores", "customers", "purchases"]

MONGO_CONN_ID = "mongodb_default"
CLICKHOUSE_CONN_ID = "clickhouse_raw"
KAFKA_CONN_ID = "kafka_default"
SPARK_CONN_ID = "spark_default"
S3_CONN_ID = "s3_default"


def check_mongo_connection(**context):
    """Проверяет подключение к MongoDB."""
    try:
        mongo_hook = MongoHook(conn_id=MONGO_CONN_ID)
        client = mongo_hook.get_conn()
        client.admin.command("ping")
        client.close()
        print("✅ MongoDB доступна")
        return True
    except Exception as e:
        print(f"❌ Ошибка MongoDB: {e}")
        raise


def check_kafka_connection(**context):
    """Проверяет подключение к Kafka."""
    try:
        # Получаем параметры Kafka из Connection
        kafka_conn = BaseHook.get_connection(KAFKA_CONN_ID)
        broker = (
            f"{kafka_conn.host}:{kafka_conn.port}"
            if kafka_conn.port
            else kafka_conn.host
        )

        # Дополнительная информация для логов
        print(f"📨 Kafka broker: {broker}")
        print(f"📋 Kafka connection ID: {KAFKA_CONN_ID}")

        # Создаем consumer для проверки
        consumer = Consumer(
            {
                "bootstrap.servers": broker,
                "group.id": "connection_check",
                "session.timeout.ms": 5000,
            }
        )

        # Пробуем получить список топиков
        topics = consumer.list_topics(timeout=5)
        consumer.close()

        print(f"✅ Kafka доступна. Найдено топиков: {len(topics.topics)}")
        return True

    except Exception as e:
        print(f"❌ Ошибка подключения к Kafka: {e}")
        raise


def check_clickhouse_connection(**context):
    """Проверяет подключение к ClickHouse."""
    try:
        client = Client(
            host=CLICKHOUSE_HOST,
            port=CLICKHOUSE_NATIVE_PORT,
            user=CLICKHOUSE_USER,
            password=CLICKHOUSE_PASSWORD,
            database=CLICKHOUSE_DB,
        )
        client.execute("SELECT 1")
        client.disconnect()
        print("✅ ClickHouse доступен")
        return True
    except Exception as e:
        print(f"❌ Ошибка ClickHouse: {e}")
        raise


# Загрузка В MONGODB
def load_to_mongodb(**context):
    """Загружает данные в MongoDB (обёртка для совместимости)."""
    return load_mongo_data(**context)


# Проверка данных MONGODB
def check_mongodb_data(**context):
    """Проверяет данные в MongoDB (обёртка для совместимости)."""
    return check_mongo_data(**context)


def transfer_data_to_kafka(**context):
    """Перенос данных из MongoDB в Kafka."""
    mongo_client = None
    kafka_producer = None
    try:
        # MongoDB подключение
        print(f"📦 Подключение к MongoDB (conn_id: {MONGO_CONN_ID})")
        mongo_hook = MongoHook(conn_id=MONGO_CONN_ID)
        mongo_client = mongo_hook.get_conn()

        # Информация из конфига
        print(f"   📌 Хост: {MONGO_HOST}:{MONGO_PORT}, БД: {MONGO_DATABASE}")
        print("✅ Подключение к MongoDB установлено")

        # Kafka подключение
        print(f"📨 Подключение к Kafka (conn_id: {KAFKA_CONN_ID})")

        # Получаем connection Kafka
        kafka_conn = BaseHook.get_connection(KAFKA_CONN_ID)
        broker = (
            f"{kafka_conn.host}:{kafka_conn.port}"
            if kafka_conn.port
            else kafka_conn.host
        )

        # Создаем producer

        kafka_producer = Producer(
            {
                "bootstrap.servers": broker,
                "acks": "all",
                "retries": 3,
            }
        )

        print(f"   📌 Kafka broker: {broker}")
        print("✅ Подключение к Kafka установлено")

        # Создаем трансфер
        transfer = MongoKafkaTransfer(
            mongo_conn=mongo_client,
            kafka_producer=kafka_producer,
            sensitive_topics=SENSITIVE_TOPICS,
        )

        print(f"\n📊 Начинаем перенос данных для топиков: {TOPICS}")
        results = transfer.transfer_all(TOPICS)

        total = sum(results.values())
        print(f"\n{'=' * 50}")
        print("✅ ИТОГИ ПЕРЕНОСА:")
        print(f"{'=' * 50}")
        for topic, count in results.items():
            print(f"   📍 {topic}: {count} документов")
        print(f"{'=' * 50}")
        print(f"✅ ВСЕГО: {total} документов")
        print(f"{'=' * 50}")

        return results

    except Exception as e:
        print(f"❌ Критическая ошибка: {e}")
        raise
    finally:
        if mongo_client:
            try:
                mongo_client.close()
                print("🔒 Соединение с MongoDB закрыто")
            except Exception as e:
                print(f"⚠️ Ошибка при закрытии MongoDB: {e}")

        if kafka_producer:
            try:
                kafka_producer.flush()
                print("🔒 Kafka producer завершил работу")
            except Exception as e:
                print(f"⚠️ Ошибка при завершении Kafka: {e}")


def consume_kafka_to_clickhouse(**context):
    """Запускает consumer для переноса данных из Kafka в ClickHouse."""
    print("=" * 60)
    print("📥 ЗАПУСК ПЕРЕНОСА ДАННЫХ ИЗ KAFKA В CLICKHOUSE")
    print("=" * 60)

    try:
        # Используем значения из конфига
        kafka_conn = BaseHook.get_connection(KAFKA_CONN_ID)
        kafka_broker = (
            f"{kafka_conn.host}:{kafka_conn.port}"
            if kafka_conn.port
            else kafka_conn.host
        )

        print(
            f"📊 ClickHouse: {CLICKHOUSE_HOST}:{CLICKHOUSE_NATIVE_PORT}/{CLICKHOUSE_DB}"
        )
        print(f"📨 Kafka broker: {kafka_broker}")
        print(f"📋 Топики: {TOPICS}")

        consumer_kwargs = {
            "clickhouse_host": CLICKHOUSE_HOST,
            "clickhouse_port": CLICKHOUSE_NATIVE_PORT,
            "clickhouse_user": CLICKHOUSE_USER,
            "clickhouse_password": CLICKHOUSE_PASSWORD,
            "clickhouse_db": CLICKHOUSE_DB,
            "kafka_broker": kafka_broker,
            "kafka_group": f"clickhouse_consumer_{datetime.now().strftime('%Y%m%d')}",
            "kafka_topics": ",".join(TOPICS),
            "max_time": 300,
            "batch_size": 1000,
        }

        result = transfer(**consumer_kwargs)
        print(f"\n✅ Перенос завершен. Обработано сообщений: {result}")
        context["ti"].xcom_push(key="kafka_to_clickhouse_result", value=result)
        return result
    except Exception as e:
        print(f"❌ Ошибка при переносе данных: {e}")
        raise


def run_spark_etl(**context):
    run_etl()


# Создание DAG
dag = DAG(
    dag_id="pipeline_retail_data",
    default_args=default_args,
    description="MongoDB → Kafka → ClickHouse → Spark → S3",
    schedule_interval="0 10 * * *",
    catchup=False,
    tags=["mongodb", "kafka", "clickhouse", "spark", "s3"],
)


# Задачи на проверку подключений
check_mongo = PythonOperator(
    task_id="check_mongo", python_callable=check_mongo_connection, dag=dag
)
check_kafka = PythonOperator(
    task_id="check_kafka", python_callable=check_kafka_connection, dag=dag
)
check_clickhouse = PythonOperator(
    task_id="check_clickhouse", python_callable=check_clickhouse_connection, dag=dag
)

# Задачи для MONGODB
load_mongo_task = PythonOperator(
    task_id="load_mongo_task",
    python_callable=load_to_mongodb,
    provide_context=True,
    dag=dag,
    op_kwargs={
        "params": {
            "clear_collections": True  # Очищаем перед загрузкой
        }
    },
)

check_mongo_data_task = PythonOperator(
    task_id="check_mongo_data_task",
    python_callable=check_mongodb_data,
    provide_context=True,
    dag=dag,
)

transfer_mongo_to_kafka = PythonOperator(
    task_id="transfer_mongo_to_kafka",
    python_callable=transfer_data_to_kafka,
    dag=dag,
)

transfer_kafka_to_clickhouse = PythonOperator(
    task_id="transfer_kafka_to_clickhouse",
    python_callable=consume_kafka_to_clickhouse,
    dag=dag,
)

# === Задача для SPARK ETL ===
spark_etl_task = PythonOperator(
    task_id="spark_etl_task",
    python_callable=run_spark_etl,
    dag=dag,
)

# Порядок выполнения
(
    [check_mongo, check_kafka]
    >> load_mongo_task
    >> check_mongo_data_task
    >> transfer_mongo_to_kafka
    >> check_clickhouse
    >> transfer_kafka_to_clickhouse
    >> spark_etl_task
)
