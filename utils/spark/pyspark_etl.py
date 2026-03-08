from pyspark.sql import SparkSession

from config.config import (
    S3_ENDPOINT,
    S3_ACCESS_KEY,
    S3_SECRET_KEY,
    S3_BUCKET,
    S3_PREFIX,
    CLICKHOUSE_HOST,
    CLICKHOUSE_HTTP_PORT,
    CLICKHOUSE_USER,
    CLICKHOUSE_PASSWORD,
    CLICKHOUSE_DB,
)
from utils.clickhouse.clickhouse_reader import ClickHouseReader
from utils.spark.feature_engineering import FeatureEngineer
from utils.s3.s3_writer import S3Writer
from utils.logging_config import get_airflow_logger

logger = get_airflow_logger()


def run_etl() -> None:
    """
    Запускает ETL процесс для расчета признаков покупателей.

    Функция выполняет полный цикл ETL:
    1. Инициализирует Spark сессию с необходимыми конфигурациями
    для подключения к S3
    2. Проверяет подключение к S3 хранилищу
    3. Читает данные из ClickHouse
    (customers, purchases, products, stores, purchase_items)
    4. Рассчитывает признаки покупателей с помощью FeatureEngineer
    5. Записывает результаты в S3 в формате CSV

    Returns:
        None

    Raises:
        Exception: При любой ошибке в процессе ETL
        (логируется и пробрасывается дальше)

    Note:
        Функция использует глобальные константы из config.config:
            - S3_ENDPOINT, S3_ACCESS_KEY, S3_SECRET_KEY, S3_BUCKET,
            S3_PREFIX
            - CLICKHOUSE_HOST, CLICKHOUSE_PORT, CLICKHOUSE_USER,
            CLICKHOUSE_PASSWORD, CLICKHOUSE_DB
    """
    logger.info("🚀 Запуск ETL для признаков покупателей")

    # Инициализация Spark с локальным режимом
    spark = (
        SparkSession.builder.appName("Customer Features ETL")
        .config(
            "spark.jars",
            "/opt/spark/spark-3.4.2-bin-hadoop3/"
            "jars/clickhouse-jdbc-0.6.3-all.jar,"
            "/opt/spark/spark-3.4.2-bin-hadoop3/"
            "jars/hadoop-aws-3.3.4.jar,"
            "/opt/spark/spark-3.4.2-bin-hadoop3/"
            "jars/aws-java-sdk-bundle-1.12.262.jar",
        )
        .config("spark.driver.memory", "1g")
        .config("spark.executor.memory", "1g")
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", S3_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", S3_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.sql.adaptive.enabled", "true")
        .master("local[1]")
        .getOrCreate()
    )

    try:
        # 1. Проверка подключения к S3
        logger.info("🔍 Проверка подключения к S3 Selectel...")
        S3Writer(
            spark=spark,
            endpoint=S3_ENDPOINT,
            access_key=S3_ACCESS_KEY,
            secret_key=S3_SECRET_KEY,
            bucket=S3_BUCKET,
        )
        logger.info("✅ Проверка подключения к S3 пройдена успешно!")

        # 2. Чтение из ClickHouse
        reader = ClickHouseReader(
            spark,
            CLICKHOUSE_HOST,
            CLICKHOUSE_HTTP_PORT,
            CLICKHOUSE_USER,
            CLICKHOUSE_PASSWORD,
            CLICKHOUSE_DB,
        )

        customers_df = reader.read_table("mart_data.customers_details")
        logger.info("Схема customers_df:")
        customers_df.printSchema()
        purchases_df = reader.read_table("mart_data.purchases_details")
        logger.info("Схема purchases_df:")
        purchases_df.printSchema()
        products_df = reader.read_table("mart_data.products_details")
        logger.info("Схема products_df:")
        products_df.printSchema()
        stores_df = reader.read_table("mart_data.stores_details")
        logger.info("Схема stores_df:")
        stores_df.printSchema()
        purchase_items_df = (
            reader.read_table("mart_data.purchase_item_details")
        )
        logger.info("Схема purchase_items_df:")
        purchase_items_df.printSchema()
        delivery_addresses_df = (
            reader.read_table("mart_data.dim_delivery_addresses")
        )
        logger.info("Схема delivery_addresses_df:")
        delivery_addresses_df.printSchema()
        payment_methods_df = reader.read_table("mart_data.dim_payment_method")
        logger.info("Схема payment_methods_df:")
        payment_methods_df.printSchema()
        categories_df = reader.read_table("mart_data.dim_categories")
        logger.info("Схема categories_df:")
        categories_df.printSchema()
        addresses_df = reader.read_table("mart_data.dim_address")
        logger.info("Схема addresses_df:")
        addresses_df.printSchema()

        customers_df.cache()
        stores_df.cache()
        categories_df.cache()
        payment_methods_df.cache()
        products_df.cache()
        logger.info(f"📊 customers: {customers_df.count()} записей")
        logger.info(f"📊 purchases: {purchases_df.count()} записей")
        logger.info(f"📊 products: {products_df.count()} записей")
        logger.info(f"📊 stores: {stores_df.count()} записей")
        logger.info(f"📊 purchase_items: {purchase_items_df.count()} записей")
        logger.info(f"📊 delivery_address: "
                    f"{delivery_addresses_df.count()} записей")
        logger.info(f"📊 payment_method: {payment_methods_df.count()} записей")
        logger.info(f"📊 categories: {categories_df.count()} записей")
        logger.info(f"📊 address: {addresses_df.count()} записей")

        # 3. Расчёт признаков
        engineer = FeatureEngineer(spark)
        features_df = engineer.calculate_features(
            customers_df, purchases_df, products_df, stores_df,
            purchase_items_df, delivery_addresses_df,
            payment_methods_df, categories_df, addresses_df
        )

        features_df.printSchema()
        features_df.show(5, truncate=False)

        # 4. Запись в S3
        writer = S3Writer(
            spark, S3_ENDPOINT, S3_ACCESS_KEY, S3_SECRET_KEY, S3_BUCKET
        )
        s3_path = writer.write_csv(features_df, S3_PREFIX)

        logger.info(f"✅ ETL завершён. Файл: {s3_path}")

    except Exception as e:
        logger.error(f"❌ Ошибка в ETL: {e}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    run_etl()
