from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.types import *
from pyspark.sql import functions as F
from utils.logging_config import get_airflow_logger

logger = get_airflow_logger()


class ClickHouseReader:
    """Читает данные из ClickHouse в Spark DataFrame."""
    # Словарь со схемами для каждой таблицы
    SCHEMAS = {
        "mart_data.customers_details": StructType([
            StructField("customer_pk", DecimalType(20, 0), True),  # 👈 UInt64 → Decimal
            StructField("customer_id", StringType(), True),
            StructField("registration_date", StringType(), True),
            StructField("is_loyalty_member", BooleanType(), True),
            StructField("version", DecimalType(20, 0), True)  # 👈 UInt64 → Decimal
        ]),

        "mart_data.purchases_details": StructType([
            StructField("purchase_pk", DecimalType(20, 0), True),
            StructField("customer_pk", DecimalType(20, 0), True),
            StructField("store_pk", DecimalType(20, 0), True),
            StructField("delivery_address_id", DecimalType(20, 0), True),
            StructField("total_amount", DoubleType(), True),
            StructField("payment_method_id", DecimalType(20, 0), True),  # UInt8 → Decimal
            StructField("is_delivery", BooleanType(), True),
            StructField("purchase_datetime", StringType(), True),
            StructField("version", DecimalType(20, 0), True)
        ]),

        "mart_data.dim_delivery_addresses": StructType([
            StructField("delivery_address_id", DecimalType(20, 0), True),
            StructField("delivery_city_id", DecimalType(20, 0), True),
            StructField("version", DecimalType(20, 0), True)
        ]),

        "mart_data.dim_payment_method": StructType([
            StructField("payment_method_id", DecimalType(20, 0), True),  # UInt8 → Decimal
            StructField("payment_method_name", StringType(), True),
            StructField("version", DecimalType(20, 0), True)
        ]),

        "mart_data.products_details": StructType([
            StructField("product_pk", DecimalType(20, 0), True),
            StructField("product_name", StringType(), True),
            StructField("price", DoubleType(), True),
            StructField("is_organic", BooleanType(), True),
            StructField("product_id", StringType(), True),
            StructField("category_id", DecimalType(20, 0), True),
            StructField("version", DecimalType(20, 0), True)
        ]),

        "mart_data.dim_categories": StructType([
            StructField("category_id", DecimalType(20, 0), True),
            StructField("category_name", StringType(), True),
            StructField("version", DecimalType(20, 0), True)
        ]),

        "mart_data.stores_details": StructType([
            StructField("store_pk", DecimalType(20, 0), True),
            StructField("store_delivery_available", BooleanType(), True),
            StructField("location_id", DecimalType(20, 0), True),
            StructField("version", DecimalType(20, 0), True)
        ]),

        "mart_data.dim_address": StructType([
            StructField("address_id", DecimalType(20, 0), True),
            StructField("store_city_id", DecimalType(20, 0), True),
            StructField("version", DecimalType(20, 0), True)
        ]),

        "mart_data.purchase_item_details": StructType([
            StructField("item_pk", DecimalType(20, 0), True),
            StructField("purchase_pk", DecimalType(20, 0), True),
            StructField("product_pk", DecimalType(20, 0), True),
            StructField("quantity", DoubleType(), True),
            StructField("total_price", DoubleType(), True),
            StructField("version", DecimalType(20, 0), True)
        ])
    }
    def __init__(
        self,
        spark: SparkSession,
        host: str,
        port: int,
        user: str,
        password: str,
        database: str,
    ) -> None:
        """
        Инициализация ридера для чтения данных из ClickHouse.

        Args:
            spark: Spark сессия
            host: Хост ClickHouse
            port: Порт ClickHouse
            user: Имя пользователя
            password: Пароль
            database: Название базы данных
        """
        self.spark = spark
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database

    def _get_query(self, table: str) -> str:
        """Возвращает SQL запрос для таблицы."""
        queries = {
            "mart_data.customers_details": """
                (SELECT
                    customer_pk,
                    customer_id,
                    CAST(registration_date AS String) as registration_date,
                    is_loyalty_member,
                    version
                FROM mart_data.customers_details) AS tmp
            """,
            "mart_data.purchases_details": """
                (SELECT
                    purchase_pk,
                    customer_pk,
                    store_pk,
                    delivery_address_id,
                    total_amount,
                    payment_method_id,
                    is_delivery,
                    CAST(purchase_datetime AS String) as purchase_datetime,
                    version
                FROM mart_data.purchases_details) AS tmp
            """,
            "mart_data.dim_delivery_addresses": """
                (SELECT
                    delivery_address_id,
                    city_id as delivery_city_id,
                    version
                FROM mart_data.dim_delivery_addresses) AS tmp
            """,
            "mart_data.dim_payment_method": """
                (SELECT
                    payment_method_id,
                    payment_method_name,
                    version
                FROM mart_data.dim_payment_method) AS tmp
            """,
            "mart_data.products_details": """
                (SELECT
                    product_pk,
                    name as product_name,
                    price,
                    is_organic,
                    product_id,
                    category_id,
                    version
                FROM mart_data.products_details) AS tmp
            """,
            "mart_data.dim_categories": """
                (SELECT
                    category_id,
                    clear_category_name as category_name,
                    version
                FROM mart_data.dim_categories) AS tmp
            """,
            "mart_data.stores_details": """
                (SELECT
                    store_pk,
                    delivery_available as store_delivery_available,
                    location_id,
                    version
                FROM mart_data.stores_details) AS tmp
            """,
            "mart_data.dim_address": """
                (SELECT
                    address_id,
                    city_id as store_city_id,
                    version
                FROM mart_data.dim_address) AS tmp
            """,
            "mart_data.purchase_item_details": """
                (SELECT
                    item_pk,
                    purchase_pk,
                    product_pk,
                    quantity,
                    total_price,
                    version
                FROM mart_data.purchase_item_details) AS tmp
            """
        }
        return queries.get(table, f"(SELECT * FROM {table}) AS tmp")

    def read_table(self, table: str) -> DataFrame:
        """
        Читает таблицу из ClickHouse в Spark DataFrame с явной схемой.
        """
        url = (
            f"jdbc:clickhouse://{self.host}:{self.port}"
            f"/{self.database}?compress=false"
        )
        query = self._get_query(table)
        reader = self.spark.read.format("jdbc")

        # Добавляем все необходимые опции
        df = (
            self.spark.read.format("jdbc")
            .option("driver", "com.clickhouse.jdbc.ClickHouseDriver")
            .option("url", url)
            .option("user", self.user)
            .option("password", self.password)
            .option("dbtable", query)
            .option("fetchsize", "10000")
            .option("socket_timeout", "300000")
            .option("connect_timeout", "30000")
            .load()
        )

        # Автоматически преобразуем все Decimal(20,0) в Long
        decimal_cols = []
        for field in df.schema.fields:
            if (isinstance(field.dataType, DecimalType) and
                    field.dataType.precision == 20):
                decimal_cols.append(field.name)
                df = df.withColumn(
                    field.name,
                    F.col(field.name).cast(LongType())
                )

        if decimal_cols:
            logger.info(f"🔄 Преобразованы в Long: {', '.join(decimal_cols)}")

        logger.info(f"✅ Загружено {df.count()} записей из {table}")
        df.show(5, truncate=False)

        return df
