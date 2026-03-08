from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from utils.logging_config import get_airflow_logger

logger = get_airflow_logger()


class ClickHouseReader:
    """Читает данные из ClickHouse в Spark DataFrame."""

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

    def read_table(self, table: str) -> DataFrame:
        """
        Читает таблицу из ClickHouse в Spark DataFrame.

        Args:
            table: Имя таблицы в формате schema.table_name
                (например, 'mart_data.customers_details')

        Returns:
            DataFrame: Spark DataFrame с данными из ClickHouse

        Raises:
            ValueError: Если указана неподдерживаемая таблица
        """
        url = (
            f"jdbc:clickhouse://{self.host}:{self.port}"
            f"/{self.database}?compress=false"
        )
        query = ""
        if table == "mart_data.customers_details":
            query = f"""
            (SELECT
                customer_pk,
                customer_id,
                CAST(registration_date AS String) as registration_date,
                is_loyalty_member,
                version
            FROM {table}) AS tmp
           """
        elif table == "mart_data.purchases_details":
            query = f"""
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
             FROM {table}
            ) AS tmp
            """
        elif table == "mart_data.dim_delivery_addresses":
            query = f"""
             (SELECT
                 delivery_address_id,
                 city_id as delivery_city_id,
                 version
             FROM {table}
            ) AS tmp
            """
        elif table == "mart_data.dim_payment_method":
            query = f"""
             (SELECT
                 payment_method_id,
                 payment_method_name,
                 version
             FROM {table}
            ) AS tmp
            """
        elif table == "mart_data.products_details":
            query = f"""
            (SELECT product_pk,
                    name as product_name,
                    price,
                    is_organic,
                    product_id,
                    category_id,
                    version
                    FROM {table}
            ) AS tmp
            """
        elif table == "mart_data.dim_categories":
            query = f"""
            (SELECT category_id,
                    clear_category_name as category_name,
                    version
                    FROM {table}
            ) AS tmp
            """
        elif table == "mart_data.stores_details":
            query = f"""
            (SELECT store_pk,
                    delivery_available as store_delivery_available,
                    location_id,
                    version
              FROM {table}
            ) AS tmp
            """
        elif table == "mart_data.dim_address":
            query = f"""
            (SELECT address_id,
                    city_id as store_city_id,
                    version
              FROM {table}
            ) AS tmp
            """
        elif table == "mart_data.purchase_item_details":
            query = f"""
            (SELECT
               item_pk,
               purchase_pk,
               product_pk,
               quantity,
               total_price,
               version
            FROM {table}) AS tmp
            """

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
        df.show()

        return df
