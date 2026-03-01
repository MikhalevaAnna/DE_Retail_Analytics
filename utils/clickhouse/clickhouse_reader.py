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
            f"jdbc:clickhouse://{self.host}:{self.port}/{self.database}?compress=false"
        )
        query = ""
        if table == "mart_data.customers_details":
            query = f"""
            (SELECT
                customer_pk,
                customer_id,
                CAST(registration_date AS String) as registration_date,
                is_loyalty_member
            FROM {table}) AS tmp
           """
        elif table == "mart_data.purchases_details":
            query = f"""
             (SELECT DISTINCT
                 purchase_pk,
                 customer_pk,
                 store_pk,
                 dda.delivery_address_id as delivery_address_id,
                 dda.city_id as delivery_city_id,
                 total_amount,
                 pd.payment_method_id as payment_method_id,
                 dpm.payment_method_name as payment_method_name,
                 is_delivery,
                 CAST(purchase_datetime AS String) as purchase_datetime
             FROM {table} pd
                LEFT JOIN mart_data.dim_delivery_addresses dda
             ON pd.delivery_address_id = dda.delivery_address_id
                LEFT JOIN mart_data.dim_payment_method dpm
             ON pd.payment_method_id = dpm.payment_method_id
            ) AS tmp
            """
        elif table == "mart_data.products_details":
            query = f"""
            (SELECT pd.product_pk,
                    pd.name as product_name,
                    pd.price,
                    pd.is_organic,
                    dc.clear_category_name as category_name
                    FROM {table} pd LEFT JOIN  mart_data.dim_categories dc
                    ON pd.category_id =dc.category_id
            ) AS tmp
            """
        elif table == "mart_data.stores_details":
            query = f"""
            (SELECT sd.store_pk,
                    sd.delivery_available as store_delivery_available,
                    da.city_id as store_city_id
              FROM {table} sd LEFT JOIN mart_data.dim_address da
                   ON sd.location_id =da.address_id
            ) AS tmp
            """
        elif table == "mart_data.purchase_item_details":
            query = f"""
            (SELECT
               item_pk,
               purchase_pk,
               product_pk,
               quantity,
               total_price
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
