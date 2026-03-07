from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StructType, StructField, ByteType
from utils.logging_config import get_airflow_logger
from typing import Tuple

logger = get_airflow_logger()


class FeatureEngineer:
    """Рассчитывает признаки для кластеризации покупателей."""

    def __init__(self, spark: SparkSession) -> None:
        """
        Инициализация инженера признаков.

        Args:
            spark: Spark сессия для выполнения операций
        """
        self.spark = spark
        self.cutoff_30d = F.date_sub(F.current_date(), 30)
        self.cutoff_14d = F.date_sub(F.current_date(), 14)
        self.cutoff_7d = F.date_sub(F.current_date(), 7)
        self.cutoff_90d = F.date_sub(F.current_date(), 90)

    def prepare_data(
        self,
        customers_df: DataFrame,
        purchases_df: DataFrame,
        products_df: DataFrame,
        stores_df: DataFrame,
        delivery_addresses_df: DataFrame,
        payment_methods_df: DataFrame,
        categories_df: DataFrame,
        addresses_df: DataFrame,
        purchase_item_df: DataFrame
    ) -> Tuple[DataFrame, DataFrame, DataFrame, DataFrame]:
        """
        Подготавливает и объединяет все необходимые данные.

        Выполняет предобработку входных DataFrame:
        - Преобразует даты и временные метки
        - Создает дополнительные колонки (час покупки, день недели)
        - Очищает и нормализует данные

        Args:
            customers_df: DataFrame с данными клиентов
            purchases_df: DataFrame с данными о покупках
            products_df: DataFrame с данными о продуктах
            stores_df: DataFrame с данными о магазинах
            delivery_addresses_df: DataFrame с адресами доставки,
            payment_methods_df: DataFrame с методами оплаты,
            categories_df: DataFrame с категориями товаров,
            addresses_df: DataFrame с юридическими адресами
            purchases_item_df: DataFrame c купленными товарами
        Returns:
            Tuple[DataFrame, DataFrame, DataFrame, DataFrame]:
                Кортеж из обработанных DataFrame
                (customers, purchases, products, stores)
        """

        # 1. PURCHASES
        logger.info("🔄 Обработка purchases")
        try:
            latest_versions_purchases = (
                purchases_df
                .groupBy("purchase_pk")
                .agg(F.max("version").alias("version"))
            )

            if latest_versions_purchases.isEmpty():
                raise ValueError("❌ Не удалось получить "
                                 "последние версии purchases")

            purchases_latest = (
                purchases_df
                .join(latest_versions_purchases,
                      ["purchase_pk", "version"], "inner")
                .drop("version")
            )

            before = purchases_df.count()
            after = purchases_latest.count()
            if after == 0:
                raise ValueError("❌ После дедупликации purchases пустой!")

            logger.info(f"📊 purchases: {before} → {after} "
                        f"(удалено {before - after})")

        except Exception as e:
            logger.error(f"❌ Ошибка при обработке purchases: {e}")
            raise

        try:
            purchases = (
                purchases_latest
                .join(
                    F.broadcast(delivery_addresses_df).alias('da'),
                    ["delivery_address_id"], "left"
                )
                .join(
                    F.broadcast(payment_methods_df).alias('pm'),
                    ["payment_method_id"], "left"
                )
                .select(
                    "purchase_pk", "customer_pk", "store_pk",
                    "da.delivery_address_id",
                    F.col("da.delivery_city_id"),
                    "total_amount", "pm.payment_method_name", "is_delivery",
                    F.to_timestamp(
                        "purchase_datetime",
                        "yyyy-MM-dd HH:mm:ss"
                    ).alias("purchase_datetime")
                )
                .withColumn("purchase_date", F.to_date("purchase_datetime"))
                .withColumn("purchase_hour", F.hour("purchase_datetime"))
                .withColumn(
                    "weekday",
                    F.date_format("purchase_datetime", "EEEE")
                )
            )
        except Exception as e:
            logger.error(f"❌ Ошибка при обогащении purchases: {e}")
            raise

        # 2. CUSTOMERS
        logger.info("🔄 Обработка customers")
        try:
            latest_versions_customers = (
                customers_df
                .groupBy("customer_pk")
                .agg(F.max("version").alias("version"))
            )
            if latest_versions_customers.isEmpty():
                raise ValueError("❌ Не удалось получить "
                                 "последние версии customers")

            customers_latest = (
                customers_df
                .join(F.broadcast(latest_versions_customers),
                      ["customer_pk", "version"], "inner")
                .drop("version")
            )
        except Exception as e:
            logger.error(f"❌ Ошибка при обработке purchases: {e}")
            raise

        try:
            customers = (
                customers_latest
                .select(
                    F.col("customer_pk"),
                    F.col("customer_id"),
                    F.to_date("registration_date").alias("reg_date"),
                    F.col("is_loyalty_member")
                )
            )

            before = customers_df.count()
            after = customers_latest.count()
            logger.info(f"📊 customers: {before} → {after} "
                        f"(удалено {before - after} дубликатов)")
        except Exception as e:
            logger.error(f"❌ Ошибка при обогащении customers: {e}")
            raise

        # 3. PRODUCTS
        logger.info("🔄 Обработка products")
        try:
            latest_versions_products = (
                products_df
                .groupBy("product_pk")
                .agg(F.max("version").alias("version"))
            )

            if latest_versions_products.isEmpty():
                raise ValueError("❌ Не удалось получить "
                                 "последние версии products")
            products_latest = (
                products_df.alias("p")
                .join(
                    F.broadcast(latest_versions_products),
                    ["product_pk", "version"],
                    "inner"
                )
                .drop("version")
                .select("p.*")
            )

        except Exception as e:
            logger.error(f"❌ Ошибка при обработке products: {e}")
            raise

        try:
            products = (
                products_latest.alias("pd")
                .join(
                    F.broadcast(categories_df).alias("cd"),
                    F.col("pd.category_id") == F.col("cd.category_id"),
                    "left"
                )
                .select(
                    F.col("pd.product_pk"),
                    F.col("pd.product_name"),
                    F.col("pd.price"),
                    F.coalesce(
                        F.col("cd.category_name"),
                        F.lit("Без категории")).alias("category_name"),
                    F.col("pd.is_organic")
                )
            )
            before = products_df.count()
            after = products_latest.count()
            logger.info(f"📊 products: {before} → {after} "
                        f"(удалено {before - after} дубликатов)")
        except Exception as e:
            logger.error(f"❌ Ошибка при обогащении products: {e}")
            raise

        # 4. STORES
        logger.info("🔄 Обработка stores...")
        try:
            latest_versions_stores = (
                stores_df
                .groupBy("store_pk")
                .agg(F.max("version").alias("version"))
            )

            if latest_versions_stores.isEmpty():
                raise ValueError("❌ Не удалось получить "
                                 "последние версии stores")
            stores_latest = (
                stores_df.alias("s")
                .join(
                    F.broadcast(latest_versions_stores),
                    ["store_pk", "version"],
                    "inner"
                )
                .drop("version")
                .select("s.*")
            )
        except Exception as e:
            logger.error(f"❌ Ошибка при обработке stores: {e}")
            raise

        try:
            stores = (
                stores_latest.alias("sd")
                .join(
                    addresses_df.alias("da"),
                    F.col("sd.location_id") == F.col("da.address_id"),
                    "left"
                )
                .select(
                    F.col("sd.store_pk"),
                    F.col("sd.store_delivery_available"),
                    F.coalesce(
                        F.col("da.store_city_id"),
                        F.lit(-1)
                    ).alias("store_city_id")
                )
            )
            before = stores_df.count()
            after = stores_latest.count()
            logger.info(f"📊 stores: {before} → {after} "
                        f"(удалено {before - after} дубликатов)")
        except Exception as e:
            logger.error(f"❌ Ошибка при обогащении stores: {e}")
            raise

        # 5. purchase_item_detail
        logger.info("🔄 Обработка purchase_item_detail")
        try:
            latest_versions_purchase_item = (
                purchase_item_df
                .groupBy("item_pk")
                .agg(F.max("version").alias("version"))
            )

            if latest_versions_purchase_item.isEmpty():
                raise ValueError("❌ Не удалось получить "
                                 "последние версии purchase_item_detail")

            before = purchase_item_df.count()

            purchase_item_latest = (
                purchase_item_df
                .join(latest_versions_purchase_item,
                      ["item_pk", "version"], "inner")
                .drop("version")
            )

            after = purchase_item_latest.count()
            logger.info(f"📊 purchase_items: {before} → {after} "
                        f"(удалено {before - after} дубликатов)")

            purchase_items = (
                purchase_item_latest
                .select(
                    "item_pk",
                    "purchase_pk",
                    "product_pk",
                    "quantity",
                    "total_price"
                )
            )

            logger.info(f"✅ purchase_items готово: "
                        f"{purchase_items.count()}")

        except Exception as e:
            logger.error(f"❌ Ошибка при обработке "
                         f"purchase_item_detail: {e}")
            raise

        # 5. КЭШИРОВАНИЕ
        logger.info("💾 Кэширование данных")
        customers.cache()
        products.cache()
        stores.cache()

        # Форсируем кэширование
        customers.count()
        products.count()
        stores.count()

        return customers, purchases, products, stores, purchase_items

    def cast_to_schema(self, df, schema):
        """Приводит DataFrame к заданной схеме"""
        return df.select([
            F.col(field.name).cast(field.dataType).alias(field.name)
            for field in schema.fields
        ])

    def calculate_features(
        self,
        customers_df: DataFrame,
        purchases_df: DataFrame,
        products_df: DataFrame,
        stores_df: DataFrame,
        purchase_items_df: DataFrame,
        delivery_addresses_df: DataFrame,
        payment_methods_df: DataFrame,
        categories_df: DataFrame,
        addresses_df: DataFrame
    ) -> DataFrame:
        """
        Рассчитывает все признаки для каждого customer_id.

        Функция вычисляет 30 различных признаков для кластеризации
        покупателей на основе
        их покупательского поведения.

        Args:
            customers_df: DataFrame с данными клиентов
            purchases_df: DataFrame с данными о покупках
            products_df: DataFrame с данными о продуктах
            stores_df: DataFrame с данными о магазинах
            purchase_items_df: DataFrame с деталями покупок (позиции)
            delivery_address_df: DataFrame с адресами доставки,
            payment_method_df: DataFrame с методами оплаты,
            categories_df: DataFrame с категориями товаров,
            address_df: DataFrame с юридическими адресами

        Returns:
            DataFrame: Результирующий DataFrame с колонками:
                - customer_id: идентификатор клиента
                - [30 признаков]: бинарные признаки (0 или 1)

        Note:
            Все NULL значения в финальном DataFrame заменяются на 0
            Используется кэширование для purchases и products для оптимизации
        """

        # Подготовка данных
        customers, purchases, products, stores, purchase_items \
            = self.prepare_data(
                customers_df, purchases_df, products_df, stores_df,
                delivery_addresses_df, payment_methods_df, categories_df,
                addresses_df, purchase_items_df)

        # Словарь для хранения всех признаков
        feature_dfs = {}

        # 1. bought_milk_last_30d (категория молочные)
        # Покупал молочные продукты за последние 30 дней
        milk_purchases_schema = StructType([
            StructField("customer_pk", LongType(), True),
            StructField("bought_milk_last_30d", ByteType(), True)
        ])
        empty_milk = (
            self.spark.createDataFrame([], schema=milk_purchases_schema)
        )

        try:
            milk_purchases = (
                purchases.filter(F.col("purchase_date") >= self.cutoff_30d)
                .join(purchase_items, "purchase_pk")
                .join(F.broadcast(products), "product_pk")
                .filter(F.col("category_name") == "молочные продукты")
                .select("customer_pk")
                .distinct()
                .withColumn("bought_milk_last_30d", F.lit(1))
            )

            if milk_purchases.isEmpty():
                logger.warning("⚠️ bought_milk_last_30d пустой")
                feature_dfs["milk_purchases"] = empty_milk
            else:
                feature_dfs["milk_purchases"] = self.cast_to_schema(
                    milk_purchases, milk_purchases_schema
                )
                logger.info(f"✅ bought_milk_last_30d: "
                            f"{feature_dfs['milk_purchases'].count()}")
        except Exception as e:
            logger.error(f"❌ Ошибка в bought_milk_last_30d: {e}")
            feature_dfs["milk_purchases"] = empty_milk

        # 2. bought_fruits_last_14d (категория фрукты)
        # Покупал фрукты и ягоды за последние 14 дней
        fruits_purchases_schema = StructType([
            StructField("customer_pk", LongType(), True),
            StructField("bought_fruits_last_14d", ByteType(), True)
        ])
        empty_fruits = (
            self.spark.createDataFrame([], schema=fruits_purchases_schema)
        )

        try:
            fruits_purchases = (
                purchases.filter(F.col("purchase_date") >= self.cutoff_14d)
                .join(purchase_items, "purchase_pk")
                .join(F.broadcast(products), "product_pk")
                .filter(F.col("category_name") == "фрукты и ягоды")
                .select("customer_pk")
                .distinct()
                .withColumn("bought_fruits_last_14d", F.lit(1))
            )

            if fruits_purchases.isEmpty():
                logger.warning("⚠️ bought_fruits_last_14d пустой")
                feature_dfs["fruits_purchases"] = empty_fruits
            else:
                feature_dfs["fruits_purchases"] = self.cast_to_schema(
                    fruits_purchases, fruits_purchases_schema
                )
                logger.info(f"✅ bought_fruits_last_14d: "
                            f"{feature_dfs['fruits_purchases'].count()}")
        except Exception as e:
            logger.error(f"❌ Ошибка в bought_fruits_last_14d: {e}")
            feature_dfs["fruits_purchases"] = empty_fruits

        # 3. not_bought_veggies_14d
        # Не покупал овощи и зелень за последние 14 дней
        not_veggies_schema = StructType([
            StructField("customer_pk", LongType(), True),
            StructField("not_bought_veggies_14d", ByteType(), True)
        ])
        empty_not_veggies = (
            self.spark.createDataFrame([], schema=not_veggies_schema)
        )

        try:
            veggies_purchases = (
                purchases.filter(F.col("purchase_date") >= self.cutoff_14d)
                .join(purchase_items, "purchase_pk")
                .join(F.broadcast(products), "product_pk")
                .filter(F.col("category_name") == "овощи и зелень")
                .select("customer_pk")
                .distinct()
            )

            # Не покупал овощи
            not_veggies = (
                customers.select("customer_pk")
                .join(veggies_purchases, "customer_pk", "left_anti")
                .withColumn("not_bought_veggies_14d", F.lit(1))
            )

            if not_veggies.isEmpty():
                logger.warning("⚠️ not_bought_veggies_14d пустой")
                feature_dfs["not_veggies"] = empty_not_veggies
            else:
                feature_dfs["not_veggies"] = self.cast_to_schema(
                    not_veggies, not_veggies_schema
                )
                logger.info(f"✅ not_bought_veggies_14d: "
                            f"{feature_dfs['not_veggies'].count()}")
        except Exception as e:
            logger.error(f"❌ Ошибка в not_bought_veggies_14d: {e}")
            feature_dfs["not_veggies"] = empty_not_veggies

        # 4. recurrent_buyer (>2 покупок за 30 дней)
        # Делал более 2 покупок за последние 30 дней
        recurrent_schema = StructType([
            StructField("customer_pk", LongType(), True),
            StructField("recurrent_buyer", ByteType(), True)
        ])
        empty_recurrent = (
            self.spark.createDataFrame([], schema=recurrent_schema)
        )

        try:
            recurrent = (
                purchases.filter(F.col("purchase_date") >= self.cutoff_30d)
                .groupBy("customer_pk")
                .agg(F.count("*").alias("purchase_count_30d"))
                .withColumn(
                    "recurrent_buyer",
                    F.when(F.col("purchase_count_30d") > 2, 1).otherwise(0),
                )
            )

            if recurrent.isEmpty():
                logger.warning("⚠️ recurrent_buyer пустой")
                feature_dfs["recurrent"] = empty_recurrent
            else:
                feature_dfs["recurrent"] = self.cast_to_schema(
                    recurrent, recurrent_schema
                )
                logger.info(f"✅ recurrent_buyer: "
                            f"{feature_dfs['recurrent'].count()}")
        except Exception as e:
            logger.error(f"❌ Ошибка в recurrent_buyer: {e}")
            feature_dfs["recurrent"] = empty_recurrent

        # 5. inactive_14_30 (покупал 14-30 дней назад, но не в последние 14)
        # Не покупал 14–30 дней (ушедший клиент?)
        inactive_schema = StructType([
            StructField("customer_pk", LongType(), True),
            StructField("inactive_14_30", ByteType(), True)
        ])
        empty_inactive = self.spark.createDataFrame([], schema=inactive_schema)

        try:
            last_purchase = purchases.groupBy("customer_pk").agg(
                F.max("purchase_date").alias("last_purchase_date")
            )

            inactive = last_purchase.withColumn(
                "inactive_14_30",
                F.when(
                    F.col("last_purchase_date").isNotNull()
                    & (F.col("last_purchase_date") < self.cutoff_14d)
                    & (F.col("last_purchase_date") >= self.cutoff_30d),
                    1,
                ).otherwise(0),
            )

            if inactive.isEmpty():
                logger.warning("⚠️ inactive_14_30 пустой")
                feature_dfs["inactive"] = empty_inactive
            else:
                feature_dfs["inactive"] = self.cast_to_schema(
                    inactive, inactive_schema
                )
                logger.info(f"✅ inactive_14_30: "
                            f"{feature_dfs['inactive'].count()}")
        except Exception as e:
            logger.error(f"❌ Ошибка в inactive_14_30: {e}")
            feature_dfs["inactive"] = empty_inactive

        # 6. new_customer (зарегистрирован <30 дней назад)
        # Покупатель зарегистрировался менее 30 дней назад
        new_cust_schema = StructType([
            StructField("customer_pk", LongType(), True),
            StructField("new_customer", ByteType(), True)
        ])
        empty_new_cust = (
            self.spark.createDataFrame([], schema=new_cust_schema)
        )

        try:
            new_cust = customers.withColumn(
                "new_customer",
                F.when(
                    F.col("reg_date") >= self.cutoff_30d,
                    1
                ).otherwise(0)
            )

            if new_cust.isEmpty():
                logger.warning("⚠️ new_customer пустой")
                feature_dfs["new_cust"] = empty_new_cust
            else:
                feature_dfs["new_cust"] = self.cast_to_schema(
                    new_cust, new_cust_schema
                )
                logger.info(f"✅ new_customer: "
                            f"{feature_dfs['new_cust'].count()}")
        except Exception as e:
            logger.error(f"❌ Ошибка в new_customer: {e}")
            feature_dfs["new_cust"] = empty_new_cust

        # 7. delivery_user (пользовался доставкой хотя бы раз)
        # Пользовался доставкой хотя бы раз
        delivery_schema = StructType([
            StructField("customer_pk", LongType(), True),
            StructField("delivery_user", ByteType(), True)
        ])
        empty_delivery = (
            self.spark.createDataFrame([], schema=delivery_schema)
        )

        try:
            delivery = (
                purchases.filter(F.col("is_delivery") == True)
                .select("customer_pk")
                .distinct()
                .withColumn("delivery_user", F.lit(1))
            )

            if delivery.isEmpty():
                logger.warning("⚠️ delivery_user пустой")
                feature_dfs["delivery"] = empty_delivery
            else:
                feature_dfs["delivery"] = self.cast_to_schema(
                    delivery, delivery_schema
                )
                logger.info(f"✅ delivery_user: "
                            f"{feature_dfs['delivery'].count()}")
        except Exception as e:
            logger.error(f"❌ Ошибка в delivery_user: {e}")
            feature_dfs["delivery"] = empty_delivery

        # 8. organic_preference (купил хотя бы 1 органический продукт)
        # Купил хотя бы 1 органический продукт
        organic_schema = StructType([
            StructField("customer_pk", LongType(), True),
            StructField("organic_preference", ByteType(), True)
        ])
        empty_organic = (
            self.spark.createDataFrame([], schema=organic_schema)
        )

        try:
            organic = (
                purchases.join(purchase_items, "purchase_pk")
                .join(F.broadcast(products), "product_pk")
                .filter(F.col("is_organic") == True)
                .select("customer_pk")
                .distinct()
                .withColumn("organic_preference", F.lit(1))
            )

            if organic.isEmpty():
                logger.warning("⚠️ organic_preference пустой")
                feature_dfs["organic"] = empty_organic
            else:
                feature_dfs["organic"] = self.cast_to_schema(
                    organic, organic_schema
                )
                logger.info(f"✅ organic_preference: "
                            f"{feature_dfs['organic'].count()}")
        except Exception as e:
            logger.error(f"❌ Ошибка в organic_preference: {e}")
            feature_dfs["organic"] = empty_organic

        # 9-10. bulk_buyer / low_cost_buyer (средняя корзина)
        # Средняя корзина > 1000₽
        # Средняя корзина < 200₽
        avg_basket_schema = StructType([
            StructField("customer_pk", LongType(), True),
            StructField("bulk_buyer", ByteType(), True),
            StructField("low_cost_buyer", ByteType(), True)
        ])
        empty_avg_basket = (
            self.spark.createDataFrame([], schema=avg_basket_schema)
        )

        try:
            avg_basket = (
                purchases.groupBy("customer_pk")
                .agg(F.avg("total_amount").alias("avg_basket"))
                .withColumn(
                    "bulk_buyer",
                    F.when(
                        F.col("avg_basket") > 1000,
                        1
                    ).otherwise(0)
                )
                .withColumn(
                    "low_cost_buyer",
                    F.when(
                        F.col("avg_basket") < 200,
                        1
                    ).otherwise(0)
                )
            )

            if avg_basket.isEmpty():
                logger.warning("⚠️ avg_basket пустой")
                feature_dfs["avg_basket"] = empty_avg_basket
            else:
                feature_dfs["avg_basket"] = self.cast_to_schema(
                    avg_basket, avg_basket_schema
                )
                logger.info(f"✅ avg_basket: "
                            f"{feature_dfs['avg_basket'].count()}")
        except Exception as e:
            logger.error(f"❌ Ошибка в avg_basket: {e}")
            feature_dfs["avg_basket"] = empty_avg_basket

        # 11. buys_bakery (хлеб/выпечка)
        # Покупал хлеб/выпечку хотя бы раз
        bakery_keywords = ["булочка", "хлеб", "лаваш", "пряник",
                           "вафли", "кекс", "батон", "печенье"]
        condition = (F.lower(F.col("product_name"))
                     .rlike('|'.join(bakery_keywords)))

        bakery_schema = StructType([
            StructField("customer_pk", LongType(), True),
            StructField("buys_bakery", ByteType(), True)
        ])
        empty_bakery = (
            self.spark.createDataFrame([], schema=bakery_schema)
        )

        try:
            bakery = (
                purchases.join(purchase_items, "purchase_pk")
                .join(F.broadcast(products), "product_pk")
                .filter(condition)
                .select("customer_pk")
                .distinct()
                .withColumn("buys_bakery", F.lit(1))
            )

            if bakery.isEmpty():
                logger.warning("⚠️ buys_bakery пустой")
                feature_dfs["bakery"] = empty_bakery
            else:
                feature_dfs["bakery"] = self.cast_to_schema(
                    bakery, bakery_schema
                )
                logger.info(f"✅ buys_bakery: "
                            f"{feature_dfs['bakery'].count()}")
        except Exception as e:
            logger.error(f"❌ Ошибка в buys_bakery: {e}")
            feature_dfs["bakery"] = empty_bakery

        # 12. loyal_customer (карта и ≥3 покупки)
        # Лояльный клиент (карта и ≥3 покупки)
        loyal_schema = StructType([
            StructField("customer_pk", LongType(), True),
            StructField("loyal_customer", ByteType(), True)
        ])
        empty_loyal = self.spark.createDataFrame([], schema=loyal_schema)

        try:
            loyal = (
                customers.filter(F.col("is_loyalty_member") == True)
                .join(purchases, "customer_pk")
                .groupBy("customer_pk")
                .agg(F.countDistinct("purchase_pk").alias("purchase_count"))
                .withColumn(
                    "loyal_customer",
                    F.when((F.col("purchase_count") >= 3), 1).otherwise(0)
                )
            )

            if loyal.isEmpty():
                logger.warning("⚠️ loyal_customer пустой")
                feature_dfs["loyal"] = empty_loyal
            else:
                feature_dfs["loyal"] = self.cast_to_schema(
                    loyal, loyal_schema
                )
                logger.info(f"✅ loyal_customer: "
                            f"{feature_dfs['loyal'].count()}")
        except Exception as e:
            logger.error(f"❌ Ошибка в loyal_customer: {e}")
            feature_dfs["loyal"] = empty_loyal

        # 13. multicity_buyer (покупки в разных городах)
        # Делал покупки в разных городах
        customer_cities_schema = StructType([
            StructField("customer_pk", LongType(), True),
            StructField("multicity_buyer", ByteType(), True)
        ])
        empty_customer_cities = (
            self.spark.createDataFrame([], schema=customer_cities_schema)
        )

        try:
            city_count_df = (
                purchases.alias("p")
                .join(F.broadcast(stores).alias("s"), "store_pk")
                .select(
                    F.col("p.customer_pk"),
                    F.col("p.purchase_pk"),
                    F.coalesce(
                        F.when(
                            F.col("p.is_delivery") == True,
                            F.col("p.delivery_city_id")
                        ),
                        F.col("s.store_city_id")
                    ).alias("city_id"),
                )
                .groupBy("customer_pk")
                .agg(F.countDistinct("city_id").alias("city_count"))
            )

            customer_cities = city_count_df.withColumn(
                "multicity_buyer",
                F.when(F.col("city_count") > 1,
                       1).otherwise(0)
            ).select("customer_pk", "multicity_buyer")

            if customer_cities.isEmpty():
                logger.warning("⚠️ multicity_buyer пустой")
                feature_dfs["customer_cities"] = empty_customer_cities
            else:
                feature_dfs["customer_cities"] = self.cast_to_schema(
                    customer_cities, customer_cities_schema
                )
                logger.info(f"✅ multicity_buyer: "
                            f"{feature_dfs['customer_cities'].count()}")
        except Exception as e:
            logger.error(f"❌ Ошибка в multicity_buyer: {e}")
            feature_dfs["customer_cities"] = empty_customer_cities

        # 14. bought_meat_last_week (мясо/рыба/яйца)
        # Покупал мясо/рыбу/яйца за последнюю неделю
        meat_schema = StructType([
            StructField("customer_pk", LongType(), True),
            StructField("bought_meat_last_week", ByteType(), True)
        ])
        empty_meat = self.spark.createDataFrame([], schema=meat_schema)

        try:
            meat = (
                purchases.filter(F.col("purchase_date") >= self.cutoff_7d)
                .join(purchase_items, "purchase_pk")
                .join(F.broadcast(products), "product_pk")
                .filter(F.col("category_name") == "мясо, рыба, яйца и бобовые")
                .select("customer_pk")
                .distinct()
                .withColumn("bought_meat_last_week", F.lit(1))
            )

            if meat.isEmpty():
                logger.warning("⚠️ bought_meat_last_week пустой")
                feature_dfs["meat"] = empty_meat
            else:
                feature_dfs["meat"] = self.cast_to_schema(
                    meat, meat_schema
                )
                logger.info(f"✅ bought_meat_last_week: "
                            f"{feature_dfs['meat'].count()}")
        except Exception as e:
            logger.error(f"❌ Ошибка в bought_meat_last_week: {e}")
            feature_dfs["meat"] = empty_meat

        # 15-16. night/morning shopper
        # Делал покупки после 20:00
        # Делал покупки до 10:00
        time_pref_schema = StructType([
            StructField("customer_pk", LongType(), True),
            StructField("night_shopper", ByteType(), True),
            StructField("morning_shopper", ByteType(), True)
        ])
        empty_time_pref = (
            self.spark.createDataFrame([], schema=time_pref_schema)
        )

        try:
            time_pref = (
                purchases.groupBy("customer_pk")
                .agg(
                    F.sum(
                        F.when(
                            F.col("purchase_hour") >= 20,
                            1
                        ).otherwise(0)).alias(
                        "night_count"
                    ),
                    F.sum(
                        F.when(
                            F.col("purchase_hour") < 10,
                            1
                        ).otherwise(0)).alias(
                        "morning_count"
                    ),
                    F.count("*").alias("total_purchases"),
                )
                .withColumn(
                    "night_shopper",
                    F.when(
                        F.col("night_count") > 0,
                        1
                    ).otherwise(0)
                )
                .withColumn(
                    "morning_shopper",
                    F.when(
                        F.col("morning_count") > 0,
                        1
                    ).otherwise(0)
                )
            )

            if time_pref.isEmpty():
                logger.warning("⚠️ time_pref пустой")
                feature_dfs["time_pref"] = empty_time_pref
            else:
                feature_dfs["time_pref"] = self.cast_to_schema(
                    time_pref, time_pref_schema
                )
                logger.info(f"✅ time_pref: "
                            f"{feature_dfs['time_pref'].count()}")
        except Exception as e:
            logger.error(f"❌ Ошибка в time_pref: {e}")
            feature_dfs["time_pref"] = empty_time_pref

        # 17-18. prefers_cash / prefers_card
        # Оплачивал наличными ≥ 70 % покупок
        # Оплачивал картой ≥ 70% покупок
        pay_pref_schema = StructType([
            StructField("customer_pk", LongType(), True),
            StructField("prefers_cash", ByteType(), True),
            StructField("prefers_card", ByteType(), True)
        ])
        empty_pay_pref = self.spark.createDataFrame([], schema=pay_pref_schema)

        try:
            pay_pref = (
                purchases.groupBy("customer_pk")
                .agg(
                    F.sum(
                        F.when(
                            F.col("payment_method_name") == "cash",
                            1
                        ).otherwise(0)
                    ).alias("cash_count"),
                    F.sum(
                        F.when(
                            F.col("payment_method_name") == "card",
                            1
                        ).otherwise(0)
                    ).alias("card_count"),
                    F.count("*").alias("total_purchases_pay"),
                )
                .withColumn(
                    "prefers_cash",
                    F.when(
                        (F.col("total_purchases_pay") > 0),
                        (F.col("cash_count") /
                         F.col("total_purchases_pay") >= 0.7)
                        .cast("int"),
                    ).otherwise(0),
                )
                .withColumn(
                    "prefers_card",
                    F.when(
                        (F.col("total_purchases_pay") > 0),
                        (F.col("card_count") /
                         F.col("total_purchases_pay") >= 0.7)
                        .cast("int"),
                    ).otherwise(0),
                )
            )

            if pay_pref.isEmpty():
                logger.warning("⚠️ pay_pref пустой")
                feature_dfs["pay_pref"] = empty_pay_pref
            else:
                feature_dfs["pay_pref"] = self.cast_to_schema(
                    pay_pref, pay_pref_schema
                )
                logger.info(f"✅ pay_pref: {feature_dfs['pay_pref'].count()}")
        except Exception as e:
            logger.error(f"❌ Ошибка в pay_pref: {e}")
            feature_dfs["pay_pref"] = empty_pay_pref

        # 19-20. weekend/weekday shopper
        # Делал ≥ 60% покупок в выходные
        # Делал ≥ 60% покупок в будни
        weekend_pref_schema = StructType([
            StructField("customer_pk", LongType(), True),
            StructField("weekend_shopper", ByteType(), True),
            StructField("weekday_shopper", ByteType(), True)
        ])
        empty_weekend_pref = (
            self.spark.createDataFrame([], schema=weekend_pref_schema)
        )

        try:
            weekend_pref = (
                purchases.withColumn(
                    "is_weekend",
                    F.when(
                        F.col("weekday").isin(["Saturday", "Sunday"]),
                        1
                    ).otherwise(0),
                )
                .groupBy("customer_pk")
                .agg(
                    F.avg("is_weekend").alias("weekend_ratio"),
                    F.count("*").alias("total_purchases_week"),
                )
                .withColumn(
                    "weekend_shopper",
                    F.when(F.col("weekend_ratio") >= 0.6, 1).otherwise(0)
                )
                .withColumn(
                    "weekday_shopper",
                    F.when(F.col("weekend_ratio") <= 0.4, 1).otherwise(0)
                )
            )

            if weekend_pref.isEmpty():
                logger.warning("⚠️ weekend_pref пустой")
                feature_dfs["weekend_pref"] = empty_weekend_pref
            else:
                feature_dfs["weekend_pref"] = self.cast_to_schema(
                    weekend_pref, weekend_pref_schema
                )
                logger.info(f"✅ weekend_pref: "
                            f"{feature_dfs['weekend_pref'].count()}")
        except Exception as e:
            logger.error(f"❌ Ошибка в weekend_pref: {e}")
            feature_dfs["weekend_pref"] = empty_weekend_pref

        # 21. single_item_buyer (≥50% покупок — 1 товар)
        # ≥50% покупок — 1 товар в корзине
        single_item_schema = StructType([
            StructField("customer_pk", LongType(), True),
            StructField("single_item_buyer", ByteType(), True)
        ])
        empty_single_item = (
            self.spark.createDataFrame([], schema=single_item_schema)
        )

        try:
            item_count_per_purchase = (
                purchase_items
                .groupBy("purchase_pk")
                .agg(
                    F.countDistinct("item_pk")
                    .alias("items_count")
                )
            )

            single_item = (
                purchases.join(item_count_per_purchase, "purchase_pk")
                .groupBy("customer_pk")
                .agg(
                    F.avg(
                        F.when(
                            F.col("items_count") == 1,
                            1
                        ).otherwise(0)).alias(
                        "single_item_ratio"
                    )
                )
                .withColumn(
                    "single_item_buyer",
                    F.when(
                        F.col("single_item_ratio") >= 0.5,
                        1
                    ).otherwise(0),
                )
            )

            if single_item.isEmpty():
                logger.warning("⚠️ single_item_buyer пустой")
                feature_dfs["single_item"] = empty_single_item
            else:
                feature_dfs["single_item"] = self.cast_to_schema(
                    single_item, single_item_schema
                )
                logger.info(f"✅ single_item_buyer: "
                            f"{feature_dfs['single_item'].count()}")
        except Exception as e:
            logger.error(f"❌ Ошибка в single_item_buyer: {e}")
            feature_dfs["single_item"] = empty_single_item

        # 22. varied_shopper (≥4 категорий)
        # Покупал ≥4 разных категорий продуктов
        varied_schema = StructType([
            StructField("customer_pk", LongType(), True),
            StructField("varied_shopper", ByteType(), True)
        ])
        empty_varied = self.spark.createDataFrame([], schema=varied_schema)

        try:
            varied = (
                purchases.join(purchase_items, "purchase_pk")
                .join(F.broadcast(products), "product_pk")
                .groupBy("customer_pk")
                .agg(F.countDistinct("category_name").alias("category_count"))
                .withColumn(
                    "varied_shopper",
                    F.when(F.col("category_count") >= 4, 1).otherwise(0)
                )
            )

            if varied.isEmpty():
                logger.warning("⚠️ varied_shopper пустой")
                feature_dfs["varied"] = empty_varied
            else:
                feature_dfs["varied"] = self.cast_to_schema(
                    varied, varied_schema
                )
                logger.info(f"✅ varied_shopper: "
                            f"{feature_dfs['varied'].count()}")
        except Exception as e:
            logger.error(f"❌ Ошибка в varied_shopper: {e}")
            feature_dfs["varied"] = empty_varied

        # 23-24. store_loyal / switching_store
        # Ходит только в один магазин
        # Ходит в разные магазины
        store_loyalty_schema = StructType([
            StructField("customer_pk", LongType(), True),
            StructField("store_loyal", ByteType(), True),
            StructField("switching_store", ByteType(), True)
        ])
        empty_store_loyalty = (
            self.spark.createDataFrame([], schema=store_loyalty_schema))

        try:
            store_loyalty = (
                purchases.filter(F.col("is_delivery") == False)
                .groupBy("customer_pk")
                .agg(
                    F.countDistinct("store_pk").alias("store_count"),
                    F.count("*").alias("total_purchases_store"),
                )
                .withColumn(
                    "store_loyal",
                    F.when(F.col("store_count") == 1, 1).otherwise(0)
                )
                .withColumn(
                    "switching_store",
                    F.when(F.col("store_count") > 1, 1).otherwise(0)
                )
            )

            if store_loyalty.isEmpty():
                logger.warning("⚠️ store_loyalty пустой")
                feature_dfs["store_loyalty"] = empty_store_loyalty
            else:
                feature_dfs["store_loyalty"] = self.cast_to_schema(
                    store_loyalty, store_loyalty_schema
                )
                logger.info(f"✅ store_loyalty: "
                            f"{feature_dfs['store_loyalty'].count()}")
        except Exception as e:
            logger.error(f"❌ Ошибка в store_loyalty: {e}")
            feature_dfs["store_loyalty"] = empty_store_loyalty

        # 25. family_shopper (среднее кол-во позиций ≥4)
        # Среднее кол-во позиций в корзине ≥4
        family_schema = StructType([
            StructField("customer_pk", LongType(), True),
            StructField("family_shopper", ByteType(), True)
        ])
        empty_family = self.spark.createDataFrame([], schema=family_schema)

        try:
            family_count = (
                purchases.join(purchase_items, "purchase_pk")
                .groupBy("customer_pk", "purchase_pk")
                .agg(F.count("item_pk").alias("count_items"))
            )

            family = (
                family_count.groupBy("customer_pk")
                .agg(F.avg("count_items").alias("avg_items"))
                .select("customer_pk", "avg_items")
                .withColumn(
                    "family_shopper",
                    F.when(F.col("avg_items") >= 4, 1).otherwise(0)
                )
            )

            if family.isEmpty():
                logger.warning("⚠️ family_shopper пустой")
                feature_dfs["family"] = empty_family
            else:
                feature_dfs["family"] = self.cast_to_schema(
                    family, family_schema
                )
                logger.info(f"✅ family_shopper: "
                            f"{feature_dfs['family'].count()}")
        except Exception as e:
            logger.error(f"❌ Ошибка в family_shopper: {e}")
            feature_dfs["family"] = empty_family

        # 26. early_bird (покупка между 12 и 15)
        # Покупка в промежутке между 12 и 15 часами дня
        early_schema = StructType([
            StructField("customer_pk", LongType(), True),
            StructField("early_bird", ByteType(), True)
        ])
        empty_early = self.spark.createDataFrame([], schema=early_schema)

        try:
            early = (
                purchases.filter(
                    (F.col("purchase_hour") >= 12) &
                    (F.col("purchase_hour") <= 15)
                )
                .select("customer_pk")
                .distinct()
                .withColumn("early_bird", F.lit(1))
            )

            if early.isEmpty():
                logger.warning("⚠️ early_bird пустой")
                feature_dfs["early"] = empty_early
            else:
                feature_dfs["early"] = self.cast_to_schema(
                    early, early_schema
                )
                logger.info(f"✅ early_bird: {feature_dfs['early'].count()}")
        except Exception as e:
            logger.error(f"❌ Ошибка в early_bird: {e}")
            feature_dfs["early"] = empty_early

        # 27. no_purchases (только регистрация)
        # Не совершал ни одной покупки (только регистрация)
        no_purch_schema = StructType([
            StructField("customer_pk", LongType(), True),
            StructField("no_purchases", ByteType(), True)
        ])
        empty_no_purch = self.spark.createDataFrame([], schema=no_purch_schema)

        try:
            no_purch = customers.join(
                purchases
                .select("customer_pk").distinct(), "customer_pk", "left_anti"
            ).withColumn("no_purchases", F.lit(1))

            if no_purch.isEmpty():
                logger.warning("⚠️ no_purchases пустой")
                feature_dfs["no_purch"] = empty_no_purch
            else:
                feature_dfs["no_purch"] = self.cast_to_schema(
                    no_purch, no_purch_schema
                )
                logger.info(f"✅ no_purchases: "
                            f"{feature_dfs['no_purch'].count()}")
        except Exception as e:
            logger.error(f"❌ Ошибка в no_purchases: {e}")
            feature_dfs["no_purch"] = empty_no_purch

        # 28. recent_high_spender (>2000₽ за 7 дней)
        # Купил на сумму >2000₽ за последние 7 дней
        high_spender_schema = StructType([
            StructField("customer_pk", LongType(), True),
            StructField("recent_high_spender", ByteType(), True)
        ])
        empty_high_spender = (
            self.spark.createDataFrame([], schema=high_spender_schema)
        )

        try:
            high_spender = (
                purchases.filter(F.col("purchase_date") >= self.cutoff_7d)
                .groupBy("customer_pk")
                .agg(F.sum("total_amount").alias("spent_7d"))
                .withColumn(
                    "recent_high_spender",
                    F.when(F.col("spent_7d") > 2000, 1).otherwise(0)
                )
            )

            if high_spender.isEmpty():
                logger.warning("⚠️ recent_high_spender пустой")
                feature_dfs["high_spender"] = empty_high_spender
            else:
                feature_dfs["high_spender"] = self.cast_to_schema(
                    high_spender, high_spender_schema
                )
                logger.info(f"✅ recent_high_spender: "
                            f"{feature_dfs['high_spender'].count()}")
        except Exception as e:
            logger.error(f"❌ Ошибка в recent_high_spender: {e}")
            feature_dfs["high_spender"] = empty_high_spender

        # 29. fruit_lover (≥3 покупок фруктов за 30 дней)
        # ≥3 покупок фруктов за 30 дней
        fruit_lover_schema = StructType([
            StructField("customer_pk", LongType(), True),
            StructField("fruit_lover", ByteType(), True)
        ])
        empty_fruit_lover = (
            self.spark.createDataFrame([], schema=fruit_lover_schema)
        )

        try:
            fruit_lover = (
                purchases.filter(F.col("purchase_date") >= self.cutoff_30d)
                .join(purchase_items, "purchase_pk")
                .join(F.broadcast(products), "product_pk")
                .filter(F.col("category_name") == "фрукты и ягоды")
                .groupBy("customer_pk")
                .agg(F.countDistinct("purchase_pk").alias("fruit_purchases"))
                .withColumn(
                    "fruit_lover",
                    F.when(F.col("fruit_purchases") >= 3, 1).otherwise(0)
                )
            )

            if fruit_lover.isEmpty():
                logger.warning("⚠️ fruit_lover пустой")
                feature_dfs["fruit_lover"] = empty_fruit_lover
            else:
                feature_dfs["fruit_lover"] = self.cast_to_schema(
                    fruit_lover, fruit_lover_schema
                )
                logger.info(f"✅ fruit_lover: "
                            f"{feature_dfs['fruit_lover'].count()}")
        except Exception as e:
            logger.error(f"❌ Ошибка в fruit_lover: {e}")
            feature_dfs["fruit_lover"] = empty_fruit_lover

        # 30. vegetarian_profile (нет мясных продуктов за 90 дней)
        # Не купил ни одного мясного продукта за 90 дней
        vegetarian_schema = StructType([
            StructField("customer_pk", LongType(), True),
            StructField("vegetarian_profile", ByteType(), True)
        ])
        empty_vegetarian = (
            self.spark.createDataFrame([], schema=vegetarian_schema)
        )

        try:
            # Покупатели мяса
            meat_buyers = (
                purchases.filter(F.col("purchase_date") >= self.cutoff_90d)
                .join(purchase_items, "purchase_pk")
                .join(F.broadcast(products), "product_pk")
                .filter(F.col("category_name") == "мясо, рыба, яйца и бобовые")
                .select("customer_pk")
                .distinct()
            )

            # Вегетарианцы
            vegetarian = (
                customers.select("customer_pk")
                .join(meat_buyers, "customer_pk", "left_anti")
                .withColumn("vegetarian_profile", F.lit(1))
            )

            if vegetarian.isEmpty():
                logger.warning("⚠️ vegetarian_profile пустой")
                feature_dfs["vegetarian"] = empty_vegetarian
            else:
                feature_dfs["vegetarian"] = self.cast_to_schema(
                    vegetarian, vegetarian_schema
                )
                logger.info(f"✅ vegetarian_profile: "
                            f"{feature_dfs['vegetarian'].count()}")
        except Exception as e:
            logger.error(f"❌ Ошибка в vegetarian_profile: {e}")
            feature_dfs["vegetarian"] = empty_vegetarian

        logger.info("Собираем всё в один DataFrame")

        # ШАГ 1: Начинаем с customers (все клиенты)
        features = customers.select("customer_pk", "customer_id")

        # 1. Проверяем на пустоту финальный DF
        if features.isEmpty():
            raise ValueError("❌ Финальный features пустой!")

        # 2. Проверка на дубликаты по customer_id финальный DF
        dupes = (features.groupBy("customer_id").count().
                 filter("count > 1").count())
        if dupes > 0:
            logger.warning(f"⚠️ Найдено {dupes} дубликатов customer_id")

        # Формируем словарь с колонками для каждого признака
        feature_columns = {
            "milk_purchases": ["bought_milk_last_30d"],
            "fruits_purchases": ["bought_fruits_last_14d"],
            "not_veggies": ["not_bought_veggies_14d"],
            "recurrent": ["recurrent_buyer"],
            "inactive": ["inactive_14_30"],
            "new_cust": ["new_customer"],
            "delivery": ["delivery_user"],
            "organic": ["organic_preference"],
            "avg_basket": ["bulk_buyer", "low_cost_buyer"],
            "bakery": ["buys_bakery"],
            "loyal": ["loyal_customer"],
            "customer_cities": ["multicity_buyer"],
            "meat": ["bought_meat_last_week"],
            "time_pref": ["night_shopper", "morning_shopper"],
            "pay_pref": ["prefers_cash", "prefers_card"],
            "weekend_pref": ["weekend_shopper", "weekday_shopper"],
            "single_item": ["single_item_buyer"],
            "varied": ["varied_shopper"],
            "store_loyalty": ["store_loyal", "switching_store"],
            "family": ["family_shopper"],
            "early": ["early_bird"],
            "no_purch": ["no_purchases"],
            "high_spender": ["recent_high_spender"],
            "fruit_lover": ["fruit_lover"],
            "vegetarian": ["vegetarian_profile"]
        }
        # ШАГ 2: Добавляем все признаки через left join
        for feature_name, columns in feature_columns.items():
            if feature_name in feature_dfs:
                df_to_join = (feature_dfs[feature_name]
                              .select("customer_pk", *columns)
                              )
                features = features.join(df_to_join, "customer_pk", "left")

        # ШАГ 3: Кэшируем промежуточный результат
        features = features.cache()

        # ШАГ 4: Заполняем NULL → 0
        feature_cols = [c for c in features.columns
                        if c not in ["customer_pk", "customer_id"]]
        for col in feature_cols:
            features = features.withColumn(
                col,
                F.coalesce(F.col(col), F.lit(0))
            )

        # ШАГ 5: Финальные проверки
        features = features.dropDuplicates(["customer_pk"])
        logger.info(f"Всего признаков рассчитано: {len(feature_cols)}")
        logger.info(f"Клиентов с признаками: {features.count()}")
        return features.select("customer_id", *feature_cols)
