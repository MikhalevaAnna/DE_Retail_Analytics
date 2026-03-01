from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
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

        Returns:
            Tuple[DataFrame, DataFrame, DataFrame, DataFrame]:
                Кортеж из обработанных DataFrame
                (customers, purchases, products, stores)
        """

        # purchases с датой покупки (уже UTC)
        purchases = (
            purchases_df.select(
                "purchase_pk",
                "customer_pk",
                "store_pk",
                "delivery_address_id",
                "delivery_city_id",
                "total_amount",
                "payment_method_name",
                "is_delivery",
                F.to_timestamp("purchase_datetime", "yyyy-MM-dd HH:mm:ss").alias(
                    "purchase_datetime"
                ),
            )
            .withColumn("purchase_date", F.to_date("purchase_datetime"))
            .withColumn("purchase_hour", F.hour("purchase_datetime"))
            .withColumn("weekday", F.date_format("purchase_datetime", "EEEE"))
        )

        # customers с датой регистрации
        customers = customers_df.select(
            "customer_pk",
            "customer_id",
            "registration_date",
            "is_loyalty_member",
            F.to_date("registration_date").alias("reg_date"),
        )

        # products с категориями и organic
        products = products_df.select(
            "product_pk", "product_name", "price", "category_name", "is_organic"
        ).distinct()

        # purchase_item_details с деталями покупок
        # (нужно читать отдельно, но для упрощения будем джойнить)

        # stores с id городов
        stores = stores_df.select(
            "store_pk", "store_delivery_available", "store_city_id"
        )

        return customers, purchases, products, stores

    def calculate_features(
        self,
        customers_df: DataFrame,
        purchases_df: DataFrame,
        products_df: DataFrame,
        stores_df: DataFrame,
        purchase_items_df: DataFrame,
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

        Returns:
            DataFrame: Результирующий DataFrame с колонками:
                - customer_id: идентификатор клиента
                - [30 признаков]: бинарные признаки (0 или 1)

        Note:
            Все NULL значения в финальном DataFrame заменяются на 0
            Используется кэширование для purchases и products для оптимизации
        """

        # Подготовка данных
        customers, purchases, products, stores = self.prepare_data(
            customers_df, purchases_df, products_df, stores_df
        )
        purchases.cache()
        products.cache()
        # 1. bought_milk_last_30d (категория молочные)
        # Покупал молочные продукты за последние 30 дней
        milk_purchases = (
            purchases.filter(F.col("purchase_date") >= self.cutoff_30d)
            .join(purchase_items_df, "purchase_pk")
            .join(products, "product_pk")
            .filter(F.col("category_name") == "молочные продукты")
            .select("customer_pk")
            .distinct()
            .withColumn("bought_milk_last_30d", F.lit(1))
        )

        # 2. bought_fruits_last_14d (категория фрукты)
        # Покупал фрукты и ягоды за последние 14 дней
        fruits_purchases = (
            purchases.filter(F.col("purchase_date") >= self.cutoff_14d)
            .join(purchase_items_df, "purchase_pk")
            .join(products, "product_pk")
            .filter(F.col("category_name") == "фрукты и ягоды")
            .select("customer_pk")
            .distinct()
            .withColumn("bought_fruits_last_14d", F.lit(1))
        )

        # 3. not_bought_veggies_14d
        # Не покупал овощи и зелень за последние 14 дней
        veggies_purchases = (
            purchases.filter(F.col("purchase_date") >= self.cutoff_14d)
            .join(purchase_items_df, "purchase_pk")
            .join(products, "product_pk")
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

        # 4. recurrent_buyer (>2 покупок за 30 дней)
        # Делал более 2 покупок за последние 30 дней
        recurrent = (
            purchases.filter(F.col("purchase_date") >= self.cutoff_30d)
            .groupBy("customer_pk")
            .agg(F.count("*").alias("purchase_count_30d"))
            .withColumn(
                "recurrent_buyer",
                F.when(F.col("purchase_count_30d") > 2, 1).otherwise(0),
            )
        )

        # 5. inactive_14_30 (покупал 14-30 дней назад, но не в последние 14)
        # Не покупал 14–30 дней (ушедший клиент?)
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

        # 6. new_customer (зарегистрирован <30 дней назад)
        # Покупатель зарегистрировался менее 30 дней назад
        new_cust = customers.withColumn(
            "new_customer", F.when(F.col("reg_date") >= self.cutoff_30d, 1).otherwise(0)
        )

        # 7. delivery_user (пользовался доставкой хотя бы раз)
        # Пользовался доставкой хотя бы раз
        delivery = (
            purchases.filter(F.col("is_delivery") == True)
            .select("customer_pk")
            .distinct()
            .withColumn("delivery_user", F.lit(1))
        )

        # 8. organic_preference (купил хотя бы 1 органический продукт)
        # Купил хотя бы 1 органический продукт
        organic = (
            purchases.join(purchase_items_df, "purchase_pk")
            .join(products, "product_pk")
            .filter(F.col("is_organic") == True)
            .select("customer_pk")
            .distinct()
            .withColumn("organic_preference", F.lit(1))
        )

        # 9-10. bulk_buyer / low_cost_buyer (средняя корзина)
        # Средняя корзина > 1000₽
        # Средняя корзина < 200₽
        avg_basket = (
            purchases.groupBy("customer_pk")
            .agg(F.avg("total_amount").alias("avg_basket"))
            .withColumn(
                "bulk_buyer", F.when(F.col("avg_basket") > 1000, 1).otherwise(0)
            )
            .withColumn(
                "low_cost_buyer", F.when(F.col("avg_basket") < 200, 1).otherwise(0)
            )
        )

        # 11. buys_bakery (хлеб/выпечка)
        # Покупал хлеб/выпечку хотя бы раз
        bakery_keywords = ["булочка", "хлеб", "лаваш", "пряник",
                           "вафли", "кекс", "батон", "печенье"]
        condition = (F.lower(F.col("product_name"))
                     .rlike('|'.join(bakery_keywords)))
        bakery = (
            purchases.join(purchase_items_df, "purchase_pk")
            .join(products, "product_pk")
            .filter(condition)
            .select("customer_pk")
            .distinct()
            .withColumn("buys_bakery", F.lit(1))
        )

        # 12. loyal_customer (карта и ≥3 покупки)
        # Лояльный клиент (карта и ≥3 покупки)
        loyal = (
            customers.filter(F.col("is_loyalty_member") == True)
            .join(purchases, "customer_pk")
            .groupBy("customer_pk")
            .agg(F.countDistinct("purchase_pk").alias("purchase_count"))
            .withColumn(
                "loyal_customer", F.when((F.col("purchase_count") >= 3), 1).otherwise(0)
            )
        )

        # 13. multicity_buyer (покупки в разных городах)
        # Делал покупки в разных городах
        city_count_df = (
            purchases.alias("p")
            .join(stores.alias("s"), "store_pk")
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
            "multicity_buyer", F.when(F.col("city_count") > 1, 1).otherwise(0)
        ).select("customer_pk", "multicity_buyer")

        # 14. bought_meat_last_week (мясо/рыба/яйца)
        # Покупал мясо/рыбу/яйца за последнюю неделю
        meat = (
            purchases.filter(F.col("purchase_date") >= self.cutoff_7d)
            .join(purchase_items_df, "purchase_pk")
            .join(products, "product_pk")
            .filter(F.col("category_name") == "мясо, рыба, яйца и бобовые")
            .select("customer_pk")
            .distinct()
            .withColumn("bought_meat_last_week", F.lit(1))
        )

        # 15-16. night/morning shopper
        # Делал покупки после 20:00
        # Делал покупки до 10:00
        time_pref = (
            purchases.groupBy("customer_pk")
            .agg(
                F.sum(F.when(F.col("purchase_hour") >= 20, 1).otherwise(0)).alias(
                    "night_count"
                ),
                F.sum(F.when(F.col("purchase_hour") < 10, 1).otherwise(0)).alias(
                    "morning_count"
                ),
                F.count("*").alias("total_purchases"),
            )
            .withColumn(
                "night_shopper", F.when(F.col("night_count") > 0, 1).otherwise(0)
            )
            .withColumn(
                "morning_shopper", F.when(F.col("morning_count") > 0, 1).otherwise(0)
            )
        )

        # 17-18. prefers_cash / prefers_card
        # Оплачивал наличными ≥ 70 % покупок
        # Оплачивал картой ≥ 70% покупок
        pay_pref = (
            purchases.groupBy("customer_pk")
            .agg(
                F.sum(
                    F.when(F.col("payment_method_name") == "cash", 1).otherwise(0)
                ).alias("cash_count"),
                F.sum(
                    F.when(F.col("payment_method_name") == "card", 1).otherwise(0)
                ).alias("card_count"),
                F.count("*").alias("total_purchases_pay"),
            )
            .withColumn(
                "prefers_cash",
                F.when(
                    (F.col("total_purchases_pay") > 0),
                    (F.col("cash_count") / F.col("total_purchases_pay") >= 0.7)
                    .cast("int"),
                ).otherwise(0),
            )
            .withColumn(
                "prefers_card",
                F.when(
                    (F.col("total_purchases_pay") > 0),
                    (F.col("card_count") / F.col("total_purchases_pay") >= 0.7)
                    .cast("int"),
                ).otherwise(0),
            )
        )

        # 19-20. weekend/weekday shopper
        # Делал ≥ 60% покупок в выходные
        # Делал ≥ 60% покупок в будни
        weekend_pref = (
            purchases.withColumn(
                "is_weekend",
                F.when(F.col("weekday").isin(["Saturday", "Sunday"]), 1).otherwise(0),
            )
            .groupBy("customer_pk")
            .agg(
                F.avg("is_weekend").alias("weekend_ratio"),
                F.count("*").alias("total_purchases_week"),
            )
            .withColumn(
                "weekend_shopper", F.when(F.col("weekend_ratio") >= 0.6, 1).otherwise(0)
            )
            .withColumn(
                "weekday_shopper", F.when(F.col("weekend_ratio") <= 0.4, 1).otherwise(0)
            )
        )

        # 21. single_item_buyer (≥50% покупок — 1 товар)
        # ≥50% покупок — 1 товар в корзине
        item_count_per_purchase = purchase_items_df.groupBy("purchase_pk").agg(
            F.sum("quantity").alias("items_count")
        )

        single_item = (
            purchases.join(item_count_per_purchase, "purchase_pk")
            .groupBy("customer_pk")
            .agg(
                F.avg(F.when(F.col("items_count") == 1, 1).otherwise(0)).alias(
                    "single_item_ratio"
                )
            )
            .withColumn(
                "single_item_buyer",
                F.when(F.col("single_item_ratio") >= 0.5, 1).otherwise(0),
            )
        )

        # 22. varied_shopper (≥4 категорий)
        # Покупал ≥4 разных категорий продуктов
        varied = (
            purchases.join(purchase_items_df, "purchase_pk")
            .join(products, "product_pk")
            .groupBy("customer_pk")
            .agg(F.countDistinct("category_name").alias("category_count"))
            .withColumn(
                "varied_shopper", F.when(F.col("category_count") >= 4, 1).otherwise(0)
            )
        )

        # 23-24. store_loyal / switching_store
        # Ходит только в один магазин
        # Ходит в разные магазины
        store_loyalty = (
            purchases.filter(F.col("is_delivery") == False)
            .groupBy("customer_pk")
            .agg(
                F.countDistinct("store_pk").alias("store_count"),
                F.count("*").alias("total_purchases_store"),
            )
            .withColumn(
                "store_loyal", F.when(F.col("store_count") == 1, 1).otherwise(0)
            )
            .withColumn(
                "switching_store", F.when(F.col("store_count") > 1, 1).otherwise(0)
            )
        )

        # 25. family_shopper (среднее кол-во позиций ≥4)
        # Среднее кол-во позиций в корзине ≥4
        family_count = (
            purchases.join(purchase_items_df, "purchase_pk")
            .groupBy("customer_pk", "purchase_pk")
            .agg(F.count("item_pk").alias("count_items"))
        )

        family = (
            family_count.groupBy("customer_pk")
            .agg(F.avg("count_items").alias("avg_items"))
            .select("customer_pk", "avg_items")
            .withColumn(
                "family_shopper", F.when(F.col("avg_items") >= 4, 1).otherwise(0)
            )
        )

        # 26. early_bird (покупка между 12 и 15)
        # Покупка в промежутке между 12 и 15 часами дня
        early = (
            purchases.filter(
                (F.col("purchase_hour") >= 12) & (F.col("purchase_hour") <= 15)
            )
            .select("customer_pk")
            .distinct()
            .withColumn("early_bird", F.lit(1))
        )

        # 27. no_purchases (только регистрация)
        # Не совершал ни одной покупки (только регистрация)
        no_purch = customers.join(
            purchases.select("customer_pk").distinct(), "customer_pk", "left_anti"
        ).withColumn("no_purchases", F.lit(1))

        # 28. recent_high_spender (>2000₽ за 7 дней)
        # Купил на сумму >2000₽ за последние 7 дней
        high_spender = (
            purchases.filter(F.col("purchase_date") >= self.cutoff_7d)
            .groupBy("customer_pk")
            .agg(F.sum("total_amount").alias("spent_7d"))
            .withColumn(
                "recent_high_spender", F.when(F.col("spent_7d") > 2000, 1).otherwise(0)
            )
        )

        # 29. fruit_lover (≥3 покупок фруктов за 30 дней)
        # ≥3 покупок фруктов за 30 дней
        fruit_lover = (
            purchases.filter(F.col("purchase_date") >= self.cutoff_30d)
            .join(purchase_items_df, "purchase_pk")
            .join(products, "product_pk")
            .filter(F.col("category_name") == "фрукты и ягоды")
            .groupBy("customer_pk", "purchase_pk")  # группируем по чеку
            .agg(F.count("*").alias("items_in_purchase"))
            .groupBy("customer_pk")
            .agg(F.count("purchase_pk").alias("fruit_purchases"))  # количество чеков
            .withColumn(
                "fruit_lover", F.when(F.col("fruit_purchases") >= 3, 1).otherwise(0)
            )
        )

        # 30. vegetarian_profile (нет мясных продуктов за 90 дней)
        # Не купил ни одного мясного продукта за 90 дней
        # Покупатели мяса
        meat_buyers = (
            purchases.filter(F.col("purchase_date") >= self.cutoff_90d)
            .join(purchase_items_df, "purchase_pk")
            .join(products, "product_pk")
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
        logger.info(f"milk_purchases: {milk_purchases.count()}")
        logger.info(f"fruits_purchases: {fruits_purchases.count()}")
        logger.info(f"not_veggies: {not_veggies.count()}")
        logger.info(f"recurrent: {recurrent.count()}")
        logger.info(f"inactive: {inactive.count()}")
        logger.info(f"new_cust: {new_cust.count()}")
        logger.info(f"organic: {organic.count()}")
        logger.info(f"avg_basket: {avg_basket.count()}")
        logger.info(f"bakery: {bakery.count()}")
        logger.info(f"loyal: {loyal.count()}")
        logger.info(f"customer_cities: {customer_cities.count()}")
        logger.info(f"meat: {meat.count()}")
        logger.info(f"time_pref: {time_pref.count()}")
        logger.info(f"pay_pref: {pay_pref.count()}")
        logger.info(f"weekend_pref: {weekend_pref.count()}")
        logger.info(f"single_item: {single_item.count()}")
        logger.info(f"varied: {varied.count()}")
        logger.info(f"store_loyalty: {store_loyalty.count()}")
        logger.info(f"family: {family.count()}")
        logger.info(f"early: {early.count()}")
        logger.info(f"no_purch: {no_purch.count()}")
        logger.info(f"high_spender: {high_spender.count()}")
        logger.info(f"fruit_lover: {fruit_lover.count()}")
        logger.info(f"vegetarian: {vegetarian.count()}")

        # Собираем всё в один DataFrame
        features = (
            customers.select("customer_pk", "customer_id")
            .join(
                milk_purchases.select("customer_pk", "bought_milk_last_30d"),
                "customer_pk",
                "left",
            )
            .join(
                fruits_purchases.select("customer_pk", "bought_fruits_last_14d"),
                "customer_pk",
                "left",
            )
            .join(
                not_veggies.select("customer_pk", "not_bought_veggies_14d"),
                "customer_pk",
                "left",
            )
            .join(
                recurrent.select("customer_pk", "recurrent_buyer"),
                "customer_pk",
                "left",
            )
            .join(
                inactive.select("customer_pk", "inactive_14_30"), "customer_pk", "left"
            )
            .join(new_cust.select("customer_pk", "new_customer"), "customer_pk", "left")
            .join(
                delivery.select("customer_pk", "delivery_user"), "customer_pk", "left"
            )
            .join(
                organic.select("customer_pk", "organic_preference"),
                "customer_pk",
                "left",
            )
            .join(
                avg_basket.select("customer_pk", "bulk_buyer", "low_cost_buyer"),
                "customer_pk",
                "left",
            )
            .join(bakery.select("customer_pk", "buys_bakery"), "customer_pk", "left")
            .join(loyal.select("customer_pk", "loyal_customer"), "customer_pk", "left")
            .join(
                customer_cities.select("customer_pk", "multicity_buyer"),
                "customer_pk",
                "left",
            )
            .join(
                meat.select("customer_pk", "bought_meat_last_week"),
                "customer_pk",
                "left",
            )
            .join(
                time_pref.select("customer_pk", "night_shopper", "morning_shopper"),
                "customer_pk",
                "left",
            )
            .join(
                pay_pref.select("customer_pk", "prefers_cash", "prefers_card"),
                "customer_pk",
                "left",
            )
            .join(
                weekend_pref.select(
                    "customer_pk", "weekend_shopper", "weekday_shopper"
                ),
                "customer_pk",
                "left",
            )
            .join(
                single_item.select("customer_pk", "single_item_buyer"),
                "customer_pk",
                "left",
            )
            .join(varied.select("customer_pk", "varied_shopper"), "customer_pk", "left")
            .join(
                store_loyalty.select("customer_pk", "store_loyal", "switching_store"),
                "customer_pk",
                "left",
            )
            .join(family.select("customer_pk", "family_shopper"), "customer_pk", "left")
            .join(early.select("customer_pk", "early_bird"), "customer_pk", "left")
            .join(no_purch.select("customer_pk", "no_purchases"), "customer_pk", "left")
            .join(
                high_spender.select("customer_pk", "recent_high_spender"),
                "customer_pk",
                "left",
            )
            .join(
                fruit_lover.select("customer_pk", "fruit_lover"), "customer_pk", "left"
            )
            .join(
                vegetarian.select("customer_pk", "vegetarian_profile"),
                "customer_pk",
                "left",
            )
        )

        # Заполняем NULL значением 0
        feature_cols = [
            c for c in features.columns if c not in ["customer_pk", "customer_id"]
        ]
        for col in feature_cols:
            features = features.withColumn(col, F.coalesce(F.col(col), F.lit(0)))

        logger.info(f"Всего признаков рассчитано: {len(feature_cols)}")
        logger.info(f"Клиентов с признаками: {features.count()}")
        return features.select("customer_id", *feature_cols)
