"""
Определения таблиц для ClickHouse.
"""

from typing import Dict
from clickhouse_driver import Client


class TableManager:
    """Менеджер для создания таблиц ClickHouse."""

    # SQL для создания таблиц в RAW слое
    CREATE_TABLES: Dict[str, str] = {
        "raw_data.raw_products": """
            CREATE TABLE IF NOT EXISTS raw_data.raw_products (
                id String,
                name String,
                group String,
                description String,
                calories Float64,
                protein Float64,
                fat Float64,
                carbohydrates Float64,
                price Float64,
                unit String,
                origin_country String,
                expiry_days UInt16,
                is_organic Bool,
                barcode String,
                manufacturer_name String,
                manufacturer_country String,
                manufacturer_website String,
                manufacturer_inn String,
                load_date DateTime,
                version UInt64 DEFAULT toUnixTimestamp(load_date)
            ) ENGINE = ReplacingMergeTree(version)
            ORDER BY id
            PARTITION BY toYYYYMM(load_date)
            TTL load_date + INTERVAL 180 DAY DELETE
            SETTINGS index_granularity = 8192
        """,
        "raw_data.raw_customers": """
            CREATE TABLE IF NOT EXISTS raw_data.raw_customers (
                customer_id String,
                first_name String,
                last_name String,
                email String,
                phone String,
                birth_date Date32,
                gender String,
                registration_date DateTime64(0, 'UTC'),
                is_loyalty_member Bool,
                loyalty_card_number String,
                purchase_location_store_id String,
                purchase_location_store_name String,
                purchase_location_store_network String,
                purchase_location_store_type_description String,
                purchase_location_country String,
                purchase_location_city String,
                purchase_location_street String,
                purchase_location_house String,
                purchase_location_postal_code String,
                delivery_address_country String,
                delivery_address_city String,
                delivery_address_street String,
                delivery_address_house String,
                delivery_address_apartment String,
                delivery_address_postal_code String,
                preferred_language String,
                preferred_payment_method String,
                receive_promotions Bool,
                load_date DateTime,
                version UInt64 DEFAULT toUnixTimestamp(load_date)
            ) ENGINE = ReplacingMergeTree(version)
            ORDER BY customer_id
            PARTITION BY toYYYYMM(load_date)
            TTL load_date + INTERVAL 180 DAY DELETE
            SETTINGS index_granularity = 8192
        """,
        "raw_data.raw_stores": """
            CREATE TABLE IF NOT EXISTS raw_data.raw_stores (
                store_id String,
                store_name String,
                store_network String,
                store_type_description String,
                type String,
                categories Array(String),
                manager_name String,
                manager_phone String,
                manager_email String,
                location_country String,
                location_city String,
                location_street String,
                location_house String,
                location_postal_code String,
                location_latitude Float64,
                location_longitude Float64,
                opening_hours_mon_fri String,
                opening_hours_sat String,
                opening_hours_sun String,
                accepts_online_orders Bool,
                delivery_available Bool,
                warehouse_connected Bool,
                last_inventory_date Date,
                load_date DateTime,
                version UInt64 DEFAULT toUnixTimestamp(load_date)
            ) ENGINE = ReplacingMergeTree(version)
            ORDER BY store_id
            PARTITION BY toYYYYMM(load_date)
            TTL load_date + INTERVAL 180 DAY DELETE
            SETTINGS index_granularity = 8192
        """,
        "raw_data.raw_purchases": """
            CREATE TABLE IF NOT EXISTS raw_data.raw_purchases (
                purchase_id String,
                customer_id String,
                customer_first_name String,
                customer_last_name String,
                customer_email String,
                customer_phone String,
                customer_is_loyalty_member Bool,
                customer_loyalty_card_number String,
                store_id String,
                store_name String,
                store_network String,
                store_type_description String,
                store_city String,
                store_street String,
                store_house String,
                store_postal_code String,
                items String,
                delivery_city String,
                delivery_street String,
                delivery_house String,
                delivery_apartment String,
                delivery_postal_code String,
                total_amount Float64,
                payment_method String,
                is_delivery Bool,
                purchase_datetime DateTime('UTC'),
                load_date DateTime,
                version UInt64 DEFAULT toUnixTimestamp(load_date)
            ) ENGINE = ReplacingMergeTree(version)
            ORDER BY purchase_id
            PARTITION BY toYYYYMM(load_date)
            TTL load_date + INTERVAL 180 DAY DELETE
            SETTINGS index_granularity = 8192
        """,
    }

    # Соответствие топиков таблицам
    TOPIC_TABLE: Dict[str, str] = {
        "products": "raw_data.raw_products",
        "customers": "raw_data.raw_customers",
        "stores": "raw_data.raw_stores",
        "purchases": "raw_data.raw_purchases",
    }

    # Создание справочников и таблиц с фактами с использованием MV
    CREATE_ALL_TABLES: Dict[str, str] = {
        "mart_data.dim_categories": """
                 CREATE TABLE IF NOT EXISTS mart_data.dim_categories (
                     category_id UInt64,
                     category_name String,
                     clear_category_name String,
                     load_date DateTime,
                     version UInt64 DEFAULT toUnixTimestamp(load_date)
                 ) ENGINE = ReplacingMergeTree(version)
                 ORDER BY category_id
                 TTL load_date + INTERVAL 365 DAY DELETE
                 SETTINGS index_granularity = 8192
             """,
        "mart_data.mv_dim_categories": """
                 CREATE MATERIALIZED VIEW IF NOT EXISTS
                 mart_data.mv_dim_categories TO mart_data.dim_categories AS
                     SELECT
                         cityHash64(
                             lowerUTF8(
                                  trim(
                                      replaceRegexpOne(group, '^\\p{So}+\\s*', '')
                                  )
                             )
                         ) AS category_id,
                         lowerUTF8(group) AS category_name,
                         lowerUTF8(
                             trim(
                                 replaceRegexpOne(group, '^\\p{So}+\\s*', '')
                             )
                         ) AS clear_category_name,
                         now() AS load_date,
                         toUnixTimestamp(now()) as version
                     FROM raw_data.raw_products
                     WHERE group != ''
             """,
        "mart_data.dim_units": """
                 CREATE TABLE IF NOT EXISTS mart_data.dim_units (
                     unit_id UInt64,
                     unit_name String,
                     load_date DateTime,
                     version UInt64 DEFAULT toUnixTimestamp(load_date)
                 ) ENGINE = ReplacingMergeTree(version)
                 ORDER BY unit_id
                 TTL load_date + INTERVAL 365 DAY DELETE
                 SETTINGS index_granularity = 8192
            """,
        "mart_data.mv_dim_units": """
                CREATE MATERIALIZED VIEW IF NOT EXISTS
                mart_data.mv_dim_units TO mart_data.dim_units AS
                    SELECT
                        cityHash64(lowerUTF8(unit)) as unit_id,
                        lowerUTF8(unit) as unit_name,
                        now() as load_date,
                        toUnixTimestamp(now()) as version
                    FROM raw_data.raw_products
                    WHERE unit != ''
            """,
        "mart_data.dim_countries": """
                 CREATE TABLE IF NOT EXISTS mart_data.dim_countries (
                     country_id UInt64,
                     country_name String,
                     load_date DateTime,
                    record_source String,
                    version UInt64 DEFAULT toUnixTimestamp(load_date)
                 ) ENGINE = ReplacingMergeTree(version)
                ORDER BY country_id
                TTL load_date + INTERVAL 365 DAY DELETE
                SETTINGS index_granularity = 8192
            """,
        "mart_data.mv_dim_countries": """
                CREATE MATERIALIZED VIEW IF NOT EXISTS
                mart_data.mv_dim_countries TO mart_data.dim_countries AS
                             SELECT
                                 cityHash64(
                                     lowerUTF8(origin_country)
                                 ) as country_id,
                                 lowerUTF8(origin_country) as country_name,
                                 'raw_products' as record_source,
                                 now() as load_date,
                                 toUnixTimestamp(now()) as version
                             FROM raw_data.raw_products
                             WHERE origin_country != ''
                             UNION ALL
                             SELECT
                                 cityHash64(
                                     lowerUTF8(location_country)
                                 ) as country_id,
                                 lowerUTF8(location_country) as country_name,
                                 'raw_stores' as record_source,
                                 now() as load_date,
                                 toUnixTimestamp(now()) as version
                             FROM raw_data.raw_stores
                             WHERE location_country != ''
                             UNION ALL
                             SELECT
                                  cityHash64(
                                      lowerUTF8(delivery_address_country)
                                  ) as country_id,
                                  lowerUTF8(
                                      delivery_address_country
                                  ) as country_name,
                                  'raw_customers' as record_source,
                                  now() as load_date,
                                  toUnixTimestamp(now()) as version
                             FROM raw_data.raw_customers
                             WHERE delivery_address_country != ''
                        """,
        "mart_data.dim_language": """
                 CREATE TABLE IF NOT EXISTS mart_data.dim_language (
                     language_id UInt64,
                     language_name String,
                     load_date DateTime,
                     version UInt64 DEFAULT toUnixTimestamp(load_date)
                 ) ENGINE = ReplacingMergeTree(version)
                 ORDER BY language_id
                 TTL load_date + INTERVAL 365 DAY DELETE
                 SETTINGS index_granularity = 8192
            """,
        "mart_data.mv_dim_language": """
                 CREATE MATERIALIZED VIEW IF NOT EXISTS
                 mart_data.mv_dim_language TO mart_data.dim_language AS
                     SELECT
                         cityHash64(
                             lowerUTF8(preferred_language)
                         ) as language_id,
                        lowerUTF8(preferred_language) as language_name,
                        now() as load_date,
                        toUnixTimestamp(now()) as version
                     FROM raw_data.raw_customers
                     WHERE preferred_language != ''
             """,
        "mart_data.dim_cities": """
                CREATE TABLE IF NOT EXISTS mart_data.dim_cities (
                    city_id UInt64,
                    city_name String,
                    country_id UInt64,
                    load_date DateTime,
                    version UInt64 DEFAULT toUnixTimestamp(load_date)
                ) ENGINE = ReplacingMergeTree(version)
                ORDER BY city_id
                TTL load_date + INTERVAL 365 DAY DELETE
                SETTINGS index_granularity = 8192
             """,
        "mart_data.mv_dim_cities": """
                  CREATE MATERIALIZED VIEW IF NOT EXISTS
                  mart_data.mv_dim_cities TO mart_data.dim_cities AS
                      SELECT
                          cityHash64(lowerUTF8(location_city)) as city_id,
                          lowerUTF8(location_city) as city_name,
                          if(location_country != '',
                               cityHash64(lowerUTF8(location_country)),
                            0) as country_id,
                          now() as load_date,
                          toUnixTimestamp(now()) as version
                      FROM raw_data.raw_stores
                      WHERE location_city != ''
             """,
        "mart_data.dim_address": """
                CREATE TABLE IF NOT EXISTS mart_data.dim_address (
                    address_id UInt64,
                    country_id UInt64,
                    city_id UInt64,
                    street String,
                    house String,
                    postal_code String,
                    latitude Float64,
                    longitude Float64,
                    record_source String,
                    load_date DateTime,
                    version UInt64 DEFAULT toUnixTimestamp(load_date)
                ) ENGINE = ReplacingMergeTree(version)
                ORDER BY address_id
                TTL load_date + INTERVAL 365 DAY DELETE
                SETTINGS index_granularity = 8192
            """,
        "mart_data.mv_dim_address": """
                CREATE MATERIALIZED VIEW IF NOT EXISTS
                mart_data.mv_dim_address TO mart_data.dim_address AS
                        SELECT
                            cityHash64(
                                lowerUTF8(location_city),
                                lowerUTF8(location_street),
                                lowerUTF8(location_house),
                                lowerUTF8(location_postal_code)
                            ) as address_id,
                            if(location_country != '',
                                 cityHash64(lowerUTF8(location_country)),
                            0) as country_id,
                            cityHash64(lowerUTF8(location_city)) as city_id,
                            lowerUTF8(location_street) as street,
                            lowerUTF8(location_house) as house,
                            lowerUTF8(location_postal_code) as postal_code,
                            location_latitude as latitude,
                            location_longitude as longitude,
                            'raw_stores' as record_source,
                            now() as load_date,
                            toUnixTimestamp(now()) as version
                        FROM raw_data.raw_stores
                        WHERE
                            location_city != '' AND
                            location_street != '' AND
                            location_house != '' AND
                            location_postal_code != ''
                        UNION ALL
                        SELECT
                            cityHash64(
                                lowerUTF8(delivery_address_city),
                                lowerUTF8(delivery_address_street),
                                lowerUTF8(delivery_address_house),
                                lowerUTF8(delivery_address_postal_code)
                            ) as address_id,
                            if(delivery_address_country != '',
                                cityHash64(lowerUTF8(delivery_address_country)),
                            0) as country_id,
                            cityHash64(
                                lowerUTF8(delivery_address_city)
                            ) as city_id,
                            lowerUTF8(delivery_address_street) as street,
                            lowerUTF8(delivery_address_house) as house,
                            lowerUTF8(
                                delivery_address_postal_code
                            ) as postal_code,
                            0.0 as latitude,
                            0.0 as longitude,
                            'raw_customers' as record_source,
                            now() as load_date,
                            toUnixTimestamp(now()) as version
                        FROM raw_data.raw_customers
                        WHERE
                            delivery_address_city != '' AND
                            delivery_address_street != '' AND
                            delivery_address_house != '' AND
                            delivery_address_postal_code != ''
            """,
        "mart_data.dim_delivery_addresses": """
               CREATE TABLE IF NOT EXISTS mart_data.dim_delivery_addresses (
                   delivery_address_id UInt64,
                   address_id UInt64,
                   city_id UInt64,
                   country_id UInt64,
                   apartment String,
                   load_date DateTime,
                   version UInt64 DEFAULT toUnixTimestamp(load_date)
               ) ENGINE = ReplacingMergeTree(version)
               ORDER BY delivery_address_id
               TTL load_date + INTERVAL 365 DAY DELETE
               SETTINGS index_granularity = 8192
            """,
        "mart_data.mv_dim_delivery_addresses": """
                CREATE MATERIALIZED VIEW IF NOT EXISTS
                mart_data.mv_dim_delivery_addresses TO
                mart_data.dim_delivery_addresses AS
                    SELECT
                        cityHash64(
                            lowerUTF8(delivery_address_city),
                            lowerUTF8(delivery_address_street),
                            lowerUTF8(delivery_address_house),
                            lowerUTF8(delivery_address_apartment),
                            lowerUTF8(delivery_address_postal_code)
                        ) as delivery_address_id,
                        cityHash64(
                            lowerUTF8(delivery_address_city),
                            lowerUTF8(delivery_address_street),
                            lowerUTF8(delivery_address_house),
                            lowerUTF8(delivery_address_postal_code)
                        ) as address_id,
                        cityHash64(
                            lowerUTF8(delivery_address_city)
                        ) as city_id,
                        if(delivery_address_country != '',
                            cityHash64(lowerUTF8(delivery_address_country)),
                        0) as country_id,
                        lowerUTF8(delivery_address_apartment) as apartment,
                        now() as load_date,
                        toUnixTimestamp(now()) as version
                    FROM raw_data.raw_customers
                    WHERE
                        delivery_address_city != '' AND
                        delivery_address_street != '' AND
                        delivery_address_house != '' AND
                        delivery_address_apartment != '' AND
                        delivery_address_postal_code != ''
            """,
        "mart_data.dim_managers": """
                CREATE TABLE IF NOT EXISTS mart_data.dim_managers (
                    manager_id UInt64,
                    manager_name String,
                    manager_phone String,
                    manager_email String,
                    load_date DateTime,
                    version UInt64 DEFAULT toUnixTimestamp(load_date)
                ) ENGINE = ReplacingMergeTree(version)
                ORDER BY manager_id
                TTL load_date + INTERVAL 365 DAY DELETE
                SETTINGS index_granularity = 8192
            """,
        "mart_data.mv_dim_managers": """
                CREATE MATERIALIZED VIEW IF NOT EXISTS
                mart_data.mv_dim_managers TO
                mart_data.dim_managers AS
                    SELECT
                        cityHash64(
                            lowerUTF8(manager_name),
                            manager_phone,
                            manager_email
                        ) as manager_id,
                        lowerUTF8(manager_name) as manager_name,
                        manager_phone as manager_phone,
                        manager_email as manager_email,
                        now() as load_date,
                        toUnixTimestamp(now()) as version
                    FROM raw_data.raw_stores
                    WHERE
                        manager_name != ''
                        AND (manager_phone != ''
                        OR manager_email != '')
           """,
        "mart_data.dim_store_networks": """
                CREATE TABLE IF NOT EXISTS mart_data.dim_store_networks (
                    store_network_id UInt64,
                    store_network_name String,
                    load_date DateTime,
                    version UInt64 DEFAULT toUnixTimestamp(load_date)
                ) ENGINE = ReplacingMergeTree(version)
               ORDER BY store_network_id
               TTL load_date + INTERVAL 365 DAY DELETE
               SETTINGS index_granularity = 8192
            """,
        "mart_data.mv_dim_store_networks": """
                CREATE MATERIALIZED VIEW IF NOT EXISTS
                mart_data.mv_dim_store_networks TO
                mart_data.dim_store_networks AS
                    SELECT
                        cityHash64(
                            lowerUTF8(store_network)
                        ) as store_network_id,
                        lowerUTF8(store_network) as store_network_name,
                        now() as load_date,
                        toUnixTimestamp(now()) as version
                    FROM raw_data.raw_stores
                    WHERE store_network != ''
            """,
        "mart_data.dim_opening_hours": """
                CREATE TABLE IF NOT EXISTS mart_data.dim_opening_hours (
                    opening_hours_id UInt64,
                    mon_fri String,
                    sat String,
                    sun String,
                    load_date DateTime,
                    version UInt64 DEFAULT toUnixTimestamp(load_date)
                ) ENGINE = ReplacingMergeTree(version)
                ORDER BY opening_hours_id
                TTL load_date + INTERVAL 365 DAY DELETE
                SETTINGS index_granularity = 8192
            """,
        "mart_data.mv_dim_opening_hours": """
                CREATE MATERIALIZED VIEW IF NOT EXISTS
                mart_data.mv_dim_opening_hours TO
                mart_data.dim_opening_hours AS
                    SELECT
                        cityHash64(
                            coalesce(lowerUTF8(opening_hours_mon_fri), ''),
                            coalesce(lowerUTF8(opening_hours_sat), ''),
                            coalesce(lowerUTF8(opening_hours_sun), '')
                        ) as opening_hours_id,
                        coalesce(
                            lowerUTF8(opening_hours_mon_fri), ''
                        ) as mon_fri,
                        coalesce(lowerUTF8(opening_hours_sat), '') as sat,
                        coalesce(lowerUTF8(opening_hours_sun), '') as sun,
                        now() as load_date,
                        toUnixTimestamp(now()) as version
                        FROM raw_data.raw_stores
                        WHERE
                            opening_hours_mon_fri != ''
                            OR opening_hours_sat != ''
                            OR opening_hours_sun != ''
            """,
        "mart_data.dim_gender": """
                CREATE TABLE IF NOT EXISTS mart_data.dim_gender (
                    gender_id UInt8,
                    gender_name String,
                    load_date DateTime,
                    version UInt64 DEFAULT toUnixTimestamp(load_date)
                ) ENGINE = ReplacingMergeTree(version)
                ORDER BY gender_id
                TTL load_date + INTERVAL 365 DAY DELETE
                SETTINGS index_granularity = 8192
            """,
        "mart_data.mv_dim_gender": """
                CREATE MATERIALIZED VIEW IF NOT EXISTS
                mart_data.mv_dim_gender TO
                mart_data.dim_gender AS
                    SELECT
                        CASE lowerUTF8(gender)
                            WHEN 'male' THEN 1
                            WHEN 'female' THEN 2
                            ELSE 0
                        END as gender_id,
                        lowerUTF8(gender) as gender_name,
                        now() as load_date,
                        toUnixTimestamp(now()) as version
                        FROM raw_data.raw_customers
                        WHERE gender != ''
            """,
        "mart_data.dim_manufacturer": """
                CREATE TABLE IF NOT EXISTS mart_data.dim_manufacturer (
                    manufacturer_id UInt64,
                    country_id UInt64,
                    manufacturer_name String,
                    manufacturer_website String,
                    manufacturer_inn String,
                    load_date DateTime,
                    version UInt64 DEFAULT toUnixTimestamp(load_date)
                ) ENGINE = ReplacingMergeTree(version)
                ORDER BY manufacturer_id
                TTL load_date + INTERVAL 365 DAY DELETE
                SETTINGS index_granularity = 8192
            """,
        "mart_data.mv_dim_manufacturer": """
                CREATE MATERIALIZED VIEW IF NOT EXISTS
                mart_data.mv_dim_manufacturer TO
                mart_data.dim_manufacturer AS
                    SELECT
                        cityHash64(
                            lowerUTF8(manufacturer_name),
                            lowerUTF8(manufacturer_inn)
                        ) as manufacturer_id,
                        if(
                            manufacturer_country='',
                            0,
                            cityHash64(lowerUTF8(manufacturer_country))
                        ) as country_id,
                        lowerUTF8(manufacturer_name) as manufacturer_name,
                        coalesce(
                            lowerUTF8(manufacturer_website),
                            ''
                        ) as manufacturer_website,
                        lowerUTF8(manufacturer_inn) as manufacturer_inn,
                        now() as load_date,
                        toUnixTimestamp(now()) as version
                    FROM raw_data.raw_products
                    WHERE
                        manufacturer_name != '' AND
                        manufacturer_inn != ''
            """,
        "mart_data.dim_payment_method": """
                CREATE TABLE IF NOT EXISTS mart_data.dim_payment_method (
                    payment_method_id UInt8,
                    payment_method_name String,
                    load_date DateTime,
                    version UInt64 DEFAULT toUnixTimestamp(load_date)
                ) ENGINE = ReplacingMergeTree(version)
                ORDER BY payment_method_id
                TTL load_date + INTERVAL 365 DAY DELETE
                SETTINGS index_granularity = 8192
            """,
        "mart_data.mv_dim_payment_method": """
                CREATE MATERIALIZED VIEW IF NOT EXISTS
                mart_data.mv_dim_payment_method TO
                mart_data.dim_payment_method AS
                    SELECT
                        multiIf(
                            lowerUTF8(payment_method) = 'card', 1,
                            lowerUTF8(payment_method) = 'cash', 2,
                            lowerUTF8(payment_method) = 'online', 3,
                            0
                        ) as payment_method_id,
                        lowerUTF8(payment_method) as payment_method_name,
                        now() as load_date,
                        toUnixTimestamp(now()) as version
                    FROM raw_data.raw_purchases
                    WHERE payment_method != ''
            """,
        "mart_data.dim_type_store": """
                CREATE TABLE IF NOT EXISTS mart_data.dim_type_store (
                    type_id UInt64,
                    type_name String,
                    load_date DateTime,
                    version UInt64 DEFAULT toUnixTimestamp(load_date)
                ) ENGINE = ReplacingMergeTree(version)
                ORDER BY type_id
                TTL load_date + INTERVAL 365 DAY DELETE
                SETTINGS index_granularity = 8192
            """,
        "mart_data.mv_dim_type_store": """
                CREATE MATERIALIZED VIEW IF NOT EXISTS
                mart_data.mv_dim_type_store TO
                mart_data.dim_type_store AS
                    SELECT
                        cityHash64(
                            lowerUTF8(type)
                        ) as type_id,
                        lowerUTF8(type) as type_name,
                        now() as load_date,
                        toUnixTimestamp(now()) as version
                    FROM  raw_data.raw_stores
                    WHERE type  != ''
            """,
        "mart_data.products_details": """
                CREATE TABLE IF NOT EXISTS mart_data.products_details (
                    product_pk UInt64,
                    product_id String,
                    name String,
                    category_id UInt64,
                    description String,
                    calories Float64,
                    protein Float64,
                    fat Float64,
                    carbohydrates Float64,
                    price Float64,
                    unit_id UInt64,
                    country_id UInt64,
                    expiry_days UInt16,
                    is_organic Bool,
                    barcode String,
                    manufacturer_id UInt64,
                    load_date DateTime,
                    record_source String,
                    version UInt64
                ) ENGINE = ReplacingMergeTree(version)
                ORDER BY product_pk
                PARTITION BY toYYYYMM(load_date)
                TTL load_date + INTERVAL 365 DAY delete
                SETTINGS index_granularity = 8192
            """,
        "mart_data.mv_products_details": """
                CREATE MATERIALIZED VIEW IF NOT EXISTS
                mart_data.mv_products_details TO
                mart_data.products_details AS
                    SELECT
                        cityHash64(
                            lowerUTF8(id)
                        ) as product_pk,
                        lowerUTF8(id) as product_id,
                        lowerUTF8(name) as name,
                        cityHash64(
                             lowerUTF8(
                                  trim(
                                      replaceRegexpOne(group, '^\\p{So}+\\s*', '')
                                  )
                             )
                         ) AS category_id,
                        if(
                            description != '',
                            lowerUTF8(description),
                            ''
                        ) as description,
                        calories as calories,
                        protein as protein,
                        fat as fat,
                        carbohydrates as carbohydrates,
                        price as price,
                        cityHash64(lowerUTF8(unit)) as unit_id,
                        if(
                            origin_country !='',
                            cityHash64(lowerUTF8(origin_country)),
                            0
                        ) as country_id,
                        expiry_days as expiry_days,
                        is_organic,
                        barcode as barcode,
                        cityHash64(
                            lowerUTF8(manufacturer_name),
                            lowerUTF8(manufacturer_inn)
                        ) as manufacturer_id,
                        now() as load_date,
                        'raw_products' as record_source,
                        toUnixTimestamp(now()) as version
                    FROM raw_data.raw_products
                    WHERE id != '' AND
                        name != '' AND
                        group != '' AND
                        unit != '' AND
                        price > 0.0 AND
                        manufacturer_name !='' AND
                        manufacturer_inn != ''
            """,
        "mart_data.customers_details": """
                CREATE TABLE IF NOT EXISTS mart_data.customers_details (
                    customer_pk UInt64,
                    customer_id String,
                    first_name String,
                    last_name String,
                    email String,
                    phone String,
                    birth_date Date32,
                    gender_id Enum8('male' = 1, 'female' = 2, 'other' = 0),
                    registration_date DateTime64(0),
                    is_loyalty_member Bool,
                    loyalty_card_number String,
                    language_id UInt64,
                    payment_method_id Enum8(
                        'card' = 1,
                        'cash' = 2,
                        'online' = 3,
                        'other' = 0
                    ),
                    receive_promotions Bool,
                    load_date DateTime,
                    record_source String,
                    version UInt64 DEFAULT toUnixTimestamp(load_date)
                ) ENGINE = ReplacingMergeTree(version)
                ORDER BY customer_pk
                PARTITION BY toYYYYMM(load_date)
                TTL load_date + INTERVAL 365 DAY DELETE
                SETTINGS index_granularity = 8192
            """,
        "mart_data.mv_customers_details": """
            CREATE MATERIALIZED VIEW IF NOT EXISTS
            mart_data.mv_customers_details TO
            mart_data.customers_details AS
                SELECT
                    cityHash64(lowerUTF8(customer_id)) as customer_pk,
                    lowerUTF8(customer_id) as customer_id,
                    lowerUTF8(first_name) as first_name,
                    lowerUTF8(last_name) as last_name,
                    email,
                    phone,
                    birth_date,
                    multiIf(
                        lowerUTF8(gender) = 'male', 1,
                        lowerUTF8(gender) = 'female', 2,
                        0
                    ) as gender_id,
                    toTimeZone(registration_date, 'UTC') as registration_date,
                    is_loyalty_member,
                    lowerUTF8(loyalty_card_number) as loyalty_card_number,
                    cityHash64(lowerUTF8(preferred_language)) as language_id,
                    multiIf(
                        lowerUTF8(preferred_payment_method) = 'card', 1,
                        lowerUTF8(preferred_payment_method) = 'cash', 2,
                        lowerUTF8(preferred_payment_method) = 'online', 3,
                        0
                    ) as payment_method_id,
                    receive_promotions,
                    now() as load_date,
                    'raw_customers' as record_source,
                    toUnixTimestamp(now()) as version
                FROM raw_data.raw_customers
                WHERE
                    customer_id != ''
                    AND first_name != ''
                    AND last_name != ''
                    AND birth_date <= today()
                    AND birth_date > '1900-01-01'
                    AND registration_date  <= now()
            """,
        "mart_data.stores_details": """
                CREATE TABLE IF NOT EXISTS mart_data.stores_details (
                    store_pk UInt64,
                    store_id String,
                    store_name String,
                    type_id UInt64,
                    store_network_id UInt64,
                    category_ids Array(UInt64),
                    manager_id UInt64,
                    location_id UInt64,
                    opening_hours_id UInt64,
                    accepts_online_orders Bool,
                    delivery_available Bool,
                    warehouse_connected Bool,
                    last_inventory_date Date,
                    load_date DateTime,
                    version UInt64 DEFAULT toUnixTimestamp(load_date)
                ) ENGINE = ReplacingMergeTree(version)
                ORDER BY store_pk
                PARTITION BY toYYYYMM(load_date)
                TTL load_date + INTERVAL 365 DAY DELETE
                SETTINGS index_granularity = 8192
            """,
        "mart_data.mv_stores_details": """
                CREATE MATERIALIZED VIEW IF NOT EXISTS
                mart_data.mv_stores_details TO
                mart_data.stores_details AS
                    SELECT
                        cityHash64(
                            lowerUTF8(store_id)
                        ) as store_pk,
                        lowerUTF8(store_id) as store_id,
                        lowerUTF8(store_name) as store_name,
                        if(
                            type !='',
                            cityHash64(lowerUTF8(type)),
                            0
                        ) as type_id,
                        if(
                            store_network !='',
                            cityHash64(lowerUTF8(store_network)),
                            0
                        ) as store_network_id,
                        arrayMap(x -> 
                        cityHash64(
                             lowerUTF8(
                                  trim(
                                      replaceRegexpOne(
                                          x, '^\\p{So}+\\s*', ''
                                      )
                                  )
                             )
                         ),
                        categories) as category_ids,
                        cityHash64(lowerUTF8(manager_name),
                        manager_phone,
                        manager_email) as manager_id,
                        cityHash64(
                            lowerUTF8(location_city),
                            lowerUTF8(location_street),
                            lowerUTF8(location_house),
                            lowerUTF8(location_postal_code)
                        ) as location_id,
                        cityHash64(
                            coalesce(lowerUTF8(opening_hours_mon_fri), ''),
                            coalesce(lowerUTF8(opening_hours_sat), ''),
                            coalesce(lowerUTF8(opening_hours_sun), '')
                        ) as opening_hours_id,
                        accepts_online_orders as accepts_online_orders,
                        delivery_available as delivery_available,
                        warehouse_connected as warehouse_connected,
                        last_inventory_date as last_inventory_date,
                        now() as load_date,
                        toUnixTimestamp(now()) as version
                    FROM raw_data.raw_stores
                    WHERE store_id != ''
                        AND store_name != ''
                        AND last_inventory_date <= today()
                        AND last_inventory_date > '1900-01-01'
                        AND location_city != ''
                        AND location_street != ''
                        AND location_house != ''
                        AND location_postal_code != ''
                        AND manager_name != ''
                        AND manager_phone != ''
                        AND manager_email != ''
            """,
        "mart_data.link_customers_stores": """
                CREATE TABLE IF NOT EXISTS mart_data.link_customers_stores (
                    link_pk UInt64,
                    customer_pk UInt64,
                    store_pk UInt64,
                    delivery_address_id UInt64,
                    load_date DateTime,
                    record_source String,
                    version UInt64 DEFAULT toUnixTimestamp(load_date)
                ) ENGINE = ReplacingMergeTree(version)
                ORDER BY link_pk
                PARTITION BY toYYYYMM(load_date)
                TTL load_date + INTERVAL 365 DAY DELETE
                SETTINGS index_granularity = 8192
            """,
        "mart_data.mv_link_customers_stores": """
                CREATE MATERIALIZED VIEW IF NOT EXISTS
                mart_data.mv_link_customers_stores TO
                mart_data.link_customers_stores AS
                    SELECT
                        cityHash64(
                            lowerUTF8(customer_id),
                            lowerUTF8(purchase_location_store_id)
                        ) as link_pk,
                        cityHash64(lowerUTF8(customer_id)) as customer_pk,
                        cityHash64(
                            lowerUTF8(purchase_location_store_id)
                        ) as store_pk,
                        cityHash64(
                            lowerUTF8(delivery_address_city),
                            lowerUTF8(delivery_address_street),
                            lowerUTF8(delivery_address_house),
                            lowerUTF8(delivery_address_postal_code)
                        ) as delivery_address_id,
                        now() as load_date,
                        'raw_customers' as record_source,
                        toUnixTimestamp(now()) as version
                    FROM raw_data.raw_customers
                    WHERE
                        customer_id != ''
                        AND purchase_location_store_id != ''
                        AND delivery_address_city != ''
                        AND delivery_address_street != ''
                        AND delivery_address_house != ''
                        AND delivery_address_postal_code != ''
            """,
        "mart_data.purchases_details": """
                CREATE TABLE IF NOT EXISTS mart_data.purchases_details (
                    purchase_pk UInt64,
                    purchase_id String,
                    customer_pk UInt64,
                    store_pk UInt64,
                    delivery_address_id UInt64,
                    total_amount Float64,
                    payment_method_id UInt8,
                    is_delivery Bool,
                    purchase_datetime DateTime,
                    load_date DateTime,
                    version UInt64 DEFAULT toUnixTimestamp(load_date)
                ) ENGINE = ReplacingMergeTree(version)
                ORDER BY purchase_pk
                PARTITION BY toYYYYMM(load_date)
                TTL load_date + INTERVAL 365 DAY DELETE
                SETTINGS index_granularity = 8192
            """,
        "mart_data.mv_purchases_details": """
                CREATE MATERIALIZED VIEW IF NOT EXISTS
                mart_data.mv_purchases_details TO
                mart_data.purchases_details AS
                    SELECT
                        cityHash64(lowerUTF8(purchase_id)) as purchase_pk,
                        lowerUTF8(purchase_id) as purchase_id,
                        cityHash64(lowerUTF8(customer_id)) as customer_pk,
                        cityHash64(lowerUTF8(store_id)) as store_pk,
                        multiIf(
                            delivery_city != '' AND delivery_street != ''
                            AND delivery_house != ''
                            AND delivery_postal_code != ''
                            AND delivery_apartment != '',
                                cityHash64(
                                    lowerUTF8(delivery_city),
                                    lowerUTF8(delivery_street),
                                    lowerUTF8(delivery_house),
                                    lowerUTF8(delivery_apartment),
                                    lowerUTF8(delivery_postal_code)
                                ),
                            0
                        ) as delivery_address_id,
                        total_amount,
                        multiIf(
                            lowerUTF8(payment_method) = 'card', 1,
                            lowerUTF8(payment_method) = 'cash', 2,
                            lowerUTF8(payment_method) = 'online', 3,
                            0
                        ) as payment_method_id,
                        is_delivery,
                        toTimeZone(
                            purchase_datetime, 'UTC') as purchase_datetime,
                        now() as load_date,
                        toUnixTimestamp(now()) as version
                    FROM raw_data.raw_purchases
                    WHERE
                        purchase_id != ''
                        AND customer_id != ''
                        AND store_id != ''
                        AND total_amount > 0
            """,
        "mart_data.purchase_item_details": """
                CREATE TABLE IF NOT EXISTS mart_data.purchase_item_details (
                    item_pk UInt64,
                    purchase_pk UInt64,
                    product_pk UInt64,
                    category_id UInt64,
                    quantity Float64,
                    unit_id UInt64,
                    total_price Float64,
                    manufacturer_id UInt64,
                    load_date DateTime,
                    version UInt64 DEFAULT toUnixTimestamp(load_date)
                ) ENGINE = ReplacingMergeTree(version)
                ORDER BY item_pk
                PARTITION BY toYYYYMM(load_date)
                TTL load_date + INTERVAL 365 DAY DELETE
                SETTINGS index_granularity = 8192
            """,
        "mart_data.mv_purchase_item_details": """
                CREATE MATERIALIZED VIEW IF NOT EXISTS
                    mart_data.mv_purchase_item_details TO
                    mart_data.purchase_item_details AS
                    SELECT
                        cityHash64(
                            lowerUTF8(purchase_id),
                            lowerUTF8(JSONExtractString(item, 'product_id')),
                            toString(JSONExtractFloat(item, 'quantity'))
                        ) as item_pk,
                        cityHash64(lowerUTF8(purchase_id)) as purchase_pk,
                        cityHash64(
                            lowerUTF8(
                                JSONExtractString(item, 'product_id')
                            )
                        ) as product_pk,
                        cityHash64(
                            lowerUTF8(
                                 trim(
                                     replaceRegexpOne(
                                         JSONExtractString(item, 'category'),
                                         '^\\p{So}+\\s*', ''
                                     )
                                 )
                            )
                        ) as category_id,
                        JSONExtractFloat(item, 'quantity') as quantity,
                        cityHash64(
                            lowerUTF8(JSONExtractString(item, 'unit'))
                        ) as unit_id,
                        JSONExtractFloat(item, 'total_price') as total_price,
                        multiIf(
                            JSONExtractString(
                                JSONExtractString(
                                    item, 'manufacturer'), 'name') = '' and
                            JSONExtractString(
                                JSONExtractString(
                                    item, 'manufacturer'), 'inn') = '', 0,
                            cityHash64(
                                lowerUTF8(
                                    JSONExtractString(
                                        JSONExtractString(
                                            item, 'manufacturer'
                                        ), 'name')
                                ),
                                lowerUTF8(
                                    JSONExtractString(
                                        JSONExtractString(
                                            item, 'manufacturer'
                                        ), 'inn')
                                )
                            )
                        ) as manufacturer_id,
                        now() as load_date,
                        toUnixTimestamp(now()) as version
                    FROM raw_data.raw_purchases
                    ARRAY JOIN JSONExtractArrayRaw(items) as item
                    WHERE
                        purchase_id != ''
                        AND JSONExtractString(item, 'product_id') != ''
                        AND JSONExtractString(item, 'category') != ''
                        AND JSONExtractFloat(item, 'total_price') > 0
                        AND JSONExtractString(item, 'unit') != ''
             """,
    }

    def __init__(self, client: Client) -> None:
        """
        Инициализация менеджера таблиц.

        Args:
            client: Подключение к ClickHouse
        """
        self.client = client

    def create_table(self, table_name: str) -> None:
        """
        Создает одну таблицу.

        Args:
            table_name: Имя таблицы для создания
        """
        if table_name in self.CREATE_TABLES:
            self.client.execute(self.CREATE_TABLES[table_name])
            print(f"✅ Таблица {table_name} готова")

    def create_all_tables(self) -> None:
        """Создает все таблицы и MV в mart_data."""
        for table_name in self.CREATE_ALL_TABLES:
            self.client.execute(self.CREATE_ALL_TABLES[table_name])
            print(f"✅ Таблица/MV {table_name} готова")
