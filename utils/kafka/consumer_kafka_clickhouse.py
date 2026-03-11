"""
Consumer для загрузки данных из Kafka в ClickHouse.
"""

import json
import traceback
from datetime import datetime, timezone, date
from typing import Any, Dict, Optional, Union
from confluent_kafka import Consumer, KafkaError
from clickhouse_driver import Client
from utils.clickhouse.clickhouse_tables import TableManager
import signal
from utils.logging_config import get_airflow_logger

logger = get_airflow_logger()


class GracefulExiter:
    """
    Класс для graceful обработки сигналов завершения.

    Позволяет корректно завершить работу consumer
    при получении сигналов SIGINT или SIGTERM.
    """

    def __init__(self) -> None:
        """Инициализирует обработчик сигналов."""
        self.state = False
        signal.signal(signal.SIGINT, self.change_state)
        signal.signal(signal.SIGTERM, self.change_state)
        logger.debug("✅ GracefulExiter инициализирован")

    def change_state(self, signum: int, frame: Any) -> None:
        """
        Изменяет состояние при получении сигнала.

        Args:
            signum: Номер полученного сигнала
            frame: Текущий стек вызовов
        """
        logger.info(f"⏳ Получен сигнал {signum}, завершаем работу...")
        self.state = True

    def exit(self) -> bool:
        """
        Возвращает текущее состояние флага завершения.

        Returns:
            bool: True если получен сигнал завершения, иначе False
        """
        return self.state


def safe_float_convert(value: Any, default: float = 0.0) -> float:
    """
    Безопасное преобразование значения в float.

    Args:
        value: Значение для преобразования
        default: Значение по умолчанию (0.0)

    Returns:
        float: Преобразованное значение или default при ошибке
    """
    try:
        if value is None:
            return default
        return float(value)
    except (ValueError, TypeError):
        return default


def safe_bool_convert(value: Any, default: bool = False) -> bool:
    """
    Безопасное преобразование значения в bool.

    Args:
        value: Значение для преобразования
        default: Значение по умолчанию (False)

    Returns:
        bool: Преобразованное значение или default при ошибке
    """
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        if value.lower() in ("true", "1", "yes", "да"):
            return True
    if isinstance(value, (int, float)):
        return bool(value)
    return default


def safe_json_loads(value: Any,
                    default: Optional[Dict] = None
                    ) -> Union[Dict, Any]:
    """
    Безопасная загрузка JSON.

    Args:
        value: JSON-строка для парсинга
        default: Значение по умолчанию (пустой словарь)

    Returns:
        Union[Dict, Any]: Распарсенный JSON или default при ошибке
    """
    if default is None:
        default = {}
    try:
        if isinstance(value, str):
            return json.loads(value)
        return value or default
    except json.JSONDecodeError:
        return default


def safe_int_convert(value: Any, default: int = 0) -> int:
    """
    Безопасное преобразование значения в int.

    Args:
        value: Значение для преобразования
        default: Значение по умолчанию (0)

    Returns:
        int: Преобразованное значение или default при ошибке
    """
    try:
        if value is None:
            return default
        # Если это строка с плавающей точкой, конвертируем
        if isinstance(value, str) and "." in value:
            return int(float(value))
        return int(value)
    except (ValueError, TypeError):
        return default


def parse_datetime(value: Any, default: Optional[datetime] = None) -> datetime:
    """
    Преобразует значение в объект datetime UTC.

    Args:
        value: Значение для преобразования (datetime, строка, None)
        default: Значение по умолчанию (текущее UTC время)

    Returns:
        datetime: Объект datetime в UTC
    """
    if default is None:
        default = datetime.utcnow()

    if value is None:
        return default

    if isinstance(value, datetime):
        if value.tzinfo is not None:
            return value.astimezone(timezone.utc).replace(tzinfo=None)
        return value

    if isinstance(value, str):
        # Нормализуем формат: заменяем маленькие t/z на большие
        value = value.replace("t", "T").replace("z", "Z")

        try:
            if value.endswith("Z"):
                value = value[:-1] + "+00:00"

            dt = datetime.fromisoformat(value)

            if dt.tzinfo is not None:
                dt = dt.astimezone(timezone.utc).replace(tzinfo=None)

            return dt
        except ValueError:
            try:
                return datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
            except ValueError:
                try:
                    return datetime.strptime(value, "%Y-%m-%d")
                except ValueError:
                    logger.error(f"❌ Не могу распарсить '{value}'")
                    return default

    return default


def parse_date(value: Any, default: str = "1970-01-01") -> date:
    """
    Преобразует значение в объект date.

    Args:
        value: Значение для преобразования (date, datetime, строка, None)
        default: Значение по умолчанию ('1970-01-01')

    Returns:
        date: Объект date
    """
    if value is None:
        logger.warning("⚠️ value is None")
        return datetime.strptime(default, "%Y-%m-%d").date()

    if isinstance(value, datetime):
        return value.date()

    if isinstance(value, str):
        try:
            # Очищаем строку от пробелов
            clean_value = value.strip()
            # Берем только дату (до T если есть)
            if "T" in clean_value:
                clean_value = clean_value.split("T")[0]

            # Используем date.fromisoformat
            result = date.fromisoformat(clean_value)
            return result

        except ValueError as e:
            logger.error(f"❌ Ошибка парсинга '{value}': {e}")
            logger.error(traceback.format_exc())
            return datetime.strptime(default, "%Y-%m-%d").date()

    logger.error(f"❌ Неподдерживаемый тип {type(value)}")
    return datetime.strptime(default, "%Y-%m-%d").date()


def transfer(**kwargs: Any) -> int:
    """
    Consumer для переноса данных из Kafka в ClickHouse.

    Читает сообщения из указанных топиков Kafka и вставляет их
    в соответствующие таблицы ClickHouse.
    Поддерживает graceful shutdown и обработку ошибок.

    Args:
        **kwargs: Параметры подключения:
            clickhouse_host (str): Хост ClickHouse (default: '../clickhouse')
            clickhouse_port (int): Порт ClickHouse (default: 9000)
            clickhouse_user (str): Пользователь ClickHouse (default: 'default')
            clickhouse_password (str): Пароль ClickHouse (default: '')
            clickhouse_db (str): База данных ClickHouse (default: 'default')
            kafka_broker (str): Адрес Kafka broker (default: 'kafka:9092')
            kafka_group (str): Группа consumer'а (default: 'clickhouse_group')
            kafka_topics (str): Топики через запятую
                 (default: 'products,stores,customers,purchases')
            max_time (int): Максимальное время работы в секундах (default: 120)
            batch_size (int): Размер батча для логирования (default: 1000)

    Returns:
        int: Количество обработанных сообщений

    Raises:
        Exception: При критических ошибках подключения
    """

    # Параметры
    ch_host = kwargs.get("clickhouse_host", "../clickhouse")
    ch_port = kwargs.get("clickhouse_port", 9000)
    ch_user = kwargs.get("clickhouse_user", "default")
    ch_password = kwargs.get("clickhouse_password", "")
    ch_db = kwargs.get("clickhouse_db", "default")
    kafka_broker = kwargs.get("kafka_broker", "kafka:9092")
    kafka_group = kwargs.get("kafka_group", "clickhouse_group")
    topics = (
        kwargs
        .get(
            "kafka_topics",
            "products,stores,customers,purchases"
        ).split(",")
    )
    max_time = kwargs.get("max_time", 120)
    batch_size = kwargs.get("batch_size", 1000)

    client = None
    consumer = None
    msg_count = 0
    last_commit_time = datetime.now()
    # Инициализируем обработчик сигналов
    exiter = GracefulExiter()
    try:
        # Подключение к ClickHouse
        logger.info(f"Подключение к ClickHouse {ch_host}:{ch_port}/{ch_db}")
        client = Client(
            host=ch_host,
            port=int(ch_port),
            user=ch_user,
            password=ch_password,
            database=ch_db,
            connect_timeout=10,
            settings={"use_client_time_zone": False},
        )
        client.execute("SELECT 1")
        logger.info("✅ Подключение к ClickHouse установлено")

        # Создание таблиц
        tm = TableManager(client)
        for topic in topics:
            table = TableManager.TOPIC_TABLE.get(topic)
            if table:
                tm.create_table(table)
                logger.info(f"✅ Таблица {table} готова для RAW слоя")
        # Создаем все справочники и MV
        tm.create_all_tables()
        # Kafka consumer
        consumer = Consumer(
            {
                "bootstrap.servers": kafka_broker,
                "group.id": kafka_group,
                "auto.offset.reset": "latest",
                "enable.auto.commit": False,
                "session.timeout.ms": 30000,
                "max.poll.interval.ms": 600000,
            }
        )

        consumer.subscribe(topics)
        logger.info(f"📡 Подписка на топики: {topics}")

        start = datetime.now()
        error_count = 0

        while (
                (datetime.now() - start).seconds < max_time
                and not exiter.exit()
        ):
            msg = None
            try:
                msg = consumer.poll(1.0)
                if msg is None:
                    continue

                if msg.error():
                    if msg.error().code() != KafkaError._PARTITION_EOF:
                        logger.error(f"❌ Ошибка Kafka: {msg.error()}")
                        error_count += 1
                        if error_count > 100:
                            raise Exception("Слишком много ошибок")
                    continue

                # Периодически коммитим, даже если нет сообщений
                if (datetime.now() - last_commit_time).seconds > 30:
                    consumer.commit(asynchronous=False)
                    last_commit_time = datetime.now()

                error_count = 0

                # Парсинг сообщения
                value = json.loads(msg.value().decode("utf-8"))

                # Определяем таблицу
                table = TableManager.TOPIC_TABLE.get(msg.topic())
                if not table:
                    logger.warning(f"⚠️ Нет таблицы для топика {msg.topic()}")
                    consumer.commit(msg)
                    continue

                # Вставка для products
                if msg.topic() == "products":
                    product_data = value

                    # Извлекаем вложенные структуры
                    kbju = product_data.get("kbju", {})
                    manufacturer = product_data.get("manufacturer", {})

                    kafka_send_date = parse_datetime(
                        product_data.get("kafka_send_date")
                    )

                    client.execute(
                        f"""
                        INSERT INTO {table}
                        (id, name, group, description, price,
                         unit, origin_country,
                         calories, protein, fat, carbohydrates,
                         expiry_days, is_organic, barcode,
                         manufacturer_name, manufacturer_country,
                         manufacturer_website,
                         manufacturer_inn, load_date)
                        VALUES
                    """,
                        [
                            (
                                product_data.get("id", ""),
                                product_data.get("name", ""),
                                product_data.get("group", ""),
                                product_data.get("description", ""),
                                safe_float_convert(
                                    product_data.get("price", 0.0)
                                ),
                                product_data.get("unit", ""),
                                product_data.get("origin_country", ""),
                                safe_float_convert(
                                    kbju.get("calories", 0)
                                ),
                                safe_float_convert(
                                    kbju.get("protein", 0)
                                ),
                                safe_float_convert(
                                    kbju.get("fat", 0)
                                ),
                                safe_float_convert(
                                    kbju.get("carbohydrates", 0)
                                ),
                                safe_int_convert(
                                    product_data.get("expiry_days", 0)
                                ),
                                safe_bool_convert(
                                    product_data.get("is_organic", False)
                                ),
                                product_data.get("barcode", ""),
                                manufacturer.get("name", ""),
                                manufacturer.get("country", ""),
                                manufacturer.get("website", ""),
                                manufacturer.get("inn", ""),
                                kafka_send_date,
                            )
                        ],
                    )

                # Вставка для customers
                elif msg.topic() == "customers":
                    customer_data = value

                    # Извлекаем вложенные структуры с защитой от None
                    purchase_location = (
                            customer_data.get("purchase_location") or {}
                    )
                    delivery_address = (
                            customer_data.get("delivery_address") or {}
                    )
                    preferences = customer_data.get("preferences") or {}

                    # Парсим даты
                    birth_date = parse_date(customer_data.get("birth_date"))
                    registration_date = parse_datetime(
                        customer_data.get("registration_date")
                    )
                    kafka_send_date = parse_datetime(
                        customer_data.get("kafka_send_date")
                    )

                    client.execute(
                        f"""
                        INSERT INTO {table}
                        (customer_id, first_name, last_name, email,
                         phone, birth_date, gender, registration_date,
                         is_loyalty_member, loyalty_card_number,
                         purchase_location_store_id,
                         purchase_location_store_name,
                         purchase_location_store_network,
                         purchase_location_store_type_description,
                         purchase_location_country, purchase_location_city,
                         purchase_location_street, purchase_location_house,
                         purchase_location_postal_code,
                         delivery_address_country, delivery_address_city,
                         delivery_address_street, delivery_address_house,
                         delivery_address_apartment,
                         delivery_address_postal_code,
                         preferred_language, preferred_payment_method,
                         receive_promotions,
                         load_date)
                        VALUES
                    """,
                        [
                            (
                                customer_data.get("customer_id", ""),
                                customer_data.get("first_name", ""),
                                customer_data.get("last_name", ""),
                                customer_data.get("email", ""),
                                customer_data.get("phone", ""),
                                birth_date,
                                customer_data.get("gender", ""),
                                registration_date,
                                safe_bool_convert(
                                    customer_data.get(
                                        "is_loyalty_member", False
                                    )
                                ),
                                customer_data.get("loyalty_card_number", ""),
                                purchase_location.get("store_id", ""),
                                purchase_location.get("store_name", ""),
                                purchase_location.get("store_network", ""),
                                purchase_location.get(
                                    "store_type_description", ""
                                ),
                                purchase_location.get("country", ""),
                                purchase_location.get("city", ""),
                                purchase_location.get("street", ""),
                                purchase_location.get("house", ""),
                                purchase_location.get("postal_code", ""),
                                delivery_address.get("country", ""),
                                delivery_address.get("city", ""),
                                delivery_address.get(
                                    "street", ""
                                ),
                                delivery_address.get(
                                    "house", ""
                                ),
                                delivery_address.get(
                                    "apartment", ""
                                ),
                                delivery_address.get(
                                    "postal_code", ""
                                ),
                                preferences.get(
                                    "preferred_language", ""
                                ),
                                preferences.get(
                                    "preferred_payment_method", ""
                                ),
                                safe_bool_convert(
                                    preferences
                                    .get("receive_promotions", False)
                                ),
                                kafka_send_date,
                            )
                        ],
                    )

                # Вставка для stores
                elif msg.topic() == "stores":
                    store_data = value

                    # Извлекаем вложенные структуры с защитой от None
                    manager = store_data.get("manager") or {}
                    location = store_data.get("location") or {}
                    coordinates = location.get("coordinates") or {}
                    opening_hours = store_data.get("opening_hours") or {}
                    categories = store_data.get("categories") or []

                    last_inventory_date = parse_date(
                        store_data.get("last_inventory_date")
                    )
                    kafka_send_date = (
                        parse_datetime(store_data.get("kafka_send_date"))
                    )

                    client.execute(
                        f"""
                        INSERT INTO {table}
                        (store_id, store_name, store_network,
                         store_type_description, type, categories,
                         manager_name, manager_phone, manager_email,
                         location_country, location_city,
                         location_street, location_house,
                         location_postal_code, location_latitude,
                         location_longitude, opening_hours_mon_fri,
                         opening_hours_sat, opening_hours_sun,
                         accepts_online_orders, delivery_available,
                         warehouse_connected, last_inventory_date,
                         load_date)
                        VALUES
                    """,
                        [
                            (
                                store_data.get("store_id", ""),
                                store_data.get("store_name", ""),
                                store_data.get("store_network", ""),
                                store_data.get("store_type_description", ""),
                                store_data.get("type", ""),
                                categories,
                                manager.get("name", ""),
                                manager.get("phone", ""),
                                manager.get("email", ""),
                                location.get("country", ""),
                                location.get("city", ""),
                                location.get("street", ""),
                                location.get("house", ""),
                                location.get("postal_code", ""),
                                safe_float_convert(
                                    coordinates.get("latitude", 0)
                                ),
                                safe_float_convert(
                                    coordinates.get("longitude", 0)
                                ),
                                opening_hours.get("mon_fri", ""),
                                opening_hours.get("sat", ""),
                                opening_hours.get("sun", ""),
                                safe_bool_convert(
                                    store_data
                                    .get("accepts_online_orders", False)
                                ),
                                safe_bool_convert(
                                    store_data
                                    .get("delivery_available", False)
                                ),
                                safe_bool_convert(
                                    store_data
                                    .get("warehouse_connected", False)
                                ),
                                last_inventory_date,
                                kafka_send_date,
                            )
                        ],
                    )

                # Вставка для purchases
                elif msg.topic() == "purchases":
                    purchase_data = value
                    # Извлекаем вложенные структуры с защитой от None
                    pd_customer = purchase_data.get("customer") or {}
                    pd_store = purchase_data.get("store") or {}
                    pd_store_location = pd_store.get("location") or {}
                    pd_delivery_address = (
                            purchase_data.get("delivery_address") or {}
                    )
                    pd_items = purchase_data.get("items") or []

                    items_str = json.dumps(pd_items, ensure_ascii=False)

                    # Парсим даты
                    purchase_datetime = parse_datetime(
                        purchase_data.get("purchase_datetime")
                    )
                    kafka_send_date = parse_datetime(
                        purchase_data.get("kafka_send_date")
                    )
                    client.execute(
                        f"""
                        INSERT INTO {table}
                        (purchase_id, customer_id, customer_first_name,
                        customer_last_name,
                         customer_email, customer_phone,
                         customer_is_loyalty_member,
                         customer_loyalty_card_number, store_id,
                         store_name, store_network,
                         store_type_description, store_city, store_street,
                         store_house, store_postal_code, items,
                         delivery_city, delivery_street, delivery_house,
                         delivery_apartment, delivery_postal_code,
                         total_amount, payment_method,
                         is_delivery, purchase_datetime, load_date)
                        VALUES
                    """,
                        [
                            (
                                purchase_data.get("purchase_id", ""),
                                pd_customer.get("customer_id", ""),
                                pd_customer.get("first_name", ""),
                                pd_customer.get("last_name", ""),
                                pd_customer.get("email", ""),
                                pd_customer.get("phone", ""),
                                safe_bool_convert(
                                    pd_customer.get("is_loyalty_member", False)
                                ),
                                pd_customer.get("loyalty_card_number", ""),
                                pd_store.get("store_id", ""),
                                pd_store.get("store_name", ""),
                                pd_store.get("store_network", ""),
                                pd_store.get("store_type_description", ""),
                                pd_store_location.get("city", ""),
                                pd_store_location.get("street", ""),
                                pd_store_location.get("house", ""),
                                pd_store_location.get("postal_code", ""),
                                items_str,
                                pd_delivery_address.get("city", ""),
                                pd_delivery_address.get("street", ""),
                                pd_delivery_address.get("house", ""),
                                pd_delivery_address.get("apartment", ""),
                                pd_delivery_address.get("postal_code", ""),
                                safe_float_convert(
                                    purchase_data.get("total_amount", 0.0)
                                ),
                                purchase_data.get("payment_method", ""),
                                safe_bool_convert(
                                    purchase_data.get("is_delivery", False)
                                ),
                                purchase_datetime,
                                kafka_send_date,
                            )
                        ],
                    )

                consumer.commit(msg)
                msg_count += 1

                if msg_count % batch_size == 0:
                    logger.info(f"📊 Обработано {msg_count} сообщений")

            except Exception as e:
                logger.error(f"❌ Ошибка: {e}")
                logger.error(traceback.format_exc())
                if msg is not None:
                    consumer.commit(msg)
                continue

    finally:
        if consumer:
            consumer.close()
        if client:
            client.disconnect()
        logger.info(f"📊 ИТОГО: Обработано {msg_count} сообщений")

    return msg_count
