import os
import sys
import json
import random
from pathlib import Path
import uuid
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
from faker import Faker
from unidecode import unidecode
import logging
from config.logger_setup import get_logger
from config.constants import Config

# Добавляем путь к проекту в sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


fake: Faker = Faker("ru_RU")


def setup_logging() -> logging.Logger:
    """Настройка логирования для модуля загрузки"""
    try:
        logger: logging.Logger = get_logger(
            name=Path(Config.GENERATOR_LOG).stem,
            filename=Config.GENERATOR_LOG,
            level="INFO",
            console=True,
            errors_file=True,
            log_folder=Config.GENERATOR_LOG_DIR,
        )
        logger.info("Логирование успешно инициализировано")
        return logger
    except Exception as e:
        print(f"Критическая ошибка при инициализации логирования: {e}")
        sys.exit(1)


def create_directories(logger: logging.Logger) -> str:
    """Создание необходимых директорий для данных"""
    # Определяем базовый путь
    for directory in Config.GENERATOR_DIRECTORIES:
        try:
            # Создаем полный путь к директории
            full_path: str = directory  # Уже полный путь из констант
            os.makedirs(full_path, exist_ok=True)
            logger.debug(f"Директория создана/проверена: {full_path}")
        except PermissionError as e:
            logger.error(f"Ошибка доступа при создании директории {directory}: {e}")
            raise
        except Exception as e:
            logger.error(f"Ошибка создания директории {directory}: {e}")
            raise

    logger.info("Все директории для данных успешно созданы")
    return Config.ROOT_DIR


def generate_stores(
    base_dir: str, logger: logging.Logger
) -> Tuple[List[Dict[str, Any]], Dict[str, str]]:
    """Генерация магазинов"""
    logger.info("Начало генерации магазинов")
    stores: List[Dict[str, Any]] = []
    store_cities: Dict[str, str] = {}

    try:
        for network, count, description in Config.STORE_NETWORKS:
            logger.info(f"Генерация сети '{network}': {count} магазинов")

            # Распределяем магазины по городам
            cities_pool: List[str] = random.sample(
                Config.RUSSIAN_CITIES, min(count, len(Config.RUSSIAN_CITIES))
            )

            for i in range(count):
                try:
                    city: str = cities_pool[i % len(cities_pool)]
                    store_id: str = f"store-{len(stores) + 1:03d}"

                    # Генерируем уникальный адрес в городе
                    street_names: List[str] = [
                        fake.street_name(),
                        f"ул. {fake.last_name()}а",
                        f"пр. {fake.city_name()}",
                        f"ул. {fake.first_name()}а",
                    ]
                    street_name = random.choice(street_names)

                    store: Dict[str, Any] = {
                        "store_id": store_id,
                        "store_name": f"{network} — Магазин на {street_name}",
                        "store_network": network,
                        "store_type_description": description,
                        "type": "offline",
                        "categories": Config.CATEGORIES,
                        "manager": {
                            "name": fake.name(),
                            "phone": fake.phone_number(),
                            "email": fake.email(),
                        },
                        "location": {
                            "country": "Россия",
                            "city": city,
                            "street": street_name,
                            "house": str(random.randint(1, 40)),
                            "postal_code": fake.postcode(),
                            "coordinates": {
                                "latitude": float(fake.latitude()),
                                "longitude": float(fake.longitude()),
                            },
                        },
                        "opening_hours": {
                            "mon_fri": "09:00-21:00"
                            if network == "Большая Пикча"
                            else "08:00-22:00",
                            "sat": "10:00-20:00"
                            if network == "Большая Пикча"
                            else "09:00-21:00",
                            "sun": "10:00-18:00"
                            if network == "Большая Пикча"
                            else "09:00-20:00",
                        },
                        "accepts_online_orders": True,
                        "delivery_available": True
                        if network == "Большая Пикча"
                        else random.choice([True, False]),
                        "warehouse_connected": random.choice([True, False]),
                        "last_inventory_date": (
                            datetime.now() - timedelta(days=random.randint(1, 30))
                        ).strftime("%Y-%m-%d"),
                    }

                    stores.append(store)
                    store_cities[store_id] = city

                    # Сохраняем в файл
                    stores_dir: str = os.path.join(base_dir, "source_data", "stores")
                    os.makedirs(stores_dir, exist_ok=True)
                    file_path: str = os.path.join(stores_dir, f"{store_id}.json")
                    with open(file_path, "w", encoding="utf-8") as f:
                        json.dump(store, f, ensure_ascii=False, indent=2)

                    logger.debug(
                        f"Создан магазин: {store_id} - "
                        f"{store['store_name']} в г. {city}"
                    )

                except Exception as e:
                    logger.error(
                        f"Ошибка при создании магазина {i + 1} сети {network}: {e}"
                    )
                    continue

        logger.info(f"✓ Успешно сгенерировано магазинов: {len(stores)}")
        return stores, store_cities

    except Exception as e:
        logger.error(f"Критическая ошибка при генерации магазинов: {e}")
        raise


def generate_products(base_dir: str, logger: logging.Logger) -> List[Dict[str, Any]]:
    """Генерация товаров"""
    logger.info("Начало генерации товаров")
    products: List[Dict[str, Any]] = []
    product_counter: int = 1

    try:
        for category in Config.CATEGORIES:
            logger.info(f"Генерация товаров категории: {category}")
            category_products: List[Dict[str, Any]] = Config.PRODUCTS_BY_CATEGORY.get(
                category, []
            )

            for product_info in category_products:
                try:
                    manufacturer: Dict[str, Any] = random.choice(
                        Config.MANUFACTURERS_BY_CATEGORY[category]
                    )

                    product: Dict[str, Any] = {
                        "id": f"prd-{product_counter:04d}",
                        "name": product_info["name"],
                        "group": category,
                        "description": f"{product_info['name']}.",
                        "kbju": {
                            "calories": product_info["kbju"]["calories"],
                            "protein": product_info["kbju"]["protein"],
                            "fat": product_info["kbju"]["fat"],
                            "carbohydrates": product_info["kbju"]["carbohydrates"],
                        },
                        "price": round(random.uniform(*product_info["price"]), 2),
                        "unit": "кг"
                        if category in ["🥦 Овощи и зелень", "🍏 Фрукты и ягоды"]
                        else "шт"
                        if category == "🥛 Молочные продукты"
                        else "упаковка",
                        "origin_country": "Россия",
                        "expiry_days": random.randint(*product_info["expiry"]),
                        "is_organic": random.choice([True, False]),
                        "barcode": fake.ean(length=13),
                        "manufacturer": {
                            "name": manufacturer["name"],
                            "country": "Россия",
                            "website": f"https://{manufacturer['website']}",
                            "inn": manufacturer["inn"],
                        },
                    }

                    products.append(product)

                    # Сохраняем в файл
                    products_dir: str = os.path.join(
                        base_dir, "source_data", "products"
                    )
                    os.makedirs(products_dir, exist_ok=True)
                    file_path: str = os.path.join(products_dir, f"{product['id']}.json")
                    with open(file_path, "w", encoding="utf-8") as f:
                        json.dump(product, f, ensure_ascii=False, indent=2)

                    product_counter += 1
                    logger.debug(
                        f"Создан товар: {product['id']} - "
                        f"{product['name']}, "
                        f"цена: {product['price']} руб."
                    )

                except Exception as e:
                    logger.error(
                        f"Ошибка при создании товара "
                        f"{product_info.get('name', 'Unknown')}: {e}"
                    )
                    continue

        logger.info(f"✓ Успешно сгенерировано товаров: {len(products)}")
        return products

    except Exception as e:
        logger.error(f"Критическая ошибка при генерации товаров: {e}")
        raise


def generate_customers(
    base_dir: str, stores: List[Dict[str, Any]], logger: logging.Logger
) -> List[Dict[str, Any]]:
    """Генерация покупателей"""
    logger.info("Начало генерации покупателей")
    customers: List[Dict[str, Any]] = []

    try:
        for store in stores:
            # Создаем 1-3 покупателей для каждого магазина
            num_customers: int = random.randint(1, 3)
            logger.debug(
                f"Генерация {num_customers} покупателей "
                f"для магазина {store['store_id']}"
            )

            for j in range(num_customers):
                try:
                    customer_id: str = f"cus-{len(customers) + 102000}"

                    # Контактная информация
                    first_name: str = fake.first_name()
                    last_name: str = fake.last_name()

                    # Транслитерируем для email
                    first_name_latin: str = unidecode(first_name).lower()
                    last_name_latin: str = unidecode(last_name).lower()

                    # Очищаем от спецсимволов
                    first_name_latin = "".join(
                        c for c in first_name_latin if c.isalnum()
                    )
                    last_name_latin = "".join(c for c in last_name_latin if c.isalnum())

                    # Создаем email
                    email: str = (
                        f"{first_name_latin}.{last_name_latin}@"
                        f"{fake.free_email_domain()}"
                    )

                    customer: Dict[str, Any] = {
                        "customer_id": customer_id,
                        "first_name": first_name,
                        "last_name": last_name,
                        "email": email,
                        "phone": fake.phone_number(),
                        "birth_date": fake.date_of_birth(
                            minimum_age=18, maximum_age=56
                        ).isoformat(),
                        "gender": random.choice(["male", "female"]),
                        "registration_date": (
                            datetime.now() - timedelta(days=random.randint(0, 730))
                        ).isoformat()
                        + "Z",
                        "is_loyalty_member": True,
                        "loyalty_card_number": f"LOYAL-{uuid.uuid4().hex[:9].upper()}",
                        "purchase_location": {
                            "store_id": store["store_id"],
                            "store_name": store["store_name"],
                            "store_network": store["store_network"],
                            "store_type_description": store["store_type_description"],
                            "country": store["location"]["country"],
                            "city": store["location"]["city"],
                            "street": store["location"]["street"],
                            "house": store["location"]["house"],
                            "postal_code": store["location"]["postal_code"],
                        },
                        "delivery_address": {
                            "country": "Россия",
                            "city": store["location"]["city"],
                            "street": fake.street_name(),
                            "house": str(random.randint(1, 50)),
                            "apartment": str(random.randint(1, 150)),
                            "postal_code": fake.postcode(),
                        },
                        "preferences": {
                            "preferred_language": "ru",
                            "preferred_payment_method": random.choice(
                                ["card", "cash", "online"]
                            ),
                            "receive_promotions": random.choice([True, False]),
                        },
                    }

                    customers.append(customer)

                    # Сохраняем в файл
                    customers_dir: str = os.path.join(
                        base_dir, "source_data", "customers"
                    )
                    os.makedirs(customers_dir, exist_ok=True)
                    file_path: str = os.path.join(customers_dir, f"{customer_id}.json")
                    with open(file_path, "w", encoding="utf-8") as f:
                        json.dump(customer, f, ensure_ascii=False, indent=2)

                    logger.debug(
                        f"Создан покупатель: {customer_id} - {first_name} {last_name}"
                    )

                except Exception as e:
                    logger.error(
                        f"Ошибка при создании покупателя "
                        f"для магазина {store['store_id']}: {e}"
                    )
                    continue

        logger.info(f"✓ Успешно сгенерировано покупателей: {len(customers)}")
        return customers

    except Exception as e:
        logger.error(f"Критическая ошибка при генерации покупателей: {e}")
        raise


def generate_purchases(
    base_dir: str,
    stores: List[Dict[str, Any]],
    products: List[Dict[str, Any]],
    customers: List[Dict[str, Any]],
    logger: logging.Logger,
    target_purchases: int = 200,
) -> int:
    """Генерация покупок"""
    logger.info(f"Начало генерации покупок (цель: {target_purchases})")
    purchases_generated: int = 0
    purchase_id_counter: int = 1
    max_attempts: int = 500  # Максимальное количество попыток

    try:
        for attempt in range(max_attempts):
            if purchases_generated >= target_purchases:
                break

            try:
                customer: Dict[str, Any] = random.choice(customers)
                # Выбираем магазин из того же города, что и покупатель
                city: str = customer["purchase_location"]["city"]
                available_stores: List[Dict[str, Any]] = [
                    s for s in stores if s["location"]["city"] == city
                ]

                if not available_stores:
                    logger.warning(
                        f"Нет доступных магазинов в городе {city} "
                        f"для покупателя {customer['customer_id']}"
                    )
                    continue

                store: Dict[str, Any] = random.choice(available_stores)

                # Выбираем 1-5 товаров
                items_count: int = random.randint(1, 5)
                selected_products: List[Dict[str, Any]] = random.sample(
                    products, k=min(items_count, len(products))
                )

                purchase_items: List[Dict[str, Any]] = []
                total: float = 0

                for product in selected_products:
                    qty: int = random.randint(1, 3)
                    total_price: float = round(product["price"] * qty, 2)
                    total += total_price

                    item: Dict[str, Any] = {
                        "product_id": product["id"],
                        "name": product["name"],
                        "category": product["group"],
                        "quantity": qty,
                        "unit": product["unit"],
                        "price_per_unit": product["price"],
                        "total_price": total_price,
                        "kbju": product["kbju"],
                    }

                    # Добавляем производителя только для некоторых категорий
                    if product["group"] in [
                        "🥛 Молочные продукты",
                        "🥩 Мясо, рыба, яйца и бобовые",
                    ]:
                        item["manufacturer"] = product["manufacturer"]

                    purchase_items.append(item)

                # Дата покупки
                purchase_date: datetime = datetime.now() - timedelta(
                    days=random.randint(0, 365)
                )
                is_delivery = random.choice([True, False])
                purchase: Dict[str, Any] = {
                    "purchase_id": f"ord-{purchase_id_counter:05d}",
                    "customer": {
                        "customer_id": customer["customer_id"],
                        "first_name": customer["first_name"],
                        "last_name": customer["last_name"],
                        "email": customer["email"],
                        "phone": customer["phone"],
                        "is_loyalty_member": customer["is_loyalty_member"],
                        "loyalty_card_number": customer["loyalty_card_number"],
                    },
                    "store": {
                        "store_id": store["store_id"],
                        "store_name": store["store_name"],
                        "store_network": store["store_network"],
                        "store_type_description": store["store_type_description"],
                        "location": {
                            "city": store["location"]["city"],
                            "street": store["location"]["street"],
                            "house": store["location"]["house"],
                            "postal_code": store["location"]["postal_code"],
                        },
                    },
                    "items": purchase_items,
                    "total_amount": round(total, 2),
                    "payment_method": random.choice(["card", "cash", "online"]),
                    "is_delivery": is_delivery
                    if store["delivery_available"]
                    else False,
                    "delivery_address": customer["delivery_address"]
                    if is_delivery
                    else None,
                    "purchase_datetime": purchase_date.isoformat() + "Z",
                }

                # Убираем delivery_address если не доставка
                if not purchase["is_delivery"]:
                    del purchase["delivery_address"]

                # Сохраняем в файл
                purchases_dir: str = os.path.join(base_dir, "source_data", "purchases")
                os.makedirs(purchases_dir, exist_ok=True)
                file_path: str = os.path.join(
                    purchases_dir, f"{purchase['purchase_id']}.json"
                )
                with open(file_path, "w", encoding="utf-8") as f:
                    json.dump(purchase, f, ensure_ascii=False, indent=2)

                purchases_generated += 1
                purchase_id_counter += 1

                logger.debug(
                    f"Создана покупка: {purchase['purchase_id']}, сумма: "
                    f"{total} руб., магазин: {store['store_id']}"
                )

                if purchases_generated % 50 == 0:
                    logger.info(f"Прогресс: создано {purchases_generated} покупок")

            except Exception as e:
                logger.error(f"Ошибка при создании покупки: {e}")
                continue

        logger.info(f"✓ Успешно сгенерировано покупок: {purchases_generated}")
        return purchases_generated

    except Exception as e:
        logger.error(f"Критическая ошибка при генерации покупок: {e}")
        raise


def print_statistics(
    base_dir: str,
    stores: List[Dict[str, Any]],
    products: List[Dict[str, Any]],
    customers: List[Dict[str, Any]],
    purchases_generated: int,
    logger: logging.Logger,
) -> None:
    """Вывод статистики"""
    logger.info("=== СТАТИСТИКА ГЕНЕРАЦИИ ===")

    # Подсчет магазинов по сетям
    big_pikcha: int = sum(1 for s in stores if s["store_network"] == "Большая Пикча")
    small_pikcha: int = sum(
        1 for s in stores if s["store_network"] == "Маленькая Пикча"
    )

    # Подсчет товаров по категориям
    products_by_category: Dict[str, int] = {}
    for product in products:
        category: str = product["group"]
        products_by_category[category] = products_by_category.get(category, 0) + 1

    # Города присутствия
    cities_used: set = set(store["location"]["city"] for store in stores)

    # Логирование статистики
    logger.info(
        f"Магазины: {len(stores)} "
        f"(Большая Пикча: {big_pikcha}, "
        f"Маленькая Пикча: {small_pikcha})"
    )
    logger.info(f"Товары: {len(products)}")
    for category, count in products_by_category.items():
        logger.info(f"  {category}: {count} шт.")
    logger.info(f"Покупатели: {len(customers)}")
    logger.info(f"Покупки: {purchases_generated}")
    logger.info(f"Города присутствия: {len(cities_used)}")
    logger.info(f"Первые 10 городов: {list(cities_used)[:10]}")

    # Сохраняем статистику в JSON
    stats: Dict[str, Any] = {
        "generation_date": datetime.now().isoformat(),
        "stores": {
            "total": len(stores),
            "big_pikcha": big_pikcha,
            "small_pikcha": small_pikcha,
            "cities": len(cities_used),
        },
        "products": {"total": len(products), "by_category": products_by_category},
        "customers": len(customers),
        "purchases": purchases_generated,
    }

    stats_path: str = os.path.join(base_dir, "source_data", "generation_stats.json")
    os.makedirs(os.path.dirname(stats_path), exist_ok=True)
    with open(stats_path, "w", encoding="utf-8") as f:
        json.dump(stats, f, ensure_ascii=False, indent=2)

    logger.info(f"Статистика сохранена в файл: {stats_path}")


def main() -> None:
    """Основная функция генерации данных"""
    print("=" * 60)
    print("НАЧАЛО ГЕНЕРАЦИИ ДАННЫХ ДЛЯ МАГАЗИНОВ ПИКЧА")
    print("=" * 60)

    start_time: datetime = datetime.now()
    logger: Optional[logging.Logger] = None

    try:
        logger = setup_logging()
    except Exception as e:
        print(f"❌ Критическая ошибка при инициализации: {e}")
        print("💡 Невозможно продолжить работу без логирования")
        sys.exit(1)

    try:
        # Создание директорий для данных
        base_dir: str = create_directories(logger)

        # Генерация магазинов
        stores, store_cities = generate_stores(base_dir, logger)

        # Генерация товаров
        products: List[Dict[str, Any]] = generate_products(base_dir, logger)

        # Генерация покупателей
        customers: List[Dict[str, Any]] = generate_customers(base_dir, stores, logger)

        # Генерация покупок
        purchases_generated: int = generate_purchases(
            base_dir, stores, products, customers, logger, Config.MAX_TARGET_PURCHASES
        )

        # Вывод статистики
        print_statistics(
            base_dir, stores, products, customers, purchases_generated, logger
        )

        end_time: datetime = datetime.now()
        duration: float = (end_time - start_time).total_seconds()

        logger.info("=" * 60)
        logger.info(f"ГЕНЕРАЦИЯ ЗАВЕРШЕНА ЗА {duration:.2f} СЕКУНД")
        logger.info("=" * 60)

    except PermissionError as e:
        print(f"❌ Ошибка доступа: {e}")
        print("Проверьте права на запись в директории проекта")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Критическая ошибка: {e}")
        if "logger" in locals() and logger:
            logger.critical(f"КРИТИЧЕСКАЯ ОШИБКА В ГЕНЕРАТОРЕ: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
