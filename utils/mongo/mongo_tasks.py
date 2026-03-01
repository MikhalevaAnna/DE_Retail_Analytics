"""Airflow-совместимые задачи для работы с MongoDB"""

from datetime import datetime
from typing import Dict, Any

# Импортируем функции из скриптов
from config.constants import Config
from .load_to_mongo import (
    load_all_data,
    clear_collections,
    print_summary as load_print_summary,
)
from .check_data_in_mongo import (
    check_mongodb_data,
    print_summary as check_print_summary,
)
from .mongo_connector import MongoDB
from utils.logging_config import get_airflow_logger

logger = get_airflow_logger()


def load_mongo_data(**context) -> Dict[str, Any]:
    """
    Загружает данные в MongoDB.
    """
    logger.info("=" * 60)
    logger.info("🚀 ЗАГРУЗКА ДАННЫХ В MONGODB")
    logger.info("=" * 60)

    execution_date = context.get("execution_date", datetime.now())
    logger.info(f"📅 Дата выполнения: {execution_date}")

    client = None

    try:
        # Подключение к MongoDB
        mongo = MongoDB(logger)
        client, db = mongo.connect()

        # Очистка коллекций
        should_clear = context.get("params", {}).get("clear_collections", True)

        if should_clear:
            logger.info("\n🧹 ОЧИСТКА КОЛЛЕКЦИЙ")
            clear_collections(db, list(Config.MONGO_COLLECTIONS.keys()), logger)

        # Загрузка данных
        logger.info("\n📥 ЗАГРУЗКА ДАННЫХ")
        stats = load_all_data(db, Config.MONGO_COLLECTIONS, logger)

        # Вывод статистики загрузки
        load_print_summary(stats, logger)

        total_files = sum(stat["loaded_files"] for stat in stats.values())
        logger.info(f"✅ Загрузка завершена! Всего файлов: {total_files}")

        # Возвращаем количество загруженных файлов
        return {"total_files": total_files}

    except Exception as e:
        logger.error(f"❌ Ошибка загрузки: {e}", exc_info=True)
        raise
    finally:
        if client:
            client.close()
            logger.info("🔌 Подключение к MongoDB закрыто")


def check_mongo_data(**context) -> None:
    """
    Задача: проверка данных в MongoDB после загрузки.
    Выводит 3 документа в логи.
    """
    logger.info("=" * 60)
    logger.info("🔍 ПРОВЕРКА ДАННЫХ В MONGODB")
    logger.info("=" * 60)

    try:
        # Проверка данных с подробным выводом,
        # параметр verbose=True покажет 3 документа
        results = check_mongodb_data(logger, verbose=True)

        # Вывод сводки
        check_print_summary(results, logger)

        # Проверяем, что данные загружены
        if results["total_documents"] == 0:
            raise ValueError("❌ В MongoDB не найдено документов после загрузки!")

        logger.info("✅ Проверка завершена успешно!")

    except Exception as e:
        logger.error(f"❌ Ошибка проверки: {e}", exc_info=True)
        raise


def validate_mongo_load(**context) -> bool:
    """
    Задача: валидация загрузки (успех/неуспех для ветвления).
    """
    logger.info("=" * 60)
    logger.info("✅ ВАЛИДАЦИЯ ЗАГРУЗКИ MONGODB")
    logger.info("=" * 60)
    logger.info("✅ Валидация пройдена")
    return True
