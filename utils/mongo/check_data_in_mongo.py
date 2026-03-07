"""
Модуль для проверки загрузки данных в MongoDB.
Выполняет подключение к базе данных и отображает статистику по коллекциям.
"""

import sys
from typing import List, Dict, Any
from logging import Logger
from pymongo.database import Database
from .mongo_connector import MongoDB
from config.constants import Config
from utils.logging_config import get_airflow_logger


def get_collections_to_check(logger: Logger) -> List[str]:
    """
    Возвращает список коллекций для проверки.

    Args:
        logger: Объект логгера

    Returns:
        List[str]: Список имен коллекций
    """
    logger.debug(
        f"Список коллекций для проверки: {', '.join(Config.MONGO_COLLECTIONS)}"
    )
    return Config.MONGO_COLLECTIONS


def get_collection_stats(
    db: Database, collection_name: str, logger: Logger
) -> Dict[str, Any]:
    """
    Получение статистики по коллекции.

    Args:
        db: Объект базы данных
        collection_name: Имя коллекции
        logger: Объект логгера

    Returns:
        Dict со статистикой коллекции
    """
    logger.debug(f"Получение статистики для коллекции '{collection_name}'")

    collection = db[collection_name]

    # Подсчет документов
    total_docs = collection.count_documents({})
    logger.debug(f"  Найдено документов: {total_docs}")

    # Получение первых документов
    sample_docs = []
    if total_docs > 0:
        cursor = collection.find().limit(3)
        sample_docs = list(cursor)
        logger.debug(f"  Получено примеров: {len(sample_docs)}")

    return {
        "name": collection_name,
        "total_documents": total_docs,
        "sample_documents": sample_docs,
        "is_empty": total_docs == 0,
    }


def display_collection_stats(
    stats: Dict[str, Any], logger: Logger, verbose: bool = True
) -> None:
    """
    Отображение статистики коллекции.

    Args:
        stats: Статистика коллекции
        logger: Объект логгера
        verbose: Подробный вывод (с примерами документов)
    """
    collection_name = stats["name"]
    total_docs = stats["total_documents"]

    if total_docs == 0:
        logger.warning(f"📁 Коллекция '{collection_name}' - ПУСТА")
        return

    logger.info(f"📁 Коллекция '{collection_name}': {total_docs} документов")

    if verbose and stats["sample_documents"]:
        logger.info("  📋 Примеры документов:")
        for i, doc in enumerate(stats["sample_documents"]):
            # Сокращаем вывод для читаемости
            doc_preview = str(doc)
            if len(doc_preview) > 200:
                doc_preview = doc_preview[:200] + "..."
            logger.info(f"    {i + 1}. {doc_preview}")


def get_database_stats(
    db: Database, collection_names: List[str], logger: Logger
) -> List[Dict[str, Any]]:
    """
    Получение статистики по всем коллекциям.

    Args:
        db: Объект базы данных
        collection_names: Список коллекций
        logger: Объект логгера

    Returns:
        List[Dict]: Список со статистикой по каждой коллекции
    """
    logger.info("📊 Сбор статистики по коллекциям...")
    stats = []

    for collection_name in collection_names:
        try:
            collection_stats = get_collection_stats(
                db,
                collection_name,
                logger
            )
            stats.append(collection_stats)
        except Exception as e:
            logger.error(f"⚠️ Ошибка при проверке "
                         f"коллекции {collection_name}: {e}")
            stats.append(
                {
                    "name": collection_name,
                    "total_documents": 0,
                    "sample_documents": [],
                    "is_empty": True,
                    "error": str(e),
                }
            )

    return stats


def check_mongodb_data(logger: Logger, verbose: bool = True) -> Dict[str, Any]:
    """
    Основная функция проверки данных в MongoDB.

    Args:
        logger: Объект логгера
        verbose: Подробный вывод

    Returns:
        Dict с результатами проверки

    Raises:
        Exception: при критических ошибках
    """
    client = None
    results = {
        "success": False,
        "database": None,
        "collections": [],
        "total_documents": 0,
        "errors": [],
    }

    try:
        # Подключение к MongoDB
        mongo = MongoDB(logger)
        client, db = mongo.connect()

        # Получение списка коллекций
        collection_names = get_collections_to_check(logger)

        # Сбор статистики
        stats_list = get_database_stats(db, collection_names, logger)
        results["collections"] = stats_list
        results["database"] = mongo.database_name

        # Подсчет общего количества документов
        results["total_documents"] = (
            sum(s["total_documents"] for s in stats_list)
        )

        # Отображение результатов
        logger.info(f"\n{'=' * 60}")
        logger.info("📊 РЕЗУЛЬТАТЫ ПРОВЕРКИ")
        logger.info(f"{'=' * 60}")
        logger.info(f"📌 База данных: {results['database']}")

        for stats in stats_list:
            display_collection_stats(stats, logger, verbose)

        logger.info(f"\n{'=' * 60}")
        logger.info(f"✅ ВСЕГО ДОКУМЕНТОВ: {results['total_documents']}")
        logger.info(f"{'=' * 60}")

        results["success"] = True
        return results

    except ValueError as e:
        logger.error(f"\n❌ Ошибка конфигурации: {e}")
        results["errors"].append(str(e))
        raise
    except ConnectionError as e:
        logger.error(f"\n❌ Ошибка подключения: {e}")
        results["errors"].append(str(e))
        raise
    except Exception as e:
        logger.error(f"\n❌ Непредвиденная ошибка: {e}", exc_info=True)
        results["errors"].append(str(e))
        raise
    finally:
        if client:
            client.close()
            logger.info("\n🔌 Соединение с MongoDB закрыто")


def print_summary(results: Dict[str, Any], logger: Logger) -> None:
    """
    Вывод краткой сводки результатов.

    Args:
        results: Результаты проверки
        logger: Объект логгера
    """
    logger.info(f"\n{'=' * 60}")
    logger.info("📋 СВОДКА ПРОВЕРКИ")
    logger.info(f"{'=' * 60}")

    if not results["success"]:
        logger.error("❌ Проверка не выполнена")
        if results["errors"]:
            for error in results["errors"]:
                logger.error(f"  • {error}")
        return

    logger.info("✅ Статус: УСПЕШНО")
    logger.info(f"📌 База данных: {results['database']}")
    logger.info(f"📊 Всего документов: {results['total_documents']}")
    logger.info("\nКоллекции:")

    for collection in results["collections"]:
        status = "✅" if not collection["is_empty"] else "⚠️"
        logger.info(
            f"  {status} {collection['name']}: "
            f"{collection['total_documents']} док."
        )


def main() -> None:
    """Главная функция программы."""
    # Настройка логирования
    logger = get_airflow_logger()

    logger.info(f"\n{'=' * 60}")
    logger.info("🚀 ЗАПУСК ПРОВЕРКИ ДАННЫХ В MONGODB")
    logger.info(f"{'=' * 60}")

    try:
        # Проверка данных
        results = check_mongodb_data(logger, verbose=True)

        # Вывод сводки
        print_summary(results, logger)

        # Возвращаем код результата
        if results["total_documents"] > 0:
            logger.info("\n✅ Проверка завершена успешно!")
            sys.exit(0)
        else:
            logger.warning("\n⚠️ Данные не найдены в MongoDB!")
            sys.exit(1)

    except Exception as e:
        logger.error(f"\n❌ Критическая ошибка: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
