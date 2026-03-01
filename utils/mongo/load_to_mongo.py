"""
Модуль для загрузки JSON-данных в MongoDB.
Загружает файлы из структурированных директорий в соответствующие коллекции.
"""

import sys
import json
from os import path, listdir
from datetime import datetime
from typing import Dict, List, Any
from pymongo.database import Database
from pymongo.collection import Collection
from logging import Logger
from utils.logging_config import get_airflow_logger

# Импорты проекта
from config.constants import Config
from .mongo_connector import MongoDB


def load_json_files(directory: str, collection: Collection, logger: Logger) -> int:
    """
    Загружает JSON-файлы из указанной директории в коллекцию MongoDB.

    Args:
        directory (str): Путь к директории с JSON-файлами
        collection: Коллекция MongoDB для вставки данных
        logger: Логгер для записи информации

    Returns:
        int: Количество загруженных файлов
    """
    inserted_count: int = 0

    # Проверка существования директории
    if not path.exists(directory):
        logger.warning(f"⚠️ Директория не существует: {directory}")
        return inserted_count

    # Получаем список JSON-файлов
    json_files: List[str] = [f for f in listdir(directory) if f.endswith(".json")]

    if not json_files:
        logger.warning(f"⚠️ Нет JSON-файлов в директории: {directory}")
        return inserted_count

    logger.info(f"📁 Найдено {len(json_files)} файлов в {directory}")

    # Загружаем каждый файл
    for filename in json_files:
        filepath: str = path.join(directory, filename)

        try:
            # Чтение и загрузка JSON
            with open(filepath, "r", encoding="utf-8") as f:
                data: Dict[str, Any] = json.load(f)

            # Добавляем метаданные
            if isinstance(data, dict):
                data["_loaded_at"] = datetime.now().isoformat()
                data["_source_file"] = filename

            # Вставка в MongoDB
            collection.insert_one(data)
            inserted_count += 1
            logger.debug(f"  ✓ Загружен: {filename}")

        except json.JSONDecodeError as e:
            logger.error(f"❌ Ошибка парсинга JSON в файле {filename}: {e}")
        except UnicodeDecodeError as e:
            logger.error(f"❌ Ошибка кодировки в файле {filename}: {e}")
        except Exception as e:
            logger.error(f"❌ Непредвиденная ошибка при загрузке {filename}: {e}")

    logger.info(f"✅ Загружено {inserted_count} из {len(json_files)} файлов")
    return inserted_count


def clear_collections(db: Database, collections: List[str], logger: Logger) -> None:
    """Очистка коллекций перед загрузкой"""
    for collection_name in collections:
        try:
            result = db[collection_name].delete_many({})
            logger.info(
                f"🧹 Коллекция '{collection_name}' очищена "
                f"(удалено {result.deleted_count} записей)"
            )
        except Exception as e:
            logger.error(f"❌ Ошибка при очистке коллекции {collection_name}: {e}")


def load_all_data(
    db: Database, collections: Dict[str, str], logger: Logger
) -> Dict[str, Dict[str, Any]]:
    """
    Загружает данные из всех директорий в соответствующие коллекции.

    Args:
        db: Объект базы данных MongoDB
        collections: Словарь {collection_name: directory_path}
        logger: Логгер для записи информации

    Returns:
        dict: Статистика загрузки по коллекциям
    """
    stats: Dict[str, Dict[str, Any]] = {}

    for collection_name, directory in collections.items():
        logger.info(f"\n{'=' * 60}")
        logger.info(f"📦 Загрузка в коллекцию: {collection_name}")
        logger.info(f"📁 Директория: {directory}")
        logger.info(f"{'=' * 60}")

        # Загружаем данные
        loaded_count: int = load_json_files(
            directory=directory, collection=db[collection_name], logger=logger
        )

        stats[collection_name] = {"directory": directory, "loaded_files": loaded_count}

    return stats


def print_summary(stats: Dict[str, Dict[str, Any]], logger: Logger) -> None:
    """Вывод итоговой статистики загрузки"""
    logger.info("\n" + "=" * 60)
    logger.info("📊 ИТОГОВАЯ СТАТИСТИКА ЗАГРУЗКИ")
    logger.info("=" * 60)

    total_files: int = 0
    for collection_name, stat in stats.items():
        logger.info(f"📦 {collection_name}: {stat['loaded_files']} файлов")
        total_files += stat["loaded_files"]

    logger.info("-" * 60)
    logger.info(f"✅ ВСЕГО ЗАГРУЖЕНО: {total_files} файлов")
    logger.info("=" * 60)


def main(mongo_client=None, logger=None) -> None:
    """Основная функция выполнения скрипта"""
    # Настройка логирования

    logger = get_airflow_logger()

    logger.info("\n" + "=" * 60)
    logger.info("🚀 ЗАПУСК ЗАГРУЗЧИКА ДАННЫХ В MONGODB")
    logger.info("=" * 60)

    # Подключение к MongoDB
    mongo = MongoDB(logger)
    client, db = mongo.connect()

    try:
        # Очистка коллекций
        logger.info("\n🧹 ОЧИСТКА КОЛЛЕКЦИЙ")
        clear_collections(db, list(Config.MONGO_COLLECTIONS.keys()), logger)

        # Загрузка данных
        logger.info("\n📥 ЗАГРУЗКА ДАННЫХ")
        stats: Dict[str, Dict[str, Any]] = load_all_data(
            db, Config.MONGO_COLLECTIONS, logger
        )

        # Вывод статистики
        print_summary(stats, logger)

        logger.info("\n✅ Загрузка успешно завершена!")

    except Exception as e:
        logger.error(f"\n❌ Критическая ошибка в процессе загрузки: {e}", exc_info=True)
        sys.exit(1)

    finally:
        # Закрытие подключения
        client.close()
        logger.info("🔌 Подключение к MongoDB закрыто")


if __name__ == "__main__":
    main()
