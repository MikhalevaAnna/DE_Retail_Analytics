"""
Класс для работы с MongoDB.
Объединяет функции загрузки конфигурации и подключения.
"""

from typing import Dict, Tuple
from pymongo import MongoClient, errors
from config.config import MONGO_DATABASE, MONGO_URI
from logging import Logger


class MongoDB:
    """
    Класс для подключения к MongoDB.

    Пример использования:
        mongo = MongoDB(logger)
        client, db = mongo.connect()
    """

    def __init__(self, logger: Logger):
        """
        Инициализация с логгером.

        Args:
            logger: Логгер для вывода информации
        """
        self.logger = logger
        self.config = self._get_config()

    def _get_config(self) -> Dict[str, str]:
        """
        Получение конфигурации MongoDB из переменных окружения.

        Returns:
            Dict с параметрами подключения

        Raises:
            ValueError: если отсутствуют обязательные переменные
        """
        config = {"uri": MONGO_URI, "database": MONGO_DATABASE}

        self.logger.debug("Загружена конфигурация MongoDB")
        self.logger.debug(f"  URI: {config['uri']}")
        self.logger.debug(f"  Database: {config['database']}")

        # Проверка обязательных параметров
        missing_vars = []
        if not config["uri"]:
            missing_vars.append(MONGO_URI)
        if not config["database"]:
            missing_vars.append(MONGO_DATABASE)

        if missing_vars:
            error_msg = (
                f"Отсутствуют обязательные "
                f"переменные окружения: {', '.join(missing_vars)}"
            )
            self.logger.error(f"❌ {error_msg}")
            raise ValueError(error_msg)

        return config

    def connect(self, timeout_ms: int = 5000) -> Tuple[MongoClient, any]:
        """
        Установка соединения с MongoDB.

        Args:
            timeout_ms: Таймаут подключения в миллисекундах

        Returns:
            tuple: (client, db) - объекты клиента и базы данных

        Raises:
            ConnectionError: при ошибках подключения
            ValueError: при неправильном URI
            Exception: при других ошибках
        """
        client = None
        try:
            self.logger.info("🔄 Подключение к MongoDB...")
            self.logger.debug(f"📌 База данных: {self.config['database']}")
            self.logger.debug(f"📌 Таймаут: {timeout_ms}ms")

            # Подключение с таймаутом
            client = MongoClient(
                self.config["uri"], serverSelectionTimeoutMS=timeout_ms
            )

            # Проверка подключения
            client.admin.command("ping")
            self.logger.info("✅ Подключение успешно установлено")

            db = client[self.config["database"]]
            return client, db

        except errors.ConnectionFailure as e:
            error_msg = f"Не удалось подключиться к MongoDB: {e}"
            self.logger.error(f"❌ {error_msg}")
            if client:
                client.close()
            raise ConnectionError(error_msg)

        except errors.InvalidURI as e:
            error_msg = f"Неправильный формат URI: {e}"
            self.logger.error(f"❌ {error_msg}")
            if client:
                client.close()
            raise ValueError(error_msg)

        except Exception as e:
            error_msg = f"Ошибка при подключении: {e}"
            self.logger.error(f"❌ {error_msg}")
            if client:
                client.close()
            raise

    @property
    def uri(self) -> str:
        """URI подключения"""
        return self.config["uri"]

    @property
    def database_name(self) -> str:
        """Имя базы данных"""
        return self.config["database"]
