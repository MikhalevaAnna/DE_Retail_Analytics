"""
Бизнес-логика для переноса данных из MongoDB в Kafka.
"""

import json
import hashlib
import re
from typing import List, Dict, Any, Optional
from datetime import datetime
from utils.logging_config import get_airflow_logger

logger = get_airflow_logger()


class MongoKafkaTransfer:
    """Класс для переноса данных из MongoDB в Kafka."""

    def __init__(self, mongo_conn, kafka_producer,
                 sensitive_topics: List[str] = None):
        self.mongo = mongo_conn
        self.kafka = kafka_producer
        self.sensitive_topics = sensitive_topics or []

    @staticmethod
    def normalize_phone(phone: Any) -> Optional[str]:
        """Нормализует номер телефона."""
        if not phone:
            return phone
        phone = re.sub(r"[^\d+]", "", str(phone).strip())
        if phone.startswith("8"):
            return "+7" + phone[1:]
        return phone

    @staticmethod
    def normalize_email(email: Any) -> Optional[str]:
        """Нормализует email."""
        return str(email).strip().lower() if email else email

    @staticmethod
    def hash_value(value: Any) -> Optional[str]:
        """Хэширует значение."""
        if not value:
            return value
        return hashlib.md5(str(value).encode("utf-8")).hexdigest()

    def _process_value(self, key: str, value: Any) -> Any:
        """
        Обрабатывает одно значение: приводит к нижнему регистру,
        нормализует и хэширует если нужно.
        """
        # Для чувствительных топиков применяем нормализацию и хэширование
        # Проверяем ключи на чувствительность
        if "phone" in key.lower():
            value = self.normalize_phone(value)
            value = self.hash_value(value)
        elif "email" in key.lower():
            value = self.normalize_email(value)
            value = self.hash_value(value)

        return value

    def _hash_sensitive_fields(self, data: Any, key_context: str = "") -> Any:
        """
        Рекурсивно обходит структуру данных и обрабатывает чувствительные поля.

        Args:
            data: Данные для обработки
            key_context: Контекст ключа для понимания типа данных

        Returns:
            Обработанные данные
        """
        if isinstance(data, dict):
            result = {}
            for key, value in data.items():
                # Передаем ключ как контекст для вложенных структур
                result[key] = self._hash_sensitive_fields(value, key)
            return result

        elif isinstance(data, list):
            # Для списков передаем тот же контекст всем элементам
            return [self._hash_sensitive_fields(item, key_context)
                    for item in data]

        else:
            # Для примитивов обрабатываем с учетом контекста
            return self._process_value(key_context, data)

    def process_document(self, doc: Dict, topic: str) -> Dict:
        """Обрабатывает один документ."""
        # Конвертируем ObjectId в строку и приводим к нижнему регистру
        if "_id" in doc:
            doc["_id"] = str(doc["_id"]).lower()

        # Добавляем дату отправки
        doc["kafka_send_date"] = datetime.now().isoformat()

        # Применяем обработку в зависимости от топика
        if topic in self.sensitive_topics:
            # Для чувствительных топиков: lower + нормализация + хэш
            doc = self._hash_sensitive_fields(doc)

        return doc

    def transfer_collection(self,
                            collection_name: str,
                            batch_size: int = 1000) -> int:
        """Переносит одну коллекцию из MongoDB в Kafka."""
        count = 0
        db = self.mongo.get_default_database()

        for doc in db[collection_name].find():
            try:
                processed = self.process_document(doc, collection_name)

                # Используем produce
                self.kafka.produce(
                    topic=collection_name,
                    value=json.dumps(
                        processed,
                        ensure_ascii=False)
                    .encode("utf-8"),
                )
                count += 1

                if count % batch_size == 0:
                    self.kafka.flush()
                    logger.info(
                        f"{collection_name}: обработано {count} документов"
                    )

            except Exception as e:
                logger.error(f"Ошибка в документе {doc.get('_id')}: {e}")

        self.kafka.flush()
        return count

    def transfer_all(self, topics: List[str]) -> Dict[str, int]:
        """Переносит все указанные коллекции."""
        results = {}
        for topic in topics:
            logger.info(f"Начинаем перенос {topic}")
            results[topic] = self.transfer_collection(topic)
            logger.info(f"Завершен {topic}: {results[topic]} документов")
        return results
