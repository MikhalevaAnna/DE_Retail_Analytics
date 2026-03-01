from pyspark.sql import DataFrame, SparkSession
from datetime import datetime
import boto3
from botocore.client import Config
from botocore.exceptions import ClientError
import tempfile
import os
from utils.logging_config import get_airflow_logger

logger = get_airflow_logger()


class S3Writer:
    """Записывает DataFrame в S3 (Selectel) как CSV."""

    def __init__(
        self,
        spark: SparkSession,
        endpoint: str,
        access_key: str,
        secret_key: str,
        bucket: str,
    ) -> None:
        """
        Инициализация S3 писателя.

        Args:
            spark: Spark сессия
            endpoint: Endpoint S3 хранилища
                (например, https://s3.ru-7.selcloud.ru)
            access_key: Ключ доступа
            secret_key: Секретный ключ
            bucket: Название бакета
        """
        self.spark = spark
        self.endpoint = endpoint
        self.access_key = access_key
        self.secret_key = secret_key
        self.bucket = bucket

        # S3 клиент
        self.s3_client = boto3.client(
            "s3",
            endpoint_url=endpoint,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            config=Config(
                s3={"addressing_style": "path"},
                connect_timeout=30,
                retries={"max_attempts": 3},
            ),
        )

        # Проверяем подключение при инициализации
        self.check_connection()

    def check_connection(self) -> bool:
        """
        Проверяет подключение к S3 Selectel.

        Выполняет следующие проверки:
        1. Проверяет доступность бакета (head_bucket)
        2. Пробует записать тестовый объект
        3. Удаляет тестовый объект

        Returns:
            bool: True если подключение успешно

        Raises:
            ClientError: При ошибках подключения с детализацией причины
        """
        try:
            logger.info("🔍 Проверка подключения к S3 Selectel...")
            logger.info(f"📌 Endpoint: {self.endpoint}")
            logger.info(f"📌 Bucket: {self.bucket}")
            logger.info("📌 Регион: ru-7")

            # Проверяем доступность бакета
            self.s3_client.head_bucket(Bucket=self.bucket)
            logger.info(f"✅ Бакет '{self.bucket}' доступен")
            logger.info("✅ Подключение к S3 Selectel успешно!")
            return True

        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code", "Unknown")
            logger.error(f"❌ Ошибка подключения к S3: {error_code}")
            logger.error(f"❌ Детали: {e}")

            if error_code == "403":
                logger.error("❌ Доступ запрещен. Проверьте ключи доступа.")
            elif error_code == "404":
                logger.error(f"❌ Бакет '{self.bucket}' не найден.")
            elif error_code == "400":
                logger.error("❌ Bad Request. Проверьте endpoint и регион.")

            raise

    def _configure_hadoop(self) -> None:
        """
        Настраивает Hadoop конфигурацию для Selectel.

        Устанавливает необходимые параметры для работы Spark с S3:
        - endpoint, ключи доступа
        - регион ru-7
        - path style access
        - подпись v4
        - таймауты и настройки для больших файлов
        """
        hadoop_conf = self.spark._jsc.hadoopConfiguration()

        # Очищаем старые настройки
        hadoop_conf.unset("fs.s3a.endpoint.region")
        hadoop_conf.unset("fs.s3a.signing-algorithm")

        # Базовые настройки для Selectel
        hadoop_conf.set("fs.s3a.endpoint", self.endpoint)
        hadoop_conf.set("fs.s3a.access.key", self.access_key)
        hadoop_conf.set("fs.s3a.secret.key", self.secret_key)
        hadoop_conf.set("fs.s3a.path.style.access", "true")
        hadoop_conf.set("fs.s3a.connection.ssl.enabled", "true")

        # ВАЖНО: регион ru-7
        hadoop_conf.set("fs.s3a.endpoint.region", "ru-7")

        # Для Selectel нужна подпись v4
        hadoop_conf.set("fs.s3a.signing-algorithm", "S3SignerType")

        # Настройки для работы с большими файлами
        hadoop_conf.set("fs.s3a.connection.timeout", "600000")
        hadoop_conf.set("fs.s3a.socket.timeout", "600000")
        hadoop_conf.set("fs.s3a.attempts.maximum", "20")
        hadoop_conf.set("fs.s3a.fast.upload", "true")
        hadoop_conf.set("fs.s3a.multipart.size", "104857600")

        logger.debug("✅ Hadoop конфигурация для Selectel (ru-7) настроена")

    def write_csv(self, df: DataFrame, prefix: str = "analytics") -> str:
        """
        Записывает DataFrame в S3 Selectel как CSV.

        Процесс записи:
        1. Формирует имя файла на основе текущей даты
        2. Создает временную директорию
        3. Сохраняет DataFrame как CSV (один файл через coalesce(1))
        4. Находит part-файл и переименовывает его
        5. Загружает файл в S3
        6. Удаляет временные файлы

        Args:
            df: DataFrame для записи
            prefix: Префикс пути в бакете (папка)

        Returns:
            str: Путь к сохраненному файлу в формате
                 s3a://bucket/prefix/filename.csv

        Raises:
            Exception: Если не найден part-файл после записи
        """

        # Формируем имя файла
        date_str = datetime.now().strftime("%Y_%m_%d")
        filename = f"analytic_result_{date_str}.csv"

        # Создаем временную директорию
        with tempfile.TemporaryDirectory() as tmp_dir:
            local_path = os.path.join(tmp_dir, filename)

            logger.info(f"💾 Запись во временный файл: {local_path}")

            # Сохраняем локально как CSV (одним файлом)
            (
                df.coalesce(1)
                .write.mode("overwrite")
                .option("header", "true")
                .option("delimiter", ",")
                .csv(tmp_dir)
            )

            # Находим part-файл
            part_files = [
                f
                for f in os.listdir(tmp_dir)
                if f.startswith("part-") and f.endswith(".csv")
            ]
            if part_files:
                part_file = os.path.join(tmp_dir, part_files[0])

                # Переименовываем в нужное имя
                final_local = os.path.join(tmp_dir, filename)
                os.rename(part_file, final_local)

                # Загружаем в S3
                final_key = f"{prefix}/{filename}" if prefix else filename

                logger.info(f"📤 Загрузка в S3: {final_key}")

                self.s3_client.upload_file(
                    Filename=final_local, Bucket=self.bucket, Key=final_key
                )

                s3_path = f"s3a://{self.bucket}/{final_key}"
                logger.info(f"✅ Файл сохранён: {s3_path}")

                return s3_path
            else:
                raise Exception("Не найден part-файл")
