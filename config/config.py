from airflow.hooks.base import BaseHook
import json


# Получаем из Airflow Connections
clickhouse_conn = BaseHook.get_connection("clickhouse_raw")
CLICKHOUSE_HOST = clickhouse_conn.host or "clickhouse"
CLICKHOUSE_NATIVE_PORT = clickhouse_conn.port or 9000
CLICKHOUSE_HTTP_PORT = 8123
CLICKHOUSE_USER = clickhouse_conn.login or "default"
CLICKHOUSE_PASSWORD = clickhouse_conn.password or "strongpassword"
CLICKHOUSE_DB = clickhouse_conn.schema or "mart_data"

s3_conn = BaseHook.get_connection("s3_default")
extra = json.loads(s3_conn.extra or "{}")
S3_ENDPOINT = extra.get("endpoint_url", "https://s3.ru-7.storage.selcloud.ru")
S3_ACCESS_KEY = s3_conn.login
S3_SECRET_KEY = s3_conn.password
S3_REGION = extra.get("region_name", "ru-7")
S3_BUCKET = extra.get("bucket", "reports-for-customer")
S3_PREFIX = "analytics"

# Получаем из Airflow connection
# Значения по умолчанию (на случай отсутствия connection)
MONGO_DATABASE = "retail_data"
MONGO_USER = "admin"
MONGO_PASSWORD = "password123"
MONGO_HOST = "mongodb"
MONGO_PORT = 27017
MONGO_AUTH_SOURCE = "admin"
mongo_conn = BaseHook.get_connection("mongodb_default")

# Берем значения из connection
MONGO_HOST = mongo_conn.host or MONGO_HOST
MONGO_PORT = mongo_conn.port or MONGO_PORT
MONGO_USER = mongo_conn.login or MONGO_USER
MONGO_PASSWORD = mongo_conn.password or MONGO_PASSWORD
MONGO_DATABASE = mongo_conn.schema or MONGO_DATABASE

# Парсим extra
extra = json.loads(mongo_conn.extra or "{}")
MONGO_AUTH_SOURCE = extra.get("authSource", MONGO_AUTH_SOURCE)

# Формируем URI
MONGO_URI = (
    f"mongodb://{MONGO_USER}:{MONGO_PASSWORD}@{MONGO_HOST}:"
    f"{MONGO_PORT}/{MONGO_DATABASE}?authSource={MONGO_AUTH_SOURCE}"
)
