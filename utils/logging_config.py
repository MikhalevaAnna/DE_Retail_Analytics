import logging
import sys


def get_airflow_logger() -> logging.Logger:
    """
    Возвращает логгер, совместимый с Airflow.

    Returns:
        logging.Logger: Объект логгера
    """
    if any("airflow" in arg for arg in sys.argv) or "airflow" in sys.modules:
        return logging.getLogger("airflow.task")
    return logging.getLogger(__name__)
