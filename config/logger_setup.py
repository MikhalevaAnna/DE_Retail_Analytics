import logging
from pathlib import Path
from typing import Optional, Union
from config.constants import Config

LOG_DIR: Path = Path(Config.LOGS_DIR)
LOG_DIR.mkdir(exist_ok=True)


def get_logger(
    name: str,
    filename: Optional[str] = None,
    level: str = "INFO",
    console: bool = True,
    errors_file: bool = True,
    log_folder: Union[str, Path] = LOG_DIR,
) -> logging.Logger:
    """
    Создает или получает логгер с записью в файл.

    Args:
        name: Имя логгера
        filename: Имя файла для логов
        level: Уровень логирования
        console: Выводить ли в консоль
        errors_file: Создавать ли отдельный файл для ошибок
        log_folder: Папка для сохранения логов
    """
    logger = logging.getLogger(name)
    directory = Path(log_folder)
    directory.mkdir(exist_ok=True)

    if not logger.handlers:  # Если логгер еще не настроен
        logger.setLevel(getattr(logging, level.upper()))

        # ✅ СОЗДАЕМ ФОРМАТТЕР С ДАТОЙ
        formatter = logging.Formatter(Config.LOG_FORMAT, Config.DATE_FORMAT)

        if filename:
            # Основной лог (все уровни)
            main_handler = logging.FileHandler(
                directory / filename, encoding="utf-8", mode="a"
            )
            main_handler.setFormatter(formatter)
            main_handler.setLevel(getattr(logging, level.upper()))
            logger.addHandler(main_handler)

            # Лог ошибок (только ERROR и выше)
            if errors_file:
                err_handler = logging.FileHandler(
                    directory / f"ERROR_{filename}", encoding="utf-8", mode="a"
                )
                err_handler.setLevel(logging.ERROR)
                err_handler.setFormatter(formatter)
                logger.addHandler(err_handler)

        # Консоль (если нужно)
        if console:
            console_handler = logging.StreamHandler()
            console_handler.setFormatter(formatter)
            console_handler.setLevel(getattr(logging, level.upper()))
            logger.addHandler(console_handler)

    return logger
