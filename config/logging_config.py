import configparser
import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path

APP_LOGGER_NAME = "stock_analysis"
DEFAULT_LOG_FILE_NAME = "stock_analysis.log"
DEFAULT_LOG_DIR_NAME = "logs"
DEFAULT_LOG_LEVEL = "INFO"
DEFAULT_MAX_BYTES = 5 * 1024 * 1024
DEFAULT_BACKUP_COUNT = 5


def _project_root():
    return Path(__file__).resolve().parent.parent


def _load_logging_settings():
    parser = configparser.ConfigParser()
    parser.read(_project_root() / "config" / "config.ini")

    raw_log_dir = parser.get("Logging", "LOG_DIR", fallback=DEFAULT_LOG_DIR_NAME)
    log_dir_name = raw_log_dir.strip().strip('"').strip("'") or DEFAULT_LOG_DIR_NAME
    log_level = parser.get("Logging", "LOG_LEVEL", fallback=DEFAULT_LOG_LEVEL).upper()
    log_file_name = parser.get("Logging", "LOG_FILE_NAME", fallback=DEFAULT_LOG_FILE_NAME)

    return {
        "log_dir": _project_root() / log_dir_name,
        "log_level": getattr(logging, log_level, logging.INFO),
        "log_file_name": log_file_name,
    }


def setup_logging():
    settings = _load_logging_settings()
    logger = logging.getLogger(APP_LOGGER_NAME)

    if getattr(logger, "_stock_analysis_configured", False):
        return logger

    logger.setLevel(settings["log_level"])
    logger.propagate = False

    log_dir = settings["log_dir"]
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / settings["log_file_name"]

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    )

    file_handler = RotatingFileHandler(
        log_path,
        maxBytes=DEFAULT_MAX_BYTES,
        backupCount=DEFAULT_BACKUP_COUNT,
        encoding="utf-8",
    )
    file_handler.setLevel(settings["log_level"])
    file_handler.setFormatter(formatter)

    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(settings["log_level"])
    stream_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    #logger.addHandler(stream_handler)
    logger._stock_analysis_configured = True
    logger.info("Logging initialized. log_file=%s", log_path)
    return logger


def get_logger(name):
    setup_logging()
    return logging.getLogger(f"{APP_LOGGER_NAME}.{name}")
