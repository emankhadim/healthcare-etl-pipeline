import logging
import logging.handlers
from pathlib import Path
from healthcare_etl.core.config import LOGS_DIR

DEFAULT_FMT = "%(asctime)s - %(levelname)-5s - %(message)s"
DATE_FMT = "%Y-%m-%d %H:%M:%S"

def setup_logging(level: str = "INFO", file_name: str = "pipeline.log") -> None:
    if getattr(setup_logging, "_configured", False):
        return  # prevent double-config
    Path(LOGS_DIR).mkdir(parents=True, exist_ok=True)

    root = logging.getLogger()
    root.setLevel(level.upper())

    # Console
    ch = logging.StreamHandler()
    ch.setFormatter(logging.Formatter(DEFAULT_FMT, datefmt=DATE_FMT))
    root.addHandler(ch)

    # Rotating file
    fh = logging.handlers.RotatingFileHandler(
        Path(LOGS_DIR) / file_name, maxBytes=2_000_000, backupCount=3, encoding="utf-8"
    )
    fh.setFormatter(logging.Formatter(DEFAULT_FMT, datefmt=DATE_FMT))
    root.addHandler(fh)

    setup_logging._configured = True
