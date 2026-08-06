import logging
import sys


def setup_logging(warning_level_loggers: list[str] = None):
    log_format = logging.Formatter(
        " %(asctime)s | %(levelname)s | %(name)s ::: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(log_format)

    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    root_logger.handlers = []
    root_logger.addHandler(console_handler)

    for target_logger in warning_level_loggers or []:
        logging.getLogger(target_logger).setLevel(logging.WARNING)
