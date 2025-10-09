import logging
from pathlib import Path
from typing import Union

LOG_DIR = Path("logs")
LOG_DIR.mkdir(exist_ok=True)


def get_logger(name: str, log_to_console: bool = False, level: Union[int, str] = logging.INFO) -> logging.Logger:
    """
    Create and return a logger.

    Parameters
    ----------
    name : str
        Name of the logger (e.g., "occ_scraper").
    log_to_console : bool, optional
        If True, also print log messages to the console. Default = False.
    level : int or str, optional
        Logging level (e.g., logging.DEBUG, logging.INFO, "WARNING").
        Default = logging.INFO.

    Raises
    ------
    ValueError
        If `level` is a string but not a valid logging level.
    """
    logger = logging.getLogger(name)
    
    # Convert string levels like "DEBUG" → logging.DEBUG
    if isinstance(level, str):
        level = level.upper()
        if level not in logging._nameToLevel:
            raise ValueError(
                f"Invalid log level: {level}. "
                f"Valid levels are: {list(logging._nameToLevel.keys())}"
            )
        level = logging._nameToLevel[level]

    logger.setLevel(level)

    # Prevent duplicate handlers if function called multiple times
    if not logger.handlers:
        file_handler = logging.FileHandler(LOG_DIR / f"{name}.log")
        file_handler.setLevel(level)
        file_handler.setFormatter(
            logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
        )
        logger.addHandler(file_handler)

        if log_to_console:
            console_handler = logging.StreamHandler()
            console_handler.setLevel(level)
            console_handler.setFormatter(
                logging.Formatter("[%(levelname)s] %(message)s")
            )
            logger.addHandler(console_handler)

    return logger

#%%
#%%
# ----------------------------------------------------------------------
# Example usage:
#
# logger = get_logger("occ_scraper", log_to_console=True, level="DEBUG")
# error_logger = get_logger("occ_errors", level=logging.ERROR)
#
# logger.debug("Debugging details")
# logger.info("Scraper started")
# logger.warning("Response time slower than expected")
# logger.error("Something went wrong!")
#
# # Example invalid case:
# bad_logger = get_logger("bad", level="NOTALEVEL")
# -> ValueError: Invalid log level: NOTALEVEL. Valid levels are: [...]
# ----------------------------------------------------------------------