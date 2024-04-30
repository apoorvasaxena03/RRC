import os
import logging
# %%
class CustomLogger:
    def __init__(self, logger_name: str, log_dir: str):
        self.logger = logging.getLogger(logger_name)
        self.logger.setLevel(logging.DEBUG)
        self.log_dir = log_dir
        self._create_file_handler()
        self._create_stream_handler()


    def _create_file_handler(self):
        file_name = os.path.basename(__file__).split('.')[0] + ".log"
        file_path = os.path.join(self.log_dir, file_name)
        file_handler = logging.FileHandler(file_path, mode='a+', encoding='utf-8')
        file_handler.setLevel(logging.DEBUG)
        formatter = logging.Formatter(
            fmt = '[%(name)s] %(levelname)s (%(asctime)s): %(message)s (Line: %(lineno)d) [%(filename)s]', datefmt='%m-%d %I:%M %p'
            )
        file_handler.setFormatter(formatter)
        self.logger.addHandler(file_handler)


    def _create_stream_handler(self):
        stream_handler = logging.StreamHandler()
        stream_handler.setLevel(logging.INFO)
        formatter = logging.Formatter(
            fmt = '[%(name)s] %(levelname)s (%(asctime)s): %(message)s (Line: %(lineno)d) [%(filename)s]', datefmt='%m-%d %I:%M %p'
            )
        stream_handler.setFormatter(formatter)
        self.logger.addHandler(stream_handler)


    def get_logger(self):
        return self.logger
    
    
    def log_dataframe(df, logger: logging.Logger, name: str = "DataFrame") -> None:

        logger.debug(
            f'''{name} head:\n {df.head()}\n----------\n''')


    def log_dataframes(*args, logger: logging.Logger) -> None:

        for gdf in args:

            logger.debug(
                f'''DataFrame head:\n {gdf.head()}\n----------\n''')
# %%
# Usage example:

# if __name__ == "__main__":
#     # Create an instance of CustomLogger with logger name and log directory
#     logger_instance = CustomLogger("example_logger", "/path/to/log/directory")

#     # Get the logger
#     logger = logger_instance.get_logger()

#     # Log some messages
#     logger.debug('This is a debug message')
#     logger.info('This is an info message')
#     logger.warning('This is a warning message')
#     logger.error('This is an error message')
#     logger.critical('This is a critical message')