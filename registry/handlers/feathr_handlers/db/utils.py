import time

from loguru import logger


def retry(max_attempts: int = 3, delay_secs: int = 1):
    def decorator(func):
        def wrapper(*args, **kwargs):
            for i in range(max_attempts):
                try:
                    result = func(*args, **kwargs)
                    return result
                except Exception as e:
                    logger.error(f"Error: {e}.\nRetrying...{i}")
                    time.sleep(delay_secs)
            raise Exception(f"Max retry attempts reached: {max_attempts}")

        return wrapper

    return decorator
