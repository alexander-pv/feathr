import os
import sys

import uvicorn
from feathr_registry.registry.api import get_application
from feathr_registry.registry.config import RegistryConfig
from loguru import logger

app = get_application()

if __name__ == "__main__":
    logger.remove()
    logger.add(sys.stdout, level=os.getenv("LOGGING_LEVEL", "DEBUG"))
    logger.debug(f"Registry API prefix: {RegistryConfig.FEATHR_API_BASE}")
    uvicorn.run(
        "__main__:app",
        host="0.0.0.0",
        port=RegistryConfig.FEATHR_REGISTRY_LISTENING_PORT,
        log_level=os.getenv("UVICORN_LOG_LEVEL", "info")
    )
