import os
import sys

import uvicorn
from loguru import logger

if __name__ == "__main__":
    logger.remove()
    logger.add(sys.stdout, level=os.getenv("LOGGING_LEVEL", "DEBUG"))
    uvicorn.run(
        "endpoints:app",
        host="0.0.0.0",
        port=int(os.getenv("FEATHER_REGISTRY_LISTENING_PORT", 8000)),
        log_level=os.getenv("UVICORN_LOG_LEVEL", "info")
    )
