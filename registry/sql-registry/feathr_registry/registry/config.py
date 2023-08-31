import dataclasses
import os


@dataclasses.dataclass
class RegistrySettings:
    # Database settings
    FEATHR_REGISTRY_DATABASE: str = os.getenv("FEATHR_REGISTRY_DATABASE", "sqlite")
    FEATHR_REGISTRY_CONNECTION_STR: str = os.getenv("FEATHR_REGISTRY_CONNECTION_STR")

    # API settings
    FEATHR_REGISTRY_LISTENING_PORT = int(os.getenv("FEATHR_REGISTRY_LISTENING_PORT", 8000))
    FEATHR_API_BASE = os.getenv("FEATHR_API_BASE", "/api/v1")
    REGISTRY_DEBUGGING = bool(os.environ.get("REGISTRY_DEBUGGING", 0))


RegistryConfig = RegistrySettings()
