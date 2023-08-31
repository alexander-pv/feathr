import dataclasses
import os


@dataclasses.dataclass
class RBACSettings:
    # Authentication
    RBAC_API_CLIENT_ID: str = os.getenv("RBAC_API_CLIENT_ID")
    RBAC_AAD_TENANT_ID: str = os.getenv("RBAC_AAD_TENANT_ID")
    RBAC_AAD_INSTANCE: str = os.getenv("RBAC_AAD_INSTANCE", "https://login.microsoftonline.com")
    RBAC_API_AUDIENCE: str = os.getenv("RBAC_API_AUDIENCE")
    RBAC_DEFAULT_ADMIN: str = os.getenv("RBAC_DEFAULT_ADMIN", "abc@microsoft.com")

    # Registry Database
    RBAC_CONNECTION_STR: str = os.getenv("RBAC_CONNECTION_STR")
    RBAC_DATABASE: str = os.getenv("RBAC_DATABASE")

    # RBAC API
    RBAC_LISTENING_PORT: int = int(os.getenv("RBAC_LISTENING_PORT", 18000))
    RBAC_API_BASE: str = os.getenv("RBAC_API_BASE", "/api/v1")

    # Registry API
    FEATHR_REGISTRY_URL = os.getenv("FEATHR_REGISTRY_URL")


RBACConfig = RBACSettings()
