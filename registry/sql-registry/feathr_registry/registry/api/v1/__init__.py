from feathr_registry.registry.config import RegistryConfig
from feathr_registry.registry.db_registry import DbRegistry

registry = DbRegistry(
    db_name=RegistryConfig.FEATHR_REGISTRY_DATABASE,
    conn_str=RegistryConfig.FEATHR_REGISTRY_CONNECTION_STR
)
