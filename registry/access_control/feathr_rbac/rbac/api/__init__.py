from fastapi import FastAPI
from feathr_rbac.rbac.api.v1.access_control import router as control_router
from feathr_rbac.rbac.api.v1.registry import router as registry_router
from feathr_rbac.rbac.config import RBACConfig
from starlette.middleware.cors import CORSMiddleware


def get_application() -> FastAPI:
    app = FastAPI()
    # Enables CORS
    app.add_middleware(CORSMiddleware,
                       allow_origins=["*"],
                       allow_credentials=True,
                       allow_methods=["*"],
                       allow_headers=["*"],
                       )

    app.include_router(prefix=RBACConfig.RBAC_API_BASE, router=control_router, tags=["access_control"])
    app.include_router(prefix=RBACConfig.RBAC_API_BASE, router=registry_router, tags=["registry"])
    return app
