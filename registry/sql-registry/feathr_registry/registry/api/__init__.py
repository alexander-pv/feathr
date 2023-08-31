import traceback
from http import HTTPStatus
from typing import Dict

from fastapi import FastAPI
from fastapi.responses import JSONResponse
from feathr_registry.registry.api.v1.projects import router as projects_router
from feathr_registry.registry.api.v1.datasources import router as datasources_router
from feathr_registry.registry.api.v1.features import router as features_router
from feathr_registry.registry.api.v1.entities import router as entities_router

from feathr_registry.registry.config import RegistryConfig
from feathr_registry.registry.db_registry import ConflictError
from starlette.middleware.cors import CORSMiddleware


def exc_to_content(e: Exception) -> Dict:
    content = {"message": str(e)}
    if RegistryConfig.REGISTRY_DEBUGGING:
        content["traceback"] = "".join(traceback.TracebackException.from_exception(e).format())
    return content


def get_application() -> FastAPI:
    app = FastAPI()

    @app.exception_handler(ConflictError)
    async def conflict_error_handler(_, exc: ConflictError):
        return JSONResponse(
            status_code=HTTPStatus.CONFLICT,
            content=exc_to_content(exc),
        )

    @app.exception_handler(ValueError)
    async def value_error_handler(_, exc: ValueError):
        return JSONResponse(
            status_code=HTTPStatus.BAD_REQUEST,
            content=exc_to_content(exc),
        )

    @app.exception_handler(TypeError)
    async def type_error_handler(_, exc: ValueError):
        return JSONResponse(
            status_code=HTTPStatus.BAD_REQUEST,
            content=exc_to_content(exc),
        )

    @app.exception_handler(KeyError)
    async def key_error_handler(_, exc: KeyError):
        return JSONResponse(
            status_code=HTTPStatus.NOT_FOUND,
            content=exc_to_content(exc),
        )

    @app.exception_handler(IndexError)
    async def index_error_handler(_, exc: IndexError):
        return JSONResponse(
            status_code=HTTPStatus.NOT_FOUND,
            content=exc_to_content(exc),
        )

    # Enables CORS
    app.add_middleware(CORSMiddleware,
                       allow_origins=["*"],
                       allow_credentials=True,
                       allow_methods=["*"],
                       allow_headers=["*"],
                       )
    app.include_router(prefix=RegistryConfig.FEATHR_API_BASE, router=projects_router, tags=["projects"])
    app.include_router(prefix=RegistryConfig.FEATHR_API_BASE, router=datasources_router, tags=["datasources"])
    app.include_router(prefix=RegistryConfig.FEATHR_API_BASE, router=features_router, tags=["features"])
    app.include_router(prefix=RegistryConfig.FEATHR_API_BASE, router=entities_router, tags=["entities"])
    return app
