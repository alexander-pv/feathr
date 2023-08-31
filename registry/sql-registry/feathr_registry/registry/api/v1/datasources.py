from http import HTTPStatus
from typing import Dict, List

from fastapi import APIRouter, HTTPException
from feathr_registry.registry.api.v1 import registry
from feathr_registry.registry.models import base as rgm

router = APIRouter()


@router.get("/projects/{project}/datasources")
def get_project_datasources(project: str) -> List:
    p = registry.get_entity(project)
    source_ids = [s.id for s in p.attributes.sources]
    sources = registry.get_entities(source_ids)
    return list([e.to_dict() for e in sources])


@router.get("/projects/{project}/datasources/{datasource}")
def get_datasource(project: str, datasource: str) -> Dict:
    p = registry.get_entity(project)
    for s in p.attributes.sources:
        if str(s.id) == datasource:
            return s.to_dict()
    # If datasource is not found, raise 404 error
    raise HTTPException(
        status_code=HTTPStatus.NOT_FOUND, detail=f"Data Source {datasource} not found")


@router.post("/projects/{project}/datasources")
def new_project_datasource(project: str, definition: Dict) -> Dict:
    project_id = registry.get_entity_id(project)
    id = registry.create_project_datasource(project_id, rgm.SourceDef(**rgm.to_snake(definition)))
    return {"guid": str(id)}
