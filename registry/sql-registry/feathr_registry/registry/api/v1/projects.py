from typing import Dict, List

from fastapi import APIRouter
from feathr_registry.registry.api.v1 import registry
from feathr_registry.registry.models import base as rgm

router = APIRouter()


@router.get("/projects")
def get_projects() -> List[str]:
    return registry.get_projects()


@router.get("/projects-ids")
def get_projects_ids() -> Dict:
    return registry.get_projects_ids()


@router.get("/projects/{project}")
def get_projects(project: str) -> Dict:
    return registry.get_project(project).to_dict()


@router.post("/projects")
def new_project(definition: Dict) -> Dict:
    id = registry.create_project(rgm.ProjectDef(**rgm.to_snake(definition)))
    return {"guid": str(id)}
