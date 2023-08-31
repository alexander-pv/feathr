from http import HTTPStatus
from typing import List

from fastapi import APIRouter, HTTPException
from feathr_registry.registry.api.v1 import registry

router = APIRouter()


@router.get("/dependent/{entity}")
def get_dependent_entities(entity: str) -> List:
    entity_id = registry.get_entity_id(entity)
    downstream_entities = registry.get_dependent_entities(entity_id)
    return list([e.to_dict() for e in downstream_entities])


@router.delete("/entity/{entity}")
def delete_entity(entity: str):
    entity_id = registry.get_entity_id(entity)
    downstream_entities = registry.get_dependent_entities(entity_id)
    if len(downstream_entities) > 0:
        registry.delete_empty_entities(downstream_entities)
        if len(registry.get_dependent_entities(entity_id)) > 0:
            raise HTTPException(
                status_code=HTTPStatus.PRECONDITION_FAILED,
                detail=f"""Entity cannot be deleted as it has downstream/dependent entities.
                Entities: {list([e.qualified_name for e in downstream_entities])}"""
            )
    registry.delete_entity(entity_id)
