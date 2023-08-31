from typing import Dict, List, Optional
from http import HTTPStatus
from fastapi import APIRouter, HTTPException
from feathr_registry.registry.api.v1 import registry
from feathr_registry.registry.models import base as rgm

router = APIRouter()


@router.get("/projects/{project}/features")
def get_project_features(project: str, keyword: Optional[str] = None, page: Optional[int] = None,
                         limit: Optional[int] = None) -> List:
    if keyword:
        start = None
        size = None
        if page is not None and limit is not None:
            start = (page - 1) * limit
            size = limit
        efs = registry.search_entity(
            keyword, [rgm.EntityType.AnchorFeature, rgm.EntityType.DerivedFeature], project=project, start=start,
            size=size)
        feature_ids = [ef.id for ef in efs]
        features = registry.get_entities(feature_ids)
        return list([e.to_dict() for e in features])
    else:
        p = registry.get_entity(project)
        feature_ids = [s.id for s in p.attributes.anchor_features] + \
                      [s.id for s in p.attributes.derived_features]
        features = registry.get_entities(feature_ids)
        return list([e.to_dict() for e in features])


@router.get("/features/{feature}")
def get_feature(feature: str) -> Dict:
    e = registry.get_entity(feature)
    if e.entity_type not in [rgm.EntityType.DerivedFeature, rgm.EntityType.AnchorFeature]:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND, detail=f"Feature {feature} not found")
    return e.to_dict()


@router.get("/features/{feature}/lineage")
def get_feature_lineage(feature: str) -> Dict:
    lineage = registry.get_lineage(feature)
    return lineage.to_dict()


@router.post("/projects/{project}/anchors")
def new_project_anchor(project: str, definition: Dict) -> Dict:
    project_id = registry.get_entity_id(project)
    id = registry.create_project_anchor(project_id, rgm.AnchorDef(**rgm.to_snake(definition)))
    return {"guid": str(id)}


@router.post("/projects/{project}/anchors/{anchor}/features")
def new_project_anchor_feature(project: str, anchor: str, definition: Dict) -> Dict:
    project_id = registry.get_entity_id(project)
    anchor_id = registry.get_entity_id(anchor)
    id = registry.create_project_anchor_feature(project_id, anchor_id, rgm.AnchorFeatureDef(**rgm.to_snake(definition)))
    return {"guid": str(id)}


@router.post("/projects/{project}/derivedfeatures")
def new_project_derived_feature(project: str, definition: Dict) -> Dict:
    project_id = registry.get_entity_id(project)
    id = registry.create_project_derived_feature(project_id, rgm.DerivedFeatureDef(**rgm.to_snake(definition)))
    return {"guid": str(id)}
