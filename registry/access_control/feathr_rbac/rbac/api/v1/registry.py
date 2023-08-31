import json
from typing import Optional

import requests
from fastapi import APIRouter, Depends, Response
from feathr_rbac.rbac import access as ac
from feathr_rbac.rbac.api.v1 import rbac, check
from feathr_rbac.rbac.config import RBACConfig
from feathr_rbac.rbac.models.base import User, UserAccess, AccessType

router = APIRouter()
from http import HTTPStatus


@router.get('/projects', name="Get a list of Project Names [No Auth Required]", status_code=HTTPStatus.OK)
async def get_projects(response: Response) -> list[str]:
    response.status_code, res = check(
        requests.get(url=f"{RBACConfig.FEATHR_REGISTRY_URL}/projects"))
    return res


@router.get('/projects/{project}', name="Get My Project [Read Access Required]", status_code=HTTPStatus.OK)
async def get_project(project: str, response: Response, access: UserAccess = Depends(ac.project_read_access)):
    response.status_code, res = check(requests.get(url=f"{RBACConfig.FEATHR_REGISTRY_URL}/projects/{project}",
                                                   headers=ac.get_api_header(access.user_name)))
    return res


@router.get("/dependent/{entity}", name="Get downstream/dependent entitites for a given entity [Read Access Required]",
            status_code=HTTPStatus.OK)
def get_dependent_entities(entity: str, access: UserAccess = Depends(ac.project_read_access)):
    response = requests.get(url=f"{RBACConfig.FEATHR_REGISTRY_URL}/dependent/{entity}",
                            headers=ac.get_api_header(access.user_name)).content.decode('utf-8')
    return json.loads(response)


@router.get("/projects/{project}/datasources", name="Get data sources of my project [Read Access Required]",
            status_code=HTTPStatus.OK)
def get_project_datasources(project: str, response: Response,
                            access: UserAccess = Depends(ac.project_read_access)) -> list:
    response.status_code, res = check(
        requests.get(url=f"{RBACConfig.FEATHR_REGISTRY_URL}/projects/{project}/datasources",
                     headers=ac.get_api_header(access.user_name)))
    return res


@router.get("/projects/{project}/datasources/{datasource}",
            name="Get a single data source by datasource Id [Read Access Required]", status_code=HTTPStatus.OK)
def get_project_datasource(project: str, datasource: str, response: Response,
                           requestor: UserAccess = Depends(ac.project_read_access)) -> list:
    response.status_code, res = check(
        requests.get(url=f"{RBACConfig.FEATHR_REGISTRY_URL}/projects/{project}/datasources/{datasource}",
                     headers=ac.get_api_header(requestor.user_name)))
    return res


@router.get("/projects/{project}/features", name="Get features under my project [Read Access Required]",
            status_code=HTTPStatus.OK)
def get_project_features(project: str, response: Response, keyword: Optional[str] = None,
                         access: UserAccess = Depends(ac.project_read_access)) -> list:
    response.status_code, res = check(requests.get(url=f"{RBACConfig.FEATHR_REGISTRY_URL}/projects/{project}/features",
                                                   headers=ac.get_api_header(access.user_name)))
    return res


@router.get("/features/{feature}", name="Get a single feature by feature Id [Read Access Required]",
            status_code=HTTPStatus.OK)
def get_feature(feature: str, response: Response, requestor: User = Depends(ac.get_user)) -> dict:
    response.status_code, res = check(requests.get(url=f"{RBACConfig.FEATHR_REGISTRY_URL}/features/{feature}",
                                                   headers=ac.get_api_header(requestor.username)))

    feature_qualifiedName = res['attributes']['qualifiedName']
    ac.validate_project_access_for_feature(
        feature_qualifiedName, requestor, AccessType.READ)
    return res


@router.delete("/entity/{entity}", name="Deletes a single entity by qualified name [Write Access Required]",
               status_code=HTTPStatus.OK)
def delete_entity(entity: str, response: Response, access: UserAccess = Depends(ac.project_write_access)) -> str:
    response.status_code, res = check(requests.delete(
        url=f"{RBACConfig.FEATHR_REGISTRY_URL}/entity/{entity}", headers=ac.get_api_header(access.user_name)))
    return res


@router.get("/features/{feature}/lineage", name="Get Feature Lineage [Read Access Required]", status_code=HTTPStatus.OK)
def get_feature_lineage(feature: str, response: Response, requestor: User = Depends(ac.get_user)) -> dict:
    response.status_code, res = check(requests.get(url=f"{RBACConfig.FEATHR_REGISTRY_URL}/features/{feature}/lineage",
                                                   headers=ac.get_api_header(requestor.username)))

    feature_qualifiedName = res['guidEntityMap'][feature]['attributes']['qualifiedName']
    ac.validate_project_access_for_feature(
        feature_qualifiedName, requestor, AccessType.READ)
    return res


@router.post("/projects", name="Create new project with definition [Auth Required]", status_code=HTTPStatus.OK)
def new_project(definition: dict, response: Response, requestor: User = Depends(ac.get_user)) -> dict:
    rbac.init_userrole(requestor.username, definition["name"])
    response.status_code, res = check(requests.post(url=f"{RBACConfig.FEATHR_REGISTRY_URL}/projects", json=definition,
                                                    headers=ac.get_api_header(requestor.username)))
    return res


@router.post("/projects/{project}/datasources", name="Create new data source of my project [Write Access Required]",
             status_code=HTTPStatus.OK)
def new_project_datasource(project: str, definition: dict, response: Response,
                           access: UserAccess = Depends(ac.project_write_access)) -> dict:
    response.status_code, res = check(
        requests.post(url=f"{RBACConfig.FEATHR_REGISTRY_URL}/projects/{project}/datasources", json=definition,
                      headers=ac.get_api_header(
                          access.user_name)))
    return res


@router.post("/projects/{project}/anchors", name="Create new anchors of my project [Write Access Required]",
             status_code=HTTPStatus.OK)
def new_project_anchor(project: str, definition: dict, response: Response,
                       access: UserAccess = Depends(ac.project_write_access)) -> dict:
    response.status_code, res = check(
        requests.post(url=f"{RBACConfig.FEATHR_REGISTRY_URL}/projects/{project}/anchors", json=definition,
                      headers=ac.get_api_header(
                          access.user_name)))
    return res


@router.post("/projects/{project}/anchors/{anchor}/features",
             name="Create new anchor features of my project [Write Access Required]", status_code=HTTPStatus.OK)
def new_project_anchor_feature(project: str, anchor: str, definition: dict, response: Response,
                               access: UserAccess = Depends(ac.project_write_access)) -> dict:
    response.status_code, res = check(
        requests.post(url=f"{RBACConfig.FEATHR_REGISTRY_URL}/projects/{project}/anchors/{anchor}/features",
                      json=definition,
                      headers=ac.get_api_header(
                          access.user_name)))
    return res


@router.post("/projects/{project}/derivedfeatures",
             name="Create new derived features of my project [Write Access Required]", status_code=HTTPStatus.OK)
def new_project_derived_feature(project: str, definition: dict, response: Response,
                                access: UserAccess = Depends(ac.project_write_access)) -> dict:
    response.status_code, res = check(
        requests.post(url=f"{RBACConfig.FEATHR_REGISTRY_URL}/projects/{project}/derivedfeatures",
                      json=definition, headers=ac.get_api_header(access.user_name)))
    return res
