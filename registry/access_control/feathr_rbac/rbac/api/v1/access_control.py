from http import HTTPStatus

from fastapi import APIRouter, Depends
from feathr_rbac.rbac import access
from feathr_rbac.rbac.api.v1 import rbac
from feathr_rbac.rbac.models.base import User, UserAccess
from loguru import logger

router = APIRouter()


@router.get("/userroles", name="List all active user role records [Project Manage Access Required]",
            status_code=HTTPStatus.OK)
def get_userroles(requestor: User = Depends(access.get_user)) -> list:
    """
    :param requestor:
    :return:
    """
    user_roles = rbac.list_userroles(requestor.username)
    logger.debug(f"Found user roles: {user_roles}")
    return user_roles


@router.post("/users/{user}/userroles/add", name="Add a new user role [Project Manage Access Required]",
             status_code=HTTPStatus.OK)
def add_userrole(project: str, user: str, role: str, reason: str,
                 access: UserAccess = Depends(access.project_manage_access)):
    """
    :param project:
    :param user:
    :param role:
    :param reason:
    :param access:
    :return:
    """
    logger.debug(f"Adding role: {role} for {user}. project: {access.project_name}")
    return rbac.add_userrole(access.project_name, user, role, reason, access.user_name)


@router.delete("/users/{user}/userroles/delete", name="Delete a user role [Project Manage Access Required]",
               status_code=HTTPStatus.OK)
def delete_userrole(user: str, role: str, reason: str, access: UserAccess = Depends(access.project_manage_access)):
    """
    :param user:
    :param role:
    :param reason:
    :param access:
    :return:
    """
    logger.debug(f"Deleting role: {role} for {user}. project: {access.project_name}")
    return rbac.delete_userrole(access.project_name, user, role, reason, access.user_name)
