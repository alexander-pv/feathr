import datetime
from typing import Any

from fastapi import HTTPException, status
from feathr_handlers.db.databases import get_db_provider
from feathr_rbac.rbac.config import RBACConfig
from feathr_rbac.rbac.interface import RBAC
from feathr_rbac.rbac.models import base as rgm
from feathr_rbac.rbac.models import orm
from loguru import logger


class BadRequest(HTTPException):
    def __init__(self, detail: Any = None) -> None:
        super().__init__(status_code=status.HTTP_400_BAD_REQUEST,
                         detail=detail, headers={"WWW-Authenticate": "Bearer"})


class DbRBAC(RBAC):
    def __init__(self):
        self.db_conn = get_db_provider(
            db_name=RBACConfig.RBAC_DATABASE,
            conn_str=RBACConfig.RBAC_CONNECTION_STR
        )
        self.projects_ids = {}
        if self.db_conn.is_sqlalchemy_supported:
            self.sql_session = self.db_conn.session()
            self.connection = self.db_conn.engine.connect()
            self._create_tables()
            self.get_userroles()
            self._init_admin()

        logger.info("Initialized DbRBAC")

    def _create_tables(self) -> None:
        """
        Create necessary tables from models.orm.
        If the tables with the same names already exist in the database,
        SQLAlchemy will not re-create them, and it will not make any changes to the existing tables.
        :return: None
        """
        orm.ORMBase.metadata.create_all(self.db_conn.engine)

    def _init_admin(self) -> None:
        """
        Initialize default admin role
        :return: None
        """
        user_names = set([user.user_name for user in self.userroles])
        default_admin = RBACConfig.RBAC_DEFAULT_ADMIN
        if default_admin not in user_names:
            new_user_role = orm.UserRole(
                project_name="global",
                user_name=RBACConfig.RBAC_DEFAULT_ADMIN,
                role_name=rgm.RoleType.ADMIN.value,
                create_by="system",
                create_reason="Initialize First Global Admin",
                delete_by=None,
                delete_reason=None,
                delete_time=None
            )
            session = self.db_conn.session()
            session.add(new_user_role)
            session.commit()
            logger.debug(f"Default admin user: {default_admin} was not found. The new one was successfully created.")

    def get_userroles(self) -> None:
        """
        Cache is not supported in cluster, make sure every operation read from database
        :return: None
        """
        self.userroles = self._get_userroles()
        logger.debug(f"Current userroles: {self.userroles}")

    def _get_userroles(self) -> list[rgm.UserRole]:
        """
        Query all the active user role records in SQL table
        :return: list[rgm.UserRole]
        """
        rows = self.db_conn.query(
            fr"""select record_id, project_name, user_name, role_name, create_by, create_reason, create_time, delete_by, delete_reason, delete_time
            from userroles
            where delete_reason is null""")
        ret = []
        for row in rows:
            ret.append(rgm.UserRole(**row))
        logger.info(f"{len(ret)} user roles were fetched")
        return ret

    def get_global_admin_users(self) -> list[str]:
        """
        :return: list[str]
        """
        self.get_userroles()
        return [u.user_name for u in self.userroles if
                (u.project_name == rgm.SUPER_ADMIN_SCOPE and u.role_name == rgm.RoleType.ADMIN.value)]

    def validate_project_access_users(self, project: str, user: str, access: str = rgm.AccessType.READ) -> bool:
        """
        :param project:
        :param user:
        :param access:
        :return: bool
        """
        self.get_userroles()
        for u in self.userroles:
            if (u.user_name == user.lower() and u.project_name in [project.lower(), rgm.SUPER_ADMIN_SCOPE] and (
                    access in u.access)):
                return True
        return False

    def get_userroles_by_user(self, user_name: str, role_name: str = None) -> list[rgm.UserRole]:
        """
        Query the active user role of certain user
        :param user_name:
        :param role_name:
        :return:
        """
        query = fr"""select record_id, project_name, user_name, role_name, create_by, create_reason, create_time, delete_by, delete_reason, delete_time
            from userroles
            where delete_reason is null and user_name ='%s'"""
        if role_name:
            query += fr"and role_name = '%s'"
            rows = self.db_conn.query(query % (user_name.lower(), role_name.lower()))
        else:
            rows = self.db_conn.query(query % (user_name.lower()))
        ret = []
        for row in rows:
            ret.append(rgm.UserRole(**row))
        return ret

    def get_userroles_by_project(self, project_name: str, role_name: str = None) -> list[rgm.UserRole]:
        """
        Query the active user role of certain project
        :param project_name:
        :param role_name:
        :return:
        """
        query = fr"""select record_id, project_name, user_name, role_name, create_by, create_reason, create_time, delete_reason, delete_time
            from userroles
            where delete_reason is null and project_name ='%s'"""
        if role_name:
            query += fr"and role_name = '%s'"
            rows = self.db_conn.query(query % (project_name.lower(), role_name.lower()))
        else:
            rows = self.db_conn.query(query % (project_name.lower()))
        ret = []
        for row in rows:
            ret.append(rgm.UserRole(**row))
        return ret

    def list_userroles(self, user_name: str) -> list[rgm.UserRole]:
        """
        :param user_name:
        :return:
        """
        ret = []
        if user_name in self.get_global_admin_users():
            return list([r.to_dict() for r in self.userroles])
        else:
            admin_roles = self.get_userroles_by_user(
                user_name, rgm.RoleType.ADMIN.value)
            ret = []
            for r in admin_roles:
                ret.extend(self.get_userroles_by_project(r.project_name))
        return list([r.to_dict() for r in ret])

    def add_userrole(self, project_name: str, user_name: str, role_name: str, create_reason: str, by: str):
        """
        insert new user role relationship into sql table
        :param project_name:
        :param user_name:
        :param role_name:
        :param create_reason:
        :param by:
        :return:
        """
        # check if record already exist
        self.get_userroles()
        for u in self.userroles:
            if u.project_name == project_name.lower() and u.user_name == user_name.lower() and u.role_name == role_name:
                logger.warning(
                    f"User {user_name} already have {role_name} role of {project_name}.")
                return True

        # insert new record
        query = fr"""insert into userroles (project_name, user_name, role_name, create_by, create_reason, create_time)
            values ('%s','%s','%s','%s' ,'%s', '%s')"""
        utc = str(datetime.datetime.utcnow())
        prepared_query = query % (project_name.lower(), user_name.lower(),
                                  role_name.lower(), by, create_reason.replace("'", "''"), utc)
        self.db_conn.update(prepared_query)
        logger.info(f"Userrole added with query:\n{prepared_query}")
        self.get_userroles()
        return

    def delete_userrole(self, project_name: str, user_name: str, role_name: str, delete_reason: str, by: str):
        """
        Mark existing user role relationship as deleted with reason
        :param project_name:
        :param user_name:
        :param role_name:
        :param delete_reason:
        :param by:
        :return:
        """
        query = fr"""UPDATE userroles SET
            [delete_by] = '%s',
            [delete_reason] = '%s',
            [delete_time] = '%s'
            WHERE [user_name] = '%s' and [project_name] = '%s' and [role_name] = '%s'
            and [delete_time] is null"""
        utc = str(datetime.datetime.utcnow())
        prepared_query = query % (by, delete_reason.replace("'", "''"), utc,
                                  user_name.lower(), project_name.lower(), role_name.lower())
        self.db_conn.update(prepared_query)
        logger.info(f"Userrole removed with query:\n{prepared_query}")
        self.get_userroles()
        return

    def init_userrole(self, creator_name: str, project_name: str):
        """
        Project name validation and project admin initialization
        :param creator_name:
        :param project_name:
        :return:
        """
        # project name cannot be `global`
        if project_name.casefold() == rgm.SUPER_ADMIN_SCOPE.casefold():
            raise BadRequest(
                f"{rgm.SUPER_ADMIN_SCOPE} is keyword for Global Admin (admin of all projects), please try other project name.")
        else:
            # check if project already exist (have valid db_rbac records)
            # no 400 exception to align the registry api behaviors
            query = fr"""select project_name, user_name, role_name, create_by, create_reason, create_time, delete_reason, delete_time
                from userroles
                where delete_reason is null and project_name ='%s'"""
            rows = self.db_conn.query(query % (project_name.lower()))
            if len(rows) > 0:
                logger.warning(f"{project_name} already exist, please pick another name.")
                return
            else:
                # initialize project admin if project not exist: 
                self.init_project_admin(creator_name, project_name)

    def init_project_admin(self, creator_name: str, project_name: str) -> None:
        """
        initialize the creator as project admin when a new project is created
        :param creator_name:
        :param project_name:
        :return: None
        """
        create_by = "system"
        create_reason = "creator of project, get admin by default."
        utc = str(datetime.datetime.utcnow())
        query = fr"""insert into userroles (project_name, user_name, role_name, create_by, create_reason, create_time)
            values ('%s','%s','%s','%s','%s','%s')"""
        prepared_query = query % (
            project_name.lower(), creator_name.lower(), rgm.RoleType.ADMIN.value, create_by, create_reason, utc)
        self.db_conn.update(prepared_query)
        logger.info(f"User role initialized with query:\n{prepared_query}")
        self.get_userroles()
