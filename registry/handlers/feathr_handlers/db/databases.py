import os
# Checks if the platform is Max (Darwin).
# If so, imports _scproxy that is necessary for pymssql to work on MacOS
import platform
import threading
from abc import ABC, ABCMeta, abstractmethod
from contextlib import contextmanager
from typing import List, Dict, Any, Optional

import randomname
from loguru import logger

if platform.system().lower().startswith('dar'):
    pass

from sqlalchemy import orm
import sqlalchemy as sa
from sqlalchemy.engine.reflection import Inspector
import sqlite3
import pymssql
from .utils import retry


class DbConnection(ABC):

    def __init__(self, conn_string: str):
        """
        :param conn_string: SQL database connection string
        """
        self.conn_string = conn_string
        self.retries = int(os.getenv("DB_CONN_RETRIES", 3))

    @abstractmethod
    def query(self, sql: str, *args, **kwargs) -> List[Dict]:
        pass

    @abstractmethod
    def parse_conn_str(self, s: str) -> Any:
        pass

    @abstractmethod
    def make_connection(self) -> Any:
        pass

    @abstractmethod
    def is_table_in_db(self, name: str) -> bool:
        pass

    @property
    def is_sqlalchemy_supported(self) -> bool:
        return False


class SQLAlchemyConnection(DbConnection, metaclass=ABCMeta):
    def __init__(self, conn_string: str):
        """
        :param conn_string:
        """
        super().__init__(conn_string=conn_string)
        self.engine = None
        self.session = None
        self.make_connection()
        self.mutex = threading.Lock()

    @property
    def is_sqlalchemy_supported(self) -> bool:
        return True

    def is_table_in_db(self, name: str) -> bool:
        """
        :param name:
        :return:
        """
        if self.engine is None:
            self.make_connection()
        inspector = Inspector.from_engine(self.engine)
        return name in set(inspector.get_table_names())

    def parse_conn_str(self, s: str) -> Dict[str, Any]:
        """
        :param s:
        :return:
        """
        return {"url": s}

    @retry(max_attempts=3, delay_secs=1)
    def query(self, sql: str, *args, **kwargs) -> List[Dict]:
        """
        Make SQL query and return result
        """
        logger.debug(f"SQL: `{sql}`")
        with self.mutex:
            with self.engine.connect() as connection:
                statement = sa.text(sql)
                result = connection.execute(statement, *args, **kwargs)
                return [dict(row) for row in result]

    @retry(max_attempts=3, delay_secs=1)
    def update(self, sql: str, *args, **kwargs):
        """
        Update data in the database using SQL statement within a transaction.
        :param sql:
        :param args:
        :param kwargs:
        :return:
        """
        logger.debug(f"SQL: `{sql}`")
        with self.mutex:
            with self.transaction() as sess:
                statement = sa.text(sql)
                sess.execute(statement, *args, **kwargs)
            return True

    @contextmanager
    def transaction(self):
        """
        Start a transaction so we can run multiple SQL in one batch.
        """
        session = self.session()
        try:
            yield session
            session.commit()
        except Exception as e:
            logger.warning(f"Exception: {e}")
            session.rollback()
            raise e
        finally:
            session.close()


class SQLAlchemyServerDBConnection(SQLAlchemyConnection):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def make_connection(self) -> None:
        """
        :return:
        """
        try:
            conn_params = self.parse_conn_str(self.conn_string)
            self.engine = sa.create_engine(**conn_params)
            self.session = orm.sessionmaker(bind=self.engine)
        except Exception as e:
            logger.error(f"Error connecting to the database: {e}")
            raise e


class SQLiteConnection(SQLAlchemyConnection):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def make_connection(self) -> None:
        """
        # use ` check_same_thread=False` otherwise an error like
        # sqlite3.ProgrammingError: SQLite objects created in a thread can only be used in that same thread.
        # The object was created in thread id 140309046605632 and this is thread id 140308968896064 will be thrown out
        # Use the mem just to make sure it can connect. The actual file path will be initialized in the db_registry.py file
        :return:
        """
        try:
            # Keep this test here along with general connection. TODO: Remove it asap
            sqlite3.connect("file::memory:?cache=shared", uri=True, check_same_thread=False)
            conn_params = self.parse_conn_str(self.conn_string)
            self.engine = sa.create_engine(**conn_params)
            self.session = orm.sessionmaker(self.engine)
        except Exception as e:
            logger.error(f"Error connecting to the database: {e}")
            raise e


class PgsqlConnection(SQLAlchemyServerDBConnection):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)


class MySQLConnection(SQLAlchemyServerDBConnection):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)


class MariaDBConnection(SQLAlchemyServerDBConnection):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)


class MssqlConnectionV2(SQLAlchemyServerDBConnection):
    """
    TODO: May be used as the main mssql connection instead of MssqlConnection after appropriate tests.
    # sqlalchemy documentation details:
    # https://docs.sqlalchemy.org/en/14/dialects/index.html
    # https://docs.sqlalchemy.org/en/14/core/connections.html#dbapi-autocommit
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)


class MssqlConnection(DbConnection):
    def __init__(self, conn_string: str, autocommit: bool = True):

        super().__init__(conn_string=conn_string)
        self.autocommit = autocommit
        self.conn = self.make_connection()
        self.mutex = threading.Lock()

    def is_table_in_db(self, name: str) -> bool:
        query = f"IF OBJECT_ID('{name}', 'U') IS NOT NULL SELECT 1 ELSE SELECT 0"
        cursor = self.conn.cursor()
        cursor.execute(query)
        exists = cursor.fetchone()[0]
        return bool(exists)

    def parse_conn_str(self, s: str) -> Any:
        """
        TODO: Not a sound and safe implementation, but useful enough in this case
        as the connection string is provided by users themselves.
        :param s:
        :return:
        """

        parts = dict([p.strip().split("=", 1)
                      for p in s.split(";") if len(p.strip()) > 0])
        server = parts["Server"].split(":")[1].split(",")[0]
        return {
            "host": server,
            "database": parts["Initial Catalog"],
            "user": parts["User ID"],
            "password": parts["Password"],
            "autocommit": self.autocommit
            # "charset": "utf-8",   ## For unknown reason this causes connection failure
        }

    def make_connection(self) -> Any:
        self.conn = pymssql.connect(**self.parse_conn_str(self.conn_string))
        return self.conn

    @retry(max_attempts=3, delay_secs=1)
    def query(self, sql: str, *args, **kwargs) -> List[Dict]:
        """
        Make SQL query and return result
        """
        logger.debug(f"SQL: `{sql}`")
        # NOTE: Only one cursor is allowed at the same time
        self.make_connection()
        with self.mutex:
            c = self.conn.cursor(as_dict=True)
            c.execute(sql, *args, **kwargs)
            return c.fetchall()

    @retry(max_attempts=3, delay_secs=1)
    def update(self, sql: str, *args, **kwargs):
        """
        Update data in the database using SQL statement within a transaction.
        :param sql:
        :param args:
        :param kwargs:
        :return:
        """
        logger.debug(f"SQL: `{sql}`")
        self.make_connection()
        with self.mutex:
            c = self.conn.cursor(as_dict=True)
            c.execute(sql, *args, **kwargs)
            self.conn.commit()
            return True

    @contextmanager
    def transaction(self):
        """
        Start a transaction so we can run multiple SQL in one batch.
        User should use `with` with the returned value, look into db_registry.py for more real usage.

        NOTE: `self.query` and `self.execute` will use a different MSSQL connection so any change made
        in this transaction will *not* be visible in these calls.

        The minimal implementation could look like this if the underlying engine doesn't support transaction.
        ```
        @contextmanager
        def transaction(self):
            try:
                c = self.create_or_get_connection(...)
                yield c
            finally:
                c.close(...)
        ```
        """
        conn = None
        cursor = None
        try:
            # As one MssqlConnection has only one connection, we need to create a new one to disable `autocommit`
            conn_params = self.parse_conn_str(self.conn_string)
            conn_params["autocommit"] = False
            conn = self.make_connection()
            cursor = conn.cursor(as_dict=True)
            yield cursor
        except Exception as e:
            logger.warning(f"Exception: {e}")
            if conn:
                conn.rollback()
            raise e
        finally:
            if conn:
                conn.commit()


PROVIDERS = {
    "sqlite": SQLiteConnection,
    "pgsql": PgsqlConnection,
    "mysql": MySQLConnection,
    "mariadb": MariaDBConnection,
    "mssql": MssqlConnection,
    "mssqlv2": MssqlConnectionV2
}


def get_db_provider(db_name: Optional[str], conn_str: Optional[str]) -> DbConnection:
    if db_name not in PROVIDERS.keys():
        err_msg = f"Cannot connect to {db_name} database. Supported: {list(PROVIDERS.keys())}"
        logger.error(err_msg)
        raise RuntimeError(err_msg)

    if conn_str is None or conn_str == "":
        db_name = "sqlite"
        conn_str = f"sqlite:////tmp/{randomname.get_name()}.sqlite?check_same_thread=False"
        msg = f"Connection string is not set.\n Selecting {db_name} as default. Connection: {conn_str}"
        logger.info(msg)

    db_provider = PROVIDERS.get(db_name)
    return db_provider(conn_string=conn_str)
