import datetime

import sqlalchemy as sa
from sqlalchemy.ext.declarative import declarative_base

ORMBase = declarative_base()


class UserRole(ORMBase):
    __tablename__ = "userroles"

    record_id = sa.Column(sa.Integer, primary_key=True, autoincrement=True)
    project_name = sa.Column(sa.String(255), nullable=False)
    user_name = sa.Column(sa.String(255), nullable=False)
    role_name = sa.Column(sa.String(255), nullable=False)
    create_by = sa.Column(sa.String(255))
    create_reason = sa.Column(sa.String(255))
    create_time = sa.Column(sa.DateTime, default=datetime.datetime.utcnow)
    delete_by = sa.Column(sa.String(255))
    delete_reason = sa.Column(sa.String(255))
    delete_time = sa.Column(sa.DateTime)
