import sqlalchemy as sa
from feathr_registry.registry.models.base import EntityType
from sqlalchemy.ext.declarative import declarative_base

ORMBase = declarative_base()


class Entities(ORMBase):
    """
    entity_id:      default is uuid4, 36 characters
    qualified_name: unknown length, maximum - 255
    entity_type:    enum, see: base.EntityType
    attributes:     unknown length, maximum - 2000
    """
    __tablename__ = 'entities'

    entity_id = sa.Column('entity_id', sa.String(36), nullable=False, primary_key=True)
    qualified_name = sa.Column('qualified_name', sa.String(255), nullable=False)
    entity_type = sa.Column('entity_type', sa.Enum(EntityType), nullable=False)
    attributes = sa.Column('attributes', sa.String(2000), nullable=False)


class Edges(ORMBase):
    """
    edge_id:    default is uuid4,     36 characters
    from_id:    derived from edge_id, 36 characters
    to_id:      derived from edge_id, 36 characters
    conn_type:  unknown length, maximum varchar - 255
    """
    __tablename__ = 'edges'

    edge_id = sa.Column('edge_id', sa.String(36), nullable=False, primary_key=True)
    from_id = sa.Column('from_id', sa.String(36), nullable=False)
    to_id = sa.Column('to_id', sa.String(36), nullable=False)
    conn_type = sa.Column('conn_type', sa.String(255), nullable=False)
