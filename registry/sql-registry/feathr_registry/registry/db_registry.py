import json
from typing import List, Set, Dict
from typing import Optional, Tuple, Union
from uuid import UUID, uuid4

from feathr_handlers.db.databases import get_db_provider
from feathr_registry.registry.interface import Registry
from feathr_registry.registry.models import base as rgm
from feathr_registry.registry.models import orm
from loguru import logger


class ConflictError(Exception):
    pass


from sqlalchemy.orm.query import Query
from sqlalchemy import and_
import sqlalchemy as sa


def quote(id):
    if isinstance(id, str):
        return f"'{id}'"
    if isinstance(id, UUID):
        return f"'{str(id)}'"
    else:
        return ",".join([quote(i) for i in id])


class DbRegistry(Registry):
    def __init__(self, db_name: str, conn_str: str):
        self.db_conn = get_db_provider(db_name=db_name, conn_str=conn_str)
        if self.db_conn.is_sqlalchemy_supported:
            self.sql_session = self.db_conn.session()
            self.connection = self.db_conn.engine.connect()
            self._create_tables()
            self._init_project()
        logger.info("Initialized DbRegistry")

    def _create_tables(self) -> None:
        """
        Create necessary tables.
        If the tables with the same names already exist in the database,
        SQLAlchemy will not re-create them, and it will not make any changes to the existing tables.
        :return: None
        """
        orm.ORMBase.metadata.create_all(self.db_conn.engine)

    def _init_project(self) -> None:
        self.create_project(rgm.ProjectDef(name="global", qualified_name="global"))

    def _fetch_helper(self, query: Union[Query, str]):
        """serves as a function to have max code similarity between the ORM based code and the SQL based code.
        Basically fetch all and return a dict (otherwise it might just return a list of `LegacyRow` object)
        """
        logger.debug(f"_fetch_helper query:\n{query}")
        if isinstance(query, Query):
            # if this is already a query object, execute it
            r = query.all()
        else:
            # otherwise need a session to execute it
            r = self.connection.execute(query).fetchall()
        r = [ele._asdict() for ele in r]
        return r

    def get_projects(self) -> List[str]:
        if self.db_conn.is_sqlalchemy_supported:
            query = (sa.select(
                orm.Entities.qualified_name)
            .where(
                (orm.Entities.entity_type == rgm.EntityType.Project
                 )
            )
            )
            ret = self._fetch_helper(query)
        else:
            ret = self.db_conn.query(
                f"select qualified_name from entities where entity_type=%s", rgm.EntityType.Project)
        return list([r["qualified_name"] for r in ret])

    def get_projects_ids(self) -> Dict:
        projects = {}
        if self.db_conn.is_sqlalchemy_supported:
            query = sa.select(
                orm.Entities.entity_id,
                orm.Entities.qualified_name
            ).where(
                (orm.Entities.entity_type == rgm.EntityType.Project
                 )
            )
            ret = self._fetch_helper(query)
        else:
            ret = self.db_conn.query(
                f"select entity_id, qualified_name from entities where entity_type=%s", rgm.EntityType.Project)
        for r in ret:
            projects[r['entity_id']] = r['qualified_name']
        return projects

    def get_entity(self, id_or_name: Union[str, UUID]) -> rgm.Entity:
        return self._fill_entity(self._get_entity(id_or_name))

    def get_entities(self, ids: List[UUID]) -> List[rgm.Entity]:
        return list([self._fill_entity(e) for e in self._get_entities(ids)])

    def get_entity_id(self, id_or_name: Union[str, UUID]) -> UUID:
        try:
            id = rgm._to_uuid(id_or_name)
            return id
        except ValueError:
            pass
        # It is a name
        if self.db_conn.is_sqlalchemy_supported:
            query = sa.select(
                orm.Entities.entity_id
            ).where(
                (orm.Entities.qualified_name == str(id_or_name)
                 )
            )
            ret = self._fetch_helper(query)
        else:
            ret = self.db_conn.query(
                f"select entity_id from entities where qualified_name=%s", str(id_or_name))
        if len(ret) == 0:
            raise KeyError(f"Entity {id_or_name} not found")
        return ret[0]["entity_id"]

    def get_neighbors(self, id_or_name: Union[str, UUID], relationship: rgm.RelationshipType) -> List[rgm.Edge]:
        if self.db_conn.is_sqlalchemy_supported:
            query = sa.select(
                orm.Edges.edge_id,
                orm.Edges.from_id,
                orm.Edges.to_id,
                orm.Edges.conn_type
            ).where(
                (orm.Edges.from_id == str(self.get_entity_id(id_or_name))) &
                (orm.Edges.conn_type == relationship.name)
            )
            rows = self._fetch_helper(query)
        else:
            rows = self.db_conn.query(fr'''
            select edge_id, from_id, to_id, conn_type
            from edges
            where from_id = %s
            and conn_type = %s
        ''', (str(self.get_entity_id(id_or_name)), relationship.name))
        return list([rgm.Edge(**row) for row in rows])

    def get_lineage(self, id_or_name: Union[str, UUID]) -> rgm.EntitiesAndRelations:
        """
        Get feature lineage on both upstream and downstream
        Returns [entity_id:entity] map and list of edges have been traversed.
        """
        id = self.get_entity_id(id_or_name)
        upstream_entities, upstream_edges = self._bfs(
            id, rgm.RelationshipType.Consumes)
        downstream_entities, downstream_edges = self._bfs(
            id, rgm.RelationshipType.Produces)
        return rgm.EntitiesAndRelations(
            upstream_entities + downstream_entities,
            upstream_edges + downstream_edges)

    def get_project(self, id_or_name: Union[str, UUID]) -> rgm.EntitiesAndRelations:
        """
        This function returns not only the project itself, but also everything in the project
        """
        project = self._get_entity(id_or_name)
        edges = set(self.get_neighbors(id_or_name, rgm.RelationshipType.Contains))
        ids = list([e.to_id for e in edges])
        children = self._get_entities(ids)
        child_map = dict([(e.id, e) for e in children])
        project.attributes.children = children
        for anchor in project.attributes.anchors:
            conn = self.get_neighbors(anchor.id, rgm.RelationshipType.Contains)
            feature_ids = [e.to_id for e in conn]
            edges = edges.union(conn)
            features = list([child_map[id] for id in feature_ids])
            anchor.attributes.features = features
            source_id = self.get_neighbors(
                anchor.id, rgm.RelationshipType.Consumes)[0].to_id
            anchor.attributes.source = child_map[source_id]
        for df in project.attributes.derived_features:
            conn = self.get_neighbors(df.id, rgm.RelationshipType.Consumes)
            input_ids = [e.to_id for e in conn]
            edges = edges.union(conn)
            features = list([child_map[id] for id in input_ids])
            df.attributes.input_features = features
        all_edges = self._get_edges(ids)
        return rgm.EntitiesAndRelations([project] + children, list(edges.union(all_edges)))

    def get_dependent_entities(self, entity_id: Union[str, UUID]) -> List[rgm.Entity]:
        """
        Given entity id, returns list of all entities that are downstream/dependent on the given entity
        """
        entity_id = self.get_entity_id(entity_id)
        entity = self.get_entity(entity_id)
        downstream_entities = []
        if entity.entity_type == rgm.EntityType.Project:
            downstream_entities, _ = self._bfs(entity_id, rgm.RelationshipType.Contains)
        if entity.entity_type == rgm.EntityType.Source:
            downstream_entities, _ = self._bfs(entity_id, rgm.RelationshipType.Produces)
        if entity.entity_type == rgm.EntityType.Anchor:
            downstream_entities, _ = self._bfs(entity_id, rgm.RelationshipType.Contains)
        if entity.entity_type in (rgm.EntityType.AnchorFeature, rgm.EntityType.DerivedFeature):
            downstream_entities, _ = self._bfs(entity_id, rgm.RelationshipType.Produces)
        return [e for e in downstream_entities if str(e.id) != str(entity_id)]

    def delete_empty_entities(self, entities: List[rgm.Entity]):
        """
        Given entity list, deleting all anchors that have no features and all sources that have no anchors.
        """
        if len(entities) == 0:
            return

        # clean up empty anchors
        for e in entities:
            if e.entity_type == rgm.EntityType.Anchor:
                downstream_entities, _ = self._bfs(e.id, rgm.RelationshipType.Contains)
                if len(downstream_entities) == 1:  # only anchor itself
                    self.delete_entity(e.id)
        # clean up empty sources
        for e in entities:
            if e.entity_type == rgm.EntityType.Source:
                downstream_entities, _ = self._bfs(e.id, rgm.RelationshipType.Produces)
                if len(downstream_entities) == 1:  # only source itself
                    self.delete_entity(e.id)

        return

    def delete_entity(self, entity_id: Union[str, UUID]):
        """
        Deletes given entity
        """
        entity_id = self.get_entity_id(entity_id)
        with self.db_conn.transaction() as c:
            self._delete_all_entity_edges(c, entity_id)
            self._delete_entity(c, entity_id)

    def search_entity(self,
                      keyword: str,
                      type: List[rgm.EntityType],
                      project: Optional[Union[str, UUID]] = None,
                      start: Optional[int] = None,
                      size: Optional[int] = None) -> List[rgm.EntityRef]:
        """
        WARN: This search function is implemented via `like` operator, which could be extremely slow.
        """

        top_clause = ""
        if start is not None and size is not None:
            top_clause = f"TOP({int(start) + int(size)})"

        if project:
            project_id = self.get_entity_id(project)
            if self.db_conn.is_sqlalchemy_supported:
                query = self.sql_session.query(
                    orm.Entities.entity_id.label("id"),
                    orm.Entities.qualified_name,
                    orm.Entities.entity_type.label("type")
                ).join(orm.Edges,
                       and_(orm.Entitiesentity_id == orm.Edges.from_id,
                            orm.Edges.conn_type == 'BelongsTo')
                       ).filter(
                    orm.Edges.to_id == str(project_id),
                    orm.Entities.qualified_name.ilike("%" + keyword + "%"),
                    orm.Entities.entity_type.in_(tuple([str(t) for t in type]))).order_by(
                    orm.Entities.qualified_name).slice(
                    int(start), int(start + size))
                rows = self._fetch_helper(query)
            else:
                sql = fr'''select {top_clause} entity_id as id, qualified_name, entity_type as type
                    from entities
                    inner join edges on entity_id=edges.from_id and edges.conn_type='BelongsTo'
                    where
                    edges.to_id=%s and qualified_name like %s and entity_type in %s
                    order by qualified_name'''
                rows = self.db_conn.query(sql, (str(project_id), '%' + keyword + '%', tuple([str(t) for t in type])))

        else:
            if self.db_conn.is_sqlalchemy_supported:
                query = self.sql_session.query(orm.Entities.entity_id.label("id"), orm.Entitiesqualified_name,
                                               orm.Entities.entity_type.label("type")).filter(
                    orm.Entities.qualified_name.ilike("%" + keyword + "%"),
                    orm.Entities.entity_type.in_(tuple([t for t in type]))).order_by(
                    orm.Entities.qualified_name).slice(
                    int(start), int(start + size))
                rows = self._fetch_helper(query)
            else:
                sql = fr'''select {top_clause} entity_id as id, qualified_name, entity_type as type from entities where qualified_name like %s and entity_type in %s order by qualified_name'''
                rows = self.db_conn.query(sql, ('%' + keyword + '%', tuple([str(t) for t in type])))
        if size:
            rows = rows[-size:]
        return list([rgm.EntityRef(**row) for row in rows])

    def create_project(self, definition: rgm.ProjectDef) -> UUID:
        # Here we start a transaction, any following step failed, everything rolls back
        definition.qualified_name = definition.name
        logger.debug(f"Trying to find project: [{definition.name}] before creation")
        with self.db_conn.transaction() as c:
            # First we try to find existing entity with the same qualified name
            if self.db_conn.is_sqlalchemy_supported:
                query = sa.select(
                    orm.Entities.entity_id,
                    orm.Entities.entity_type,
                    orm.Entities.attributes
                ).where(
                    (orm.Entities.qualified_name == definition.qualified_name))
                r = self._fetch_helper(query)
            else:
                c.execute(f'''select entity_id, entity_type, attributes from entities where qualified_name = %s''',
                          definition.qualified_name)
                r = c.fetchall()
            if r:
                if len(r) > 1:
                    assert False, "Data inconsistency detected, %d entities have same qualified_name %s" % (
                        len(r), definition.qualified_name)
                # The entity with same name already exists but with different type
                if rgm._to_type(r[0]["entity_type"], rgm.EntityType) != rgm.EntityType.Project:
                    raise ConflictError("Entity %s already exists" %
                                        definition.qualified_name)
                # Just return the existing project id
                logger.debug(f"Found project: [{definition.name}]. No need to create it again")
                return rgm._to_uuid(r[0]["entity_id"])

            entity_id = str(uuid4())
            logger.debug(f"Creating a new project with entity_id: {entity_id}")
            if self.db_conn.is_sqlalchemy_supported:
                query = sa.insert(orm.Entities).values(entity_id=entity_id,
                                                       entity_type=rgm.EntityType.Project,
                                                       qualified_name=definition.qualified_name,
                                                       attributes=definition.to_attr().to_json())
                r = self.connection.execute(query)
            else:
                c.execute(
                    f"insert into entities (entity_id, entity_type, qualified_name, attributes) values (%s, %s, %s, %s)",
                    (entity_id,
                     rgm.EntityType.Project,
                     definition.qualified_name,
                     definition.to_attr().to_json()))
            logger.debug(f"Created a new project with entity_id: {entity_id}")
            return entity_id

    def create_project_datasource(self, project_id: UUID, definition: rgm.SourceDef) -> UUID:
        project = self.get_entity(project_id)
        definition.qualified_name = f"{project.qualified_name}__{definition.name}"
        # Here we start a transaction, any following step failed, everything rolls back
        with self.db_conn.transaction() as c:
            # First we try to find existing entity with the same qualified name
            if self.db_conn.is_sqlalchemy_supported:
                query = sa.select(orm.Entities.entity_id, orm.Entities.entity_type,
                                  orm.Entities.attributes).where(
                    (orm.Entities.qualified_name == definition.qualified_name))
                r = self._fetch_helper(query)
            else:
                c.execute(f'''select entity_id, entity_type, attributes from entities where qualified_name = %s''',
                          definition.qualified_name)
                r = c.fetchall()
            if r:
                if len(r) > 1:
                    # There are multiple entities with same qualified name， that means we already have errors in the sa
                    assert False, "Data inconsistency detected, %d entities have same qualified_name %s" % (
                        len(r), definition.qualified_name)
                # The entity with same name already exists but with different type
                if rgm._to_type(r[0]["entity_type"], rgm.EntityType) != rgm.EntityType.Source:
                    raise ConflictError("Entity %s already exists" %
                                        definition.qualified_name)
                attr: rgm.SourceAttributes = rgm._to_type(
                    json.loads(r[0]["attributes"]), rgm.SourceAttributes)
                if attr.name == definition.name \
                        and attr.type == definition.type \
                        and attr.options == definition.options \
                        and attr.preprocessing == definition.preprocessing \
                        and attr.event_timestamp_column == definition.event_timestamp_column \
                        and attr.timestamp_format == definition.timestamp_format:
                    # Creating exactly same entity
                    # Just return the existing id
                    return rgm._to_uuid(r[0]["entity_id"])
                raise ConflictError("Entity %s already exists" %
                                    definition.qualified_name)
            id = uuid4()
            if self.db_conn.is_sqlalchemy_supported:
                query = sa.insert(orm.Entities).values(entity_id=str(id), entity_type=rgm.EntityType.Source,
                                                       qualified_name=definition.qualified_name,
                                                       attributes=definition.to_attr().to_json())
                self.connection.execute(query)
            else:
                c.execute(
                    f"insert into entities (entity_id, entity_type, qualified_name, attributes) values (%s, %s, %s, %s)",
                    (str(id),
                     rgm.EntityType.Source,
                     definition.qualified_name,
                     definition.to_attr().to_json()))
            self._create_edge(c, project_id, id, rgm.RelationshipType.Contains)
            self._create_edge(c, id, project_id, rgm.RelationshipType.BelongsTo)
            return id

    def create_project_anchor(self, project_id: UUID, definition: rgm.AnchorDef) -> UUID:
        project = self.get_entity(project_id)
        definition.qualified_name = f"{project.qualified_name}__{definition.name}"
        # Here we start a transaction, any following step failed, everything rolls back
        with self.db_conn.transaction() as c:
            # First we try to find existing entity with the same qualified name
            if self.db_conn.is_sqlalchemy_supported:
                query = sa.select(orm.Entities.entity_id, orm.Entities.entity_type,
                                  orm.Entities.attributes).where(
                    (orm.Entities.qualified_name == definition.qualified_name))
                r = self._fetch_helper(query)
            else:
                c.execute(f'''select entity_id, entity_type, attributes from entities where qualified_name = %s''',
                          definition.qualified_name)
                r = c.fetchall()
            if r:
                if len(r) > 1:
                    # There are multiple entities with same qualified name， that means we already have errors in the sa
                    assert False, "Data inconsistency detected, %d entities have same qualified_name %s" % (
                        len(r), definition.qualified_name)
                # The entity with same name already exists but with different type
                if rgm._to_type(r[0]["entity_type"], rgm.EntityType) != rgm.EntityType.Anchor:
                    raise ConflictError("Entity %s already exists" %
                                        definition.qualified_name)
                attr: rgm.AnchorAttributes = rgm._to_type(
                    json.loads(r[0]["attributes"]), rgm.AnchorAttributes)
                if attr.name == definition.name:
                    # Creating exactly same entity
                    # Just return the existing id
                    return rgm._to_uuid(r[0]["entity_id"])
                raise ConflictError("Entity %s already exists" %
                                    definition.qualified_name)
            if self.db_conn.is_sqlalchemy_supported:
                query = sa.select(orm.Entities.entity_id, orm.Entities.qualified_name).where(
                    (orm.Entities.entity_id == str(definition.source_id)) & (
                            orm.Entities.entity_type == rgm.EntityType.Source))
                r = self._fetch_helper(query)
            else:
                c.execute("select entity_id, qualified_name from entities where entity_id = %s and entity_type = %s",
                          (str(
                              definition.source_id), rgm.EntityType.Source))
                r = c.fetchall()
            if not r:
                raise ValueError("Source %s does not exist" %
                                 definition.source_id)
            ref = rgm.EntityRef(r[0]["entity_id"],
                                rgm.EntityType.Source, r[0]["qualified_name"])
            entity_id = uuid4()
            if self.db_conn.is_sqlalchemy_supported:
                query = sa.insert(orm.Entities).values(entity_id=str(entity_id), entity_type=rgm.EntityType.Anchor,
                                                       qualified_name=definition.qualified_name,
                                                       attributes=definition.to_attr(ref).to_json())
                self.connection.execute(query)
            else:
                c.execute(
                    f"insert into entities (entity_id, entity_type, qualified_name, attributes) values (%s, %s, %s, %s)",
                    (str(id),
                     rgm.EntityType.Anchor,
                     definition.qualified_name,
                     definition.to_attr(ref).to_json()))
            # Add "Contains/BelongsTo" relations between anchor and project
            self._create_edge(c, project_id, entity_id, rgm.RelationshipType.Contains)
            self._create_edge(c, entity_id, project_id, rgm.RelationshipType.BelongsTo)
            # Add "Consumes/Produces" relations between anchor and datasource
            self._create_edge(c, entity_id, definition.source_id,
                              rgm.RelationshipType.Consumes)
            self._create_edge(c, definition.source_id, entity_id,
                              rgm.RelationshipType.Produces)
            return entity_id

    def create_project_anchor_feature(self, project_id: UUID, anchor_id: UUID,
                                      definition: rgm.AnchorFeatureDef) -> UUID:
        anchor = self.get_entity(anchor_id)
        definition.qualified_name = f"{anchor.qualified_name}__{definition.name}"
        # Here we start a transaction, any following step failed, everything rolls back
        with self.db_conn.transaction() as c:
            # First we try to find existing entity with the same qualified name
            if self.db_conn.is_sqlalchemy_supported:
                query = sa.select(orm.Entities.entity_id, orm.Entities.entity_type,
                                  orm.Entities.attributes).where(
                    (orm.Entities.qualified_name == definition.qualified_name))
                r = self._fetch_helper(query)
            else:
                c.execute(f'''select entity_id, entity_type, attributes from entities where qualified_name = %s''',
                          definition.qualified_name)
                r = c.fetchall()
            if r:
                if len(r) > 1:
                    # There are multiple entities with same qualified name， that means we already have errors in the sa
                    assert False, "Data inconsistency detected, %d entities have same qualified_name %s" % (
                        len(r), definition.qualified_name)
                # The entity with same name already exists but with different type
                if rgm._to_type(r[0]["entity_type"], rgm.EntityType) != rgm.EntityType.AnchorFeature:
                    raise ConflictError("Entity %s already exists" %
                                        definition.qualified_name)
                attr: rgm.AnchorFeatureAttributes = rgm._to_type(
                    json.loads(r[0]["attributes"]), rgm.AnchorFeatureAttributes)
                if attr.name == definition.name \
                        and attr.type == definition.feature_type \
                        and attr.transformation == definition.transformation \
                        and attr.key == definition.key:
                    # Creating exactly same entity
                    # Just return the existing id
                    return rgm._to_uuid(r[0]["entity_id"])
                # The existing entity has different definition, that's a conflict
                raise ConflictError("Entity %s already exists" %
                                    definition.qualified_name)
            source_id = anchor.attributes.source.id
            id = uuid4()
            if self.db_conn.is_sqlalchemy_supported:
                query = sa.insert(orm.Entities).values(
                    entity_id=str(id),
                    entity_type=rgm.EntityType.AnchorFeature,
                    qualified_name=definition.qualified_name,
                    attributes=definition.to_attr().to_json()
                )
                r = self.connection.execute(query)
            else:
                c.execute(
                    f"insert into entities (entity_id, entity_type, qualified_name, attributes) values (%s, %s, %s, %s)",
                    (str(id),
                     rgm.EntityType.AnchorFeature,
                     definition.qualified_name,
                     definition.to_attr().to_json()))
            # Add "Contains/BelongsTo" relations between anchor feature and project
            self._create_edge(c, project_id, id, rgm.RelationshipType.Contains)
            self._create_edge(c, id, project_id, rgm.RelationshipType.BelongsTo)
            # Add "Contains/BelongsTo" relations between anchor feature and anchor
            self._create_edge(c, anchor_id, id, rgm.RelationshipType.Contains)
            self._create_edge(c, id, anchor_id, rgm.RelationshipType.BelongsTo)
            # Add "Consumes/Produces" relations between anchor feature and datasource used by anchor
            self._create_edge(c, id, source_id, rgm.RelationshipType.Consumes)
            self._create_edge(c, source_id, id, rgm.RelationshipType.Produces)
            return id

    def create_project_derived_feature(self, project_id: UUID, definition: rgm.DerivedFeatureDef) -> UUID:
        project = self.get_entity(project_id)
        definition.qualified_name = f"{project.qualified_name}__{definition.name}"
        # Here we start a transaction, any following step failed, everything rolls back
        with self.db_conn.transaction() as c:
            # First we try to find existing entity with the same qualified name
            if self.db_conn.is_sqlalchemy_supported:
                query = sa.select(orm.Entities.entity_id, orm.Entities.entity_type,
                                  orm.Entities.attributes).where(
                    (orm.Entities.qualified_name == definition.qualified_name))
                r = self._fetch_helper(query)
            else:
                c.execute(f'''select entity_id, entity_type, attributes from entities where qualified_name = %s''',
                          definition.qualified_name)
                r = c.fetchall()
            if r:
                if len(r) > 1:
                    # There are multiple entities with same qualified name， that means we already have errors in the sa
                    assert False, "Data inconsistency detected, %d entities have same qualified_name %s" % (
                        len(r), definition.qualified_name)
                # The entity with same name already exists but with different type, that's conflict
                if rgm._to_type(r[0]["entity_type"], rgm.EntityType) != rgm.EntityType.DerivedFeature:
                    raise ConflictError("Entity %s already exists" %
                                        definition.qualified_name)
                attr: rgm.DerivedFeatureAttributes = rgm._to_type(
                    json.loads(r[0]["attributes"]), rgm.DerivedFeatureAttributes)
                if attr.name == definition.name \
                        and attr.type == definition.feature_type \
                        and attr.transformation == definition.transformation \
                        and attr.key == definition.key:
                    # Creating exactly same entity
                    # Just return the existing id
                    return rgm._to_uuid(r[0]["entity_id"])
                # The existing entity has different definition, that's a conflict
                raise ConflictError("Entity %s already exists" %
                                    definition.qualified_name)
            r1 = []
            # Fill `input_anchor_features`, from `definition` we have ids only, we still need qualified names
            if definition.input_anchor_features:
                if self.db_conn.is_sqlalchemy_supported:
                    query = self.sql_session.query(orm.Entities.entity_id, orm.Entities.entity_type,
                                                   orm.Entities.qualified_name).filter(
                        orm.Entities.entity_id.in_(tuple([str(id) for id in definition.input_anchor_features])),
                        orm.Entities.entity_type == rgm.EntityType.AnchorFeature)
                    r1 = self._fetch_helper(query)
                else:
                    c.execute(
                        fr'''select entity_id, entity_type, qualified_name from entities where entity_id in %s and entity_type = %s ''',
                        (
                            tuple([str(id) for id in definition.input_anchor_features]),
                            rgm.EntityType.AnchorFeature))
                    r1 = c.fetchall()
                if len(r1) != len(definition.input_anchor_features):
                    # TODO: More detailed error
                    raise (ValueError("Missing input anchor features"))
            # Fill `input_derived_features`, from `definition` we have ids only, we still need qualified names
            r2 = []
            if definition.input_derived_features:
                if self.db_conn.is_sqlalchemy_supported:
                    query = self.sql_session.query(orm.Entities.entity_id, orm.Entities.entity_type,
                                                   orm.Entities.qualified_name).filter(
                        orm.Entities.entity_id.in_(tuple([str(id) for id in definition.input_derived_features])),
                        orm.Entities.entity_type == rgm.EntityType.DerivedFeature)
                    r2 = self._fetch_helper(query)
                else:
                    c.execute(
                        fr'''select entity_id, entity_type, qualified_name from entities where entity_id in %s and entity_type = %s ''',
                        (tuple([str(id) for id in definition.input_derived_features]),
                         rgm.EntityType.DerivedFeature))
                    r2 = c.fetchall()
                if len(r2) != len(definition.input_derived_features):
                    # TODO: More detailed error
                    raise (ValueError("Missing input derived features"))
            refs = list([rgm.EntityRef(r["entity_id"], r["entity_type"], r["qualified_name"]) for r in r1 + r2])
            id = uuid4()
            if self.db_conn.is_sqlalchemy_supported:
                query = sa.insert(orm.Entities).values(entity_id=str(id),
                                                       entity_type=rgm.EntityType.DerivedFeature,
                                                       qualified_name=definition.qualified_name,
                                                       attributes=definition.to_attr(refs).to_json())
                self.connection.execute(query)
            else:
                c.execute(
                    f"insert into entities (entity_id, entity_type, qualified_name, attributes) values (%s, %s, %s, %s)",
                    (str(id),
                     rgm.EntityType.DerivedFeature,
                     definition.qualified_name,
                     definition.to_attr(refs).to_json()))
            # Add "Contains/BelongsTo" relations between derived feature and project
            self._create_edge(c, project_id, id, rgm.RelationshipType.Contains)
            self._create_edge(c, id, project_id, rgm.RelationshipType.BelongsTo)
            for r in r1 + r2:
                # Add "Consumes/Produces" relations between derived feature and all its upstream
                input_feature_id = r["entity_id"]
                self._create_edge(c, id, input_feature_id,
                                  rgm.RelationshipType.Consumes)
                self._create_edge(c, input_feature_id, id,
                                  rgm.RelationshipType.Produces)
            return id

    def _create_edge(self, cursor, from_id: UUID, to_id: UUID, type: rgm.RelationshipType):
        """
        Create an edge with specified type between 2 entities, skip if the same connection already exists
        """
        if self.db_conn.is_sqlalchemy_supported:
            # TODO: might not be a safe solution since it's not transactional
            query = sa.insert(orm.Edges).values(
                edge_id=str(uuid4()),
                from_id=str(from_id),
                to_id=str(to_id),
                conn_type=type.name
            )
            self.connection.execute(query)
        else:
            sql = r'''
            IF NOT EXISTS (SELECT 1 FROM edges WHERE from_id=%(from_id)s and to_id=%(to_id)s and conn_type=%(type)s)
                    BEGIN
                        INSERT INTO edges
                        (edge_id, from_id, to_id, conn_type)
                        values
                        (%(edge_id)s, %(from_id)s, %(to_id)s, %(type)s)
                    END'''
            cursor.execute(sql, {
                "edge_id": str(uuid4()),
                "from_id": str(from_id),
                "to_id": str(to_id),
                "type": type.name
            })

    def _delete_all_entity_edges(self, cursor, entity_id: UUID):
        """
        Deletes all edges associated with an entity
        """
        if self.db_conn.is_sqlalchemy_supported:
            row_to_delete = self.sql_session.query(orm.Edges).filter(
                (orm.Edges.from_id == str(entity_id)) | orm.Edges.to_id == str(entity_id))
            row_to_delete.delete()
            self.sql_session.commit()
        else:
            sql = fr'''DELETE FROM edges WHERE from_id = %s OR to_id = %s'''
            cursor.execute(sql, (str(entity_id), str(entity_id)))

    def _delete_entity(self, cursor, entity_id: UUID):
        """
        Deletes entity from entities table
        """
        if self.db_conn.is_sqlalchemy_supported:
            row_to_delete = self.sql_session.query(orm.Entities).filter((orm.Entities.entity_id == str(entity_id)))
            # self.sql_session.delete(row_to_delete)
            row_to_delete.delete()
            self.sql_session.commit()
        else:
            sql = fr'''DELETE FROM entities WHERE entity_id = %s'''
            cursor.execute(sql, str(entity_id))

    def _fill_entity(self, e: rgm.Entity) -> rgm.Entity:
        """
        Entities in the DB contains only attributes belong to itself, but the returned
        data model contains connections/contents, so we need to fill this gap
        """
        if e.entity_type == rgm.EntityType.Project:
            edges = self.get_neighbors(e.id, rgm.RelationshipType.Contains)
            ids = list([e.to_id for e in edges])
            children = self._get_entities(ids)
            e.attributes.children = children
            return e
        if e.entity_type == rgm.EntityType.Anchor:
            conn = self.get_neighbors(e.id, rgm.RelationshipType.Contains)
            feature_ids = [e.to_id for e in conn]
            features = self._get_entities(feature_ids)
            e.attributes.features = features
            source_id = self.get_neighbors(
                e.id, rgm.RelationshipType.Consumes)[0].to_id
            source = self.get_entity(source_id)
            e.attributes.source = source
            return e
        if e.entity_type == rgm.EntityType.DerivedFeature:
            conn = self.get_neighbors(e.id, rgm.RelationshipType.Consumes)
            feature_ids = [e.to_id for e in conn]
            features = self._get_entities(feature_ids)
            e.attributes.input_features = features
            return e
        return e

    def _get_edges(self, ids: List[UUID], types: List[rgm.RelationshipType] = []) -> List[rgm.Edge]:
        if not ids:
            return []
        if self.db_conn.is_sqlalchemy_supported:
            if len(types) > 0:
                query = self.sql_session.query(orm.Edges.edge_id, orm.Edges.from_id, orm.Edges.to_id,
                                               orm.Edges.conn_type).filter(
                    (orm.Edges.from_id.in_(tuple([str(id) for id in ids]))) & (
                        orm.Edges.to_id.in_(tuple([str(id) for id in ids]))) & (
                        orm.Edges.conn_type.in_(tuple([t.name for t in types]))))
            else:
                query = self.sql_session.query(orm.Edges.edge_id, orm.Edges.from_id, orm.Edges.to_id,
                                               orm.Edges.conn_type).filter(
                    (orm.Edges.from_id.in_(tuple([str(id) for id in ids]))) & (
                        orm.Edges.to_id.in_(tuple([str(id) for id in ids]))))

            rows = self._fetch_helper(query)
        else:
            sql = fr"""select edge_id, from_id, to_id, conn_type from edges
            where from_id in %(ids)s
            and to_id in %(ids)s"""
            if len(types) > 0:
                sql = fr"""select edge_id, from_id, to_id, conn_type from edges
                where conn_type in %(types)s
                and from_id in %(ids)s
                and to_id in %(ids)s"""

            rows = self.db_conn.query(sql, {
                "ids": tuple([str(id) for id in ids]),
                "types": tuple([t.name for t in types]),
            })
        return list([rgm._to_type(row, rgm.Edge) for row in rows])

    def _get_entity(self, id_or_name: Union[str, UUID]) -> rgm.Entity:
        if self.db_conn.is_sqlalchemy_supported:
            query = sa.select(orm.Entities.entity_id, orm.Entities.qualified_name,
                              orm.Entities.entity_type, orm.Entities.attributes).where(
                (orm.Entities.entity_id == str(self.get_entity_id(id_or_name))))
            row = self._fetch_helper(query)
        else:
            row = self.db_conn.query(fr'''
            select entity_id, qualified_name, entity_type, attributes
            from entities
            where entity_id = %s
        ''', self.get_entity_id(id_or_name))
        if not row:
            raise KeyError(f"Entity {id_or_name} not found")
        row = row[0]
        row["attributes"] = json.loads(row["attributes"])
        return rgm._to_type(row, rgm.Entity)

    def _get_entities(self, ids: List[UUID]) -> List[rgm.Entity]:
        if not ids:
            return []
        if self.db_conn.is_sqlalchemy_supported:
            query = self.sql_session.query(orm.Entities.entity_id, orm.Entities.qualified_name,
                                           orm.Entities.entity_type,
                                           orm.Entities.attributes).filter(
                orm.Entities.entity_id.in_(tuple([str(id) for id in ids]), ))
            rows = self._fetch_helper(query)
        else:
            rows = self.db_conn.query(fr'''select entity_id, qualified_name, entity_type, attributes
                from entities
                where entity_id in %s
            ''', (tuple([str(id) for id in ids]),))
        ret = []
        for row in rows:
            row["attributes"] = json.loads(row["attributes"])
            ret.append(rgm.Entity(**row))
        return ret

    def _bfs(self, id: UUID, conn_type: rgm.RelationshipType) -> Tuple[List[rgm.Entity], List[rgm.Edge]]:
        """
        Breadth first traversal
        Starts from `id`, follow edges with `conn_type` only.

        WARN: There is no depth limit.
        """
        connections = []
        to_ids = [{
            "to_id": id,
        }]
        # BFS over SQL
        while len(to_ids) != 0:
            to_ids = self._bfs_step(to_ids, conn_type)
            connections.extend(to_ids)
        ids = set([id])
        for r in connections:
            ids.add(r["from_id"])
            ids.add(r["to_id"])
        entities = self.get_entities(ids)
        edges = list([rgm.Edge(**c) for c in connections])
        return (entities, edges)

    def _bfs_step(self, ids: List[UUID], conn_type: rgm.RelationshipType) -> Set[Dict]:
        """
        One step of the BFS process
        Returns all edges that connect to node ids the next step
        """
        ids = list([id["to_id"] for id in ids])
        if self.db_conn.is_sqlalchemy_supported:
            query = self.sql_session.query(orm.Edges.edge_id, orm.Edges.from_id, orm.Edges.to_id,
                                           orm.Edges.conn_type).filter(
                orm.Edges.conn_type == conn_type.name, orm.Edges.from_id.in_(tuple([str(id) for id in ids])))
            r = self._fetch_helper(query)
            return r
        else:
            sql = fr"""select edge_id, from_id, to_id, conn_type from edges where conn_type = %s and from_id in %s"""
            return self.db_conn.query(sql, (conn_type.name, tuple([str(id) for id in ids])))
