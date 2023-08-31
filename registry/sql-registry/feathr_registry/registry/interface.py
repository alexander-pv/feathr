from abc import ABC, abstractmethod
from typing import Union, List, Dict, Optional
from uuid import UUID

from feathr_registry.registry.models import base as rgm


class Registry(ABC):
    @abstractmethod
    def get_projects(self) -> List[str]:
        """
        Returns the names of all projects
        """
        pass

    @abstractmethod
    def get_projects_ids(self) -> Dict:
        """
        Returns the ids to names mapping of all projects
        """
        pass

    @abstractmethod
    def get_entity(self, id_or_name: Union[str, UUID]) -> rgm.Entity:
        """
        Get one entity by its id or qualified name
        """
        pass

    @abstractmethod
    def get_entities(self, ids: List[UUID]) -> List[rgm.Entity]:
        """
        Get list of entities by their ids
        """
        pass

    @abstractmethod
    def get_entity_id(self, id_or_name: Union[str, UUID]) -> UUID:
        """
        Get entity id by its name
        """
        pass

    @abstractmethod
    def get_neighbors(self, id_or_name: Union[str, UUID], relationship: rgm.RelationshipType) -> List[rgm.Edge]:
        """
        Get list of edges with specified type that connect to this entity.
        The edge contains fromId and toId so we can follow to the entity it connects to
        """
        pass

    @abstractmethod
    def get_lineage(self, id_or_name: Union[str, UUID]) -> rgm.EntitiesAndRelations:
        """
        Get all the upstream and downstream entities of an entity, along with all edges connect them.
        Only meaningful to features and data sources.
        """
        pass

    @abstractmethod
    def get_project(self, id_or_name: Union[str, UUID]) -> rgm.EntitiesAndRelations:
        """
        Get a project and everything inside of it, both entities and edges
        """
        pass

    @abstractmethod
    def search_entity(self,
                      keyword: str,
                      type: List[rgm.EntityType],
                      project: Optional[Union[str, UUID]] = None,
                      start: Optional[int] = None,
                      size: Optional[int] = None) -> List[rgm.EntityRef]:
        """
        Search entities with specified type that also match the keyword in a project
        """
        pass

    @abstractmethod
    def create_project(self, definition: rgm.ProjectDef) -> UUID:
        """
        Create a new project
        """
        pass

    @abstractmethod
    def create_project_datasource(self, project_id: UUID, definition: rgm.SourceDef) -> UUID:
        """
        Create a new datasource under the project
        """
        pass

    @abstractmethod
    def create_project_anchor(self, project_id: UUID, definition: rgm.AnchorDef) -> UUID:
        """
        Create a new anchor under the project
        """
        pass

    @abstractmethod
    def create_project_anchor_feature(self, project_id: UUID, anchor_id: UUID,
                                      definition: rgm.AnchorFeatureDef) -> UUID:
        """
        Create a new anchor feature under the anchor in the project
        """
        pass

    @abstractmethod
    def create_project_derived_feature(self, project_id: UUID, definition: rgm.DerivedFeatureDef) -> UUID:
        """
        Create a new derived feature under the project
        """
        pass

    @abstractmethod
    def get_dependent_entities(self, entity_id: Union[str, UUID]) -> List[rgm.Entity]:
        """
        Given entity id, returns list of all entities that are downstream/dependant on the given entity
        """
        pass

    @abstractmethod
    def delete_entity(self, entity_id: Union[str, UUID]):
        """
        Deletes given entity
        """
        pass
