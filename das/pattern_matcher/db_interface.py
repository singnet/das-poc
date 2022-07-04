from abc import ABC, abstractmethod
from typing import List, Dict, Any

WILDCARD = '*'
UNORDERED_LINK_TYPES = ['Similarity', 'Set']

class DBInterface(ABC):
    """
    TODO: documentation
    """
    def __repr__(self):
        return "<DBInterface>"

    @abstractmethod
    def node_exists(self, node_type: str, node_name: str) -> bool:
        pass

    @abstractmethod
    def link_exists(self, link_type: str, targets: List[str]) -> bool:
        pass

    @abstractmethod
    def get_node_handle(self, node_type: str, node_name: str) -> str:
        pass

    @abstractmethod
    def get_link_handle(self, link_type: str, target_handles: List[str]) -> str:
        pass

    @abstractmethod
    def get_link_targets(self, handle: str) -> List[str]:
        pass

    @abstractmethod
    def is_ordered(self, handle: str) -> bool:
        pass

    @abstractmethod
    def get_matched_links(self, link_type: str, target_handles: List[str]):
        pass

    @abstractmethod
    def get_all_nodes(self, node_type: str, names: bool = False) -> List[str]:
        pass

    @abstractmethod
    def get_matched_type_template(self, template: List[Any]) -> List[str]:
        pass

    @abstractmethod
    def get_node_name(self, node_handle: str) -> str:
        pass
