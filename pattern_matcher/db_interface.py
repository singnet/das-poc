from abc import ABC, abstractmethod
from typing import List

class DBInterface(ABC):
    """
    TODO: documentation
    """
    def __repr__(self):
        return "<DBInterface>"

    @abstractmethod
    def node_exists(self, atom_type: str, node_name: str) -> bool:
        pass

    @abstractmethod
    def link_exists(self, atom_type: str, targets: List[str]) -> bool:
        pass

    @abstractmethod
    def get_node_handle(self, node_type: str, node_name: str) -> str:
        pass

    @abstractmethod
    def get_link_handle(self, link_type: str, target_handles: List[str]) -> str:
        pass

    @abstractmethod
    def get_matched_links(self, target_handles: List[str]) -> str:
        pass
