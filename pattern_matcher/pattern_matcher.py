from typing import Dict, List
from abc import ABC, abstractmethod

from db_interface import DBInterface

class PatternMatchingAnswer:
    """
    TODO: documentation
    """

    variables_mappings: List[Dict[str, str]]

    def __init__(self):
        self.variables_mappings = []

    def __repr__(self):
        s = ''
        for mapping in self.variables_mappings:
            s += mapping
            s += '\n'
        return s

class LogicalExpression(ABC):
    """
    TODO: documentation
    """
    
    @abstractmethod
    def match(self, db: DBInterface, answer: PatternMatchingAnswer) -> bool:
        pass

    def __repr__(self):
        return "<LogicalExpression>"

class Atom(LogicalExpression, ABC):
    """
    TODO: documentation
    """
    
    atom_type: str

    def __init__(self, atom_type: str):
        self.atom_type = atom_type

    def __repr__(self):
        return f"{self.atom_type}"

    @abstractmethod
    def get_handle(self, db: DBInterface):
        pass

class Node(Atom):
    """
    TODO: documentation
    """

    name: str

    def __init__(self, node_type: str, node_name: str):
        super().__init__(node_type)
        self.name = node_name

    def __repr__(self):
        return f"<{super().__repr__()}: {self.name}>"

    def get_handle(self, db: DBInterface):
        return db.get_node_handle(self.atom_type, self.name)

    def match(self, db: DBInterface, answer: PatternMatchingAnswer) -> bool:
        return db.node_exists(self.atom_type, self.name)

class Link(Atom):
    """
    TODO: documentation
    """

    targets: List[Atom]

    def __init__(self, link_type: str, targets: List[Atom]):
        super().__init__(link_type)
        self.targets = targets

    def __repr__(self):
        return f"<{super().__repr__()}: {self.targets}>"

    def get_handle(self, db: DBInterface):
        target_handles = [handle for handle in [target.get_handle(db) for target in self.targets] if handle is not None]
        if any(handle is None for handle in target_handles):
            return None
        return db.get_link_handle(self.atom_type, target_handles)

    def match(self, db: DBInterface, answer: PatternMatchingAnswer) -> bool:
        if not all(atom.match(db, answer) for atom in self.targets):
            return False
        target_handles = [handle for handle in [atom.get_handle(db) for atom in self.targets] if handle is not None]
        return db.link_exists(self.atom_type, target_handles)
        
