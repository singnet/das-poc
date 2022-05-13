from typing import Dict, List
from abc import ABC, abstractmethod

from db_interface import DBInterface

WILDCARD = '*'

class VariablesAssignment:
    """
    TODO: documentation
    """

    def __init__(self):
        self.assignment = {}

    def __repr__(self):
        return self.assignment.__repr__()

    def reset(self):
        self.assignment = {}

    def assign(self, variable: str, value: str) -> bool:
        if variable is None or value is None:
            raise ValueError(f'Invalid assignment: variable = {variable} value = {value}')
        if variable in self.assignment:
            return self.assignment[variable] == value
        else:
            self.assignment[variable] = value
            return True

class PatternMatchingAnswer:
    """
    TODO: documentation
    """

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
    def matched(self, db: DBInterface, answer: VariablesAssignment) -> bool:
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
    def get_handle(self, db: DBInterface) -> str:
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

    def get_handle(self, db: DBInterface) -> str:
        return db.get_node_handle(self.atom_type, self.name)

    def matched(self, db: DBInterface, answer: VariablesAssignment) -> bool:
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

    def get_handle(self, db: DBInterface) -> str:
        target_handles = [target.get_handle(db) for target in self.targets]
        if any(handle is None for handle in target_handles):
            return None
        return db.get_link_handle(self.atom_type, target_handles)

    def matched(self, db: DBInterface, answer: VariablesAssignment) -> bool:
        if not all(atom.matched(db, answer) for atom in self.targets):
            return False
        target_handles = [atom.get_handle(db) for atom in self.targets]
        if any(handle == WILDCARD for handle in target_handles):
            matched = db.get_matched_links(target_handles)
            assignment = VariableAssignment()
            for link in matched:
                assignment.reset()
                for i in range(0, len(self.targets)):
                    if matched[i] == WILDCARD:
                        if not assignment.assign(self.targets[i].name, link.[i + 1]):
                            break
                else:
                    WRONG AQUI
                    como controlar answer? Deve ser assignment ou Answer? Provavelmebte o segundo. Mas como controlar?
                    if answer.assign_multiple_values(assignment)
        else:
            return db.link_exists(self.atom_type, target_handles)

        for atom in self.targets:
            if isinstance(atom, Variable):

class Variable(Atom):
    """
    TODO: documentation
    """

    targets: List[Atom]

    def __init__(self, variable_name: str):
        super().__init__('ANY')
        self.name = variable_name

    def __repr__(self):
        return f"{self.name}"

    def get_handle(self, db: DBInterface) -> str:
        return '*'

    def matched(self, db: DBInterface, answer: VariablesAssignment) -> bool:
        return True
        
