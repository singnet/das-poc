from typing import Optional, Dict, List
from functools import cmp_to_key
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
        self.variables_mappings: List[VariablesAssignment] = []

    def __repr__(self):
        s = ''
        for mapping in self.variables_mappings:
            s += str(mapping)
            s += '\n'
        return s

class LogicalExpression(ABC):
    """
    TODO: documentation
    """
    
    @abstractmethod
    def matched(self, db: DBInterface, answer: PatternMatchingAnswer) -> bool:
        pass

    def __repr__(self):
        return "<LogicalExpression>"

class Atom(LogicalExpression, ABC):
    """
    TODO: documentation
    """
    
    def __init__(self, atom_type: str):
        self.atom_type = atom_type
        self.handle = None

    def __repr__(self):
        return f"{self.atom_type}"

    @abstractmethod
    def get_handle(self, db: DBInterface) -> str:
        pass

class Node(Atom):
    """
    TODO: documentation
    """

    def __init__(self, node_type: str, node_name: str):
        super().__init__(node_type)
        self.name = node_name

    def __repr__(self):
        return f"<{super().__repr__()}: {self.name}>"

    def get_handle(self, db: DBInterface) -> str:
        if not self.handle:
            self.handle = db.get_node_handle(self.atom_type, self.name)
        return self.handle

    def matched(self, db: DBInterface, answer: PatternMatchingAnswer) -> bool:
        return db.node_exists(self.atom_type, self.name)

class Link(Atom):
    """
    TODO: documentation
    """

    def __init__(self, link_type: str, targets: List[Atom], ordered: bool):
        super().__init__(link_type)
        def comparator(t1, t2):
            if isinstance(t1, Variable):
                return 1
            elif isinstance(t2, Variable):
                return -1
            else:
                return 0
        self.ordered = ordered
        if ordered:
            self.targets = targets
        else:
            self.targets = sorted(targets, key=cmp_to_key(comparator))

    def __repr__(self):
        return f"<{super().__repr__()}: {self.targets}>"

    def get_handle(self, db: DBInterface) -> str:
        if not self.handle:
            target_handles = [target.get_handle(db) for target in self.targets]
            if any(handle is None for handle in target_handles):
                return None
            self.handle = db.get_link_handle(self.atom_type, target_handles)
        return self.handle

    def _assign_variables(self, db: DBInterface, link: str) -> Optional[VariablesAssignment]:
        #print('_assign_variables()', f'link = {link}')
        link_targets = db.get_link_targets(link)
        assert(len(link_targets) == len(self.targets))
        answer: VariablesAssignment = VariablesAssignment()
        #print('_assign_variables()', f'link_targets = {link_targets}')
        if db.is_ordered(link):
            for atom, handle in zip(self.targets, link_targets):
                if isinstance(atom, Variable):
                    if not answer.assign(atom.name, handle):
                        return None
        else:
            for atom in self.targets:
                if isinstance(atom, Variable):
                    handle = link_targets.pop(0)
                    if not answer.assign(atom.name, handle):
                        return None
                else:
                    link_targets.remove(atom.get_handle(db))

        return answer

    def matched(self, db: DBInterface, answer: PatternMatchingAnswer) -> bool:
        if not all(atom.matched(db, answer) for atom in self.targets):
            return False
        target_handles = [atom.get_handle(db) for atom in self.targets]
        if any(handle == WILDCARD for handle in target_handles):
            matched = db.get_matched_links(self.atom_type, target_handles)
            #print('matched()', f'matched = {matched}')
            answer.variables_mappings = [asn for asn in [self._assign_variables(db, link) for link in matched] if asn is not None]
            #print('matched()', 'answer.variables_mappings = ', answer.variables_mappings)
            return bool(answer.variables_mappings)
        else:
            return db.link_exists(self.atom_type, target_handles)

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

    def matched(self, db: DBInterface, answer: PatternMatchingAnswer) -> bool:
        return True
        
