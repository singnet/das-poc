from enum import Enum, auto
from typing import Optional, Dict, List
from functools import cmp_to_key
from abc import ABC, abstractmethod

from db_interface import DBInterface

WILDCARD = '*'
DEBUG = False

class VariablesAssignment:
    """
    TODO: documentation
    """

    def __init__(self):
        self.assignment: Dict[str, str] = {}
        self.variables: Union[Set[str], FrozenSet] = set()
        self.hash: int = 0
        self.frozen = False

    def __repr__(self):
        return self.assignment.__repr__()

    def reset(self):
        self.assignment = {}

    def freeze_assignment(self):
        self.variables = frozenset(self.variables)
        self.hash = hash(frozenset(self.assignment.items()))
        self.frozen = True

    def assign(self, variable: str, value: str) -> bool:
        if variable is None or value is None or self.frozen:
            raise ValueError(f'Invalid assignment: variable = {variable} value = {value} frozen = {self.frozen}')
        if variable in self.variables:
            return self.assignment[variable] == value
        else:
            self.variables.add(variable)
            self.assignment[variable] = value
            return True

class PatternMatchingAnswer:
    """
    TODO: documentation
    """

    def __init__(self):
        self.assignments: List[VariablesAssignment] = []
        self.negation: bool = False

    def __repr__(self):
        s = 'NOT\n' if self.negation else ''
        for mapping in self.assignments:
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
        link_targets = db.get_link_targets(link)
        assert(len(link_targets) == len(self.targets))
        answer: VariablesAssignment = VariablesAssignment()
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

        answer.freeze_assignment()
        return answer

    def matched(self, db: DBInterface, answer: PatternMatchingAnswer) -> bool:
        if not all(atom.matched(db, answer) for atom in self.targets):
            return False
        target_handles = [atom.get_handle(db) for atom in self.targets]
        if any(handle == WILDCARD for handle in target_handles):
            matched = db.get_matched_links(self.atom_type, target_handles)
            #print('matched()', f'matched = {matched}')
            answer.assignments = [asn for asn in [self._assign_variables(db, link) for link in matched] if asn is not None]
            #print('matched()', 'answer.assignments = ', answer.assignments)
            return bool(answer.assignments)
        else:
            return db.link_exists(self.atom_type, target_handles)

class Variable(Atom):
    """
    TODO: documentation
    """

    def __init__(self, variable_name: str):
        super().__init__('ANY')
        self.name = variable_name

    def __repr__(self):
        return f"{self.name}"

    def get_handle(self, db: DBInterface) -> str:
        return '*'

    def matched(self, db: DBInterface, answer: PatternMatchingAnswer) -> bool:
        return True

class Not(LogicalExpression):
    """
    TODO: documentation
    """

    def __init__(self, term: LogicalExpression):
        self.term = term

    def __repr__(self):
        return f"NOT({self.term})"

    def matched(self, db: DBInterface, answer: PatternMatchingAnswer) -> bool:
        self.term.matched(db, answer)
        answer.negation = not answer.negation
        return True

