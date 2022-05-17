from enum import Enum, auto
from typing import Optional, Dict, List
from functools import cmp_to_key
from abc import ABC, abstractmethod

from db_interface import DBInterface

WILDCARD = '*'
DEBUG = True

#TODO: Flag to enforce Vi != Vj for i != j

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

class _AssignmentCompatibilityStatus(int, Enum):
    """
    Enum for validate_match_only() warning messages.
    """
    INCOMPATIBLE = auto()
    NO_COVERING = auto()
    FIRST_COVERS_SECOND = auto()
    SECOND_COVERS_FIRST = auto()
    EQUAL = auto()

def _evaluate_compatibility(assignment1: VariablesAssignment, assignment2: VariablesAssignment) -> _AssignmentCompatibilityStatus:
    if assignment1.hash == assignment2.hash:
        return _AssignmentCompatibilityStatus.EQUAL
    for variable in assignment1.variables.intersection(assignment2.variables):
        if assignment1.assignment[variable] != assignment2.assignment[variable]:
            return _AssignmentCompatibilityStatus.INCOMPATIBLE
    if assignment2.variables < assignment1.variables:
        return _AssignmentCompatibilityStatus.FIRST_COVERS_SECOND
    elif assignment1.variables < assignment2.variables:
        return _AssignmentCompatibilityStatus.SECOND_COVERS_FIRST
    else:
        return _AssignmentCompatibilityStatus.NO_COVERING

class And(LogicalExpression):
    """
    TODO: documentation
    """

    def __init__(self, terms: List[LogicalExpression]):
        self.terms = terms

    def __repr__(self):
        return f'AND({self.terms})'

    def matched(self, db: DBInterface, answer: PatternMatchingAnswer) -> bool:
        if len(self.terms) < 1:
            return False
        assert not answer.assignments
        and_answer = PatternMatchingAnswer()
        #AQUI transformar and_answer em set()
        for term in self.terms:
            if DEBUG: print(f'Term: {term}')
            if DEBUG: print(f'Current and_answer = {and_answer}')
            term_answer = PatternMatchingAnswer()
            if not term.matched(db, term_answer):
                if DEBUG: print('NOT MATCHED')
                return False
            if not term_answer.assignments:
                if DEBUG: print('term_answer empty')
                continue
            # AQUI: se for NOT, add todos os assignments num set() e continua
            if not and_answer.assignments:
                if DEBUG: print('and_answer empty')
                and_answer.assignments = term_answer.assignments
                continue
            if DEBUG: print('Computing AND')
            new_and_answer = PatternMatchingAnswer()
            for and_assignment in and_answer.assignments:
                for term_assignment in term_answer.assignments:
                    if DEBUG: print(f'Checking {and_assignment} and {term_assignment}')
                    status = _evaluate_compatibility(and_assignment, term_assignment)
                    if DEBUG: print(f'status = {status}')
                    if status == _AssignmentCompatibilityStatus.INCOMPATIBLE:
                        continue
                    if status == _AssignmentCompatibilityStatus.EQUAL or \
                         status == _AssignmentCompatibilityStatus.FIRST_COVERS_SECOND:
                        new_and_answer.assignments.append(and_assignment)
                    elif status == _AssignmentCompatibilityStatus.SECOND_COVERS_FIRST:
                        new_and_answer.assignments.append(term_assignment)
                    elif status == _AssignmentCompatibilityStatus.NO_COVERING:
                        new_assignment = VariablesAssignment()
                        for variable, value in and_assignment.assignment.items():
                            new_assignment.assign(variable, value)
                        for variable, value in term_assignment.assignment.items():
                            new_assignment.assign(variable, value)
                        new_assignment.freeze_assignment()
                        new_and_answer.assignments.append(new_assignment)
                    else:
                        raise ValueError(f'Invalid assignment status: {status}')
                    if DEBUG: print(f'Updated new_and_answer = {new_and_answer}')
            and_answer = new_and_answer
            if DEBUG: print(f'New and_answer = {and_answer}')
        # AQUI Checar a compatibilidade de tudo no and_answer com o set() do NOT
        answer.assignments = and_answer.assignments
        if DEBUG: print(f'AND result = {answer}')
        return bool(answer.assignments)
