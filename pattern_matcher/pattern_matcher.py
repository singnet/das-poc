from copy import deepcopy
from enum import Enum, auto
from typing import Optional, Dict, List, Set
from functools import cmp_to_key
from abc import ABC, abstractmethod

from db_interface import DBInterface

WILDCARD = '*'
DEBUG = True

#TODO: Flag to enforce Vi != Vj for i != j

class CompatibilityStatus(int, Enum):
    """
    Enum for validate_match_only() warning messages.
    """
    INCOMPATIBLE = auto()
    NO_COVERING = auto()
    FIRST_COVERS_SECOND = auto()
    SECOND_COVERS_FIRST = auto()
    EQUAL = auto()

class Assignment(ABC):
    """
    TODO: documentation
    """

    def __init__(self, ordered: bool):
        self.ordered = ordered
        self.variables: Union[Set[str], FrozenSet] = set()
        self.hash: int = 0
        self.frozen = False

    def __hash__(self) -> int:
        assert self.hash
        return self.hash

    def __eq__(self, other) -> bool:
        assert self.hash and other.hash
        return self.hash == other.hash

    def __lt__(self, other) -> bool:
        assert self.hash and other.hash
        return self.hash < other.hash

    def freeze(self) -> bool:
        if self.frozen:
            return False
        else:
            self.frozen = True
            self.variables = frozenset(self.variables)
            return True

    @abstractmethod
    def assign(self, variable: str, value: str) -> bool:
        pass

    @abstractmethod
    def check_negation(self, negation: Assignment) -> bool:
        pass

    @abstractmethod
    def join(self, other: Assignment) -> Assignment:
        pass

class OrderedAssignment(Assignment):
    """
    TODO: documentation
    """

    def __init__(self):
        super().__init__(True)
        self.mapping: Dict[str, str] = {}

    def __repr__(self):
        return self.mapping.__repr__()

    def freeze(self):
        assert super().freeze()
        self.hash = hash(frozenset(self.mapping.items()))
        return True

    def assign(self, variable: str, value: str) -> bool:
        if variable is None or value is None or self.frozen:
            raise ValueError(f'Invalid assignment: variable = {variable} value = {value} frozen = {self.frozen}')
        if variable in self.variables:
            return self.mapping[variable] == value
        else:
            self.variables.add(variable)
            self.mapping[variable] = value
            return True

    def join(self, other: Assignment) -> Assignment:
        if other.ordered:
            return self._join_ordered(other)
        else:
            return other.join(self)

    def _join_ordered(self, other):
        status = self.evaluate_compatibility(other)
        if status == CompatibilityStatus.INCOMPATIBLE:
            return None
        if status == CompatibilityStatus.EQUAL or \
           status == CompatibilityStatus.FIRST_COVERS_SECOND:
            return self
        elif status == CompatibilityStatus.SECOND_COVERS_FIRST:
            return other
        elif status == CompatibilityStatus.NO_COVERING:
            answer = OrderedAssignment()
            for variable, value in self.mapping.items():
                answer.assign(variable, value)
            for variable, value in other.mapping.items():
                answer.assign(variable, value)
            answer.freeze()
            return answer
        else:
            raise ValueError(f'Invalid assignment status: {status}')

    def evaluate_compatibility(self, other) -> CompatibilityStatus:
        if self.hash == other.hash:
            return CompatibilityStatus.EQUAL
        for variable in self.variables.intersection(other.variables):
            if self.mapping[variable] != other.mapping[variable]:
                return CompatibilityStatus.INCOMPATIBLE
        if other.variables < self.variables:
            return CompatibilityStatus.FIRST_COVERS_SECOND
        elif self.variables < other.variables:
            return CompatibilityStatus.SECOND_COVERS_FIRST
        else:
            return CompatibilityStatus.NO_COVERING

    def _check_ordered_negation(self, negation: Assignment) -> bool:
        check = self.evaluate_compatibility(negation)
        return check != CompatibilityStatus.EQUAL and check != CompatibilityStatus.FIRST_COVERS_SECOND)

    def _check_unordered_negation(self, negation: Assignment) -> bool:
        if not self._check_ordered_negation(negation.ordered_mappings):
            return False
        for variable, value in self.mapping.items():
            for variable_count, value_count in negation.unordered_mappings:
                if variable_count.get(variable, 0) == 0 or value_count.get(value, 0) == 0:
                    break
            else:
                return False
        return True

    def check_negation(self, negation: Assignment) -> bool:
        if negation.ordered:
            return self._check_ordered_negation(negation)
        else:
            return self._check_unordered_negation(negation)

# TODO: cleanup comments
#    def _evaluate_compatibility_mixed(self, other: VariablesAssignment) -> CompatibilityStatus:
#        other_variables = deepcopy(other.variables_count)
#        other_values = deepcopy(other.values_count)
#        for variable, value in self.ordered_assignment:
#            if variable in other_variables:
#                assert(other_variables[variable] > 0)
#                other_variables[variable] -= 1
#                if other_values[value] == 0:
#                    return CompatibilityStatus.INCOMPATIBLE
#                other_values[value] -= 1
#        if all(c == 0 for c in other_variables.values()) and all(c == 0 for c in other_values.values()):
#            return CompatibilityStatus.FIRST_COVERS_SECOND
#        else:
#            return return CompatibilityStatus.NO_COVERING
#
#    def evaluate_compatibility(self, other: VariablesAssignment) -> CompatibilityStatus:
#        if self.ordered:
#            if other.ordered:
#                return self._evaluate_compatibility_ordered(other)
#            else:
#                return self._evaluate_compatibility_mixed(other)
#        else:
#            if other.ordered:
#                # TODO: converter returno first_covers em second_covers etc.
#                return other._evaluate_compatibility_mixed(self)
#            else:
#                return self._evaluate_compatibility_unordered(other)
#        
            

class UnorderedAssignment(Assignment):
    """
    TODO: documentation
    """

    def __init__(self):
        super().__init__(False)
        self.unordered_mappings: List[Tuple[Dict[str, int], Dict[str, int]]] = [({}, {})]
        self.ordered_mappings: OrderedAssignment = None
        

    def __repr__(self):
        return f'Variables = {self.unordered_mappings}'

    def freeze(self):
        assert super().freeze()
        assert self.ordered_mappings is None
        _hash = 1
        for variables, values in self.unordered_mappings:
            _hash ^= hash(tuple([hash(frozenset(variables.items())), hash(frozenset(values.items()))]))
            variables_count = tuple(sorted(variables.values()))
            values_count = tuple(sorted(values.values()))
            if variables_count != values_count:
                return False
        self.hash = _hash
        return True

    def assign(self, variable: str, value: str) -> bool:
        if variable is None or value is None or self.frozen:
            raise ValueError(f'Invalid assignment: variable = {variable} value = {value} frozen = {self.frozen}')
        variables, values = self.unordered_mappings[-1]
        variables[variable] = variables.get(variable, 0) + 1
        values[value] = values.get(value, 0) + 1
        if variable not in self.variables:
            self.variables.add(variable)
        return True
#    def _evaluate_compatibility_mixed(self, other: VariablesAssignment) -> CompatibilityStatus:
#        other_variables = deepcopy(other.variables_count)
#        other_values = deepcopy(other.values_count)
#        for variable, value in self.ordered_assignment:
#            if variable in other_variables:
#                assert(other_variables[variable] > 0)
#                other_variables[variable] -= 1
#                if other_values[value] == 0:
#                    return CompatibilityStatus.INCOMPATIBLE
#                other_values[value] -= 1
#        if all(c == 0 for c in other_variables.values()) and all(c == 0 for c in other_values.values()):
#            return CompatibilityStatus.FIRST_COVERS_SECOND
#        else:
#            return return CompatibilityStatus.NO_COVERING

    def check_viability(self) -> bool:
        unordered_mappings = deepcopy(self.unordered_mappings)
        for variable, value in self.ordered_mappings.mapping:
            for unordered_variables, unordered_values in unordered_mappings:
                if unordered_variables.get(variable, 0) < 1 or unordered_values.get(value, 0) < 1:
                    return False
                else:
                    unordered_variables[variable] -= 1
                    unordered_values[value] -= 1
        new_unordered_mappings: List[Tuple[Dict[str, int], Dict[str, int]]] = []
        for unordered_variables, unordered_values in unordered_mappings:
            if any(c1 != 0 or c2 != 0 for c1, c2 in zip(unordered_variables.values(), unordered_values.values())):
                new_unordered_mappings.append(tuple([unordered_variables, unordered_values]))
        self.unordered_mappings = new_unordered_mappings
        return True

    def recompute_hash(self) -> None:
        _hash = 1
        for variables, values in self.unordered_mappings:
            _hash ^= hash(tuple([hash(frozenset(variables.items())), hash(frozenset(values.items()))]))
            variables_count = tuple(sorted(variables.values()))
            values_count = tuple(sorted(values.values()))
            assert variables_count == values_count
        _hash ^= self.ordered_mappings.hash
        self.hash = _hash

    def add_ordered_mapping(self, other: OrderedAssignment) -> bool:
        if self.ordered_mappings is None:
            self.ordered_mappings = other
        else:
            self.ordered_mappings = self.ordered_mappings.join(other)
        if self.check_viability():
            self.recompute_hash()
            return True
        else:
            return False

    def add_unordered_mappings(self, others) -> bool:
        self.unordered_mappings.extend(others)
        if self.check_viability():
            self.recompute_hash()
            return True
        else:
            return False

    def join(self, other: Assignment) -> Assignment:
        answer = deepcopy(self)
        if other.ordered:
            return answer if answer.add_ordered_mapping(other) else None
        else:
            return answer if answer.add_ordered_mappings(other.ordered_mappings) else None

    def _check_ordered_negation(self, negation: Assignment) -> bool:
        check = self.evaluate_compatibility(negation)
        return check != CompatibilityStatus.EQUAL and check != CompatibilityStatus.FIRST_COVERS_SECOND)

    def _check_unordered_negation(self, negation: Assignment) -> bool:
        if not self._check_ordered_negation(negation.ordered_mappings):
            return False
        for variable, value in self.mapping.items():
            for variable_count, value_count in negation.unordered_mappings:
                if variable_count.get(variable, 0) == 0 or value_count.get(value, 0) == 0:
                    break
            else:
                return False
        return True

    def check_negation(self, negation: Assignment) -> bool:
        if negation.ordered:
            return self._check_ordered_negation(negation)
        else:
            return self._check_unordered_negation(negation)

class PatternMatchingAnswer:
    """
    TODO: documentation
    """

    def __init__(self):
        self.assignments: Set[Assignment] = set()
        self.negation: bool = False

    def __repr__(self):
        s = 'NOT\n' if self.negation else ''
        for assignment in self.assignments:
            s += str(assignment)
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

    def _assign_variables(self, db: DBInterface, link: str) -> Optional[Assignment]:
        link_targets = db.get_link_targets(link)
        assert(len(link_targets) == len(self.targets))
        answer = None
        # TODO: use self.ordered
        if db.is_ordered(link):
            answer = OrderedAssignment()
            for atom, handle in zip(self.targets, link_targets):
                if isinstance(atom, Variable):
                    if not answer.assign(atom.name, handle):
                        return None
            answer.freeze()
            return answer
        else:
            answer = UnorderedAssignment()
            targets_to_match = []
            for atom in self.targets:
                if isinstance(atom, Variable):
                    targets_to_match.append(atom)
                else:
                    link_targets.remove(atom.get_handle(db))
            assert(len(targets_to_match) == len(link_targets))
            for atom, handle in zip(targets_to_match, link_targets):
                answer.assign(atom.name, handle)
            return answer if answer.freeze() else None

    def matched(self, db: DBInterface, answer: PatternMatchingAnswer) -> bool:
        if not all(atom.matched(db, answer) for atom in self.targets):
            return False
        target_handles = [atom.get_handle(db) for atom in self.targets]
        if any(handle == WILDCARD for handle in target_handles):
            matched = db.get_matched_links(self.atom_type, target_handles)
            answer.assignments = set([asn for asn in [self._assign_variables(db, link) for link in matched] if asn is not None])
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
        return WILDCARD

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

# TODO: considerar ordered e unordered
def _check_assignment(assignment: Assignment, forbidden: Set[Assignment]) -> bool:
    for tabu in forbidden:
        check = assignment.evaluate_compatibility(tabu)
        if check == CompatibilityStatus.EQUAL or\
           check == CompatibilityStatus.FIRST_COVERS_SECOND:
            return False
    return True

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
        forbidden_assignments = set()
        ordered_terms = []
        unordered_terms = []
        for term in self.terms:
            term_answer = PatternMatchingAnswer()
            if not term.matched(db, term_answer):
                if DEBUG: print(f'NOT MATCHED: {term}')
                return False
            if not term_answer.assignments:
                if DEBUG: print('term_answer empty: {term}')
                continue
            if term_answer.negation:
                if DEBUG: print(f'Negation: {term}')
                if DEBUG: print(f'term_answer = {term_answer}')
                forbidden_assignments.update(term_answer.assignments)
                continue
            if not and_answer.assignments:
                if DEBUG: print(f'First term: {term}')
                if DEBUG: print(f'term_answer = {term_answer}')
                and_answer.assignments = term_answer.assignments
                continue
            joint_assignments = []
            for and_assignment in and_answer.assignments:
                for term_assignment in term_answer.assignments:
                    joint_assignments.append(and_assignment.join(term_assignment))
            and_answer.assignments = joint_assignments
        for assignment in and_answer.assignments:
            if _check_assignment(assignment, forbidden_assignments):
                answer.assignments.add(assignment)
            else:
                if DEBUG: print(f'Excluding {assignment}')
        if DEBUG: print(f'AND result = {answer}')
        return bool(answer.assignments)
        

#    def matched(self, db: DBInterface, answer: PatternMatchingAnswer) -> bool:
#        if len(self.terms) < 1:
#            return False
#        assert not answer.assignments
#        and_answer = PatternMatchingAnswer()
#        forbidden_assignments = set()
#        for term in self.terms:
#            if DEBUG: print(f'Term: {term}')
#            if DEBUG: print(f'Current and_answer = {and_answer}')
#            term_answer = PatternMatchingAnswer()
#            if not term.matched(db, term_answer):
#                if DEBUG: print('NOT MATCHED')
#                return False
#            if DEBUG: print(f'term_answer = {term_answer}')
#            if not term_answer.assignments:
#                if DEBUG: print('term_answer empty')
#                continue
#            if term_answer.negation:
#                if DEBUG: print('term_answer is a negation')
#                forbidden_assignments.update(term_answer.assignments)
#                continue
#            if not and_answer.assignments:
#                if DEBUG: print('and_answer empty')
#                and_answer.assignments = term_answer.assignments
#                continue
#            if DEBUG: print('Computing AND')
#            new_and_answer = PatternMatchingAnswer()
#            for and_assignment in and_answer.assignments:
#                for term_assignment in term_answer.assignments:
#                    #if DEBUG: print(f'Checking {and_assignment} and {term_assignment}')
#                    status = _evaluate_compatibility(and_assignment, term_assignment)
#                    #if DEBUG: print(f'status = {status}')
#                    if status == CompatibilityStatus.INCOMPATIBLE:
#                        continue
#                    if status == CompatibilityStatus.EQUAL or \
#                         status == CompatibilityStatus.FIRST_COVERS_SECOND:
#                        new_and_answer.assignments.add(and_assignment)
#                    elif status == CompatibilityStatus.SECOND_COVERS_FIRST:
#                        new_and_answer.assignments.add(term_assignment)
#                    elif status == CompatibilityStatus.NO_COVERING:
#                        new_assignment = VariablesAssignment()
#                        for variable, value in and_assignment.assignment.items():
#                            new_assignment.assign(variable, value)
#                        for variable, value in term_assignment.assignment.items():
#                            new_assignment.assign(variable, value)
#                        new_assignment.freeze()
#                        new_and_answer.assignments.add(new_assignment)
#                    else:
#                        raise ValueError(f'Invalid assignment status: {status}')
#                    #if DEBUG: print(f'Updated new_and_answer = {new_and_answer}')
#            and_answer = new_and_answer
#            if DEBUG: print(f'New and_answer = {and_answer}')
#        if DEBUG: print(f'negations = {forbidden_assignments}')
#        for assignment in and_answer.assignments:
#            if _check_assignment(assignment, forbidden_assignments):
#                answer.assignments.add(assignment)
#            else:
#                if DEBUG: print(f'Excluding {assignment}')
#        if DEBUG: print(f'AND result = {answer}')
#        return bool(answer.assignments)
