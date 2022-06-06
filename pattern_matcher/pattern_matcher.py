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

    def __init__(self):
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
    def check_negation(self, negation: 'Assignment') -> bool:
        pass

    @abstractmethod
    def join(self, other: 'Assignment') -> 'Assignment':
        pass

class OrderedAssignment(Assignment):
    """
    TODO: documentation
    """

    def __init__(self):
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
        if isinstance(other, OrderedAssignment):
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
        assert other is not None
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

    def check_negation(self, negation: Assignment) -> bool:
        if negation.ordered:
            check = self.evaluate_compatibility(negation)
            return check != CompatibilityStatus.EQUAL and check != CompatibilityStatus.FIRST_COVERS_SECOND
        else:
            return not negation.contains_ordered(self)

class UnorderedAssignment(Assignment):
    """
    TODO: documentation
    """
    def __init__(self):
        self.symbols: Dict[str, int] = {}
        self.values: Dict[str, int] = {}

    def __repr__(self):
        return self.symbols.__repr__() + ' ' + self.values.__repr__()

    def freeze(self):
        assert super().freeze()
        symbols_count = tuple(sorted(self.symbols.values()))
        values_count = tuple(sorted(self.values.values()))
        if vsymbols_count != values_count:
            return False
        self.hash = hash(tuple([hash(frozenset(self.symbols.items())), hash(frozenset(self.values.items()))]))
        return True

    def assign(self, variable: str, value: str) -> bool:
        if variable is None or value is None or self.frozen:
            raise ValueError(f'Invalid assignment: variable = {variable} value = {value} frozen = {self.frozen}')
        self.variables[variable] = self.variables.get(variable, 0) + 1
        self.values[value] = self.values.get(value, 0) + 1
        self.variables.add(variable)
        return True

    def join(self, other: Assignment) -> Assignment:
        if isinstance(other, CompositeAssignment):
            return other.join(self)
        else:
            composite = CompositeAssignment(self)
            return composite.join(other)

    def contains_ordered(self, ordered_assignment) -> bool:
        count_values = {}
        for variable, value in ordered_assignment.items():
            if variable not in self.variables:
                return False
            count_values[value] = count_values.get(value, 0) + 1
        for value in count_values.keys():
            if self.values.get(value, 0) < count_values[value]:
                return False
        return True

    def check_negation(self, negation: Assignment) -> bool:
AQUI: reescrever
        if isinstance(negation, OrtderedAssignment):
            return check != CompatibilityStatus.EQUAL and check != CompatibilityStatus.FIRST_COVERS_SECOND
        else:
            return not negation.contains_ordered(self)


class CompositeAssignment(Assignment):
    """
    TODO: documentation
    """

    def __init__(self, assignment: UnorderedAssignment):
        super().__init__(False)
        self.unordered_mappings: List[UnorderedAssignment] = [assignment]
        self.ordered_mappings: OrderedAssignment = None
        

    def __repr__(self):
        return f'Ordered = {self.ordered_mappings} | Unordered = {self.unordered_mappings}'

    def freeze(self):
        assert super().freeze()
        assert self.ordered_mappings is None
        _hash = 1
        for unordered in self.unordered_mappings:
            if not unordered.freeze():
                return False
            _hash ^= unordered.hash
        self.hash = _hash
        return True

    def assign(self, variable: str, value: str) -> bool:
        assert False

    def _contains_ordered(self, variables, values, ordered_mapping) -> bool:
        count_values = {}
        for variable, value in ordered_mappings.items():
            if variable not in variables:
                return False
            count_values[value] = count_values.get(value, 0) + 1
        for value in count_values:
            if values.get(value, 0) < count_values[value]:
                return False
        return True

    def contains_ordered(self, other: OrderedAssignment) -> bool:

        for variables, values in self.unoprdered_mappings:
            if not self._contains_ordered(variables, values, other):
                return False
        return True

        #AQUI: Reescrever

        variables = other.mapping.keys()
        if all(any(v not in unordered_variables for v in variables) for unordered_variables, _ in self.unordered_mappings):
            return False
        _unordered_mappings = deepcopy(self.unordered_mappings)
        for variable, value in other.mapping.items():
            for unordered_variables, unordered_values in _unordered_mappings:
                if variable in unordered_variables:
                    if unordered_variables.get(variable, 0) == 0 or unordered_values.get(value, 0) == 0:
                        return False
                    else:
                        unordered_variables[variable] -= 1
                        unordered_values[value] -= 1
        print(f'variables = {variables}')
        for unordered_variables, unordered_values in _unordered_mappings:
            for ((var, cvar), (val, cval)) in zip(unordered_variables.items(), unordered_values.items()):
                print(f'var = {var} val = {val} cvar = {cvar} cval = {cval}')
                if var in variables and (cvar > 0 or cval > 0):
                    return False
        return True
            
    def _check_viability(self) -> bool:
        #print(f'_check_viability() self = {self}')
        if not self.ordered_mappings and not self.unordered_mappings:
            return False
        _unordered_mappings = deepcopy(self.unordered_mappings)
        if self.ordered_mappings is not None:
            for variable, value in self.ordered_mappings.mapping.items():
                for unordered_variables, unordered_values in _unordered_mappings:
                    if variable in unordered_variables:
                        if unordered_variables.get(variable, 0) == 0 or unordered_values.get(value, 0) == 0:
                            #print(f'Return False on {variable} = {value}')
                            return False
                        else:
                            unordered_variables[variable] -= 1
                            unordered_values[value] -= 1
        # TODO: check if this cleanup is really needed
        new_unordered_mappings: List[Tuple[Dict[str, int], Dict[str, int]]] = []
        for unordered_variables, unordered_values in _unordered_mappings:
            if any(c1 != 0 or c2 != 0 for c1, c2 in zip(unordered_variables.values(), unordered_values.values())):
                new_unordered_mappings.append(tuple([unordered_variables, unordered_values]))
        self.unordered_mappings = new_unordered_mappings
        return True

    def _recompute_hash(self) -> None:
        _hash = 1
        for variables, values in self.unordered_mappings:
            _hash ^= hash(tuple([hash(frozenset(variables.items())), hash(frozenset(values.items()))]))
            variables_count = tuple(sorted(variables.values()))
            values_count = tuple(sorted(values.values()))
            assert variables_count == values_count
        if self.ordered_mappings is not None:
            _hash ^= self.ordered_mappings.hash
        self.hash = _hash

    def _commit_changes(self) -> bool:
        if self._check_viability():
            self._recompute_hash()
            return True
        else:
            return False

    def _add_ordered_mapping(self, other: OrderedAssignment) -> bool:
        if self.ordered_mappings is None:
            self.ordered_mappings = other
        else:
            self.ordered_mappings = self.ordered_mappings.join(other)
            if self.ordered_mappings is None:
                return False
        return self._commit_changes()

    def _add_unordered_mappings(self, others) -> bool:
        self.unordered_mappings.extend(others)
        return self._commit_changes()

    def join(self, other: Assignment) -> Assignment:
        answer = deepcopy(self)
        if other.ordered:
            return answer if answer._add_ordered_mapping(other) else None
        else:
            # TODO: why not joing the ordered part?
            answer._add_unordered_mappings(other.unordered_mappings)
            return answer if answer._add_ordered_mapping(other.ordered_mappings) else None

    def _remove_ordered_mapping(self, other: OrderedAssignment) -> bool:
        if self.ordered_mappings is not None:
            check = self.ordered_mappings.evaluate_compatibility(negation)
            if check == CompatibilityStatus.EQUAL or check == CompatibilityStatus.FIRST_COVERS_SECOND:
                return False
        new_mappings: List[Tuple[Dict[str, int], Dict[str, int]]] = []
        for variables, values in self.unordered_mappings:
            variables_copy = deepcopy(variables)
            values_copy = deepcopy(values)
            for variable, value in other.mapping.items():
                if variables_copy.get(variable, 0) == 0 or values_copy.get(value, 0) == 0:
                    new_mappings.append(tuple([variables, values]))
                    continue
                else:
                    variables_copy[variable] -= 1
                    values_copy[value] -= 1
        self.unordered_mappings = new_mappings
        return self._commit_changes()

    def _is_covered_by(self, variables1, values1, variables2, values2):
        for variable, count in variables1.items():
            if variables2[variable] < count:
                return False
        for value, count in values1.items():
            if values2[value] < count:
                return False
        return True

    def _remove_unordered_mapping(self, negation: 'CompositeAssignment') -> bool:
        if not self._remove_ordered_mapping(negation.ordered_mappings):
            return False
        for other_variables, other_values in other.unordered_mappings:
            new_mappings: List[Tuple[Dict[str, int], Dict[str, int]]] = []
            for self_variables, self_values in self.unordered_mappings:
                if not _is_covered_by(self_variables, self_values, other_variables, other_values):
                    new_mappings.append(tuple([self_variables, self_values]))
            self.unordered_mappings = new_mappings
        return self._commit_changes()

    def check_negation(self, negation: Assignment) -> bool:
        if negation.ordered:
            return self._remove_ordered_mapping(negation)
        else:
            return self._remove_unordered_mapping(negation)

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
            answer = CompositeAssignment()
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

class And(LogicalExpression):
    """
    TODO: documentation
    """

    def __init__(self, terms: List[LogicalExpression]):
        self.terms = terms

    def __repr__(self):
        return f'AND({self.terms})'

    def matched(self, db: DBInterface, answer: PatternMatchingAnswer) -> bool:
        if not self.terms:
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
                if DEBUG: print(f'term_answer:\n{term_answer}')
                forbidden_assignments.update(term_answer.assignments)
                continue
            if not and_answer.assignments:
                if DEBUG: print(f'First term: {term}')
                if DEBUG: print(f'term_answer:\n{term_answer}')
                and_answer.assignments = term_answer.assignments
                continue
            if DEBUG: print(f'New term: {term}')
            if DEBUG: print(f'term_answer:\n{term_answer}')
            joint_assignments = []
            for and_assignment in and_answer.assignments:
                for term_assignment in term_answer.assignments:
                    joint_assignment = and_assignment.join(term_assignment)
                    #print(f'ea = {and_assignment}\nta = {term_assignment}\nja = {joint_assignment}\n')
                    if joint_assignment is not None:
                        joint_assignments.append(joint_assignment)
            and_answer.assignments = joint_assignments
            if DEBUG: print(f'and_answer after join:\n{and_answer}')
        #print(f'FORBIDDEN = {forbidden_assignments}')
        for assignment in and_answer.assignments:
            #print(f'CHECK: {assignment}')
            if all(assignment.check_negation(tabu) for tabu in forbidden_assignments):
                answer.assignments.add(assignment)
            else:
                if DEBUG: print(f'Excluding {assignment}')
        if DEBUG: print(f'AND result = {answer}')
        return bool(answer.assignments)
