import time
from abc import ABC, abstractmethod
from copy import deepcopy
from enum import Enum, auto
from functools import cmp_to_key
from typing import Dict, FrozenSet, List, Optional, Set, Union

from das.pattern_matcher.db_interface import DBInterface

WILDCARD = '*'
DEBUG = True

CONFIG = {
    # Enforce different values for different variables in ordered assignments
    'no_overload': False, # Enforce different values for different variables in ordered assignments
}

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
    def join(self, other: 'Assignment') -> 'Assignment':
        pass

    @abstractmethod
    def check_negation(self, negation: 'Assignment') -> bool:
        pass

class OrderedAssignment(Assignment):
    """
    TODO: documentation
    """

    def __init__(self):
        super().__init__()
        self.mapping: Dict[str, str] = {}
        self.values: Union[Set[str], FrozenSet] = set()

    def __repr__(self):
        return self.mapping.__repr__()

    def freeze(self):
        assert super().freeze()
        self.values = frozenset(self.values)
        self.hash = hash(frozenset(self.mapping.items()))
        return True

    def assign(self, variable: str, value: str) -> bool:
        if variable is None or value is None or self.frozen:
            raise ValueError(f'Invalid assignment: variable = {variable} value = {value} frozen = {self.frozen}')
        if variable in self.variables:
            return self.mapping[variable] == value
        else:
            if CONFIG['no_overload'] and value in self.values:
                return False
            self.variables.add(variable)
            self.values.add(value)
            self.mapping[variable] = value
            return True

    def join(self, other: Assignment) -> Assignment:
        assert self.frozen and other.frozen
        if isinstance(other, OrderedAssignment):
            return self._join_ordered(other)
        else:
            return other.join(self)

    def check_negation(self, negation: Assignment) -> bool:
        if isinstance(negation, OrderedAssignment):
            check = self.evaluate_compatibility(negation)
            return check != CompatibilityStatus.EQUAL and check != CompatibilityStatus.FIRST_COVERS_SECOND
        else:
            return not negation.is_covered_by_ordered(self)

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
                if not answer.assign(variable, value):
                    return None
            for variable, value in other.mapping.items():
                if not answer.assign(variable, value):
                    return None
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

    def compatible(self, other) -> bool:
        return self.evaluate_compatibility(other) != CompatibilityStatus.INCOMPATIBLE

class UnorderedAssignment(Assignment):
    """
    TODO: documentation
    """
    def __init__(self):
        super().__init__()
        self.symbols: Dict[str, int] = {}
        self.values: Dict[str, int] = {}

    #def __repr__(self):
    #    return self.symbols.__repr__() + ' ' + self.values.__repr__()

    def __repr__(self):
        symbols = [] 
        for key in self.symbols: 
            for i in range(self.symbols[key]): 
                symbols.append(key) 
        values = [] 
        for key in self.values: 
            for i in range(self.values[key]): 
                values.append(key) 
        mapping = {}
        for symbol, value in zip(symbols, values): 
            mapping[symbol] = value
        return '*' + mapping.__repr__()

    def freeze(self):
        assert super().freeze()
        symbols_count = tuple(sorted(self.symbols.values()))
        values_count = tuple(sorted(self.values.values()))
        if symbols_count != values_count:
            return False
        self.hash = hash(tuple([hash(frozenset(self.symbols.items())), hash(frozenset(self.values.items()))]))
        return True

    def assign(self, variable: str, value: str) -> bool:
        if variable is None or value is None or self.frozen:
            raise ValueError(f'Invalid assignment: variable = {variable} value = {value} frozen = {self.frozen}')
        if variable in self.variables:
            return False
        self.symbols[variable] = self.symbols.get(variable, 0) + 1
        self.values[value] = self.values.get(value, 0) + 1
        self.variables.add(variable)
        return True

    def join(self, other: Assignment) -> Assignment:
        assert self.frozen and other.frozen
        if isinstance(other, CompositeAssignment):
            return other.join(self)
        else:
            composite = CompositeAssignment(self)
            return composite.join(other)

    def check_negation(self, negation: Assignment) -> bool:
        if isinstance(negation, OrderedAssignment):
            return not self.contains_ordered(negation)
        elif isinstance(negation, UnorderedAssignment):
            return not self.contains_unordered(negation)
        else:
            return all(not self.contains_unordered(unordered_negation) for unordered_negation in negation.unordered_mappings)

    def contains_ordered(self, ordered_assignment) -> bool:
        count_values = {}
        for variable, value in ordered_assignment.mapping.items():
            if variable not in self.variables:
                return False
            count_values[value] = count_values.get(value, 0) + 1
        for value in count_values.keys():
            if self.values.get(value, 0) < count_values[value]:
                return False
        return True

    def is_covered_by_ordered(self, ordered_assignment) -> bool:
        copy = deepcopy(self)
        for variable, value in ordered_assignment.mapping.items():
            copy.symbols[variable] = copy.symbols.get(variable, 0) - 1
            copy.values[value] = copy.values.get(value, 0) - 1
        return all(count <= 0 for count in copy.symbols.values()) and all(count <= 0 for count in copy.values.values())
        

    def contains_unordered(self, unordered_assignment) -> bool:
        for symbol, count in unordered_assignment.symbols.items():
            if self.symbols.get(symbol, 0) < count:
                return False
        for value, count in unordered_assignment.values.items():
            if self.values.get(value, 0) < count:
                return False
        return True

    def compatible(self, other) -> bool:
        symbol_intersection = self.variables.intersection(other.variables)
        sum_symbol_count_self = 0
        sum_symbol_count_other = 0
        for variable in symbol_intersection:
            sum_symbol_count_self += self.symbols[variable]
            sum_symbol_count_other += other.symbols[variable]
        values_self = set(self.values.keys())
        values_other = set(other.values.keys())
        value_intersection = values_self.intersection(values_other)
        sum_value_count_self = 0
        sum_value_count_other = 0
        for value in value_intersection:
            sum_value_count_self += self.values[value]
            sum_value_count_other += other.values[value]
        return sum_value_count_other >= sum_symbol_count_self and sum_value_count_self >= sum_symbol_count_other

class CompositeAssignment(Assignment):
    """
    TODO: documentation
    """

    def __init__(self, assignment: UnorderedAssignment):
        super().__init__()
        self.unordered_mappings: List[UnorderedAssignment] = [assignment]
        self.ordered_mapping: OrderedAssignment = None
        self.variables = deepcopy(assignment.variables)
        assert self._freeze()

    def __repr__(self):
        return f'Ordered = {self.ordered_mapping} | Unordered = {self.unordered_mappings}'

    def _freeze(self):
        assert super().freeze()
        assert self.ordered_mapping is None
        _hash = 1
        for unordered in self.unordered_mappings:
            _hash ^= unordered.hash
        self.hash = _hash
        return True

    def freeze(self):
        assert False

    def assign(self, variable: str, value: str) -> bool:
        assert False

    def _check_ordered_viability(self) -> bool:
        #print(f'_check_ordered_viability() self = {self}')
        if not self.ordered_mapping:
            if not self.unordered_mappings:
                return False
            else:
                return True
        for unordered_assignment in self.unordered_mappings:
            if not unordered_assignment.contains_ordered(self.ordered_mapping) and \
               not unordered_assignment.is_covered_by_ordered(self.ordered_mapping):
                return False
        return True

    def _recompute_hash(self) -> None:
        if self.ordered_mapping:
            _hash = self.ordered_mapping.hash
        else:
            _hash = 1
        for unordered_assignment in self.unordered_mappings:
            _hash ^= unordered_assignment.hash
        self.hash = _hash

    def _add_ordered_mapping(self, other: OrderedAssignment) -> bool:
        if self.ordered_mapping is None:
            self.ordered_mapping = other
        else:
            self.ordered_mapping = self.ordered_mapping.join(other)
            if self.ordered_mapping is None:
                return False
        if self._check_ordered_viability():
            self._recompute_hash()
            return True
        else:
            return False

    def _add_unordered_mapping(self, unordered_assignment) -> bool:
        if self.ordered_mapping and not unordered_assignment.contains_ordered(self.ordered_mapping):
            return False
        if any(not assignment.compatible(unordered_assignment) for assignment in self.unordered_mappings):
            return False
        self.unordered_mappings.append(unordered_assignment)
        self._recompute_hash()
        return True

    def _add_unordered_mappings(self, others) -> bool:
        return all(self._add_unordered_mapping(assignment) for assignment in others)

    def join(self, other: Assignment) -> Assignment:
        assert self.frozen and other.frozen
        answer = deepcopy(self)
        if isinstance(other, OrderedAssignment):
            return answer if answer._add_ordered_mapping(other) else None
        elif isinstance(other, UnorderedAssignment):
            return answer if answer._add_unordered_mapping(other) else None
        else:
            if not answer._add_ordered_mapping(other.ordered_mapping):
                return None
            return answer if answer._add_unordered_mappings(other.unordered_mappings) else None

    def check_negation(self, negation: Assignment) -> bool:
        if isinstance(negation, OrderedAssignment):
            return all(not assignment.contains_ordered(negation) for assignment in self.unordered_mappings)
        elif isinstance(negation, UnorderedAssignment):
            return all(not assignment.contains_unordered(negation) for assignment in self.unordered_mappings)
        else:
            for assignment in self.unordered_assignments:
                if all(assignment.contains_unordered(negation_assignment) for negation_assignment in negation.unordered_assignments):
                    return False
            return True

    def contains_ordered(self, ordered_assignment) -> bool:
        return all(assignment.contains_ordered(ordered_assignment) for assignment in self.unordered_mappings)

    def contains_unordered(self, unordered_assignment) -> bool:
        return all(assignment.contains_unordered(unordered_assignment) for assignment in self.unordered_mappings)

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
        return f'{self.atom_type}'

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
        

    def _assign_variables(self, db: DBInterface, link: str, link_targets: List[str]) -> Optional[Assignment]:
        #link_targets = db.get_link_targets(link)
        assert(len(link_targets) == len(self.targets)), f'link_targets = {link_targets} self.targets = {self.targets}'
        answer = None
        if self.ordered:
            answer = OrderedAssignment()
            for atom, handle in zip(self.targets, link_targets):
                if isinstance(atom, Variable):
                    if not answer.assign(atom.name, handle):
                        return None
            return answer if answer.freeze() else None
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
                if not answer.assign(atom.name, handle):
                    return None
            return answer if answer.freeze() else None

    def matched(self, db: DBInterface, answer: PatternMatchingAnswer) -> bool:
        #print('XXXX', 'matched()', f'entering self = {self}')
        if not all(atom.matched(db, answer) for atom in self.targets):
            #print('XXXX', 'matched()', f'leaving 0 self = {self}')
            return False
        #print('XXXX', f'self = {self}')
        target_handles = [atom.get_handle(db) for atom in self.targets]
        #print('XXXX', f'target_handles = {target_handles}')
        if any(handle == WILDCARD for handle in target_handles):
            matched = db.get_matched_links(self.atom_type, target_handles)
            #print('XXXX', f'matched = {matched}')
            #print('XXXX', f'len(matched) = {len(matched)}')
            #answer.assignments = set([asn for asn in [self._assign_variables(db, link) for link in matched] if asn is not None])
            count = 1
            total = len(matched)
            start = time.perf_counter()
            answer.assignments = set()
            for match in matched:
                link = match['handle']
                targets = match['targets'][1:]
                #print('XXXX', f'match = {match}')
                #print('XXXX', f'link = {link}')
                #print('XXXX', f'targets = {targets}')
                asn = self._assign_variables(db, link, targets)
                if asn:
                    answer.assignments.add(asn)
                #if count % 10000 == 0:
                #    print(f'{count}/{total} {time.perf_counter() - start} seconds')
                #    start = time.perf_counter()
                #count += 1
            #print('XXXX', f'len(answer.assignments) = {len(answer.assignments)}')
            #print('XXXX', f'answer.assignments = {answer.assignments}')
            #print('XXXX', 'matched()', f'leaving 1 self = {self}')
            return bool(answer.assignments)
        else:
            #print('XXXX', 'matched()', f'leaving 2 self = {self}')
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

class Or(LogicalExpression):
    """
    TODO: documentation
    """

    def __init__(self, terms: List[LogicalExpression]):
        self.terms = terms

    def __repr__(self):
        return f"OR({self.terms})"

    def matched(self, db: DBInterface, answer: PatternMatchingAnswer) -> bool:
        if not self.terms:
            return False
        assert not answer.assignments
        or_answer = PatternMatchingAnswer()
        or_matched = False
        negative_terms = set()
        for term in self.terms:
            term_answer = PatternMatchingAnswer()
            if isinstance(term, Not):
                if DEBUG: print(f'negative term: {term}')
                negative_terms.add(term)
                continue
            if not term.matched(db, term_answer):
                if DEBUG: print(f'NOT MATCHED: {term}')
                continue
            or_matched = True
            if not term_answer.assignments:
                if DEBUG: print(f'term_answer empty: {term}')
                continue
            if not or_answer.assignments:
                if DEBUG: print(f'First term: {term}')
                if DEBUG: print(f'term_answer:\n{term_answer}')
                or_answer.assignments = term_answer.assignments
                continue
            if DEBUG: print(f'New term: {term}')
            if DEBUG: print(f'term_answer:\n{term_answer}')
            or_answer.assignments.update(term_answer.assignments)
            if DEBUG: print(f'or_answer after extending:\n{or_answer}')
        if negative_terms:
            joint_negative_term = And([t.term for t in negative_terms])
            if DEBUG: print(f'Joint negative term: {joint_negative_term}')
            term_answer = PatternMatchingAnswer()
            joint_negative_term.matched(db, term_answer)
            #print('XXXX', f'term_answer.assignments = {term_answer.assignments}')
            #print('XXXX', f'or_answer.assignments = {or_answer.assignments}')
            answer.assignments = term_answer.assignments - or_answer.assignments
            #print('XXXX', f'answer.assignments = {answer.assignments}')
            answer.negation = True
        else:
            answer.assignments = or_answer.assignments
        if DEBUG: print(f'OR result = {answer}')
        return or_matched

class And(LogicalExpression):
    """
    TODO: documentation
    """

    def __init__(self, terms: List[LogicalExpression]):
        self.terms = terms

    def __repr__(self):
        return f'AND({self.terms})'

    def post_process(self, assignment) -> Assignment:
        if not isinstance(assignment, CompositeAssignment):
            return assignment
        return assignment

    def matched(self, db: DBInterface, answer: PatternMatchingAnswer) -> bool:
        if not self.terms:
            return False
        assert not answer.assignments
        and_answer = PatternMatchingAnswer()
        forbidden_assignments = set()
        for term in self.terms:
            term_answer = PatternMatchingAnswer()
            if not term.matched(db, term_answer):
                if DEBUG: print(f'NOT MATCHED: {term}')
                return False
            if not term_answer.assignments:
                if DEBUG: print(f'term_answer empty: {term}')
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
                answer.assignments.add(self.post_process(assignment))
            else:
                if DEBUG: print(f'Excluding {assignment}')
        if DEBUG: print(f'AND result = {answer}')
        return bool(answer.assignments)
