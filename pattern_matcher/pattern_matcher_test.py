import pytest
from stub_db import StubDB
from pattern_matcher import _AssignmentCompatibilityStatus, _evaluate_compatibility, VariablesAssignment, PatternMatchingAnswer, LogicalExpression, Variable, Node, Link

def test_basic_matching():

    db: StubDB = StubDB()
    answer: PatternMatchingAnswer = PatternMatchingAnswer()

    # Nodes
    assert Node("Concept", "mammal").matched(db, answer)
    assert not Node("Concept", "blah").matched(db, answer)
    assert not Node("blah", "mammal").matched(db, answer)
    assert not Node("blah", "blah").matched(db, answer)

    # Asymmetric links
    assert Link("Inheritance", [Node("Concept", "human"), Node("Concept", "mammal")], True).matched(db, answer)
    assert not Link("Similarity", [Node("Concept", "human"), Node("Concept", "mammal")], False).matched(db, answer)
    assert not Link("blah", [Node("Concept", "human"), Node("Concept", "mammal")], True).matched(db, answer)
    assert not Link("Inheritance", [Node("Concept", "mammal"), Node("Concept", "human")], True).matched(db, answer)

    # Symmetric links
    assert Link("Similarity", [Node("Concept", "snake"), Node("Concept", "earthworm")], False).matched(db, answer)
    assert Link("Similarity", [Node("Concept", "earthworm"), Node("Concept", "snake")], False).matched(db, answer)
    assert not Link("Similarity", [Node("Concept", "earthworm"), Node("Concept", "vine")], False).matched(db, answer)
    assert not Link("Similarity", [Node("Concept", "vine"), Node("Concept", "earthworm")], False).matched(db, answer)
    assert Link("Similarity", [Node("Concept", "snake"), Node("Concept", "vine")], False).matched(db, answer)
    assert Link("Similarity", [Node("Concept", "vine"), Node("Concept", "snake")], False).matched(db, answer)
    assert not Link("Similarity", [Node("Concept", "vine")], False).matched(db, answer)
    assert not Link("Similarity", [Node("Concept", "snake")], False).matched(db, answer)
    assert not Link("Similarity", [Node("Concept", "blah"), Node("Concept", "snake"), Node("Concept", "vine")], False).matched(db, answer)
    assert not Link("Similarity", [Node("Concept", "vine"), Node("Concept", "snake"), Node("Concept", "blah")], False).matched(db, answer)
    assert not Link("Similarity", [Node("Concept", "snake"), Node("blah", "earthworm")], False).matched(db, answer)
    assert not Link("Similarity", [Node("blah", "snake"), Node("Concept", "earthworm")], False).matched(db, answer)
    assert not Link("Similarity", [Node("blah", "earthworm"), Node("Concept", "snake")], False).matched(db, answer)

    # Nested links
    assert Link('List', [Link('Inheritance', [Node('Concept', 'dinosaur'), Node('Concept', 'reptile')], True), Link('Inheritance', [Node('Concept', 'triceratops'), Node('Concept', 'dinosaur')], True)], True).matched(db, answer)
    assert not Link('List', [Link('Inheritance', [Node('Concept', 'triceratops'), Node('Concept', 'dinosaur')], True), Link('Inheritance', [Node('Concept', 'dinosaur'), Node('Concept', 'reptile')], True)], True).matched(db, answer)
    assert Link('Set', [Link('Inheritance', [Node('Concept', 'dinosaur'), Node('Concept', 'reptile')], True), Link('Inheritance', [Node('Concept', 'triceratops'), Node('Concept', 'dinosaur')], True)], False).matched(db, answer)
    assert Link('Set', [Link('Inheritance', [Node('Concept', 'triceratops'), Node('Concept', 'dinosaur')], True), Link('Inheritance', [Node('Concept', 'dinosaur'), Node('Concept', 'reptile')], True)], False).matched(db, answer)
    assert not Link('List', [Link('Inheritance', [Node('Concept', 'blah'), Node('Concept', 'reptile')], True), Link('Inheritance', [Node('Concept', 'triceratops'), Node('Concept', 'dinosaur')], True)], True).matched(db, answer)
    assert not Link('List', [Link('blah', [Node('Concept', 'dinosaur'), Node('Concept', 'reptile')], True), Link('Inheritance', [Node('Concept', 'triceratops'), Node('Concept', 'dinosaur')], True)], True).matched(db, answer)
    assert not Link('Set', [Link('blah', [Node('Concept', 'dinosaur'), Node('Concept', 'reptile')], True), Link('Inheritance', [Node('Concept', 'triceratops'), Node('Concept', 'dinosaur')], True)], False).matched(db, answer)
    assert not Link('Set', [Link('Inheritance', [Node('blah', 'dinosaur'), Node('Concept', 'reptile')], True), Link('Inheritance', [Node('Concept', 'triceratops'), Node('Concept', 'dinosaur')], False)], False).matched(db, answer)
    assert not Link('Set', [Link('Inheritance', [Node('Concept', 'dinosaur'), Node('Concept', 'blah')], True), Link('Inheritance', [Node('Concept', 'triceratops'), Node('Concept', 'dinosaur')], False)], False).matched(db, answer)

    # Variables
    animal = Node('Concept', 'animal')
    mammal = Node('Concept', 'mammal')
    human = Node('Concept', 'human')
    chimp = Node('Concept', 'chimp')
    monkey = Node('Concept', 'monkey')
    ent = Node('Concept', 'ent')
    assert Link('Inheritance', [human, mammal], True).matched(db, answer)
    assert Link('Inheritance', [monkey, mammal], True).matched(db, answer)
    assert Link('Inheritance', [chimp, mammal], True).matched(db, answer)
    assert Link('Similarity', [human, monkey], False).matched(db, answer)
    assert Link('Similarity', [chimp, monkey], False).matched(db, answer)
    assert Link('Inheritance', [Variable('V1'), mammal], True).matched(db, answer)
    assert Link('Inheritance', [Variable('V1'), Variable('V2')], True).matched(db, answer)
    assert not Link('Inheritance', [Variable('V1'), Variable('V1')], True).matched(db, answer)
    assert Link('Inheritance', [Variable('V2'), Variable('V1')], True).matched(db, answer)
    assert Link('Inheritance', [mammal, Variable('V1')], True).matched(db, answer)
    assert not Link('Inheritance', [animal, Variable('V1')], True).matched(db, answer)
    assert Link('Similarity', [Variable('V1'), Variable('V2')], False).matched(db, answer)
    assert Link('Similarity', [human, Variable('V1')], False).matched(db, answer)
    assert Link('Similarity', [Variable('V1'), human], False).matched(db, answer)
    assert Link('List', [human, ent, Variable('V1'), Variable('V2')], True).matched(db, answer)
    assert not Link('List', [human, Variable('V1'), Variable('V2'), ent], True).matched(db, answer)
    assert not Link('List', [ent, Variable('V1'), Variable('V2'), human], True).matched(db, answer)
    assert Link('Set', [human, ent, Variable('V1'), Variable('V2')], False).matched(db, answer)
    assert Link('Set', [human, Variable('V1'), Variable('V2'), ent], False).matched(db, answer)
    assert Link('Set', [ent, Variable('V1'), Variable('V2'), human], False).matched(db, answer)
    assert Link('Set', [monkey, Variable('V1'), Variable('V2'), chimp], False).matched(db, answer)

def test_evaluate_compatibility():

    def build_assignment(d):
        answer = VariablesAssignment()
        for key in d.keys():
            answer.assign(key, d[key])
        answer.freeze_assignment()
        return answer
            
    a1 = build_assignment({'v1': 1, 'v2': 2})
    a2 = build_assignment({'v1': 1, 'v2': 2})
    a3 = build_assignment({'v1': 1, 'v2': 2, 'v3': 3})
    a4 = build_assignment({'v1': 1})
    a5 = build_assignment({'v3': 3, 'v2': 2})
    a6 = build_assignment({'v3': 3, 'v4': 4})
    a7 = build_assignment({'v4': 1, 'v5': 2, 'v6': 3})
    a8 = build_assignment({'v4': 1, 'v5': 2, 'v1': 3})

    assert(_evaluate_compatibility(a1, a2) == _AssignmentCompatibilityStatus.EQUAL)
    assert(_evaluate_compatibility(a1, a3) == _AssignmentCompatibilityStatus.SECOND_COVERS_FIRST)
    assert(_evaluate_compatibility(a3, a1) == _AssignmentCompatibilityStatus.FIRST_COVERS_SECOND)
    assert(_evaluate_compatibility(a4, a2) == _AssignmentCompatibilityStatus.SECOND_COVERS_FIRST)
    assert(_evaluate_compatibility(a2, a4) == _AssignmentCompatibilityStatus.FIRST_COVERS_SECOND)
    assert(_evaluate_compatibility(a2, a5) == _AssignmentCompatibilityStatus.NO_COVERING)
    assert(_evaluate_compatibility(a5, a2) == _AssignmentCompatibilityStatus.NO_COVERING)
    assert(_evaluate_compatibility(a2, a6) == _AssignmentCompatibilityStatus.NO_COVERING)
    assert(_evaluate_compatibility(a6, a2) == _AssignmentCompatibilityStatus.NO_COVERING)
    assert(_evaluate_compatibility(a3, a7) == _AssignmentCompatibilityStatus.NO_COVERING)
    assert(_evaluate_compatibility(a3, a8) == _AssignmentCompatibilityStatus.INCOMPATIBLE)
    assert(_evaluate_compatibility(a8, a4) == _AssignmentCompatibilityStatus.INCOMPATIBLE)

def test_patterns():

    def check_pattern(db, pattern, expected_match, assignments):
        answer: PatternMatchingAnswer = PatternMatchingAnswer()
        assert expected_match == pattern.matched(db, answer)
        if expected_match:
            assert len(answer.assignments) == len(assignments)
            l1 = sorted([sorted([f'{x}={y}' for x, y in a.assignment.items()]) for a in answer.assignments])
            l2 = sorted([sorted([f'{x}={y}' for x, y in d.items()]) for d in assignments])
            print(f'l1 = {l1}\nl2 = {l2}')
            assert l1 == l2

    db: StubDB = StubDB()
    animal = Node('Concept', 'animal')
    mammal = Node('Concept', 'mammal')
    human = Node('Concept', 'human')
    chimp = Node('Concept', 'chimp')
    monkey = Node('Concept', 'monkey')
    ent = Node('Concept', 'ent')

    check_pattern(db,
        Link('Inheritance', [Variable('V1'), mammal], True),
        True,
        [
            {'V1': '<Concept: chimp>'},
            {'V1': '<Concept: monkey>'},
            {'V1': '<Concept: rhino>'},
            {'V1': '<Concept: human>'}
        ]
    )

    check_pattern(db,
        Link('Similarity', [Variable('V1'), human], False),
        True,
        [
            {'V1': '<Concept: chimp>'},
            {'V1': '<Concept: monkey>'},
            {'V1': '<Concept: ent>'}
        ]
    )

