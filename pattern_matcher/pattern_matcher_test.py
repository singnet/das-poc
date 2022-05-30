import pytest
from copy import deepcopy
from stub_db import StubDB
from pattern_matcher import CompatibilityStatus, OrderedAssignment, UnorderedAssignment, PatternMatchingAnswer, LogicalExpression, Variable, Node, Link

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

def test_ordered_assignment_sets():

    va1 = OrderedAssignment()
    va2 = OrderedAssignment()
    va3 = OrderedAssignment()
    va1.assign('v1', '1')
    va1.assign('v2', '2')
    va2.assign('v2', '2')
    va2.assign('v1', '1')
    va3.assign('v1', '2')
    va3.assign('v2', '1')

    with pytest.raises(Exception):
        s1 = set([va1, va2])
    with pytest.raises(Exception):
        s2 = set([va1, va3])

    va1.freeze()
    va2.freeze()
    va3.freeze()
    s1 = set([va1, va2])
    s2 = set([va1, va3])
    assert len(s1) == 1
    assert len(s2) == 2
    assert va1 in s1 and va2 in s1 and va3 not in s1
    assert va1 in s2 and va2 in s2 and va3 in s2

def test_unordered_assignment_sets():

    va1 = UnorderedAssignment()
    va2 = UnorderedAssignment()
    va3 = UnorderedAssignment()
    va4 = UnorderedAssignment()
    va5 = UnorderedAssignment()
    va6 = UnorderedAssignment()
    va7 = UnorderedAssignment()
    va1.assign('v1', '1')
    va1.assign('v2', '2')
    va2.assign('v2', '2')
    va2.assign('v1', '1')
    va3.assign('v1', '2')
    va3.assign('v2', '1')
    va4.assign('v1', '1')
    va4.assign('v2', '2')
    va4.assign('v3', '3')
    va5.assign('v1', '2')
    va5.assign('v1', '1')
    va5.assign('v2', '1')
    va6.assign('v1', '1')
    va6.assign('v1', '1')
    va6.assign('v2', '2')
    va7.assign('v1', '1')
    va7.assign('v2', '2')
    va7.assign('v3', '2')

    with pytest.raises(Exception):
        s1 = set([va1, va2])
    with pytest.raises(Exception):
        s2 = set([va1, va3])
    with pytest.raises(Exception):
        s3 = set([va1, va4])
    with pytest.raises(Exception):
        s4 = set([va1, va5])
    with pytest.raises(Exception):
        s5 = set([va1, va6])

    assert(va1.freeze())
    assert(va2.freeze())
    assert(va3.freeze())
    assert(va4.freeze())
    assert(va5.freeze())
    assert(va6.freeze())
    assert(not va7.freeze())
    s1 = set([va1, va2])
    s2 = set([va1, va3])
    s3 = set([va1, va4])
    s4 = set([va1, va5])
    s5 = set([va1, va6])
    s6 = set([va5, va6])
    assert len(s1) == 1
    assert len(s2) == 1
    assert va1 in s1 and va2 in s1 and va3 in s1 and va4 not in s1 and va5 not in s1
    assert va1 in s2 and va2 in s2 and va3 in s2 and va4 not in s2 and va5 not in s2
    assert len(s3) == 2
    assert len(s4) == 2
    assert len(s5) == 2
    assert va1 in s3 and va2 in s3 and va3 in s3 and va4 in s3 and va5 not in s3
    assert va1 in s4 and va2 in s4 and va3 in s4 and va4 not in s4 and va5 in s4
    assert va1 in s5 and va2 in s5 and va3 in s5 and va4 not in s5 and va5 in s5
    assert len(s6) == 1
    assert va1 not in s6 and va2 not in s6 and va3 not in s6 and va4 not in s6 and va5 in s6 and va6 in s6

def _build_ordered_assignment(d):
    answer = OrderedAssignment()
    for key in d.keys():
        assert answer.assign(key, d[key])
    answer.freeze()
    return answer

def _build_unordered_assignment(d):
    answer = UnorderedAssignment()
    for key in d.keys():
        answer.assign(key, d[key])
    assert answer.freeze()
    return answer
            
def test_evaluate_compatibility():

    a1 = _build_ordered_assignment({'v1': '1', 'v2': '2'})
    a2 = _build_ordered_assignment({'v1': '1', 'v2': '2'})
    a3 = _build_ordered_assignment({'v1': '1', 'v2': '2', 'v3': '3'})
    a4 = _build_ordered_assignment({'v1': '1'})
    a5 = _build_ordered_assignment({'v3': '3', 'v2': '2'})
    a6 = _build_ordered_assignment({'v3': '3', 'v4': '4'})
    a7 = _build_ordered_assignment({'v4': '1', 'v5': '2', 'v6': '3'})
    a8 = _build_ordered_assignment({'v4': '1', 'v5': '2', 'v1': '3'})

    assert(a1.evaluate_compatibility(a2) == CompatibilityStatus.EQUAL)
    assert(a1.evaluate_compatibility(a3) == CompatibilityStatus.SECOND_COVERS_FIRST)
    assert(a3.evaluate_compatibility(a1) == CompatibilityStatus.FIRST_COVERS_SECOND)
    assert(a4.evaluate_compatibility(a2) == CompatibilityStatus.SECOND_COVERS_FIRST)
    assert(a2.evaluate_compatibility(a4) == CompatibilityStatus.FIRST_COVERS_SECOND)
    assert(a2.evaluate_compatibility(a5) == CompatibilityStatus.NO_COVERING)
    assert(a5.evaluate_compatibility(a2) == CompatibilityStatus.NO_COVERING)
    assert(a2.evaluate_compatibility(a6) == CompatibilityStatus.NO_COVERING)
    assert(a6.evaluate_compatibility(a2) == CompatibilityStatus.NO_COVERING)
    assert(a3.evaluate_compatibility(a7) == CompatibilityStatus.NO_COVERING)
    assert(a3.evaluate_compatibility(a8) == CompatibilityStatus.INCOMPATIBLE)
    assert(a8.evaluate_compatibility(a4) == CompatibilityStatus.INCOMPATIBLE)

def test_join():
    
    a1 = _build_ordered_assignment({'v1': '1', 'v2': '2'})
    a2 = _build_ordered_assignment({'v1': '1'})
    a3 = _build_ordered_assignment({'v1': '1', 'v2': '2'})
    a4 = _build_ordered_assignment({'v1': '1', 'v2': '2', 'v3': '3'})
    a5 = _build_ordered_assignment({'v2': '2', 'v3': '3'})
    a6 = _build_ordered_assignment({'v3': '3', 'v4': '4'})
    a7 = _build_ordered_assignment({'v1': '2', 'v2': '1'})

    r = _build_ordered_assignment({'v1': '1', 'v2': '2'})
    assert(a1.evaluate_compatibility(a1.join(a2)) == CompatibilityStatus.EQUAL)
    assert(r.evaluate_compatibility(a1.join(a2)) == CompatibilityStatus.EQUAL)
    assert(r.evaluate_compatibility(a1.join(a3)) == CompatibilityStatus.EQUAL)
    assert(r.evaluate_compatibility(a2.join(a1)) == CompatibilityStatus.EQUAL)
    assert(r.evaluate_compatibility(a3.join(a1)) == CompatibilityStatus.EQUAL)
    r = _build_ordered_assignment({'v1': '1', 'v2': '2', 'v3': '3'})
    assert(a4.evaluate_compatibility(a1.join(a4)) == CompatibilityStatus.EQUAL)
    assert(r.evaluate_compatibility(a1.join(a4)) == CompatibilityStatus.EQUAL)
    assert(r.evaluate_compatibility(a1.join(a5)) == CompatibilityStatus.EQUAL)
    assert(r.evaluate_compatibility(a4.join(a1)) == CompatibilityStatus.EQUAL)
    assert(r.evaluate_compatibility(a5.join(a1)) == CompatibilityStatus.EQUAL)
    r = _build_ordered_assignment({'v1': '1', 'v2': '2', 'v3': '3', 'v4': '4'})
    assert(r.evaluate_compatibility(a1.join(a6)) == CompatibilityStatus.EQUAL)
    assert(r.evaluate_compatibility(a6.join(a1)) == CompatibilityStatus.EQUAL)
    assert(a1.join(a7) is None)
    assert(a7.join(a1) is None)

    j1 = a1.join(_build_unordered_assignment({'v1': '1', 'v2': '2', 'v3': '3'}))
    assert(a1.evaluate_compatibility(j1.ordered_mappings) == CompatibilityStatus.EQUAL)
    assert(not j1.contains_ordered(_build_ordered_assignment({'v1': '1'})))
    assert(not j1.contains_ordered(_build_ordered_assignment({'v2': '2'})))
    assert(j1.contains_ordered(_build_ordered_assignment({'v3': '3'})))
    assert(not j1.contains_ordered(a1))
    assert(not j1.contains_ordered(a2))
    assert(not j1.contains_ordered(a4))

    b1 = _build_unordered_assignment({'v1': '1', 'v2': '2', 'v3': '3'})
    b2 = _build_unordered_assignment({'v1': '1', 'v2': '2'})
    j2 = b1.join(b2)
    assert(j2.ordered_mappings is None)
    assert(j2.contains_ordered(_build_ordered_assignment({'v1': '1'})))
    assert(j2.contains_ordered(_build_ordered_assignment({'v2': '2'})))
    assert(not j2.contains_ordered(_build_ordered_assignment({'v3': '3'})))
    assert(j2.contains_ordered(a1))
    assert(j2.contains_ordered(a2))
    assert(not j2.contains_ordered(a4))
    assert(not j2.contains_ordered(a5))
    assert(not j2.contains_ordered(a6))
    assert(j2.contains_ordered(a7))

    a1 = _build_ordered_assignment({'v1': 'c', 'v3': 'm', 'v2': 'h'})
    b1 = _build_unordered_assignment({'v1': 'h', 'v2': 'c'})
    b2 = _build_unordered_assignment({'v1': 'c', 'v2': 'h'})
    j1 = a1.join(b1)
    j2 = a1.join(b2)
    assert(not j1.unordered_mappings)
    assert(j1.contains_ordered(a1))
    assert(not j2.unordered_mappings)
    assert(j2.contains_ordered(a1))

def test_check_negation():

    a1 = _build_ordered_assignment({'v1': '1', 'v2': '2'})
    a2 = _build_ordered_assignment({'v1': '1'})
    a3 = _build_ordered_assignment({'v1': '2', 'v2': '1'})
    a4 = _build_ordered_assignment({'v1': '1', 'v2': '2', 'v3': '3'})
    a5 = _build_ordered_assignment({'v1': '1', 'v2': '4'})
    a6 = _build_ordered_assignment({'v4': '1', 'v2': '2'})
    a7 = _build_ordered_assignment({'v4': '1', 'v5': '2', 'v6': '3'})
    a8 = _build_ordered_assignment({'v1': '4', 'v2': '5', 'v3': '6'})
    a9 = _build_ordered_assignment({'v1': '1', 'v2': '1'})
    a10 = _build_ordered_assignment({'v1': '1', 'v2': '1', 'v4': '2'})

    b1 = _build_unordered_assignment({'v1': '1', 'v2': '2', 'v3': '3'})

    assert(not a1.check_negation(a1))
    assert(not a1.check_negation(a2))
    assert(a1.check_negation(a3))
    assert(a2.check_negation(a1))
    assert(not a2.check_negation(a2))
    assert(a2.check_negation(a3))
    assert(a3.check_negation(a1))
    assert(a3.check_negation(a2))
    assert(not a3.check_negation(a3))

    assert(not a1.check_negation(b1))
    assert(not a2.check_negation(b1))
    assert(not a3.check_negation(b1))
    assert(not a4.check_negation(b1))
    assert(a5.check_negation(b1))
    assert(a6.check_negation(b1))
    assert(a7.check_negation(b1))
    assert(a8.check_negation(b1))
    assert(not a9.check_negation(b1))
    assert(a10.check_negation(b1))

    assert(not deepcopy(b1).check_negation(a1))
    assert(not deepcopy(b1).check_negation(a2))
    assert(not deepcopy(b1).check_negation(a3))
    assert(not deepcopy(b1).check_negation(a4))
    assert(deepcopy(b1).check_negation(a5))
    assert(deepcopy(b1).check_negation(a6))
    assert(deepcopy(b1).check_negation(a7))
    assert(deepcopy(b1).check_negation(a8))
    assert(deepcopy(b1).check_negation(a9))
    assert(deepcopy(b1).check_negation(a10))

def _test_patterns():

    def check_pattern(db, pattern, expected_match, assignments):
        answer: PatternMatchingAnswer = PatternMatchingAnswer()
        assert expected_match == pattern.matched(db, answer)
        if expected_match:
            assert len(answer.assignments) == len(assignments)
            l1 = sorted([sorted([f'{x}={y}' for x, y in a.mapping.items()]) for a in answer.assignments])
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
