import pytest
from stub_db import StubDB
from pattern_matcher import PatternMatchingAnswer, LogicalExpression, Variable, Node, Link


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


def test_patterns():

    def check_pattern(db, pattern, expected_match, assignments):
        answer: PatternMatchingAnswer = PatternMatchingAnswer()
        assert expected_match == pattern.matched(db, answer)
        if expected_match:
            assert len(answer.variables_mappings) == len(assignments)
            l1 = sorted([sorted([f'{x}={y}' for x, y in a.assignment.items()]) for a in answer.variables_mappings])
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

