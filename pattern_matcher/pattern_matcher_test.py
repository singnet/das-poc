import pytest
from stub_db import StubDB
from pattern_matcher import PatternMatchingAnswer, LogicalExpression, Node, Link


def test_basic_matches():

    db: StubDB = StubDB()
    answer: PatternMatchingAnswer = PatternMatchingAnswer()

    # Nodes
    assert Node("Concept", "mammal").matched(db, answer)
    assert not Node("Concept", "blah").matched(db, answer)
    assert not Node("blah", "mammal").matched(db, answer)
    assert not Node("blah", "blah").matched(db, answer)

    # Asymmetric links
    assert Link("Inheritance", [Node("Concept", "human"), Node("Concept", "mammal")]).matched(db, answer)
    assert not Link("Similarity", [Node("Concept", "human"), Node("Concept", "mammal")]).matched(db, answer)
    assert not Link("blah", [Node("Concept", "human"), Node("Concept", "mammal")]).matched(db, answer)
    assert not Link("Inheritance", [Node("Concept", "mammal"), Node("Concept", "human")]).matched(db, answer)

    # Symmetric links
    assert Link("Similarity", [Node("Concept", "snake"), Node("Concept", "earthworm")]).matched(db, answer)
    assert Link("Similarity", [Node("Concept", "earthworm"), Node("Concept", "snake")]).matched(db, answer)
    assert not Link("Similarity", [Node("Concept", "earthworm"), Node("Concept", "vine")]).matched(db, answer)
    assert not Link("Similarity", [Node("Concept", "vine"), Node("Concept", "earthworm")]).matched(db, answer)
    assert Link("Similarity", [Node("Concept", "snake"), Node("Concept", "vine")]).matched(db, answer)
    assert Link("Similarity", [Node("Concept", "vine"), Node("Concept", "snake")]).matched(db, answer)
    assert not Link("Similarity", [Node("Concept", "vine")]).matched(db, answer)
    assert not Link("Similarity", [Node("Concept", "snake")]).matched(db, answer)
    assert not Link("Similarity", [Node("Concept", "blah"), Node("Concept", "snake"), Node("Concept", "vine")]).matched(db, answer)
    assert not Link("Similarity", [Node("Concept", "vine"), Node("Concept", "snake"), Node("Concept", "blah")]).matched(db, answer)
    assert not Link("Similarity", [Node("Concept", "snake"), Node("blah", "earthworm")]).matched(db, answer)
    assert not Link("Similarity", [Node("blah", "snake"), Node("Concept", "earthworm")]).matched(db, answer)
    assert not Link("Similarity", [Node("blah", "earthworm"), Node("Concept", "snake")]).matched(db, answer)

    # Nested links
    assert Link('List', [Link('Inheritance', [Node('Concept', 'dinosaur'), Node('Concept', 'reptile')]), Link('Inheritance', [Node('Concept', 'triceratops'), Node('Concept', 'dinosaur')])]).matched(db, answer)
    assert not Link('List', [Link('Inheritance', [Node('Concept', 'triceratops'), Node('Concept', 'dinosaur')]), Link('Inheritance', [Node('Concept', 'dinosaur'), Node('Concept', 'reptile')])]).matched(db, answer)
    assert Link('Set', [Link('Inheritance', [Node('Concept', 'dinosaur'), Node('Concept', 'reptile')]), Link('Inheritance', [Node('Concept', 'triceratops'), Node('Concept', 'dinosaur')])]).matched(db, answer)
    assert Link('Set', [Link('Inheritance', [Node('Concept', 'triceratops'), Node('Concept', 'dinosaur')]), Link('Inheritance', [Node('Concept', 'dinosaur'), Node('Concept', 'reptile')])]).matched(db, answer)
    assert not Link('List', [Link('Inheritance', [Node('Concept', 'blah'), Node('Concept', 'reptile')]), Link('Inheritance', [Node('Concept', 'triceratops'), Node('Concept', 'dinosaur')])]).matched(db, answer)
    assert not Link('List', [Link('blah', [Node('Concept', 'dinosaur'), Node('Concept', 'reptile')]), Link('Inheritance', [Node('Concept', 'triceratops'), Node('Concept', 'dinosaur')])]).matched(db, answer)
    assert not Link('Set', [Link('blah', [Node('Concept', 'dinosaur'), Node('Concept', 'reptile')]), Link('Inheritance', [Node('Concept', 'triceratops'), Node('Concept', 'dinosaur')])]).matched(db, answer)
    assert not Link('Set', [Link('Inheritance', [Node('blah', 'dinosaur'), Node('Concept', 'reptile')]), Link('Inheritance', [Node('Concept', 'triceratops'), Node('Concept', 'dinosaur')])]).matched(db, answer)
    assert not Link('Set', [Link('Inheritance', [Node('Concept', 'dinosaur'), Node('Concept', 'blah')]), Link('Inheritance', [Node('Concept', 'triceratops'), Node('Concept', 'dinosaur')])]).matched(db, answer)
