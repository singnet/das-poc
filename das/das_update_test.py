import tempfile
import shutil
import os
import pytest
from das.distributed_atom_space import DistributedAtomSpace, WILDCARD
from das.database.db_interface import UNORDERED_LINK_TYPES

das = DistributedAtomSpace()
transaction = das.open_transaction()
transaction.add_toplevel_expression('(: "human" Concept)')
transaction.add_toplevel_expression('(Similarity "human" "monkey")')
transaction.add_toplevel_expression('(: "gorilla" Concept)')
transaction.add_toplevel_expression('(Inheritance "gorilla" "mammal")')
transaction.add_toplevel_expression('(Similarity "gorilla" "human")')
transaction.add_toplevel_expression('(Similarity "gorilla" "monkey")')
transaction.add_toplevel_expression('(Similarity "gorilla" "chimp")')
transaction.add_toplevel_expression('(Similarity "human" "gorilla")')
transaction.add_toplevel_expression('(Similarity "monkey" "gorilla")')
transaction.add_toplevel_expression('(Similarity "chimp" "gorilla")')
das.commit_transaction(transaction)

similarity = "Similarity"
inheritance = "Inheritance"
concept = "Concept"
animal = das.get_node(concept, "animal")
mammal = das.get_node(concept, "mammal")
reptile = das.get_node(concept, "reptile")
plant = das.get_node(concept, "plant")
human = das.get_node(concept, "human")
monkey = das.get_node(concept, "monkey")
chimp = das.get_node(concept, "chimp")
gorilla = das.get_node(concept, "gorilla")
earthworm = das.get_node(concept, "earthworm")
snake = das.get_node(concept, "snake")
triceratops = das.get_node(concept, "triceratops")
rhino = das.get_node(concept, "rhino")
vine = das.get_node(concept, "vine")
ent = das.get_node(concept, "ent")
dinosaur = das.get_node(concept, "dinosaur")
all_similarities = [
    set([human, monkey]),
    set([human, chimp]),
    set([chimp, monkey]),
    set([human, gorilla]),
    set([monkey, gorilla]),
    set([chimp, gorilla]),
    set([earthworm, snake]),
    set([triceratops, rhino]),
    set([vine, snake]),
    set([ent, human]),
]
all_inheritances = [
    [human, mammal],
    [monkey, mammal],
    [chimp, mammal],
    [gorilla, mammal],
    [mammal, animal],
    [reptile, animal],
    [snake, reptile],
    [dinosaur, reptile],
    [triceratops, dinosaur],
    [earthworm, animal],
    [rhino, mammal],
    [vine, plant],
    [ent, plant],
]

template_similarity = [similarity, concept, concept]
template_inheritance = [inheritance, concept, concept]

def test_get_node():
    human_document = das.get_node(concept, "human", build_node_dict=True)
    assert human_document["handle"] == human
    assert human_document["type"] == concept
    assert human_document["name"] == "human"

    gorilla_document = das.get_node(concept, "gorilla", build_node_dict=True)
    assert gorilla_document["handle"] == gorilla
    assert gorilla_document["type"] == concept
    assert gorilla_document["name"] == "gorilla"

def test_get_link():
    link_handle = das.get_link(similarity, [human, monkey])
    link = das.get_link(similarity, [human, monkey], build_link_dict = True)
    assert link["handle"] == link_handle
    assert link["type"] == similarity
    assert link["template"] == template_similarity

    link_handle = das.get_link(similarity, [human, gorilla])
    link = das.get_link(similarity, [human, gorilla], build_link_dict = True)
    assert link["handle"] == link_handle
    assert link["type"] == similarity
    assert link["template"] == template_similarity

    link_handle = das.get_link(similarity, [gorilla, monkey])
    link = das.get_link(similarity, [gorilla, monkey], build_link_dict = True)
    assert link["handle"] == link_handle
    assert link["type"] == similarity
    assert link["template"] == template_similarity

    link_handle = das.get_link(similarity, [gorilla, chimp])
    link = das.get_link(similarity, [gorilla, chimp], build_link_dict = True)
    assert link["handle"] == link_handle
    assert link["type"] == similarity
    assert link["template"] == template_similarity

    link_handle = das.get_link(inheritance, [gorilla, mammal])
    link = das.get_link(inheritance, [gorilla, mammal], build_link_dict = True)
    assert link["handle"] == link_handle
    assert link["type"] == inheritance
    assert link["template"] == template_inheritance

def test_get_links_with_link_templates():
    link_handles = das.get_links(link_type=similarity, target_types=[concept, concept])
    links = das.get_links(link_type=similarity, target_types=[concept, concept], build_link_dict=True)
    assert len(link_handles) == len(links)
    for link in links:
        assert link["handle"] in link_handles
        assert link["type"] == similarity
        assert link["template"] == template_similarity
        assert set(link["targets"]) in all_similarities

def _check_pattern(link_type, targets, expected):
    link_handles = das.get_links(link_type=link_type, targets=targets)
    links = das.get_links(link_type=link_type, targets=targets, build_link_dict=True)
    for link in links:
        print(link)
    assert len(link_handles) == len(links)
    assert len(links) == len(expected)
    for link in links:
        assert link["handle"] in link_handles
        assert link["type"] == link_type or link_type == WILDCARD
        if link_type == similarity:
            assert link["template"] == template_similarity
        if link_type == inheritance:
            assert link["template"] == template_inheritance
        if link["type"] in UNORDERED_LINK_TYPES:
            assert set(link["targets"]) in expected
        else:
            assert link["targets"] in expected

def test_get_links_with_patterns():
    #TODO: uncomment this check (and make it pass :-))
    #_check_pattern(similarity, [WILDCARD, WILDCARD], all_similarities)
    print("human", human)
    print("monkey", monkey)
    print("chimp", chimp)
    print("ent", ent)
    print("gorilla", gorilla)
    _check_pattern(similarity, [human, WILDCARD], [
        set([human, monkey]),
        set([human, chimp]),
        set([human, ent]),
        set([human, gorilla]),
    ])
    _check_pattern(similarity, [WILDCARD, human], [
        set([human, monkey]),
        set([human, chimp]),
        set([human, ent]),
        set([human, gorilla]),
    ])
    _check_pattern(inheritance, [WILDCARD, WILDCARD], all_inheritances)
    _check_pattern(inheritance, [human, WILDCARD], [
        [human, mammal],
    ])
    _check_pattern(inheritance, [WILDCARD, animal], [
        [mammal, animal],
        [reptile, animal],
        [earthworm, animal],
    ])
    _check_pattern(WILDCARD, [gorilla, monkey], [
        set([gorilla, monkey]),
    ])
    _check_pattern(WILDCARD, [gorilla, chimp], [
        set([chimp, gorilla]),
    ])
    _check_pattern(WILDCARD, [human, mammal], [
        [human, mammal],
    ])
    _check_pattern(WILDCARD, [mammal, human], [
    ])
    _check_pattern(WILDCARD, [gorilla, mammal], [
        [gorilla, mammal],
    ])
    _check_pattern(WILDCARD, [mammal, gorilla], [
    ])
    _check_pattern(WILDCARD, [human, WILDCARD], [
        set([human, monkey]),
        set([human, chimp]),
        set([human, ent]),
        set([human, gorilla]),
        [human, mammal],
    ])
