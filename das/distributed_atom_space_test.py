import tempfile
import shutil
import os
import pytest
from das.distributed_atom_space import DistributedAtomSpace, WILDCARD, QueryOutputFormat
from das.database.db_interface import UNORDERED_LINK_TYPES

das = DistributedAtomSpace()

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
    set([earthworm, snake]),
    set([triceratops, rhino]),
    set([vine, snake]),
    set([ent, human]),
]
all_inheritances = [
    [human, mammal],
    [monkey, mammal],
    [chimp, mammal],
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

all_nodes = [
    animal,
    mammal,
    reptile,
    plant,
    human,
    monkey,
    chimp,
    earthworm,
    snake,
    triceratops,
    rhino,
    vine,
    ent,
    dinosaur
]

template_similarity = [similarity, concept, concept]
template_inheritance = [inheritance, concept, concept]

def test_get_file_list():

    tmp_prefix = 'das_pytest_'
    temp_dir1 = tempfile.mkdtemp(prefix=tmp_prefix)
    _, temp_file1 = tempfile.mkstemp(suffix=".metta", prefix=tmp_prefix)
    _, temp_file2 = tempfile.mkstemp(suffix=".blah", prefix=tmp_prefix)
    _, temp_file3 = tempfile.mkstemp(dir=temp_dir1, suffix=".metta", prefix=tmp_prefix)
    _, temp_file4 = tempfile.mkstemp(dir=temp_dir1, suffix=".metta", prefix=tmp_prefix)
    _, temp_file5 = tempfile.mkstemp(dir=temp_dir1, suffix=".blah", prefix=tmp_prefix)
    temp_dir2 = tempfile.mkdtemp(dir=temp_dir1, prefix=tmp_prefix)
    _, temp_file6 = tempfile.mkstemp(dir=temp_dir2, suffix=".metta", prefix=tmp_prefix)
    temp_dir3 = tempfile.mkdtemp(dir=temp_dir1, prefix=tmp_prefix)

    file_list = das._get_file_list(temp_file1)
    assert len(file_list) == 1
    assert temp_file1 in file_list

    with pytest.raises(ValueError):
        file_list = das._get_file_list(temp_file2)

    file_list = das._get_file_list(temp_dir1)
    assert len(file_list) == 2
    assert temp_file3 in file_list
    assert temp_file4 in file_list

    with pytest.raises(ValueError):
        file_list = das._get_file_list(temp_dir3)

    shutil.rmtree(temp_dir1)
    os.remove(temp_file1)
    os.remove(temp_file2)

def test_get_node():
    human_document = das.get_node(concept, "human", output_format=QueryOutputFormat.ATOM_INFO)
    assert human_document["handle"] == human
    assert human_document["type"] == concept
    assert human_document["name"] == "human"

def test_get_atom():
    assert human == das.get_atom(human)
    human_document = das.get_atom(human, output_format=QueryOutputFormat.ATOM_INFO)
    assert human_document["handle"] == human
    assert human_document["type"] == concept
    assert human_document["name"] == "human"
    link = das.get_link(inheritance, [human, mammal])
    assert das.get_atom(link) == link

def test_get_nodes():
    human_document = das.get_nodes(concept, "human", output_format=QueryOutputFormat.ATOM_INFO)[0]
    assert human_document["handle"] == human
    assert human_document["type"] == concept
    assert human_document["name"] == "human"

    concepts = das.get_nodes(concept)
    assert sorted(concepts) == sorted(all_nodes)
    

def test_get_link():
    link_handle = das.get_link(similarity, [human, monkey])
    link = das.get_link(similarity, [human, monkey], output_format=QueryOutputFormat.ATOM_INFO)
    assert link["handle"] == link_handle
    assert link["type"] == similarity
    assert link["template"] == template_similarity

def test_get_link_targets():
    test_links = [(similarity, list(v)) for v in all_similarities] + \
                 [(inheritance, v) for v in all_inheritances]

    for link_type, targets in test_links:
        link_handle = das.get_link(link_type, targets)
        answer = das.get_link_targets(link_handle)
        assert len(answer) == len(targets)
        if link_type == similarity:
            for node in targets:
                assert node in answer
        else:
            for n1, n2 in zip(answer, targets):
                assert n1 == n2

def test_get_link_type():
    test_links = [(similarity, list(v)) for v in all_similarities] + \
                 [(inheritance, v) for v in all_inheritances]

    for link_type, targets in test_links:
        link_handle = das.get_link(link_type, targets)
        answer = das.get_link_type(link_handle)
        assert answer == link_type

def test_get_node_name():
    
    test_nodes = [
        (animal, "animal"),
        (mammal, "mammal"),
        (reptile, "reptile"),
        (plant, "plant"),
        (human, "human"),
        (monkey, "monkey"),
        (chimp, "chimp"),
        (earthworm, "earthworm"),
        (snake, "snake"),
        (triceratops, "triceratops"),
        (rhino, "rhino"),
        (vine, "vine"),
        (ent, "ent"),
        (dinosaur, "dinosaur")
    ]

    for node_handle, node_name in test_nodes:
        name = das.get_node_name(node_handle)
        assert name == node_name
    for link_handle in das.get_links(link_type=similarity, target_types=[concept, concept]):
        with pytest.raises(ValueError):
            name = das.get_node_name(link_handle)
    with pytest.raises(ValueError):
        name = das.get_node_name("blah")

def test_get_links_with_link_templates():
    link_handles = das.get_links(link_type=similarity, target_types=[concept, concept])
    links = das.get_links(link_type=similarity, target_types=[concept, concept], output_format=QueryOutputFormat.ATOM_INFO)
    assert len(link_handles) == len(links)
    for link in links:
        assert link["handle"] in link_handles
        assert link["type"] == similarity
        assert link["template"] == template_similarity
        assert set(link["targets"]) in all_similarities

def _check_pattern(link_type, targets, expected):
    link_handles = list(set(das.get_links(link_type=link_type, targets=targets)))
    links = das.get_links(link_type=link_type, targets=targets, output_format=QueryOutputFormat.ATOM_INFO)
    assert len(link_handles) == len(expected)
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
    _check_pattern(similarity, [human, WILDCARD], [
        set([human, monkey]),
        set([human, chimp]),
        set([human, ent]),
    ])
    _check_pattern(similarity, [WILDCARD, human], [
        set([human, monkey]),
        set([human, chimp]),
        set([human, ent]),
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
    _check_pattern(WILDCARD, [chimp, monkey], [
        set([chimp, monkey]),
    ])
    _check_pattern(WILDCARD, [monkey, chimp], [
        set([chimp, monkey]),
    ])
    _check_pattern(WILDCARD, [human, mammal], [
        [human, mammal],
    ])
    _check_pattern(WILDCARD, [mammal, human], [
    ])
    _check_pattern(WILDCARD, [human, WILDCARD], [
        set([human, monkey]),
        set([human, chimp]),
        set([human, ent]),
        [human, mammal],
    ])
