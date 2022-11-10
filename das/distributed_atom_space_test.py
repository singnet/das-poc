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

def test_get_link():
    link_handle = das.get_link(similarity, [human, monkey])
    link = das.get_link(similarity, [human, monkey], output_format=QueryOutputFormat.ATOM_INFO)
    assert link["handle"] == link_handle
    assert link["type"] == similarity
    assert link["template"] == template_similarity

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
    link_handles = das.get_links(link_type=link_type, targets=targets)
    links = das.get_links(link_type=link_type, targets=targets, output_format=QueryOutputFormat.ATOM_INFO)
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
