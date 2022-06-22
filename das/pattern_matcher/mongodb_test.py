import os

import pytest

from das.helpers import get_mongodb
from das.pattern_matcher.mongodb import DASMongoDB


@pytest.fixture()
def mongo_db():
    mongodb_specs = {
        "hostname": os.environ.get("DAS_MONGODB_HOSTNAME", "localhost"),
        "port": os.environ.get("DAS_MONGODB_PORT", 27017),
        "username": os.environ.get("DAS_DATABASE_USERNAME", "dbadmin"),
        "password": os.environ.get("DAS_DATABASE_PASSWORD", "das#secret"),
        "database": os.environ.get("DAS_DATABASE_NAME", "toy"),
    }
    return get_mongodb(mongodb_specs)


@pytest.fixture()
def das_db(mongo_db):
    return DASMongoDB(mongo_db)


def test_node_exists(das_db: DASMongoDB):
    assert das_db.node_exists("Concept", "ent"), "Concept:ent should exist"
    assert not das_db.node_exists("Concept", "none"), "Concept:none shouldn't exist"
    assert not das_db.node_exists(
        "concept", "ent"
    ), "concept:ent (all lower case) shouldn't exist"


def test_link_exists(das_db: DASMongoDB):
    assert das_db.link_exists(
        "Inheritance",
        ["83638a4598185ca13b43140029b494f7", "4581aeda36530cca36f83d53d6fff0c3"],
    )
    assert not das_db.link_exists("Similarity", ["0123456789abcdef", "fedcba9876543210"])


def test_get_node_handle(das_db: DASMongoDB):
    assert das_db.get_node_handle("Concept", "ent") == "13ba6904f6987307e3bce206c350fdf1"

    with pytest.raises(ValueError) as excinfo:
        das_db.get_node_handle("Concept", "none")
    assert "invalid node: type: Concept, name: none" == str(excinfo.value)


def test_get_link_targets(das_db: DASMongoDB):
    with pytest.raises(ValueError, match=r".* inexistent key$") as excinfo:
        das_db.get_link_targets("inexistent key")
    assert "invalid handle: inexistent key" == str(
        excinfo.value
    ), "handle 'inexistent key' should raise an error"
    handle = "187208d65719605d601f56ad8767998b"
    res = das_db.get_link_targets(handle)
    assert isinstance(res, list), "get_link_targets should return a list"
    assert len(res) == 3, f"handle '{handle}' should return 3 targets"


def test_get_matched_links(das_db: DASMongoDB):
    res = das_db.get_matched_links("Inheritance", ["*", "*"])
    assert isinstance(res, list), "get_matched_links should return a list"
    assert len(res) > 0, "get_matched_links should return at least one link"
    assert all(
        isinstance(h, str) for h in res
    ), "get_matched_links should return a list of strings"

    res = das_db.get_matched_links(
        "Inheritance", ["83638a4598185ca13b43140029b494f7", "*"]
    )
    assert isinstance(res, list), "get_matched_links should return a list"
    assert len(res) > 0, "get_matched_links should return at least one link"
    assert all(
        isinstance(h, str) for h in res
    ), "get_matched_links should return a list of strings"

    res = das_db.get_matched_links(
        "Similarity", ["83638a4598185ca13b43140029b494f7", "*"]
    )
    assert isinstance(res, list), "get_matched_links should return a list"
    assert len(res) > 0, "get_matched_links should return at least one link"
    assert all(
        isinstance(h, str) for h in res
    ), "get_matched_links should return a list of strings"


def test_get_link_handle_should_be_equal_to_generate_link_handle(das_db: DASMongoDB):
    link_handle = das_db.get_link_handle(
        "Similarity",
        ["83638a4598185ca13b43140029b494f7", "c8ec81db392b765d010fa1eaf052b51e"],
    )
    generated_link_handle = das_db._generate_link_handle(
        "df5aa67f61bf6485dc74cd319e5681a0",
        ["83638a4598185ca13b43140029b494f7", "c8ec81db392b765d010fa1eaf052b51e"],
    )
    assert (
        link_handle == generated_link_handle
    ), "link_handle and generated_link_handle should be equal"


def test_generate_link_handle(das_db: DASMongoDB):
    handle = das_db._generate_link_handle(
        "8c9f9d98fbfd0563ce23bf8ed77a7ee8",
        ["83638a4598185ca13b43140029b494f7", "4581aeda36530cca36f83d53d6fff0c3"],
    )
    assert handle == "bd49fe6a8f57c7e6ec77d7b8898372d6", "handle should be equal to 'bd49fe6a8f57c7e6ec77d7b8898372d6'"


def test_get_doc(das_db: DASMongoDB):
    doc = das_db._get_doc("bd49fe6a8f57c7e6ec77d7b8898372d6")
    assert isinstance(doc, dict), "get_doc should return a dict"
    assert "_id" in doc, "get_doc should return a dict with an _id"

    doc = das_db._get_doc("bd49fe6a8f57c7e6ec77d7b8898372d6", das_db.COLL_LINKS_3)
    assert isinstance(doc, dict), "get_doc should return a dict"
    assert "_id" in doc, "get_doc should return a dict with an _id"

    assert das_db._get_doc("inexistent key", das_db.COLL_LINKS) is None, "get_doc should return None for inexistent key"

    assert das_db._get_doc("bd49fe6a8f57c7e6ec77d7b8898372d6", das_db.COLL_LINKS) is None, "get_doc should return None if the doc doesn't exist"