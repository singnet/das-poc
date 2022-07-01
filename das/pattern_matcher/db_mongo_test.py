import os

import pytest

from das.helpers import get_mongodb
from das.pattern_matcher.db_mongo import DASMongoDB


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
    assert not das_db.link_exists(
        "Similarity", ["0123456789abcdef", "fedcba9876543210"]
    )


def test_get_node_handle(das_db: DASMongoDB):
    assert (
        das_db.get_node_handle("Concept", "ent") == "13ba6904f6987307e3bce206c350fdf1"
    )

    with pytest.raises(ValueError) as excinfo:
        das_db.get_node_handle("Concept", "none")
    assert "Invalid node: type=Concept name=none" == str(excinfo.value)


def test_get_link_targets(das_db: DASMongoDB):
    with pytest.raises(ValueError, match=r".* inexistent key$") as excinfo:
        das_db.get_link_targets("inexistent key")
    assert "Invalid handle: inexistent key" == str(
        excinfo.value
    ), "handle 'inexistent key' should raise an error"
    handle = "187208d65719605d601f56ad8767998b"
    res = das_db.get_link_targets(handle)
    assert isinstance(res, list), "get_link_targets should return a list"
    assert len(res) == 3, f"handle '{handle}' should return 3 targets"


def test_get_matched_links(das_db: DASMongoDB):
    res = das_db.get_matched_links("Inheritance", ["*", "*"])
    assert isinstance(res, list), (
        f"get_matched_links should return a list. Returned a {type(res)} instead."
    )
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


def test_get_node_name(das_db: DASMongoDB):
    assert (
        das_db.get_node_name("13ba6904f6987307e3bce206c350fdf1")
        == "\"Concept:ent\""
    )

    with pytest.raises(ValueError) as excinfo:
        das_db.get_node_name("invalid_handle")
    assert "Invalid node handle: invalid_handle" == str(excinfo.value)