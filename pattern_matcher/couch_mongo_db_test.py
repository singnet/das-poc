import os

import pytest
from couchbase.auth import PasswordAuthenticator
from couchbase.bucket import Bucket
from couchbase.cluster import Cluster

from pattern_matcher.couch_mongo_db import CouchMongoDB
from scripts.helpers import get_mongodb


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
def couch_db():
    couchbase_specs = {
        "hostname": os.environ.get("DAS_COUCHBASE_HOSTNAME", "localhost"),
        "username": os.environ.get("DAS_DATABASE_USERNAME", "dbadmin"),
        "password": os.environ.get("DAS_DATABASE_PASSWORD", "das#secret"),
    }
    cluster = Cluster(
        f'couchbase://{couchbase_specs["hostname"]}',
        authenticator=PasswordAuthenticator(
            couchbase_specs["username"], couchbase_specs["password"]
        ),
    )
    return cluster.bucket("das")


@pytest.fixture()
def cm_db(couch_db, mongo_db):
    return CouchMongoDB(couch_db, mongo_db)


def test_node_exists(cm_db: CouchMongoDB):
    assert cm_db.node_exists("Concept", "ent"), "Concept:ent should exist"
    assert not cm_db.node_exists("Concept", "none"), "Concept:none shouldn't exist"
    assert not cm_db.node_exists(
        "concept", "ent"
    ), "concept:ent (all lower case) shouldn't exist"


def test_get_node_handle(cm_db: CouchMongoDB):
    assert cm_db.get_node_handle("Concept", "ent") == "13ba6904f6987307e3bce206c350fdf1"

    with pytest.raises(ValueError) as excinfo:
        cm_db.get_node_handle("Concept", "none")
    assert "invalid node: type: Concept, name: none" == str(excinfo.value)


def test_get_link_targets(cm_db: CouchMongoDB):
    with pytest.raises(ValueError, match=r".* inexistent key$") as excinfo:
        cm_db.get_link_targets("inexistent key")
    assert "invalid handle: inexistent key" == str(
        excinfo.value
    ), "handle 'inexistent key' should raise an error"
    handle = "187208d65719605d601f56ad8767998b"
    res = cm_db.get_link_targets(handle)
    assert isinstance(res, list), "get_link_targets should return a list"
    assert len(res) == 3, f"handle '{handle}' should return 3 targets"
    assert (
        "13ba6904f6987307e3bce206c350fdf1" in res
    ), "handle '{handle}' should contain '13ba6904f6987307e3bce206c350fdf1'"

