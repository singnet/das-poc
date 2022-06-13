import os

import pytest

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
def cm_db(mongo_db):
    return CouchMongoDB(None, mongo_db)


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
