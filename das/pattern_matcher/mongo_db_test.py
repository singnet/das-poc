import pytest
from couchbase.auth import PasswordAuthenticator
from couchbase.cluster import Cluster

from das.pattern_matcher.mongo_db import MongoDB
from.couch_mongo_db_test import mongo_db
from.couch_mongo_db_test import test_get_link_targets as couch_mongo_get_link_targets
from.couch_mongo_db_test import test_get_matched_links as couch_mongo_get_matched_links
from.couch_mongo_db_test import test_get_matched_type_template as couch_mongo_get_matched_type_template

@pytest.fixture()
def db(mongo_db):
    return MongoDB(mongo_db)

def test_mongo_get_link_targets(db: MongoDB):
    couch_mongo_get_link_targets(db)
    
def test_mongo_get_matched_links(db: MongoDB):
    couch_mongo_get_matched_links(db)
    
def test_mongo_get_matched_typed_template(db: MongoDB):
    couch_mongo_get_matched_type_template(db)
