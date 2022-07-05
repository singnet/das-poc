import pytest
from couchbase.auth import PasswordAuthenticator
from couchbase.cluster import Cluster

from das.pattern_matcher.couch_db import CouchDB
from.couch_mongo_db_test import couch_db, mongo_db, db
from.couch_mongo_db_test import test_link_exists as couch_mongo_link_exists
from.couch_mongo_db_test import test_get_link_targets as couch_mongo_get_link_handle
from.couch_mongo_db_test import test_is_ordered as couch_mongo_is_ordered

def test_couch_link_exists(db: CouchDB):
    couch_mongo_link_exists(db)

def test_couch_get_link_handle(db: CouchDB):
    couch_mongo_get_link_handle(db)

def test_couch_is_ordered(db: CouchDB):
    couch_mongo_is_ordered(db)
