from couchbase.cluster import Cluster
from couchbase.auth import PasswordAuthenticator

from couchbase.management.collections import CollectionSpec
from couchbase import exceptions as cb_exceptions

from pymongo import MongoClient


def create_collections(bucket, collections_names=None):
  if collections_names is None:
    collections_names = []
  # Creating Couchbase collections
  coll_manager = bucket.collections()
  for name in collections_names:
    print(f'Creating Couchbase collection: "{name}"...')
    try:
      coll_manager.create_collection(CollectionSpec(name))
    except cb_exceptions.CollectionAlreadyExistsException as e:
      print(f'Collection exists : {e}')
      pass
    except Exception as e:
      print(e)


def get_couchbase():
  return Cluster('couchbase://localhost', authenticator=PasswordAuthenticator('couchadmin', 'das#secret'))


def get_mongodb():
  mongo_username = 'mongoadmin'
  mongo_password = 'das#secret'
  mongo_hostname = 'localhost'
  mongo_port = '27017'
  mongo_database = 'UBERON'
  client = MongoClient(f"mongodb://{mongo_username}:{mongo_password}@{mongo_hostname}:{mongo_port}")
  return client[mongo_database]


def migrate():
  cluster = get_couchbase()
  bucket = cluster.bucket('das')

  collections_names = ['node_types', 'nodes', 'links_2', 'links_3']
  create_collections(bucket=bucket, collections_names=collections_names)

  mongodb = get_mongodb()

  # Mongo -> Couchbase
  for name in collections_names:
    print(f'Migrating "{name}" MongoDB -> Couchbase...')
    # Couchbase collection
    coll = bucket.collection(name)
    for document in mongodb[name].find({}):
      key = document['_id']
      del document['_id']
      value = document
      coll.upsert(key, value)
