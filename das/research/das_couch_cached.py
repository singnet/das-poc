import argparse
import os

from couchbase import exceptions as cb_exceptions
from couchbase.auth import PasswordAuthenticator
from couchbase.cluster import Cluster
from couchbase.management.collections import CollectionSpec
from pymongo.collection import Collection
from pymongo.mongo_client import MongoClient

from das.helpers import get_logger
from das.research.cache import (CachedCouchbaseClient, CouchbaseClient,
                                DocumentNotFoundException)
from das.util import AccumulatorClock, Clock, Statistics

logger = get_logger()

INCOMING_COLL_NAME = 'IncomingSet'
OUTGOING_COLL_NAME = 'OutgoingSet'

clock = Clock()
incoming_clock = Clock()
outgoing_clock = Clock()
incoming_time_statistics = Statistics()
outgoing_time_statistics = Statistics()
incoming_size_statistics = Statistics()
outgoing_size_statistics = Statistics()
get_time_statistics = Statistics()
upsert_time_statistics = Statistics()
batch_clock = Clock()

acc_clock_block1 = AccumulatorClock()
acc_clock_block2 = AccumulatorClock()
acc_clock_block3 = AccumulatorClock()
acc_clock_block4 = AccumulatorClock()
acc_clock_full = AccumulatorClock()


def append(couchbase_client: CachedCouchbaseClient, key: str, new_value):
  value = []
  try:
    clock.reset()
    value = couchbase_client.get(key)
    # logger.info(result.content)
    # value = result.content
  except DocumentNotFoundException:
    pass
  finally:
    get_time_statistics.add(clock.elapsed_time_ms())

  value.extend(new_value)
  clock.reset()
  v = list(set(value))
  couchbase_client.add(key=key, value=v, size=len(v))
  incoming_size_statistics.add(len(v))
  upsert_time_statistics.add(clock.elapsed_time_ms())


def populate_sets(collection: Collection, bucket):
  incoming_cached = CachedCouchbaseClient(
    couchbase_client=CouchbaseClient(bucket=bucket, collection_name=INCOMING_COLL_NAME),
    limit=10000000
  )

  outgoing_set = bucket.collection(OUTGOING_COLL_NAME)

  total = collection.count_documents({})
  cursor = collection.find({}, no_cursor_timeout=True).batch_size(100)
  count = 0
  clock.reset()
  batch_clock.reset()
  for doc in cursor:
    acc_clock_full.start()
    acc_clock_block1.start()
    _id = doc['_id']
    if 'keys' in doc:
      keys = doc['keys']
    else:
      keys = {v for k, v in doc.items() if k.startswith('key')}
    acc_clock_block1.pause()

    acc_clock_block2.start()
    outgoing_clock.reset()
    outgoing_list = list(set(keys))
    outgoing_set.upsert(_id, outgoing_list)
    outgoing_time_statistics.add(outgoing_clock.elapsed_time_ms())
    outgoing_size_statistics.add(len(outgoing_list))
    acc_clock_block2.pause()

    acc_clock_block3.start()
    incoming_dict = {}
    for key in keys:
      if key in incoming_dict:
        incoming_dict[key].append(_id)
      else:
        incoming_dict[key] = [_id]
    acc_clock_block3.pause()

    acc_clock_block4.start()
    incoming_clock.reset()
    for key, values in incoming_dict.items():
      append(incoming_cached, key=key, new_value=values)
    incoming_time_statistics.add(incoming_clock.elapsed_time_ms())
    acc_clock_block4.pause()

    count += 1
    if count % 10000 == 0:
      logger.info('\n')
      logger.info('Documents processed: [{}/{}]'.format(count, total))
      logger.info('Batch time (sec):         {}'.format(batch_clock.elapsed_time_seconds()))
      logger.info('Block full (sec):         {}'.format(acc_clock_full.acc_seconds()))
      logger.info('Block1 (sec):             {}'.format(acc_clock_block1.acc_seconds()))
      logger.info('Block2 (sec):             {}'.format(acc_clock_block2.acc_seconds()))
      logger.info('Block3 (sec):             {}'.format(acc_clock_block3.acc_seconds()))
      logger.info('Block4 (sec):             {}'.format(acc_clock_block4.acc_seconds()))

      logger.info('Time incoming (ms):       {}'.format(incoming_time_statistics.pretty_print()))
      logger.info("Time outgoing (ms):       {}".format(outgoing_time_statistics.pretty_print()))

      logger.info('Couch incoming get (ms):  {}'.format(get_time_statistics.pretty_print()))
      logger.info('Incoming upsert (ms):     {}'.format(upsert_time_statistics.pretty_print()))

      logger.info('Size incoming:            {}'.format(incoming_size_statistics.pretty_print()))
      logger.info('Size outgoing:            {}'.format(outgoing_size_statistics.pretty_print()))

      incoming_time_statistics.reset()
      outgoing_time_statistics.reset()
      incoming_size_statistics.reset()
      outgoing_size_statistics.reset()
      get_time_statistics.reset()
      upsert_time_statistics.reset()
      batch_clock.reset()

      acc_clock_block1.reset()
      acc_clock_block2.reset()
      acc_clock_block3.reset()
      acc_clock_block4.reset()
      acc_clock_full.reset()

    acc_clock_full.pause()

  cursor.close()
  incoming_cached.flush()


def create_collections(bucket, collections_names=None):
  if collections_names is None:
    collections_names = []
  # Creating Couchbase collections
  coll_manager = bucket.collections()
  for name in collections_names:
    logger.info(f'Creating Couchbase collection: "{name}"...')
    try:
      coll_manager.create_collection(CollectionSpec(name))
    except cb_exceptions.CollectionAlreadyExistsException as _:
      logger.info(f'Collection exists!')
      pass
    except Exception as e:
      logger.error(f'[create_collections] Failed: {e}')


def get_mongodb(mongodb_specs):
  client = MongoClient(
    f'mongodb://'
    f'{mongodb_specs["username"]}:{mongodb_specs["password"]}'
    f'@{mongodb_specs["hostname"]}:{mongodb_specs["port"]}')
  return client[mongodb_specs['database']]


def main(mongodb_specs, couchbase_specs):
  cluster = Cluster(
    f'couchbase://{couchbase_specs["hostname"]}',
    authenticator=PasswordAuthenticator(couchbase_specs['username'], couchbase_specs['password']))
  bucket = cluster.bucket('das')

  create_collections(
    bucket=bucket,
    collections_names=[INCOMING_COLL_NAME, OUTGOING_COLL_NAME])

  db = get_mongodb(mongodb_specs)

  logger.info('Indexing links_1')
  populate_sets(db['links_1'], bucket)
  logger.info('Indexing links_2')
  populate_sets(db['links_2'], bucket)
  logger.info('Indexing links_3')
  populate_sets(db['links_3'], bucket)
  logger.info('Indexing links')
  populate_sets(db['links'], bucket)


def run():
  parser = argparse.ArgumentParser(
    'Indexes DAS (from MongoDB) to Couchbase', formatter_class=argparse.ArgumentDefaultsHelpFormatter
  )
  parser.add_argument('--mongo-hostname', help='mongo hostname to connect to')
  parser.add_argument('--mongo-port', help='mongo port to connect to')
  parser.add_argument('--mongo-username', help='mongo username')
  parser.add_argument('--mongo-password', help='mongo password')
  parser.add_argument('--mongo-database', help='mongo database name to connect to')

  parser.add_argument('--couchbase-hostname', help='couchbase hostname to connect to')
  parser.add_argument('--couchbase-username', help='couchbase username')
  parser.add_argument('--couchbase-password', help='couchbase password')

  args = parser.parse_args()

  mongodb_specs = {
    'hostname': args.mongo_hostname or os.environ.get('DAS_MONGODB_HOSTNAME', 'localhost'),
    'port': args.mongo_port or os.environ.get('DAS_MONGODB_PORT', 27017),
    'username': args.mongo_username or os.environ.get('DAS_DATABASE_USERNAME', 'dbadmin'),
    'password': args.mongo_password or os.environ.get('DAS_DATABASE_PASSWORD', 'das#secret'),
    'database': args.mongo_database or os.environ.get('DAS_DATABASE_NAME', 'das'),
  }

  couchbase_specs = {
    'hostname': args.couchbase_hostname or os.environ.get('DAS_COUCHBASE_HOSTNAME', 'localhost'),
    'username': args.couchbase_username or os.environ.get('DAS_DATABASE_USERNAME', 'dbadmin'),
    'password': args.couchbase_password or os.environ.get('DAS_DATABASE_PASSWORD', 'das#secret'),
  }

  main(mongodb_specs, couchbase_specs)


if __name__ == '__main__':
  run()
