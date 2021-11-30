import argparse
import datetime
import logging
import os
from typing import Iterator

from couchbase import exceptions as cb_exceptions
from couchbase.auth import PasswordAuthenticator
from couchbase.cluster import Cluster
from couchbase.management.collections import CollectionSpec

logger = logging.getLogger("das")
logger.setLevel(logging.INFO)

stream_handler = logging.StreamHandler()
stream_handler.setLevel(logging.INFO)

formatter = logging.Formatter("[%(asctime)s %(levelname)s]: %(message)s")
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)

INCOMING_COLL_NAME = 'IncomingSet'
OUTGOING_COLL_NAME = 'OutgoingSet'


def key_value_generator(input_filename: str) -> Iterator[tuple[str, list[str]]]:
  last_key = ''
  last_list = []
  with open(input_filename, 'r') as fh:
    for line in fh:
      line = line.strip()
      if line == '':
        continue
      key, value = line.split(',')
      if last_key == key:
        last_list.append(value)
      else:
        if last_key != '':
          yield last_key, last_list
        last_key = key
        last_list = [value]
  if last_key != '':
    yield last_key, last_list


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


def main(couchbase_specs, input_filename: str) -> None:
  cluster = Cluster(
    f'couchbase://{couchbase_specs["hostname"]}',
    authenticator=PasswordAuthenticator(couchbase_specs["username"], couchbase_specs["password"]),

  )
  bucket = cluster.bucket('das')
  collection = bucket.collection(INCOMING_COLL_NAME)

  i = 0
  for k, v in key_value_generator(input_filename):
    collection.upsert(k, v, timeout=datetime.timedelta(seconds=100))
    i += 1
    if i % 10000 == 0:
      logger.info(f'processed {i}')


def run():
  parser = argparse.ArgumentParser()

  parser.add_argument('--file-path', help='pairs file path')

  parser.add_argument('--couchbase-hostname', help='couchbase hostname to connect to')
  parser.add_argument('--couchbase-username', help='couchbase username')
  parser.add_argument('--couchbase-password', help='couchbase password')

  args = parser.parse_args()

  couchbase_specs = {
    'hostname': args.couchbase_hostname or os.environ.get('DAS_DATABASE_HOSTNAME', 'localhost'),
    'username': args.couchbase_username or os.environ.get('DAS_DATABASE_USERNAME', 'dbadmin'),
    'password': args.couchbase_password or os.environ.get('DAS_DATABASE_PASSWORD', 'das#secret'),
  }

  main(couchbase_specs, args.file_path or '/tmp/all_pairs.txt')


if __name__ == '__main__':
  run()
