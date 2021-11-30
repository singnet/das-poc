import argparse
import logging
from typing import Iterator

from couchbase import exceptions as cb_exceptions
from couchbase.auth import PasswordAuthenticator
from couchbase.cluster import Cluster
from couchbase.management.collections import CollectionSpec

from cache import CouchbaseClient

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


def main(input_filename: str, couchbase_user: str, couchbase_password: str) -> None:
  cluster = Cluster(
    'couchbase://localhost',
    authenticator=PasswordAuthenticator(couchbase_user, couchbase_password),

  )
  bucket = cluster.bucket('das')

  couchbase_client = CouchbaseClient(bucket=bucket, collection_name=INCOMING_COLL_NAME)

  i = 0
  for k, v in key_value_generator(input_filename):
    couchbase_client.add(k, v)
    i += 1
    if i % 10000 == 0:
      logger.info(f'processed {i}')


if __name__ == '__main__':
  parser = argparse.ArgumentParser()
  parser.add_argument('filename', help='Pairs filename', type=str)
  parser.add_argument('--couchbase-user', help='Couchbase user', type=str)
  parser.add_argument('--couchbase-password', help='Couchbase password', type=str)
  args = parser.parse_args()
  main(args.filename, args.couchbase_user, args.couchbase_password)
