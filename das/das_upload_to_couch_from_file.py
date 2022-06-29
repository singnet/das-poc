import argparse
import datetime
import os
from typing import Iterator, Tuple, List

from couchbase import exceptions as cb_exceptions
from couchbase.auth import PasswordAuthenticator
from couchbase.cluster import Cluster
from couchbase.management.collections import CollectionSpec

from das.helpers import get_logger

logger = get_logger()

INCOMING_COLL_NAME = 'IncomingSet'
OUTGOING_COLL_NAME = 'OutgoingSet'
PATTERNS_COLL_NAME = 'Patterns'
TEMPLATES_COLL_NAME = 'Templates'

# There is a Couchbase limitation for long values (max: 20Mb)
# So we set the it to ~15Mb, if this max size is reached
# we create a new key to store the next 15Mb batch and so on.
MAX_BLOCK_SIZE = 500000


def key_value_generator(
  input_filename: str,
  *,
  block_size: int = MAX_BLOCK_SIZE) -> Iterator[Tuple[str, List[str], int]]:
  last_key = ''
  last_list = []
  counter = 0
  with open(input_filename, 'r') as fh:
    for line in fh:
      line = line.strip()
      if line == '':
        continue
      key, value = line.split(',')
      if last_key == key:
        last_list.append(value)
        if len(last_list) >= block_size:
          yield last_key, last_list, counter
          counter += 1
          last_list = []
      else:
        if last_key != '':
          yield last_key, last_list, counter
        counter = 0
        last_key = key
        last_list = [value]
  if last_key != '':
    yield last_key, last_list, counter

def key_value_targets_generator(
  input_filename: str,
  *,
  block_size: int = MAX_BLOCK_SIZE / 4) -> Iterator[Tuple[str, List[str], int]]:
  last_key = ''
  last_list = []
  counter = 0
  with open(input_filename, 'r') as fh:
    for line in fh:
      line = line.strip()
      if line == '':
        continue
      key, value, *targets = line.split(',')
      if last_key == key:
        last_list.append({'handle':value, 'targets': targets})
        if len(last_list) >= block_size:
          yield last_key, last_list, counter
          counter += 1
          last_list = []
      else:
        if last_key != '':
          yield last_key, last_list, counter
        counter = 0
        last_key = key
        last_list = [value, *targets]
        last_list = [{'handle':value, 'targets': targets}]
  if last_key != '':
    yield last_key, last_list, counter


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
    authenticator=PasswordAuthenticator(couchbase_specs["username"], couchbase_specs["password"]))
  bucket = cluster.bucket('das')
  atoms_file_name = input_filename + '.atoms'
  patterns_file_name = input_filename + '.patterns'
  templates_file_name = input_filename + '.templates'
  atoms_collection = bucket.collection(INCOMING_COLL_NAME)
  patterns_collection = bucket.collection(PATTERNS_COLL_NAME)
  templates_collection = bucket.collection(TEMPLATES_COLL_NAME)

  # Incoming/outgoing
  with open(atoms_file_name, 'r') as f:
    total_entries = len(f.readlines())
  i = 0
  done = 0
  for k, v, c in key_value_generator(atoms_file_name):
    if c == 0:
      atoms_collection.upsert(k, v, timeout=datetime.timedelta(seconds=100))
    else:
      if c == 1:
        first_block = atoms_collection.get(k)
        atoms_collection.upsert(f"{k}_0", first_block.content, timeout=datetime.timedelta(seconds=100))
      atoms_collection.upsert(k, c + 1)
      atoms_collection.upsert(f"{k}_{c}", v, timeout=datetime.timedelta(seconds=100))
    i += 1
    done += len(v)
    if i % 100000 == 0:
      logger.info(f'Entries uploaded: [{done}/{total_entries}]')
  logger.info(f'Entries uploaded: [{done}/{total_entries}]')

  # Patterns
  with open(patterns_file_name, 'r') as f:
    total_entries = len(f.readlines())
  i = 0
  done = 0
  for k, v, c in key_value_targets_generator(patterns_file_name):
    if c == 0:
      patterns_collection.upsert(k, v, timeout=datetime.timedelta(seconds=100))
    else:
      if c == 1:
        first_block = patterns_collection.get(k)
        patterns_collection.upsert(f"{k}_0", first_block.content, timeout=datetime.timedelta(seconds=100))
      patterns_collection.upsert(k, c + 1)
      patterns_collection.upsert(f"{k}_{c}", v, timeout=datetime.timedelta(seconds=100))
    i += 1
    done += len(v)
    if i % 100000 == 0:
      logger.info(f'Patterns uploaded: [{done}/{total_entries}]')
  logger.info(f'Patterns uploaded: [{done}/{total_entries}]')

  # Templates
  with open(templates_file_name, 'r') as f:
    total_entries = len(f.readlines())
  i = 0
  done = 0
  for k, v, c in key_value_targets_generator(templates_file_name):
    if c == 0:
      templates_collection.upsert(k, v, timeout=datetime.timedelta(seconds=100))
    else:
      if c == 1:
        first_block = templates_collection.get(k)
        templates_collection.upsert(f"{k}_0", first_block.content, timeout=datetime.timedelta(seconds=100))
      templates_collection.upsert(k, c + 1)
      templates_collection.upsert(f"{k}_{c}", v, timeout=datetime.timedelta(seconds=100))
    i += 1
    done += len(v)
    if i % 100000 == 0:
      logger.info(f'Templates uploaded: [{done}/{total_entries}]')
  logger.info(f'Templates uploaded: [{done}/{total_entries}]')

def run():
  parser = argparse.ArgumentParser()

  parser.add_argument('--file-path', help='pairs file path')

  parser.add_argument('--couchbase-hostname', help='couchbase hostname to connect to')
  parser.add_argument('--couchbase-username', help='couchbase username')
  parser.add_argument('--couchbase-password', help='couchbase password')

  args = parser.parse_args()

  couchbase_specs = {
    'hostname': args.couchbase_hostname or os.environ.get('DAS_COUCHBASE_HOSTNAME', 'localhost'),
    'username': args.couchbase_username or os.environ.get('DAS_DATABASE_USERNAME', 'dbadmin'),
    'password': args.couchbase_password or os.environ.get('DAS_DATABASE_PASSWORD', 'das#secret'),
  }

  main(couchbase_specs, args.file_path or '/tmp/all_pairs.txt')


if __name__ == '__main__':
  run()
