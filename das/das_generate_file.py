import argparse
import logging
import os
import re
import shutil
from typing import Iterator

from couchbase import exceptions as cb_exceptions
from couchbase.auth import PasswordAuthenticator
from couchbase.cluster import Cluster
from couchbase.management.collections import CollectionSpec
from pymongo.collection import Collection

from das.helpers import get_logger, get_mongodb
from das.util import AccumulatorClock, Clock, Statistics

logger = get_logger()

INCOMING_COLL_NAME = "IncomingSet"
OUTGOING_COLL_NAME = "OutgoingSet"
PATTERNS_COLL_NAME = "Patterns"
TEMPLATES_COLL_NAME = "Templates"

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
acc_clock_block5 = AccumulatorClock()
acc_clock_full = AccumulatorClock()

from das.hashing import Hasher, flatten_list


def populate_sets(hasher: Hasher, fh1, fh2, fh3, collection: Collection, bucket, composite_keys_masks: dict[str, list[set[int]]]):

  outgoing_set = bucket.collection(OUTGOING_COLL_NAME)

  total = collection.count_documents({})
  cursor = collection.find({}, no_cursor_timeout=True).batch_size(100)
  count = 0
  clock.reset()
  batch_clock.reset()

  for doc in cursor:
    acc_clock_full.start()
    acc_clock_block1.start()
    _id = doc["_id"]
    if "keys" in doc:
      keys = doc["keys"]
    else:
      keys = [v for k, v in sorted(doc.items(), key=lambda x: x[0]) if k.startswith("key")]
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
      for v in values:
        fh1.write("{},{}\n".format(key, v))
    incoming_time_statistics.add(incoming_clock.elapsed_time_ms())
    acc_clock_block4.pause()

    acc_clock_block5.start()
    if keys[0] in composite_keys_masks:
      for mask in composite_keys_masks[keys[0]]:
        keys_copy = keys.copy()
        for pos in mask:
          keys_copy[pos-1] = '*'
        fh2.write("{},{},{}\n".format(hasher.apply_alg(''.join(keys_copy)), _id, ','.join(outgoing_list[1:])))

    template_key = hasher.apply_alg(flatten_list(doc['type']))
    fh3.write("{},{},{}\n".format(template_key, _id, ','.join(outgoing_list[1:])))
    acc_clock_block5.pause()

    count += 1
    if count % 100000 == 0:
      logger.debug("\n")
      logger.info("Documents processed: [{}/{}]".format(count, total))
      logger.debug("Batch time (sec):         {}".format(batch_clock.elapsed_time_seconds()))
      logger.debug("Block full (sec):         {}".format(acc_clock_full.acc_seconds()))
      logger.debug("Block1 (sec):             {}".format(acc_clock_block1.acc_seconds()))
      logger.debug("Block2 (sec):             {}".format(acc_clock_block2.acc_seconds()))
      logger.debug("Block3 (sec):             {}".format(acc_clock_block3.acc_seconds()))
      logger.debug("Block4 (sec):             {}".format(acc_clock_block4.acc_seconds()))
      logger.debug("Block5 (sec):             {}".format(acc_clock_block5.acc_seconds()))

      logger.debug("Time incoming (ms):       {}".format(incoming_time_statistics.pretty_print()))
      logger.debug("Time outgoing (ms):       {}".format(outgoing_time_statistics.pretty_print()))

      logger.debug("Couch incoming get (ms):  {}".format(get_time_statistics.pretty_print()))
      logger.debug("Incoming upsert (ms):     {}".format(upsert_time_statistics.pretty_print()))

      logger.debug("Size incoming:            {}".format(incoming_size_statistics.pretty_print()))
      logger.debug("Size outgoing:            {}".format(outgoing_size_statistics.pretty_print()))

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
      acc_clock_block5.reset()
      acc_clock_full.reset()

    acc_clock_full.pause()

  cursor.close()
  logger.info("Documents processed: [{}/{}]".format(count, total))


def create_collections(bucket, collections_names=None):
  if collections_names is None:
    collections_names = []
  # Creating Couchbase collections
  coll_manager = bucket.collections()
  for name in collections_names:
    logger.info(f"Creating Couchbase collection: '{name}'...")
    try:
      coll_manager.create_collection(CollectionSpec(name))
    except cb_exceptions.CollectionAlreadyExistsException:
      logger.info(f"Collection exists!")
      pass
    except Exception as e:
      logger.error(f"[create_collections] Failed: {e}")



def generate_non_trivial_binary_masks(dimension: int) -> Iterator[set[int]]:
  if dimension < 1:
    return

  for i in range(1, 2**dimension-1):
    j = dimension-1
    s = set()
    while j >= 0:
      if i >= 2**j:
        s.add(j)
        i -= 2**j
      j -= 1
    yield s



def process_index_pattern_file(filename: str) -> dict[str, list[set[int]]]:
  p = re.compile(r'(?:\s|\t)+')
  d = {}
  with open(filename, 'r') as fh:
    for line in fh:
      tokens = p.split(line.strip())
      label = tokens[0]
      dimension = int(tokens[1])
      keys = [int(t) for t in tokens[2:]]
      result = []
      d[label] = result

      for m in generate_non_trivial_binary_masks(dimension):
        s = set()
        for i in range(dimension):
          if i in m:
            s.add(keys[i])
        result.append(s)
  return d


def group_index_pattern_by_hash(index_pattern: dict[str, list[set[int]]], node_type_db) -> dict[str, list[set[int]]]:
  node_type_to_hash: dict[str, str] = {}
  cursor = node_type_db.find({}, no_cursor_timeout=True).batch_size(100)
  for doc in cursor:
    _id = doc["_id"]
    name = doc["name"]
    node_type_to_hash[name] = _id

  d = {}
  for label, l in index_pattern.items():
    assert label in node_type_to_hash, f'label = {label} node_type_to_hash, = {node_type_to_hash,}'
    d[node_type_to_hash[label]] = l

  return d


def main(mongodb_specs, couchbase_specs, file_path, index_path):
  cluster = Cluster(
    f"couchbase://{couchbase_specs['hostname']}",
    authenticator=PasswordAuthenticator(couchbase_specs["username"], couchbase_specs["password"]))
  bucket = cluster.bucket("das")

  create_collections(
    bucket=bucket,
    collections_names=[INCOMING_COLL_NAME, OUTGOING_COLL_NAME, PATTERNS_COLL_NAME, TEMPLATES_COLL_NAME])

  db = get_mongodb(mongodb_specs)
  hasher = Hasher()

  node_type_to_keys = {}
  if index_path is not None:
    index_pattern = process_index_pattern_file(index_path)
    node_type_to_keys = group_index_pattern_by_hash(index_pattern, db["node_types"])

  atoms_file_path = file_path + ".atoms"
  patterns_file_path = file_path + ".patterns"
  templates_file_path = file_path + ".templates"

  # TODO: Cover all possible links_N collections.
  with open(atoms_file_path, "w") as fh1, open(patterns_file_path, "w") as fh2, open(templates_file_path, "w") as fh3:
    logger.info("Indexing links_1")
    populate_sets(hasher, fh1, fh2, fh3, db["links_1"], bucket, node_type_to_keys)
    logger.info("Indexing links_2")
    populate_sets(hasher, fh1, fh2, fh3, db["links_2"], bucket, node_type_to_keys)
    logger.info("Indexing links_3")
    populate_sets(hasher, fh1, fh2, fh3, db["links_3"], bucket, node_type_to_keys)
    logger.info("Indexing links")
    populate_sets(hasher, fh1, fh2, fh3, db["links"], bucket, [])

  # TODO: Use python. (?)
  logger.info("Sorting index files (1/4)")
  os.system(f"sort -t , -k 1,1 {atoms_file_path} > {atoms_file_path}.sorted")
  logger.info("Sorting index files (2/4)")
  os.system(f"sort -t , -k 1,1 {patterns_file_path} > {patterns_file_path}.sorted")
  logger.info("Sorting index files (3/4)")
  os.system(f"sort -t , -k 1,1 {templates_file_path} > {templates_file_path}.sorted")
  logger.info("Sorting index files (4/4)")
  shutil.move(f"{atoms_file_path}.sorted", atoms_file_path)
  shutil.move(f"{patterns_file_path}.sorted", patterns_file_path)
  shutil.move(f"{templates_file_path}.sorted", templates_file_path)


def run():
  parser = argparse.ArgumentParser(
    "Indexes DAS (from MongoDB) to Couchbase", formatter_class=argparse.ArgumentDefaultsHelpFormatter
  )

  parser.add_argument("--file-path", help="output file path")

  parser.add_argument("--index-config", help="index patterns configuration file")

  parser.add_argument("--mongo-hostname", help="mongo hostname to connect to")
  parser.add_argument("--mongo-port", help="mongo port to connect to")
  parser.add_argument("--mongo-username", help="mongo username")
  parser.add_argument("--mongo-password", help="mongo password")
  parser.add_argument("--mongo-database", "-d", help="mongo database name to connect to")

  parser.add_argument("--couchbase-hostname", help="couchbase hostname to connect to")
  parser.add_argument("--couchbase-username", help="couchbase username")
  parser.add_argument("--couchbase-password", help="couchbase password")

  parser.add_argument("--verbose", "-v", help="debug mode")

  args = parser.parse_args()

  if args.verbose:
    logger.setLevel(logging.INFO)

  mongodb_specs = {
    "hostname": args.mongo_hostname or os.environ.get("DAS_MONGODB_HOSTNAME", "localhost"),
    "port": args.mongo_port or os.environ.get("DAS_MONGODB_PORT", 27017),
    "username": args.mongo_username or os.environ.get("DAS_DATABASE_USERNAME", "dbadmin"),
    "password": args.mongo_password or os.environ.get("DAS_DATABASE_PASSWORD", "das#secret"),
    "database": args.mongo_database or os.environ.get("DAS_DATABASE_NAME", "das"),
  }

  couchbase_specs = {
    "hostname": args.couchbase_hostname or os.environ.get("DAS_COUCHBASE_HOSTNAME", "localhost"),
    "username": args.couchbase_username or os.environ.get("DAS_DATABASE_USERNAME", "dbadmin"),
    "password": args.couchbase_password or os.environ.get("DAS_DATABASE_PASSWORD", "das#secret"),
  }

  main(mongodb_specs, couchbase_specs, args.file_path or "/tmp/all_pairs.txt", args.index_config)


if __name__ == "__main__":
  run()
