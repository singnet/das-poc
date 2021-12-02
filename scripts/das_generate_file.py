import argparse
import os
import shutil

from couchbase import exceptions as cb_exceptions
from couchbase.auth import PasswordAuthenticator
from couchbase.cluster import Cluster
from couchbase.management.collections import CollectionSpec
from pymongo.collection import Collection

from helpers import get_mongodb, get_logger
from util import Clock, Statistics, AccumulatorClock

logger = get_logger()

INCOMING_COLL_NAME = "IncomingSet"
OUTGOING_COLL_NAME = "OutgoingSet"

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


def populate_sets(fh, collection: Collection, bucket):
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
      keys = {v for k, v in doc.items() if k.startswith("key")}
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
        fh.write("{},{}\n".format(key, v))
    incoming_time_statistics.add(incoming_clock.elapsed_time_ms())
    acc_clock_block4.pause()

    count += 1
    if count % 10000 == 0:
      logger.debug("\n")
      logger.info("Documents processed: [{}/{}]".format(count, total))
      logger.debug("Batch time (sec):         {}".format(batch_clock.elapsed_time_seconds()))
      logger.debug("Block full (sec):         {}".format(acc_clock_full.acc_seconds()))
      logger.debug("Block1 (sec):             {}".format(acc_clock_block1.acc_seconds()))
      logger.debug("Block2 (sec):             {}".format(acc_clock_block2.acc_seconds()))
      logger.debug("Block3 (sec):             {}".format(acc_clock_block3.acc_seconds()))
      logger.debug("Block4 (sec):             {}".format(acc_clock_block4.acc_seconds()))

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
      acc_clock_full.reset()

    acc_clock_full.pause()

  cursor.close()


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


def main(mongodb_specs, couchbase_specs, file_path):
  cluster = Cluster(
    f"couchbase://{couchbase_specs['hostname']}",
    authenticator=PasswordAuthenticator(couchbase_specs["username"], couchbase_specs["password"]))
  bucket = cluster.bucket("das")

  create_collections(
    bucket=bucket,
    collections_names=[INCOMING_COLL_NAME, OUTGOING_COLL_NAME])

  db = get_mongodb(mongodb_specs)

  # TODO: Cover all possible links_N collections.
  with open(file_path, "w") as fh:
    logger.info("Indexing links_1")
    populate_sets(fh, db["links_1"], bucket)
    logger.info("Indexing links_2")
    populate_sets(fh, db["links_2"], bucket)
    logger.info("Indexing links_3")
    populate_sets(fh, db["links_3"], bucket)
    logger.info("Indexing links")
    populate_sets(fh, db["links"], bucket)

  # TODO: Use python. (?)
  os.system(f"sort -t , -k 1,1 {file_path} > {file_path}.sorted")
  shutil.move(f"{file_path}.sorted", file_path)


def run():
  parser = argparse.ArgumentParser(
    "Indexes DAS (from MongoDB) to Couchbase", formatter_class=argparse.ArgumentDefaultsHelpFormatter
  )

  parser.add_argument("--file-path", help="output file path")

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
    logger.setLevel(logging)

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

  main(mongodb_specs, couchbase_specs, args.file_path or "/tmp/all_pairs.txt")


if __name__ == "__main__":
  run()
