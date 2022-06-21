import argparse
import os

from das.helpers import get_mongodb


class QueryDas:
  NODE_COLLS = [
    "node_types",
    "nodes",
  ]

  EXPRESSION_COLLS = [
    'links_1',
    'links_2',
    'links_3',
    'links',
  ]

  def __init__(self, db):
    self.db = db
    self.nodes = {node['_id']: node for node in self.get_nodes()}

  def get_nodes(self):
    nodes = []
    for col_name in self.NODE_COLLS:
      col = self.db[col_name]
      nodes.extend(list(col.find({})))
    return nodes

  def get_expression_by_id(self, _id):
    expressions = []
    for coll_name in self.EXPRESSION_COLLS:
      expressions.extend(list(self.db[coll_name].find({"_id": _id})))

    if len(expressions) > 1:
      raise ValueError("expressions must have unique _id")

    if not expressions:
      return None

    return expressions[0]


  def query_keys(self, key1=None, key2=None, key3=None):
    pass



def main(mongodb_specs):
  db = get_mongodb(mongodb_specs)
  query = QueryDas(db)

  print(query.get_expression_by_id("6ffeda368429361ea22067edd506931d"))


def run():
  parser = argparse.ArgumentParser(
    "Querying", formatter_class=argparse.ArgumentDefaultsHelpFormatter
  )

  parser.add_argument('--hostname', help='mongo hostname to connect to')
  parser.add_argument('--port', help='mongo port to connect to')
  parser.add_argument('--username', help='mongo username')
  parser.add_argument('--password', help='mongo password')
  parser.add_argument('--database', '-d', help='mongo database name to connect to')

  args = parser.parse_args()

  mongodb_specs = {
    'hostname': args.hostname or os.environ.get('DAS_MONGODB_HOSTNAME', 'localhost'),
    'port': args.port or os.environ.get('DAS_MONGODB_PORT', 27017),
    'username': args.username or os.environ.get('DAS_DATABASE_USERNAME', 'dbadmin'),
    'password': args.password or os.environ.get('DAS_DATABASE_PASSWORD', 'das#secret'),
    'database': args.database or os.environ.get('DAS_DATABASE_NAME', 'das'),
  }

  main(mongodb_specs)


if __name__ == "__main__":
  run()
