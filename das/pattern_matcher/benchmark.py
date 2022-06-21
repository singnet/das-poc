from abc import ABC, abstractmethod
from typing import List
from enum import Enum, auto
import numpy as np
import os
from couchbase.auth import PasswordAuthenticator
from couchbase.bucket import Bucket
from couchbase.cluster import Cluster
from das.helpers import get_mongodb
from das.pattern_matcher.db_interface import DBInterface
from das.pattern_matcher.couch_mongo_db import CouchMongoDB
from das.pattern_matcher.pattern_matcher import PatternMatchingAnswer, LogicalExpression, Node, Link, Variable, Not, And, Or

class DB_Architecture(int, Enum):
    """
    TODO: documentation
    """
    COUCHBASE_AND_MONGODB = auto()

class QueryType(int, Enum):
    """
    TODO: documentation
    """
    SAME_BIOLOGICAL_PROCESS = auto()

class TestLayout(int, Enum):
    """
    TODO: documentation
    """
    SIMPLE_AND_QUERY = auto()

def _single_random_selection(v):
    return v[np.random.randint(len(v))]

def _random_selection(v, n=1):
    if n == 1:
        return _single_random_selection(v)
    assert n <= (len(v) / 2)
    a = v.copy()
    selected = []
    for i in range(n):
        s = _single_random_selection(a)
        a.remove(s)
        selected.append(s)
    return selected

def _same_biological_process(gene_list: List[str]):
    v1 = Variable('BiologicalProcess')
    gene_nodes = [Node('Gene', name) for name in gene_list]
    member_links = [Link('Member', [gene_node, v1], True) for gene_node in gene_nodes]
    return And(member_links)

def build_query(query_type: QueryType, gene_list: List[str]):
    if query_type == QueryType.SAME_BIOLOGICAL_PROCESS:
        return _same_biological_process(gene_list)
    else:
        raise ValueError("Invalid query type: {query_type}")

class DAS_Benchmark():
    """
    TODO: documentation
    """

    def __init__(self, architecture: DB_Architecture, rounds: int, gene_count: int, test_layout: TestLayout):
        self.rounds = rounds
        self.gene_count = gene_count
        self.test_layout = test_layout
        #TODO Refactory to put these collection names just in one place
        mongodb_specs = {
            "hostname": "localhost",
            "port":  27017,
            "username": "dbadmin",
            "password": "das#secret",
            "database": "BIO",
            #"hostname": os.environ.get("DAS_MONGODB_HOSTNAME", "localhost"),
            #"port": os.environ.get("DAS_MONGODB_PORT", 27017),
            #"username": os.environ.get("DAS_DATABASE_USERNAME", "dbadmin"),
            #"password": os.environ.get("DAS_DATABASE_PASSWORD", "das#secret"),
            #"database": os.environ.get("DAS_DATABASE_NAME", "BIO"),
        }
        couchbase_specs = {
            "hostname": "localhost",
            "username": "dbadmin",
            "password": "das#secret",
            #"hostname": os.environ.get("DAS_COUCHBASE_HOSTNAME", "localhost"),
            #"username": os.environ.get("DAS_DATABASE_USERNAME", "dbadmin"),
            #"password": os.environ.get("DAS_DATABASE_PASSWORD", "das#secret"),
        }
        cluster = Cluster(
            f'couchbase://{couchbase_specs["hostname"]}',
            authenticator=PasswordAuthenticator(
                couchbase_specs["username"], couchbase_specs["password"]
            ),
        )
        if architecture == DB_Architecture.COUCHBASE_AND_MONGODB:
            self.db = CouchMongoDB(cluster.bucket("das"), get_mongodb(mongodb_specs))
        else:
            raise ValueError("Invalid DB architecture: {architecture}")
        self._populate_all_genes()

    def _populate_all_genes(self):
        self.all_genes = self.db.get_all_nodes('Gene')

    def _simple_and_query(self, print_query_results=False):
        gene_list = _random_selection(self.all_genes, self.gene_count)
        query = build_query(QueryType.SAME_BIOLOGICAL_PROCESS, gene_list)
        query_answer = PatternMatchingAnswer()
        matched = query.matched(self.db, query_answer)
        if print_query_results and matched:
            print(query)
            print(query_answer)

    def run(self, print_query_results=False):
        for i in range(self.rounds):
            if self.test_layout == TestLayout.SIMPLE_AND_QUERY:
                self._simple_and_query(print_query_results)
            else:
                raise ValueError("Invalid test layout: {self.test_layout}")

benchmark = DAS_Benchmark(
    DB_Architecture.COUCHBASE_AND_MONGODB,
    1000,
    5,
    TestLayout.SIMPLE_AND_QUERY
)
benchmark.run()
