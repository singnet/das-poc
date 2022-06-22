from abc import ABC, abstractmethod
from datetime import datetime
import time
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
    REACTOME_LINKED_TO_UNIPROT = auto()

class TestLayout(int, Enum):
    """
    TODO: documentation
    """
    SIMPLE_AND_QUERY = auto()
    COMPLEX_AND_QUERY = auto()

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

def _context_link(va, vb, vc):
    return \
    Link('Context', [
        Link('Member', [va, vb], True),
        Link('Evaluation', [
            Node('Predicate', 'has_location'),
            Link('List', [va, vc], True)
        ], True)
    ], True)

def _evaluation_link(p, va, vb):
    return \
    Link('Evaluation', [
        Node('Predicate', p),
        Link('List', [va, vb], True),
    ], True)

def _member_link(va, vb):
    return Link('Member', [va, vb], True)

def _same_biological_process(gene_list: List[str]):
    v1 = Variable('BiologicalProcess')
    gene_nodes = [Node('Gene', name) for name in gene_list]
    member_links = [_member_link(gene_node, v1) for gene_node in gene_nodes]
    return And(member_links)

def _linked_reactome_uniprot(gene_list: List[str]):
    v1 = Variable('BiologicalProcess')
    v2 = Variable('UniProt')
    v3 = Variable('Reactome')
    v4 = Variable('ReactomeName')
    v5 = Variable('Location')
    v6 = Variable('UniprotName')
    
    return And([
        _same_biological_process(gene_list),
        _evaluation_link('has_name', v3, v4),
        _context_link(v2, v3, v5),
        _evaluation_link('has_name', v2, v6),
        _member_link(v2, v1)
    ])

def build_query(query_type: QueryType, gene_list: List[str]):
    if query_type == QueryType.SAME_BIOLOGICAL_PROCESS:
        return _same_biological_process(gene_list)
    elif query_type == QueryType.REACTOME_LINKED_TO_UNIPROT:
        return _linked_reactome_uniprot(gene_list)
    else:
        raise ValueError("Invalid query type: {query_type}")

class BenchmarkResults:

    def __init__(self):
        self.wall_time_per_run = []
        self.total_wall_time = None
        self.matched_queries = 0
        self._start_time = None
        self._stop_time = None
        self._start_round_time = None
        self._stop_round_time = None

    def __repr__(self):
        wall_time = np.array(self.wall_time_per_run)
        mean = np.mean(wall_time)
        stdev = np.std(wall_time)
        txt = []
        txt.append(f'{len(wall_time)} runs ({self.matched_queries} matched)')
        txt.append(f'Total time: {self.total_wall_time:.2f} seconds')
        txt.append(f'Average time per round: {mean:.2f} seconds (stdev: {stdev:.2f})')
        return '\n'.join(txt)

    def elapsed_time(self):
        if self.total_wall_time:
            return self.total_wall_time
        else:
            return time.perf_counter() - self._start_time
    
    def start(self):
        assert not self._start_time
        self._start_time = time.perf_counter()

    def stop(self):
        assert self._start_time
        self._stop_time = time.perf_counter()
        self.total_wall_time = self._stop_time - self._start_time

    def start_round(self):
        assert not self._start_round_time
        assert not self.total_wall_time
        self._start_round_time = time.perf_counter()

    def stop_round(self):
        assert self._start_round_time
        assert not self.total_wall_time
        self._stop_round_time = time.perf_counter()
        self.wall_time_per_run.append(self._stop_round_time - self._start_round_time)
        self._start_round_time = None
        self._stop_round_time = None

class DAS_Benchmark:
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
        self.results = BenchmarkResults()

    def _populate_all_genes(self):
        self.all_genes = self.db.get_all_nodes('Gene')

    def _plain_query(self, query_type, print_query_results):
        gene_list = _random_selection(self.all_genes, self.gene_count)
        query = build_query(query_type, gene_list)
        query_answer = PatternMatchingAnswer()
        self.results.start_round()
        matched = query.matched(self.db, query_answer)
        self.results.stop_round()
        if matched:
            self.results.matched_queries += 1
            if print_query_results:
                print(query)
                print(query_answer)

    def _simple_and_query(self, print_query_results):
        self._plain_query(QueryType.SAME_BIOLOGICAL_PROCESS, print_query_results)

    def _complex_and_query(self, print_query_results):
        self._plain_query(QueryType.REACTOME_LINKED_TO_UNIPROT, print_query_results)

    def _print_progress_bar(self, iteration, total, length=50):
        filled_length = int(length * iteration // total)
        previous = int(length * (iteration - 1) // total)
        if iteration == 1 or filled_length > previous or iteration == total:
            percent = ("{0:.0f}").format(100 * (iteration / float(total)))
            fill='█'
            bar = fill * filled_length + '-' * (length - filled_length)
            matched = self.results.matched_queries
            elapsed = self.results.elapsed_time()
            print(f'\rProgress: |{bar}| {percent}% complete ({iteration}/{total}) {matched} matched {elapsed:.0f} seconds', end = '\r')
            if iteration == total: 
                print()

    def run(self, print_query_results=False, progress_bar=False):
        v1 = Variable('V_BiologicalProcess')
        v2 = Variable('V_UniProt')
        v3 = Variable('V_Reactome')
        v4 = Variable('V_ReactomeName')
        v5 = Variable('V_Location')
        v6 = Variable('V_UniprotName')
        v7 = Variable('V_Gene')

        expression = And([
            #_evaluation_link('has_name', v3, v4),
            _context_link(v2, v3, v5),
            #_evaluation_link('has_name', v2, v6),
            #_member_link(v2, v1)
        ])

        print(expression)
        answer: PatternMatchingAnswer = PatternMatchingAnswer()
        print(expression.matched(self.db, answer))
        print(answer)
        return

        count = 1
        self.results.start()
        for i in range(self.rounds):
            if self.test_layout == TestLayout.SIMPLE_AND_QUERY:
                self._simple_and_query(print_query_results)
            elif self.test_layout == TestLayout.COMPLEX_AND_QUERY:
                self._complex_and_query(print_query_results)
            else:
                raise ValueError("Invalid test layout: {self.test_layout}")
            if progress_bar:
                self._print_progress_bar(count, self.rounds)
            count += 1
        self.results.stop()

#benchmark = DAS_Benchmark(
#    DB_Architecture.COUCHBASE_AND_MONGODB,
#    100,
#    2,
#    TestLayout.SIMPLE_AND_QUERY
#)
#benchmark.run(print_query_results=False, progress_bar=True)
#print(benchmark.results)

benchmark = DAS_Benchmark(
    DB_Architecture.COUCHBASE_AND_MONGODB,
    1000,
    2,
    TestLayout.COMPLEX_AND_QUERY
)
benchmark.run(print_query_results=False, progress_bar=True)
#print(benchmark.results)





