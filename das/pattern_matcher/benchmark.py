from abc import ABC, abstractmethod
from datetime import datetime
import time
from typing import List, Dict
from enum import Enum, auto
import numpy as np
import re
from couchbase.auth import PasswordAuthenticator
from couchbase.bucket import Bucket
from couchbase.cluster import Cluster
from das.helpers import get_mongodb
from das.pattern_matcher.db_interface import DBInterface
from das.pattern_matcher.couch_db import CouchDB
from das.pattern_matcher.mongo_db import MongoDB
from das.pattern_matcher.couch_mongo_db import CouchMongoDB
from das.pattern_matcher.pattern_matcher import PatternMatchingAnswer, LogicalExpression, Node, Link, Variable, Not, And, Or, LinkTemplate, TypedVariable

class DB_Architecture(int, Enum):
    """
    TODO: documentation
    """
    COUCHBASE = auto()
    MONGODB = auto()
    COUCHBASE_AND_MONGODB = auto()

class QueryType(int, Enum):
    """
    TODO: documentation
    """
    SAME_BIOLOGICAL_PROCESS = auto()
    SAME_OR_INHERITED_BIOLOGICAL_PROCESS = auto()
    REACTOME_LINKED_TO_UNIPROT = auto()

class TestLayout(int, Enum):
    """
    TODO: documentation
    """
    QUERY_1 = auto()
    QUERY_2 = auto()
    QUERY_3 = auto()

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

def _inheritance_template_link(va, vb):
    assert(all(isinstance(v, TypedVariable) for v in [va, vb]))
    return LinkTemplate('Inheritance', [va, vb], True)

def _inheritance_link(a, b):
    assert(not any(isinstance(v, TypedVariable) for v in [a, b]))
    return Link('Inheritance', [a, b], True)

def _member_template_link(va, vb):
    assert(all(isinstance(v, TypedVariable) for v in [va, vb]))
    return LinkTemplate('Member', [va, vb], True)

def _member_link(a, b):
    assert(not any(isinstance(v, TypedVariable) for v in [a, b]))
    return Link('Member', [a, b], True)

def _list_template_link(va, vb):
    assert(all(isinstance(v, TypedVariable) for v in [va, vb]))
    return LinkTemplate('List', [va, vb], True)

def _list_link(a, b):
    assert(not any(isinstance(v, TypedVariable) for v in [a, b]))
    return Link('List', [va, vb], True)

def _context_link(va, vb, vc):
    return \
    Link('Context', [
        _member_template_link(va, vb),
        Link('Evaluation', [
            Node('Predicate', 'has_location'),
            _list_template_link(va, vc)
        ], True)
    ], True)

def _evaluation_link(p, va, vb):
    return \
    Link('Evaluation', [
        Node('Predicate', p),
        _list_template_link(va, vb)
    ], True)

def _same_biological_process(gene_list: List[str]):
    v1 = Variable('V_BiologicalProcess')
    gene_nodes = [Node('Gene', name) for name in gene_list]
    member_links = [_member_link(gene_node, v1) for gene_node in gene_nodes]
    return And(member_links)

def _same_or_inherited_biological_process(gene_list: List[str]):
    v1 = Variable('V1_BiologicalProcess')
    v2 = Variable('V2_BiologicalProcess')
    tv1 = TypedVariable('V1_BiologicalProcess', 'BiologicalProcess')
    tv2 = TypedVariable('V2_BiologicalProcess', 'BiologicalProcess')
    tv3 = TypedVariable('V3_BiologicalProcess', 'BiologicalProcess')
    gene_node1 = Node('Gene', gene_list[0])
    gene_node2 = Node('Gene', gene_list[1])
    return And([
        _member_link(gene_node1, v1),
        Or([
            And([
                _member_link(gene_node2, v2),
                _inheritance_template_link(tv2, tv3),
                _inheritance_template_link(tv1, tv3),
            ]),
            _member_link(gene_node2, v1),
        ])
    ])

def _linked_reactome_uniprot(gene_list: List[str]):
    v1 = TypedVariable('V_BiologicalProcess', 'BiologicalProcess')
    v2 = TypedVariable('V_Uniprot', 'Uniprot')
    v3 = TypedVariable('V_Reactome', 'Reactome')
    v4 = TypedVariable('V_ReactomeName', 'Concept')
    v5 = TypedVariable('V_Location', 'Concept')
    v6 = TypedVariable('V_UniprotName', 'Concept')
    return And([
        _same_biological_process(gene_list),
        _member_template_link(v2, v1),
        _evaluation_link('has_name', v2, v6),
        _context_link(v2, v3, v5),
        _evaluation_link('has_name', v3, v4),
    ])

def build_query(query_type: QueryType, gene_list: List[str]):
    if query_type == QueryType.SAME_BIOLOGICAL_PROCESS:
        return _same_biological_process(gene_list)
    elif query_type == QueryType.SAME_OR_INHERITED_BIOLOGICAL_PROCESS:
        return _same_or_inherited_biological_process(gene_list)
    elif query_type == QueryType.REACTOME_LINKED_TO_UNIPROT:
        return _linked_reactome_uniprot(gene_list)
    else:
        raise ValueError(f"Invalid query type: {query_type.name}")

class BenchmarkResults:

    def __init__(self, architecture: str, test_layout: str):
        self.wall_time_per_run = []
        self.total_wall_time = None
        self.matched_queries = 0
        self._start_time = None
        self._stop_time = None
        self._start_round_time = None
        self._stop_round_time = None
        self.architecture = architecture
        self.test_layout = test_layout

    def __repr__(self):
        wall_time = np.array(self.wall_time_per_run)
        mean = np.mean(wall_time)
        stdev = np.std(wall_time)
        txt = []
        txt.append(f'DB backend architecture: {self.architecture}')
        txt.append(f'Test layout: {self.test_layout}')
        txt.append(f'{len(wall_time)} runs ({self.matched_queries} matched)')
        txt.append(f'Total time: {self.total_wall_time:.3f} seconds')
        txt.append(f'Average time per query: {mean:.3f} seconds (stdev: {stdev:.3f})')
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
            "hostname": "mongo",
            "port":  27017,
            "username": "dbadmin",
            "password": "dassecret",
            "database": "BIO",
        }
        couchbase_specs = {
            "hostname": "couchbase",
            "username": "dbadmin",
            "password": "dassecret",
        }
        cluster = Cluster(
            f'couchbase://{couchbase_specs["hostname"]}',
            authenticator=PasswordAuthenticator(
                couchbase_specs["username"], couchbase_specs["password"]
            ),
        )
        if architecture == DB_Architecture.COUCHBASE_AND_MONGODB:
            self.db = CouchMongoDB(cluster.bucket("das"), get_mongodb(mongodb_specs))
        elif architecture == DB_Architecture.COUCHBASE:
            self.db = CouchDB(cluster.bucket("das"), get_mongodb(mongodb_specs))
        elif architecture == DB_Architecture.MONGODB:
            self.db = MongoDB(get_mongodb(mongodb_specs))
        else:
            raise ValueError(f"Invalid DB architecture: {architecture.name}")
        self.query = {
            TestLayout.QUERY_1: '_query_1',
            TestLayout.QUERY_2: '_query_2',
            TestLayout.QUERY_3: '_query_3',
        }
        self._populate_all_genes()
        self.results = BenchmarkResults(architecture.name, test_layout.name)

    def _populate_all_genes(self):
        self.all_genes = self.db.get_all_nodes('Gene', names=True)

    def _add_node_names(self, query_answer: PatternMatchingAnswer):
        txt = str(query_answer)
        handles = re.findall("'[a-z0-9]{32}'", txt)
        for quoted_handle in handles:
            handle = quoted_handle[1:-1]
            try:
                node_name = self.db.get_node_name(handle)
                txt = re.sub(quoted_handle, f'{quoted_handle} ({node_name})', txt)
            except Exception:
                pass
        return txt

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
                print(self._add_node_names(query_answer))

    def _query_1(self, print_query_results):
        self._plain_query(QueryType.SAME_BIOLOGICAL_PROCESS, print_query_results)

    def _query_2(self, print_query_results):
        self._plain_query(QueryType.SAME_OR_INHERITED_BIOLOGICAL_PROCESS, print_query_results)

    def _query_3(self, print_query_results):
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
        count = 1
        self.results.start()
        for i in range(self.rounds):
            if self.test_layout == TestLayout.QUERY_1:
                self._query_1(print_query_results)
            elif self.test_layout == TestLayout.QUERY_2:
                self._query_2(print_query_results)
            elif self.test_layout == TestLayout.QUERY_3:
                self._query_3(print_query_results)
            else:
                raise ValueError(f"Invalid test layout: {self.test_layout.name}")
            if progress_bar:
                self._print_progress_bar(count, self.rounds)
            count += 1
        self.results.stop()

# Query 1

benchmark = DAS_Benchmark(DB_Architecture.COUCHBASE, 10000, 2, TestLayout.QUERY_1)
benchmark.run(print_query_results=False, progress_bar=True)
print(benchmark.results)

#benchmark = DAS_Benchmark(DB_Architecture.MONGODB, 10, 2, TestLayout.QUERY_1)
#benchmark.run(print_query_results=False, progress_bar=True)
#print(benchmark.results)

benchmark = DAS_Benchmark(DB_Architecture.COUCHBASE_AND_MONGODB, 10000, 2, TestLayout.QUERY_1)
benchmark.run(print_query_results=False, progress_bar=True)
print(benchmark.results)

# Query 2

benchmark = DAS_Benchmark(DB_Architecture.COUCHBASE, 1000, 2, TestLayout.QUERY_2)
benchmark.run(print_query_results=False, progress_bar=True)
print(benchmark.results)

#benchmark = DAS_Benchmark(DB_Architecture.MONGODB, 10, 2, TestLayout.QUERY_2)
#benchmark.run(print_query_results=False, progress_bar=True)
#print(benchmark.results)

benchmark = DAS_Benchmark(DB_Architecture.COUCHBASE_AND_MONGODB, 1000, 2, TestLayout.QUERY_2)
benchmark.run(print_query_results=False, progress_bar=True)
print(benchmark.results)

# Query 3

#benchmark = DAS_Benchmark(DB_Architecture.COUCHBASE_AND_MONGODB, 10, 2, TestLayout.QUERY_3)
#benchmark.run(print_query_results=False, progress_bar=True)
#print(benchmark.results)





