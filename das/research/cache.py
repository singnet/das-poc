import datetime
from abc import ABC, abstractmethod
from copy import deepcopy
from typing import Any, Optional

import couchbase.exceptions
from couchbase.bucket import Bucket

from das.research.heap import Heap, PrioritizedItem


class CacheException(Exception):
  pass


class DocumentNotFoundException(CacheException):
  pass


class AbstractCouchbaseClient(ABC):
  @abstractmethod
  def add(self, key: str, value: Any) -> None:
    pass

  @abstractmethod
  def get(self, key: str) -> Any:
    pass


class FakeCouchbaseClient(AbstractCouchbaseClient):
  def __init__(self):
    self.d = dict()
    self.total_add_calls = 0

  def add(self, key: str, value: Any) -> None:
    self.total_add_calls += 1
    self.d[key] = value

  def get(self, key: str) -> Any:
    if key in self.d:
      return deepcopy(self.d[key])
    else:
      raise DocumentNotFoundException


class CouchbaseClient(AbstractCouchbaseClient):
  def __init__(self, bucket: Bucket, collection_name: str):
    self.collection_client = bucket.collection(collection_name)

  def add(self, key: str, value: Any) -> None:
    self.collection_client.upsert(key, value, timeout=datetime.timedelta(seconds=100))

  def get(self, key: str) -> Optional[Any]:
    try:
      return self.collection_client.get(key).content
    except couchbase.exceptions.DocumentNotFoundException:
      raise DocumentNotFoundException


class CachedCouchbaseClient:
  def __init__(self, couchbase_client: CouchbaseClient, limit: int):
    self.couchbase_client = couchbase_client
    self.heap = Heap()
    self.limit = limit
    self.current_size = 0

  def remove_until_below_limit(self, delta: int):
    while self.current_size + delta > self.limit:
      item = self.heap.heap_pop()
      self.current_size -= item.size
      self.couchbase_client.add(item.key, item.value)

  def add(self, key: str, value: Any, size: int) -> None:
    if (self.heap and size < self.heap[0].size) or size > self.limit:
      self.couchbase_client.add(key, value)
      return

    old_item = None
    if self.heap.contains(key):
      old_item = self.heap.get_item_by_key(key)
      delta = size - old_item.size
    else:
      delta = size

    item = PrioritizedItem(key=key, value=value, size=size)

    if self.current_size + delta > self.limit:
      self.remove_until_below_limit(delta)

    if old_item is not None:
      idx = self.heap.get_idx_by_key(key)
      self.heap[idx] = item
      self.heap.fix_down(item)
    else:
      self.heap.heap_push(item)

    self.current_size += delta

  def flush(self):
    for item in self.heap:
      self.couchbase_client.add(item.key, item.value)
    self.heap = Heap()
    self.current_size = 0

  def get(self, key: str) -> Optional[Any]:
    if self.heap.contains(key):
      return self.heap.get_item_by_key(key).value
    else:
      return self.couchbase_client.get(key)


def test_cached_client_should_return_values_from_embedded_client() -> None:
  fake = FakeCouchbaseClient()
  cached = CachedCouchbaseClient(fake, limit=3)

  fake.add('1', [1])
  fake.add('2', [2, 2])
  fake.add('3', [3, 3, 3])

  assert cached.get('1') == [1]
  assert cached.get('2') == [2, 2]
  assert cached.get('3') == [3, 3, 3]

  assert fake.total_add_calls == 3


def test_cached_client_should_update_value_without_updating_actual_client() -> None:
  fake = FakeCouchbaseClient()
  cached = CachedCouchbaseClient(fake, limit=3)

  fake.add('1', [1])
  fake.add('2', [2, 2])
  fake.add('3', [3, 3, 3])

  assert cached.get('1') == [1]
  cached.add('1', [10], size=1)
  assert cached.current_size == 1

  e = cached.get('1')
  cached.add('1', [10, 10], size=2)
  assert cached.current_size == 2

  e = cached.get('2')
  e.append(2)
  assert len(e) == 3
  assert e == [2, 2, 2]

  assert fake.total_add_calls == 3


def test_cached_client_should_call_actual_client_if_threshold_() -> None:
  fake = FakeCouchbaseClient()
  cached = CachedCouchbaseClient(fake, limit=7)

  fake.add('1', [1])
  fake.add('2', [2])
  fake.add('3', [3])

  item = cached.get('1')
  item.append(1)
  item.append(1)
  cached.add('1', item, 3)

  assert cached.current_size == 3
  assert fake.total_add_calls == 3

  assert fake.get('1') == [1]

  item = cached.get('2')
  item.append(2)
  item.append(2)
  cached.add('2', item, 3)

  assert cached.current_size == 6
  assert fake.total_add_calls == 3

  item = cached.get('3')
  item.append(3)
  item.append(3)


def test_cached_should_not_call_actual_client_without_limit_being_achieved() -> None:
  fake = FakeCouchbaseClient()
  cached = CachedCouchbaseClient(fake, limit=8)

  cached.add('1', [1], size=1)

  cached.add('2', [2], size=1)
  v2 = cached.get('2')
  v2.append(2)
  cached.add('2', v2, size=len(v2))

  assert cached.current_size == 3

  v2 = cached.get('2')
  v2.append(2)
  cached.add('2', v2, size=len(v2))

  assert cached.current_size == 4

  cached.add('3', [3], size=1)
  v3 = cached.get('3')
  v3.append(3)
  cached.add('3', v3, size=len(v3))
  v3 = cached.get('3')
  v3.append(3)
  cached.add('3', v3, size=len(v3))

  assert cached.current_size == 7
  assert fake.total_add_calls == 0

  cached.add('4', [4, 4], size=2)
  assert fake.total_add_calls == 1
  assert cached.current_size == 8


def test_cached_should_flush_correctly() -> None:
  fake = FakeCouchbaseClient()
  cached = CachedCouchbaseClient(fake, limit=8)

  cached.add('1', [1], size=1)
  cached.add('2', [2], size=1)
  cached.add('3', [3], size=1)

  assert fake.total_add_calls == 0

  cached.flush()

  assert fake.total_add_calls == 3


def test_cached_should_just_call_embedded_client_if_size_greater_than_limit() -> None:
  fake = FakeCouchbaseClient()
  cached = CachedCouchbaseClient(fake, limit=1)

  cached.add('1', [1, 2], size=2)

  assert fake.total_add_calls == 1
  assert cached.current_size == 0

  assert cached.get('1') == [1, 2]


def test_cached_should_just_call_embedded_client_if_size_greater_than_limit_zero() -> None:
  fake = FakeCouchbaseClient()
  cached = CachedCouchbaseClient(fake, limit=0)

  cached.add('1', [1, 2], size=2)

  assert fake.total_add_calls == 1
  assert cached.current_size == 0

  assert cached.get('1') == [1, 2]
