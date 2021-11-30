from dataclasses import dataclass, field
from typing import Any, MutableSequence


@dataclass(order=True)
class PrioritizedItem:
  size: int
  key: str = field(compare=False)
  value: Any = field(compare=False)


class Heap(MutableSequence[PrioritizedItem]):
  def __init__(self):
    self.v = []
    self.map_key_to_pos: dict[str, int] = dict()

  def __setitem__(self, i: int, value: PrioritizedItem) -> None:
    self.v[i] = value
    self.map_key_to_pos[value.key] = i

  def __getitem__(self, i: int) -> PrioritizedItem:
    return self.v[i]

  def __delitem__(self, key: int) -> None:
    del self.v[key]

  def __len__(self):
    return len(self.v)

  def insert(self, index: int, value: PrioritizedItem) -> None:
    self.v.insert(index, value)

  def _sift_down(self, startpos: int, pos: int) -> None:
    newitem = self[pos]
    # Follow the path to the root, moving parents down until finding a place
    # newitem fits.
    while pos > startpos:
      parentpos = (pos - 1) >> 1
      parent = self[parentpos]
      if newitem < parent:
        self[pos] = parent
        pos = parentpos
        continue
      break
    self[pos] = newitem

  def _sift_up(self, pos: int) -> None:
    endpos = len(self)
    startpos = pos
    newitem = self[pos]
    # Bubble up the smaller child until hitting a leaf.
    childpos = 2 * pos + 1  # leftmost child position
    while childpos < endpos:
      # Set childpos to index of smaller child.
      rightpos = childpos + 1
      if rightpos < endpos and not self[childpos] < self[rightpos]:
        childpos = rightpos
      # Move the smaller child up.
      self[pos] = self[childpos]
      pos = childpos
      childpos = 2 * pos + 1
    # The leaf at pos is empty now.  Put newitem there, and bubble it up
    # to its final resting place (by sifting its parents down).
    self[pos] = newitem

    self._sift_down(startpos, pos)

  def contains(self, key: str) -> bool:
    return key in self.map_key_to_pos

  def get_item_by_key(self, key: str) -> PrioritizedItem:
    return self.v[self.map_key_to_pos[key]]

  def get_idx_by_key(self, key: str) -> int:
    return self.map_key_to_pos[key]

  def fix_down(self, item: PrioritizedItem) -> None:
    if item.key not in self.map_key_to_pos:
      return
    i = self.map_key_to_pos[item.key]
    self._fix_down(i)

  def _fix_down(self, i: int) -> None:
    n = len(self)
    if i >= n:
      return
    lpos = 2*i
    rpos = lpos + 1
    minpos = i
    if rpos < n:
      if self[minpos] > self[rpos]:
        minpos = rpos
    if lpos < n:
      if self[minpos] > self[lpos]:
        minpos = lpos
    if minpos != i:
      self[i], self[minpos] = self[minpos], self[i]
      self._fix_down(minpos)

  def heap_push(self, item: PrioritizedItem):
    """Push item onto heap, maintaining the heap invariant."""
    self.append(item)
    self._sift_down(0, len(self) - 1)

  def heap_pop(self) -> PrioritizedItem:
    """Pop the smallest item off the heap, maintaining the heap invariant."""
    assert len(self) > 0
    last_element = self.pop()  # raises appropriate IndexError if heap is empty
    if len(self) > 0:
      return_item = self[0]
      self[0] = last_element
      self._sift_up(0)
      del self.map_key_to_pos[return_item.key]
      return return_item

    del self.map_key_to_pos[last_element.key]
    return last_element


def test_heap_should_behave_like_a_heap():
  v = Heap()
  n = 1000
  for i in range(n):
    v.heap_push(PrioritizedItem(key=str(i), size=i, value=''))

  assert v[0].size == 0
  for i in range(n // 2):
    l = 2*i
    r = 2*i + 1

    if l < n:
      assert v[i] <= v[l]

    if r < n:
      assert v[i] <= v[r]


def test_fix_down_should_keep_heap_constraints():
  v = Heap()
  n = 1000
  for i in range(n):
    v.heap_push(PrioritizedItem(key=str(i), size=i, value=''))

  v[13].size = n+1

  assert v[13].size > v[26].size
  assert v[13].size > v[27].size

  assert v.map_key_to_pos[v[13].key] == 13
  v.fix_down(v[13])
  assert v[13].size == 26

  for i in range(n // 2):
    l = 2 * i
    r = 2 * i + 1
    if l < n:
      assert v[i] <= v[l]
    if r < n:
      assert v[i] <= v[r]


def test_heap_pop_should_return_items_in_order():
  h = Heap()
  h.heap_push(PrioritizedItem(key=str('3'), size=3, value=''))
  h.heap_push(PrioritizedItem(key=str('2'), size=2, value=''))
  h.heap_push(PrioritizedItem(key=str('7'), size=7, value=''))
  h.heap_push(PrioritizedItem(key=str('4'), size=4, value=''))
  h.heap_push(PrioritizedItem(key=str('1'), size=1, value=''))
  h.heap_push(PrioritizedItem(key=str('5'), size=5, value=''))
  h.heap_push(PrioritizedItem(key=str('6'), size=6, value=''))

  for i in range(1, 8):
    assert h.heap_pop().size == i




