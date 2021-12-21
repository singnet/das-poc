from collections import defaultdict
from hashlib import md5
from operator import itemgetter
from typing import Any, Union

from atomese2metta.translator import AtomType, Expression, MSet


def sort_by_key_hash(keys_hashes: list[Any], keys: list[Any], reverse=False) -> tuple[tuple[Any], ...]:
  return tuple(zip(*sorted(zip(keys_hashes, keys), key=itemgetter(0), reverse=reverse)))


class Hasher:
  atom_type_dict = dict()
  hash_index = defaultdict(list)
  algorithm = md5

  def sort_expression(self, expression: Expression) -> tuple[tuple[str, ...], tuple[str, ...]]:
      keys_hashes = tuple( e._id for e in expression )

      keys = tuple(expression)
      set_from = expression.SET_FROM - 1
      to_keep_kh, to_sort_kh = keys_hashes[:set_from], keys_hashes[set_from:]
      to_keep_keys, to_sort_keys = keys[:set_from], keys[set_from:]
      sorted_keys_hashes, sorted_keys = sort_by_key_hash(to_sort_kh, to_sort_keys)
      keys: tuple[str, ...] = tuple(to_keep_keys) + sorted_keys
      keys_hashes: tuple[str, ...] = tuple(to_keep_kh) + sorted_keys_hashes
      return keys, keys_hashes

  def apply_alg(self, value: str) -> str:
    return self.algorithm(value.encode("utf-8")).digest().hex()

  def search_by_name(self, name: str) -> AtomType:
    return self.atom_type_dict.get(name, None)

  def get_type(self, name: str) -> AtomType:
    return self.search_by_name(self.search_by_name(name).type)

  def get_type_signature(self, atom_type: AtomType) -> str:
    name = atom_type.symbol
    _atom_type = self.search_by_name(atom_type.type)

    atom_type_id = (_atom_type._id or "") if _atom_type is not None else ""

    return name + atom_type_id

  def get_expression_type_hash(self, types: list[Union[list, str]], salt=None) -> str:
    ids = []
    for e in types:
      if isinstance(e, Expression):
        type_hash = e.type_hash
        type_hash = (type_hash or self.get_expression_type_hash(e, salt=e.SET_FROM))
        ids.append(type_hash)
      elif isinstance(e, AtomType):
        ids.append(e.type._id)
      elif isinstance(e, str):
        ids.append(self.get_type(e)._id)
      else:
        raise ValueError(e)

    if salt is not None:
      ids.insert(0, str(salt))

    return self.apply_alg("".join(ids))

  def get_expression_hash(self, expression: Union[Expression, str], level=0) -> str:
    if hasattr(expression, "_id") and expression._id is not None:
      return expression._id

    elif isinstance(expression, Expression):
      keys_hashes = [
        self.get_expression_hash(key, level=level + 1) for key in expression
      ]

      if expression.SET_FROM is None:
        keys = tuple(expression)
      else:
        keys, keys_hashes = self.sort_expression(expression)


      if expression.type_hash is None:
        expression_type_hash = self.get_expression_type_hash(keys, salt=expression.SET_FROM)
        expression.type_hash = expression_type_hash
      else:
        expression_type_hash = expression.type_hash

      signature = expression_type_hash + "".join(keys_hashes)
      hash_id = self.apply_alg(signature)
      expression._id = hash_id
      self.add_hash(expression)
      return hash_id

    else:
      raise ValueError(f"InvalidSymbol: {expression}")

  def hash_atom_type(self, atom_type):
    value = self.get_type_signature(atom_type)
    _id = self.apply_alg(value)
    atom_type._id = _id
    self.atom_type_dict[atom_type.symbol] = atom_type
    self.add_hash(atom_type)

  def hash_expression(self, expression):
    self.get_expression_hash(expression)

  def add_hash(self, value):
    self.hash_index[value._id].append(value)
