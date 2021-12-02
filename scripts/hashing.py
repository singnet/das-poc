from collections import defaultdict
from hashlib import md5
from operator import itemgetter
from typing import Any, Union

from atomese2metta.translator import AtomType, Expression, MSet


def sort_by_key_hash(keys_hashes: list[Any], keys: list[Any], reverse=False) -> tuple[tuple[Any], ...]:
  return tuple(zip(*sorted(zip(keys_hashes, keys), key=itemgetter(0), reverse=reverse)))


class Hasher:

  def __init__(self, algorithm=md5):
    self.algorithm = algorithm
    self.atom_type_dict = dict()
    self.hash_index = defaultdict(list)

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
        type_hash = (type_hash or self.get_expression_type_hash(e, salt=e.SALT))
        ids.append(type_hash)
      elif isinstance(e, str):
        ids.append(self.get_type(e)._id)
      else:
        raise ValueError(e)

    if salt is not None:
      ids.insert(0, salt)

    return self.apply_alg("".join(ids))

  def get_expression_hash(self, expression: Union[Expression, str], level=0) -> str:
    if isinstance(expression, str):
      return self.search_by_name(expression)._id

    elif isinstance(expression, Expression):
      if expression._id is not None:
        return expression._id

      keys_hashes = [
        self.get_expression_hash(key, level=level + 1) for key in expression
      ]

      keys = list(expression)
      if isinstance(expression, MSet):
        keys_hashes, keys = sort_by_key_hash(keys_hashes, keys)

      if expression.type_hash is None:
        expression_type_hash = self.get_expression_type_hash(keys, salt=expression.SALT)
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
