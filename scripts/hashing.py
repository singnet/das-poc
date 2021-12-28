from collections import defaultdict
from hashlib import md5
from operator import itemgetter
from typing import Any, Union

from atomese2metta.translator import AtomType, Expression, MSet


class Hasher:
  atom_type_dict = dict()
  hash_index = defaultdict(list)
  algorithm = md5

  @staticmethod
  def sort_expression(expression: Expression):
    if expression.SET_FROM is None:
      return
    set_from = expression.SET_FROM - 1
    expression[set_from:] = sorted(expression[set_from:], key=lambda e: e._id)

  def apply_alg(self, value: str) -> str:
    return self.algorithm(value.encode("utf-8")).digest().hex()

  def search_by_name(self, name: str) -> AtomType:
    return self.atom_type_dict.get(name, None)

  def get_type(self, name: str) -> AtomType:
    return self.search_by_name(name).type

  def get_type_signature(self, atom_type: AtomType) -> str:
    name = atom_type.symbol
    _atom_type = atom_type.type

    atom_type_id = (_atom_type._id or "") if _atom_type is not None else ""

    return name + atom_type_id

  def _set_expression_type_hash(self, expression: Expression):
    ids = []
    for e in expression:
      if isinstance(e, Expression):
        if e.type_hash is None:
          self._set_expression_type_hash(e)
        ids.append(e.type_hash)
      elif isinstance(e, AtomType):
        ids.append(e.type._id)
      elif isinstance(e, str):
        ids.append(self.get_type(e)._id)
      else:
        raise ValueError(e)

    if set_from := expression.SET_FROM is not None:
      ids.insert(0, str(set_from))

    expression.type_hash = self.apply_alg("".join(ids))

  def get_expression_hash(self, expression: Union[Expression, AtomType], level=0) -> str:
    if hasattr(expression, "_id") and expression._id is not None:
      return expression._id

    elif isinstance(expression, Expression):
      for e in expression:
        self.get_expression_hash(e, level=level+1)

      if expression.SET_FROM is not None:
        self.sort_expression(expression)

      if expression.type_hash is None:
        self._set_expression_type_hash(expression)
      expression_type_hash = expression.type_hash

      signature = expression_type_hash + "".join([e._id for e in expression])
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
