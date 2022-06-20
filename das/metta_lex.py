import argparse
from typing import Any, List, Union

from atomese2metta.translator import (AtomType, Expression, MSet,
                                      UnorderedExpression)
from hashing import Hasher
from helpers import get_logger
from ply.lex import lex

logger = get_logger()


class MettaLex(object):
  # List of token names.   This is always required
  tokens = (
    "COLON",
    "LPAREN",
    "RPAREN",
    "LCBRACKET",
    "RCBRACKET",
    "STRING",
    "ID",
  )

  # Regular expression rules for simple tokens
  t_COLON = r"\:"
  t_LPAREN = r"\("
  t_RPAREN = r"\)"
  t_LCBRACKET = r"\{"
  t_RCBRACKET = r"\}"

  def t_STRING(self, t):
    r'\"([^"]*?)\"'
    return t

  def t_ID(self, t):
    r"[a-zA-Z]+"
    return t

  # Define a rule so we can track line numbers
  def t_newline(self, t):
    r"\n+"
    t.lexer.lineno += len(t.value)

  # A string containing ignored characters (spaces and tabs)
  t_ignore = " \t"

  def t_error(self, t):
    raise AttributeError("Illegal character '{}' at line {}".format(t.value[0], t.lexer.lineno))
    # t.lexer.skip(1)

  def build(self, **kwargs):
    self.lexer = lex(module=self, **kwargs)

  def get_tokens(self, data: str):
    self.lexer.input(data)
    while True:
      tok = self.lexer.token()
      if not tok:
        break
      yield tok.lexpos, tok.type, tok.value


class MettaParser:
  LEX_CLASS = MettaLex
  HASHER_CLASS = Hasher
  NODE_TYPE = 'NODE_TYPE'
  NODE = 'NODE'
  EXPRESSION = 'EXPRESSION'
  SET_FROM_REST = ('Similarity',)

  def __init__(self):
    self.lex = self.LEX_CLASS()
    self.lex.build()
    self.hasher = self.HASHER_CLASS()

  def _parse(self, text: str):
    list_stack: List[Any] = list()
    current: List[Union[AtomType, Expression]] = list()

    unknown = AtomType(symbol='Unknown', mtype=None)
    self.hasher.hash_atom_type(unknown)
    yield self.NODE_TYPE, unknown

    base_type = AtomType(symbol='Type', mtype=None)
    self.hasher.hash_atom_type(base_type)
    yield self.NODE_TYPE, base_type

    for (_, token_type, value) in self.lex.get_tokens(text):
      if token_type in ("LPAREN", "LCBRACKET"):
        pointer = []
        current.append(pointer)
        list_stack.append(current)
        current = pointer
      elif token_type in ("RPAREN", "RCBRACKET"):
        current = list_stack.pop()
        pointer = current.pop()
        if isinstance(pointer[0], str) and pointer[0] == ":":
          _, symbol, type_ = pointer
          type_ = self.hasher.search_by_name(type_)
          atom_type = AtomType(symbol, type_)
          self.hasher.hash_atom_type(atom_type)
          if atom_type.type.symbol == "Type":
            yield self.NODE_TYPE, atom_type
          else:
            yield self.NODE, atom_type
        else:
          expression = []
          for v in pointer:
            if isinstance(v, str):
              expression.append(self.hasher.search_by_name(v))
            else:
              expression.append(v)
          if token_type == "RPAREN":
            if isinstance(expression[0], AtomType) and expression[0].symbol in self.SET_FROM_REST:
              expression = UnorderedExpression(expression)
            else:
              expression = Expression(expression)
          else:
            expression = MSet(expression)

          if len(list_stack) == 0:
            expression.is_root = True
          else:
            current.append(expression)

          yield self.EXPRESSION, expression
      else:
        current.append(value)

  @classmethod
  def parse(cls, text: str):
    return cls()._parse(text)


def main():
  parser = argparse.ArgumentParser("Parse MeTTa file")
  parser.add_argument("filename", type=str, help="Input .metta filename")

  args = parser.parse_args()

  with open(args.filename, 'r') as f:
    text = f.read()

  for type_name, expression in MettaParser.parse(text):
    logger.info(type_name, expression)


if __name__ == "__main__":
  main()
